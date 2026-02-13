#!/usr/bin/env python
# This script intended to enrich data of catalogs entries

import copy
import logging
import typer
import datetime
import re
from urllib.parse import urlparse
import requests
from requests.exceptions import ConnectionError
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
import csv
import json
import os
import shutil
import pprint
import tqdm
import zstandard as zstd
import duckdb
import hashlib
import subprocess
from dataclasses import dataclass, field
from collections import defaultdict
from typing import List, Dict, Any, Optional

from constants import (
    ENTRY_TEMPLATE,
    CUSTOM_SOFTWARE_KEYS,
    MAP_SOFTWARE_OWNER_CATALOG_TYPE,
    DOMAIN_LOCATIONS,
    DEFAULT_LOCATION,
    COUNTRIES_LANGS,
    MAP_CATALOG_TYPE_SUBDIR,
    COUNTRIES,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Get script directory and repository root for path resolution
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)

ROOT_DIR = os.path.join(_REPO_ROOT, "data", "entities")
SCHEDULED_DIR = os.path.join(_REPO_ROOT, "data", "scheduled")
SOFTWARE_DIR = os.path.join(_REPO_ROOT, "data", "software")
DATASETS_DIR = os.path.join(_REPO_ROOT, "data", "datasets")
UNPROCESSED_DIR = os.path.join(_REPO_ROOT, "data", "_unprocessed")

app = typer.Typer()


def load_jsonl(filepath):
    data = []
    f = open(filepath, "r", encoding="utf8")
    for l in f:
        data.append(json.loads(l))
    f.close()
    return data


def load_jsonl_zst(filepath):
    """Load and decompress JSONL.zst file"""
    data = []
    dctx = zstd.ZstdDecompressor()
    with open(filepath, "rb") as f:
        with dctx.stream_reader(f) as reader:
            text_stream = reader.read().decode("utf-8")
            for line in text_stream.strip().split("\n"):
                if line:
                    data.append(json.loads(line))
    return data


def normalize_for_duckdb(record: Dict[str, Any]) -> Dict[str, Any]:
    """Convert nested structures (lists, dicts) to JSON strings for DuckDB compatibility.
    
    DuckDB can have issues inferring types from nested JSON structures, especially when
    there are mixed types or when it tries to cast values incorrectly. By converting
    complex nested structures to JSON strings, we avoid type inference issues.
    
    Also ensures boolean fields are properly typed.
    """
    normalized = {}
    for key, value in record.items():
        if isinstance(value, (dict, list)):
            # Convert nested structures to JSON strings
            normalized[key] = json.dumps(value, ensure_ascii=False)
        elif key == 'api' and not isinstance(value, bool) and value is not None:
            # Ensure 'api' field is always boolean or None
            # If it's a string like 'dataset', convert to False
            if isinstance(value, str):
                logger.warning(f"Record {record.get('id', 'unknown')} has non-boolean 'api' value: {value}, converting to False")
                normalized[key] = False
            else:
                normalized[key] = bool(value)
        else:
            # Keep primitive types as-is (str, int, float, bool, None)
            normalized[key] = value
    return normalized


def compress_jsonl(input_path, output_path, compression_level=19):
    """Compress JSONL file with zstandard compression"""
    input_file = os.path.join(DATASETS_DIR, input_path)
    output_file = os.path.join(DATASETS_DIR, output_path)
    
    # Get file size for progress bar
    input_size = os.path.getsize(input_file)
    
    cctx = zstd.ZstdCompressor(level=compression_level)
    with open(input_file, "rb") as ifh:
        with open(output_file, "wb") as ofh:
            with tqdm.tqdm(
                total=input_size,
                unit="B",
                unit_scale=True,
                desc=f"Compressing {os.path.basename(input_path)}",
            ) as pbar:
                compressor = cctx.stream_writer(ofh)
                while True:
                    chunk = ifh.read(8192)
                    if not chunk:
                        break
                    compressor.write(chunk)
                    pbar.update(len(chunk))
                compressor.flush(zstd.FLUSH_FRAME)
    logger.info(
        "Compressed %s to %s",
        os.path.basename(input_path),
        os.path.basename(output_path),
    )


def verify_both_formats_exist(jsonl_filename):
    """Verify that both JSONL and JSONL.zst files exist"""
    jsonl_path = os.path.join(DATASETS_DIR, jsonl_filename)
    jsonl_zst_path = os.path.join(DATASETS_DIR, jsonl_filename + ".zst")
    
    jsonl_exists = os.path.exists(jsonl_path)
    jsonl_zst_exists = os.path.exists(jsonl_zst_path)
    
    if jsonl_exists and jsonl_zst_exists:
        logger.info(
            "Verified both formats exist: %s and %s",
            os.path.basename(jsonl_filename),
            os.path.basename(jsonl_filename + ".zst"),
        )
        return True
    else:
        missing = []
        if not jsonl_exists:
            missing.append(os.path.basename(jsonl_filename))
        if not jsonl_zst_exists:
            missing.append(os.path.basename(jsonl_filename + ".zst"))
        logger.warning(
            "Missing files for %s: %s",
            os.path.basename(jsonl_filename),
            ", ".join(missing),
        )
        return False


def build_dataset(datapath, dataset_filename):
    # Collect all YAML files first
    all_files = []
    for root, dirs, files in os.walk(datapath):
        all_files.extend(
            [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        )
    
    out = open(os.path.join(DATASETS_DIR, dataset_filename), "w", encoding="utf8")
    validation_errors = []
    
    with tqdm.tqdm(
        total=len(all_files),
        desc=f"Building {os.path.basename(dataset_filename)}",
        unit="files",
    ) as pbar:
        for filename in all_files:
            f = open(filename, "r", encoding="utf8")
            data = yaml.load(f, Loader=Loader)
            f.close()
            
            # Skip records without id (invalid catalog entries)
            if not data or not data.get("id"):
                pbar.update(1)
                continue

            # Validate software profile if building software dataset
            if dataset_filename == "software.jsonl" and data:
                issues = validate_software_profile(data)
                if issues:
                    rel_path = os.path.relpath(filename, datapath)
                    for issue in issues:
                        issue["file_path"] = rel_path
                        issue["record_id"] = data.get("id", "unknown")
                        validation_errors.append(issue)
            
            out.write(json.dumps(data, ensure_ascii=False) + "\n")
            pbar.update(1)
    out.close()
    
    if validation_errors:
        logger.warning(f"Found {len(validation_errors)} validation issues in software profiles:")
        for error in validation_errors[:10]:  # Show first 10
            logger.warning(f"  {error.get('file_path')} ({error.get('record_id')}): {error.get('issue_type')} - {error.get('field')}")
        if len(validation_errors) > 10:
            logger.warning(f"  ... and {len(validation_errors) - 10} more issues")
    
    logger.info("Processed %d files", len(all_files))


def merge_datasets(list_datasets, result_file):
    # Count total lines for progress bar
    total_lines = 0
    for filename in list_datasets:
        filepath = os.path.join(DATASETS_DIR, filename)
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf8") as f:
                total_lines += sum(1 for _ in f)
    
    out = open(os.path.join(DATASETS_DIR, result_file), "w", encoding="utf8")
    processed_lines = 0
    with tqdm.tqdm(
        total=total_lines,
        desc=f"Merging to {os.path.basename(result_file)}",
        unit="lines",
    ) as pbar:
        for filename in list_datasets:
            logger.info("adding %s", os.path.basename(filename).split(".", 1)[0])
            filepath = os.path.join(DATASETS_DIR, filename)
            if os.path.exists(filepath):
                f = open(filepath, "r", encoding="utf8")
                for line in f:
                    line = line.rstrip()
                    if not line:
                        pbar.update(1)
                        continue
                    try:
                        record = json.loads(line)
                        if record and record.get("id"):
                            out.write(line + "\n")
                            processed_lines += 1
                    except json.JSONDecodeError:
                        out.write(line + "\n")
                        processed_lines += 1
                    pbar.update(1)
                f.close()
    out.close()


@app.command()
def build():
    """Build datasets as JSONL from entities as YAML"""
    logger.info("Started building software dataset")
    build_dataset(SOFTWARE_DIR, "software.jsonl")
    logger.info(
        "Finished building software dataset. File saved as %s",
        os.path.join(DATASETS_DIR, "software.jsonl"),
    )
    
    logger.info("Compressing software dataset")
    compress_jsonl("software.jsonl", "software.jsonl.zst")
    verify_both_formats_exist("software.jsonl")
    
    logger.info("Started building catalogs dataset")
    build_dataset(ROOT_DIR, "catalogs.jsonl")
    logger.info(
        "Finished building catalogs dataset. File saved as %s",
        os.path.join(DATASETS_DIR, "catalogs.jsonl"),
    )
    
    logger.info("Compressing catalogs dataset")
    compress_jsonl("catalogs.jsonl", "catalogs.jsonl.zst")
    verify_both_formats_exist("catalogs.jsonl")
    
    logger.info("Started building scheduled dataset")
    build_dataset(SCHEDULED_DIR, "scheduled.jsonl")
    logger.info(
        "Finished building scheduled dataset. File saved as %s",
        os.path.join(DATASETS_DIR, "scheduled.jsonl"),
    )
    
    logger.info("Compressing scheduled dataset")
    compress_jsonl("scheduled.jsonl", "scheduled.jsonl.zst")
    verify_both_formats_exist("scheduled.jsonl")
    
    merge_datasets(["catalogs.jsonl", "scheduled.jsonl"], "full.jsonl")
    logger.info(
        "Merged datasets %s as %s",
        ",".join(["catalogs.jsonl", "scheduled.jsonl"]),
        "full.jsonl",
    )
    
    logger.info("Compressing full dataset")
    compress_jsonl("full.jsonl", "full.jsonl.zst")
    verify_both_formats_exist("full.jsonl")
    
    # Build DuckDB database
    db_path = os.path.join(DATASETS_DIR, "datasets.duckdb")
    logger.info("Building DuckDB database at %s", db_path)
    
    # Remove existing database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = duckdb.connect(db_path)
    
    # Create catalogs table from full.jsonl.zst
    logger.info("Creating catalogs table from full.jsonl.zst")
    full_jsonl_zst_path = os.path.join(DATASETS_DIR, "full.jsonl.zst")
    
    # Decompress and load into DuckDB
    # DuckDB can read JSONL directly, but we need to handle decompression
    # We'll decompress to a temporary file or use DuckDB's JSON reading
    with tqdm.tqdm(desc="Loading catalogs into DuckDB", unit="records") as pbar:
        # Read compressed file and insert into DuckDB
        dctx = zstd.ZstdDecompressor()
        temp_data = []
        with open(full_jsonl_zst_path, "rb") as f:
            with dctx.stream_reader(f) as reader:
                text_stream = reader.read().decode("utf-8")
                for line in text_stream.strip().split("\n"):
                    if line:
                        record = json.loads(line)
                        # Normalize nested structures to JSON strings to avoid type inference issues
                        normalized_record = normalize_for_duckdb(record)
                        temp_data.append(normalized_record)
                        pbar.update(1)
        
        # Create table from JSON data
        if temp_data:
            import pandas as pd
            df = pd.DataFrame(temp_data)
            conn.execute("CREATE TABLE catalogs AS SELECT * FROM df")
            logger.info("Created catalogs table with %d records", len(temp_data))
    
    # Create software table from software.jsonl.zst
    logger.info("Creating software table from software.jsonl.zst")
    software_jsonl_zst_path = os.path.join(DATASETS_DIR, "software.jsonl.zst")
    
    with tqdm.tqdm(desc="Loading software into DuckDB", unit="records") as pbar:
        dctx = zstd.ZstdDecompressor()
        temp_data = []
        with open(software_jsonl_zst_path, "rb") as f:
            with dctx.stream_reader(f) as reader:
                text_stream = reader.read().decode("utf-8")
                for line in text_stream.strip().split("\n"):
                    if line:
                        record = json.loads(line)
                        # Normalize nested structures to JSON strings to avoid type inference issues
                        normalized_record = normalize_for_duckdb(record)
                        temp_data.append(normalized_record)
                        pbar.update(1)
        
        if temp_data:
            import pandas as pd
            df = pd.DataFrame(temp_data)
            conn.execute("CREATE TABLE software AS SELECT * FROM df")
            logger.info("Created software table with %d records", len(temp_data))
    
    conn.close()
    logger.info("DuckDB database created successfully at %s", db_path)
    
    # Keep existing parquet file generation
    logger.info(
        "Building final parquet file %s", os.path.join(DATASETS_DIR, "full.parquet")
    )
    os.system(
        "duckdb -c \"copy '%s' to '%s'  (FORMAT 'parquet', COMPRESSION 'zstd');\""
        % (
            os.path.join(DATASETS_DIR, "full.jsonl"),
            os.path.join(DATASETS_DIR, "full.parquet"),
        )
    )


@app.command()
def report():
    """Report incomplete data per set"""
    data = load_jsonl(os.path.join(DATASETS_DIR, "full.jsonl"))
    typer.echo("")
    for d in data:
        irep = []
        if "name" not in d.keys():
            irep.append("no name")
        if "countries" not in d.keys():
            irep.append("no countries")
        if "tags" not in d.keys():
            irep.append("no tags")
        if "software" not in d.keys():
            irep.append("no software")
        if "langs" not in d.keys():
            irep.append("no langs")
        if "owner_name" not in d.keys():
            irep.append("no owner name")
        if len(irep) > 0:
            logger.warning("%s / %s", d["id"], d["name"] if "name" in d.keys() else "")
            for r in irep:
                logger.warning("- %s", r)


@app.command()
def export(output="export.csv"):
    """Export to CSV"""
    data = load_jsonl(os.path.join(DATASETS_DIR, "catalogs.jsonl"))
    typer.echo("")
    items = []
    for record in data:
        item = {}
        for k in [
            "api_status",
            "catalog_type",
            "id",
            "link",
            "name",
            "status",
            "api",
            "catalog_export",
        ]:
            item[k] = record[k] if k in record.keys() else ""

        for k in ["type", "link", "name"]:
            item["owner_" + k] = (
                record["owner"][k] if k in record["owner"].keys() else ""
            )
        item["owner_country_id"] = record["owner"]["location"]["country"]["id"]
        item["software_id"] = record["software"]["id"]

        for k in ["access_mode", "content_types", "langs", "tags"]:
            item[k] = ",".join(record[k]) if k in record.keys() else ""

        countries_ids = []
        for location in record["coverage"]:
            cid = str(location["location"]["country"]["id"])
            if cid not in countries_ids:
                countries_ids.append(cid)
        item["coverage_countries"] = ",".join(countries_ids)
        items.append(item)
    outfile = open(output, "w", encoding="utf8")
    writer = csv.DictWriter(
        outfile,
        fieldnames=[
            "id",
            "link",
            "name",
            "owner_name",
            "catalog_type",
            "owner_type",
            "software_id",
            "langs",
            "content_types",
            "access_mode",
            "owner_country_id",
            "coverage_countries",
            "tags",
            "status",
            "api",
            "owner_link",
            "catalog_export",
            "api_status",
        ],
        delimiter="\t",
    )
    writer.writeheader()
    writer.writerows(items)
    outfile.close()
    typer.echo("Wrote %s" % (output))


@app.command()
def stats(output="country_software.csv"):
    """Generates statistics tables"""
    data = load_jsonl(os.path.join(DATASETS_DIR, "catalogs.jsonl"))
    typer.echo("")
    items = []
    countries = []
    software = []
    for record in data:
        if "coverage" in record.keys():
            for loc_rec in record["coverage"]:
                country = loc_rec["location"]["country"]
                if country["name"] not in countries:
                    countries.append(country["name"])
        if "software" in record.keys():
            if record["software"]["name"] not in software:
                software.append(record["software"]["name"])
    countries.sort()
    software.sort()
    matrix = {}
    for country in countries:
        matrix[country] = {"country": country}
        for soft in software:
            matrix[country][soft] = 0
    for record in data:
        if "coverage" in record.keys():
            for loc_rec in record["coverage"]:
                country = loc_rec["location"]["country"]
                if "software" in record.keys():
                    matrix[country["name"]][record["software"]["name"]] += 1
    results = matrix.values()
    outfile = open(output, "w", encoding="utf8")
    fieldnames = [
        "country",
    ]
    fieldnames.extend(software)
    logger.debug("Fieldnames: %s", fieldnames)
    writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter="\t")
    writer.writeheader()
    writer.writerows(results)
    outfile.close()
    typer.echo("Wrote %s" % (output))


def assign_by_dir(prefix="cdi", dirpath=ROOT_DIR):
    max_num = 0
    n = 0
    for root, dirs, files in tqdm.tqdm(os.walk(dirpath)):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            n += 1
            #            if n % 1000 == 0: print('Processed %d' % (n))
            filepath = filename
            f = open(filepath, "r", encoding="utf8")
            record = yaml.load(f, Loader=Loader)
            if "uid" in record.keys():
                if record["uid"].find(prefix) == -1:
                    continue
                num = int(record["uid"].split(prefix, 1)[-1])
                if num > max_num:
                    max_num = num
            f.close()
    logger.info("Processed %d", n)
    for root, dirs, files in tqdm.tqdm(os.walk(dirpath)):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            filepath = filename
            f = open(filepath, "r", encoding="utf8")
            record = yaml.load(f, Loader=Loader)
            f.close()
            if "uid" not in record.keys():
                max_num += 1
                record["uid"] = f"{prefix}{max_num:08}"
                logger.info(
                    "Wrote %s uid for %s",
                    record["uid"],
                    os.path.basename(filename).split(".", 1)[0],
                )
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()


@app.command()
def assign(dryrun=False, mode="entries"):
    """Assign unique identifier to each data catalog entry"""
    if mode == "entries":
        assign_by_dir("cdi", ROOT_DIR)
    else:
        assign_by_dir("temp", SCHEDULED_DIR)


@app.command()
def validate():
    """Validates the built JSONL export (full.jsonl) against the Cerberus catalog schema."""
    from cerberus import Validator

    schema_file = os.path.join(_REPO_ROOT, "data", "schemes", "catalog.json")
    f = open(schema_file, "r", encoding="utf8")
    schema = json.load(f)
    f.close()
    records = load_jsonl(os.path.join(DATASETS_DIR, "full.jsonl"))
    typer.echo("Loaded %d data catalog records" % (len(records)))

    v = Validator(schema)
    errors = []
    total = 0
    valid = 0
    for d in records:
        if d:
            total += 1
            try:
                if v.validate(d, schema):
                    valid += 1
                else:
                    errors.append((d.get("id", "unknown"), dict(v.errors)))
            except Exception as e:
                errors.append((d.get("id", "unknown"), {"_exception": str(e)}))

    typer.echo("Validation complete:")
    typer.echo("  Total: %d" % total)
    typer.echo("  Valid: %d" % valid)
    typer.echo("  Errors: %d" % len(errors))

    if errors:
        typer.echo("\nErrors found:")
        for record_id, err in errors:
            logger.error("%s: %s", record_id, err)
        raise typer.Exit(1)
    typer.echo("\nAll records valid!")


@app.command()
def validate_yaml():
    """Validates all YAML files in entities directory against Cerberus schema"""
    from cerberus import Validator

    schema_file = os.path.join(_REPO_ROOT, "data", "schemes", "catalog.json")
    f = open(schema_file, "r", encoding="utf8")
    schema = json.load(f)
    f.close()

    v = Validator(schema)
    errors = []
    total = 0
    valid = 0

    typer.echo("Validating YAML files in entities directory...")

    for root, dirs, files in tqdm.tqdm(os.walk(ROOT_DIR)):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            total += 1
            try:
                f = open(filename, "r", encoding="utf8")
                record = yaml.load(f, Loader=Loader)
                f.close()

                if record is None:
                    errors.append((filename, "File is empty or invalid YAML"))
                    continue

                if not v.validate(record, schema):
                    record_id = record.get("id", "unknown")
                    errors.append((filename, f"{record_id}: {str(v.errors)}"))
                else:
                    valid += 1
            except yaml.YAMLError as e:
                errors.append((filename, f"YAML parsing error: {str(e)}"))
            except Exception as e:
                record_id = "unknown"
                try:
                    f = open(filename, "r", encoding="utf8")
                    record = yaml.load(f, Loader=Loader)
                    f.close()
                    if record:
                        record_id = record.get("id", "unknown")
                except:
                    pass
                errors.append((filename, f"{record_id}: {str(e)}"))

    typer.echo(f"\nValidation complete:")
    typer.echo(f"  Total files: {total}")
    typer.echo(f"  Valid: {valid}")
    typer.echo(f"  Errors: {len(errors)}")

    if errors:
        typer.echo("\nErrors found:")
        for filename, error in errors:
            rel_path = os.path.relpath(filename, ROOT_DIR)
            logger.error("%s: %s", rel_path, error)
        return 1
    else:
        typer.echo("\nAll files are valid!")
        return 0


@app.command()
def validate_typing():
    """Validates YAML entities files against pydantic model"""
    from cerberus import Validator

    records = load_jsonl(os.path.join(DATASETS_DIR, "catalogs.jsonl"))
    typer.echo("Loaded %d data catalog records" % (len(records)))
    from cdiapi.data.datacatalog import DataCatalog

    for d in records:
        logger.info("Validating %s", d["id"])
        try:
            entry = DataCatalog.parse_obj(d)
        except Exception as e:
            logger.error("%s error %s", d["id"], str(e))


@app.command()
def add_legacy():
    """Adds all legacy catalogs"""

    scheduled_data = load_jsonl(os.path.join(DATASETS_DIR, "full.jsonl"))
    scheduled_list = []
    for row in scheduled_data:
        scheduled_list.append(row["id"])

    files = os.listdir(UNPROCESSED_DIR)
    for filename in files:
        if filename[-4:] != ".txt":
            continue
        software = filename[0:-4]
        f = open(os.path.join(UNPROCESSED_DIR, filename), "r", encoding="utf8")
        for l in f:
            url = l.rstrip()
            _add_single_entry(url, software, preloaded=scheduled_list)
        f.close()


def _add_single_entry(
    url,
    software,
    catalog_type="Open data portal",
    name=None,
    description=None,
    lang=None,
    country=None,
    owner_name=None,
    owner_link=None,
    owner_type=None,
    scheduled=True,
    force=False,
    preloaded=None,
):
    from apidetect import detect_single

    domain = urlparse(url).netloc.lower()
    record_id = (
        domain.split(":", 1)[0].replace("_", "").replace("-", "").replace(".", "")
    )

    if record_id in preloaded:
        logger.info("URL %s already scheduled to be added", record_id)
        return

    software_data = load_jsonl(os.path.join(DATASETS_DIR, "software.jsonl"))
    software_map = {}
    for row in software_data:
        software_map[row["id"]] = row["name"]

    record = copy.deepcopy(ENTRY_TEMPLATE)
    record["id"] = record_id

    postfix = None
    has_location = False
    if country is not None:
        if country in COUNTRIES.keys():
            location = {
                "location": {"country": {"id": country, "name": COUNTRIES[country]}}
            }
            has_location = True

    if not has_location:
        postfix = domain.rsplit(".", 1)[-1].split(":", 1)[0]
        if postfix in DOMAIN_LOCATIONS.keys():
            location = DOMAIN_LOCATIONS[postfix]
        else:
            location = DEFAULT_LOCATION

    record["langs"] = []
    if lang:
        record["langs"].append(lang)
    if has_location and postfix in COUNTRIES_LANGS.keys():
        record["langs"].append(COUNTRIES_LANGS[postfix])

    record["link"] = url
    record["name"] = domain if name is None else name
    if description is not None:
        record["description"] = description

    record["coverage"].append(copy.deepcopy(location))
    record["owner"].update(copy.deepcopy(location))
    if owner_name is not None:
        record["owner"]["name"] = owner_name
    if owner_link is not None:
        record["owner"]["link"] = owner_link
    if owner_type is not None:
        record["owner"]["type"] = owner_type

    if software in MAP_SOFTWARE_OWNER_CATALOG_TYPE.keys():
        record["catalog_type"] = MAP_SOFTWARE_OWNER_CATALOG_TYPE[software]
    else:
        record["catalog_type"] = catalog_type
    if record["catalog_type"] == "Geoportal":
        record["content_types"].append("map_layer")
    if software in CUSTOM_SOFTWARE_KEYS:
        record["software"] = {"id": "custom", "name": "Custom software"}
    elif software in software_map.keys():
        record["software"] = {"id": software, "name": software_map[software]}
    else:
        record["software"] = {"id": software, "name": software.title()}
    root_dir = SCHEDULED_DIR if scheduled else ROOT_DIR
    country_dir = os.path.join(root_dir, location["location"]["country"]["id"])
    if not os.path.exists(country_dir):
        os.mkdir(country_dir)
    subdir_name = (
        MAP_CATALOG_TYPE_SUBDIR[record["catalog_type"]]
        if record["catalog_type"] in MAP_CATALOG_TYPE_SUBDIR.keys()
        else "opendata"
    )
    subdir_dir = os.path.join(country_dir, subdir_name)
    if not os.path.exists(subdir_dir):
        os.mkdir(subdir_dir)
    filename = os.path.join(subdir_dir, record_id + ".yaml")
    if os.path.exists(filename) and force:
        logger.info("Already processed and force not set")
    else:
        f = open(filename, "w", encoding="utf8")
        #        logger.debug(record)
        f.write(yaml.safe_dump(record, allow_unicode=True))
        f.close()
        logger.info("%s saved", record_id)
        detect_single(
            record_id,
            dryrun=False,
            mode="scheduled" if scheduled else "entries",
        )


@app.command()
def add_single(
    url,
    software="custom",
    catalog_type="Open data portal",
    name=None,
    description=None,
    lang=None,
    country=None,
    owner_name=None,
    owner_link=None,
    owner_type=None,
    force=False,
    scheduled=True,
):
    """Adds data catalog to the scheduled list"""

    full_data = load_jsonl(os.path.join(DATASETS_DIR, "full.jsonl"))
    full_list = []
    for row in full_data:
        full_list.append(row["id"])
    _add_single_entry(
        url,
        software,
        name=name,
        description=description,
        lang=lang,
        country=country,
        owner_name=owner_name,
        owner_link=owner_link,
        owner_type=owner_type,
        scheduled=scheduled,
        force=force,
        preloaded=full_list,
    )


@app.command()
def add_list(
    filename,
    software="custom",
    catalog_type="Open data portal",
    name=None,
    description=None,
    lang=None,
    country=None,
    owner_name=None,
    owner_link=None,
    owner_type=None,
):
    """Adds data catalog one by one from list to the scheduled list"""
    if not os.path.exists(filename):
        logger.error("File %s not exists", filename)
        return
    full_data = load_jsonl(os.path.join(DATASETS_DIR, "full.jsonl"))
    full_list = []
    for row in full_data:
        full_list.append(row["id"])
    f = open(filename, "r", encoding="utf8")
    for line in f:
        line = line.strip()
        if not line:
            continue
        _add_single_entry(
            line,
            software,
            catalog_type=catalog_type,
            name=name,
            description=description,
            lang=lang,
            country=country,
            owner_name=owner_name,
            owner_link=owner_link,
            owner_type=owner_type,
            preloaded=full_list,
        )
    f.close()


@app.command()
def add_opendatasoft_catalog(filename):
    """Adds OpenDataSoft prepared data catalogs list"""
    full_data = load_jsonl(os.path.join(DATASETS_DIR, "full.jsonl"))
    full_list = []
    for row in full_data:
        full_list.append(row["id"])
    ods_data = load_jsonl(filename)
    for item in ods_data:
        lang = item["lang"].rsplit("/", 1)[-1].upper()
        _add_single_entry(
            item["website"],
            software="opendatasoft",
            name=item["title"],
            description=item["description"],
            lang=lang,
            preloaded=full_list,
        )


@app.command()
def add_socrata_catalog(filename):
    """Adds Socrata prepared data catalogs list"""
    full_data = load_jsonl(os.path.join(DATASETS_DIR, "full.jsonl"))
    full_list = []
    for row in full_data:
        full_list.append(row["id"])
    ods_data = load_jsonl(filename)
    for item in ods_data:
        lang = item["locale"].rsplit("/", 1)[-1].upper()
        _add_single_entry(
            item["website"],
            software="socrata",
            name=item["title"],
            lang=lang,
            preloaded=full_list,
        )


@app.command()
def add_arcgishub_catalog(filename, force=False):
    """Adds ArcGIS Hub prepared data catalogs list"""
    full_data = load_jsonl(os.path.join(DATASETS_DIR, "full.jsonl"))
    full_list = []
    for row in full_data:
        full_list.append(row["id"])
    ods_data = load_jsonl(filename)
    for item in ods_data:
        if item["culture"] is not None:
            lang = (
                item["culture"].rsplit("-", 1)[0].upper()
                if item["culture"] != "zh-TW"
                else item["culture"]
            )
        else:
            lang = "EN"
        country = item["region"]
        if country == "WO":
            country = "US"
        _add_single_entry(
            item["website"],
            software="arcgishub",
            name=item["title"],
            description=item["description"],
            lang=lang,
            owner_name=item["owner_name"],
            country=country,
            force=force,
            scheduled=False,
            preloaded=full_list,
        )


SOFTWARE_MD_TEMPLATE = """---
sidebar_position: 1
position: 1
---
# %s


Type: %s
Website: %s

## Schema types

## API Endpoints

## Notes
"""

# Resolve docs path relative to script location (goes up to repo root, then to sibling cdi-docs)
SOFTWARE_DOCS_PATH = os.path.join(os.path.dirname(_REPO_ROOT), "cdi-docs", "docs", "kb", "software")

SOFTWARE_PATH_MAP = {
    "Open data portal": "opendata",
    "Geoportal": "geo",
    "Indicators catalog": "indicators",
    "Metadata catalog": "metadata",
    "Microdata catalog": "microdata",
    "Scientific data repository": "scientific",
}


@app.command()
def build_docs(rewrite=True):
    """Generates docs stubs"""
    software_data = load_jsonl(os.path.join(DATASETS_DIR, "software.jsonl"))
    for row in software_data:
        category = SOFTWARE_PATH_MAP[row["category"]]
        filename = os.path.join(SOFTWARE_DOCS_PATH, category, row["id"] + ".md")
        if os.path.exists(filename) and rewrite is False:
            logger.info("Already exists %s", row["id"])
        else:
            text = SOFTWARE_MD_TEMPLATE % (row["name"], row["category"], row["website"])
            f = open(filename, "w", encoding="utf8")
            f.write(text)
            f.close()
            logger.info("Wrote %s", row["id"])


@app.command()
def get_countries():
    """Generate countries code list"""
    ids = []
    full_data = load_jsonl(os.path.join(DATASETS_DIR, "full.jsonl"))
    text = "COUNTRIES = { "
    for row in full_data:
        for loc in row["coverage"]:
            id = loc["location"]["country"]["id"]
            name = loc["location"]["country"]["name"]
            if id not in ids:
                ids.append(id)
                text += '"%s" : "%s",\n' % (id, name)
            else:
                continue
    text += "}"
    logger.info(text)


METRICS = {
    "has_owner_name": "Has owner organization name",
    "has_owner_type": "Has owner organization type",
    "has_owner_link": "Has owner organization link",
    "has_catalog_type": "Has catalog type",
    "has_country": "Owner country known",
    "has_coverage": "Coverage known",
    "has_macroregion": "Macroregion information known",
    "has_subregion": "Subregion information known",
    "has_description": "Has description",
    "has_langs": "Has languages",
    "has_tags": "Has tags",
    "has_topics": "Has topics",
    "has_endpoints": "Has endpoints",
    "valid_title": "Title is not empty or temporary",
    "perm_records": "Permanent records",
}


@app.command()
def quality_control(mode="full"):
    """Quality control metrics"""
    from rich.console import Console
    from rich.table import Table

    data = load_jsonl(os.path.join(DATASETS_DIR, f"{mode}.jsonl"))
    metrics = {}
    for key in METRICS.keys():
        metrics[key] = [key, METRICS[key], 0, 0, 0]
    total = 0
    for d in data:
        total += 1
        if "coverage" in d.keys() and len(d["coverage"]) > 0:
            metrics["has_coverage"][3] += 1
            if "location" in d["coverage"][0].keys():
                location = d["coverage"][0]["location"]
                if "macroregion" in location.keys():
                    metrics["has_macroregion"][3] += 1
        if "langs" in d.keys() and len(d["langs"]) > 0:
            metrics["has_langs"][3] += 1
        if "tags" in d.keys() and len(d["tags"]) > 0:
            metrics["has_tags"][3] += 1
        if "topics" in d.keys() and len(d["topics"]) > 0:
            metrics["has_topics"][3] += 1
        if "endpoints" in d.keys() and len(d["endpoints"]) > 0:
            metrics["has_endpoints"][3] += 1
        if "status" in d.keys() and d["status"] == "active":
            metrics["perm_records"][3] += 1
        if "catalog_type" in d.keys() and d["catalog_type"] not in [None, "Unknown"]:
            metrics["has_catalog_type"][3] += 1
        if (
            "description" in d.keys()
            and d["description"]
            != "This is a temporary record with some data collected but it should be updated befor adding to the index"
        ):
            metrics["has_description"][3] += 1
        if "name" in d.keys():
            if not d["name"].lower() == urlparse(d["link"]).netloc.lower():
                metrics["valid_title"][3] += 1
        if "owner" in d.keys():
            if "type" in d["owner"].keys() and d["owner"]["type"] != "Unknown":
                metrics["has_owner_type"][3] += 1
            if (
                "link" in d["owner"].keys()
                and d["owner"]["link"] is not None
                and len(d["owner"]["link"]) > 0
            ):
                metrics["has_owner_link"][3] += 1
            if (
                "name" in d["owner"].keys()
                and d["owner"]["name"] is not None
                and len(d["owner"]["name"]) > 0
                and d["owner"]["name"] != "Unknown"
            ):
                metrics["has_owner_name"][3] += 1
            if (
                "location" in d["owner"].keys()
                and d["owner"]["location"] is not None
                and "country" in d["owner"]["location"].keys()
                and d["owner"]["location"]["country"]["id"] != "Unknown"
            ):
                metrics["has_country"][3] += 1
            if d["owner"]["type"] in [
                "Regional government",
                "Local government",
                "Unknown",
            ]:
                metrics["has_subregion"][2] += 1
                if (
                    "location" in d["owner"].keys()
                    and d["owner"]["location"] is not None
                    and "subregion" in d["owner"]["location"].keys()
                ):
                    metrics["has_subregion"][3] += 1

        for key in [
            "has_tags",
            "has_langs",
            "has_topics",
            "has_endpoints",
            "has_description",
            "perm_records",
            "has_owner_link",
            "has_owner_type",
            "has_owner_name",
            "valid_title",
            "has_country",
            "has_catalog_type",
            "has_coverage",
            "has_macroregion",
        ]:
            metrics[key][2] += 1
    #    for metric in metrics.values():
    #        print('%s, total %d, found %d, share %0.2f' % (metric[1], metric[2], metric[3], metric[3]*100.0 / metric[2] if metric[2] > 0 else 0))
    table = Table(title="Common Data Index registry. Metadata quality metrics")
    table.add_column("Metric name", justify="right", style="cyan", no_wrap=True)
    table.add_column("Total", style="magenta")
    table.add_column("Count", style="magenta")
    table.add_column("Share", justify="right", style="green", no_wrap=True)
    for metric in metrics.values():
        item = []
        for o in metric[1:-1]:
            item.append(str(o))
        item.append("%0.2f" % (metric[3] * 100.0 / metric[2] if metric[2] > 0 else 0))
        table.add_row(*item)
    table.add_section()
    table.add_row("Total", str(total))
    console = Console()
    console.print(table)


# Data Quality Analysis Helper Functions

def check_missing_topics(record):
    """Check if topics field is missing or empty"""
    if "topics" not in record or not record["topics"] or len(record["topics"]) == 0:
        return {
            "issue_type": "MISSING_TOPICS",
            "field": "topics",
            "current_value": record.get("topics", []),
            "suggested_action": "Add relevant topics based on catalog_type and description",
        }
    return None


def check_missing_tags(record):
    """Check if tags field is missing or empty"""
    if "tags" not in record or not record["tags"] or len(record["tags"]) == 0:
        return {
            "issue_type": "MISSING_TAGS",
            "field": "tags",
            "current_value": record.get("tags", []),
            "suggested_action": "Extract tags from description and catalog_type",
        }
    return None


def check_missing_description(record):
    """Check for placeholder or empty descriptions"""
    description = record.get("description", "")
    placeholder_text = "This is a temporary record with some data collected but it should be updated befor adding to the index"
    
    if not description or description == placeholder_text or description.strip() == "":
        return {
            "issue_type": "MISSING_DESCRIPTION",
            "field": "description",
            "current_value": description if description else None,
            "suggested_action": "Add meaningful description based on portal content and purpose",
        }
    return None


def check_missing_langs(record):
    """Check if langs field is missing or empty"""
    if "langs" not in record or not record["langs"] or len(record["langs"]) == 0:
        return {
            "issue_type": "MISSING_LANGS",
            "field": "langs",
            "current_value": record.get("langs", []),
            "suggested_action": "Add language codes based on portal content and location",
        }
    return None


def check_missing_endpoints(record):
    """Check if API records have missing endpoints"""
    if record.get("api") is True and (
        "endpoints" not in record
        or not record["endpoints"]
        or len(record["endpoints"]) == 0
    ):
        return {
            "issue_type": "MISSING_ENDPOINTS",
            "field": "endpoints",
            "current_value": record.get("endpoints", []),
            "suggested_action": "Add API endpoints based on software type and portal structure",
        }
    return None


def check_software_expected_endpoints(record):
    """Check endpoint presence for software that is expected to provide APIs."""
    software = record.get("software", {}) or {}
    software_id = software.get("id")
    if not software_id:
        return None

    # Skip inactive catalogs because their endpoints may be intentionally unavailable.
    if record.get("status") == "inactive":
        return None

    software_map = get_cached_software_map()
    software_meta = software_map.get(software_id) if software_map else None
    if not software_meta:
        return None

    has_api = software_meta.get("has_api")
    requires_endpoints = isinstance(has_api, str) and has_api.strip().lower() == "yes"
    if not requires_endpoints:
        return None

    endpoints = record.get("endpoints", [])
    if not isinstance(endpoints, list) or len(endpoints) == 0:
        return {
            "issue_type": "SOFTWARE_EXPECTED_ENDPOINTS_MISSING",
            "field": "endpoints",
            "current_value": {
                "software_id": software_id,
                "has_api": has_api,
                "endpoints_count": len(endpoints) if isinstance(endpoints, list) else 0,
            },
            "suggested_action": (
                f"Add at least one endpoint because software.id='{software_id}' "
                "is marked as API-capable in software definitions"
            ),
        }
    return None


def check_owner_info(record):
    """Check completeness of owner information"""
    issues = []
    owner = record.get("owner", {})
    
    if not owner.get("name") or owner.get("name") == "Unknown" or owner.get("name") == "":
        issues.append({
            "issue_type": "MISSING_OWNER_NAME",
            "field": "owner.name",
            "current_value": owner.get("name"),
            "suggested_action": "Add owner organization name",
        })
    else:
        owner_name = owner.get("name")
        if isinstance(owner_name, str):
            owner_name_norm = owner_name.strip().lower()
            placeholder_owner_names = {
                "unknown",
                "not specified",
                "unspecified",
                "not available",
                "n/a",
                "na",
                "none",
                "null",
                "tbd",
                "to be determined",
                "placeholder",
                "-",
            }
            if owner_name_norm in placeholder_owner_names:
                issues.append({
                    "issue_type": "PLACEHOLDER_OWNER_NAME",
                    "field": "owner.name",
                    "current_value": owner_name,
                    "suggested_action": "Replace placeholder owner name with a real organization name",
                })
    
    if not owner.get("type") or owner.get("type") == "Unknown":
        issues.append({
            "issue_type": "MISSING_OWNER_TYPE",
            "field": "owner.type",
            "current_value": owner.get("type"),
            "suggested_action": "Add owner organization type (e.g., Academy, Government, etc.)",
        })
    
    if not owner.get("link") or owner.get("link") == "":
        issues.append({
            "issue_type": "MISSING_OWNER_LINK",
            "field": "owner.link",
            "current_value": owner.get("link"),
            "suggested_action": "Add owner organization website URL",
        })
    
    owner_location = owner.get("location", {})
    if not owner_location or not owner_location.get("country") or owner_location.get("country", {}).get("id") == "Unknown":
        issues.append({
            "issue_type": "MISSING_OWNER_LOCATION",
            "field": "owner.location",
            "current_value": owner_location,
            "suggested_action": "Add owner organization location with country information",
        })
    
    return issues if issues else None


def check_coverage(record):
    """Check if coverage field is missing or empty"""
    if "coverage" not in record or not record["coverage"] or len(record["coverage"]) == 0:
        return {
            "issue_type": "MISSING_COVERAGE",
            "field": "coverage",
            "current_value": record.get("coverage", []),
            "suggested_action": "Add coverage information with location details",
        }
    return None


def check_placeholder_values(record):
    """Check for placeholder values like 'Unknown', 'None', etc."""
    issues = []
    
    # Check catalog_type
    catalog_type = record.get("catalog_type")
    if catalog_type in [None, "Unknown", ""]:
        issues.append({
            "issue_type": "PLACEHOLDER_CATALOG_TYPE",
            "field": "catalog_type",
            "current_value": catalog_type,
            "suggested_action": "Set appropriate catalog_type based on portal content",
        })
    
    # Check status
    status = record.get("status")
    if status in [None, "Unknown", ""]:
        issues.append({
            "issue_type": "PLACEHOLDER_STATUS",
            "field": "status",
            "current_value": status,
            "suggested_action": "Set status to 'active', 'inactive', or 'scheduled'",
        })
    
    # Check software
    software = record.get("software", {})
    if not software or software.get("id") in [None, "Unknown", ""] or software.get("name") in [None, "Unknown", ""]:
        issues.append({
            "issue_type": "PLACEHOLDER_SOFTWARE",
            "field": "software",
            "current_value": software,
            "suggested_action": "Set software.id and software.name based on portal platform",
        })
    
    return issues if issues else None


def check_urls(record):
    """Validate URL formats"""
    issues = []
    
    # Check main link
    link = record.get("link")
    if link:
        try:
            parsed = urlparse(link)
            if not parsed.scheme or not parsed.netloc:
                issues.append({
                    "issue_type": "INVALID_URL",
                    "field": "link",
                    "current_value": link,
                    "suggested_action": f"Fix URL format: {link}",
                })
        except Exception:
            issues.append({
                "issue_type": "INVALID_URL",
                "field": "link",
                "current_value": link,
                "suggested_action": f"Fix URL format: {link}",
            })
    
    # Check owner link
    owner_link = record.get("owner", {}).get("link")
    if owner_link:
        try:
            parsed = urlparse(owner_link)
            if not parsed.scheme or not parsed.netloc:
                issues.append({
                    "issue_type": "INVALID_OWNER_URL",
                    "field": "owner.link",
                    "current_value": owner_link,
                    "suggested_action": f"Fix owner URL format: {owner_link}",
                })
        except Exception:
            issues.append({
                "issue_type": "INVALID_OWNER_URL",
                "field": "owner.link",
                "current_value": owner_link,
                "suggested_action": f"Fix owner URL format: {owner_link}",
            })
    
    # Check endpoint URLs
    endpoints = record.get("endpoints", [])
    for idx, endpoint in enumerate(endpoints):
        endpoint_url = endpoint.get("url")
        if endpoint_url:
            try:
                parsed = urlparse(endpoint_url)
                if not parsed.scheme or not parsed.netloc:
                    issues.append({
                        "issue_type": "INVALID_ENDPOINT_URL",
                        "field": f"endpoints[{idx}].url",
                        "current_value": endpoint_url,
                        "suggested_action": f"Fix endpoint URL format: {endpoint_url}",
                    })
            except Exception:
                issues.append({
                    "issue_type": "INVALID_ENDPOINT_URL",
                    "field": f"endpoints[{idx}].url",
                    "current_value": endpoint_url,
                    "suggested_action": f"Fix endpoint URL format: {endpoint_url}",
                })
    
    return issues if issues else None


def check_required_fields(record):
    """Check for missing required fields based on schema"""
    issues = []
    required_fields = ["id", "uid", "name", "link", "catalog_type", "status", "software", "owner"]
    
    for field in required_fields:
        if field not in record or record[field] is None:
            issues.append({
                "issue_type": "MISSING_REQUIRED_FIELD",
                "field": field,
                "current_value": None,
                "suggested_action": f"Add required field: {field}",
            })
        elif field == "software" and (not isinstance(record[field], dict) or not record[field].get("id")):
            issues.append({
                "issue_type": "MISSING_REQUIRED_FIELD",
                "field": "software.id",
                "current_value": record.get("software"),
                "suggested_action": "Add required field: software.id",
            })
        elif field == "owner" and (not isinstance(record[field], dict) or not record[field].get("name")):
            issues.append({
                "issue_type": "MISSING_REQUIRED_FIELD",
                "field": "owner.name",
                "current_value": record.get("owner"),
                "suggested_action": "Add required field: owner.name",
            })
    
    return issues if issues else None


def check_identifiers(record):
    """Check for missing or incomplete identifiers"""
    issues = []
    
    identifiers = record.get("identifiers", [])
    if identifiers and len(identifiers) > 0:
        for idx, identifier in enumerate(identifiers):
            if not identifier.get("id") or not identifier.get("value"):
                issues.append({
                    "issue_type": "INCOMPLETE_IDENTIFIER",
                    "field": f"identifiers[{idx}]",
                    "current_value": identifier,
                    "suggested_action": "Add id and value fields to identifier",
                })
    
    return issues if issues else None


def check_license_completeness(record):
    """Check license information completeness"""
    issues = []
    rights = record.get("rights", {})
    
    license_id = rights.get("license_id")
    license_name = rights.get("license_name")
    license_url = rights.get("license_url")
    
    # Check for inconsistent combinations
    if license_name and not license_url:
        issues.append({
            "issue_type": "INCONSISTENT_LICENSE",
            "field": "rights.license_url",
            "current_value": {"license_name": license_name, "license_url": license_url},
            "suggested_action": "Add license_url to complement license_name",
        })
    elif license_id and not (license_name or license_url):
        issues.append({
            "issue_type": "INCONSISTENT_LICENSE",
            "field": "rights.license_name or rights.license_url",
            "current_value": {"license_id": license_id},
            "suggested_action": "Add license_name or license_url to complement license_id",
        })
    
    return issues if issues else None


def check_api_status_coherence(record):
    """Check API status coherence"""
    issues = []
    
    api = record.get("api", False)
    api_status = record.get("api_status")
    endpoints = record.get("endpoints", [])
    
    # Check if api_status is missing
    if api_status is None or api_status == "":
        issues.append({
            "issue_type": "MISSING_API_STATUS",
            "field": "api_status",
            "current_value": api_status,
            "suggested_action": "Set api_status to 'active', 'inactive', or 'uncertain'",
        })
    
    # Check for mismatches
    if api is True:
        if api_status in ["inactive", "uncertain"] and len(endpoints) > 0:
            issues.append({
                "issue_type": "API_STATUS_MISMATCH",
                "field": "api_status",
                "current_value": f"api={api}, api_status={api_status}, endpoints={len(endpoints)}",
                "suggested_action": f"Update api_status to 'active' since endpoints are present",
            })
    elif api is False and len(endpoints) > 0:
        issues.append({
            "issue_type": "API_STATUS_MISMATCH",
            "field": "api",
            "current_value": f"api={api}, endpoints={len(endpoints)}",
            "suggested_action": "Set api=True since endpoints are present, or remove endpoints",
        })
    
    return issues if issues else None


def check_content_types_access_mode(record):
    """Check for missing content_types or access_mode"""
    issues = []
    
    content_types = record.get("content_types", [])
    if not content_types or len(content_types) == 0:
        issues.append({
            "issue_type": "MISSING_CONTENT_TYPES",
            "field": "content_types",
            "current_value": content_types,
            "suggested_action": "Add content_types (e.g., ['dataset', 'map_layer'])",
        })
    
    access_mode = record.get("access_mode", [])
    if not access_mode or len(access_mode) == 0:
        issues.append({
            "issue_type": "MISSING_ACCESS_MODE",
            "field": "access_mode",
            "current_value": access_mode,
            "suggested_action": "Add access_mode (e.g., ['open'])",
        })
    
    return issues if issues else None


def check_language_validation(record):
    """Check language codes validity"""
    issues = []
    langs = record.get("langs", [])
    
    for idx, lang in enumerate(langs):
        if not is_valid_language(lang):
            issues.append({
                "issue_type": "INVALID_LANGUAGE",
                "field": f"langs[{idx}]",
                "current_value": lang,
                "suggested_action": "Ensure language has both 'id' and 'name' fields",
            })
    
    return issues if issues else None


def check_coverage_normalization(record):
    """Check coverage normalization issues"""
    issues = []
    coverage = record.get("coverage", [])
    
    # Track seen country+level combinations for duplicates
    seen_combinations = set()
    
    for idx, cov_entry in enumerate(coverage):
        location = cov_entry.get("location", {})
        country = location.get("country", {})
        country_id = country.get("id")
        
        if country_id:
            # Check for missing level
            if "level" not in location or location.get("level") is None:
                issues.append({
                    "issue_type": "COVERAGE_NORMALIZATION",
                    "field": f"coverage[{idx}].location.level",
                    "current_value": location,
                    "suggested_action": "Add level field to coverage location",
                })
            
            # Check for missing macroregion when country is present
            if "macroregion" not in location or not location.get("macroregion"):
                issues.append({
                    "issue_type": "COVERAGE_NORMALIZATION",
                    "field": f"coverage[{idx}].location.macroregion",
                    "current_value": location,
                    "suggested_action": "Add macroregion information to coverage location",
                })
            
            # Check for duplicates
            level = location.get("level")
            combo = (country_id, level)
            if combo in seen_combinations:
                issues.append({
                    "issue_type": "DUPLICATE_COVERAGE",
                    "field": f"coverage[{idx}]",
                    "current_value": cov_entry,
                    "suggested_action": "Remove duplicate coverage entry",
                })
            seen_combinations.add(combo)
    
    return issues if issues else None


def check_software_normalization(record):
    """Check software ID and name normalization"""
    issues = []
    software = record.get("software", {})
    
    if not software:
        return None
    
    software_id = software.get("id")
    software_name = software.get("name")
    
    if not software_id:
        return None
    
    software_map = get_cached_software_map()
    
    # Check if software ID exists in known software
    if software_map and software_id not in software_map:
        issues.append({
            "issue_type": "SOFTWARE_ID_UNKNOWN",
            "field": "software.id",
            "current_value": software_id,
            "suggested_action": f"Verify software.id '{software_id}' exists in software definitions",
        })
    elif software_map and software_id in software_map:
        # Check if name matches
        expected_name = software_map[software_id].get("name", "")
        if expected_name and software_name and software_name != expected_name:
            issues.append({
                "issue_type": "SOFTWARE_NAME_MISMATCH",
                "field": "software.name",
                "current_value": f"id={software_id}, name={software_name}",
                "suggested_action": f"Update software.name to '{expected_name}' to match software.id",
            })
    
    return issues if issues else None


def check_catalog_software_coherence(record):
    """Check if catalog_type matches expected type for software"""
    issues = []
    
    software = record.get("software", {})
    software_id = software.get("id")
    catalog_type = record.get("catalog_type")
    
    if software_id and catalog_type and software_id in MAP_SOFTWARE_OWNER_CATALOG_TYPE:
        expected_type = MAP_SOFTWARE_OWNER_CATALOG_TYPE[software_id]
        if catalog_type != expected_type:
            issues.append({
                "issue_type": "CATALOG_SOFTWARE_MISMATCH",
                "field": "catalog_type",
                "current_value": f"catalog_type={catalog_type}, software.id={software_id}",
                "suggested_action": f"Update catalog_type to '{expected_type}' to match software.id",
            })
    
    return issues if issues else None


def validate_software_profile(software_record):
    """Validate extended software profile fields"""
    issues = []
    
    if not software_record or software_record.get("type") != "Software":
        return issues
    
    software_id = software_record.get("id", "unknown")
    
    # Validate version format
    version = software_record.get("version")
    if version and version not in [None, "latest", "unknown"]:
        # Version should be semantic versioning or date format
        version_pattern = re.compile(r'^(\d+\.\d+\.\d+|v?\d+\.\d+|latest|\d{4}-\d{2}-\d{2})$')
        if not version_pattern.match(str(version)):
            issues.append({
                "issue_type": "SOFTWARE_VERSION_FORMAT",
                "field": "version",
                "current_value": version,
                "suggested_action": "Version should follow semantic versioning (e.g., 2.10.0) or date format (YYYY-MM-DD)",
            })
    
    # Validate URLs
    url_fields = ["repository_url", "documentation_url", "changelog_url"]
    for field in url_fields:
        url = software_record.get(field)
        if url:
            try:
                parsed = urlparse(url)
                if not parsed.scheme or not parsed.netloc:
                    issues.append({
                        "issue_type": "SOFTWARE_INVALID_URL",
                        "field": field,
                        "current_value": url,
                        "suggested_action": f"URL should be a valid HTTP/HTTPS URL",
                    })
            except Exception:
                issues.append({
                    "issue_type": "SOFTWARE_INVALID_URL",
                    "field": field,
                    "current_value": url,
                    "suggested_action": f"URL format is invalid",
                })
    
    # Validate release_date format
    release_date = software_record.get("release_date")
    if release_date:
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
        if not date_pattern.match(str(release_date)):
            issues.append({
                "issue_type": "SOFTWARE_INVALID_DATE",
                "field": "release_date",
                "current_value": release_date,
                "suggested_action": "Release date should be in ISO format (YYYY-MM-DD)",
            })
    
    # Validate last_updated format
    last_updated = software_record.get("last_updated")
    if last_updated:
        datetime_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$')
        if not datetime_pattern.match(str(last_updated)):
            issues.append({
                "issue_type": "SOFTWARE_INVALID_DATETIME",
                "field": "last_updated",
                "current_value": last_updated,
                "suggested_action": "Last updated should be in ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)",
            })
    
    # Validate plugins structure
    plugins = software_record.get("plugins")
    if plugins is not None:
        if not isinstance(plugins, list):
            issues.append({
                "issue_type": "SOFTWARE_INVALID_PLUGINS",
                "field": "plugins",
                "current_value": type(plugins).__name__,
                "suggested_action": "Plugins should be a list of objects with 'name' field",
            })
        else:
            for idx, plugin in enumerate(plugins):
                if not isinstance(plugin, dict):
                    issues.append({
                        "issue_type": "SOFTWARE_INVALID_PLUGIN_ITEM",
                        "field": f"plugins[{idx}]",
                        "current_value": type(plugin).__name__,
                        "suggested_action": "Each plugin should be an object with 'name' field",
                    })
                elif "name" not in plugin:
                    issues.append({
                        "issue_type": "SOFTWARE_PLUGIN_MISSING_NAME",
                        "field": f"plugins[{idx}]",
                        "current_value": plugin,
                        "suggested_action": "Plugin object must have 'name' field",
                    })
    
    # Validate capabilities structure
    capabilities = software_record.get("capabilities")
    if capabilities is not None:
        if not isinstance(capabilities, list):
            issues.append({
                "issue_type": "SOFTWARE_INVALID_CAPABILITIES",
                "field": "capabilities",
                "current_value": type(capabilities).__name__,
                "suggested_action": "Capabilities should be a list of strings",
            })
        else:
            for idx, capability in enumerate(capabilities):
                if not isinstance(capability, str):
                    issues.append({
                        "issue_type": "SOFTWARE_INVALID_CAPABILITY_ITEM",
                        "field": f"capabilities[{idx}]",
                        "current_value": type(capability).__name__,
                        "suggested_action": "Each capability should be a string",
                    })
    
    # Validate export_formats structure
    export_formats = software_record.get("export_formats")
    if export_formats is not None:
        if not isinstance(export_formats, list):
            issues.append({
                "issue_type": "SOFTWARE_INVALID_EXPORT_FORMATS",
                "field": "export_formats",
                "current_value": type(export_formats).__name__,
                "suggested_action": "Export formats should be a list of strings",
            })
        else:
            for idx, fmt in enumerate(export_formats):
                if not isinstance(fmt, str):
                    issues.append({
                        "issue_type": "SOFTWARE_INVALID_EXPORT_FORMAT_ITEM",
                        "field": f"export_formats[{idx}]",
                        "current_value": type(fmt).__name__,
                        "suggested_action": "Each export format should be a string",
                    })
    
    # Validate license structure
    license_obj = software_record.get("license")
    if license_obj is not None:
        if not isinstance(license_obj, dict):
            issues.append({
                "issue_type": "SOFTWARE_INVALID_LICENSE",
                "field": "license",
                "current_value": type(license_obj).__name__,
                "suggested_action": "License should be an object with 'type' field",
            })
        elif "type" not in license_obj:
            issues.append({
                "issue_type": "SOFTWARE_LICENSE_MISSING_TYPE",
                "field": "license",
                "current_value": license_obj,
                "suggested_action": "License object must have 'type' field",
            })
    
    return issues


def check_tag_topic_hygiene(record):
    """Check tag and topic hygiene"""
    issues = []
    
    # Check tags
    tags = record.get("tags", [])
    seen_tags = set()
    for idx, tag in enumerate(tags):
        if not isinstance(tag, str):
            continue
        
        tag_lower = tag.lower().strip()
        
        # Check for very short or long tags
        if len(tag_lower) < 3:
            issues.append({
                "issue_type": "TAG_HYGIENE",
                "field": f"tags[{idx}]",
                "current_value": tag,
                "suggested_action": "Tag is too short (less than 3 characters), consider removing or expanding",
            })
        elif len(tag_lower) > 40:
            issues.append({
                "issue_type": "TAG_HYGIENE",
                "field": f"tags[{idx}]",
                "current_value": tag,
                "suggested_action": "Tag is too long (more than 40 characters), consider shortening",
            })
        
        # Check for duplicates (case-insensitive)
        if tag_lower in seen_tags:
            issues.append({
                "issue_type": "DUPLICATE_TAGS",
                "field": f"tags[{idx}]",
                "current_value": tag,
                "suggested_action": "Remove duplicate tag",
            })
        seen_tags.add(tag_lower)
    
    # Check topics
    topics = record.get("topics", [])
    for idx, topic in enumerate(topics):
        if not isinstance(topic, dict):
            continue
        
        topic_id = topic.get("id")
        topic_name = topic.get("name")
        topic_type = topic.get("type")
        
        # Check if topic is incomplete
        if not topic_id and not topic_name:
            issues.append({
                "issue_type": "TOPIC_INCOMPLETE",
                "field": f"topics[{idx}]",
                "current_value": topic,
                "suggested_action": "Add id or name to topic",
            })
        elif topic_id and not topic_name:
            issues.append({
                "issue_type": "TOPIC_INCOMPLETE",
                "field": f"topics[{idx}].name",
                "current_value": topic,
                "suggested_action": "Add name to topic",
            })
        elif not topic_type:
            issues.append({
                "issue_type": "TOPIC_INCOMPLETE",
                "field": f"topics[{idx}].type",
                "current_value": topic,
                "suggested_action": "Add type to topic (e.g., 'eudatatheme')",
            })
    
    return issues if issues else None


def check_description_quality(record):
    """Check description quality"""
    issues = []
    description = record.get("description", "")
    
    if not description or not isinstance(description, str):
        return None
    
    description = description.strip()
    
    # Check for very short descriptions (less than 40 characters)
    if len(description) < 40:
        issues.append({
            "issue_type": "SHORT_DESCRIPTION",
            "field": "description",
            "current_value": description[:50] + "..." if len(description) > 50 else description,
            "suggested_action": "Expand description to provide more meaningful information (at least 40 characters)",
        })
    
    return issues if issues else None


def check_uid_id_consistency(record):
    """Check UID and ID consistency"""
    issues = []
    
    uid = record.get("uid")
    record_id = record.get("id")
    
    # Check UID format
    if uid:
        if not is_valid_uid(uid):
            issues.append({
                "issue_type": "INVALID_UID",
                "field": "uid",
                "current_value": uid,
                "suggested_action": "UID should match format 'cdi' or 'temp' followed by 8 digits (e.g., 'cdi00001234' or 'temp00001234')",
            })
    else:
        # UID missing is already caught by check_required_fields, but we can add a specific check
        pass
    
    # Check ID format (should be alphanumeric, no special chars except maybe dashes/underscores)
    if record_id:
        if not isinstance(record_id, str) or len(record_id) == 0:
            issues.append({
                "issue_type": "INVALID_ID",
                "field": "id",
                "current_value": record_id,
                "suggested_action": "ID should be a non-empty string",
            })
        elif not re.match(r'^[a-zA-Z0-9_-]+$', record_id):
            issues.append({
                "issue_type": "INVALID_ID",
                "field": "id",
                "current_value": record_id,
                "suggested_action": "ID should contain only alphanumeric characters, dashes, and underscores",
            })
    
    return issues if issues else None


def check_contact_info(record):
    """Check for missing contact information.

    This is a low-priority advisory rule that focuses on records which are
    active and have at least some restricted access, where explicit contact
    details are more important.
    """
    issues = []
    status = record.get("status")
    access_mode = record.get("access_mode") or []
    owner = record.get("owner", {}) or {}

    if not isinstance(access_mode, list):
        return None

    # Only consider currently active, partially restricted portals
    if status == "active" and "restricted" in access_mode:
        owner_link = owner.get("link")
        if not owner_link:
            issues.append(
                {
                    "issue_type": "MISSING_CONTACT_INFO",
                    "field": "owner.link",
                    "current_value": owner_link,
                    "suggested_action": (
                        "Add a contact or support URL for the organization or portal, "
                        "so users know how to request access or ask questions."
                    ),
                }
            )

    return issues if issues else None


def check_path_country_consistency(record):
    """(Deprecated) Path vs metadata country check is currently disabled."""
    return None


def check_status_directory_uid_consistency(record):
    """Check coherence between record status/uid and its logical directory."""
    issues = []
    directory = record.get("_directory", "entities")
    status = record.get("status")
    uid = record.get("uid")

    # Currently analyze_quality scans verified entities; treat everything as entities
    if directory == "entities":
        if status == "scheduled":
            issues.append(
                {
                    "issue_type": "STATUS_DIRECTORY_MISMATCH",
                    "field": "status",
                    "current_value": status,
                    "suggested_action": (
                        "Move scheduled records to data/scheduled or update status "
                        "to 'active' or 'inactive' for verified entities."
                    ),
                }
            )

    return issues if issues else None


def check_id_host_correlation(record):
    """(Deprecated) ID vs host correlation check is currently disabled."""
    return None


def check_status_api_status_coherence_extended(record):
    """Check coherence between overall catalog status and api_status."""
    status = record.get("status")
    api_status = record.get("api_status")
    endpoints = record.get("endpoints") or []

    if status is None or api_status is None:
        return None

    issues = []

    if status == "inactive" and api_status == "active":
        issues.append(
            {
                "issue_type": "STATUS_API_STATUS_MISMATCH",
                "field": "api_status",
                "current_value": {"status": status, "api_status": api_status},
                "suggested_action": (
                    "Catalog is inactive but API is marked active; update api_status "
                    "or catalog status to keep them consistent."
                ),
            }
        )

    if status == "active" and api_status == "inactive" and endpoints:
        issues.append(
            {
                "issue_type": "STATUS_API_STATUS_MISMATCH",
                "field": "api_status",
                "current_value": {
                    "status": status,
                    "api_status": api_status,
                    "endpoints": len(endpoints),
                },
                "suggested_action": (
                    "Catalog is active and has endpoints but api_status is 'inactive'; "
                    "verify API availability and update api_status accordingly."
                ),
            }
        )

    return issues if issues else None


def check_title_quality(record):
    """Check title quality and detect placeholder-like names."""
    name = record.get("name")
    link = record.get("link")

    if not isinstance(name, str):
        return None

    title = name.strip()
    if not title:
        return None

    issues = []

    # Very short titles are usually not descriptive
    if len(title) < 5:
        issues.append(
            {
                "issue_type": "PLACEHOLDER_TITLE",
                "field": "name",
                "current_value": title,
                "suggested_action": (
                    "Expand the portal title to be more descriptive than a few characters."
                ),
            }
        )

    # Generic placeholder titles
    placeholder_titles = {
        "DATA PORTAL",
        "OPEN DATA",
        "PORTAL",
        "CATALOG",
        "NOT SPECIFIED",
        "UNKNOWN",
        "N/A",
        "NONE",
        "TBD",
        "PLACEHOLDER",
    }
    if title.upper() in placeholder_titles:
        issues.append(
            {
                "issue_type": "PLACEHOLDER_TITLE",
                "field": "name",
                "current_value": title,
                "suggested_action": (
                    "Replace generic titles like 'Data portal' or 'Open Data' with "
                    "a more specific portal name."
                ),
            }
        )

    # Title that looks like a bare URL/domain or domain+path
    title_as_url = title if "://" in title else f"https://{title}"
    parsed_title = urlparse(title_as_url)
    if parsed_title.netloc and "." in parsed_title.netloc and " " not in title:
        issues.append(
            {
                "issue_type": "PLACEHOLDER_TITLE",
                "field": "name",
                "current_value": title,
                "suggested_action": (
                    "Use a human-readable portal title instead of a URL/domain string."
                ),
            }
        )

    # Title equal to normalized host (no human-friendly name)
    if link:
        host = host_from_url(link)
        host_norm = normalize_host_for_id(host)
        if host_norm and title.lower() == host_norm.lower():
            issues.append(
                {
                    "issue_type": "PLACEHOLDER_TITLE",
                    "field": "name",
                    "current_value": title,
                    "suggested_action": (
                        "Use a human-readable portal name instead of just the hostname."
                    ),
                }
            )

        # Title equal to link domain or domain+path
        parsed_link = urlparse(link)
        link_host = (parsed_link.netloc or "").lower()
        link_host_no_www = link_host[4:] if link_host.startswith("www.") else link_host
        link_path = (parsed_link.path or "").strip("/").lower()
        title_norm = title.strip().lower().rstrip("/")
        if link_host_no_www:
            domain_like_values = {link_host, link_host_no_www}
            if link_path:
                domain_like_values.add(f"{link_host_no_www}/{link_path}")
                domain_like_values.add(f"{link_host}/{link_path}")
            if title_norm in domain_like_values:
                issues.append(
                    {
                        "issue_type": "PLACEHOLDER_TITLE",
                        "field": "name",
                        "current_value": title,
                        "suggested_action": (
                            "Replace domain/path-based title with a descriptive portal name."
                        ),
                    }
                )

    return issues if issues else None


def check_owner_coverage_coherence(record):
    """(Deprecated) Owner vs coverage coherence check is currently disabled."""
    return None


def check_rights_completeness(record):
    """Check that the rights object is not only partially populated."""
    rights = record.get("rights")

    if not isinstance(rights, dict):
        return None

    keys_present = [
        key for key in ("license_id", "license_name", "license_url") if rights.get(key)
    ]

    # Single-field rights objects are often incomplete and ambiguous
    if 0 < len(keys_present) < 2:
        return {
            "issue_type": "RIGHTS_INCOMPLETE",
            "field": "rights",
            "current_value": rights,
            "suggested_action": (
                "Add complementary license fields (id, name, and/or url) to make rights "
                "information complete and unambiguous."
            ),
        }

    return None


# Priority mapping for issue types
ISSUE_PRIORITY_MAP = {
    "CRITICAL": [
        "MISSING_REQUIRED_FIELD",
        "INVALID_URL",
        "INVALID_OWNER_URL",
        "INVALID_ENDPOINT_URL",
        "INVALID_UID",
        "INVALID_ID",
        "CATALOG_SOFTWARE_MISMATCH",
    ],
    "IMPORTANT": [
        "MISSING_OWNER_NAME",
        "MISSING_OWNER_TYPE",
        "MISSING_OWNER_LOCATION",
        "MISSING_COVERAGE",
        "PLACEHOLDER_CATALOG_TYPE",
        "PLACEHOLDER_STATUS",
        "PLACEHOLDER_SOFTWARE",
        "INCOMPLETE_IDENTIFIER",
        "INCONSISTENT_LICENSE",
        "API_STATUS_MISMATCH",
        "MISSING_API_STATUS",
        "SOFTWARE_ID_UNKNOWN",
        "SOFTWARE_NAME_MISMATCH",
        "SOFTWARE_EXPECTED_ENDPOINTS_MISSING",
        "COVERAGE_NORMALIZATION",
        "STATUS_DIRECTORY_MISMATCH",
    ],
    "MEDIUM": [
        "MISSING_DESCRIPTION",
        "MISSING_ENDPOINTS",
        "MISSING_LANGS",
        "MISSING_CONTENT_TYPES",
        "MISSING_ACCESS_MODE",
        "INVALID_LANGUAGE",
        "SHORT_DESCRIPTION",
        "TAG_HYGIENE",
        "TOPIC_INCOMPLETE",
        "STATUS_API_STATUS_MISMATCH",
        "RIGHTS_INCOMPLETE",
        "PLACEHOLDER_TITLE",
        "PLACEHOLDER_OWNER_NAME",
    ],
    "LOW": [
        "MISSING_TOPICS",
        "MISSING_TAGS",
        "MISSING_OWNER_LINK",
        "DUPLICATE_TAGS",
        "DUPLICATE_COVERAGE",
        "MISSING_CONTACT_INFO",
    ],
}

# Reverse mapping for quick lookup
PRIORITY_BY_ISSUE_TYPE = {}
for priority, issue_types in ISSUE_PRIORITY_MAP.items():
    for issue_type in issue_types:
        PRIORITY_BY_ISSUE_TYPE[issue_type] = priority


def extract_country_codes(record):
    """Extract country codes from a record.
    Returns a list of country codes (can be multiple if record has multiple coverage entries).
    """
    country_codes = []
    
    # Try owner location first
    owner = record.get("owner", {})
    if owner and owner.get("location") and owner["location"].get("country"):
        country_id = owner["location"]["country"].get("id")
        if country_id and country_id not in country_codes:
            country_codes.append(country_id)
    
    # Try coverage locations
    coverage = record.get("coverage", [])
    for cov_entry in coverage:
        if cov_entry.get("location") and cov_entry["location"].get("country"):
            country_id = cov_entry["location"]["country"].get("id")
            if country_id and country_id not in country_codes:
                country_codes.append(country_id)
    
    # If no country found, return ["UNKNOWN"]
    return country_codes if country_codes else ["UNKNOWN"]


def get_priority_level(issue_type):
    """Get priority level for an issue type."""
    return PRIORITY_BY_ISSUE_TYPE.get(issue_type, "MEDIUM")


# Helper utilities for validation
def is_valid_uid(uid):
    """Check if UID matches expected format: cdi or temp followed by 8 digits"""
    if not uid or not isinstance(uid, str):
        return False
    return bool(re.match(r'^(cdi|temp)\d{8}$', uid))


def is_valid_language(lang):
    """Check if language entry has required id and name fields"""
    if not isinstance(lang, dict):
        return False
    return bool(lang.get("id") and lang.get("name"))


def host_from_url(url):
    """Extract host from URL"""
    try:
        parsed = urlparse(url)
        return parsed.netloc.lower() if parsed.netloc else None
    except Exception:
        return None


def normalize_host_for_id(host):
    """Normalize host to match ID format (remove dots, dashes, underscores)"""
    if not host:
        return ""
    return host.replace(".", "").replace("-", "").replace("_", "").lower()


def get_software_map():
    """Load software definitions for validation"""
    software_map = {}
    try:
        software_data = load_jsonl(os.path.join(DATASETS_DIR, "software.jsonl"))
        for row in software_data:
            software_map[row["id"]] = {
                "id": row["id"],
                "name": row.get("name", ""),
                "has_api": row.get("has_api"),
            }
    except Exception:
        # If software.jsonl doesn't exist, return empty map
        pass
    return software_map


# Cache software map
_software_map_cache = None

def get_cached_software_map():
    """Get cached software map"""
    global _software_map_cache
    if _software_map_cache is None:
        _software_map_cache = get_software_map()
    return _software_map_cache


# Fix command helper functions
def calculate_file_hash(file_path: str) -> Optional[str]:
    """Calculate MD5 hash of a file."""
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logger.debug(f"Error calculating hash for {file_path}: {e}")
        return None


def read_jsonl_issues(file_path: str) -> List[Dict[str, Any]]:
    """Read and parse JSONL file for issues."""
    records = []
    if not os.path.exists(file_path):
        logger.error(f"Error: File not found: {file_path}")
        return records
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    record = json.loads(line)
                    issues_list = record.get('issues', [])
                    if issues_list:
                        records.append(record)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON at line {line_num}: {e}")
                    continue
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
    
    return records


def build_prompt(record: Dict[str, Any]) -> str:
    """Build a simple, effective prompt for cursor-agent."""
    record_id = record.get('record_id', 'unknown')
    file_path = record.get('file_path', '')
    issues = record.get('issues', [])
    
    prompt_parts = [
        f"Fix all data quality issues for record {record_id} in file {file_path}.",
        "",
        f"Issues to fix ({len(issues)} total):"
    ]
    
    for issue in issues:
        issue_type = issue.get('issue_type', '')
        field = issue.get('field', '')
        action = issue.get('suggested_action', '')
        prompt_parts.append(f"- {issue_type}: {field} - {action}")
    
    prompt_parts.extend([
        "",
        "For each issue, follow the suggested action. Update the metadata comprehensively."
    ])
    
    return "\n".join(prompt_parts)


def call_cursor_agent(workspace: str, prompt: str) -> tuple[bool, Optional[str]]:
    """Call cursor-agent and return success status and output."""
    try:
        # Ensure workspace is an absolute path
        workspace_abs = os.path.abspath(workspace)
        
        # Pass prompt as command-line argument (matching shell script behavior)
        result = subprocess.run(
            [
                "cursor-agent",
                "--print",
                "--workspace", workspace_abs,
                "--output-format", "text",
                prompt
            ],
            text=True,
            capture_output=True,
            timeout=300  # 5 minute timeout per record
        )
        
        # Log detailed information for debugging
        stdout_content = result.stdout.strip() if result.stdout else ""
        stderr_content = result.stderr.strip() if result.stderr else ""
        
        # Combine stdout and stderr for comprehensive error reporting
        # (cursor-agent may write to either stream)
        combined_output = ""
        if stdout_content:
            combined_output += stdout_content
        if stderr_content:
            if combined_output:
                combined_output += "\n--- stderr ---\n" + stderr_content
            else:
                combined_output = stderr_content
        
        if result.returncode != 0:
            # Build comprehensive error message
            error_parts = []
            if combined_output:
                error_parts.append(f"output: {combined_output}")
            else:
                error_parts.append("No output")
            error_parts.append(f"return code: {result.returncode}")
            error_msg = " | ".join(error_parts)
            logger.debug(f"cursor-agent failed: {error_msg}")
            return False, error_msg
        
        # Success case - return combined output (cursor-agent might write to stderr even on success)
        return True, combined_output if combined_output else stdout_content
    except subprocess.TimeoutExpired:
        return False, "Timeout after 5 minutes"
    except FileNotFoundError:
        return False, "cursor-agent not found in PATH"
    except Exception as e:
        return False, f"Exception calling cursor-agent: {str(e)}"


@dataclass
class RecordStats:
    """Statistics for a single record processing."""
    record_id: str
    file_path: str
    issues_count: int
    changed: bool = False
    error: Optional[str] = None
    changes_detail: Optional[Dict[str, List[Dict[str, Any]]]] = None


@dataclass
class SummaryStats:
    """Summary statistics for all processed records."""
    total: int = 0
    updated: int = 0
    no_change: int = 0
    failed: int = 0
    records: List[RecordStats] = field(default_factory=list)
    total_fields_added: int = 0
    total_fields_modified: int = 0
    total_fields_removed: int = 0


def process_record(record: Dict[str, Any], stats: SummaryStats, current_index: int, base_dir: str) -> RecordStats:
    """Process a single record with change detection."""
    record_id = record.get('record_id', 'unknown')
    file_path_str = record.get('file_path', '')
    issues = record.get('issues', [])
    issues_count = len(issues)
    
    full_file_path = os.path.join(base_dir, "data", "entities", file_path_str)
    relative_file_path = f"data/entities/{file_path_str}"
    
    record_stats = RecordStats(
        record_id=record_id,
        file_path=file_path_str,
        issues_count=issues_count
    )
    
    logger.info(f"[{current_index + 1}/{stats.total}] Processing: {file_path_str}")
    logger.info(f"  Record ID: {record_id}")
    logger.info(f"  Issues to fix: {issues_count}")
    
    # Check if file exists
    if not os.path.exists(full_file_path):
        error_msg = f"File not found: {full_file_path}"
        logger.error(f"  ✗ {error_msg}")
        record_stats.error = error_msg
        stats.failed += 1
        return record_stats
    
    # Load YAML before processing
    try:
        with open(full_file_path, 'r', encoding='utf-8') as f:
            before_yaml = yaml.load(f, Loader=Loader)
    except Exception as e:
        error_msg = f"Error loading YAML before processing: {e}"
        logger.error(f"  ✗ {error_msg}")
        record_stats.error = error_msg
        stats.failed += 1
        return record_stats
    
    # Calculate hash before processing
    before_hash = calculate_file_hash(full_file_path)
    
    # Build prompt
    prompt = build_prompt(record)
    
    # Call cursor-agent
    success, output = call_cursor_agent(base_dir, prompt)
    
    if not success:
        error_msg = output or "Unknown error"
        logger.error(f"  ✗ Failed to process {record_id}")
        logger.error(f"  Error details: {error_msg}")
        record_stats.error = error_msg
        stats.failed += 1
        return record_stats
    
    # Log successful cursor-agent call (even if no file changes detected)
    if output:
        logger.debug(f"  cursor-agent output: {output[:200]}...")
    
    # Calculate hash after processing
    after_hash = calculate_file_hash(full_file_path)
    
    # Check if file was modified
    if before_hash and after_hash and before_hash != after_hash:
        logger.info(f"  ✓ Updated {record_id}")
        logger.info(f"  File modified: {file_path_str}")
        record_stats.changed = True
        stats.updated += 1
        
        # Load YAML after processing and detect changes
        try:
            with open(full_file_path, 'r', encoding='utf-8') as f:
                after_yaml = yaml.load(f, Loader=Loader)
            
            # Detect detailed changes
            changes = detect_yaml_changes(before_yaml, after_yaml)
            record_stats.changes_detail = changes
            
            # Log detailed changes
            added_count = len(changes.get("added", []))
            modified_count = len(changes.get("modified", []))
            removed_count = len(changes.get("removed", []))
            
            logger.info(f"  Changes detected:")
            logger.info(f"    - Added: {added_count} field(s)")
            logger.info(f"    - Modified: {modified_count} field(s)")
            logger.info(f"    - Removed: {removed_count} field(s)")
            
            # Log specific changes (limit to first 10 of each type for readability)
            if changes.get("added"):
                logger.info(f"  Added fields:")
                for change in changes["added"][:10]:
                    logger.info(f"    + {change['path']}: {str(change['value'])[:100]}")
                if len(changes["added"]) > 10:
                    logger.info(f"    ... and {len(changes['added']) - 10} more")
            
            if changes.get("modified"):
                logger.info(f"  Modified fields:")
                for change in changes["modified"][:10]:
                    old_val = str(change['old_value'])[:80]
                    new_val = str(change['new_value'])[:80]
                    logger.info(f"    ~ {change['path']}:")
                    logger.info(f"      Old: {old_val}")
                    logger.info(f"      New: {new_val}")
                if len(changes["modified"]) > 10:
                    logger.info(f"    ... and {len(changes['modified']) - 10} more")
            
            if changes.get("removed"):
                logger.info(f"  Removed fields:")
                for change in changes["removed"][:10]:
                    logger.info(f"    - {change['path']}: {str(change['value'])[:100]}")
                if len(changes["removed"]) > 10:
                    logger.info(f"    ... and {len(changes['removed']) - 10} more")
            
            # Update summary statistics
            stats.total_fields_added += added_count
            stats.total_fields_modified += modified_count
            stats.total_fields_removed += removed_count
            
        except Exception as e:
            logger.warning(f"  Warning: Could not detect detailed changes: {e}")
    else:
        logger.info(f"  ○ No changes detected for {record_id}")
        logger.info(f"  File unchanged: {file_path_str}")
        stats.no_change += 1
    
    return record_stats


def detect_yaml_changes(before_dict: Dict[str, Any], after_dict: Dict[str, Any], path: str = "") -> Dict[str, List[Dict[str, Any]]]:
    """
    Recursively compare two YAML dictionaries and detect changes.
    
    Returns a dictionary with:
    - "added": list of fields added (present in after, not in before)
    - "modified": list of fields modified (present in both but different)
    - "removed": list of fields removed (present in before, not in after)
    """
    changes = {
        "added": [],
        "modified": [],
        "removed": []
    }
    
    def _normalize_value(value):
        """Normalize value for comparison (handle lists, dicts, etc.)"""
        if isinstance(value, list):
            return tuple(sorted(_normalize_value(v) for v in value) if all(isinstance(v, (str, int, float, bool)) for v in value) else value)
        elif isinstance(value, dict):
            return tuple(sorted((k, _normalize_value(v)) for k, v in value.items()))
        return value
    
    def _compare_dicts(before: Dict, after: Dict, current_path: str):
        """Recursively compare dictionaries"""
        # Get all keys from both dicts
        all_keys = set(before.keys()) | set(after.keys())
        
        for key in all_keys:
            field_path = f"{current_path}.{key}" if current_path else key
            
            if key not in before:
                # Field was added
                changes["added"].append({
                    "path": field_path,
                    "value": after[key]
                })
            elif key not in after:
                # Field was removed
                changes["removed"].append({
                    "path": field_path,
                    "value": before[key]
                })
            else:
                before_val = before[key]
                after_val = after[key]
                
                # If both are dicts, recurse
                if isinstance(before_val, dict) and isinstance(after_val, dict):
                    _compare_dicts(before_val, after_val, field_path)
                # If both are lists, compare elements
                elif isinstance(before_val, list) and isinstance(after_val, list):
                    # Simple comparison - if lists are different, mark as modified
                    if _normalize_value(before_val) != _normalize_value(after_val):
                        changes["modified"].append({
                            "path": field_path,
                            "old_value": before_val,
                            "new_value": after_val
                        })
                # Otherwise, direct comparison
                else:
                    if before_val != after_val:
                        changes["modified"].append({
                            "path": field_path,
                            "old_value": before_val,
                            "new_value": after_val
                        })
    
    _compare_dicts(before_dict, after_dict, path)
    return changes


def generate_full_report(issues, records_with_issues, total_records, output_path):
    """Generate the full comprehensive report."""
    report_lines = []
    report_lines.append("DATA QUALITY ANALYSIS REPORT")
    report_lines.append("=" * 80)
    report_lines.append(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"Total Records Analyzed: {total_records}")
    report_lines.append(f"Total Issues Found: {len(issues)}")
    report_lines.append(f"Records with Issues: {len(records_with_issues)}")
    report_lines.append("")
    
    # Group issues by type
    issues_by_type = {}
    for issue in issues:
        issue_type = issue["issue_type"]
        if issue_type not in issues_by_type:
            issues_by_type[issue_type] = []
        issues_by_type[issue_type].append(issue)
    
    # Report issues by type
    report_lines.append("=== ISSUES BY TYPE ===")
    report_lines.append("")
    
    for issue_type in sorted(issues_by_type.keys()):
        issues_list = issues_by_type[issue_type]
        report_lines.append(f"[{issue_type}]")
        report_lines.append(f"Count: {len(issues_list)}")
        report_lines.append(f"Priority: {issues_list[0].get('priority', 'MEDIUM')}")
        report_lines.append("")
        
        for issue in issues_list[:50]:  # Limit to first 50 per type for readability
            report_lines.append(f"File: {issue['file_path']}")
            report_lines.append(f"Record ID: {issue['record_id']}")
            report_lines.append(f"Country: {issue.get('country_code', 'UNKNOWN')}")
            report_lines.append(f"Issue: {issue_type}")
            report_lines.append(f"Field: {issue['field']}")
            report_lines.append(f"Current Value: {issue['current_value']}")
            report_lines.append(f"Suggested Action: {issue['suggested_action']}")
            report_lines.append("")
        
        if len(issues_list) > 50:
            report_lines.append(f"... and {len(issues_list) - 50} more records with this issue")
            report_lines.append("")
    
    # Summary by issue type
    report_lines.append("")
    report_lines.append("=== SUMMARY BY ISSUE TYPE ===")
    report_lines.append("")
    for issue_type in sorted(issues_by_type.keys()):
        count = len(issues_by_type[issue_type])
        priority = issues_by_type[issue_type][0].get("priority", "MEDIUM") if issues_by_type[issue_type] else "MEDIUM"
        report_lines.append(f"{issue_type} ({priority}): {count} issues")
    
    # Summary by priority
    report_lines.append("")
    report_lines.append("=== SUMMARY BY PRIORITY ===")
    report_lines.append("")
    issues_by_priority = {}
    for issue in issues:
        priority = issue.get("priority", "MEDIUM")
        if priority not in issues_by_priority:
            issues_by_priority[priority] = []
        issues_by_priority[priority].append(issue)
    
    for priority in ["CRITICAL", "IMPORTANT", "MEDIUM", "LOW"]:
        if priority in issues_by_priority:
            count = len(issues_by_priority[priority])
            report_lines.append(f"{priority}: {count} issues")
    
    # Records with multiple issues
    report_lines.append("")
    report_lines.append("=== RECORDS WITH MULTIPLE ISSUES (3+) ===")
    report_lines.append("")
    
    multi_issue_records = {
        rid: data for rid, data in records_with_issues.items() if len(data["issues"]) >= 3
    }
    
    if multi_issue_records:
        for record_id, data in sorted(multi_issue_records.items(), key=lambda x: len(x[1]["issues"]), reverse=True)[:100]:
            report_lines.append(f"Record ID: {record_id}")
            report_lines.append(f"File: {data['file_path']}")
            report_lines.append(f"Country: {data.get('country_code', 'UNKNOWN')}")
            report_lines.append(f"Issue Count: {len(data['issues'])}")
            report_lines.append("Issues:")
            for issue in data["issues"]:
                report_lines.append(f"  - {issue['issue_type']} ({issue.get('priority', 'MEDIUM')}): {issue['field']}")
            report_lines.append("")
    else:
        report_lines.append("No records found with 3+ issues")
    
    # Write report to file
    report_content = "\n".join(report_lines)
    with open(output_path, "w", encoding="utf8") as f:
        f.write(report_content)


def generate_country_reports(issues_by_country, records_by_country, output_dir):
    """Generate reports for each country."""
    countries_dir = os.path.join(output_dir, "countries")
    os.makedirs(countries_dir, exist_ok=True)
    
    for country_code, country_issues in issues_by_country.items():
        if not country_issues:
            continue
        
        country_records = records_by_country.get(country_code, {})
        
        report_lines = []
        report_lines.append(f"DATA QUALITY REPORT - COUNTRY: {country_code}")
        report_lines.append("=" * 80)
        report_lines.append(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Country Code: {country_code}")
        report_lines.append(f"Total Records with Issues: {len(country_records)}")
        report_lines.append(f"Total Issues Found: {len(country_issues)}")
        report_lines.append("")
        
        # Group issues by type
        issues_by_type = {}
        for issue in country_issues:
            issue_type = issue["issue_type"]
            if issue_type not in issues_by_type:
                issues_by_type[issue_type] = []
            issues_by_type[issue_type].append(issue)
        
        # Report issues by type
        report_lines.append("=== ISSUES BY TYPE ===")
        report_lines.append("")
        
        for issue_type in sorted(issues_by_type.keys()):
            issues_list = issues_by_type[issue_type]
            report_lines.append(f"[{issue_type}]")
            report_lines.append(f"Count: {len(issues_list)}")
            report_lines.append(f"Priority: {issues_list[0].get('priority', 'MEDIUM')}")
            report_lines.append("")
            
            for issue in issues_list[:100]:  # Show more for country-specific reports
                report_lines.append(f"File: {issue['file_path']}")
                report_lines.append(f"Record ID: {issue['record_id']}")
                report_lines.append(f"Issue: {issue_type}")
                report_lines.append(f"Field: {issue['field']}")
                report_lines.append(f"Current Value: {issue['current_value']}")
                report_lines.append(f"Suggested Action: {issue['suggested_action']}")
                report_lines.append("")
            
            if len(issues_list) > 100:
                report_lines.append(f"... and {len(issues_list) - 100} more records with this issue")
                report_lines.append("")
        
        # Summary by issue type
        report_lines.append("")
        report_lines.append("=== SUMMARY BY ISSUE TYPE ===")
        report_lines.append("")
        for issue_type in sorted(issues_by_type.keys()):
            count = len(issues_by_type[issue_type])
            report_lines.append(f"{issue_type}: {count} issues")
        
        # Records with multiple issues
        multi_issue_records = {
            rid: data for rid, data in country_records.items() if len(data["issues"]) >= 3
        }
        
        if multi_issue_records:
            report_lines.append("")
            report_lines.append("=== RECORDS WITH MULTIPLE ISSUES (3+) ===")
            report_lines.append("")
            for record_id, data in sorted(multi_issue_records.items(), key=lambda x: len(x[1]["issues"]), reverse=True)[:50]:
                report_lines.append(f"Record ID: {record_id}")
                report_lines.append(f"File: {data['file_path']}")
                report_lines.append(f"Issue Count: {len(data['issues'])}")
                report_lines.append("Issues:")
                for issue in data["issues"]:
                    report_lines.append(f"  - {issue['issue_type']}: {issue['field']}")
                report_lines.append("")
        
        # Write country report
        country_file = os.path.join(countries_dir, f"{country_code}.txt")
        report_content = "\n".join(report_lines)
        with open(country_file, "w", encoding="utf8") as f:
            f.write(report_content)


def generate_priority_reports(issues_by_priority, output_dir):
    """Generate reports for each priority level."""
    priorities_dir = os.path.join(output_dir, "priorities")
    os.makedirs(priorities_dir, exist_ok=True)
    
    for priority in ["CRITICAL", "IMPORTANT", "MEDIUM", "LOW"]:
        priority_issues = issues_by_priority.get(priority, [])
        report_lines = []
        if not priority_issues:
            report_lines.append(f"DATA QUALITY REPORT - PRIORITY: {priority}")
            report_lines.append("=" * 80)
            report_lines.append(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            report_lines.append(f"Priority Level: {priority}")
            report_lines.append(f"Total Issues Found: 0")
            report_lines.append("")
            report_lines.append("No issues at this priority level.")
            priority_file = os.path.join(priorities_dir, f"{priority}.txt")
            with open(priority_file, "w", encoding="utf8") as f:
                f.write("\n".join(report_lines))
            continue

        report_lines = []
        report_lines.append(f"DATA QUALITY REPORT - PRIORITY: {priority}")
        report_lines.append("=" * 80)
        report_lines.append(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Priority Level: {priority}")
        report_lines.append(f"Total Issues Found: {len(priority_issues)}")
        report_lines.append("")
        
        # Group issues by type
        issues_by_type = {}
        for issue in priority_issues:
            issue_type = issue["issue_type"]
            if issue_type not in issues_by_type:
                issues_by_type[issue_type] = []
            issues_by_type[issue_type].append(issue)
        
        # Report issues by type
        report_lines.append("=== ISSUES BY TYPE ===")
        report_lines.append("")
        
        for issue_type in sorted(issues_by_type.keys()):
            issues_list = issues_by_type[issue_type]
            report_lines.append(f"[{issue_type}]")
            report_lines.append(f"Count: {len(issues_list)}")
            report_lines.append("")
            
            for issue in issues_list[:100]:
                report_lines.append(f"File: {issue['file_path']}")
                report_lines.append(f"Record ID: {issue['record_id']}")
                report_lines.append(f"Country: {issue.get('country_code', 'UNKNOWN')}")
                report_lines.append(f"Issue: {issue_type}")
                report_lines.append(f"Field: {issue['field']}")
                report_lines.append(f"Current Value: {issue['current_value']}")
                report_lines.append(f"Suggested Action: {issue['suggested_action']}")
                report_lines.append("")
            
            if len(issues_list) > 100:
                report_lines.append(f"... and {len(issues_list) - 100} more records with this issue")
                report_lines.append("")
        
        # Summary by issue type
        report_lines.append("")
        report_lines.append("=== SUMMARY BY ISSUE TYPE ===")
        report_lines.append("")
        for issue_type in sorted(issues_by_type.keys()):
            count = len(issues_by_type[issue_type])
            report_lines.append(f"{issue_type}: {count} issues")
        
        # Summary by country
        report_lines.append("")
        report_lines.append("=== SUMMARY BY COUNTRY ===")
        report_lines.append("")
        issues_by_country = {}
        for issue in priority_issues:
            country_code = issue.get("country_code", "UNKNOWN")
            if country_code not in issues_by_country:
                issues_by_country[country_code] = []
            issues_by_country[country_code].append(issue)
        
        for country_code in sorted(issues_by_country.keys()):
            count = len(issues_by_country[country_code])
            report_lines.append(f"{country_code}: {count} issues")
        
        # Write priority report
        priority_file = os.path.join(priorities_dir, f"{priority}.txt")
        report_content = "\n".join(report_lines)
        with open(priority_file, "w", encoding="utf8") as f:
            f.write(report_content)


def generate_rule_reports(issues_by_type, output_dir):
    """Generate reports for each issue type (rule)."""
    rules_dir = os.path.join(output_dir, "rules")
    os.makedirs(rules_dir, exist_ok=True)

    for issue_type in sorted(issues_by_type.keys()):
        issues_list = issues_by_type[issue_type]
        if not issues_list:
            continue

        priority = issues_list[0].get("priority", "MEDIUM")
        report_lines = []
        report_lines.append(f"DATA QUALITY REPORT - RULE: {issue_type}")
        report_lines.append("=" * 80)
        report_lines.append(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Issue Type: {issue_type}")
        report_lines.append(f"Priority: {priority}")
        report_lines.append(f"Total Issues Found: {len(issues_list)}")
        report_lines.append("")

        # Summary by country
        issues_by_country = {}
        for issue in issues_list:
            country = issue.get("country_code", "UNKNOWN")
            issues_by_country[country] = issues_by_country.get(country, 0) + 1

        report_lines.append("=== SUMMARY BY COUNTRY ===")
        report_lines.append("")
        for country_code in sorted(issues_by_country.keys()):
            report_lines.append(f"{country_code}: {issues_by_country[country_code]} issues")
        report_lines.append("")

        # Summary by record
        issues_by_record = {}
        for issue in issues_list:
            rid = issue.get("record_id", "unknown")
            issues_by_record[rid] = issues_by_record.get(rid, 0) + 1

        report_lines.append("=== SUMMARY BY RECORD ===")
        report_lines.append("")
        for rid, count in sorted(issues_by_record.items(), key=lambda x: x[1], reverse=True):
            report_lines.append(f"{rid}: {count} issues")
        report_lines.append("")

        # List all issues (no limit)
        report_lines.append("=== ISSUES ===")
        report_lines.append("")
        for issue in issues_list:
            report_lines.append(f"File: {issue['file_path']}")
            report_lines.append(f"Record ID: {issue['record_id']}")
            report_lines.append(f"Country: {issue.get('country_code', 'UNKNOWN')}")
            report_lines.append(f"Issue: {issue_type}")
            report_lines.append(f"Field: {issue['field']}")
            report_lines.append(f"Current Value: {issue['current_value']}")
            report_lines.append(f"Suggested Action: {issue['suggested_action']}")
            report_lines.append("")

        # Write rule report with sanitized filename
        safe_name = re.sub(r"[^A-Za-z0-9_-]+", "_", issue_type)
        rule_file = os.path.join(rules_dir, f"{safe_name}.txt")
        report_content = "\n".join(report_lines)
        with open(rule_file, "w", encoding="utf8") as f:
            f.write(report_content)


@app.command()
def analyze_quality(output: str = None):
    """Analyze data portal records for missing values and data quality issues, generating organized reports"""
    typer.echo("Analyzing data quality in YAML files...")
    
    # Verify path exists
    if not os.path.exists(ROOT_DIR):
        typer.echo(f"Error: Entities directory not found at {ROOT_DIR}")
        raise typer.Exit(1)
    
    # Set up output directory
    if output is None:
        output_dir = os.path.join(_REPO_ROOT, "dataquality")
    else:
        output_dir = output
    
    # Create directory structure
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "countries"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "priorities"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "rules"), exist_ok=True)
    
    typer.echo(f"Scanning entities directory: {ROOT_DIR}")
    typer.echo(f"Output directory: {output_dir}")
    
    all_issues = []
    total_records = 0
    records_with_issues = {}
    records_metadata = []
    
    # Walk through all YAML files
    for root, dirs, files in tqdm.tqdm(os.walk(ROOT_DIR)):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            total_records += 1
            try:
                f = open(filename, "r", encoding="utf8")
                record = yaml.load(f, Loader=Loader)
                f.close()
                
                if record is None:
                    continue
                
                record_id = record.get("id", "unknown")
                record_issues = []  # All issues with country-specific duplicates (for country reports)
                unique_record_issues = []  # Unique issues per record (for primary priority report)
                
                # Calculate relative path from ROOT_DIR for report
                rel_file_path = os.path.relpath(filename, ROOT_DIR)
                
                # Extract country codes from record
                country_codes = extract_country_codes(record)

                # Attach helper metadata for advanced checks
                record["_file_path"] = rel_file_path
                record["_country_codes"] = country_codes
                record["_directory"] = "entities"
                
                # Run all quality checks
                checks = [
                    check_missing_topics,
                    check_missing_tags,
                    check_missing_description,
                    check_missing_langs,
                    check_missing_endpoints,
                    check_software_expected_endpoints,
                    check_owner_info,
                    check_coverage,
                    check_placeholder_values,
                    check_urls,
                    check_required_fields,
                    check_identifiers,
                    check_license_completeness,
                    check_api_status_coherence,
                    check_content_types_access_mode,
                    check_language_validation,
                    check_coverage_normalization,
                    check_software_normalization,
                    check_catalog_software_coherence,
                    check_tag_topic_hygiene,
                    check_description_quality,
                    check_uid_id_consistency,
                    check_contact_info,
                    check_status_directory_uid_consistency,
                    check_status_api_status_coherence_extended,
                    check_title_quality,
                    check_owner_coverage_coherence,
                    check_rights_completeness,
                ]
                
                for check_func in checks:
                    result = check_func(record)
                    if result:
                        primary_country = country_codes[0] if country_codes else "UNKNOWN"
                        
                        if isinstance(result, list):
                            for issue in result:
                                issue["file_path"] = rel_file_path
                                issue["record_id"] = record_id
                                issue["priority"] = get_priority_level(issue["issue_type"])
                                issue["country_code"] = primary_country
                                
                                # Add to all_issues once (for full/priority reports) - use primary country
                                all_issues.append(issue)
                                
                                # Add unique issue to unique_record_issues (for primary priority report)
                                unique_record_issues.append(issue)
                                
                                # Add to record_issues with all country codes (for country reports)
                                for country_code in country_codes:
                                    issue_copy = issue.copy()
                                    issue_copy["country_code"] = country_code
                                    record_issues.append(issue_copy)
                        else:
                            result["file_path"] = rel_file_path
                            result["record_id"] = record_id
                            result["priority"] = get_priority_level(result["issue_type"])
                            result["country_code"] = primary_country
                            
                            # Add to all_issues once (for full/priority reports) - use primary country
                            all_issues.append(result)
                            
                            # Add unique issue to unique_record_issues (for primary priority report)
                            unique_record_issues.append(result)
                            
                            # Add to record_issues with all country codes (for country reports)
                            for country_code in country_codes:
                                result_copy = result.copy()
                                result_copy["country_code"] = country_code
                                record_issues.append(result_copy)
                
                if record_issues:
                    # Store record info with primary country code and all country codes
                    primary_country = country_codes[0] if country_codes else "UNKNOWN"
                    records_with_issues[record_id] = {
                        "file_path": rel_file_path,
                        "issues": record_issues,  # This contains issues with all country codes
                        "unique_issues": unique_record_issues,  # Unique issues per record
                        "country_code": primary_country,
                        "all_country_codes": country_codes,  # Store all countries for this record
                    }

                # Collect metadata for cross-record duplicate detection
                link = record.get("link")
                records_metadata.append(
                    {
                        "record_id": record_id,
                        "file_path": rel_file_path,
                        "country_codes": country_codes if country_codes else ["UNKNOWN"],
                        "link": link,
                    }
                )
                    
            except yaml.YAMLError as e:
                logger.warning(f"YAML parsing error in {filename}: {str(e)}")
            except Exception as e:
                logger.warning(f"Error processing {filename}: {str(e)}")

    # Detect duplicate links across records
    link_to_records = {}

    for meta in records_metadata:
        link = meta.get("link")
        if link:
            link_to_records.setdefault(link, []).append(meta)

    def _ensure_record_entry(record_id, file_path, country_codes):
        primary_country = country_codes[0] if country_codes else "UNKNOWN"
        if record_id not in records_with_issues:
            records_with_issues[record_id] = {
                "file_path": file_path,
                "issues": [],
                "unique_issues": [],
                "country_code": primary_country,
                "all_country_codes": country_codes if country_codes else [primary_country],
            }
        return records_with_issues[record_id]

    # Duplicate links
    for link, metas in link_to_records.items():
        if not link or len(metas) <= 1:
            continue

        record_ids_for_link = [m["record_id"] for m in metas]

        for meta in metas:
            record_id = meta["record_id"]
            file_path = meta["file_path"]
            country_codes = meta["country_codes"] or ["UNKNOWN"]
            primary_country = country_codes[0]

            base_issue = {
                "issue_type": "DUPLICATE_LINK",
                "field": "link",
                "current_value": {
                    "link": link,
                    "record_ids": record_ids_for_link,
                },
                "suggested_action": (
                    "Multiple records share the same portal link; review them and "
                    "deduplicate or clarify distinct roles."
                ),
                "file_path": file_path,
                "record_id": record_id,
                "priority": get_priority_level("DUPLICATE_LINK"),
                "country_code": primary_country,
            }

            all_issues.append(base_issue)
            entry = _ensure_record_entry(record_id, file_path, country_codes)
            entry["unique_issues"].append(base_issue)

            for country_code in entry.get("all_country_codes", country_codes):
                issue_copy = base_issue.copy()
                issue_copy["country_code"] = country_code
                entry["issues"].append(issue_copy)

    # Group issues by country (for country reports - include issues for all countries a record spans)
    issues_by_country = {}
    records_by_country = {}
    
    # Process records_with_issues to get all country-specific issues
    for record_id, record_data in records_with_issues.items():
        # Get all country codes this record spans
        record_country_codes = record_data.get("all_country_codes", [record_data.get("country_code", "UNKNOWN")])
        
        # For each country, add the record's issues
        for country_code in record_country_codes:
            if country_code not in records_by_country:
                records_by_country[country_code] = {}
                issues_by_country[country_code] = []
            
            # Create a country-specific record entry
            country_record_data = {
                "file_path": record_data["file_path"],
                "issues": [issue for issue in record_data["issues"] if issue.get("country_code") == country_code],
                "country_code": country_code,
            }
            records_by_country[country_code][record_id] = country_record_data
            
            # Add issues for this country
            for issue in record_data["issues"]:
                if issue.get("country_code") == country_code:
                    issues_by_country[country_code].append(issue)
    
    # Group issues by priority
    issues_by_priority = {}
    for issue in all_issues:
        priority = issue.get("priority", "MEDIUM")
        if priority not in issues_by_priority:
            issues_by_priority[priority] = []
        issues_by_priority[priority].append(issue)

    # Group issues by type (for rule reports)
    issues_by_type = {}
    for issue in all_issues:
        issue_type = issue["issue_type"]
        if issue_type not in issues_by_type:
            issues_by_type[issue_type] = []
        issues_by_type[issue_type].append(issue)
    
    # Generate all reports
    typer.echo("\nGenerating reports...")
    
    # Full report (text)
    full_report_path = os.path.join(output_dir, "full_report.txt")
    generate_full_report(all_issues, records_with_issues, total_records, full_report_path)
    typer.echo(f"  Full report (text): {full_report_path}")
    
    # Full report (JSONL) - structured output for automated processing
    full_report_jsonl_path = os.path.join(output_dir, "full_report.jsonl")
    jsonl_count = 0
    with open(full_report_jsonl_path, "w", encoding="utf8") as f:
        for issue in all_issues:
            try:
                # Ensure all values are JSON-serializable
                json_issue = {
                    "issue_type": issue.get("issue_type"),
                    "field": issue.get("field"),
                    "current_value": issue.get("current_value"),
                    "suggested_action": issue.get("suggested_action"),
                    "file_path": issue.get("file_path"),
                    "record_id": issue.get("record_id"),
                    "priority": issue.get("priority"),
                    "country_code": issue.get("country_code"),
                }
                f.write(json.dumps(json_issue, ensure_ascii=False) + "\n")
                jsonl_count += 1
            except (TypeError, ValueError) as e:
                logger.warning(f"Failed to serialize issue for record {issue.get('record_id', 'unknown')}: {str(e)}")
    typer.echo(f"  Full report (JSONL): {full_report_jsonl_path} ({jsonl_count} issues)")
    
    # Primary priority report (JSONL) - records with most errors (at least 3 issues)
    primary_priority_path = os.path.join(output_dir, "primary_priority.jsonl")
    # Filter records with at least 3 issues, then sort by number of unique issues (descending), then by priority breakdown
    filtered_records = [
        (record_id, record_data)
        for record_id, record_data in records_with_issues.items()
        if len(record_data.get("unique_issues", [])) >= 3
    ]
    sorted_records = sorted(
        filtered_records,
        key=lambda x: (
            len(x[1].get("unique_issues", [])),  # Primary sort: number of unique issues
            sum(1 for issue in x[1].get("unique_issues", []) if issue.get("priority") == "CRITICAL"),  # Secondary: critical issues
            sum(1 for issue in x[1].get("unique_issues", []) if issue.get("priority") == "IMPORTANT"),  # Tertiary: important issues
        ),
        reverse=True
    )
    
    priority_count = 0
    with open(primary_priority_path, "w", encoding="utf8") as f:
        for record_id, record_data in sorted_records:
            try:
                unique_issues = record_data.get("unique_issues", [])
                
                # Count issues by priority
                priority_counts = {}
                for issue in unique_issues:
                    priority = issue.get("priority", "MEDIUM")
                    priority_counts[priority] = priority_counts.get(priority, 0) + 1
                
                # Create structured record entry
                json_record = {
                    "record_id": record_id,
                    "file_path": record_data["file_path"],
                    "country_code": record_data.get("country_code", "UNKNOWN"),
                    "all_country_codes": record_data.get("all_country_codes", []),
                    "total_issues": len(unique_issues),
                    "priority_counts": priority_counts,
                    "issues": [
                        {
                            "issue_type": issue.get("issue_type"),
                            "field": issue.get("field"),
                            "priority": issue.get("priority"),
                            "current_value": issue.get("current_value"),
                            "suggested_action": issue.get("suggested_action"),
                        }
                        for issue in unique_issues
                    ],
                }
                f.write(json.dumps(json_record, ensure_ascii=False) + "\n")
                priority_count += 1
            except (TypeError, ValueError) as e:
                logger.warning(f"Failed to serialize record {record_id} for primary priority report: {str(e)}")
    typer.echo(f"  Primary priority report (JSONL): {primary_priority_path} ({priority_count} records)")
    
    # Country reports
    generate_country_reports(issues_by_country, records_by_country, output_dir)
    country_count = len([c for c in issues_by_country.keys() if issues_by_country[c]])
    typer.echo(f"  Country reports: {country_count} files in {os.path.join(output_dir, 'countries')}")
    
    # Priority reports
    generate_priority_reports(issues_by_priority, output_dir)
    priority_count = len([p for p in issues_by_priority.keys() if issues_by_priority[p]])
    typer.echo(f"  Priority reports: {priority_count} files in {os.path.join(output_dir, 'priorities')}")

    # Rule reports
    generate_rule_reports(issues_by_type, output_dir)
    rule_count = len([r for r in issues_by_type.keys() if issues_by_type[r]])
    typer.echo(f"  Rule reports: {rule_count} files in {os.path.join(output_dir, 'rules')}")
    
    # Summary output
    typer.echo(f"\nAnalysis complete!")
    typer.echo(f"  Total records analyzed: {total_records}")
    typer.echo(f"  Total issues found: {len(all_issues)}")
    typer.echo(f"  Records with issues: {len(records_with_issues)}")
    typer.echo(f"  Countries affected: {country_count}")
    
    # Print summary by priority
    typer.echo("\nIssue summary by priority:")
    for priority in ["CRITICAL", "IMPORTANT", "MEDIUM", "LOW"]:
        if priority in issues_by_priority:
            count = len(issues_by_priority[priority])
            typer.echo(f"  {priority}: {count} issues")
    
    # Print summary by issue type
    issues_by_type = {}
    for issue in all_issues:
        issue_type = issue["issue_type"]
        if issue_type not in issues_by_type:
            issues_by_type[issue_type] = []
        issues_by_type[issue_type].append(issue)
    
    typer.echo("\nIssue summary by type:")
    for issue_type in sorted(issues_by_type.keys()):
        count = len(issues_by_type[issue_type])
        priority = issues_by_type[issue_type][0].get("priority", "MEDIUM") if issues_by_type[issue_type] else "MEDIUM"
        typer.echo(f"  {issue_type} ({priority}): {count}")


@app.command()
def fix():
    """Fix all data quality issues using cursor-agent with detailed change tracking."""
    logger.info("Starting Cursor automation to fix all data quality issues...")
    logger.info("")
    
    # Check if cursor-agent is available
    try:
        subprocess.run(
            ["cursor-agent", "--version"],
            capture_output=True,
            timeout=5
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        logger.error("Error: cursor-agent is not installed or not in PATH")
        logger.error("Install it from: https://docs.cursor.com/tools/cli")
        raise typer.Exit(1)
    
    # Set up paths
    issues_file = os.path.join(_REPO_ROOT, "dataquality", "primary_priority.jsonl")
    
    # Read records
    logger.info(f"Reading issues from {issues_file}...")
    records = read_jsonl_issues(issues_file)
    
    if not records:
        logger.warning("No records with issues found.")
        return
    
    logger.info(f"Found {len(records)} records with issues to process.")
    logger.info("")
    
    # Initialize stats
    stats = SummaryStats()
    stats.total = len(records)
    
    # Process each record
    for index, record in enumerate(records):
        record_stats = process_record(record, stats, index, _REPO_ROOT)
        stats.records.append(record_stats)
        logger.info("")
    
    # Print summary
    logger.info("=" * 60)
    logger.info("Summary Report")
    logger.info("=" * 60)
    logger.info("")
    logger.info(f"Total records processed: {stats.total}")
    logger.info(f"Successfully updated: {stats.updated}")
    logger.info(f"No changes detected: {stats.no_change}")
    logger.info(f"Failed: {stats.failed}")
    logger.info("")
    
    # Print change statistics
    if stats.updated > 0:
        logger.info("Change Statistics:")
        logger.info(f"  Total fields added: {stats.total_fields_added}")
        logger.info(f"  Total fields modified: {stats.total_fields_modified}")
        logger.info(f"  Total fields removed: {stats.total_fields_removed}")
        logger.info("")
        
        # Find most commonly changed fields
        field_changes = defaultdict(int)
        for record_stat in stats.records:
            if record_stat.changes_detail:
                for change_type in ["added", "modified", "removed"]:
                    for change in record_stat.changes_detail.get(change_type, []):
                        field_changes[change["path"]] += 1
        
        if field_changes:
            logger.info("Most commonly changed fields:")
            sorted_fields = sorted(field_changes.items(), key=lambda x: x[1], reverse=True)
            for field_path, count in sorted_fields[:10]:
                logger.info(f"  {field_path}: {count} change(s)")
            logger.info("")
        
        # Find records with most changes
        records_by_changes = []
        for record_stat in stats.records:
            if record_stat.changes_detail:
                total_changes = (
                    len(record_stat.changes_detail.get("added", [])) +
                    len(record_stat.changes_detail.get("modified", [])) +
                    len(record_stat.changes_detail.get("removed", []))
                )
                records_by_changes.append((record_stat, total_changes))
        
        if records_by_changes:
            records_by_changes.sort(key=lambda x: x[1], reverse=True)
            logger.info("Records with most changes:")
            for record_stat, change_count in records_by_changes[:5]:
                logger.info(f"  {record_stat.record_id} ({record_stat.file_path}): {change_count} change(s)")
            logger.info("")
    
    # Return exit code based on results
    if stats.failed > 0:
        raise typer.Exit(1)


@app.command()
def country_report():
    """Country report"""
    import duckdb
    from rich.console import Console
    from rich.table import Table

    #    data = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl'))
    ids = (
        duckdb.sql(
            "select distinct(unnest(coverage).location.country.id) as id from '%s';"
            % (os.path.join(DATASETS_DIR, "full.parquet"))
        )
        .df()
        .id.tolist()
    )
    #    ids = duckdb.sql("select distinct(unnest(source.countries).id) as id from '%s' where source.catalog_type != 'Indicators catalog';" % (os.path.join("../../cdi-data/search", 'dateno.parquet'))).df().id.tolist()
    #    print(ids)
    reg_countries = set(ids)
    countries_data = {}
    tlds_data = {}
    #    for row in data:
    #        if 'owner' in row.keys() and 'location' in row['owner'].keys():
    #            if row['owner']['location']['country']['id'] not in reg_countries:
    #                reg_countries.add(row['owner']['location']['country']['id'])
    #        if 'coverage' in row.keys():
    #            for loc in row['coverage']:
    #                if loc['location']['country']['id'] not in reg_countries:
    #                    reg_countries.add(loc['location']['country']['id'])

    f = open(os.path.join(_REPO_ROOT, "data", "reference", "countries.csv"), "r", encoding="utf8")
    reader = csv.DictReader(f)
    for row in reader:
        if row["status"] == "UN member state":
            countries_data[row["alpha2"]] = row
    wb_countries = set(countries_data.keys())
    all_set = wb_countries.difference(reg_countries)
    #    all_set = wb_countries.intersection(reg_countries)
    table = Table(title="Missing countries report")
    table.add_column("Alpha-2", justify="right", style="cyan", no_wrap=True)
    table.add_column("Name", style="magenta")
    table.add_column("Internet TLD", style="magenta")
    n = 0
    for row_id in all_set:
        item = [row_id, countries_data[row_id]["name"], countries_data[row_id]["cctld"]]
        #        item = [row_id, row_id]
        table.add_row(*item)
        n += 1
    table.add_section()
    table.add_row("Total", str(n))
    console = Console()
    console.print(table)
    f = open("countries_report", "w", encoding="utf8")
    writer = csv.writer(f)
    writer.writerow(["code", "name", "tld"])
    for row_id in all_set:
        item = [row_id, countries_data[row_id]["name"], countries_data[row_id]["cctld"]]
        writer.writerow(item)
    f.close()


if __name__ == "__main__":
    app()
