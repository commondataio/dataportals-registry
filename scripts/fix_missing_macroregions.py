#!/usr/bin/env python
"""Script to find and fix records with missing macroregions in coverage"""

import logging
import argparse
import yaml
import os
import csv
from pathlib import Path

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Get script directory and set paths relative to project root
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
ROOT_DIR = PROJECT_ROOT / "data" / "entities"
MACROREGION_FILE = PROJECT_ROOT / "data" / "reference" / "macroregion_countries.tsv"


def load_macroregion_mapping(filepath: Path) -> dict:
    """Load macroregion mapping from TSV file"""
    mapping = {}
    country_names = {}
    with open(str(filepath), "r", encoding="utf8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            country_code = row["alpha2"]
            country_name = row["name"]
            macroregion_code = row["macroregion_code"]
            macroregion_name = row["macroregion_name"]
            mapping[country_code] = {
                "id": macroregion_code,
                "name": macroregion_name
            }
            country_names[country_code] = country_name
    logger.info(f"Loaded {len(mapping)} macroregion mappings")
    return mapping, country_names


def has_missing_macroregion(record: dict) -> bool:
    """Check if record has coverage with missing macroregion"""
    if "coverage" not in record:
        return False
    
    if not isinstance(record["coverage"], list):
        return False
    
    for coverage_item in record["coverage"]:
        if "location" in coverage_item:
            location = coverage_item["location"]
            if "country" in location and "id" in location["country"]:
                # Check if macroregion is missing
                if "macroregion" not in location:
                    return True
                # Also check if macroregion exists but is empty
                if location.get("macroregion") is None:
                    return True
                # Check if macroregion has required fields
                macroregion = location.get("macroregion", {})
                if not macroregion.get("id") or not macroregion.get("name"):
                    return True
    
    return False


def fix_macroregion(record: dict, macroregion_mapping: dict, country_names: dict, filepath: Path = None) -> bool:
    """Add missing macroregion to record's coverage"""
    changed = False
    
    if "coverage" not in record:
        return False
    
    if not isinstance(record["coverage"], list):
        return False
    
    # Special country code mappings
    country_code_mappings = {
        "UK": "GB",  # United Kingdom
    }
    
    for coverage_item in record["coverage"]:
        if "location" in coverage_item:
            location = coverage_item["location"]
            if "country" in location and "id" in location["country"]:
                country_code = location["country"]["id"]
                
                # Try to fix known country code issues
                if country_code in country_code_mappings:
                    country_code = country_code_mappings[country_code]
                    location["country"]["id"] = country_code
                    changed = True
                
                # Try to infer from file path if country is Unknown
                if country_code == "Unknown" and filepath:
                    # Extract country code from path like .../GA/Federal/...
                    path_parts = filepath.parts
                    for i, part in enumerate(path_parts):
                        if part == "entities" and i + 1 < len(path_parts):
                            potential_country = path_parts[i + 1]
                            if len(potential_country) == 2 and potential_country.isupper():
                                if potential_country in macroregion_mapping:
                                    country_code = potential_country
                                    location["country"]["id"] = country_code
                                    location["country"]["name"] = country_names.get(country_code, potential_country)
                                    changed = True
                                    logger.info(f"Inferred country {country_code} from file path for {record.get('id', 'unknown')}")
                                    break
                
                # Check if macroregion is missing or incomplete
                needs_fix = False
                if "macroregion" not in location:
                    needs_fix = True
                elif location.get("macroregion") is None:
                    needs_fix = True
                else:
                    macroregion = location.get("macroregion", {})
                    if not macroregion.get("id") or not macroregion.get("name"):
                        needs_fix = True
                
                if needs_fix and country_code in macroregion_mapping:
                    macroregion_data = macroregion_mapping[country_code]
                    location["macroregion"] = {
                        "id": macroregion_data["id"],
                        "name": macroregion_data["name"]
                    }
                    changed = True
                    logger.debug(f"Added macroregion {macroregion_data['name']} for country {country_code}")
                elif needs_fix:
                    logger.warning(f"No macroregion mapping found for country code: {country_code}")
    
    return changed


def find_missing(dryrun: bool = False):
    """Find all records with missing macroregions in coverage"""
    macroregion_mapping, country_names = load_macroregion_mapping(MACROREGION_FILE)
    
    if not ROOT_DIR.exists():
        logger.error(f"Directory not found: {ROOT_DIR}")
        return
    
    missing_records = []
    total_files = 0
    
    logger.info(f"Scanning {ROOT_DIR} for records with missing macroregions...")
    
    for yaml_file in ROOT_DIR.rglob("*.yaml"):
        total_files += 1
        try:
            with open(yaml_file, "r", encoding="utf8") as f:
                record = yaml.load(f, Loader=Loader)
            
            if record and has_missing_macroregion(record):
                record_id = record.get("id", "unknown")
                country_code = None
                if "coverage" in record and isinstance(record["coverage"], list):
                    for cov in record["coverage"]:
                        if "location" in cov and "country" in cov["location"]:
                            country_code = cov["location"]["country"].get("id")
                            break
                
                missing_records.append({
                    "file": str(yaml_file),
                    "id": record_id,
                    "country": country_code
                })
                
        except Exception as e:
            logger.warning(f"Error reading {yaml_file}: {e}")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Scan complete:")
    logger.info(f"  Total files scanned: {total_files}")
    logger.info(f"  Records with missing macroregions: {len(missing_records)}")
    logger.info(f"{'='*60}\n")
    
    if missing_records:
        logger.info("Records with missing macroregions:")
        for rec in missing_records:
            logger.info(f"  {rec['id']} ({rec['country']}) - {rec['file']}")
    
    if not dryrun and missing_records:
        logger.info(f"\nFixing {len(missing_records)} records...")
        fixed_count = 0
        
        for rec in missing_records:
            try:
                filepath = Path(rec["file"])
                with open(filepath, "r", encoding="utf8") as f:
                    record = yaml.load(f, Loader=Loader)
                
                if fix_macroregion(record, macroregion_mapping, country_names, filepath):
                    with open(filepath, "w", encoding="utf8") as f:
                        yaml.dump(record, f, Dumper=Dumper, allow_unicode=True, sort_keys=False)
                    fixed_count += 1
                    logger.info(f"Fixed: {rec['id']}")
                    
            except Exception as e:
                logger.error(f"Error fixing {rec['file']}: {e}")
        
        logger.info(f"\nFixed {fixed_count} out of {len(missing_records)} records")
    elif not dryrun:
        logger.info("No records to fix!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find and fix records with missing macroregions in coverage")
    parser.add_argument("--dryrun", action="store_true", help="Only report, don't fix")
    args = parser.parse_args()
    
    find_missing(dryrun=args.dryrun)

