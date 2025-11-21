#!/usr/bin/env python
# This script intended to enrich data of catalogs entries

import logging
import typer
import requests
import datetime
import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
import csv
import json
import os
from urllib.parse import urlparse
import shutil
import pprint
from requests.exceptions import ConnectionError, TooManyRedirects
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


ROOT_DIR = "../data/entities"
DATASETS_DIR = "../data/datasets"
SCHEDULED_DIR = "../data/scheduled"

app = typer.Typer()

DATA_NAMES = [
    "data",
    "dati",
    "datos",
    "dados",
    "podatki",
    "datosabiertos",
    "opendata",
    "data",
    "dados abertos",
    "daten",
    "offendaten",
]
GOV_NAMES = ["gov", "gob", "gouv", "egov", "e-gov", "go", "govt"]


def load_csv_dict(filepath, key, delimiter="\t"):
    data = {}
    f = open(filepath, "r", encoding="utf8")
    reader = csv.DictReader(f, delimiter=delimiter)
    for r in reader:
        data[r[key]] = r
    f.close()
    return data


def load_jsonl_dict(filepath, key):
    data = {}
    f = open(filepath, "r", encoding="utf8")
    for line in f:
        r = json.loads(line)
        data[r[key]] = r
    f.close()
    return data


@app.command()
def enrich_ot(dryrun=False):
    """Update owner type according to name"""
    dirs = os.listdir(ROOT_DIR)
    for adir in dirs:
        subdirs = os.listdir(os.path.join(ROOT_DIR, adir))
        for subdir in subdirs:
            files = os.listdir(os.path.join(ROOT_DIR, adir, subdir))
            for filename in files:
                filepath = os.path.join(ROOT_DIR, adir, subdir, filename)
                if os.path.isdir(filepath):
                    continue
                changed = False
                f = open(filepath, "r", encoding="utf8")
                data = yaml.load(f, Loader=Loader)
                f.close()
                if "owner_type" in data.keys() and data["owner_type"] == "Government":
                    parts = urlparse(data["link"])
                    netloc_parts = parts.netloc.lower().split(".")
                    if len(netloc_parts) > 2:
                        if (
                            netloc_parts[-2] in GOV_NAMES
                            and netloc_parts[-3] in DATA_NAMES
                        ):
                            data["owner_type"] = "Central government"
                            changed = True
                if changed:
                    if dryrun is True:
                        logger.info("Dryrun: should be updated %s", filename)
                    else:
                        f = open(filepath, "w", encoding="utf8")
                        f.write(yaml.safe_dump(data, allow_unicode=True))
                        f.close()
                        logger.info("updated %s", filename)


def __topic_find(topics, id, topic_type="eudatatheme"):
    for t in topics:
        if topic_type == topic_type and id == t["id"]:
            return True
    return False


@app.command()
def enrich_topics(dryrun=False):
    """Set topics tags from catalog metadata to be improved later"""
    dirs = os.listdir(ROOT_DIR)
    for adir in dirs:
        subdirs = os.listdir(os.path.join(ROOT_DIR, adir))
        for subdir in subdirs:
            files = os.listdir(os.path.join(ROOT_DIR, adir, subdir))
            for filename in files:
                filepath = os.path.join(ROOT_DIR, adir, subdir, filename)
                if os.path.isdir(filepath):
                    continue
                changed = False
                f = open(filepath, "r", encoding="utf8")
                data = yaml.load(f, Loader=Loader)
            f.close()
            if data is None:
                logger.error("error on %s", filename)
                break
                if "tags" in data.keys() and data["tags"] is not None:
                    tags = set(data["tags"])
                else:
                    tags = set([])
                if "topics" in data.keys():
                    topics = data["topics"]
                else:
                    data["topics"] = []
                    topics = []

                if data["catalog_type"] == "Indicators catalog":
                    tags.add("statistics")
                elif data["catalog_type"] == "Microdata catalog":
                    if "properties" not in data:
                        data["properties"] = {}
                    data["properties"]["transferable_topics"] = True
                    found = __topic_find(topics, "SOCI", topic_type="eudatatheme")
                    if not found:
                        topics.append(
                            {
                                "id": "SOCI",
                                "name": "Population and society",
                                "type": "eudatatheme",
                            }
                        )
                    found = __topic_find(topics, "Society", topic_type="iso19115")
                    if not found:
                        topics.append(
                            {"id": "Society", "name": "Society", "type": "iso19115"}
                        )
                    tags.add("microdata")
                elif data["catalog_type"] == "Geoportal":
                    tags.add("geospatial")
                elif data["catalog_type"] == "Scientific data repository":
                    if "properties" not in data:
                        data["properties"] = {}
                    data["properties"]["tranferable_topics"] = True
                    found = __topic_find(topics, "TECH", topic_type="eudatatheme")
                    if not found:
                        logger.debug(
                            "Added %s",
                            {
                                "id": "TECH",
                                "name": "Science and technology",
                                "type": "eudatatheme",
                            },
                        )
                        topics.append(
                            {
                                "id": "TECH",
                                "name": "Science and technology",
                                "type": "eudatatheme",
                            }
                        )
                    tags.add("scientific")
                if "owner" in data.keys():
                    if data["owner"]["type"] in [
                        "Regional government",
                        "Local government",
                        "Central government",
                    ]:
                        tags.add("government")
                    if data["owner"]["type"] in [
                        "Regional government",
                        "Local government",
                    ]:
                        logger.debug("transferable4")
                        if "properties" not in data:
                            data["properties"] = {}
                        data["properties"]["transferable_location"] = True
                if "api" in data.keys() and data["api"] is True:
                    tags.add("has_api")
                tags = list(tags)
                if "tags" in data.keys() and tags != data["tags"]:
                    changed = True
                data["topics"] = topics
                changed = True
                logger.debug("Data: %s", data)
                if changed:
                    if dryrun is True:
                        logger.info("Dryrun: should be updated %s", filename)
                    else:
                        f = open(filepath, "w", encoding="utf8")
                        f.write(yaml.safe_dump(data, allow_unicode=True))
                        f.close()
                        logger.info("updated %s", filename)


#                        print(data)


@app.command()
def enrich_countries(dryrun=False):
    """Update countries with codes"""
    dirs = os.listdir(ROOT_DIR)
    for adir in dirs:
        subdirs = os.listdir(os.path.join(ROOT_DIR, adir))
        for subdir in subdirs:
            files = os.listdir(os.path.join(ROOT_DIR, adir, subdir))
            for filename in files:
                filepath = os.path.join(ROOT_DIR, adir, subdir, filename)
                if os.path.isdir(filepath):
                    continue
                changed = False
                f = open(filepath, "r", encoding="utf8")
                data = yaml.load(f, Loader=Loader)
                f.close()
                if "countries" in data.keys():
                    if isinstance(data["countries"][0], str):
                        name = data["countries"][0]
                    elif isinstance(data["countries"][0]["name"], str):
                        name = data["countries"][0]["name"]
                    else:
                        name = data["countries"][0]["name"]["name"]
                    logger.debug("Country name: %s", name)
                    data["countries"] = [{"id": adir, "name": name}]

                    changed = True
                if changed:
                    if dryrun is True:
                        logger.info("Dryrun: should be updated %s", filename)
                    else:
                        f = open(filepath, "w", encoding="utf8")
                        f.write(yaml.safe_dump(data, allow_unicode=True))
                        f.close()
                        logger.info("updated %s", filename)


headers = {"Accept": "application/json"}


@app.command()
def setstatus(updatedata=False):
    """Checks every site existence and endpoints availability except Geonetwork yet"""
    session = requests.Session()
    session.max_redirects = 100
    results = []
    out = open("statusreport.jsonl", "w", encoding="utf8")
    dirs = os.listdir(ROOT_DIR)
    for adir in dirs:
        subdirs = os.listdir(os.path.join(ROOT_DIR, adir))
        for subdir in subdirs:
            files = os.listdir(os.path.join(ROOT_DIR, adir, subdir))
            for filename in files:
                filepath = os.path.join(ROOT_DIR, adir, subdir, filename)
                if os.path.isdir(filepath):
                    continue
                changed = False
                f = open(filepath, "r", encoding="utf8")
                item = yaml.load(f, Loader=Loader)
            f.close()
            if "status" in item.keys():
                continue
            logger.info("Checking status for %s", item["link"])
            report = {
                "id": item["id"],
                "name": item["name"] if "name" in item.keys() else "",
                "link": item["link"],
                "date_verified": datetime.datetime.now().isoformat(),
                "software": item["software"] if "software" in item.keys() else "",
            }
            report["api_status"] = "uncertain"
            report["api_http_code"] = ""
            report["api_content_type"] = ""
            try:
                response = session.head(
                    item["link"], verify=False, allow_redirects=True
                )
                report["link_http_code"] = response.status_code
                if not response.ok:
                    report["status"] = "deprecated"
                    report["api_status"] = "deprecated"
                else:
                    report["status"] = "active"
            except ConnectionError as e:
                report["status"] = "deprecated"
                report["api_status"] = "deprecated"
                report["link_http_code"] = ""
                item["status"] = report["status"]
                item["api_status"] = report["api_status"]
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(item, allow_unicode=True))
                f.close()
                logger.info("updated %s", filename)
                continue
            except ReadTimeout as e:
                report["status"] = "deprecated"
                report["api_status"] = "deprecated"
                report["link_http_code"] = ""
                item["status"] = report["status"]
                item["api_status"] = report["api_status"]
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(item, allow_unicode=True))
                f.close()
                logger.info("updated %s", filename)
                continue
            except TooManyRedirects as e:
                report["status"] = "deprecated"
                report["api_status"] = "deprecated"
                report["link_http_code"] = ""
                item["status"] = report["status"]
                item["api_status"] = report["api_status"]
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(item, allow_unicode=True))
                f.close()
                logger.info("updated %s", filename)
                continue

            if "software" in item.keys():
                if item["software"] == "CKAN":
                    if "endpoints" in item.keys() and len(item["endpoints"]) > 0:
                        if item["endpoints"][0]["type"] == "ckanapi":
                            search_endpoint = (
                                item["endpoints"][0]["url"] + "/action/package_search"
                            )
                            noerror = True
                            try:
                                response = session.get(
                                    search_endpoint, headers=headers, verify=False
                                )
                            except:
                                report["api_status"] = "deprecated"
                                noerror = False
                            if noerror:
                                report["api_http_code"] = response.status_code
                                report["api_content_type"] = (
                                    response.headers["content-type"]
                                    if "content-type" in response.headers.keys()
                                    else ""
                                )
                                if not response.ok:
                                    report["api_status"] = "deprecated"
                                else:
                                    if (
                                        "content-type" in response.headers.keys()
                                        and response.headers["content-type"]
                                        .split(";", 1)[0]
                                        .lower()
                                        == "application/json"
                                    ):
                                        report["api_status"] = "active"
                elif item["software"] == "NADA":
                    if "endpoints" in item.keys() and len(item["endpoints"]) > 0:
                        if item["endpoints"][0]["type"] == "nada:catalog-search":
                            search_endpoint = item["endpoints"][0]["url"]
                            response = session.get(
                                search_endpoint, headers=headers, verify=False
                            )
                            report["api_http_code"] = response.status_code
                            report["api_content_type"] = (
                                response.headers["content-type"]
                                if "content-type" in response.headers.keys()
                                else ""
                            )
                            if not response.ok:
                                report["api_status"] = "deprecated"
                            else:
                                if (
                                    response.headers["content-type"]
                                    .split(";", 1)[0]
                                    .lower()
                                    == "application/json"
                                ):
                                    report["api_status"] = "active"
                elif item["software"] == "Dataverse":
                    if "endpoints" in item.keys() and len(item["endpoints"]) > 0:
                        if item["endpoints"][0]["type"] == "dataverseapi":
                            search_endpoint = item["endpoints"][0]["url"]
                            response = session.get(
                                search_endpoint, headers=headers, verify=False
                            )
                            report["api_http_code"] = response.status_code
                            report["api_content_type"] = (
                                response.headers["content-type"]
                                if "content-type" in response.headers.keys()
                                else ""
                            )
                            if not response.ok:
                                report["api_status"] = "deprecated"
                            else:
                                if (
                                    response.headers["content-type"]
                                    .split(";", 1)[0]
                                    .lower()
                                    == "application/json"
                                ):
                                    report["api_status"] = "active"
                elif item["software"] == "ArcGIS Hub":
                    if "endpoints" in item.keys() and len(item["endpoints"]) > 0:
                        endpoints = {item["type"]: item for item in item["endpoints"]}
                        if "dcatap201" in endpoints.keys():
                            search_endpoint = endpoints["dcatap201"]["url"]
                            response = session.get(
                                search_endpoint, headers=headers, verify=False
                            )
                            report["api_http_code"] = response.status_code
                            report["api_content_type"] = (
                                response.headers["content-type"]
                                if "content-type" in response.headers.keys()
                                else ""
                            )
                            if not response.ok:
                                report["api_status"] = "deprecated"
                            else:
                                if (
                                    "content-type" in response.headers.keys()
                                    and response.headers["content-type"]
                                    .split(";", 1)[0]
                                    .lower()
                                    == "application/json"
                                ):
                                    test_resp = json.loads(response.text)
                                    if "error" not in test_resp.keys():
                                        report["api_status"] = "active"
                            report["status"] = report["api_status"]
                elif item["software"] == "InvenioRDM":
                    if "endpoints" in item.keys() and len(item["endpoints"]) > 0:
                        endpoints = {item["type"]: item for item in item["endpoints"]}
                        if "inveniordmapi:records" in endpoints.keys():
                            search_endpoint = endpoints["inveniordmapi:records"]["url"]
                            response = session.get(
                                search_endpoint, headers=headers, verify=False
                            )
                            report["api_http_code"] = response.status_code
                            report["api_content_type"] = (
                                response.headers["content-type"]
                                if "content-type" in response.headers.keys()
                                else ""
                            )
                            if not response.ok:
                                report["api_status"] = "deprecated"
                            else:
                                if (
                                    "content-type" in response.headers.keys()
                                    and response.headers["content-type"]
                                    .split(";", 1)[0]
                                    .lower()
                                    == "application/json"
                                ):
                                    report["api_status"] = "active"
                elif item["software"] == "uData":
                    search_endpoint = item["link"] + "/api/1/datasets/"
                    response = session.get(
                        search_endpoint, headers=headers, verify=False
                    )
                    report["api_http_code"] = response.status_code
                    report["api_content_type"] = response.headers["content-type"]
                    if not response.ok:
                        report["api_status"] = "deprecated"
                    else:
                        if (
                            "content-type" in response.headers.keys()
                            and response.headers["content-type"]
                            .split(";", 1)[0]
                            .lower()
                            == "application/json"
                        ):
                            report["api_status"] = "active"
            item["status"] = report["status"]
            item["api_status"] = report["api_status"]
            f = open(filepath, "w", encoding="utf8")
            f.write(yaml.safe_dump(item, allow_unicode=True))
            f.close()
            logger.info("updated %s", filename)


#        logger.debug(report)
#        out.write(json.dumps(report) + '\n')


@app.command()
def enrich_from_csv(filename="catalogs_enrich.csv "):
    """Build datasets as JSONL from entities as YAML"""
    profiles = {}
    reader = csv.DictReader(open(filename, "r", encoding="utf8"), delimiter="\t")
    for row in reader:
        profiles[row["id"]] = row

    dirs = os.listdir(ROOT_DIR)
    for root, dirs, files in os.walk(ROOT_DIR):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            logger.info("Processing %s", os.path.basename(filename).split(".", 1)[0])
            filepath = filename
            f = open(filepath, "r", encoding="utf8")
            record = yaml.load(f, Loader=Loader)
            f.close()
            if record["id"] in profiles.keys():
                for key in [
                    "status",
                    "name",
                    "owner_type",
                    "owner_name",
                    "api_status",
                    "catalog_type",
                    "software",
                ]:
                    record[key] = profiles[record["id"]][key]
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                logger.info("- saved")


@app.command()
def enrich_location(dryrun=False):
    """Enrich location codes"""
    dirs = os.listdir(ROOT_DIR)

    dirs = os.listdir(ROOT_DIR)
    for root, dirs, files in os.walk(ROOT_DIR):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            logger.info("Processing %s", os.path.basename(filename).split(".", 1)[0])
            filepath = filename
            f = open(filepath, "r", encoding="utf8")
            record = yaml.load(f, Loader=Loader)
            f.close()
            # Create owner record
            owner = {
                "name": record["owner_name"],
                "type": record["owner_type"],
                "location": {"level": 20, "country": record["countries"][0].copy()},
            }
            if owner["location"]["country"]["id"] == "UK":
                owner["location"]["country"]["id"] = "GB"
            if "owner_link" in record.keys():
                owner["link"] = record["owner_link"]
            coverage = []
            for country in record["countries"]:
                country_id = country["id"]
                if country_id == "UK":
                    country_id = "GB"
                coverage.append(
                    {
                        "location": {
                            "level": 20,
                            "country": {"id": country_id, "name": country["name"]},
                        }
                    }
                )
            for key in ["countries", "owner_name", "owner_type", "owner_link"]:
                if key in record.keys():
                    del record[key]
            parent_path = root.rsplit("\\", 2)[-2]
            if parent_path.find("-") > -1:
                owner["location"]["level"] = 30
                owner["location"]["subregion"] = {"id": parent_path}
                coverage[0]["location"]["level"] = 30
                coverage[0]["location"]["subregion"] = {"id": parent_path}
            record["coverage"] = coverage
            record["owner"] = owner
            #            if owner['location']['level'] == 2:
            #              print(yaml.safe_dump(record, allow_unicode=True))
            f = open(filepath, "w", encoding="utf8")
            f.write(yaml.safe_dump(record, allow_unicode=True))
            f.close()


@app.command()
def enrich_identifiers(filepath, idtype, dryrun=False, mode="entities"):
    """Enrich identifiers"""
    root_dir = ROOT_DIR if mode == "entities" else SCHEDULED_DIR
    f = open(filepath, "r", encoding="utf8")
    reader = csv.DictReader(f, delimiter="\t")
    reg_map = {}
    for row in reader:
        if row["registry_uid"] not in reg_map.keys():
            reg_map[row["registry_uid"]] = [
                {
                    "id": idtype,
                    "url": row["dataportals_url"],
                    "value": row["dataportals_name"],
                }
            ]
        else:
            reg_map[row["registry_uid"]].append(
                {
                    "id": idtype,
                    "url": row["dataportals_url"],
                    "value": row["dataportals_name"],
                }
            )

    dirs = os.listdir(root_dir)
    for root, dirs, files in os.walk(root_dir):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            #            logger.info('Processing %s', os.path.basename(filename).split('.', 1)[0])
            filepath = filename
            f = open(filepath, "r", encoding="utf8")
            record = yaml.load(f, Loader=Loader)
            f.close()
            changed = False

            if "identifiers" not in record.keys():
                if record["uid"] in reg_map.keys():
                    record["identifiers"] = reg_map[record["uid"]]
                    changed = True
            else:
                if record["uid"] in reg_map.keys():
                    # Clean up
                    for n in range(0, len(record["identifiers"])):
                        if record["identifiers"][n]["id"] == idtype:
                            del record["identifiers"][n]
                            changed = True
                            break
                    ids = []
                    for item in record["identifiers"]:
                        ids.append(item["url"])
                    for item in reg_map[record["uid"]]:
                        if item["url"] not in ids:
                            record["identifiers"].append(item)
                            changed = True

            if changed:
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                logger.info("Updated %s", os.path.basename(filename).split(".", 1)[0])


REGIONS_DATA_THEME = {"id": "REGI", "name": "Regions and cities", "type": "eudatatheme"}


@app.command()
def enrich_regions_topics(dryrun=False):
    """Enrich regions topics"""
    dirs = os.listdir(ROOT_DIR)
    for root, dirs, files in os.walk(ROOT_DIR):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            #            logger.info('Processing %s', os.path.basename(filename).split('.', 1)[0])
            filepath = filename
            f = open(filepath, "r", encoding="utf8")
            record = yaml.load(f, Loader=Loader)
            f.close()
            changed = False
            if record["owner"]["type"] not in [
                "Local government",
                "Regional government",
            ]:
                continue
            if "topics" not in record.keys():
                record["topics"] = [REGIONS_DATA_THEME]
                changed = True
            else:
                not_exists = True
                for topic in record["topics"]:
                    if topic["id"] == "REGI":
                        not_exists = False
                        break
                if not_exists:
                    record["topics"].append(REGIONS_DATA_THEME)
                    changed = True
            if changed:
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                logger.info("Updated %s", os.path.basename(filename).split(".", 1)[0])


TECH_DATA_THEME = {
    "id": "TECH",
    "name": "Science and technology",
    "type": "eudatatheme",
}


@app.command()
def enrich_scientific(dryrun=False):
    """Enrich scientific repositories"""
    dirs = os.listdir(ROOT_DIR)
    for root, dirs, files in os.walk(ROOT_DIR):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            #            logger.info('Processing %s', os.path.basename(filename).split('.', 1)[0])
            filepath = filename
            f = open(filepath, "r", encoding="utf8")
            record = yaml.load(f, Loader=Loader)
            f.close()
            changed = False
            if record["catalog_type"] not in [
                "Scientific data repository",
            ]:
                continue
            if "topics" not in record.keys():
                record["topics"] = [TECH_DATA_THEME]
                changed = True
            else:
                not_exists = True
                for topic in record["topics"]:
                    if topic["id"] == "TECH":
                        not_exists = False
                        break
                if not_exists:
                    record["topics"].append(TECH_DATA_THEME)
                    changed = True
            if changed:
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                logger.info("Updated %s", os.path.basename(filename).split(".", 1)[0])


@app.command()
def enrich_level(dryrun=False, root_dir=SCHEDULED_DIR):
    """Enrich geolevel"""
    dirs = os.listdir(root_dir)
    for root, dirs, files in os.walk(root_dir):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            #            logger.info('Processing %s', os.path.basename(filename).split('.', 1)[0])
            filepath = filename
            f = open(filepath, "r", encoding="utf8")
            record = yaml.load(f, Loader=Loader)
            f.close()
            changed = False
            if "owner" in record.keys() and "location" in record["owner"].keys():
                if "level" in record["owner"]["location"]:
                    record["owner"]["location"]["level"] = 10 * (
                        record["owner"]["location"]["level"] + 1
                    )
                else:
                    record["owner"]["location"]["level"] = (
                        20
                        if record["owner"]["location"]["country"]["id"]
                        not in ["Unknown", "World"]
                        else 10
                    )
                changed = True
            if "coverage" in record.keys():
                locations = []
                for item in record["coverage"]:
                    if "level" in item["location"].keys():
                        item["location"]["level"] = 10 * (item["location"]["level"] + 1)
                    else:
                        item["location"]["level"] = (
                            20
                            if item["location"]["country"]["id"]
                            not in ["Unknown", "World"]
                            else 10
                        )
                    locations.append(item)
                    if "macroregion" not in item["location"].keys():
                        if item["location"]["country"]["id"] in ["Unknown", "World"]:
                            item["location"]["macroregion"] = {
                                "id": "World",
                                "name": "World",
                            }
                    changed = True
                if changed:
                    record["coverage"] = locations
            if changed:
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                logger.info(
                    "Updated %s level %d",
                    os.path.basename(filename).split(".", 1)[0],
                    record["owner"]["location"]["level"],
                )


@app.command()
def enrich_countries_py(dryrun=False):
    """Enrich countries with pycountry values"""
    from pycountry import countries

    dirs = os.listdir(ROOT_DIR)
    ids = []
    for root, dirs, files in os.walk(ROOT_DIR):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            #            logger.info('Processing %s', os.path.basename(filename).split('.', 1)[0])
            filepath = filename
            f = open(filepath, "r", encoding="utf8")
            record = yaml.load(f, Loader=Loader)
            f.close()
            changed = False
            if "owner" in record.keys() and "location" in record["owner"].keys():
                if "country" in record["owner"]["location"]:
                    c_id = record["owner"]["location"]["country"]["id"]
                    c_name = record["owner"]["location"]["country"]["name"]
                    country = countries.get(alpha_2=c_id)
                    if c_id in ids:
                        continue
                    if not country:
                        logger.warning("Country id %s name %s not found", c_id, c_name)
                    elif c_name != country.name:
                        logger.warning("Country name %s != %s", c_name, country.name)
                    if c_id not in ids:
                        ids.append(c_id)
            #                        print(country)
            #                changed = True
            if "coverage" in record.keys():
                locations = []
                for item in record["coverage"]:
                    if "country" in item["location"].keys():
                        c_id = item["location"]["country"]["id"]
                        c_name = item["location"]["country"]["name"]
                    locations.append(item)
                #                    if 'macroregion' not in item['location'].keys():
                #                        if item['location']['country']['id'] in ['Unknown', 'World']:
                #                            item['location']['macroregion'] = {'id' : 'World', 'name' : 'World'}
                #                    changed = True
                if changed:
                    record["coverage"] = locations
            if changed:
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                logger.info(
                    "Updated %s level %d",
                    os.path.basename(filename).split(".", 1)[0],
                    record["owner"]["location"]["level"],
                )


@app.command()
def fix_api(dryrun=False, mode="entities"):
    """Fix API"""
    root_dir = ROOT_DIR if mode == "entities" else SCHEDULED_DIR
    dirs = os.listdir(root_dir)
    for root, dirs, files in os.walk(root_dir):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            #            logger.info('Processing %s', os.path.basename(filename).split('.', 1)[0])
            filepath = filename
            f = open(filepath, "r", encoding="utf8")
            record = yaml.load(f, Loader=Loader)
            f.close()
            changed = False
            if record["software"]["id"] == "geonode":
                if "endpoints" in record.keys():
                    endpoints = []
                    for endp in record["endpoints"]:
                        logger.debug("Endpoint: %s", endp)
                        if endp["type"] == "dcatus11":
                            endp["type"] = "geonode:dcatus11"
                            logger.info("Fixed endpoint")
                            changed = True
                        endpoints.append(endp)
            if "endpoints" in record.keys():
                endpoints = []
                for endp in record["endpoints"]:
                    if endp["type"] in [
                        "wfs",
                        "wcs",
                        "tms",
                        "wms-c",
                        "wms",
                        "csw",
                        "wmts",
                        "wps",
                        "oaipmh",
                    ]:
                        if "version" in endp.keys():
                            endp["type"] = endp["type"] + endp["version"].replace(
                                ".", ""
                            )
                            logger.info("Fixed endpoint")
                            changed = True
                    if endp["type"] == "ckanapi":
                        endp["type"] = "ckan"
                        logger.info("Fixed endpoint")
                        changed = True
                    if endp["type"] == "geonetworkapi":
                        endp["type"] = "geonetwork"
                        logger.info("Fixed endpoint")
                        changed = True
                    if endp["type"] == "geonetworkapi:query":
                        endp["type"] = "geonetwork:query"
                        logger.info("Fixed endpoint")
                        changed = True
                    if endp["type"] == "opendatasoft":
                        endp["type"] = "opendatasoftapi"
                        logger.info("Fixed endpoint")
                        changed = True
                    if endp["type"] == "arcgisrest":
                        endp["type"] = "arcgis:rest:services"
                        endp["url"] = endp["url"] + "?f=pjson"
                        logger.info("Fixed endpoint")
                        changed = True
                    endpoints.append(endp)
            else:
                if record["software"]["id"] == "arcgisserver":
                    endpoints = []
                    endp = {}
                    endp["type"] = "arcgis:rest:services"
                    endp["url"] = record["link"] + "?f=pjson"
                    endp["version"] = None
                    logger.info("Fixed endpoint")
                    changed = True
                    endpoints.append(endp)
            if changed is True:
                record["endpoints"] = endpoints
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                logger.info("Updated %s", os.path.basename(filename).split(".", 1)[0])


@app.command()
def fix_catalog_type(dryrun=False, mode="entities"):
    """Fix catalog_type"""
    root_dir = ROOT_DIR if mode == "entities" else SCHEDULED_DIR
    dirs = os.listdir(root_dir)
    for root, dirs, files in os.walk(root_dir):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            #            logger.info('Processing %s', os.path.basename(filename).split('.', 1)[0])
            filepath = filename
            f = open(filepath, "r", encoding="utf8")
            record = yaml.load(f, Loader=Loader)
            f.close()
            changed = False
            if record["catalog_type"] == "Unknown":
                record["catalog_type"] = "Open data portal"
                changed = True
            if changed is True:
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                logger.info("Updated %s", os.path.basename(filename).split(".", 1)[0])


@app.command()
def update_macroregions(dryrun=False, mode="entities"):
    """Update macro regions"""
    macro_dict = load_csv_dict(
        "../data/reference/macroregion_countries.tsv", delimiter="\t", key="alpha2"
    )
    root_dir = ROOT_DIR if mode == "entities" else SCHEDULED_DIR
    dirs = os.listdir(root_dir)
    for root, dirs, files in os.walk(root_dir):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            #            logger.info('Processing %s', os.path.basename(filename).split('.', 1)[0])
            filepath = filename
            f = open(filepath, "r", encoding="utf8")
            record = yaml.load(f, Loader=Loader)
            f.close()
            changed = False
            country_ids = []
            #            country_ids.append(record['owner']['location']['country']['id'])
            n = 0
            for location in record["coverage"]:
                cid = location["location"]["country"]["id"]
                if cid not in macro_dict.keys():
                    logger.warning("Not found country %s", cid)
                else:
                    location["location"]["macroregion"] = {
                        "id": macro_dict[cid]["macroregion_code"],
                        "name": macro_dict[cid]["macroregion_name"],
                    }
                    record["coverage"][n] = location
                    changed = True
                n += 1
            if changed is True:
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                logger.info("Updated %s", os.path.basename(filename).split(".", 1)[0])


@app.command()
def update_languages(dryrun=False, mode="entities"):
    """Update languages schema and codes"""
    lang_dict = load_csv_dict("../data/reference/langs.tsv", delimiter="\t", key="code")
    country_lang_dict = load_csv_dict(
        "../data/reference/country_langs.tsv", delimiter="\t", key="alpha2"
    )
    root_dir = ROOT_DIR if mode == "entities" else SCHEDULED_DIR
    dirs = os.listdir(root_dir)
    for root, dirs, files in os.walk(root_dir):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            filepath = filename
            f = open(filepath, "r", encoding="utf8")
            record = yaml.load(f, Loader=Loader)
            f.close()
            changed = False
            cid = record["owner"]["location"]["country"]["id"]
            langs = []
            new_langs = []
            if "langs" not in record.keys():
                if cid in country_lang_dict.keys():
                    langs.append(country_lang_dict[cid]["langcode"])
            else:
                if isinstance(record["langs"], dict):
                    continue
                if len(record["langs"]) == 0:
                    if cid in country_lang_dict.keys():
                        langs.append(country_lang_dict[cid]["langcode"])
                else:
                    langs = record["langs"]
            for code in langs:
                if code not in lang_dict.keys():
                    logger.warning("Not found language with code: %s", code)
                    logger.warning("Record ID: %s", record["id"])
                else:
                    new_langs.append({"id": code, "name": lang_dict[code]["name"]})
                    record["langs"] = new_langs
                    changed = True
            n = 0
            if changed is True:
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                logger.info("Updated %s", os.path.basename(filename).split(".", 1)[0])


@app.command()
def update_subregions(dryrun=False, mode="entities"):
    """Update sub regions names"""
    data_dict = load_csv_dict(
        "../data/reference/subregions/IP2LOCATION-ISO3166-2.CSV ",
        delimiter=",",
        key="code",
    )
    root_dir = ROOT_DIR if mode == "entities" else SCHEDULED_DIR
    dirs = os.listdir(root_dir)
    for root, dirs, files in os.walk(root_dir):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            #            logger.info('Processing %s', os.path.basename(filename).split('.', 1)[0])
            filepath = filename
            f = open(filepath, "r", encoding="utf8")
            record = yaml.load(f, Loader=Loader)
            f.close()
            changed = False
            country_ids = []
            #            country_ids.append(record['owner']['location']['country']['id'])
            n = 0
            for location in record["coverage"]:
                if not "level" in location["location"].keys():
                    continue
                if location["location"]["level"] != 3:
                    continue
                if "subregion" in location["location"].keys():
                    if "name" in location["location"]["subregion"].keys():
                        continue
                    sid = location["location"]["subregion"]["id"]
                    if sid not in data_dict.keys():
                        logger.warning("Not found coverage subregion %s", sid)
                    else:
                        location["location"]["subregion"]["name"] = data_dict[sid][
                            "subdivision_name"
                        ]
                        record["coverage"][n] = location
                        changed = True
                n += 1
            if "owner" in record.keys():
                if "location" in record["owner"].keys():
                    if (
                        "subregion" in record["owner"]["location"].keys()
                        and "name"
                        not in record["owner"]["location"]["subregion"].keys()
                    ):
                        sid = record["owner"]["location"]["subregion"]["id"]
                        if sid not in data_dict.keys():
                            logger.warning("Not found owner subregion %s", sid)
                        else:
                            record["owner"]["location"]["subregion"]["name"] = (
                                data_dict[sid]["subdivision_name"]
                            )
                            changed = True
            if changed is True:
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                logger.info("Updated %s", os.path.basename(filename).split(".", 1)[0])


@app.command()
def update_terms(dryrun=False, mode="entities"):
    """Update terms"""
    software_dict = {}
    f = open("../data/datasets/software.jsonl", "r", encoding="utf8")
    for l in f:
        record = json.loads(l)
        software_dict[record["id"]] = record
    f.close()
    root_dir = ROOT_DIR if mode == "entities" else SCHEDULED_DIR
    dirs = os.listdir(root_dir)
    for root, dirs, files in os.walk(root_dir):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            #            logger.info('Processing %s', os.path.basename(filename).split('.', 1)[0])
            filepath = filename
            f = open(filepath, "r", encoding="utf8")
            record = yaml.load(f, Loader=Loader)
            f.close()
            changed = False
            if record is None:
                continue
            #            print(record['id'], record['software']['id'])
            if record["software"]["id"] not in software_dict.keys():
                continue
            software = software_dict[record["software"]["id"]]
            lic_type = software["rights_management"]["licensing_type"]
            rights_type = None
            if lic_type == "Global":
                rights_type = "global"
            elif lic_type == "Per dataset":
                rights_type = "granular"
            elif lic_type == "Not applicable":
                rights_type = "inapplicable"
            else:
                rights_type = "unknown"
            rights = {
                "tos_url": None,
                "privacy_policy_url": None,
                "rights_type": rights_type,
                "license_id": None,
                "license_name": None,
                "license_url": None,
            }
            if "tos_url" in software["rights_management"].keys():
                rights["tos_url"] = software["rights_management"]["tos_url"]
            if "privacy_policy_url" in software["rights_management"].keys():
                rights["privacy_policy_url"] = software["rights_management"][
                    "privacy_policy_url"
                ]
            if software["id"] == "opendatasoft":
                rights["tos_url"] = record["link"] + "/terms/terms-and-conditions/"
                rights["privacy_policy_url"] = record["link"] + "/terms/privacy-policy/"
            properties = {}
            if software["pid_support"]["has_doi"] == "No":
                properties["has_doi"] = False
                changed = True
            elif software["pid_support"]["has_doi"] == "Yes":
                properties["has_doi"] = True
                changed = True
            if len(properties.values()) > 0:
                if "properties" in record.keys():
                    record["properties"].update(properties)
                else:
                    record["properties"] = properties
                changed = True
            record["rights"] = rights
            changed = True
            if changed is True:
                f = open(filepath, "w", encoding="utf8")
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                logger.info("Updated %s", os.path.basename(filename).split(".", 1)[0])


@app.command()
def validate_countries(dryrun=False, mode="entities"):
    """Validate and fix owner.location.country and coverage.location.country in all records"""
    from constants import COUNTRIES
    from pycountry import countries

    root_dir = ROOT_DIR if mode == "entities" else SCHEDULED_DIR
    
    # Special country codes that are not ISO 3166-1 alpha-2 but are valid in our system
    SPECIAL_COUNTRIES = {
        "Unknown", "World", "EU", "Africa", "ASEAN", "Caribbean", 
        "LatinAmerica", "Oceania", "AQ"  # AQ is Antarctic in our system
    }
    
    # Country code mappings (normalizations)
    COUNTRY_CODE_MAPPINGS = {
        "UK": "GB",  # United Kingdom
    }
    
    # Build reverse lookup: name -> code for COUNTRIES dict
    countries_by_name = {v.upper(): k for k, v in COUNTRIES.items()}
    
    total_records = 0
    updated_records = 0
    owner_fixes = 0
    coverage_fixes = 0
    invalid_countries = set()
    
    def normalize_country_id(country_id):
        """Normalize country ID (e.g., UK -> GB)"""
        if not country_id:
            return None
        country_id = country_id.strip().upper()
        return COUNTRY_CODE_MAPPINGS.get(country_id, country_id)
    
    def find_country_by_name(name):
        """Try to find country code by name using pycountry and COUNTRIES"""
        if not name:
            return None
        
        name_upper = name.strip().upper()
        
        # First check COUNTRIES dictionary
        if name_upper in countries_by_name:
            return countries_by_name[name_upper]
        
        # Try pycountry lookup
        try:
            # Try exact match
            country = countries.lookup(name)
            if country:
                return country.alpha_2
        except (LookupError, AttributeError):
            pass
        
        # Try fuzzy match in pycountry
        try:
            # Search by name
            for country in countries:
                if country.name and country.name.upper() == name_upper:
                    return country.alpha_2
        except (LookupError, AttributeError):
            pass
        
        return None
    
    def validate_country(country_dict, context=""):
        """Validate a country dictionary and fix if needed. Returns (is_valid, fixed_dict)"""
        if not country_dict or not isinstance(country_dict, dict):
            return False, {"id": "Unknown", "name": "Unknown"}
        
        country_id = country_dict.get("id")
        country_name = country_dict.get("name")
        
        # Normalize country ID
        if country_id:
            country_id = normalize_country_id(country_id)
        
        # Handle special countries
        if country_id and country_id in SPECIAL_COUNTRIES:
            expected_name = COUNTRIES.get(country_id, country_id)
            if country_name and country_name == expected_name:
                return True, country_dict
            else:
                logger.info(
                    "Fixing %s: special country id=%s name=%s -> %s",
                    context,
                    country_id,
                    country_name or "None",
                    expected_name,
                )
                return True, {"id": country_id, "name": expected_name}
        
        # Try to validate using pycountry (ISO 3166-1 alpha-2)
        if country_id:
            try:
                pycountry_obj = countries.get(alpha_2=country_id)
                if pycountry_obj:
                    # Valid ISO code - use COUNTRIES dict name if available, otherwise pycountry name
                    if country_id in COUNTRIES:
                        expected_name = COUNTRIES[country_id]
                    else:
                        expected_name = pycountry_obj.name
                    
                    # Check if name matches
                    if country_name and country_name == expected_name:
                        return True, country_dict
                    else:
                        logger.info(
                            "Fixing %s: country id=%s name=%s -> %s",
                            context,
                            country_id,
                            country_name or "None",
                            expected_name,
                        )
                        return True, {"id": country_id, "name": expected_name}
            except (LookupError, AttributeError):
                pass
        
        # If we have a name but invalid/missing ID, try to find the code
        if country_name and not country_id:
            found_id = find_country_by_name(country_name)
            if found_id:
                expected_name = COUNTRIES.get(found_id, country_name)
                logger.info(
                    "Fixing %s: found country code for name=%s -> id=%s name=%s",
                    context,
                    country_name,
                    found_id,
                    expected_name,
                )
                return True, {"id": found_id, "name": expected_name}
        
        # If we have an ID that's in COUNTRIES but not ISO, use it
        if country_id and country_id in COUNTRIES:
            expected_name = COUNTRIES[country_id]
            if country_name and country_name == expected_name:
                return True, country_dict
            else:
                logger.info(
                    "Fixing %s: country id=%s name=%s -> %s",
                    context,
                    country_id,
                    country_name or "None",
                    expected_name,
                )
                return True, {"id": country_id, "name": expected_name}
        
        # Invalid country - mark for reporting
        old_id = country_id or "None"
        old_name = country_name or "None"
        invalid_countries.add(f"{old_id}:{old_name}")
        logger.warning(
            "Invalid %s: country id=%s name=%s -> Unknown",
            context,
            old_id,
            old_name,
        )
        return False, {"id": "Unknown", "name": "Unknown"}
    
    for root, dirs, files in os.walk(root_dir):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            total_records += 1
            filepath = filename
            try:
                f = open(filepath, "r", encoding="utf8")
                record = yaml.load(f, Loader=Loader)
                f.close()
            except Exception as e:
                logger.error("Error reading file %s: %s", filepath, e)
                continue
            
            if record is None:
                continue
            
            changed = False
            record_id = os.path.basename(filename).split(".", 1)[0]
            
            # Check owner.location.country
            if "owner" in record.keys() and "location" in record["owner"].keys():
                if "country" in record["owner"]["location"]:
                    is_valid, fixed_country = validate_country(
                        record["owner"]["location"]["country"],
                        f"owner.location.country in {record_id}"
                    )
                    if not is_valid or fixed_country != record["owner"]["location"]["country"]:
                        record["owner"]["location"]["country"] = fixed_country
                        changed = True
                        owner_fixes += 1
            
            # Check coverage.location.country
            if "coverage" in record.keys() and isinstance(record["coverage"], list):
                for idx, coverage_item in enumerate(record["coverage"]):
                    if "location" in coverage_item and "country" in coverage_item["location"]:
                        is_valid, fixed_country = validate_country(
                            coverage_item["location"]["country"],
                            f"coverage[{idx}].location.country in {record_id}"
                        )
                        if not is_valid or fixed_country != coverage_item["location"]["country"]:
                            record["coverage"][idx]["location"]["country"] = fixed_country
                            changed = True
                            coverage_fixes += 1
            
            if changed:
                updated_records += 1
                if not dryrun:
                    try:
                        f = open(filepath, "w", encoding="utf8")
                        f.write(yaml.safe_dump(record, allow_unicode=True))
                        f.close()
                    except Exception as e:
                        logger.error("Error writing file %s: %s", filepath, e)
    
    logger.info(
        "Validation complete: %d records processed, %d records updated",
        total_records,
        updated_records,
    )
    logger.info(
        "Fixes: %d owner.location.country, %d coverage.location.country",
        owner_fixes,
        coverage_fixes,
    )
    if invalid_countries:
        logger.warning(
            "Found %d unique invalid country combinations: %s",
            len(invalid_countries),
            ", ".join(sorted(invalid_countries)[:20])  # Show first 20
        )
    if dryrun:
        logger.info("DRYRUN mode - no files were modified")


@app.command()
def analyze_countries(mode="entities"):
    """Analyze all owner.location.country and coverage.location.country values"""
    from constants import COUNTRIES
    from pycountry import countries
    from collections import defaultdict, Counter

    root_dir = ROOT_DIR if mode == "entities" else SCHEDULED_DIR
    
    # Special country codes
    SPECIAL_COUNTRIES = {
        "Unknown", "World", "EU", "Africa", "ASEAN", "Caribbean", 
        "LatinAmerica", "Oceania", "AQ"
    }
    
    COUNTRY_CODE_MAPPINGS = {
        "UK": "GB",
    }
    
    owner_countries = Counter()
    coverage_countries = Counter()
    invalid_owner = []
    invalid_coverage = []
    name_mismatches = []
    missing_ids = []
    missing_names = []
    
    total_records = 0
    
    def normalize_country_id(country_id):
        """Normalize country ID"""
        if not country_id:
            return None
        country_id = country_id.strip().upper()
        return COUNTRY_CODE_MAPPINGS.get(country_id, country_id)
    
    for root, dirs, files in os.walk(root_dir):
        files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in files:
            total_records += 1
            filepath = filename
            try:
                f = open(filepath, "r", encoding="utf8")
                record = yaml.load(f, Loader=Loader)
                f.close()
            except Exception as e:
                logger.error("Error reading file %s: %s", filepath, e)
                continue
            
            if record is None:
                continue
            
            record_id = os.path.basename(filename).split(".", 1)[0]
            
            # Analyze owner.location.country
            if "owner" in record.keys() and "location" in record["owner"].keys():
                if "country" in record["owner"]["location"]:
                    country_dict = record["owner"]["location"]["country"]
                    country_id = country_dict.get("id") if isinstance(country_dict, dict) else None
                    country_name = country_dict.get("name") if isinstance(country_dict, dict) else None
                    
                    if not country_id:
                        missing_ids.append(("owner", record_id, country_name))
                    if not country_name:
                        missing_names.append(("owner", record_id, country_id))
                    
                    if country_id:
                        country_id = normalize_country_id(country_id)
                        owner_countries[country_id] += 1
                        
                        # Check validity
                        is_valid = False
                        if country_id in SPECIAL_COUNTRIES:
                            is_valid = True
                        elif country_id in COUNTRIES:
                            is_valid = True
                        else:
                            try:
                                pycountry_obj = countries.get(alpha_2=country_id)
                                if pycountry_obj:
                                    is_valid = True
                            except (LookupError, AttributeError):
                                pass
                        
                        if not is_valid:
                            invalid_owner.append((record_id, country_id, country_name))
                        
                        # Check name consistency
                        if country_id in COUNTRIES:
                            expected_name = COUNTRIES[country_id]
                            if country_name and country_name != expected_name:
                                name_mismatches.append(("owner", record_id, country_id, country_name, expected_name))
            
            # Analyze coverage.location.country
            if "coverage" in record.keys() and isinstance(record["coverage"], list):
                for idx, coverage_item in enumerate(record["coverage"]):
                    if "location" in coverage_item and "country" in coverage_item["location"]:
                        country_dict = coverage_item["location"]["country"]
                        country_id = country_dict.get("id") if isinstance(country_dict, dict) else None
                        country_name = country_dict.get("name") if isinstance(country_dict, dict) else None
                        
                        if not country_id:
                            missing_ids.append((f"coverage[{idx}]", record_id, country_name))
                        if not country_name:
                            missing_names.append((f"coverage[{idx}]", record_id, country_id))
                        
                        if country_id:
                            country_id = normalize_country_id(country_id)
                            coverage_countries[country_id] += 1
                            
                            # Check validity
                            is_valid = False
                            if country_id in SPECIAL_COUNTRIES:
                                is_valid = True
                            elif country_id in COUNTRIES:
                                is_valid = True
                            else:
                                try:
                                    pycountry_obj = countries.get(alpha_2=country_id)
                                    if pycountry_obj:
                                        is_valid = True
                                except (LookupError, AttributeError):
                                    pass
                            
                            if not is_valid:
                                invalid_coverage.append((record_id, idx, country_id, country_name))
                            
                            # Check name consistency
                            if country_id in COUNTRIES:
                                expected_name = COUNTRIES[country_id]
                                if country_name and country_name != expected_name:
                                    name_mismatches.append((f"coverage[{idx}]", record_id, country_id, country_name, expected_name))
    
    # Print analysis report
    logger.info("=" * 80)
    logger.info("COUNTRY ANALYSIS REPORT")
    logger.info("=" * 80)
    logger.info("Total records processed: %d", total_records)
    logger.info("")
    
    logger.info("OWNER LOCATION COUNTRIES:")
    logger.info("-" * 80)
    logger.info("Total unique countries: %d", len(owner_countries))
    logger.info("Top 20 countries:")
    for country_id, count in owner_countries.most_common(20):
        country_name = COUNTRIES.get(country_id, "Unknown")
        logger.info("  %s (%s): %d", country_id, country_name, count)
    logger.info("")
    
    logger.info("COVERAGE LOCATION COUNTRIES:")
    logger.info("-" * 80)
    logger.info("Total unique countries: %d", len(coverage_countries))
    logger.info("Top 20 countries:")
    for country_id, count in coverage_countries.most_common(20):
        country_name = COUNTRIES.get(country_id, "Unknown")
        logger.info("  %s (%s): %d", country_id, country_name, count)
    logger.info("")
    
    if invalid_owner:
        logger.warning("INVALID OWNER COUNTRIES (%d):", len(invalid_owner))
        for record_id, country_id, country_name in invalid_owner[:20]:
            logger.warning("  %s: id=%s name=%s", record_id, country_id, country_name)
        if len(invalid_owner) > 20:
            logger.warning("  ... and %d more", len(invalid_owner) - 20)
        logger.info("")
    
    if invalid_coverage:
        logger.warning("INVALID COVERAGE COUNTRIES (%d):", len(invalid_coverage))
        for record_id, idx, country_id, country_name in invalid_coverage[:20]:
            logger.warning("  %s[%d]: id=%s name=%s", record_id, idx, country_id, country_name)
        if len(invalid_coverage) > 20:
            logger.warning("  ... and %d more", len(invalid_coverage) - 20)
        logger.info("")
    
    if name_mismatches:
        logger.warning("NAME MISMATCHES (%d):", len(name_mismatches))
        for location, record_id, country_id, current_name, expected_name in name_mismatches[:20]:
            logger.warning("  %s in %s: id=%s current='%s' expected='%s'", 
                         location, record_id, country_id, current_name, expected_name)
        if len(name_mismatches) > 20:
            logger.warning("  ... and %d more", len(name_mismatches) - 20)
        logger.info("")
    
    if missing_ids:
        logger.warning("MISSING COUNTRY IDs (%d):", len(missing_ids))
        for location, record_id, country_name in missing_ids[:20]:
            logger.warning("  %s in %s: name=%s", location, record_id, country_name)
        if len(missing_ids) > 20:
            logger.warning("  ... and %d more", len(missing_ids) - 20)
        logger.info("")
    
    if missing_names:
        logger.warning("MISSING COUNTRY NAMES (%d):", len(missing_names))
        for location, record_id, country_id in missing_names[:20]:
            logger.warning("  %s in %s: id=%s", location, record_id, country_id)
        if len(missing_names) > 20:
            logger.warning("  ... and %d more", len(missing_names) - 20)
        logger.info("")
    
    logger.info("=" * 80)
    logger.info("SUMMARY:")
    logger.info("  Invalid owner countries: %d", len(invalid_owner))
    logger.info("  Invalid coverage countries: %d", len(invalid_coverage))
    logger.info("  Name mismatches: %d", len(name_mismatches))
    logger.info("  Missing IDs: %d", len(missing_ids))
    logger.info("  Missing names: %d", len(missing_names))
    logger.info("=" * 80)


if __name__ == "__main__":
    app()
