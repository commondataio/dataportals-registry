#!/usr/bin/env python
"""Script to fetch IPT instances from GBIF and add them to the registry"""

import os
import sys
import json
import requests
import logging
import copy
import yaml
from urllib.parse import urlparse
from typing import Set, Dict, List, Optional

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

# Add parent directory to path to import from scripts
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from builder import _add_single_entry, load_jsonl
    from constants import COUNTRIES, ENTRY_TEMPLATE, MAP_CATALOG_TYPE_SUBDIR, DOMAIN_LOCATIONS, DEFAULT_LOCATION, COUNTRIES_LANGS, MAP_SOFTWARE_OWNER_CATALOG_TYPE, CUSTOM_SOFTWARE_KEYS
    BUILDER_AVAILABLE = True
except ImportError as e:
    logging.warning(f"Could not import builder modules: {e}. Will create YAML files directly.")
    BUILDER_AVAILABLE = False
    # Define minimal constants if builder not available
    COUNTRIES = {}
    ENTRY_TEMPLATE = {
        "access_mode": ["open"],
        "api": False,
        "api_status": "uncertain",
        "catalog_type": "Scientific data repository",
        "content_types": ["dataset"],
        "coverage": [],
        "id": "None",
        "langs": [],
        "link": None,
        "name": "",
        "owner": {"link": None, "location": None, "name": "", "type": "Unknown"},
        "software": {"id": "ipt", "name": "IPT"},
        "status": "scheduled",
        "tags": [],
        "topics": [],
        "description": "This is a temporary record with some data collected but it should be updated before adding to the index",
    }
    MAP_CATALOG_TYPE_SUBDIR = {
        "Scientific data repository": "scientific",
    }
    DOMAIN_LOCATIONS = {}
    DEFAULT_LOCATION = {
        "location": {"country": {"id": "Unknown", "name": "Unknown"}, "level": 0}
    }
    COUNTRIES_LANGS = {}
    MAP_SOFTWARE_OWNER_CATALOG_TYPE = {}
    CUSTOM_SOFTWARE_KEYS = []
    
    def load_jsonl(filepath):
        """Simple load_jsonl implementation when builder not available"""
        data = []
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf8") as f:
                for line in f:
                    try:
                        data.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return data

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

GBIF_API_BASE = "https://api.gbif.org/v1"
DATASETS_DIR = "../data/datasets"


def fetch_all_ipt_installations() -> List[Dict]:
    """Fetch all IPT installations from GBIF API"""
    installations = []
    offset = 0
    limit = 20
    
    logger.info("Fetching IPT installations from GBIF API...")
    
    while True:
        url = f"{GBIF_API_BASE}/installation"
        params = {
            "type": "IPT_INSTALLATION",
            "limit": limit,
            "offset": offset,
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            results = data.get("results", [])
            if not results:
                break
                
            installations.extend(results)
            logger.info(f"Fetched {len(installations)} installations so far...")
            
            if data.get("endOfRecords", False):
                break
                
            offset += limit
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching installations: {e}")
            break
    
    logger.info(f"Total IPT installations fetched: {len(installations)}")
    return installations


def extract_ipt_url(installation: Dict) -> Optional[str]:
    """Extract the IPT URL from an installation record"""
    # Check endpoints first - IPT endpoints often have URLs like:
    # https://ipt.sibbr.gov.br/obis-brasil/rss.do
    # We need to extract the base URL (https://ipt.sibbr.gov.br)
    endpoints = installation.get("endpoints", [])
    for endpoint in endpoints:
        url = endpoint.get("url", "")
        if url:
            # Normalize URL - extract base URL
            parsed = urlparse(url)
            # For IPT, the base URL is typically the scheme + netloc
            # Sometimes the path contains the IPT instance name, so we keep just the base
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            return base_url
    
    # If no endpoint found, try to construct from title/description
    # Many IPT instances follow patterns like ipt.gbif.XX or similar
    title = installation.get("title", "").lower()
    description = installation.get("description", "")
    
    # Look for URLs in description
    if description:
        import re
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        urls = re.findall(url_pattern, description)
        for url in urls:
            if "ipt" in url.lower() or "gbif" in url.lower():
                parsed = urlparse(url)
                base_url = f"{parsed.scheme}://{parsed.netloc}"
                return base_url
    
    return None


def get_organization_info(organization_key: str) -> Dict:
    """Fetch organization information from GBIF API"""
    try:
        url = f"{GBIF_API_BASE}/organization/{organization_key}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.warning(f"Error fetching organization {organization_key}: {e}")
        return {}


def get_existing_urls() -> Set[str]:
    """Get all existing URLs from the registry"""
    existing_urls = set()
    
    # Load from full.jsonl if available
    full_jsonl_path = os.path.join(DATASETS_DIR, "full.jsonl")
    if os.path.exists(full_jsonl_path):
        logger.info("Loading existing records from full.jsonl...")
        try:
            if BUILDER_AVAILABLE:
                records = load_jsonl(full_jsonl_path)
            else:
                # Use simple implementation
                records = []
                with open(full_jsonl_path, "r", encoding="utf8") as f:
                    for line in f:
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
            for record in records:
                link = record.get("link")
                if link:
                    # Normalize URL
                    parsed = urlparse(link)
                    normalized = f"{parsed.scheme}://{parsed.netloc}".lower()
                    existing_urls.add(normalized)
            logger.info(f"Loaded {len(existing_urls)} existing URLs from full.jsonl")
        except Exception as e:
            logger.warning(f"Error loading full.jsonl: {e}")
    
    # Also check entities directory
    entities_dir = "../data/entities"
    if os.path.exists(entities_dir):
        logger.info("Scanning entities directory for additional URLs...")
        import yaml
        try:
            from yaml import CLoader as Loader
        except ImportError:
            from yaml import Loader
        
        count = 0
        for root, dirs, files in os.walk(entities_dir):
            for file in files:
                if file.endswith(".yaml") or file.endswith(".yml"):
                    filepath = os.path.join(root, file)
                    try:
                        with open(filepath, "r", encoding="utf8") as f:
                            record = yaml.load(f, Loader=Loader)
                            if record and "link" in record:
                                link = record["link"]
                                parsed = urlparse(link)
                                normalized = f"{parsed.scheme}://{parsed.netloc}".lower()
                                existing_urls.add(normalized)
                                count += 1
                    except Exception as e:
                        logger.debug(f"Error reading {filepath}: {e}")
        
        logger.info(f"Scanned {count} additional YAML files")
    
    logger.info(f"Total existing URLs found: {len(existing_urls)}")
    return existing_urls


def infer_country_from_url(url: str) -> Optional[str]:
    """Try to infer country code from URL"""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    
    # Check for country codes in domain
    parts = domain.split(".")
    for part in parts:
        if len(part) == 2 and part.upper() in COUNTRIES:
            return part.upper()
    
    # Check for common patterns like gbif.XX
    if "gbif." in domain:
        tld = domain.split("gbif.")[-1].split(".")[0]
        if len(tld) == 2 and tld.upper() in COUNTRIES:
            return tld.upper()
    
    return None


def create_yaml_directly(url: str, title: str, description: str, country: Optional[str], 
                         owner_name: str, owner_link: Optional[str], scheduled_dir: str):
    """Create YAML file directly without using builder functions"""
    domain = urlparse(url).netloc.lower()
    record_id = domain.split(":", 1)[0].replace("_", "").replace("-", "").replace(".", "")
    
    record = copy.deepcopy(ENTRY_TEMPLATE)
    record["id"] = record_id
    record["link"] = url
    record["name"] = title or domain
    if description:
        record["description"] = description
    
    # Set location
    if country and country in COUNTRIES:
        location = {
            "location": {"country": {"id": country, "name": COUNTRIES[country]}}
        }
    else:
        postfix = domain.rsplit(".", 1)[-1].split(":", 1)[0]
        if postfix in DOMAIN_LOCATIONS:
            location = DOMAIN_LOCATIONS[postfix]
        else:
            location = DEFAULT_LOCATION
    
    record["coverage"].append(copy.deepcopy(location))
    record["owner"].update(copy.deepcopy(location))
    
    if owner_name:
        record["owner"]["name"] = owner_name
    if owner_link:
        record["owner"]["link"] = owner_link
    if not record["owner"]["type"] or record["owner"]["type"] == "Unknown":
        record["owner"]["type"] = "Academy"  # Default for IPT instances
    
    # Set catalog type and software
    record["catalog_type"] = "Scientific data repository"
    record["software"] = {"id": "ipt", "name": "IPT"}
    
    # Determine directory structure
    country_code = location["location"]["country"]["id"]
    if country_code == "Unknown":
        country_code = "World"  # Use World for unknown countries
    
    country_dir = os.path.join(scheduled_dir, country_code)
    if not os.path.exists(country_dir):
        os.makedirs(country_dir, exist_ok=True)
    
    subdir_name = MAP_CATALOG_TYPE_SUBDIR.get(record["catalog_type"], "scientific")
    subdir_dir = os.path.join(country_dir, subdir_name)
    if not os.path.exists(subdir_dir):
        os.makedirs(subdir_dir, exist_ok=True)
    
    filename = os.path.join(subdir_dir, record_id + ".yaml")
    
    if os.path.exists(filename):
        logger.info(f"File already exists: {filename}, skipping")
        return False
    
    with open(filename, "w", encoding="utf8") as f:
        yaml.dump(record, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
    
    logger.info(f"Created YAML file: {filename}")
    return True


def main():
    """Main function to fetch and add IPT instances"""
    print("Starting GBIF IPT discovery...", flush=True)
    logger.info("Starting GBIF IPT discovery...")
    
    # Get existing URLs
    print("Getting existing URLs...", flush=True)
    existing_urls = get_existing_urls()
    print(f"Found {len(existing_urls)} existing URLs", flush=True)
    
    # Fetch all IPT installations
    print("Fetching IPT installations from GBIF...", flush=True)
    installations = fetch_all_ipt_installations()
    print(f"Fetched {len(installations)} installations", flush=True)
    
    if not installations:
        logger.warning("No IPT installations found")
        return
    
    # Process each installation
    added_count = 0
    skipped_count = 0
    error_count = 0
    
    logger.info(f"Processing {len(installations)} installations...")
    print(f"Processing {len(installations)} installations...", flush=True)
    
    for idx, installation in enumerate(installations, 1):
        if idx % 10 == 0:
            logger.info(f"Processed {idx}/{len(installations)} installations... (Added: {added_count}, Skipped: {skipped_count}, Errors: {error_count})")
            print(f"Processed {idx}/{len(installations)} installations... (Added: {added_count}, Skipped: {skipped_count}, Errors: {error_count})", flush=True)
        try:
            # Extract URL
            url = extract_ipt_url(installation)
            if not url:
                logger.debug(f"No URL found for installation: {installation.get('title', 'Unknown')}")
                skipped_count += 1
                continue
            
            # Normalize URL for comparison
            parsed = urlparse(url)
            normalized_url = f"{parsed.scheme}://{parsed.netloc}".lower()
            
            # Check if already exists
            if normalized_url in existing_urls:
                logger.debug(f"Already exists: {url}")
                skipped_count += 1
                continue
            
            # Get organization info (optional, skip if taking too long)
            org_key = installation.get("organizationKey")
            org_info = {}
            # Skip organization fetching for now to speed up processing
            # if org_key:
            #     org_info = get_organization_info(org_key)
            
            # Extract metadata
            title = installation.get("title", "")
            description = installation.get("description", "")
            
            # Infer country
            country = infer_country_from_url(url)
            
            # Determine owner info
            owner_name = org_info.get("title", "") or title
            owner_link = org_info.get("homepage", [None])[0] if org_info.get("homepage") else None
            
            # Add to registry
            logger.info(f"Adding IPT instance: {url} - {title}")
            
            if BUILDER_AVAILABLE:
                # Get existing record IDs to pass to preloaded
                full_jsonl_path = os.path.join(DATASETS_DIR, "full.jsonl")
                preloaded_ids = set()
                if os.path.exists(full_jsonl_path):
                    try:
                        records = load_jsonl(full_jsonl_path)
                        preloaded_ids = {r.get("id") for r in records if r.get("id")}
                    except Exception:
                        pass
                
                _add_single_entry(
                    url=url,
                    software="ipt",
                    catalog_type="Scientific data repository",
                    name=title or None,
                    description=description or None,
                    country=country,
                    owner_name=owner_name or None,
                    owner_link=owner_link,
                    owner_type="Academy",  # Default, can be updated later
                    scheduled=True,  # Add to scheduled for review
                    force=False,
                    preloaded=preloaded_ids,
                )
                added_count += 1
            else:
                # Create YAML directly
                scheduled_dir = "../data/scheduled"
                if create_yaml_directly(url, title, description, country, owner_name, owner_link, scheduled_dir):
                    added_count += 1
                else:
                    skipped_count += 1
            
            existing_urls.add(normalized_url)  # Track added URLs
            
        except Exception as e:
            logger.error(f"Error processing installation {installation.get('title', 'Unknown')}: {e}")
            error_count += 1
    
    logger.info(f"Processing complete:")
    logger.info(f"  - Added: {added_count}")
    logger.info(f"  - Skipped (already exists): {skipped_count}")
    logger.info(f"  - Errors: {error_count}")


if __name__ == "__main__":
    main()

