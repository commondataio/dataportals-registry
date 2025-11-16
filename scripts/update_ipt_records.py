#!/usr/bin/env python
"""Script to update IPT records with metadata from GBIF API"""

import os
import sys
import json
import requests
import logging
import yaml
import copy
from urllib.parse import urlparse
from typing import Dict, List, Optional, Set
from pathlib import Path

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

# Add parent directory to path to import from scripts
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from constants import COUNTRIES
    CONSTANTS_AVAILABLE = True
except ImportError:
    CONSTANTS_AVAILABLE = False
    COUNTRIES = {}

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

GBIF_API_BASE = "https://api.gbif.org/v1"
ROOT_DIR = "../data/entities"
SCHEDULED_DIR = "../data/scheduled"


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
    endpoints = installation.get("endpoints", [])
    for endpoint in endpoints:
        url = endpoint.get("url", "")
        if url:
            parsed = urlparse(url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            return base_url
    return None


def normalize_url(url: str) -> str:
    """Normalize URL for comparison"""
    parsed = urlparse(url)
    # Remove port if it's standard
    netloc = parsed.netloc
    if parsed.port:
        if (parsed.scheme == "http" and parsed.port == 80) or \
           (parsed.scheme == "https" and parsed.port == 443):
            netloc = parsed.hostname
    else:
        netloc = parsed.hostname or parsed.netloc
    normalized = f"{parsed.scheme}://{netloc}".lower()
    # Remove trailing slash and common paths
    normalized = normalized.rstrip("/")
    return normalized


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


def infer_country_from_url(url: str) -> Optional[str]:
    """Try to infer country code from URL"""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    
    # Check for country codes in domain
    parts = domain.split(".")
    for part in parts:
        if len(part) == 2 and CONSTANTS_AVAILABLE and part.upper() in COUNTRIES:
            return part.upper()
    
    # Check for common patterns like gbif.XX
    if "gbif." in domain:
        tld = domain.split("gbif.")[-1].split(".")[0]
        if len(tld) == 2 and CONSTANTS_AVAILABLE and tld.upper() in COUNTRIES:
            return tld.upper()
    
    return None


def find_all_ipt_records(include_scheduled: bool = True) -> List[Dict]:
    """Find all IPT records in entities and optionally scheduled directories"""
    ipt_records = []
    directories = [ROOT_DIR]
    
    if include_scheduled:
        directories.append(SCHEDULED_DIR)
    
    for directory in directories:
        dir_path = Path(directory)
        
        if not dir_path.exists():
            logger.warning(f"Directory not found: {directory}")
            continue
        
        logger.info(f"Scanning {directory} directory for IPT records...")
        
        for yaml_file in dir_path.rglob("*.yaml"):
            try:
                with open(yaml_file, "r", encoding="utf8") as f:
                    record = yaml.load(f, Loader=Loader)
                    
                if record and record.get("software", {}).get("id") == "ipt":
                    record["_filepath"] = str(yaml_file)
                    record["_directory"] = directory  # Track which directory it's from
                    ipt_records.append(record)
                    logger.debug(f"Found IPT record: {record.get('id')} at {yaml_file}")
            except Exception as e:
                logger.warning(f"Error reading {yaml_file}: {e}")
    
    logger.info(f"Found {len(ipt_records)} IPT records total")
    return ipt_records


def match_record_with_installation(record: Dict, installations: List[Dict]) -> Optional[Dict]:
    """Match a record with a GBIF installation by URL"""
    record_url = record.get("link", "")
    if not record_url:
        return None
    
    record_normalized = normalize_url(record_url)
    
    for installation in installations:
        inst_url = extract_ipt_url(installation)
        if inst_url:
            inst_normalized = normalize_url(inst_url)
            # Also check endpoints
            for endpoint in installation.get("endpoints", []):
                ep_url = endpoint.get("url", "")
                if ep_url:
                    ep_normalized = normalize_url(ep_url)
                    if record_normalized in ep_normalized or ep_normalized in record_normalized:
                        return installation
            
            if record_normalized == inst_normalized or \
               record_normalized in inst_normalized or \
               inst_normalized in record_normalized:
                return installation
    
    return None


def update_record_from_installation(record: Dict, installation: Dict, org_info: Dict) -> bool:
    """Update record with data from GBIF installation"""
    changed = False
    
    # Update title/name if better
    inst_title = installation.get("title", "")
    if inst_title and inst_title != record.get("name", ""):
        # Only update if current name is generic or missing
        current_name = record.get("name", "")
        if not current_name or current_name.lower() in ["ipt", "ipt server", "ipt installation"]:
            record["name"] = inst_title
            changed = True
    
    # Update description if better
    inst_description = installation.get("description", "")
    if inst_description and len(inst_description) > len(record.get("description", "")):
        record["description"] = inst_description
        changed = True
    
    # Update owner information from organization
    if org_info:
        org_title = org_info.get("title", "")
        org_homepage = org_info.get("homepage", [None])[0] if org_info.get("homepage") else None
        
        if org_title and org_title != record.get("owner", {}).get("name", ""):
            if "owner" not in record:
                record["owner"] = {}
            record["owner"]["name"] = org_title
            changed = True
        
        if org_homepage and org_homepage != record.get("owner", {}).get("link"):
            if "owner" not in record:
                record["owner"] = {}
            record["owner"]["link"] = org_homepage
            changed = True
    
    # Update country/location from organization or URL
    if org_info:
        org_country = org_info.get("country")
        if org_country and CONSTANTS_AVAILABLE:
            country_code = org_country.upper()
            if country_code in COUNTRIES:
                # Update coverage
                if "coverage" not in record or not record["coverage"]:
                    record["coverage"] = []
                
                # Check if country already in coverage
                country_exists = False
                for cov in record["coverage"]:
                    if cov.get("location", {}).get("country", {}).get("id") == country_code:
                        country_exists = True
                        break
                
                if not country_exists:
                    record["coverage"].append({
                        "location": {
                            "country": {
                                "id": country_code,
                                "name": COUNTRIES[country_code]
                            },
                            "level": 20
                        }
                    })
                    changed = True
                
                # Update owner location
                if "owner" not in record:
                    record["owner"] = {}
                if "location" not in record["owner"]:
                    record["owner"]["location"] = {}
                
                if record["owner"]["location"].get("country", {}).get("id") != country_code:
                    record["owner"]["location"] = {
                        "country": {
                            "id": country_code,
                            "name": COUNTRIES[country_code]
                        },
                        "level": 20
                    }
                    changed = True
    
    # Update endpoints if missing or outdated
    inst_endpoints = installation.get("endpoints", [])
    if inst_endpoints:
        if "endpoints" not in record:
            record["endpoints"] = []
        
        existing_endpoint_urls = {ep.get("url", "") for ep in record["endpoints"]}
        
        for inst_ep in inst_endpoints:
            ep_url = inst_ep.get("url", "")
            if ep_url and ep_url not in existing_endpoint_urls:
                # Determine endpoint type
                ep_type = "ipt:dataset"
                if "/rss" in ep_url.lower() or "rss.do" in ep_url.lower():
                    ep_type = "rss"
                elif "/dcat" in ep_url.lower():
                    ep_type = "dcat:ttl"
                
                new_endpoint = {
                    "type": ep_type,
                    "url": ep_url
                }
                if ep_type == "rss":
                    new_endpoint["version"] = "2.0"
                
                record["endpoints"].append(new_endpoint)
                changed = True
    
    return changed


def save_record(record: Dict, filepath: str, dryrun: bool = False) -> bool:
    """Save updated record to YAML file"""
    if dryrun:
        logger.info(f"Would update: {filepath}")
        return True
    
    try:
        # Remove internal fields
        record_to_save = copy.deepcopy(record)
        for field in ["_filepath", "_directory"]:
            if field in record_to_save:
                del record_to_save[field]
        
        # Clean up coverage - remove "Unknown" entries if we have real country data
        if "coverage" in record_to_save and record_to_save["coverage"]:
            # Check if we have any non-Unknown countries
            has_real_country = False
            for cov in record_to_save["coverage"]:
                country_id = cov.get("location", {}).get("country", {}).get("id", "")
                if country_id and country_id not in ["Unknown", "World"]:
                    has_real_country = True
                    break
            
            # Remove Unknown entries if we have real country data
            if has_real_country:
                record_to_save["coverage"] = [
                    cov for cov in record_to_save["coverage"]
                    if cov.get("location", {}).get("country", {}).get("id", "") not in ["Unknown", "World"]
                ]
        
        with open(filepath, "w", encoding="utf8") as f:
            yaml.dump(record_to_save, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
        
        logger.info(f"Updated: {filepath}")
        return True
    except Exception as e:
        logger.error(f"Error saving {filepath}: {e}")
        return False


def main(dryrun: bool = False, scheduled_only: bool = False):
    """Main function to update IPT records"""
    logger.info("Starting IPT records update...")
    if dryrun:
        logger.info("DRY RUN MODE - no files will be modified")
    if scheduled_only:
        logger.info("Processing scheduled records only")
    
    # Find all IPT records
    ipt_records = find_all_ipt_records(include_scheduled=True)
    
    # Filter to scheduled only if requested
    if scheduled_only:
        ipt_records = [r for r in ipt_records if r.get("_directory") == SCHEDULED_DIR]
        logger.info(f"Filtered to {len(ipt_records)} scheduled IPT records")
    if not ipt_records:
        logger.warning("No IPT records found")
        return
    
    # Fetch all IPT installations from GBIF
    installations = fetch_all_ipt_installations()
    if not installations:
        logger.warning("No IPT installations found from GBIF")
        return
    
    # Create URL to installation mapping
    logger.info("Creating installation lookup map...")
    url_to_installation = {}
    for installation in installations:
        url = extract_ipt_url(installation)
        if url:
            normalized = normalize_url(url)
            url_to_installation[normalized] = installation
            # Also add endpoint URLs
            for endpoint in installation.get("endpoints", []):
                ep_url = endpoint.get("url", "")
                if ep_url:
                    ep_normalized = normalize_url(ep_url)
                    url_to_installation[ep_normalized] = installation
    
    # Process each record
    updated_count = 0
    matched_count = 0
    error_count = 0
    
    logger.info(f"Processing {len(ipt_records)} IPT records...")
    
    for record in ipt_records:
        try:
            filepath = record.get("_filepath")
            if not filepath:
                logger.warning(f"Record {record.get('id')} has no filepath")
                continue
            
            # Match with GBIF installation
            installation = match_record_with_installation(record, installations)
            
            if not installation:
                logger.debug(f"No GBIF match found for {record.get('id')} ({record.get('link')})")
                continue
            
            matched_count += 1
            logger.info(f"Matched {record.get('id')} with GBIF installation: {installation.get('title', 'Unknown')}")
            
            # Get organization info
            org_key = installation.get("organizationKey")
            org_info = {}
            if org_key:
                org_info = get_organization_info(org_key)
            
            # Update record
            changed = update_record_from_installation(record, installation, org_info)
            
            if changed:
                if save_record(record, filepath, dryrun):
                    updated_count += 1
                else:
                    error_count += 1
            else:
                logger.debug(f"No changes needed for {record.get('id')}")
                
        except Exception as e:
            logger.error(f"Error processing record {record.get('id', 'Unknown')}: {e}")
            error_count += 1
    
    logger.info(f"Update complete:")
    logger.info(f"  - Total IPT records: {len(ipt_records)}")
    logger.info(f"  - Matched with GBIF: {matched_count}")
    logger.info(f"  - Updated: {updated_count}")
    logger.info(f"  - Errors: {error_count}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Update IPT records with GBIF metadata")
    parser.add_argument("--dryrun", action="store_true", help="Dry run mode (no files modified)")
    parser.add_argument("--scheduled-only", action="store_true", help="Process scheduled records only")
    args = parser.parse_args()
    
    main(dryrun=args.dryrun, scheduled_only=args.scheduled_only)

