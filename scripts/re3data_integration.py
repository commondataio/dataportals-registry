#!/usr/bin/env python
# This script integrates with re3data to fetch trust seal information

import logging
import typer
import requests
import json
import os
import yaml
import time
from typing import Dict, Any, Optional, Set
from urllib.parse import urlparse
from requests.exceptions import RequestException, Timeout

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Get script directory and repository root for path resolution
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)

ROOT_DIR = os.path.join(_REPO_ROOT, "data", "entities")
CACHE_DIR = os.path.join(_REPO_ROOT, "data", "cache")
RE3DATA_TRUST_SEALS_FILE = os.path.join(_REPO_ROOT, "data", "re3data_trust_seals.json")

# Re3Data API endpoint (if available)
RE3DATA_API_BASE = "https://www.re3data.org/api/v1"

# Trust seal indicators in re3data
TRUST_SEAL_INDICATORS = [
    "coretrustseal",
    "core trust seal",
    "wds",
    "world data system",
    "din31644",
    "iso16363",
    "nestor seal",
    "dini certificate",
    "clarin",
    "certified",
    "certification",
]

app = typer.Typer()


def get_re3data_identifier(catalog: Dict[str, Any]) -> Optional[str]:
    """Extract re3data identifier from catalog identifiers."""
    identifiers = catalog.get("identifiers", [])
    for identifier in identifiers:
        if isinstance(identifier, dict) and identifier.get("id") == "re3data":
            return identifier.get("value")
    return None


def get_re3data_url(re3data_id: str) -> str:
    """Get re3data URL for a given identifier."""
    return f"https://www.re3data.org/repository/{re3data_id}"


def fetch_re3data_page(re3data_id: str, timeout: int = 10) -> Optional[str]:
    """Fetch re3data repository page HTML."""
    url = get_re3data_url(re3data_id)
    
    try:
        response = requests.get(url, timeout=timeout, verify=True)
        response.raise_for_status()
        return response.text
    except (RequestException, Timeout) as e:
        logger.warning(f"Could not fetch re3data page for {re3data_id}: {e}")
        return None


def check_trust_seal_in_content(content: str) -> bool:
    """Check if content indicates a trust seal/certification."""
    if not content:
        return False
    
    content_lower = content.lower()
    
    # Check for trust seal indicators
    for indicator in TRUST_SEAL_INDICATORS:
        if indicator in content_lower:
            return True
    
    return False


def fetch_re3data_via_api(re3data_id: str) -> Optional[Dict[str, Any]]:
    """Try to fetch re3data data via API (if available)."""
    # Note: Re3Data may not have a public API
    # This is a placeholder for future API integration
    try:
        # Example API call (adjust based on actual API)
        # url = f"{RE3DATA_API_BASE}/repositories/{re3data_id}"
        # response = requests.get(url, timeout=10)
        # if response.status_code == 200:
        #     return response.json()
        pass
    except Exception as e:
        logger.debug(f"API fetch failed for {re3data_id}: {e}")
    
    return None


def check_re3data_trust_seal(re3data_id: str, use_cache: bool = True) -> bool:
    """
    Check if a re3data repository has a trust seal.
    
    Uses caching to avoid repeated requests.
    """
    cache_file = os.path.join(CACHE_DIR, f"re3data_{re3data_id}.html")
    
    # Check cache first
    if use_cache and os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                content = f.read()
            return check_trust_seal_in_content(content)
        except Exception as e:
            logger.debug(f"Error reading cache for {re3data_id}: {e}")
    
    # Fetch from web
    content = fetch_re3data_page(re3data_id)
    if content:
        # Save to cache
        os.makedirs(CACHE_DIR, exist_ok=True)
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                f.write(content)
        except Exception as e:
            logger.debug(f"Error writing cache for {re3data_id}: {e}")
        
        return check_trust_seal_in_content(content)
    
    return False


def collect_re3data_identifiers() -> Dict[str, str]:
    """Collect all re3data identifiers from catalog files."""
    re3data_ids = {}
    
    logger.info("Collecting re3data identifiers from catalogs...")
    
    all_files = []
    for root, dirs, files in os.walk(ROOT_DIR):
        all_files.extend(
            [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        )
    
    for filepath in all_files:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                catalog = yaml.load(f, Loader=Loader)
            
            if not catalog:
                continue
            
            re3data_id = get_re3data_identifier(catalog)
            if re3data_id:
                catalog_id = catalog.get("id", os.path.basename(filepath))
                re3data_ids[re3data_id] = catalog_id
        
        except Exception as e:
            logger.debug(f"Error reading {filepath}: {e}")
    
    logger.info(f"Found {len(re3data_ids)} catalogs with re3data identifiers")
    return re3data_ids


@app.command()
def fetch_trust_seals(
    output_file: str = typer.Option(
        RE3DATA_TRUST_SEALS_FILE,
        "--output",
        help="Output file for trust seals mapping"
    ),
    use_cache: bool = typer.Option(True, "--use-cache/--no-cache", help="Use cached re3data pages"),
    delay: float = typer.Option(1.0, "--delay", help="Delay between requests (seconds)"),
    limit: Optional[int] = typer.Option(None, "--limit", help="Limit number of repositories to check"),
):
    """Fetch trust seal information from re3data."""
    
    # Collect re3data identifiers
    re3data_ids = collect_re3data_identifiers()
    
    if not re3data_ids:
        logger.warning("No re3data identifiers found")
        return
    
    # Load existing trust seals if file exists
    trust_seals = {}
    if os.path.exists(output_file):
        try:
            with open(output_file, "r", encoding="utf-8") as f:
                trust_seals = json.load(f)
            logger.info(f"Loaded {len(trust_seals)} existing trust seal mappings")
        except Exception as e:
            logger.warning(f"Could not load existing trust seals: {e}")
    
    # Check each re3data repository
    ids_to_check = list(re3data_ids.keys())
    if limit:
        ids_to_check = ids_to_check[:limit]
    
    logger.info(f"Checking {len(ids_to_check)} re3data repositories for trust seals...")
    
    checked = 0
    found = 0
    
    for re3data_id in ids_to_check:
        # Skip if already checked
        if re3data_id in trust_seals:
            continue
        
        logger.info(f"Checking {re3data_id}...")
        has_seal = check_re3data_trust_seal(re3data_id, use_cache=use_cache)
        trust_seals[re3data_id] = has_seal
        
        if has_seal:
            found += 1
            logger.info(f"  ✓ Trust seal found")
        else:
            logger.info(f"  ✗ No trust seal")
        
        checked += 1
        
        # Rate limiting
        if delay > 0 and checked < len(ids_to_check):
            time.sleep(delay)
    
    # Save results
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(trust_seals, f, indent=2, ensure_ascii=False)
    
    logger.info(f"\n=== Results ===")
    logger.info(f"Checked: {checked}")
    logger.info(f"Found trust seals: {found}")
    logger.info(f"Total mappings: {len(trust_seals)}")
    logger.info(f"Results saved to {output_file}")


@app.command()
def update_cache(
    re3data_id: Optional[str] = typer.Option(None, "--id", help="Specific re3data ID to update"),
    all: bool = typer.Option(False, "--all", help="Update cache for all re3data IDs"),
):
    """Update cached re3data pages."""
    
    if re3data_id:
        logger.info(f"Updating cache for {re3data_id}...")
        content = fetch_re3data_page(re3data_id)
        if content:
            cache_file = os.path.join(CACHE_DIR, f"re3data_{re3data_id}.html")
            os.makedirs(CACHE_DIR, exist_ok=True)
            with open(cache_file, "w", encoding="utf-8") as f:
                f.write(content)
            logger.info(f"Cache updated: {cache_file}")
        else:
            logger.error(f"Failed to fetch {re3data_id}")
    
    elif all:
        re3data_ids = collect_re3data_identifiers()
        logger.info(f"Updating cache for {len(re3data_ids)} repositories...")
        
        for re3data_id in re3data_ids.keys():
            logger.info(f"Updating {re3data_id}...")
            content = fetch_re3data_page(re3data_id)
            if content:
                cache_file = os.path.join(CACHE_DIR, f"re3data_{re3data_id}.html")
                os.makedirs(CACHE_DIR, exist_ok=True)
                with open(cache_file, "w", encoding="utf-8") as f:
                    f.write(content)
            time.sleep(1)  # Rate limiting
        
        logger.info("Cache update complete")
    
    else:
        logger.error("Please specify --id or --all")


if __name__ == "__main__":
    app()

