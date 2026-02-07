#!/usr/bin/env python
# Script to synchronize CKAN websites from ecosystem.ckan.org dataset

import logging
import typer
import requests
import json
import os
import time
import re
from urllib.parse import urlparse, urljoin
from typing import Dict, List, Optional, Set, Tuple
from requests.exceptions import RequestException, Timeout
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

try:
    import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    # Fallback: create a no-op wrapper
    class tqdm:
        @staticmethod
        def tqdm(iterable, *args, **kwargs):
            return iterable

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Get script directory and repository root for path resolution
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)

# Import from builder.py - need to import the module to access its functions
import sys
sys.path.insert(0, _SCRIPT_DIR)

# Import builder module and access its components
# Note: builder.py imports constants, so we need to ensure the path is set first
import builder

# Access constants and functions from builder
DATASETS_DIR = builder.DATASETS_DIR
SCHEDULED_DIR = builder.SCHEDULED_DIR
ROOT_DIR = builder.ROOT_DIR
load_jsonl = builder.load_jsonl

app = typer.Typer()

# CKAN Ecosystem API endpoint
CKAN_ECOSYSTEM_BASE = "https://ecosystem.ckan.org"
CKAN_ECOSYSTEM_API = f"{CKAN_ECOSYSTEM_BASE}/api/3/action"
CKAN_SITES_DATASET = "ckan-sites-metadata"

# User agent for requests
USER_AGENT = "Mozilla/5.0 (compatible; DataPortalsRegistry/1.0; +https://github.com/commondataio/dataportals-registry)"

# Rate limiting: delay between requests (seconds)
REQUEST_DELAY = 1.0


def normalize_url(url: str) -> str:
    """Normalize URL for duplicate detection."""
    if not url:
        return ""
    # Remove protocol, www, trailing slashes, convert to lowercase
    url = url.lower().strip()
    url = re.sub(r'^https?://', '', url)
    url = re.sub(r'^www\.', '', url)
    url = url.rstrip('/')
    return url


def normalize_domain(url: str) -> str:
    """Extract normalized domain from URL."""
    normalized = normalize_url(url)
    if not normalized:
        return ""
    # Extract domain (handle ports)
    domain = normalized.split('/')[0].split(':')[0]
    return domain


def query_ckan_api(action: str, params: Dict = None, timeout: int = 30) -> Optional[Dict]:
    """Query CKAN API endpoint."""
    url = f"{CKAN_ECOSYSTEM_API}/{action}"
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=timeout, verify=True)
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logger.error(f"Error querying CKAN API {action}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON response from {action}: {e}")
        return None


def is_ckan_extension(pkg: Dict) -> bool:
    """Check if a package is a CKAN extension/plugin rather than a CKAN site."""
    # Check type field
    pkg_type = pkg.get("type", "").lower()
    if "extension" in pkg_type or "plugin" in pkg_type:
        return True
    
    # Check groups
    groups = pkg.get("groups", [])
    if isinstance(groups, list):
        for group in groups:
            if isinstance(group, dict):
                group_name = group.get("name", "").lower()
                if "extension" in group_name or "plugin" in group_name:
                    return True
            elif isinstance(group, str):
                if "extension" in group.lower() or "plugin" in group.lower():
                    return True
    
    # Check tags
    tags = pkg.get("tags", [])
    if isinstance(tags, list):
        for tag in tags:
            if isinstance(tag, dict):
                tag_name = tag.get("name", "").lower()
                if tag_name in ["extension", "plugin", "ckan-extension", "ckan-plugin"]:
                    return True
            elif isinstance(tag, str):
                if tag.lower() in ["extension", "plugin", "ckan-extension", "ckan-plugin"]:
                    return True
    
    # Check if it has repository URL but no site URL (extensions are code, not sites)
    repo_fields = ["repository_url", "github_url", "git_url", "source_url"]
    url_fields = ["url", "site_url", "link", "homepage", "website"]
    
    has_repo = any(pkg.get(field) for field in repo_fields)
    has_site_url = any(pkg.get(field) for field in url_fields)
    
    # Check extras for repository URLs
    extras = pkg.get("extras", [])
    for extra in extras:
        if isinstance(extra, dict):
            key = extra.get("key", "").lower()
            value = extra.get("value", "")
            if "repository" in key or "github" in key or "git" in key:
                has_repo = True
            if key == "url" and value:
                # Check if URL looks like a repository (github, gitlab, etc.)
                if any(repo_host in value.lower() for repo_host in ["github.com", "gitlab.com", "bitbucket.org", "code.ckan.org"]):
                    has_repo = True
                else:
                    has_site_url = True
    
    # If it has repository URL but no site URL, it's likely an extension
    if has_repo and not has_site_url:
        return True
    
    # Check name/ID patterns that suggest extensions
    pkg_name = (pkg.get("name") or pkg.get("id") or "").lower()
    extension_patterns = ["ckanext-", "ckan-extension", "-extension", "-plugin", "-theme"]
    if any(pattern in pkg_name for pattern in extension_patterns):
        return True
    
    return False


def fetch_ckan_ecosystem_dataset() -> List[Dict]:
    """Fetch CKAN site records from ecosystem.ckan.org dataset."""
    logger.info("Fetching CKAN sites from ecosystem.ckan.org...")
    
    # First, try to get the dataset package
    package_result = query_ckan_api("package_show", {"id": CKAN_SITES_DATASET})
    if not package_result or "result" not in package_result:
        logger.error("Failed to fetch dataset package")
        return []
    
    package = package_result["result"]
    logger.info(f"Found dataset: {package.get('name', 'unknown')}")
    
    # Get all resources from the dataset
    resources = package.get("resources", [])
    if not resources:
        logger.warning("No resources found in dataset")
        return []
    
    all_records = []
    
    # Try to fetch data from resources
    for resource in resources:
        resource_url = resource.get("url")
        if not resource_url:
            continue
        
        logger.info(f"Fetching resource: {resource.get('name', 'unnamed')}")
        
        # Try CKAN API package_search to get all CKAN site records
        # Search for packages that have URLs (indicating they're CKAN sites)
        # Use pagination to get all records
        search_queries = [
            {"q": "*:*", "fq": "extras_url:*", "rows": 1000},  # Packages with URL extra
            {"q": "type:ckan-site", "rows": 1000},  # Packages with ckan-site type
            {"q": "groups:ckan-sites", "rows": 1000},  # Packages in ckan-sites group
        ]
        
        for query_params in search_queries:
            # Fetch all pages of results
            start = 0
            rows = query_params.get("rows", 1000)
            total_found = None
            
            while True:
                query_params_page = query_params.copy()
                query_params_page["start"] = start
                
                search_result = query_ckan_api("package_search", query_params_page)
                if search_result and "result" in search_result:
                    result_data = search_result["result"]
                    records = result_data.get("results", [])
                    
                    if total_found is None:
                        total_found = result_data.get("count", len(records))
                        logger.info(f"Found {total_found} total records via package_search with query: {query_params.get('q', 'default')}")
                    
                    logger.info(f"Fetching records {start} to {start + len(records)} of {total_found}")
                    all_records.extend(records)
                    
                    # Check if we've got all records
                    if len(records) < rows or start + len(records) >= total_found:
                        break
                    
                    start += rows
                    time.sleep(REQUEST_DELAY)  # Rate limiting between pages
                else:
                    break
            
            if all_records:
                break  # If we found records, we can stop trying other queries
        
        # Also try to fetch the resource URL directly if it's a data file
        if resource_url.endswith(('.json', '.csv', '.xlsx')):
            try:
                time.sleep(REQUEST_DELAY)
                response = requests.get(resource_url, headers={"User-Agent": USER_AGENT}, timeout=30)
                response.raise_for_status()
                
                # Try to parse as JSON
                try:
                    data = response.json()
                    if isinstance(data, list):
                        all_records.extend(data)
                    elif isinstance(data, dict) and "result" in data:
                        all_records.extend(data["result"].get("results", []))
                except json.JSONDecodeError:
                    logger.debug(f"Resource {resource_url} is not JSON, skipping")
            except RequestException as e:
                logger.debug(f"Could not fetch resource {resource_url}: {e}")
    
    # Alternative: Try package_list and then package_show for each
    # The ecosystem.ckan.org dataset contains CKAN sites as individual packages
    # This is a fallback if package_search doesn't work
    if not all_records:
        logger.info("Trying alternative method: package_list (processing all packages)")
        list_result = query_ckan_api("package_list", {"limit": 10000})
        if list_result and "result" in list_result:
            package_ids = list_result["result"]
            logger.info(f"Found {len(package_ids)} packages, processing all for CKAN sites...")
            
            for pkg_id in tqdm.tqdm(package_ids, desc="Checking packages", unit="package"):
                # Skip the metadata dataset itself
                if pkg_id == CKAN_SITES_DATASET:
                    continue
                    
                time.sleep(REQUEST_DELAY)
                pkg_result = query_ckan_api("package_show", {"id": pkg_id})
                if pkg_result and "result" in pkg_result:
                    pkg = pkg_result["result"]
                    
                    # Skip CKAN extensions/plugins
                    if is_ckan_extension(pkg):
                        continue
                    
                    # Check if this has a URL field indicating it's a CKAN site
                    url_fields = ["url", "site_url", "link", "homepage", "website"]
                    extras = pkg.get("extras", [])
                    # Also check extras for URL
                    for extra in extras:
                        if isinstance(extra, dict) and extra.get("key") == "url" and extra.get("value"):
                            url_fields.append(extra.get("value"))
                    
                    has_url = any(pkg.get(field) for field in url_fields) or any(
                        isinstance(extra, dict) and extra.get("key") == "url" and extra.get("value")
                        for extra in extras
                    )
                    if has_url:
                        all_records.append(pkg)
    
    # Filter out extensions and deduplicate by ID
    seen_ids = set()
    unique_records = []
    extensions_skipped = 0
    
    for record in all_records:
        # Skip CKAN extensions
        if is_ckan_extension(record):
            extensions_skipped += 1
            continue
        
        record_id = record.get("id") or record.get("name")
        if record_id and record_id not in seen_ids:
            seen_ids.add(record_id)
            unique_records.append(record)
    
    if extensions_skipped > 0:
        logger.info(f"Skipped {extensions_skipped} CKAN extensions/plugins")
    logger.info(f"Total unique CKAN site records found: {len(unique_records)}")
    return unique_records


def parse_ckan_site_record(record: Dict) -> Optional[Dict]:
    """Parse a CKAN site record from the dataset and extract relevant metadata."""
    # Extract URL - could be in various fields or in extras
    url = (
        record.get("url") or
        record.get("site_url") or
        record.get("link") or
        record.get("homepage") or
        record.get("website")
    )
    
    # Also check extras for URL
    if not url:
        extras = record.get("extras", [])
        for extra in extras:
            if isinstance(extra, dict) and extra.get("key") == "url":
                url = extra.get("value")
                break
    
    if not url:
        # Try to construct from name if it looks like a URL
        name = record.get("name", "")
        if name.startswith("http"):
            url = name
        else:
            logger.debug(f"No URL found in record: {record.get('id', 'unknown')}")
            return None
    
    # Normalize URL
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    
    # Extract other metadata
    name = record.get("title") or record.get("name") or urlparse(url).netloc
    description = record.get("notes") or record.get("description")
    
    # Extract location/geographic info
    country = None
    location_tags = record.get("tags", [])
    if isinstance(location_tags, list):
        for tag in location_tags:
            if isinstance(tag, dict):
                tag_name = tag.get("name", "").upper()
                # Check if it's a country code
                if len(tag_name) == 2 and tag_name.isalpha():
                    country = tag_name
                    break
    
    # Extract owner/organization
    owner_name = None
    owner_type = None
    org = record.get("organization")
    if isinstance(org, dict):
        owner_name = org.get("title") or org.get("name")
        owner_type = org.get("type")
    
    return {
        "url": url,
        "name": name,
        "description": description,
        "country": country,
        "owner_name": owner_name,
        "owner_type": owner_type,
        "raw_record": record,  # Keep original for debugging
    }


def enrich_metadata_from_web(url: str, existing_metadata: Dict) -> Dict:
    """Enrich metadata by scraping the CKAN website."""
    enriched = existing_metadata.copy()
    
    if not HAS_BS4:
        logger.debug("BeautifulSoup4 not available, skipping web scraping")
        return enriched
    
    try:
        time.sleep(REQUEST_DELAY)
        response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15, verify=True)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract description from meta tags or page content
        if not enriched.get("description"):
            # Try meta description
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc and meta_desc.get("content"):
                enriched["description"] = meta_desc["content"].strip()
            else:
                # Try og:description
                og_desc = soup.find("meta", attrs={"property": "og:description"})
                if og_desc and og_desc.get("content"):
                    enriched["description"] = og_desc["content"].strip()
        
        # Extract title if not set
        if not enriched.get("name") or enriched["name"] == urlparse(url).netloc:
            title_tag = soup.find("title")
            if title_tag and title_tag.text:
                enriched["name"] = title_tag.text.strip()
        
        # Try to infer owner from page content
        if not enriched.get("owner_name"):
            # Look for common patterns
            org_patterns = [
                soup.find("meta", attrs={"property": "og:site_name"}),
                soup.find(class_=re.compile(r"organization|owner|publisher", re.I)),
            ]
            for pattern in org_patterns:
                if pattern:
                    if hasattr(pattern, "get") and pattern.get("content"):
                        enriched["owner_name"] = pattern.get("content").strip()
                        break
                    elif hasattr(pattern, "text") and pattern.text:
                        enriched["owner_name"] = pattern.text.strip()
                        break
        
    except RequestException as e:
        logger.debug(f"Could not scrape {url}: {e}")
    except Exception as e:
        logger.debug(f"Error scraping {url}: {e}")
    
    return enriched


def get_existing_entries() -> Tuple[Set[str], Set[str], Dict[str, str]]:
    """Load existing registry entries and return sets for duplicate detection.
    
    Returns:
        Tuple of (existing_ids, existing_urls, url_to_id)
    """
    existing_ids = set()
    existing_urls = set()
    url_to_id = {}
    
    # Load from full.jsonl
    full_jsonl_path = os.path.join(DATASETS_DIR, "full.jsonl")
    if os.path.exists(full_jsonl_path):
        try:
            full_data = load_jsonl(full_jsonl_path)
            for row in tqdm.tqdm(full_data, desc="Loading existing entries", unit="entry", leave=False):
                record_id = row.get("id")
                record_url = row.get("link")
                
                if record_id:
                    existing_ids.add(record_id)
                
                if record_url:
                    normalized_url = normalize_url(record_url)
                    normalized_domain = normalize_domain(record_url)
                    existing_urls.add(normalized_url)
                    existing_urls.add(normalized_domain)
                    if record_id:
                        url_to_id[normalized_url] = record_id
                        url_to_id[normalized_domain] = record_id
        except Exception as e:
            logger.warning(f"Error loading existing entries: {e}")
    
    # Also check scheduled directory
    if os.path.exists(SCHEDULED_DIR):
        # Collect all YAML files first for progress tracking
        yaml_files = []
        for root, dirs, files in os.walk(SCHEDULED_DIR):
            for filename in files:
                if filename.endswith(".yaml"):
                    yaml_files.append(os.path.join(root, filename))
        
        for filepath in tqdm.tqdm(yaml_files, desc="Loading scheduled entries", unit="file", leave=False):
            try:
                import yaml
                from yaml import CLoader as Loader
                with open(filepath, "r", encoding="utf8") as f:
                    record = yaml.load(f, Loader=Loader)
                    record_id = record.get("id")
                    record_url = record.get("link")
                    
                    if record_id:
                        existing_ids.add(record_id)
                    
                    if record_url:
                        normalized_url = normalize_url(record_url)
                        normalized_domain = normalize_domain(record_url)
                        existing_urls.add(normalized_url)
                        existing_urls.add(normalized_domain)
                        if record_id:
                            url_to_id[normalized_url] = record_id
                            url_to_id[normalized_domain] = record_id
            except Exception as e:
                logger.debug(f"Error reading {filepath}: {e}")
    
    logger.info(f"Loaded {len(existing_ids)} existing IDs and {len(existing_urls)} existing URLs/domains")
    return existing_ids, existing_urls, url_to_id


def check_duplicate(url: str, existing_urls: Set[str], existing_ids: Set[str], url_to_id: Dict[str, str]) -> Tuple[bool, Optional[str]]:
    """Check if a URL already exists in the registry."""
    normalized_url = normalize_url(url)
    normalized_domain = normalize_domain(url)
    
    # Check by normalized URL
    if normalized_url in existing_urls:
        existing_id = url_to_id.get(normalized_url)
        return True, existing_id
    
    # Check by domain
    if normalized_domain in existing_urls:
        existing_id = url_to_id.get(normalized_domain)
        return True, existing_id
    
    # Check if ID would conflict (generate ID from domain)
    domain = urlparse(url).netloc.lower()
    potential_id = domain.split(":", 1)[0].replace("_", "").replace("-", "").replace(".", "")
    if potential_id in existing_ids:
        return True, potential_id
    
    return False, None


@app.command()
def sync_ckan_ecosystem(
    dryrun: bool = typer.Option(False, "--dry-run", help="Dry run mode - don't create files"),
    scheduled: bool = typer.Option(True, "--scheduled/--entities", help="Add to scheduled or entities directory"),
    enrich: bool = typer.Option(True, "--enrich/--no-enrich", help="Enrich metadata from web scraping"),
    delay: float = typer.Option(1.0, "--delay", help="Delay between requests (seconds)"),
):
    """Synchronize CKAN websites from ecosystem.ckan.org dataset."""
    global REQUEST_DELAY
    REQUEST_DELAY = delay
    
    logger.info("Starting CKAN ecosystem synchronization...")
    if dryrun:
        logger.info("DRY RUN MODE - No files will be created")
    
    # Load existing entries
    existing_ids, existing_urls, url_to_id = get_existing_entries()
    
    # Fetch CKAN sites from ecosystem
    ckan_records = fetch_ckan_ecosystem_dataset()
    if not ckan_records:
        logger.error("No CKAN records found. Exiting.")
        return
    
    # Process records
    added = 0
    skipped = 0
    errors = 0
    
    for record in tqdm.tqdm(ckan_records, desc="Processing CKAN sites", unit="site"):
        try:
            # Parse record
            parsed = parse_ckan_site_record(record)
            if not parsed:
                skipped += 1
                continue
            
            url = parsed["url"]
            
            # Check for duplicates
            is_duplicate, existing_id = check_duplicate(url, existing_urls, existing_ids, url_to_id)
            if is_duplicate:
                logger.info(f"Skipping duplicate: {url} (existing: {existing_id})")
                skipped += 1
                continue
            
            # Enrich metadata from web if requested
            if enrich:
                parsed = enrich_metadata_from_web(url, parsed)
            
            # Prepare parameters for _add_single_entry
            if dryrun:
                logger.info(f"[DRYRUN] Would add: {parsed['name']} -> {url}")
                added += 1
            else:
                # Use builder.py function to add entry
                try:
                    builder._add_single_entry(
                        url=url,
                        software="ckan",
                        catalog_type="Open data portal",
                        name=parsed.get("name"),
                        description=parsed.get("description"),
                        country=parsed.get("country"),
                        owner_name=parsed.get("owner_name"),
                        owner_link=None,
                        owner_type=parsed.get("owner_type"),
                        scheduled=scheduled,
                        force=False,
                        preloaded=existing_ids,
                    )
                    added += 1
                    # Update existing sets to avoid duplicates in same run
                    domain = urlparse(url).netloc.lower()
                    potential_id = domain.split(":", 1)[0].replace("_", "").replace("-", "").replace(".", "")
                    existing_ids.add(potential_id)
                    normalized_url = normalize_url(url)
                    normalized_domain = normalize_domain(url)
                    url_to_id[normalized_url] = potential_id
                    url_to_id[normalized_domain] = potential_id
                    logger.info(f"Added: {parsed['name']} -> {url}")
                except Exception as e:
                    logger.error(f"Error adding {url}: {e}")
                    errors += 1
        
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            errors += 1
    
    # Summary
    logger.info("=" * 60)
    logger.info("Synchronization complete!")
    logger.info(f"Added: {added}")
    logger.info(f"Skipped (duplicates): {skipped}")
    logger.info(f"Errors: {errors}")
    logger.info("=" * 60)


if __name__ == "__main__":
    app()
