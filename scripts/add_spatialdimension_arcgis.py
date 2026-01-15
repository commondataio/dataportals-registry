#!/usr/bin/env python
# Script to add ArcGIS server instances from Spatial Dimension portals

import logging
import typer
import requests
import re
import os
import yaml
from urllib.parse import urlparse, urljoin
from typing import List, Dict, Optional
import time

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
SCHEDULED_DIR = os.path.join(_REPO_ROOT, "data", "scheduled")

app = typer.Typer()

SPATIAL_DIMENSION_PORTALS_URL = "https://www.spatialdimension.com/portals"

# Common ArcGIS server paths to try
ARCGIS_PATHS = [
    "/arcgis/rest/services",
    "/server/rest/services",
    "/arcgis/services",
    "/server/services",
    "/arcgis/rest/info",
    "/server/rest/info",
]


def extract_portal_urls(html_content: str) -> List[str]:
    """Extract portal URLs from the Spatial Dimension portals page."""
    urls = []
    
    # Look for portal links in the HTML
    # Pattern: href="https://..." or href="/portals/..."
    patterns = [
        r'href="(https?://[^"]+)"',
        r'href="(/portals/[^"]+)"',
        r'portal-government-link[^>]+href="([^"]+)"',
        r'portal-map-link[^>]+href="([^"]+)"',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, html_content, re.IGNORECASE)
        for match in matches:
            if match.startswith("http"):
                urls.append(match)
            elif match.startswith("/"):
                # Convert relative URLs to absolute
                base_url = "https://www.spatialdimension.com"
                urls.append(urljoin(base_url, match))
    
    # Also look for specific portal domains
    portal_domains = re.findall(r'portals\.landfolio\.com/[^/"]+', html_content, re.IGNORECASE)
    for domain_path in portal_domains:
        urls.append(f"https://{domain_path}")
    
    # Remove duplicates and filter
    unique_urls = list(set(urls))
    # Filter out non-portal URLs
    filtered_urls = [
        url for url in unique_urls
        if any(x in url.lower() for x in ["portal", "landfolio", "cadastre", "mining"])
        and not any(x in url.lower() for x in ["spatialdimension.com", "facebook", "twitter", "linkedin"])
    ]
    
    return filtered_urls


def normalize_arcgis_url(url: str) -> str:
    """Normalize ArcGIS URL to base /arcgis/rest/services format."""
    # Remove trailing backslashes and specific service paths
    url = url.rstrip("\\").rstrip("/")
    
    # If it's a specific MapServer, get the base services directory
    if "/MapServer" in url or "/FeatureServer" in url:
        # Extract up to /rest/services
        match = re.search(r'(https?://[^/]+/arcgis/rest/services)', url, re.IGNORECASE)
        if match:
            return match.group(1)
        match = re.search(r'(https?://[^/]+/server/rest/services)', url, re.IGNORECASE)
        if match:
            return match.group(1)
    
    # If it already ends with /rest/services, return as is
    if url.endswith("/rest/services") or url.endswith("/rest/services/"):
        return url.rstrip("/")
    
    return url


def find_arcgis_server(base_url: str, timeout: int = 10) -> Optional[str]:
    """Try to find ArcGIS server instance for a given portal URL."""
    parsed = urlparse(base_url)
    domain = parsed.netloc.replace("www.", "")
    
    # Skip ArcGIS Online basemap services (but allow tiles.arcgis.com for hosted services)
    if "arcgisonline.com" in domain:
        return None
    
    # Pattern 1: Try landfolio.{domain}/arcgis/rest/services (common pattern)
    if "landfolio.com" not in domain and "portals.landfolio.com" not in domain:
        # Try landfolio subdomain
        landfolio_domain = f"landfolio.{domain}"
        test_url = f"https://{landfolio_domain}/arcgis/rest/services"
        try:
            response = requests.get(test_url, timeout=timeout, verify=True, allow_redirects=True)
            if response.status_code == 200:
                content = response.text.lower()
                if "arcgis" in content and "services" in content:
                    # Make sure it's not just a redirect to ArcGIS Online
                    if "arcgisonline.com" not in response.url.lower():
                        return test_url
        except Exception:
            pass
    
    # Pattern 2: Try {domain}/arcgis/rest/services directly
    base = f"{parsed.scheme}://{domain}"
    for path in ARCGIS_PATHS:
        test_url = urljoin(base, path)
        try:
            response = requests.get(test_url, timeout=timeout, verify=True, allow_redirects=True)
            if response.status_code == 200:
                content = response.text.lower()
                # Check if it's an ArcGIS server response
                if any(indicator in content for indicator in ["arcgis", "services", "rest", "soap"]):
                    # Make sure it's not ArcGIS Online
                    if "arcgisonline.com" not in response.url.lower() and "services.arcgis.com" not in response.url.lower():
                        # Prefer /arcgis/rest/services format
                        if "/rest/services" in test_url:
                            return test_url
        except Exception as e:
            logger.debug(f"Failed to check {test_url}: {e}")
            continue
    
    # Pattern 3: Look for ArcGIS server URLs in the page content
    try:
        response = requests.get(base_url, timeout=timeout, verify=True)
        if response.status_code == 200:
            content = response.text
            # Look for ArcGIS server URLs in the page (excluding ArcGIS Online)
            arcgis_patterns = [
                r'https?://[^"\s]+/arcgis/rest/services[^"\s]*',
                r'https?://[^"\s]+/server/rest/services[^"\s]*',
            ]
            for pattern in arcgis_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                for match in matches:
                    # Filter out ArcGIS Online
                    if "arcgisonline.com" not in match.lower() and "services.arcgis.com" not in match.lower():
                        return match
    except Exception as e:
        logger.debug(f"Failed to check page content for {base_url}: {e}")
    
    return None


def generate_record_id(url: str) -> str:
    """Generate a record ID from URL."""
    parsed = urlparse(url)
    domain = parsed.netloc.replace("www.", "")
    path = parsed.path.strip("/").replace("/", "")
    # Remove special characters
    record_id = (domain + path).lower()
    record_id = re.sub(r'[^a-z0-9]', '', record_id)
    return record_id


def create_arcgis_server_record(arcgis_url: str, portal_url: str, portal_name: str = None) -> Dict:
    """Create a catalog record for an ArcGIS server instance."""
    parsed = urlparse(arcgis_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    
    # Determine country from domain TLD or portal URL
    country = "World"  # Default
    domain = parsed.netloc.lower()
    
    # Extract country from TLD (e.g., .sn, .jm, .ug)
    tld_country_map = {
        "sn": ("SN", "Senegal"),
        "jm": ("JM", "Jamaica"),
        "ug": ("UG", "Uganda"),
        "ke": ("KE", "Kenya"),
        "bw": ("BW", "Botswana"),
        "tz": ("TZ", "Tanzania"),
        "pk": ("PK", "Pakistan"),
        "tg": ("TG", "Togo"),
        "ci": ("CI", "Côte d'Ivoire"),
        "et": ("ET", "Ethiopia"),
        "gn": ("GN", "Guinea"),
        "ss": ("SS", "South Sudan"),
        "ao": ("AO", "Angola"),
        "cm": ("CM", "Cameroon"),
        "pg": ("PG", "Papua New Guinea"),
        "mz": ("MZ", "Mozambique"),
        "mw": ("MW", "Malawi"),
        "zm": ("ZM", "Zambia"),
        "mr": ("MR", "Mauritania"),
        "na": ("NA", "Namibia"),
    }
    
    # Try to extract from domain
    country_name = country
    for tld, (code, name) in tld_country_map.items():
        if domain.endswith(f".{tld}") or domain.endswith(f".{tld}/"):
            country = code
            country_name = name
            break
    
    # Also try from portal URL
    if country == "World" and portal_url:
        portal_parsed = urlparse(portal_url)
        portal_domain = portal_parsed.netloc.lower()
        for tld, (code, name) in tld_country_map.items():
            if portal_domain.endswith(f".{tld}") or portal_domain.endswith(f".{tld}/"):
                country = code
                country_name = name
                break
    
    record_id = generate_record_id(arcgis_url)
    
    # Build endpoints
    endpoints = []
    if "/rest/services" in arcgis_url:
        base_rest = arcgis_url.split("/rest/services")[0] + "/rest"
        endpoints.extend([
            {"type": "arcgis:rest:info", "url": f"{base_rest}/info?f=pjson"},
            {"type": "arcgis:rest:services", "url": f"{base_rest}/services?f=pjson"},
            {"type": "arcgis:soap", "url": f"{base_rest.replace('/rest', '')}/services?wsdl"},
            {"type": "arcgis:sitemap", "url": f"{base_rest}/services?f=sitemap"},
            {"type": "arcgis:geositemap", "url": f"{base_rest}/services?f=geositemap"},
            {"type": "arcgis:kmz", "url": f"{base_rest}/services?f=kmz"},
        ])
    else:
        # Fallback endpoints
        endpoints.append({"type": "arcgis:rest:services", "url": arcgis_url})
    
    record = {
        "access_mode": ["open"],
        "api": True,
        "api_status": "active",
        "catalog_type": "Geoportal",
        "content_types": ["dataset", "map_layer"],
        "coverage": [{
            "location": {
                "country": {"id": country, "name": country_name},
                "level": 20 if country != "World" else 10,
            }
        }],
        "description": f"ArcGIS Server instance providing geospatial services. Associated with Spatial Dimension portal: {portal_url}",
        "endpoints": endpoints,
        "id": record_id,
        "langs": [{"id": "EN", "name": "English"}],
        "link": arcgis_url,
        "name": f"ArcGIS Server - {parsed.netloc}",
        "owner": {
            "location": {
                "country": {"id": country, "name": country_name},
                "level": 20 if country != "World" else 10,
            },
            "name": "Spatial Dimension",
            "type": "Business",
            "link": "https://www.spatialdimension.com"
        },
        "properties": {"has_doi": False},
        "rights": {
            "license_id": None,
            "license_name": None,
            "license_url": None,
            "privacy_policy_url": None,
            "rights_type": "global",
            "tos_url": None
        },
        "software": {
            "id": "arcgisserver",
            "name": "ArcGIS Server"
        },
        "status": "active",
        "tags": ["ArcGIS", "arcgisserver", "geospatial", "GIS", "REST", "Spatial Dimension"],
        "topics": [
            {"id": "GOVE", "name": "Government and public sector", "type": "eudatatheme"},
            {"id": "REGI", "name": "Regions and cities", "type": "eudatatheme"},
        ],
    }
    
    return record


@app.command()
def fetch_portals(
    output_file: str = typer.Option("spatialdimension_portals.txt", "--output", help="Output file for portal URLs"),
    dryrun: bool = typer.Option(False, "--dry-run", help="Dry run mode"),
):
    """Fetch portal URLs from Spatial Dimension website."""
    logger.info(f"Fetching portals from {SPATIAL_DIMENSION_PORTALS_URL}")
    
    try:
        response = requests.get(SPATIAL_DIMENSION_PORTALS_URL, timeout=30, verify=True)
        response.raise_for_status()
        html_content = response.text
        
        urls = extract_portal_urls(html_content)
        logger.info(f"Found {len(urls)} portal URLs")
        
        if not dryrun:
            with open(output_file, "w", encoding="utf-8") as f:
                for url in sorted(set(urls)):
                    f.write(f"{url}\n")
            logger.info(f"Saved {len(urls)} URLs to {output_file}")
        else:
            logger.info("DRY RUN - Would save URLs:")
            for url in sorted(set(urls))[:10]:
                logger.info(f"  {url}")
            if len(urls) > 10:
                logger.info(f"  ... and {len(urls) - 10} more")
        
        return urls
    
    except Exception as e:
        logger.error(f"Error fetching portals: {e}")
        return []


@app.command()
def find_arcgis_servers(
    portals_file: str = typer.Option("spatialdimension_portals.txt", "--portals-file", help="File with portal URLs"),
    output_file: str = typer.Option("spatialdimension_arcgis.txt", "--output", help="Output file for ArcGIS server URLs"),
    delay: float = typer.Option(1.0, "--delay", help="Delay between requests (seconds)"),
    dryrun: bool = typer.Option(False, "--dry-run", help="Dry run mode"),
):
    """Find ArcGIS server instances for each portal."""
    
    if not os.path.exists(portals_file):
        logger.error(f"Portals file not found: {portals_file}")
        logger.info("Run 'fetch-portals' command first to generate the portals file")
        return
    
    portals = []
    with open(portals_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                portals.append(line)
    
    logger.info(f"Checking {len(portals)} portals for ArcGIS servers...")
    
    arcgis_servers = []
    
    for i, portal_url in enumerate(portals, 1):
        logger.info(f"[{i}/{len(portals)}] Checking {portal_url}...")
        arcgis_url = find_arcgis_server(portal_url)
        
        if arcgis_url:
            # Normalize the URL
            normalized_url = normalize_arcgis_url(arcgis_url)
            logger.info(f"  ✓ Found ArcGIS server: {normalized_url}")
            arcgis_servers.append((portal_url, normalized_url))
        else:
            logger.info(f"  ✗ No ArcGIS server found")
        
        if delay > 0 and i < len(portals):
            time.sleep(delay)
    
    logger.info(f"\nFound {len(arcgis_servers)} ArcGIS servers out of {len(portals)} portals")
    
    if not dryrun and arcgis_servers:
        with open(output_file, "w", encoding="utf-8") as f:
            for portal_url, arcgis_url in arcgis_servers:
                f.write(f"{arcgis_url}\t{portal_url}\n")
        logger.info(f"Saved results to {output_file}")
    
    return arcgis_servers


@app.command()
def add_to_registry(
    arcgis_file: str = typer.Option("spatialdimension_arcgis.txt", "--arcgis-file", help="File with ArcGIS server URLs"),
    scheduled: bool = typer.Option(True, "--scheduled/--entities", help="Add to scheduled or entities directory"),
    dryrun: bool = typer.Option(False, "--dry-run", help="Dry run mode"),
):
    """Add ArcGIS server instances to the registry."""
    
    if not os.path.exists(arcgis_file):
        logger.error(f"ArcGIS file not found: {arcgis_file}")
        logger.info("Run 'find-arcgis-servers' command first")
        return
    
    arcgis_servers = []
    with open(arcgis_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                parts = line.split("\t")
                if len(parts) >= 1:
                    arcgis_url = parts[0]
                    portal_url = parts[1] if len(parts) > 1 else ""
                    arcgis_servers.append((arcgis_url, portal_url))
    
    logger.info(f"Adding {len(arcgis_servers)} ArcGIS server instances to registry...")
    
    target_dir = SCHEDULED_DIR if scheduled else ROOT_DIR
    added = 0
    skipped = 0
    
    for arcgis_url, portal_url in arcgis_servers:
        record = create_arcgis_server_record(arcgis_url, portal_url)
        record_id = record["id"]
        
        # Determine file path
        country = record["coverage"][0]["location"]["country"]["id"]
        country_dir = os.path.join(target_dir, country)
        if not os.path.exists(country_dir):
            if not dryrun:
                os.makedirs(country_dir, exist_ok=True)
        
        subdir = "geo"  # Geoportal
        subdir_path = os.path.join(country_dir, subdir)
        if not os.path.exists(subdir_path):
            if not dryrun:
                os.makedirs(subdir_path, exist_ok=True)
        
        filepath = os.path.join(subdir_path, f"{record_id}.yaml")
        
        if os.path.exists(filepath):
            logger.info(f"  ⚠ Skipping {record_id} - already exists")
            skipped += 1
        else:
            if not dryrun:
                with open(filepath, "w", encoding="utf-8") as f:
                    yaml.dump(record, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
                logger.info(f"  ✓ Added {record_id}")
                added += 1
            else:
                logger.info(f"  [DRY RUN] Would add {record_id}")
                added += 1
    
    logger.info(f"\n=== Summary ===")
    logger.info(f"Added: {added}")
    logger.info(f"Skipped: {skipped}")
    if dryrun:
        logger.info("DRY RUN - No files were created")


@app.command()
def run_all(
    delay: float = typer.Option(1.0, "--delay", help="Delay between requests"),
    scheduled: bool = typer.Option(True, "--scheduled/--entities", help="Add to scheduled or entities"),
    dryrun: bool = typer.Option(False, "--dry-run", help="Dry run mode"),
):
    """Run the complete workflow: fetch portals, find ArcGIS servers, and add to registry."""
    logger.info("=== Step 1: Fetching portals ===")
    portals = fetch_portals(dryrun=dryrun)
    
    if not portals:
        logger.error("No portals found. Aborting.")
        return
    
    logger.info("\n=== Step 2: Finding ArcGIS servers ===")
    arcgis_servers = find_arcgis_servers(delay=delay, dryrun=dryrun)
    
    if not arcgis_servers:
        logger.warning("No ArcGIS servers found. Aborting.")
        return
    
    logger.info("\n=== Step 3: Adding to registry ===")
    add_to_registry(scheduled=scheduled, dryrun=dryrun)


if __name__ == "__main__":
    app()

