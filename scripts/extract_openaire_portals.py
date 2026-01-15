#!/usr/bin/env python
# Script to extract list of all data portals registered in OpenAIRE
#
# This script attempts to extract data portals/repositories from OpenAIRE using multiple methods:
# 1. GraphQL API - queries OpenAIRE Graph API
# 2. REST API - queries various REST endpoints
# 3. OAI-PMH - queries OAI-PMH endpoint
# 4. Explore API - queries OpenAIRE Explore API
# 5. Web Scraping - fallback method to scrape OpenAIRE websites
#
# Note: OpenAIRE's API structure may change, and some endpoints may require authentication.
# The script will try all available methods and combine results.
#
# Usage:
#   python extract_openaire_portals.py fetch-portals --output openaire_portals.txt
#   python extract_openaire_portals.py fetch-portals --method web --dry-run
#   python extract_openaire_portals.py list-sources --output openaire_portals.json

import logging
import typer
import requests
import json
import os
import time
import re
from urllib.parse import urlparse
from typing import List, Dict, Optional, Set
from requests.exceptions import RequestException, Timeout

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

app = typer.Typer()

# OpenAIRE API endpoints
OPENAIRE_API_BASE = "https://api.openaire.eu"
OPENAIRE_GRAPH_API = "https://graph.openaire.eu"
OPENAIRE_GRAPHQL_API = f"{OPENAIRE_GRAPH_API}/graphql"

# OpenAIRE REST API endpoints (various versions)
OPENAIRE_SEARCH_API = f"{OPENAIRE_API_BASE}/search"
OPENAIRE_DATA_SOURCES_API = f"{OPENAIRE_API_BASE}/datasources"

# OpenAIRE OAI-PMH endpoint for listing data sources
OPENAIRE_OAI_PMH = f"{OPENAIRE_API_BASE}/oai"

# OpenAIRE Explore API (if available)
OPENAIRE_EXPLORE_API = "https://explore.openaire.eu"

# User agent for requests
USER_AGENT = "Mozilla/5.0 (compatible; DataPortalsRegistry/1.0; +https://github.com/commondataio/dataportals-registry)"


def query_openaire_graphql(query: str, variables: Dict = None, timeout: int = 30) -> Optional[Dict]:
    """Query OpenAIRE GraphQL API."""
    headers = {
        "User-Agent": USER_AGENT,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    
    try:
        response = requests.post(
            OPENAIRE_GRAPHQL_API,
            json=payload,
            headers=headers,
            timeout=timeout,
            verify=True
        )
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logger.debug(f"Error querying OpenAIRE GraphQL API: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.debug(f"Error parsing JSON response from GraphQL API: {e}")
        return None


def query_openaire_rest_api(endpoint: str, params: Dict = None, timeout: int = 30) -> Optional[Dict]:
    """Query OpenAIRE REST API endpoint."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }
    
    try:
        response = requests.get(endpoint, params=params, headers=headers, timeout=timeout, verify=True)
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logger.debug(f"Error querying OpenAIRE API {endpoint}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.debug(f"Error parsing JSON response from {endpoint}: {e}")
        return None


def query_openaire_oai_pmh(verb: str = "ListIdentifiers", metadata_prefix: str = "oai_dc", 
                           set: str = None, timeout: int = 30) -> Optional[List[Dict]]:
    """Query OpenAIRE OAI-PMH endpoint."""
    params = {
        "verb": verb,
        "metadataPrefix": metadata_prefix,
    }
    if set:
        params["set"] = set
    
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/xml",
    }
    
    try:
        response = requests.get(OPENAIRE_OAI_PMH, params=params, headers=headers, timeout=timeout, verify=True)
        response.raise_for_status()
        
        # Parse XML response (basic parsing)
        import xml.etree.ElementTree as ET
        root = ET.fromstring(response.content)
        
        # Extract identifiers
        identifiers = []
        for identifier in root.findall(".//{http://www.openarchives.org/OAI/2.0/}identifier"):
            if identifier.text:
                identifiers.append({"identifier": identifier.text})
        
        return identifiers
    except RequestException as e:
        logger.error(f"Error querying OpenAIRE OAI-PMH: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing OAI-PMH response: {e}")
        return None


def extract_data_sources_from_graphql() -> List[Dict]:
    """Extract data sources from OpenAIRE GraphQL API."""
    data_sources = []
    
    # GraphQL query to get datasources/repositories
    query = """
    query {
      datasources {
        id
        name
        url
        description
      }
    }
    """
    
    logger.info("Querying OpenAIRE GraphQL API for datasources")
    result = query_openaire_graphql(query)
    
    if result and "data" in result:
        if "datasources" in result["data"]:
            data_sources = result["data"]["datasources"]
        elif "repositories" in result["data"]:
            data_sources = result["data"]["repositories"]
    
    return data_sources


def extract_data_sources_from_search_api(max_pages: int = 50) -> List[Dict]:
    """Extract data sources from OpenAIRE search API by querying datasets and extracting datasources."""
    data_sources = []
    seen_ids = set()
    
    # Query datasets to extract datasources from the collectedfrom field
    # OpenAIRE Search API endpoint for datasets
    datasets_endpoint = f"{OPENAIRE_API_BASE}/search/datasets"
    
    # Start with a small sample to get datasources
    params = {
        "size": 100,  # Get 100 datasets per page
        "format": "json",
        "page": 1,
    }
    
    logger.info("Querying OpenAIRE Search API for datasets to extract datasources...")
    
    # Query multiple pages to get a comprehensive list
    # Note: OpenAIRE has rate limits (60 requests/hour unauthenticated)
    for page in range(1, max_pages + 1):
        params["page"] = page
        logger.info(f"Fetching page {page}...")
        
        result = query_openaire_rest_api(datasets_endpoint, params=params)
        
        if not result:
            break
        
        # Parse the OpenAIRE response structure
        response = result.get("response", {})
        results = response.get("results", {})
        result_list = results.get("result", [])
        
        if not result_list:
            logger.info(f"No more results at page {page}")
            break
        
        # Extract datasources from each dataset
        page_sources = 0
        for dataset in result_list:
            metadata = dataset.get("metadata", {})
            entity = metadata.get("oaf:entity", {})
            result_data = entity.get("oaf:result", {})
            
            # Handle both single and list formats for collectedfrom
            collected_from = result_data.get("collectedfrom", [])
            if not isinstance(collected_from, list):
                collected_from = [collected_from]
            
            for source in collected_from:
                if isinstance(source, dict):
                    source_id = source.get("@id", "")
                    source_name = source.get("@name", "")
                    
                    # Skip if we've seen this datasource already
                    if source_id and source_id not in seen_ids:
                        seen_ids.add(source_id)
                        data_sources.append({
                            "id": source_id,
                            "name": source_name,
                            "identifier": source_id,
                        })
                        page_sources += 1
        
        logger.info(f"  Page {page}: Found {page_sources} new datasources (total: {len(data_sources)})")
        
        # Check if there are more pages
        header = response.get("header", {})
        total = int(header.get("total", {}).get("$", 0))
        size = int(header.get("size", {}).get("$", 100))
        current_page = int(header.get("page", {}).get("$", 1))
        
        if current_page * size >= total or len(result_list) < size:
            logger.info(f"Reached end of results at page {page}")
            break
        
        # Rate limiting - be respectful
        time.sleep(0.5)
    
    logger.info(f"Extracted {len(data_sources)} unique datasources from datasets")
    return data_sources


def extract_data_sources_from_datasources_api() -> List[Dict]:
    """Extract data sources from OpenAIRE datasources API endpoint."""
    logger.info("Querying OpenAIRE datasources API")
    result = query_openaire_rest_api(OPENAIRE_DATA_SOURCES_API)
    
    data_sources = []
    if result:
        # Handle different response structures
        if isinstance(result, list):
            data_sources = result
        elif isinstance(result, dict):
            if "datasources" in result:
                data_sources = result["datasources"]
            elif "results" in result:
                data_sources = result["results"]
            elif "data" in result:
                data_sources = result["data"]
            else:
                # Try to extract from any list-like structure
                for key, value in result.items():
                    if isinstance(value, list):
                        data_sources = value
                        break
    
    return data_sources


def get_datasource_url_from_id(source_id: str) -> Optional[str]:
    """Try to construct or find URL for a datasource from its ID."""
    # OpenAIRE datasource IDs often contain hints about the source
    # Try to extract URL patterns
    
    # Some common patterns in OpenAIRE IDs
    if "doi" in source_id.lower():
        # DOI-based sources
        doi_match = re.search(r'10\.\d+/[^\s]+', source_id)
        if doi_match:
            return f"https://doi.org/{doi_match.group()}"
    
    if "zenodo" in source_id.lower():
        return "https://zenodo.org"
    
    if "dataverse" in source_id.lower():
        # Try to extract dataverse URL
        return None  # Would need more info
    
    # For fairsharing IDs, try to construct URL
    if "fairsharing" in source_id.lower():
        fairsharing_id = source_id.split("::")[-1] if "::" in source_id else source_id
        return f"https://fairsharing.org/{fairsharing_id}"
    
    return None


def extract_from_openaire_explore_api() -> List[Dict]:
    """Extract data sources from OpenAIRE Explore API."""
    data_sources = []
    
    # Try OpenAIRE Explore API endpoints
    explore_endpoints = [
        f"{OPENAIRE_EXPLORE_API}/api/datasources",
        f"{OPENAIRE_EXPLORE_API}/api/repositories",
        f"{OPENAIRE_EXPLORE_API}/api/data-providers",
    ]
    
    for endpoint in explore_endpoints:
        logger.debug(f"Trying OpenAIRE Explore API: {endpoint}")
        result = query_openaire_rest_api(endpoint)
        
        if result:
            if isinstance(result, list):
                data_sources.extend(result)
            elif isinstance(result, dict):
                if "datasources" in result:
                    data_sources.extend(result["datasources"])
                elif "repositories" in result:
                    data_sources.extend(result["repositories"])
                elif "data" in result:
                    data_sources.extend(result["data"])
    
    return data_sources


def extract_from_openaire_website() -> List[Dict]:
    """Extract data sources by scraping OpenAIRE website (fallback method)."""
    data_sources = []
    
    # OpenAIRE Explore page might list data sources
    explore_urls = [
        "https://explore.openaire.eu/",
        "https://www.openaire.eu/data-providers",
        "https://www.openaire.eu/repositories",
        "https://www.openaire.eu/participating-repositories",
    ]
    
    for url in explore_urls:
        try:
            logger.debug(f"Attempting to scrape {url}")
            headers = {"User-Agent": USER_AGENT}
            response = requests.get(url, headers=headers, timeout=30, verify=True)
            response.raise_for_status()
            
            # Try to find JSON-LD or structured data
            json_ld_pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
            matches = re.findall(json_ld_pattern, response.text, re.DOTALL)
            for match in matches:
                try:
                    data = json.loads(match)
                    if isinstance(data, dict) and "url" in data:
                        data_sources.append(data)
                    elif isinstance(data, list):
                        data_sources.extend([d for d in data if isinstance(d, dict) and "url" in d])
                except:
                    pass
            
            # Look for links that might be data sources
            # This is a basic approach - could be improved with BeautifulSoup
            url_pattern = r'https?://[^\s"<>]+'
            urls = re.findall(url_pattern, response.text)
            for found_url in urls:
                if any(x in found_url.lower() for x in ["repository", "datasource", "data", "archive", "dataverse", "zenodo"]):
                    parsed = urlparse(found_url)
                    if parsed.netloc and parsed.netloc not in ["openaire.eu", "explore.openaire.eu", "api.openaire.eu", "graph.openaire.eu"]:
                        data_sources.append({
                            "url": found_url,
                            "name": parsed.netloc,
                        })
        except Exception as e:
            logger.debug(f"Error scraping {url}: {e}")
            continue
    
    return data_sources


@app.command()
def fetch_portals(
    output_file: str = typer.Option("openaire_portals.txt", "--output", help="Output file for portal URLs"),
    method: str = typer.Option("rest", "--method", help="Extraction method: graphql, rest, oai, explore, web, all"),
    dryrun: bool = typer.Option(False, "--dry-run", help="Dry run mode"),
    max_pages: int = typer.Option(50, "--max-pages", help="Maximum number of pages to fetch from OpenAIRE (default: 50)"),
):
    """Fetch data portal URLs from OpenAIRE."""
    logger.info("Starting OpenAIRE data portal extraction")
    
    all_sources = []
    seen_urls = set()
    
    # Try GraphQL API method
    if method in ["graphql", "all"]:
        logger.info("=== Method 1: GraphQL API ===")
        sources = extract_data_sources_from_graphql()
        
        for source in sources:
            url = source.get("url") or source.get("link") or source.get("identifier")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_sources.append(source)
        
        logger.info(f"Found {len(sources)} sources via GraphQL API")
    
    # Try REST API method
    if method in ["rest", "all"]:
        logger.info("=== Method 2: REST API ===")
        sources = extract_data_sources_from_datasources_api()
        # Extract from search API - this is the main working method
        search_sources = extract_data_sources_from_search_api(max_pages=max_pages)
        sources.extend(search_sources)
        
        for source in sources:
            url = source.get("url") or source.get("link") or source.get("identifier")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_sources.append(source)
        
        logger.info(f"Found {len(sources)} sources via REST API")
    
    # Try OAI-PMH method
    if method in ["oai", "all"]:
        logger.info("=== Method 3: OAI-PMH ===")
        identifiers = query_openaire_oai_pmh()
        
        if identifiers:
            for item in identifiers:
                identifier = item.get("identifier", "")
                if identifier and identifier not in seen_urls:
                    seen_urls.add(identifier)
                    all_sources.append({
                        "url": identifier,
                        "name": identifier,
                        "identifier": identifier,
                    })
        
        logger.info(f"Found {len(identifiers) if identifiers else 0} sources via OAI-PMH")
    
    # Try Explore API method
    if method in ["explore", "all"]:
        logger.info("=== Method 4: Explore API ===")
        sources = extract_from_openaire_explore_api()
        
        for source in sources:
            url = source.get("url") or source.get("link")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_sources.append(source)
        
        logger.info(f"Found {len(sources)} sources via Explore API")
    
    # Try web scraping method (fallback)
    if method in ["web", "all"]:
        logger.info("=== Method 5: Web Scraping ===")
        sources = extract_from_openaire_website()
        
        for source in sources:
            url = source.get("url")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_sources.append(source)
        
        logger.info(f"Found {len(sources)} sources via web scraping")
    
    # Deduplicate and normalize
    unique_sources = []
    seen = set()
    
    for source in all_sources:
        url = source.get("url") or source.get("link")
        
        # If no URL, try to get it from identifier
        if not url and source.get("identifier"):
            url = get_datasource_url_from_id(source.get("identifier"))
            if url:
                source["url"] = url
        
        # If still no URL, use identifier as fallback
        if not url:
            identifier = source.get("identifier", "")
            if identifier:
                # For OpenAIRE IDs, we might not have a direct URL
                # Store the identifier for reference
                source["identifier"] = identifier
                # Skip sources without URLs for now, or use identifier
                if not any(x in identifier.lower() for x in ["http", "www", "."]):
                    continue
        
        if not url:
            continue
        
        # Normalize URL
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"
        
        # Remove trailing slashes for comparison
        url_normalized = url.rstrip("/").lower()
        
        if url_normalized not in seen:
            seen.add(url_normalized)
            source["url"] = url
            unique_sources.append(source)
    
    logger.info(f"\n=== Summary ===")
    logger.info(f"Total unique data sources found: {len(unique_sources)}")
    
    if not dryrun:
        # Save to file
        with open(output_file, "w", encoding="utf-8") as f:
            for source in unique_sources:
                url = source.get("url", "")
                name = source.get("name", "")
                identifier = source.get("identifier", "")
                
                # Write in tab-separated format: URL, Name, Identifier
                f.write(f"{url}\t{name}\t{identifier}\n")
        
        logger.info(f"Saved {len(unique_sources)} data sources to {output_file}")
    else:
        logger.info("DRY RUN - Would save sources:")
        for source in unique_sources[:20]:
            logger.info(f"  {source.get('url')} - {source.get('name', 'N/A')}")
        if len(unique_sources) > 20:
            logger.info(f"  ... and {len(unique_sources) - 20} more")
    
    return unique_sources


@app.command()
def list_sources(
    output_file: str = typer.Option("openaire_portals.json", "--output", help="Output JSON file"),
    dryrun: bool = typer.Option(False, "--dry-run", help="Dry run mode"),
):
    """List all data sources in JSON format."""
    sources = fetch_portals(method="all", dryrun=True)
    
    if not dryrun:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(sources, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(sources)} sources to {output_file}")
    else:
        logger.info(f"Would save {len(sources)} sources to {output_file}")
        logger.info("Sample sources:")
        for source in sources[:5]:
            logger.info(f"  {json.dumps(source, indent=2)}")


if __name__ == "__main__":
    app()

