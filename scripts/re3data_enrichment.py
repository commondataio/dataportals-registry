#!/usr/bin/env python
# This script enriches catalog entries with metadata from re3data.org

import logging
import typer
import requests
import json
import os
import yaml
import time
import re
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse
from requests.exceptions import RequestException, Timeout
from datetime import datetime

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    logger = logging.getLogger(__name__)
    logger.warning("BeautifulSoup4 not available, will use basic HTML parsing")

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
RE3DATA_CACHE_FILE = os.path.join(CACHE_DIR, "re3data_repositories.json")

# Re3Data API endpoint (if available)
RE3DATA_API_BASE = "https://www.re3data.org/api/v1"
RE3DATA_BASE_URL = "https://www.re3data.org"

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
    return f"{RE3DATA_BASE_URL}/repository/{re3data_id}"


def fetch_re3data_page(re3data_id: str, timeout: int = 10) -> Optional[str]:
    """Fetch re3data repository page HTML."""
    url = get_re3data_url(re3data_id)
    
    try:
        response = requests.get(url, timeout=timeout, verify=True, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; dataportals-registry/1.0)'
        })
        response.raise_for_status()
        return response.text
    except (RequestException, Timeout) as e:
        logger.warning(f"Could not fetch re3data page for {re3data_id}: {e}")
        return None


def parse_re3data_html(html_content: str, re3data_id: str) -> Dict[str, Any]:
    """Parse re3data HTML page and extract metadata."""
    if not html_content:
        return {}
    
    result = {
        "re3data_id": re3data_id,
        "keywords": [],
        "content_type": [],
        "contact_email": None,
        "description": None,
        "persistent_identifiers": [],
        "software": [],
        "versioning": None,
        "institutions": [],
        "repository_type": None,
        "last_updated": datetime.now().isoformat(),
        "subjects": [],
        "database_access": None,
        "data_access": [],
        "open_access": None,
        "database_licenses": [],
        "data_policy": None,
        "privacy_policy": None,
        "standards": [],
        "certifications": [],
        "repository_size": None,
        "launch_date": None,
        "apis": [],
        "protocols": []
    }
    
    if HAS_BS4:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract description - try multiple approaches
        desc_elem = (soup.find('div', class_=re.compile('description', re.I)) or 
                    soup.find('p', class_=re.compile('description', re.I)) or
                    soup.find('div', {'id': re.compile('description', re.I)}) or
                    soup.find('section', {'id': re.compile('description', re.I)}))
        if desc_elem:
            result["description"] = desc_elem.get_text(strip=True, separator=' ')
        
        # Extract keywords - look for keywords section or subject areas
        keywords_section = (soup.find('div', {'id': re.compile('keyword|subject', re.I)}) or 
                           soup.find('section', {'id': re.compile('keyword|subject', re.I)}) or
                           soup.find('div', class_=re.compile('keyword|subject', re.I)))
        if keywords_section:
            keyword_tags = keywords_section.find_all(['span', 'a', 'li', 'dd', 'dt'], class_=re.compile('keyword|tag|subject', re.I))
            if not keyword_tags:
                # Try to find all links or spans in the section
                keyword_tags = keywords_section.find_all(['a', 'span', 'li'])
            for tag in keyword_tags:
                keyword = tag.get_text(strip=True)
                if keyword and len(keyword) > 2:  # Filter out very short strings
                    result["keywords"].append(keyword)
        
        # Extract content types - look for content type or data type sections
        content_type_section = (soup.find('div', {'id': re.compile('content.*type|data.*type', re.I)}) or 
                               soup.find('section', {'id': re.compile('content.*type|data.*type', re.I)}) or
                               soup.find('div', class_=re.compile('content.*type|data.*type', re.I)))
        if content_type_section:
            content_tags = content_type_section.find_all(['span', 'li', 'dd', 'dt', 'a'])
            for tag in content_tags:
                content_type = tag.get_text(strip=True)
                if content_type and len(content_type) > 2:
                    result["content_type"].append(content_type)
        
        # Extract contact email - look for email links or contact sections
        email_links = soup.find_all('a', href=re.compile(r'^mailto:', re.I))
        if email_links:
            # Get first email
            email = email_links[0].get('href', '').replace('mailto:', '').strip()
            if email:
                result["contact_email"] = email
        else:
            # Try to find email in text
            email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
            email_match = email_pattern.search(html_content)
            if email_match:
                result["contact_email"] = email_match.group(0)
        
        # Extract persistent identifiers - look for PID section
        pid_section = (soup.find('div', {'id': re.compile('persistent.*identifier|pid', re.I)}) or 
                      soup.find('section', {'id': re.compile('persistent.*identifier|pid', re.I)}) or
                      soup.find('div', class_=re.compile('persistent.*identifier|pid', re.I)))
        if pid_section:
            pid_items = pid_section.find_all(['li', 'dd', 'dt', 'span', 'a'])
            for item in pid_items:
                pid_text = item.get_text(strip=True)
                if pid_text and any(pid in pid_text.lower() for pid in ['doi', 'handle', 'ark', 'urn', 'isbn', 'isni', 'orcid']):
                    result["persistent_identifiers"].append(pid_text)
        
        # Extract software information - look for software section
        software_section = (soup.find('div', {'id': re.compile('software', re.I)}) or 
                           soup.find('section', {'id': re.compile('software', re.I)}) or
                           soup.find('div', class_=re.compile('software', re.I)))
        if software_section:
            software_items = software_section.find_all(['li', 'dd', 'dt', 'span', 'a'])
            for item in software_items:
                software_text = item.get_text(strip=True)
                if software_text and len(software_text) > 2:
                    result["software"].append(software_text)
        
        # Extract versioning information
        versioning_section = (soup.find('div', {'id': re.compile('version', re.I)}) or 
                             soup.find('section', {'id': re.compile('version', re.I)}) or
                             soup.find('div', class_=re.compile('version', re.I)))
        if versioning_section:
            versioning_text = versioning_section.get_text(strip=True).lower()
            result["versioning"] = any(word in versioning_text for word in ['yes', 'true', 'supported', 'enabled', 'available'])
        
        # Extract institutions - look for institutions tab or section
        institutions_section = (soup.find('div', {'id': re.compile('institution', re.I)}) or 
                               soup.find('section', {'id': re.compile('institution', re.I)}) or
                               soup.find('div', class_=re.compile('institution', re.I)) or
                               soup.find('div', {'id': 'tab_institutions'}))
        if institutions_section:
            inst_items = institutions_section.find_all(['li', 'dd', 'dt', 'tr'])
            for item in inst_items:
                inst_link = item.find('a')
                if inst_link:
                    inst_name = inst_link.get_text(strip=True)
                    inst_url = inst_link.get('href', '')
                    if inst_name and len(inst_name) > 2:
                        # Make URL absolute if relative
                        if inst_url and not inst_url.startswith('http'):
                            if inst_url.startswith('/'):
                                inst_url = f"{RE3DATA_BASE_URL}{inst_url}"
                            else:
                                inst_url = None
                        result["institutions"].append({
                            "name": inst_name,
                            "url": inst_url
                        })
                else:
                    # Try to extract institution name from text
                    inst_text = item.get_text(strip=True)
                    if inst_text and len(inst_text) > 2:
                        result["institutions"].append({
                            "name": inst_text,
                            "url": None
                        })
        
        # Extract repository type - look for repository type or category
        repo_type_section = (soup.find('div', {'id': re.compile('repository.*type|type.*repository', re.I)}) or 
                            soup.find('section', {'id': re.compile('repository.*type|type.*repository', re.I)}) or
                            soup.find('div', class_=re.compile('repository.*type|type.*repository', re.I)) or
                            soup.find('span', class_=re.compile('repository.*type', re.I)))
        if repo_type_section:
            result["repository_type"] = repo_type_section.get_text(strip=True)
        
        # Extract subjects/disciplines - look for subject labels
        subject_links = soup.find_all('a', class_=re.compile('label.*subject|subject.*label', re.I))
        for link in subject_links:
            subject_name = link.get_text(strip=True)
            subject_id = link.get('href', '')
            # Extract subject ID from href if available
            subject_id_match = re.search(r'subjects\[\]=(\d+)', subject_id)
            subject_id_value = subject_id_match.group(1) if subject_id_match else None
            
            # Try to extract hierarchical path from title
            title = link.get('title', '')
            path = []
            if '>' in title:
                path = [p.strip() for p in title.split('>')]
            
            if subject_name and len(subject_name) > 2:
                subject_obj = {
                    "name": subject_name,
                    "id": subject_id_value
                }
                if path:
                    subject_obj["path"] = path
                result["subjects"].append(subject_obj)
        
        # Extract open access status - look for open access icon
        open_access_icon = soup.find('div', class_=re.compile('icon.*open.*access', re.I))
        if open_access_icon:
            result["open_access"] = 'active' in open_access_icon.get('class', []) or 'inactive' not in open_access_icon.get('class', [])
        
        # Extract database access type
        db_access_headers = soup.find_all('h2', string=re.compile('Database access', re.I))
        for header in db_access_headers:
            # Look for access type in next sibling or parent
            next_elem = header.find_next_sibling(['div', 'p', 'dd', 'dt'])
            if next_elem:
                db_access_text = next_elem.get_text(strip=True)
                if db_access_text:
                    result["database_access"] = db_access_text
                    break
            # Also check parent container
            parent = header.find_parent(['div', 'section'])
            if parent:
                access_text = parent.get_text(strip=True)
                if access_text and len(access_text) < 200:  # Reasonable length
                    result["database_access"] = access_text
                    break
        
        # Extract data access types
        data_access_headers = soup.find_all('h2', string=re.compile('Data access', re.I))
        for header in data_access_headers:
            parent = header.find_parent(['div', 'section', 'dl'])
            if parent:
                access_items = parent.find_all(['li', 'dd', 'dt', 'p', 'a'])
                for item in access_items:
                    access_text = item.get_text(strip=True)
                    if access_text and len(access_text) > 2 and len(access_text) < 100:
                        result["data_access"].append(access_text)
        
        # Extract database licenses
        db_licenses_headers = soup.find_all('h2', string=re.compile('Database license', re.I))
        for header in db_licenses_headers:
            parent = header.find_parent(['div', 'section', 'dl', 'ul'])
            if parent:
                license_links = parent.find_all('a')
                for link in license_links:
                    license_name = link.get_text(strip=True)
                    license_url = link.get('href', '')
                    if license_name and license_name.lower() not in ['other', 'none', 'n/a']:
                        if not license_url.startswith('http'):
                            if license_url.startswith('/'):
                                license_url = f"{RE3DATA_BASE_URL}{license_url}"
                            else:
                                license_url = None
                        result["database_licenses"].append({
                            "name": license_name,
                            "url": license_url
                        })
        
        # Extract data policy and privacy policy
        policy_headers = soup.find_all(string=re.compile('Policy Name', re.I))
        for policy_text in policy_headers:
            if hasattr(policy_text, 'find_parent'):
                policy_parent = policy_text.find_parent()
            else:
                # If it's a NavigableString, find parent differently
                continue
            if policy_parent:
                policy_link = policy_parent.find('a')
                if policy_link:
                    policy_name = policy_link.get_text(strip=True)
                    policy_url = policy_link.get('href', '')
                    # Check if it's a data policy or privacy policy
                    policy_context = policy_parent.get_text(strip=True).lower()
                    if 'data policy' in policy_context or ('data' in policy_name.lower() and 'privacy' not in policy_name.lower()):
                        if not policy_url.startswith('http'):
                            if policy_url.startswith('/'):
                                policy_url = f"{RE3DATA_BASE_URL}{policy_url}"
                            else:
                                policy_url = None
                        if not result.get("data_policy"):
                            result["data_policy"] = {
                                "name": policy_name,
                                "url": policy_url
                            }
                    elif 'privacy' in policy_context or 'privacy' in policy_name.lower():
                        if not policy_url.startswith('http'):
                            if policy_url.startswith('/'):
                                policy_url = f"{RE3DATA_BASE_URL}{policy_url}"
                            else:
                                policy_url = None
                        if not result.get("privacy_policy"):
                            result["privacy_policy"] = {
                                "name": policy_name,
                                "url": policy_url
                            }
        
        # Extract standards - look for standards tab
        standards_section = (soup.find('div', {'id': 'tab_standards'}) or 
                            soup.find('div', {'id': re.compile('standard', re.I)}) or
                            soup.find('section', {'id': re.compile('standard', re.I)}))
        if standards_section:
            standard_items = standards_section.find_all(['li', 'dd', 'dt', 'p', 'tr', 'td'])
            for item in standard_items:
                standard_text = item.get_text(strip=True)
                if standard_text and len(standard_text) > 2 and len(standard_text) < 200:
                    # Try to identify standard type
                    standard_type = None
                    standard_lower = standard_text.lower()
                    if any(x in standard_lower for x in ['dublin core', 'dc']):
                        standard_type = "metadata"
                    elif any(x in standard_lower for x in ['datacite', 'data cite']):
                        standard_type = "metadata"
                    elif any(x in standard_lower for x in ['oai-pmh', 'oai']):
                        standard_type = "protocol"
                    elif any(x in standard_lower for x in ['rest', 'soap']):
                        standard_type = "api"
                    
                    result["standards"].append({
                        "type": standard_type,
                        "name": standard_text
                    })
        
        # Extract certifications - look for certificate icon or certification section
        cert_icon = soup.find('div', class_=re.compile('icon.*certificate', re.I))
        if cert_icon:
            cert_hint = cert_icon.get('data-hint', '')
            if cert_hint and 'certified' in cert_hint.lower():
                # Extract certification names from hint or nearby text
                cert_text = cert_icon.find_next(['div', 'span', 'p'])
                if cert_text:
                    cert_name = cert_text.get_text(strip=True)
                    if cert_name:
                        result["certifications"].append(cert_name)
        
        # Also look for explicit certification mentions in the page
        cert_keywords = ['coretrustseal', 'core trust seal', 'din31644', 'iso16363', 'nestor', 'dini', 'wds', 'world data system']
        for keyword in cert_keywords:
            if keyword.lower() in html_content.lower():
                # Check if not already added
                if keyword.title() not in result["certifications"]:
                    result["certifications"].append(keyword.title())
        
        # Extract repository size
        size_headers = soup.find_all(string=re.compile('Repository size', re.I))
        for size_text in size_headers:
            if hasattr(size_text, 'find_parent'):
                size_parent = size_text.find_parent()
                if size_parent:
                    size_elem = size_parent.find_next(['div', 'dd', 'p', 'span'])
                    if size_elem:
                        size_value = size_elem.get_text(strip=True)
                        if size_value:
                            result["repository_size"] = size_value
                            break
        
        # Extract launch date - look for launch, founded, or established
        launch_keywords = ['launch', 'founded', 'established', 'created']
        for keyword in launch_keywords:
            launch_elems = soup.find_all(string=re.compile(keyword, re.I))
            for launch_elem in launch_elems:
                if hasattr(launch_elem, 'find_parent'):
                    launch_parent = launch_elem.find_parent()
                    if launch_parent:
                        # Try to extract date
                        date_text = launch_parent.get_text(strip=True)
                        date_match = re.search(r'\b(19|20)\d{2}\b', date_text)
                        if date_match:
                            result["launch_date"] = date_match.group(0)
                            break
            if result.get("launch_date"):
                break
        
        # Extract APIs and protocols - look for API mentions
        api_links = soup.find_all('a', href=re.compile('api|oai|rest', re.I))
        for api_link in api_links:
            api_name = api_link.get_text(strip=True)
            api_url = api_link.get('href', '')
            if api_url:
                # Make URL absolute if relative
                if not api_url.startswith('http'):
                    if api_url.startswith('/'):
                        api_url = f"{RE3DATA_BASE_URL}{api_url}"
                    else:
                        api_url = None
                
                # Determine API type
                api_type = None
                api_lower = (api_name + ' ' + api_url).lower()
                if 'rest' in api_lower:
                    api_type = "REST"
                elif 'oai' in api_lower or 'pmh' in api_lower:
                    api_type = "OAI-PMH"
                elif 'soap' in api_lower:
                    api_type = "SOAP"
                elif 'graphql' in api_lower:
                    api_type = "GraphQL"
                elif 'sparql' in api_lower:
                    api_type = "SPARQL"
                
                if api_type:
                    # Check if already added
                    if not any(api.get("url") == api_url for api in result["apis"]):
                        result["apis"].append({
                            "type": api_type,
                            "url": api_url
                        })
                        if api_type not in result["protocols"]:
                            result["protocols"].append(api_type)
        
        # Fallback: try to extract from data attributes or structured data
        if not result["description"]:
            meta_desc = soup.find('meta', {'name': 'description'})
            if meta_desc:
                result["description"] = meta_desc.get('content', '').strip()
        
        # Try to extract from JSON-LD if available
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_ld_scripts:
            try:
                json_data = json.loads(script.string)
                if isinstance(json_data, dict):
                    if 'description' in json_data and not result["description"]:
                        result["description"] = json_data['description']
                    if 'keywords' in json_data and not result["keywords"]:
                        if isinstance(json_data['keywords'], list):
                            result["keywords"].extend(json_data['keywords'])
                        elif isinstance(json_data['keywords'], str):
                            result["keywords"].append(json_data['keywords'])
            except (json.JSONDecodeError, AttributeError):
                pass
    
    else:
        # Basic regex-based parsing as fallback
        # Extract description from meta tag
        desc_match = re.search(r'<meta\s+name=["\']description["\']\s+content=["\']([^"\']+)["\']', html_content, re.I)
        if desc_match:
            result["description"] = desc_match.group(1)
        
        # Extract email
        email_match = re.search(r'mailto:([^\s"\'<>]+)', html_content, re.I)
        if email_match:
            result["contact_email"] = email_match.group(1)
    
    # Clean up empty lists
    for key in ["keywords", "content_type", "persistent_identifiers", "software", "institutions", 
                "subjects", "data_access", "database_licenses", "standards", "certifications", 
                "apis", "protocols"]:
        if not result[key]:
            result[key] = []
    
    # Clean up None values for optional dict fields
    if not result.get("data_policy") or not result["data_policy"].get("name"):
        result["data_policy"] = None
    if not result.get("privacy_policy") or not result["privacy_policy"].get("name"):
        result["privacy_policy"] = None
    
    return result


def fetch_re3data_repository(re3data_id: str, use_cache: bool = True) -> Optional[Dict[str, Any]]:
    """Fetch and parse re3data repository data."""
    # Check cache first
    if use_cache:
        cached_data = load_cached_re3data()
        if re3data_id in cached_data:
            logger.debug(f"Using cached data for {re3data_id}")
            return cached_data[re3data_id]
    
    # Fetch from web
    html_content = fetch_re3data_page(re3data_id)
    if not html_content:
        return None
    
    # Parse HTML
    parsed_data = parse_re3data_html(html_content, re3data_id)
    
    # Cache the result
    if use_cache:
        cache_re3data_data(re3data_id, parsed_data)
    
    return parsed_data


def load_cached_re3data() -> Dict[str, Dict[str, Any]]:
    """Load cached re3data data."""
    if os.path.exists(RE3DATA_CACHE_FILE):
        try:
            with open(RE3DATA_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load cached re3data data: {e}")
    return {}


def cache_re3data_data(re3data_id: str, data: Dict[str, Any]) -> None:
    """Cache re3data repository data."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    
    cached_data = load_cached_re3data()
    cached_data[re3data_id] = data
    
    try:
        with open(RE3DATA_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cached_data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"Could not cache re3data data: {e}")


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
                re3data_ids[re3data_id] = filepath
        
        except Exception as e:
            logger.debug(f"Error reading {filepath}: {e}")
    
    logger.info(f"Found {len(re3data_ids)} catalogs with re3data identifiers")
    return re3data_ids


def enrich_catalog_with_re3data(catalog: Dict[str, Any], re3data_data: Dict[str, Any]) -> Dict[str, Any]:
    """Merge re3data data into catalog under _re3data key."""
    enriched_catalog = catalog.copy()
    enriched_catalog["_re3data"] = re3data_data
    return enriched_catalog


def enrich_all_catalogs(dry_run: bool = False, delay: float = 1.0, limit: Optional[int] = None, force: bool = False) -> Dict[str, Any]:
    """Process all catalogs with re3data identifiers and enrich them."""
    re3data_ids = collect_re3data_identifiers()
    
    if not re3data_ids:
        logger.warning("No re3data identifiers found")
        return {"enriched": 0, "failed": 0, "skipped": 0, "updated": 0}
    
    stats = {"enriched": 0, "failed": 0, "skipped": 0, "updated": 0}
    processed = 0
    
    ids_to_process = list(re3data_ids.items())
    if limit:
        ids_to_process = ids_to_process[:limit]
    
    logger.info(f"Processing {len(ids_to_process)} catalogs...")
    if force:
        logger.info("Force mode enabled: will re-enrich already enriched catalogs")
    
    for re3data_id, filepath in ids_to_process:
        try:
            # Load catalog
            with open(filepath, "r", encoding="utf-8") as f:
                catalog = yaml.load(f, Loader=Loader)
            
            if not catalog:
                stats["skipped"] += 1
                continue
            
            # Check if already enriched (skip if not forcing)
            is_already_enriched = "_re3data" in catalog and catalog["_re3data"].get("re3data_id") == re3data_id
            if is_already_enriched and not force:
                logger.debug(f"Catalog {catalog.get('id')} already enriched with {re3data_id} (use --force to update)")
                stats["skipped"] += 1
                continue
            
            if is_already_enriched and force:
                logger.info(f"Force updating catalog {catalog.get('id')} with {re3data_id}...")
            
            # Fetch re3data data
            logger.info(f"Fetching re3data data for {re3data_id} (catalog: {catalog.get('id')})...")
            re3data_data = fetch_re3data_repository(re3data_id, use_cache=True)
            
            if not re3data_data:
                logger.warning(f"Could not fetch re3data data for {re3data_id}")
                stats["failed"] += 1
                continue
            
            # Enrich catalog
            enriched_catalog = enrich_catalog_with_re3data(catalog, re3data_data)
            
            if not dry_run:
                # Save enriched catalog
                with open(filepath, "w", encoding="utf-8") as f:
                    yaml.dump(enriched_catalog, f, Dumper=Dumper, allow_unicode=True, sort_keys=False)
                if is_already_enriched:
                    logger.info(f"Updated {filepath}")
                    stats["updated"] += 1
                else:
                    logger.info(f"Enriched {filepath}")
                    stats["enriched"] += 1
            else:
                if is_already_enriched:
                    logger.info(f"[DRY RUN] Would update {filepath}")
                    stats["updated"] += 1
                else:
                    logger.info(f"[DRY RUN] Would enrich {filepath}")
                    stats["enriched"] += 1
            
            processed += 1
            
            # Rate limiting
            if delay > 0 and processed < len(ids_to_process):
                time.sleep(delay)
        
        except Exception as e:
            logger.error(f"Error processing {filepath}: {e}")
            stats["failed"] += 1
    
    return stats


@app.command()
def fetch(
    re3data_id: Optional[str] = typer.Option(None, "--id", help="Specific re3data ID to fetch"),
    all: bool = typer.Option(False, "--all", help="Fetch all re3data repositories"),
    delay: float = typer.Option(1.0, "--delay", help="Delay between requests (seconds)"),
    limit: Optional[int] = typer.Option(None, "--limit", help="Limit number of repositories to fetch"),
):
    """Fetch re3data repository data."""
    
    if re3data_id:
        logger.info(f"Fetching re3data data for {re3data_id}...")
        data = fetch_re3data_repository(re3data_id, use_cache=True)
        if data:
            logger.info(f"Successfully fetched data for {re3data_id}")
            logger.info(json.dumps(data, indent=2, ensure_ascii=False))
        else:
            logger.error(f"Failed to fetch data for {re3data_id}")
    
    elif all:
        re3data_ids = collect_re3data_identifiers()
        ids_to_fetch = list(re3data_ids.keys())
        if limit:
            ids_to_fetch = ids_to_fetch[:limit]
        
        logger.info(f"Fetching data for {len(ids_to_fetch)} repositories...")
        
        fetched = 0
        failed = 0
        
        for re3data_id in ids_to_fetch:
            logger.info(f"Fetching {re3data_id}...")
            data = fetch_re3data_repository(re3data_id, use_cache=True)
            if data:
                fetched += 1
            else:
                failed += 1
            
            # Rate limiting
            if delay > 0:
                time.sleep(delay)
        
        logger.info(f"\n=== Results ===")
        logger.info(f"Fetched: {fetched}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Total: {len(ids_to_fetch)}")
    
    else:
        logger.error("Please specify --id or --all")


@app.command()
def enrich(
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview enrichment without making changes"),
    delay: float = typer.Option(1.0, "--delay", help="Delay between requests (seconds)"),
    limit: Optional[int] = typer.Option(None, "--limit", help="Limit number of catalogs to enrich"),
    force: bool = typer.Option(False, "--force", help="Force re-enrichment of already enriched catalogs"),
):
    """Enrich catalog files with re3data metadata."""
    
    if dry_run:
        logger.info("DRY RUN MODE: No files will be modified")
    
    if force:
        logger.info("FORCE MODE: Will update already enriched catalogs")
    
    stats = enrich_all_catalogs(dry_run=dry_run, delay=delay, limit=limit, force=force)
    
    logger.info(f"\n=== Enrichment Results ===")
    logger.info(f"Enriched: {stats['enriched']}")
    if stats.get('updated', 0) > 0:
        logger.info(f"Updated: {stats['updated']}")
    logger.info(f"Failed: {stats['failed']}")
    logger.info(f"Skipped: {stats['skipped']}")
    logger.info(f"Total: {sum(stats.values())}")


if __name__ == "__main__":
    app()

