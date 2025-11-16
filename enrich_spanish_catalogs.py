#!/usr/bin/env python3
"""
Script to enrich newly added Spanish data catalogs with metadata:
- Software detection
- API endpoints
- Description
- Languages
- Topics
- Tags
"""

import requests
import yaml
from pathlib import Path
from urllib.parse import urlparse, urljoin
import re
import time
import logging
from typing import Dict, List, Optional
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Add scripts directory to path to import apidetect
sys.path.insert(0, str(Path(__file__).parent))

try:
    from apidetect import detect_software, DEFAULT_TIMEOUT, USER_AGENT
except ImportError:
    logger.warning("apidetect not available, software detection will be limited")
    detect_software = None
    DEFAULT_TIMEOUT = 15  # Increased timeout
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"

# Timeout settings
FETCH_TIMEOUT = 15  # Timeout for fetching HTML content
API_TIMEOUT = 8     # Timeout for API endpoint detection

# Spanish region codes for topic mapping
REGION_TOPICS = {
    'ES-AN': ['REGI', 'GOVE'],
    'ES-AR': ['REGI', 'GOVE'],
    'ES-AS': ['REGI', 'GOVE'],
    'ES-CB': ['REGI', 'GOVE'],
    'ES-CL': ['REGI', 'GOVE'],
    'ES-CM': ['REGI', 'GOVE'],
    'ES-CN': ['REGI', 'GOVE'],
    'ES-CT': ['REGI', 'GOVE'],
    'ES-EX': ['REGI', 'GOVE'],
    'ES-GA': ['REGI', 'GOVE'],
    'ES-MD': ['REGI', 'GOVE'],
    'ES-MC': ['REGI', 'GOVE'],
    'ES-NC': ['REGI', 'GOVE'],
    'ES-PV': ['REGI', 'GOVE'],
    'ES-RI': ['REGI', 'GOVE'],
    'ES-VC': ['REGI', 'GOVE'],
    'ES-IB': ['REGI', 'GOVE'],
}

# Common software patterns
SOFTWARE_PATTERNS = {
    'ckan': {
        'id': 'ckan',
        'name': 'CKAN',
        'indicators': ['/api/3/action', '/api/action/package_search', 'ckan', 'Powered by CKAN']
    },
    'socrata': {
        'id': 'socrata',
        'name': 'Socrata',
        'indicators': ['socrata.com', '/api/views/', 'socrata']
    },
    'arcgis': {
        'id': 'arcgis',
        'name': 'ArcGIS Hub',
        'indicators': ['arcgis.com', 'hub.arcgis.com', 'arcgis hub']
    },
    'opendatasoft': {
        'id': 'opendatasoft',
        'name': 'OpenDataSoft',
        'indicators': ['opendatasoft.com', '/api/v2/catalog', 'opendatasoft']
    },
    'dkan': {
        'id': 'dkan',
        'name': 'DKAN',
        'indicators': ['dkan', '/api/1/action']
    },
    'geonetwork': {
        'id': 'geonetwork',
        'name': 'GeoNetwork',
        'indicators': ['geonetwork', '/srv/api', '/geonetwork']
    },
    'dataverse': {
        'id': 'dataverse',
        'name': 'Dataverse',
        'indicators': ['dataverse', '/api/datasets']
    },
}

def normalize_url(url: str) -> str:
    """Normalize URL."""
    if not url:
        return ""
    url = url.strip()
    if not url.startswith('http'):
        url = f"https://{url}"
    return url.rstrip('/')

def detect_software_simple(url: str, html_content: str = None) -> Optional[Dict]:
    """Simple software detection based on URL and content patterns."""
    url_lower = url.lower()
    content_lower = (html_content or "").lower()
    
    for software_id, pattern in SOFTWARE_PATTERNS.items():
        for indicator in pattern['indicators']:
            if indicator in url_lower or indicator in content_lower:
                return {
                    'id': pattern['id'],
                    'name': pattern['name']
                }
    
    # Check for CKAN API
    try:
        api_url = urljoin(url, '/api/3/action/package_list')
        response = requests.get(api_url, timeout=DEFAULT_TIMEOUT, headers={'User-Agent': USER_AGENT}, allow_redirects=True)
        if response.status_code == 200:
            try:
                data = response.json()
                if data.get('success') is True or 'result' in data:
                    return {'id': 'ckan', 'name': 'CKAN'}
            except:
                pass
    except:
        pass
    
    return None

def extract_description(html_content: str, url: str) -> Optional[str]:
    """Extract description from HTML meta tags."""
    if not html_content:
        return None
    
    # Try meta description
    meta_desc = re.search(r'<meta\s+name=["\']description["\']\s+content=["\']([^"\']+)["\']', html_content, re.I)
    if meta_desc:
        desc = meta_desc.group(1).strip()
        if desc and len(desc) > 20:
            return desc
    
    # Try og:description
    og_desc = re.search(r'<meta\s+property=["\']og:description["\']\s+content=["\']([^"\']+)["\']', html_content, re.I)
    if og_desc:
        desc = og_desc.group(1).strip()
        if desc and len(desc) > 20:
            return desc
    
    # Try to find first paragraph
    para = re.search(r'<p[^>]*>([^<]{50,300})</p>', html_content, re.I | re.DOTALL)
    if para:
        desc = re.sub(r'\s+', ' ', para.group(1)).strip()
        if len(desc) > 30:
            return desc[:300]
    
    return None

def detect_languages(html_content: str, url: str) -> List[Dict]:
    """Detect languages from HTML."""
    langs = []
    
    if not html_content:
        # Default to Spanish for Spanish portals
        return [{'id': 'ES', 'name': 'Spanish'}]
    
    # Check html lang attribute
    html_lang = re.search(r'<html[^>]*\s+lang=["\']([^"\']+)["\']', html_content, re.I)
    if html_lang:
        lang_code = html_lang.group(1).upper()[:2]
        if lang_code == 'ES':
            langs.append({'id': 'ES', 'name': 'Spanish'})
        elif lang_code == 'CA':
            langs.append({'id': 'CA', 'name': 'Catalan'})
        elif lang_code == 'EU':
            langs.append({'id': 'EU', 'name': 'Basque'})
        elif lang_code == 'GL':
            langs.append({'id': 'GL', 'name': 'Galician'})
        elif lang_code == 'EN':
            langs.append({'id': 'EN', 'name': 'English'})
    
    # Check for multilingual indicators
    if 'català' in html_content.lower() or 'catala' in html_content.lower():
        if not any(l.get('id') == 'CA' for l in langs):
            langs.append({'id': 'CA', 'name': 'Catalan'})
    
    if not langs:
        langs.append({'id': 'ES', 'name': 'Spanish'})
    
    return langs

def detect_api_endpoints(url: str, html_content: str = None) -> List[Dict]:
    """Detect API endpoints."""
    endpoints = []
    url_lower = url.lower()
    
    # Common API patterns
    api_patterns = [
        ('ckan', '/api/3', 'ckan'),
        ('ckan', '/api/3/action/package_list', 'ckan'),
        ('opendatasoft', '/api/v2/catalog', 'opendatasoft'),
        ('socrata', '/api/views', 'socrata'),
        ('arcgis', '/api/feed/dcat', 'dcatap201'),
        ('dcat', '/catalog.xml', 'dcat'),
        ('dcat', '/catalog.rdf', 'dcatap'),
        ('dcat', '/catalog.jsonld', 'dcat:jsonld'),
        ('sitemap', '/sitemap.xml', 'sitemap'),
    ]
    
    base_url = url.rstrip('/')
    for pattern_name, pattern_path, endpoint_type in api_patterns:
        test_url = urljoin(base_url, pattern_path)
        try:
            response = requests.head(test_url, timeout=API_TIMEOUT, headers={'User-Agent': USER_AGENT}, allow_redirects=True)
            if response.status_code == 200:
                endpoints.append({
                    'type': endpoint_type,
                    'url': test_url
                })
                logger.debug(f"    Found endpoint: {endpoint_type} at {test_url}")
        except requests.exceptions.Timeout:
            pass
        except Exception:
            pass
    
    # Check for CKAN API
    ckan_api = urljoin(base_url, '/api/3/action/package_list')
    try:
        response = requests.get(ckan_api, timeout=API_TIMEOUT, headers={'User-Agent': USER_AGENT}, allow_redirects=True)
        if response.status_code == 200:
            try:
                data = response.json()
                if data.get('success') is True:
                    endpoints.append({
                        'type': 'ckan',
                        'url': urljoin(base_url, '/api/3'),
                        'version': '3'
                    })
                    logger.debug(f"    Found CKAN API at {urljoin(base_url, '/api/3')}")
            except:
                pass
    except:
        pass
    
    return endpoints

def infer_topics(name: str, owner: str, region_code: str = None) -> List[Dict]:
    """Infer topics from name, owner, and region."""
    topics = []
    text = f"{name} {owner}".lower()
    
    # Always add GOVE for government portals
    topics.append({'id': 'GOVE', 'name': 'Government and public sector', 'type': 'eudatatheme'})
    
    # Add REGI for regional/local portals
    if region_code:
        topics.append({'id': 'REGI', 'name': 'Regions and cities', 'type': 'eudatatheme'})
    
    # Topic keywords
    topic_keywords = {
        'TRAN': ['transporte', 'transport', 'movilidad', 'mobility', 'tráfico', 'traffic'],
        'ENVI': ['medio ambiente', 'environment', 'ambiental', 'clima', 'climate'],
        'HEAL': ['salud', 'health', 'sanidad', 'sanitario'],
        'EDUC': ['educación', 'education', 'educativo', 'universidad', 'university'],
        'ECON': ['economía', 'economy', 'económico', 'finanzas', 'finance'],
        'SOCI': ['sociedad', 'society', 'población', 'population', 'demografía', 'demography'],
        'AGRI': ['agricultura', 'agriculture', 'rural', 'ganadería'],
        'TECH': ['tecnología', 'technology', 'ciencia', 'science', 'investigación', 'research'],
        'CULT': ['cultura', 'culture', 'turismo', 'tourism', 'ocio', 'leisure'],
    }
    
    for topic_id, keywords in topic_keywords.items():
        if any(kw in text for kw in keywords):
            topic_name_map = {
                'TRAN': 'Transport',
                'ENVI': 'Environment',
                'HEAL': 'Health',
                'EDUC': 'Education, culture and sport',
                'ECON': 'Economy and finance',
                'SOCI': 'Population and society',
                'AGRI': 'Agriculture, fisheries, forestry and food',
                'TECH': 'Science and technology',
            }
            if topic_id in topic_name_map:
                topics.append({
                    'id': topic_id,
                    'name': topic_name_map[topic_id],
                    'type': 'eudatatheme'
                })
    
    return topics

def enrich_catalog(filepath: Path, dryrun: bool = False) -> bool:
    """Enrich a single catalog YAML file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        if not data or not data.get('link'):
            return False
        
        url = normalize_url(data['link'])
        logger.info(f"Enriching: {data.get('name', 'Unknown')} - {url}")
        
        changed = False
        
        # Fetch HTML content
        html_content = None
        try:
            response = requests.get(url, timeout=FETCH_TIMEOUT, headers={'User-Agent': USER_AGENT}, allow_redirects=True)
            if response.status_code == 200:
                html_content = response.text
                logger.debug(f"  Fetched HTML content ({len(html_content)} bytes)")
        except requests.exceptions.Timeout:
            logger.warning(f"  Timeout fetching {url} (>{FETCH_TIMEOUT}s)")
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"  Connection error fetching {url}: {e}")
        except Exception as e:
            logger.warning(f"  Could not fetch {url}: {type(e).__name__}: {e}")
        
        # Detect software
        if not data.get('software'):
            software = None
            if detect_software:
                try:
                    software = detect_software(url)
                except:
                    pass
            
            if not software:
                software = detect_software_simple(url, html_content)
            
            if software:
                data['software'] = software
                changed = True
                logger.info(f"  Detected software: {software['name']}")
            else:
                data['software'] = {'id': 'custom', 'name': 'Custom software'}
                changed = True
                logger.debug("  Using default software: Custom software")
        
        # Detect API endpoints
        if not data.get('endpoints'):
            endpoints = detect_api_endpoints(url, html_content)
            if endpoints:
                data['endpoints'] = endpoints
                data['api'] = True
                data['api_status'] = 'active'
                changed = True
                logger.info(f"  Found {len(endpoints)} API endpoints")
        
        # Extract description
        if not data.get('description'):
            description = extract_description(html_content, url)
            if description:
                data['description'] = description
                changed = True
                logger.info(f"  Added description ({len(description)} chars)")
        
        # Detect languages
        if not data.get('langs'):
            langs = detect_languages(html_content, url)
            if langs:
                data['langs'] = langs
                changed = True
                logger.info(f"  Detected languages: {[l['id'] for l in langs]}")
        
        # Infer topics
        if not data.get('topics'):
            region_code = None
            if 'coverage' in data and data['coverage']:
                location = data['coverage'][0].get('location', {})
                subregion = location.get('subregion', {})
                if subregion:
                    region_code = subregion.get('id')
            
            topics = infer_topics(
                data.get('name', ''),
                data.get('owner', {}).get('name', ''),
                region_code
            )
            if topics:
                data['topics'] = topics
                changed = True
                logger.info(f"  Added {len(topics)} topics: {[t['id'] for t in topics]}")
        
        # Enhance tags
        tags = set(data.get('tags', []))
        if 'government' not in tags and 'open data' not in tags:
            tags.add('government')
            tags.add('open data')
        
        # Add software tag
        if data.get('software', {}).get('id') != 'custom':
            tags.add(data['software']['id'])
        
        # Add has_api tag if API detected
        if data.get('api'):
            tags.add('has_api')
        
        data['tags'] = sorted(list(tags))
        changed = True
        
        # Save if changed
        if changed and not dryrun:
            with open(filepath, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
            logger.info(f"  ✓ Updated {filepath.name}")
            return True
        elif changed:
            logger.info(f"  [DRYRUN] Would update {filepath.name}")
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"  Error processing {filepath}: {e}", exc_info=True)
        return False

def main(dryrun: bool = False):
    """Main function to enrich all newly added Spanish catalogs."""
    es_dir = Path('data/entities/ES')
    
    if not es_dir.exists():
        logger.error("ES directory not found")
        return
    
    # Find all YAML files in ES directory
    yaml_files = list(es_dir.rglob('*.yaml'))
    
    logger.info(f"Found {len(yaml_files)} Spanish catalog files")
    logger.info("Starting enrichment process...")
    start_time = datetime.now()
    
    enriched = 0
    errors = 0
    skipped = 0
    
    for i, yaml_file in enumerate(yaml_files, 1):
        try:
            result = enrich_catalog(yaml_file, dryrun=dryrun)
            if result:
                enriched += 1
            else:
                skipped += 1
        except Exception as e:
            errors += 1
            logger.error(f"Failed to process {yaml_file}: {e}")
        
        # Progress logging every 50 files
        if i % 50 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = i / elapsed if elapsed > 0 else 0
            logger.info(f"Progress: {i}/{len(yaml_files)} files processed ({rate:.1f} files/sec)")
        
        time.sleep(0.3)  # Be respectful with rate limiting
    
    elapsed_time = (datetime.now() - start_time).total_seconds()
    
    logger.info(f"\n=== Summary ===")
    logger.info(f"Total files: {len(yaml_files)}")
    logger.info(f"Enriched: {enriched}")
    logger.info(f"Skipped: {skipped}")
    logger.info(f"Errors: {errors}")
    logger.info(f"Time elapsed: {elapsed_time:.1f} seconds")
    logger.info(f"Average rate: {len(yaml_files)/elapsed_time:.2f} files/sec")

if __name__ == '__main__':
    import sys
    dryrun = '--dryrun' in sys.argv or '-d' in sys.argv
    main(dryrun=dryrun)

