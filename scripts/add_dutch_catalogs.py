#!/usr/bin/env python3
"""
Script to add Dutch open data catalogs from data.overheid.nl/community/catalogi to the registry.
Fetches catalogs from the website and creates YAML entries for each.
"""

import json
import requests
import yaml
import re
from pathlib import Path
from urllib.parse import urlparse, urljoin
import html

# Dutch province mapping (ISO 3166-2 codes)
# Common city/province mappings
PROVINCE_MAP = {
    # Cities and their provinces
    'amsterdam': 'NL-NH',  # North Holland
    'rotterdam': 'NL-ZH',  # South Holland
    'den haag': 'NL-ZH',   # The Hague, South Holland
    'utrecht': 'NL-UT',    # Utrecht
    'eindhoven': 'NL-NB',  # North Brabant
    'groningen': 'NL-GR',  # Groningen
    'tilburg': 'NL-NB',    # North Brabant
    'almere': 'NL-FL',     # Flevoland
    'breda': 'NL-NB',      # North Brabant
    'nijmegen': 'NL-GE',   # Gelderland
    'enschede': 'NL-OV',   # Overijssel
    'haarlem': 'NL-NH',    # North Holland
    'arnhem': 'NL-GE',     # Gelderland
    'zaanstad': 'NL-NH',   # North Holland
    'amersfoort': 'NL-UT', # Utrecht
    'apeldoorn': 'NL-GE',  # Gelderland
    'hoofddorp': 'NL-NH',  # North Holland
    'maastricht': 'NL-LI', # Limburg
    'leiden': 'NL-ZH',     # South Holland
    'dordrecht': 'NL-ZH',  # South Holland
    'zoetermeer': 'NL-ZH', # South Holland
    'zwolle': 'NL-OV',     # Overijssel
    'deventer': 'NL-OV',   # Overijssel
    'delft': 'NL-ZH',      # South Holland
    'heerlen': 'NL-LI',    # Limburg
    'alkmaar': 'NL-NH',    # North Holland
    'venlo': 'NL-LI',      # Limburg
    'leeuwarden': 'NL-FR', # Friesland
    'sittard': 'NL-LI',    # Limburg
    'geleen': 'NL-LI',     # Limburg
    'emmen': 'NL-DR',      # Drenthe
    'westland': 'NL-ZH',   # South Holland
    'haarlemmermeer': 'NL-NH', # North Holland
    'assen': 'NL-DR',      # Drenthe
    'roosendaal': 'NL-NB', # North Brabant
    'hilversum': 'NL-NH',  # North Holland
    
    # Provinces
    'noord-holland': 'NL-NH',
    'zuid-holland': 'NL-ZH',
    'utrecht': 'NL-UT',
    'noord-brabant': 'NL-NB',
    'gelderland': 'NL-GE',
    'overijssel': 'NL-OV',
    'limburg': 'NL-LI',
    'friesland': 'NL-FR',
    'groningen': 'NL-GR',
    'drenthe': 'NL-DR',
    'flevoland': 'NL-FL',
    'zeeland': 'NL-ZE',
}

PROVINCE_NAMES = {
    'NL-NH': 'Noord-Holland',
    'NL-ZH': 'Zuid-Holland',
    'NL-UT': 'Utrecht',
    'NL-NB': 'Noord-Brabant',
    'NL-GE': 'Gelderland',
    'NL-OV': 'Overijssel',
    'NL-LI': 'Limburg',
    'NL-FR': 'Friesland',
    'NL-GR': 'Groningen',
    'NL-DR': 'Drenthe',
    'NL-FL': 'Flevoland',
    'NL-ZE': 'Zeeland',
}

def normalize_name(name):
    """Normalize name for lookup."""
    return name.lower().strip().replace('_', ' ').replace('-', ' ')

def get_province_code(name, url=None):
    """Get ISO 3166-2 code for a name or URL."""
    normalized = normalize_name(name)
    
    # Check direct mapping
    for key, code in PROVINCE_MAP.items():
        if key in normalized:
            return code
    
    # Check URL for city/province names
    if url:
        url_lower = url.lower()
        for key, code in PROVINCE_MAP.items():
            if key in url_lower:
                return code
    
    return None

def generate_id_from_url(url):
    """Generate a unique ID from the portal URL."""
    # Remove protocol and www
    url = url.replace('https://', '').replace('http://', '').replace('www.', '')
    # Remove trailing slash
    url = url.rstrip('/')
    # Replace dots and special chars with nothing, keep only alphanumeric
    id_str = re.sub(r'[^a-zA-Z0-9]', '', url)
    return id_str.lower()

def parse_catalog_page(html_content):
    """Parse the catalog listing page to extract catalog information."""
    catalogs = []
    
    # Try to find catalog links and information
    # This is a simplified parser - may need adjustment based on actual HTML structure
    import re
    
    # Look for links that might be catalogs
    # Pattern: links to catalog pages or external catalog URLs
    link_pattern = r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>([^<]+)</a>'
    links = re.findall(link_pattern, html_content, re.IGNORECASE)
    
    # Also look for structured data or JSON-LD
    json_ld_pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
    json_lds = re.findall(json_ld_pattern, html_content, re.DOTALL | re.IGNORECASE)
    
    # For now, we'll need to manually extract or use a more sophisticated approach
    # Let's try to find catalog items in the page
    catalog_items = re.findall(r'<div[^>]*class=["\'][^"]*catalog[^"]*["\'][^>]*>(.*?)</div>', html_content, re.DOTALL | re.IGNORECASE)
    
    return catalogs

def fetch_catalogs_from_api():
    """Try to fetch catalogs from an API endpoint."""
    # Try different possible API endpoints
    api_urls = [
        "https://data.overheid.nl/data/api/3/action/package_search?q=type:catalog&rows=100",
        "https://data.overheid.nl/search/catalogi?format=json",
    ]
    
    for url in api_urls:
        try:
            response = requests.get(url, timeout=30, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            })
            if response.status_code == 200:
                try:
                    data = response.json()
                    return data
                except:
                    pass
        except:
            continue
    
    return None

def fetch_catalogs_from_html():
    """Fetch catalogs by parsing the HTML page."""
    url = "https://data.overheid.nl/community/catalogi"
    try:
        response = requests.get(url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        if response.status_code == 200:
            return response.text
    except Exception as e:
        print(f"Error fetching HTML: {e}")
    return None

def extract_catalogs_from_html(html_content):
    """Extract catalog information from HTML content."""
    catalogs = []
    
    # Look for catalog links - this is a simplified approach
    # The actual structure may vary
    import re
    
    # Try to find links that look like catalog URLs
    # Pattern for links that might be catalogs
    # This is a placeholder - actual implementation would need to parse the specific HTML structure
    
    # For now, return empty list - we'll need to manually create entries or use a better parser
    return catalogs

def create_catalog_yaml(catalog_data, province_code=None):
    """Create a YAML entry for a catalog."""
    name = catalog_data.get('name', '')
    url = catalog_data.get('url', '')
    description = catalog_data.get('description', '')
    
    if not url:
        return None
    
    # Generate ID from URL
    catalog_id = generate_id_from_url(url)
    
    # Determine if it's local/regional (level 30) or national (level 20)
    level = 30 if province_code else 20
    
    # Create YAML structure
    yaml_data = {
        'access_mode': ['open'],
        'api': catalog_data.get('api', False),
        'api_status': catalog_data.get('api_status', 'uncertain'),
        'catalog_type': catalog_data.get('catalog_type', 'Open data portal'),
        'content_types': catalog_data.get('content_types', ['dataset']),
        'coverage': [{
            'location': {
                'country': {
                    'id': 'NL',
                    'name': 'Netherlands'
                },
                'level': level,
                'macroregion': {
                    'id': '155',
                    'name': 'Western Europe'
                }
            }
        }],
        'description': description or f'Open data catalog from the Netherlands',
        'id': catalog_id,
        'langs': [{
            'id': 'NL',
            'name': 'Dutch'
        }],
        'link': url,
        'name': name or catalog_id,
        'owner': {
            'location': {
                'country': {
                    'id': 'NL',
                    'name': 'Netherlands'
                },
                'level': level
            },
            'name': catalog_data.get('owner_name', 'Unknown'),
            'type': catalog_data.get('owner_type', 'Government')
        },
        'rights': {
            'license_id': None,
            'license_name': None,
            'license_url': None,
            'privacy_policy_url': None,
            'rights_type': 'granular',
            'tos_url': None
        },
        'status': 'active',
        'tags': catalog_data.get('tags', ['government', 'open data', 'Netherlands']),
        'topics': catalog_data.get('topics', [])
    }
    
    # Add subregion if province code is provided
    if province_code:
        province_name = PROVINCE_NAMES.get(province_code, province_code)
        yaml_data['coverage'][0]['location']['subregion'] = {
            'id': province_code,
            'name': province_name
        }
        yaml_data['owner']['location']['subregion'] = {
            'id': province_code,
            'name': province_name
        }
        yaml_data['owner']['location']['level'] = 30
    
    # Add endpoints if available
    if catalog_data.get('endpoints'):
        yaml_data['endpoints'] = catalog_data['endpoints']
    
    # Add software if available
    if catalog_data.get('software'):
        yaml_data['software'] = catalog_data['software']
    
    # Add identifiers if available
    if catalog_data.get('identifiers'):
        yaml_data['identifiers'] = catalog_data['identifiers']
    
    return yaml_data, catalog_id, province_code

def check_existing_catalog(base_dir, catalog_url):
    """Check if a catalog URL already exists in the registry."""
    base_dir = Path(base_dir)
    if not base_dir.exists():
        return False
    
    normalized_url = catalog_url.lower().rstrip('/')
    
    for yaml_file in base_dir.rglob('*.yaml'):
        try:
            with open(yaml_file, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                if data and data.get('link'):
                    existing_url = data['link'].lower().rstrip('/')
                    if existing_url == normalized_url:
                        return True
        except:
            continue
    return False

def main():
    """Main function to process and create catalog entries."""
    print("This script needs manual catalog data.")
    print("Please provide a list of catalogs from https://data.overheid.nl/community/catalogi")
    print("\nFor now, this is a template script that can be extended.")
    
    # Base directory for entities
    base_dir = Path(__file__).parent.parent / 'data' / 'entities' / 'NL'
    
    # Example: Manual catalog list (to be filled with actual data)
    # This should be populated from the website
    manual_catalogs = [
        # Example entry - replace with actual data
        # {
        #     'name': 'Example Catalog',
        #     'url': 'https://example.nl',
        #     'description': 'Example description',
        #     'province': 'NL-NH'
        # }
    ]
    
    if not manual_catalogs:
        print("\nNo catalogs to process. Please add catalog data to the script.")
        return
    
    created = []
    skipped = []
    errors = []
    
    for catalog_data in manual_catalogs:
        url = catalog_data.get('url', '')
        if not url:
            skipped.append("Missing URL")
            continue
        
        # Check if catalog already exists
        if check_existing_catalog(base_dir, url):
            skipped.append(f"Catalog already exists: {url}")
            continue
        
        # Get province code
        province_code = catalog_data.get('province') or get_province_code(
            catalog_data.get('name', ''),
            url
        )
        
        # Create YAML entry
        result = create_catalog_yaml(catalog_data, province_code)
        if not result:
            skipped.append(f"Could not create entry for {url}")
            continue
        
        yaml_data, catalog_id, prov_code = result
        
        # Determine directory structure
        if prov_code:
            catalog_dir = base_dir / prov_code / 'opendata'
        else:
            catalog_dir = base_dir / 'Federal' / 'opendata'
        
        catalog_dir.mkdir(parents=True, exist_ok=True)
        
        # Check if file already exists
        yaml_file = catalog_dir / f"{catalog_id}.yaml"
        if yaml_file.exists():
            skipped.append(f"File already exists: {yaml_file}")
            continue
        
        # Write YAML file
        try:
            with open(yaml_file, 'w', encoding='utf-8') as f:
                yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
            created.append(f"{catalog_data.get('name', catalog_id)}: {yaml_file}")
            print(f"Created: {yaml_file}")
        except Exception as e:
            errors.append(f"Error writing {yaml_file}: {e}")
    
    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Created: {len(created)}")
    print(f"Skipped: {len(skipped)}")
    print(f"Errors: {len(errors)}")
    
    if created:
        print("\nCreated files:")
        for item in created:
            print(f"  - {item}")

if __name__ == '__main__':
    main()

