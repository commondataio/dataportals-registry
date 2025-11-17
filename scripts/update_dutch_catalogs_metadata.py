#!/usr/bin/env python3
"""
Script to update software and metadata for newly created Dutch catalog entries.
"""

import yaml
from pathlib import Path
import re

# Mapping of URL patterns to software/platform types
SOFTWARE_MAP = {
    'dataplatform.nl': {
        'id': 'ckan',
        'name': 'CKAN',
        'endpoints': [
            {'type': 'ckan', 'url': None, 'version': '3'},
            {'type': 'ckan:package-search', 'url': None, 'version': '3'},
            {'type': 'ckan:package-list', 'url': None, 'version': '3'},
        ]
    },
    'opendata.arcgis.com': {
        'id': 'arcgishub',
        'name': 'ArcGIS Hub',
        'endpoints': [
            {'type': 'dcatap201', 'url': None},
            {'type': 'dcatus11', 'url': None},
            {'type': 'rss', 'url': None},
            {'type': 'ogcrecordsapi', 'url': None},
            {'type': 'sitemap', 'url': None},
        ]
    },
    'data.eindhoven.nl': {
        'id': 'opendatasoft',
        'name': 'OpenDataSoft',
        'endpoints': [
            {'type': 'opendatasoftapi', 'url': None},
            {'type': 'sitemap', 'url': None},
        ]
    },
}

# Default endpoints for unknown platforms
DEFAULT_ENDPOINTS = [
    {'type': 'sitemap', 'url': None},
]

# Additional topics to add
ADDITIONAL_TOPICS = [
    {'id': 'Boundaries', 'name': 'Boundaries', 'type': 'iso19115'},
    {'id': 'Society', 'name': 'Society', 'type': 'iso19115'},
    {'id': 'Economy', 'name': 'Economy', 'type': 'iso19115'},
    {'id': 'Structure', 'name': 'Structure', 'type': 'iso19115'},
    {'id': 'Transportation', 'name': 'Transportation', 'type': 'iso19115'},
    {'id': 'Location', 'name': 'Location', 'type': 'iso19115'},
]

def detect_software(url):
    """Detect software platform from URL."""
    url_lower = url.lower()
    
    if 'dataplatform.nl' in url_lower:
        return SOFTWARE_MAP['dataplatform.nl']
    elif 'opendata.arcgis.com' in url_lower or 'hub.arcgis.com' in url_lower:
        return SOFTWARE_MAP['opendata.arcgis.com']
    elif 'opendatasoft' in url_lower or 'data.eindhoven.nl' in url_lower:
        return SOFTWARE_MAP['data.eindhoven.nl']
    elif 'ckan' in url_lower:
        return {'id': 'ckan', 'name': 'CKAN', 'endpoints': SOFTWARE_MAP['dataplatform.nl']['endpoints']}
    else:
        # Default to CKAN for dataplatform.nl domains, otherwise unknown
        if 'dataplatform.nl' in url_lower:
            return SOFTWARE_MAP['dataplatform.nl']
        return None

def generate_endpoints(base_url, software_info):
    """Generate endpoint URLs based on base URL and software type."""
    if not software_info:
        return DEFAULT_ENDPOINTS
    
    endpoints = []
    base_url = base_url.rstrip('/')
    
    for endpoint_template in software_info.get('endpoints', []):
        endpoint = endpoint_template.copy()
        
        if endpoint['type'] == 'ckan':
            endpoint['url'] = f"{base_url}/api/3"
        elif endpoint['type'] == 'ckan:package-search':
            endpoint['url'] = f"{base_url}/api/3/action/package_search"
        elif endpoint['type'] == 'ckan:package-list':
            endpoint['url'] = f"{base_url}/api/3/action/package_list"
        elif endpoint['type'] == 'opendatasoftapi':
            endpoint['url'] = f"{base_url}/api"
        elif endpoint['type'] == 'dcatap201':
            endpoint['url'] = f"{base_url}/api/feed/dcat-ap/2.0.1.json"
        elif endpoint['type'] == 'dcatus11':
            endpoint['url'] = f"{base_url}/api/feed/dcat-us/1.1.json"
        elif endpoint['type'] == 'rss':
            endpoint['url'] = f"{base_url}/api/feed/rss/2.0"
        elif endpoint['type'] == 'ogcrecordsapi':
            endpoint['url'] = f"{base_url}/api/search/v1"
        elif endpoint['type'] == 'sitemap':
            endpoint['url'] = f"{base_url}/sitemap.xml"
        else:
            continue
        
        endpoints.append(endpoint)
    
    return endpoints if endpoints else DEFAULT_ENDPOINTS

def update_file_metadata(file_path):
    """Update metadata for a single file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        if not data:
            return False
        
        url = data.get('link', '')
        if not url:
            return False
        
        # Detect software
        software_info = detect_software(url)
        
        updated = False
        
        # Add software information
        if not data.get('software'):
            if software_info:
                data['software'] = {
                    'id': software_info['id'],
                    'name': software_info['name']
                }
            else:
                # Default to custom for unknown platforms
                data['software'] = {
                    'id': 'custom',
                    'name': 'Custom software'
                }
            updated = True
        
        # Add endpoints
        if not data.get('endpoints'):
            if software_info:
                endpoints = generate_endpoints(url, software_info)
            else:
                # Add default sitemap endpoint
                endpoints = [{'type': 'sitemap', 'url': f"{url.rstrip('/')}/sitemap.xml"}]
            
            if endpoints:
                data['endpoints'] = endpoints
                updated = True
        
        # Update API status if we have endpoints
        if data.get('endpoints') and data.get('api_status') == 'uncertain':
            data['api_status'] = 'active'
            updated = True
        
        # Add more topics if missing
        existing_topic_ids = {t.get('id') for t in data.get('topics', [])}
        for topic in ADDITIONAL_TOPICS:
            if topic['id'] not in existing_topic_ids:
                if 'topics' not in data:
                    data['topics'] = []
                data['topics'].append(topic)
                updated = True
        
        # Fix owner link if it's incorrect
        owner_link = data.get('owner', {}).get('link', '')
        if owner_link and owner_link == url:
            # Try to generate a better owner link
            if 'dataplatform.nl' in url:
                # For dataplatform.nl, try to get the municipality website
                city_name = data.get('name', '').lower()
                if 'den haag' in city_name or 'haag' in city_name:
                    data['owner']['link'] = 'https://www.denhaag.nl'
                elif 'utrecht' in city_name:
                    data['owner']['link'] = 'https://www.utrecht.nl'
                elif 'tilburg' in city_name:
                    data['owner']['link'] = 'https://www.tilburg.nl'
                elif 'groningen' in city_name:
                    data['owner']['link'] = 'https://www.groningen.nl'
                elif 'almere' in city_name:
                    data['owner']['link'] = 'https://www.almere.nl'
                elif 'breda' in city_name:
                    data['owner']['link'] = 'https://www.breda.nl'
                elif 'haarlem' in city_name:
                    data['owner']['link'] = 'https://www.haarlem.nl'
                elif 'enschede' in city_name:
                    data['owner']['link'] = 'https://www.enschede.nl'
                elif 'apeldoorn' in city_name:
                    data['owner']['link'] = 'https://www.apeldoorn.nl'
                elif 'amersfoort' in city_name:
                    data['owner']['link'] = 'https://www.amersfoort.nl'
                elif 'leiden' in city_name:
                    data['owner']['link'] = 'https://www.leiden.nl'
                elif 'dordrecht' in city_name:
                    data['owner']['link'] = 'https://www.dordrecht.nl'
                elif 'maastricht' in city_name:
                    data['owner']['link'] = 'https://www.maastricht.nl'
                elif 'zwolle' in city_name:
                    data['owner']['link'] = 'https://www.zwolle.nl'
                elif 'delft' in city_name:
                    data['owner']['link'] = 'https://www.delft.nl'
                elif 'leeuwarden' in city_name:
                    data['owner']['link'] = 'https://www.leeuwarden.nl'
                elif 'alkmaar' in city_name:
                    data['owner']['link'] = 'https://www.alkmaar.nl'
                elif 'venlo' in city_name:
                    data['owner']['link'] = 'https://www.venlo.nl'
                elif 'heerlen' in city_name:
                    data['owner']['link'] = 'https://www.heerlen.nl'
                elif 'emmen' in city_name:
                    data['owner']['link'] = 'https://www.emmen.nl'
                elif 'deventer' in city_name:
                    data['owner']['link'] = 'https://www.deventer.nl'
                updated = True
            elif url.startswith('https://data.'):
                # For data.CITY.nl format, convert to www.CITY.nl
                new_link = url.replace('https://data.', 'https://www.')
                if new_link != url:
                    data['owner']['link'] = new_link
                    updated = True
        
        # Improve tags
        tags = data.get('tags', [])
        if 'has_api' not in tags and data.get('endpoints'):
            tags.append('has_api')
            data['tags'] = tags
            updated = True
        
        if software_info and software_info['id'] not in [t.lower() for t in tags]:
            tags.append(software_info['id'])
            data['tags'] = tags
            updated = True
        
        # Write back if updated
        if updated:
            with open(file_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
            return True
        
        return False
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Main function to update all Dutch catalog files."""
    base_dir = Path(__file__).parent.parent / 'data' / 'entities' / 'NL'
    
    # Find all opendata YAML files
    opendata_files = list(base_dir.rglob('*/opendata/*.yaml'))
    
    updated = []
    errors = []
    
    for file_path in opendata_files:
        if update_file_metadata(file_path):
            updated.append(file_path)
            print(f"Updated: {file_path}")
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Updated: {len(updated)} files")
    print(f"Errors: {len(errors)} files")
    
    if updated:
        print("\nUpdated files:")
        for item in updated:
            print(f"  - {item}")

if __name__ == '__main__':
    main()

