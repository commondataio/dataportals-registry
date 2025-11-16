#!/usr/bin/env python3
"""
Script to find CKAN instances in Japan not listed in the registry.
"""
import os
import yaml
import requests
from urllib.parse import urljoin, urlparse
import json
from typing import List, Dict, Set
import time

# Known potential CKAN instances to check (from web searches and common patterns)
POTENTIAL_CKAN_URLS = [
    # From web searches
    "https://data.city.fukuoka.lg.jp",
    "https://opendata.city.fukuoka.lg.jp",
    "https://ckan.city.fukuoka.lg.jp",
    "https://data.city.sabae.fukui.jp",
    "https://opendata.city.sabae.fukui.jp",
    "https://ckan.city.sabae.fukui.jp",
    "https://data.pref.shimane.lg.jp",
    "https://opendata.pref.shimane.lg.jp",
    "https://ckan.pref.shimane.lg.jp",
    "https://data.pref.fukuoka.lg.jp",
    "https://opendata.pref.fukuoka.lg.jp",
    "https://ckan.pref.fukuoka.lg.jp",
    "https://data.pref.kanagawa.lg.jp",
    "https://opendata.pref.kanagawa.lg.jp",
    "https://ckan.pref.kanagawa.lg.jp",
    "https://catalog.opendata.pref.kanagawa.lg.jp",
    "https://data.city.ikoma.lg.jp",  # Already in registry, but checking
    "https://opendata.city.ikoma.lg.jp",
    "https://ckan.city.ikoma.lg.jp",
    # Common patterns for Japanese municipalities
    "https://data.city.kitamoto.saitama.jp",
    "https://opendata.city.kitamoto.saitama.jp",
    "https://ckan.city.kitamoto.saitama.jp",
    # More potential instances
    "https://data.pref.fukui.lg.jp",
    "https://opendata.pref.fukui.lg.jp",
    "https://ckan.pref.fukui.lg.jp",
    "https://data.pref.ishikawa.lg.jp",
    "https://opendata.pref.ishikawa.lg.jp",
    "https://ckan.pref.ishikawa.lg.jp",
    "https://data.pref.hyogo.lg.jp",
    "https://opendata.pref.hyogo.lg.jp",
    "https://ckan.pref.hyogo.lg.jp",
    "https://data.pref.okayama.lg.jp",
    "https://opendata.pref.okayama.lg.jp",
    "https://ckan.pref.okayama.lg.jp",
    "https://data.pref.hiroshima.lg.jp",
    "https://opendata.pref.hiroshima.lg.jp",
    "https://ckan.pref.hiroshima.lg.jp",
    "https://data.pref.yamaguchi.lg.jp",
    "https://opendata.pref.yamaguchi.lg.jp",
    "https://ckan.pref.yamaguchi.lg.jp",
    "https://data.pref.tokushima.lg.jp",
    "https://opendata.pref.tokushima.lg.jp",
    "https://ckan.pref.tokushima.lg.jp",
    "https://data.pref.kagawa.lg.jp",
    "https://opendata.pref.kagawa.lg.jp",
    "https://ckan.pref.kagawa.lg.jp",
    "https://data.pref.ehime.lg.jp",
    "https://opendata.pref.ehime.lg.jp",
    "https://ckan.pref.ehime.lg.jp",
    "https://data.pref.kochi.lg.jp",
    "https://opendata.pref.kochi.lg.jp",
    "https://ckan.pref.kochi.lg.jp",
    "https://data.pref.fukuoka.lg.jp",
    "https://opendata.pref.fukuoka.lg.jp",
    "https://ckan.pref.fukuoka.lg.jp",
    "https://data.pref.saga.lg.jp",
    "https://opendata.pref.saga.lg.jp",
    "https://ckan.pref.saga.lg.jp",
    "https://data.pref.nagasaki.lg.jp",
    "https://opendata.pref.nagasaki.lg.jp",
    "https://ckan.pref.nagasaki.lg.jp",
    "https://data.pref.kumamoto.lg.jp",
    "https://opendata.pref.kumamoto.lg.jp",
    "https://ckan.pref.kumamoto.lg.jp",
    "https://data.pref.oita.lg.jp",
    "https://opendata.pref.oita.lg.jp",
    "https://ckan.pref.oita.lg.jp",
    "https://data.pref.miyazaki.lg.jp",
    "https://opendata.pref.miyazaki.lg.jp",
    "https://ckan.pref.miyazaki.lg.jp",
    "https://data.pref.kagoshima.lg.jp",
    "https://opendata.pref.kagoshima.lg.jp",
    "https://ckan.pref.kagoshima.lg.jp",
    "https://data.pref.okinawa.lg.jp",
    "https://opendata.pref.okinawa.lg.jp",
    "https://ckan.pref.okinawa.lg.jp",
]

def load_existing_ckan_urls() -> Set[str]:
    """Load all existing CKAN URLs from Japan records."""
    existing_urls = set()
    jp_dir = "data/entities/JP"
    
    if not os.path.exists(jp_dir):
        return existing_urls
    
    for root, dirs, files in os.walk(jp_dir):
        for file in files:
            if file.endswith(".yaml"):
                filepath = os.path.join(root, file)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        record = yaml.safe_load(f)
                        if record and record.get('software', {}).get('id') == 'ckan':
                            # Get link URL
                            if 'link' in record:
                                existing_urls.add(record['link'].rstrip('/'))
                            # Get endpoint URLs
                            if 'endpoints' in record:
                                for endpoint in record['endpoints']:
                                    if 'url' in endpoint:
                                        url = endpoint['url'].rstrip('/')
                                        # Extract base URL from API endpoint
                                        if '/api/3' in url:
                                            base_url = url.split('/api/3')[0]
                                            existing_urls.add(base_url)
                                        else:
                                            existing_urls.add(url)
                except Exception as e:
                    print(f"Error reading {filepath}: {e}")
    
    return existing_urls

def is_ckan_instance(url: str, timeout: int = 5) -> Dict:
    """Check if a URL is a CKAN instance."""
    result = {
        'url': url,
        'is_ckan': False,
        'api_url': None,
        'error': None
    }
    
    # Try common CKAN API endpoints
    api_endpoints = [
        '/api/3/action/package_list',
        '/api/3',
        '/api/action/package_list',
    ]
    
    for endpoint_path in api_endpoints:
        api_url = urljoin(url.rstrip('/'), endpoint_path)
        try:
            response = requests.get(
                api_url,
                timeout=timeout,
                headers={'User-Agent': 'Mozilla/5.0 (compatible; CKAN-Discovery/1.0)'},
                allow_redirects=True
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    # Check if it's a CKAN API response
                    if isinstance(data, dict):
                        if data.get('success') is True or 'result' in data or 'help' in data:
                            result['is_ckan'] = True
                            result['api_url'] = api_url
                            return result
                except json.JSONDecodeError:
                    pass
        except requests.exceptions.RequestException as e:
            result['error'] = str(e)
            continue
    
    return result

def main():
    """Main function to discover CKAN instances."""
    print("Loading existing CKAN URLs from registry...")
    existing_urls = load_existing_ckan_urls()
    print(f"Found {len(existing_urls)} existing CKAN URLs in registry")
    
    print("\nChecking potential CKAN instances...")
    found_ckan = []
    not_ckan = []
    errors = []
    
    for url in POTENTIAL_CKAN_URLS:
        # Normalize URL
        if not url.startswith('http'):
            url = f"https://{url}"
        url = url.rstrip('/')
        
        # Skip if already in registry
        if url in existing_urls:
            print(f"SKIP (in registry): {url}")
            continue
        
        print(f"Checking: {url}")
        result = is_ckan_instance(url)
        
        if result['is_ckan']:
            found_ckan.append(result)
            print(f"  ✓ CKAN found! API: {result['api_url']}")
        elif result['error']:
            errors.append(result)
            print(f"  ✗ Error: {result['error']}")
        else:
            not_ckan.append(result)
            print(f"  ✗ Not CKAN")
        
        time.sleep(0.5)  # Be polite
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"\nFound {len(found_ckan)} CKAN instances NOT in registry:")
    for item in found_ckan:
        print(f"  - {item['url']}")
        print(f"    API: {item['api_url']}")
    
    if errors:
        print(f"\n{len(errors)} URLs had errors (may need manual checking):")
        for item in errors:
            print(f"  - {item['url']}: {item['error']}")
    
    # Save results to file
    if found_ckan:
        output_file = "japan_ckan_discovered.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(found_ckan, f, indent=2, ensure_ascii=False)
        print(f"\nResults saved to {output_file}")

if __name__ == "__main__":
    main()

