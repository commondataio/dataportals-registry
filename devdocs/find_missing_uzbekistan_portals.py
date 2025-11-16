#!/usr/bin/env python3
"""
Script to find missing data portals in Uzbekistan
"""

import os
import sys
import json
from urllib.parse import urlparse
from collections import defaultdict

# Add scripts directory to path for yaml import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts'))
import yaml

ROOT_DIR = "data/entities/UZ"

def load_existing_portals():
    """Load all existing Uzbekistan portals from YAML files"""
    portals = []
    urls = set()
    
    if not os.path.exists(ROOT_DIR):
        return portals, urls
    
    for root, dirs, files in os.walk(ROOT_DIR):
        for filename in files:
            if filename.endswith('.yaml') or filename.endswith('.yml'):
                filepath = os.path.join(root, filename)
                try:
                    with open(filepath, 'r', encoding='utf8') as f:
                        data = yaml.safe_load(f)
                        if data and 'link' in data:
                            portal_info = {
                                'id': data.get('id', ''),
                                'name': data.get('name', ''),
                                'link': data.get('link', ''),
                                'catalog_type': data.get('catalog_type', ''),
                                'file': filepath
                            }
                            portals.append(portal_info)
                            # Normalize URL for comparison
                            url = data.get('link', '').lower().rstrip('/')
                            urls.add(url)
                            # Also add without https://
                            if url.startswith('http'):
                                urls.add(urlparse(url).netloc.lower())
                except Exception as e:
                    print(f"Error reading {filepath}: {e}")
    
    return portals, urls

def check_url_exists(url, existing_urls):
    """Check if a URL or its domain already exists"""
    url_lower = url.lower().rstrip('/')
    
    # Check exact match
    if url_lower in existing_urls:
        return True
    
    # Check domain match
    if url_lower.startswith('http'):
        domain = urlparse(url_lower).netloc.lower()
        if domain in existing_urls:
            return True
        # Check without www
        if domain.startswith('www.'):
            if domain[4:] in existing_urls:
                return True
        else:
            if f'www.{domain}' in existing_urls:
                return True
    
    return False

def main():
    print("=" * 80)
    print("Uzbekistan Data Portals Analysis")
    print("=" * 80)
    
    # Load existing portals
    existing_portals, existing_urls = load_existing_portals()
    
    print(f"\nCurrently recorded portals: {len(existing_portals)}")
    print("\nExisting portals:")
    print("-" * 80)
    for portal in sorted(existing_portals, key=lambda x: x['catalog_type']):
        print(f"  [{portal['catalog_type']}] {portal['name']}")
        print(f"    URL: {portal['link']}")
        print(f"    ID: {portal['id']}")
        print()
    
    # Potentially missing portals based on research
    potential_portals = [
        {
            'name': 'Uzbekistan Open Data Portal (data.gov.uz)',
            'url': 'https://data.gov.uz',
            'url_alt': 'https://data.egov.uz',
            'description': 'National open data portal - might be same as data.egov.uz or different',
            'catalog_type': 'Open data portal',
            'source': 'Web search mentions data.gov.uz'
        },
        {
            'name': 'Old Data Portal',
            'url': 'https://olddata.gov.uz',
            'description': 'Old version of data portal mentioned in search results',
            'catalog_type': 'Open data portal',
            'source': 'Web search results'
        },
        {
            'name': 'Agency for Strategic Reforms Open Data',
            'url': 'https://asr.gov.uz/en/open-data',
            'description': 'Agency for Strategic Reforms under the President - open data section',
            'catalog_type': 'Open data portal',
            'source': 'Web search results'
        },
        {
            'name': 'e-Government Portal',
            'url': 'https://egov.uz',
            'url_alt': 'https://www.egov.uz',
            'description': 'e-Government portal - might have data catalog',
            'catalog_type': 'Open data portal',
            'source': 'Web search results'
        },
    ]
    
    print("\n" + "=" * 80)
    print("Potentially Missing Portals")
    print("=" * 80)
    
    missing = []
    for portal in potential_portals:
        url = portal['url']
        exists = check_url_exists(url, existing_urls)
        
        if not exists and 'url_alt' in portal:
            exists = check_url_exists(portal['url_alt'], existing_urls)
            if exists:
                print(f"\n✓ {portal['name']}")
                print(f"  URL: {url}")
                print(f"  Status: Alternative URL ({portal['url_alt']}) already exists")
                continue
        
        if not exists:
            missing.append(portal)
            print(f"\n✗ {portal['name']}")
            print(f"  URL: {url}")
            print(f"  Type: {portal['catalog_type']}")
            print(f"  Description: {portal['description']}")
            print(f"  Source: {portal['source']}")
        else:
            print(f"\n✓ {portal['name']}")
            print(f"  URL: {url}")
            print(f"  Status: Already exists in registry")
    
    print("\n" + "=" * 80)
    print(f"Summary: {len(missing)} potentially missing portals found")
    print("=" * 80)
    
    if missing:
        print("\nNote: According to Open Data Inception, Uzbekistan has 99 open data sources.")
        print(f"Currently, only {len(existing_portals)} portals are recorded in this registry.")
        print(f"This suggests there may be {99 - len(existing_portals)} additional portals to discover.")
        print("\nRecommendations:")
        print("1. Verify if data.gov.uz is the same as data.egov.uz or a different portal")
        print("2. Check ministry-specific portals (health, education, finance, etc.)")
        print("3. Check regional/local government portals")
        print("4. Review Open Data Inception list for complete catalog")
        print("5. Check ArcGIS Hub instances in Uzbekistan")
        print("6. Review government agency websites for data sections")
    
    # Save report
    report = {
        'existing_count': len(existing_portals),
        'existing_portals': existing_portals,
        'missing_count': len(missing),
        'missing_portals': missing,
        'note': 'Open Data Inception reports 99 sources for Uzbekistan'
    }
    
    with open('uzbekistan_portals_analysis.json', 'w', encoding='utf8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"\nReport saved to: uzbekistan_portals_analysis.json")

if __name__ == '__main__':
    main()

