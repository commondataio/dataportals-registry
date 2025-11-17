#!/usr/bin/env python3
"""
Script to find missing France data portals by comparing CSV list with existing registry
"""

import os
import sys
import json
import csv
from urllib.parse import urlparse
from pathlib import Path

# Add scripts directory to path for yaml import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
import yaml

ROOT_DIR = Path(__file__).parent.parent / "data" / "entities" / "FR"
SCHEDULED_DIR = Path(__file__).parent.parent / "data" / "scheduled" / "FR"
CSV_FILE = Path(__file__).parent / "PTF territoriales de données.csv"

def normalize_url(url):
    """Normalize URL for comparison"""
    if not url:
        return None
    url = url.strip()
    if not url:
        return None
    # Remove trailing slash
    url = url.rstrip('/')
    # Convert to lowercase
    url = url.lower()
    return url

def get_domain(url):
    """Extract domain from URL"""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Remove www. prefix for comparison
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return None

def load_existing_portals():
    """Load all existing France portals from YAML files"""
    portals = []
    urls = set()
    domains = set()
    
    # Load from entities
    if ROOT_DIR.exists():
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
                                url = normalize_url(data.get('link', ''))
                                if url:
                                    urls.add(url)
                                    domain = get_domain(url)
                                    if domain:
                                        domains.add(domain)
                    except Exception as e:
                        print(f"Error reading {filepath}: {e}", file=sys.stderr)
    
    # Load from scheduled
    if SCHEDULED_DIR.exists():
        for root, dirs, files in os.walk(SCHEDULED_DIR):
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
                                    'file': filepath,
                                    'status': 'scheduled'
                                }
                                portals.append(portal_info)
                                # Normalize URL for comparison
                                url = normalize_url(data.get('link', ''))
                                if url:
                                    urls.add(url)
                                    domain = get_domain(url)
                                    if domain:
                                        domains.add(domain)
                    except Exception as e:
                        print(f"Error reading {filepath}: {e}", file=sys.stderr)
    
    return portals, urls, domains

def check_url_exists(url, existing_urls, existing_domains):
    """Check if a URL or its domain already exists"""
    if not url:
        return False
    
    url_norm = normalize_url(url)
    if not url_norm:
        return False
    
    # Check exact match
    if url_norm in existing_urls:
        return True
    
    # Check domain match
    domain = get_domain(url_norm)
    if domain and domain in existing_domains:
        return True
    
    # Also check with www prefix
    if domain:
        if domain.startswith('www.'):
            alt_domain = domain[4:]
        else:
            alt_domain = f'www.{domain}'
        if alt_domain in existing_domains:
            return True
    
    return False

def load_csv_portals():
    """Load portals from CSV file"""
    portals = []
    
    if not CSV_FILE.exists():
        print(f"Error: CSV file not found: {CSV_FILE}", file=sys.stderr)
        return portals
    
    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:  # utf-8-sig handles BOM
        reader = csv.DictReader(f)
        for row in reader:
            # Handle BOM in column name
            name_key = 'nom de plateforme'
            if name_key not in row:
                # Try with BOM
                name_key = '\ufeffnom de plateforme'
            
            name = row.get(name_key, '').strip()
            url = row.get('url', '').strip() or row.get('Lien', '').strip()
            region = row.get('Région', '').strip()
            portal_type = row.get('type', '').strip()
            techno = row.get('techno', '').strip()
            coverage = row.get('Couverture', '').strip()
            
            if name and url:
                portals.append({
                    'name': name,
                    'url': url,
                    'region': region,
                    'type': portal_type,
                    'techno': techno,
                    'coverage': coverage,
                    'raw_row': row
                })
    
    return portals

def main():
    print("=" * 80)
    print("France Data Portals - Missing Portals Analysis")
    print("=" * 80)
    
    # Load existing portals
    print("\nLoading existing portals from registry...")
    existing_portals, existing_urls, existing_domains = load_existing_portals()
    print(f"Found {len(existing_portals)} existing portals in registry")
    
    # Load CSV portals
    print(f"\nLoading portals from CSV file: {CSV_FILE}")
    csv_portals = load_csv_portals()
    print(f"Found {len(csv_portals)} portals in CSV file")
    
    # Compare
    print("\n" + "=" * 80)
    print("Comparing portals...")
    print("=" * 80)
    
    missing = []
    found = []
    
    for csv_portal in csv_portals:
        url = csv_portal['url']
        exists = check_url_exists(url, existing_urls, existing_domains)
        
        if exists:
            found.append(csv_portal)
        else:
            missing.append(csv_portal)
    
    print(f"\n✓ Found in registry: {len(found)}")
    print(f"✗ Missing from registry: {len(missing)}")
    
    # Print missing portals
    print("\n" + "=" * 80)
    print("MISSING PORTALS")
    print("=" * 80)
    
    # Group by region
    missing_by_region = {}
    for portal in missing:
        region = portal['region'] or 'Unknown'
        if region not in missing_by_region:
            missing_by_region[region] = []
        missing_by_region[region].append(portal)
    
    for region in sorted(missing_by_region.keys()):
        portals = missing_by_region[region]
        print(f"\n{region} ({len(portals)} portals):")
        print("-" * 80)
        for portal in sorted(portals, key=lambda x: x['name']):
            print(f"  • {portal['name']}")
            print(f"    URL: {portal['url']}")
            print(f"    Type: {portal['type']}")
            print(f"    Technology: {portal['techno']}")
            print(f"    Coverage: {portal['coverage']}")
            print()
    
    # Save report
    report = {
        'existing_count': len(existing_portals),
        'csv_count': len(csv_portals),
        'found_count': len(found),
        'missing_count': len(missing),
        'missing_portals': [
            {
                'name': p['name'],
                'url': p['url'],
                'region': p['region'],
                'type': p['type'],
                'techno': p['techno'],
                'coverage': p['coverage']
            }
            for p in missing
        ],
        'missing_by_region': {
            region: [
                {
                    'name': p['name'],
                    'url': p['url'],
                    'type': p['type'],
                    'techno': p['techno'],
                    'coverage': p['coverage']
                }
                for p in portals
            ]
            for region, portals in missing_by_region.items()
        }
    }
    
    report_file = Path(__file__).parent / "france_missing_portals_report.json"
    with open(report_file, 'w', encoding='utf8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"\nReport saved to: {report_file}")
    
    # Create markdown report
    md_report = Path(__file__).parent / "FRANCE_MISSING_PORTALS_REPORT.md"
    with open(md_report, 'w', encoding='utf8') as f:
        f.write("# France Data Portals - Missing Portals Report\n\n")
        f.write(f"**Generated:** {__import__('datetime').datetime.now().isoformat()}\n\n")
        f.write(f"- **Existing portals in registry:** {len(existing_portals)}\n")
        f.write(f"- **Portals in CSV:** {len(csv_portals)}\n")
        f.write(f"- **Found in registry:** {len(found)}\n")
        f.write(f"- **Missing from registry:** {len(missing)}\n\n")
        
        f.write("## Missing Portals by Region\n\n")
        
        for region in sorted(missing_by_region.keys()):
            portals = missing_by_region[region]
            f.write(f"### {region} ({len(portals)} portals)\n\n")
            f.write("| Name | URL | Type | Technology | Coverage |\n")
            f.write("|------|-----|------|------------|----------|\n")
            for portal in sorted(portals, key=lambda x: x['name']):
                name = portal['name'].replace('|', '\\|')
                url = portal['url'].replace('|', '\\|')
                ptype = portal['type'].replace('|', '\\|')
                techno = portal['techno'].replace('|', '\\|')
                coverage = portal['coverage'].replace('|', '\\|')
                f.write(f"| {name} | {url} | {ptype} | {techno} | {coverage} |\n")
            f.write("\n")
    
    print(f"Markdown report saved to: {md_report}")

if __name__ == '__main__':
    main()

