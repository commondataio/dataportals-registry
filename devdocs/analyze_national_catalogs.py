#!/usr/bin/env python3
"""
Script to find countries without national catalogs and suggest which catalogs
should be marked as national.
"""

import os
import yaml
from collections import defaultdict
from pathlib import Path

def load_yaml_file(file_path):
    """Load and parse a YAML file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return None

def is_national_catalog(record):
    """Check if a catalog is marked as national."""
    if not record:
        return False
    properties = record.get('properties', {})
    return properties.get('is_national', False) is True

def is_federal_or_central(record, file_path):
    """Check if a catalog is federal/central government level."""
    if not record:
        return False
    
    # Check if path contains "Federal"
    if 'Federal' in str(file_path):
        return True
    
    # Check owner type
    owner = record.get('owner', {})
    owner_type = owner.get('type', '')
    if owner_type in ['Central government', 'Federal government']:
        return True
    
    # Check coverage level (20 = national level)
    coverage = record.get('coverage', [])
    for cov in coverage:
        location = cov.get('location', {})
        level = location.get('level')
        if level == 20:  # National level
            return True
    
    return False

def analyze_catalogs():
    """Analyze all catalogs and find countries without national catalogs."""
    entities_dir = Path('data/entities')
    
    # Dictionary to store catalogs by country
    countries_data = defaultdict(lambda: {
        'has_national': False,
        'catalogs': [],
        'federal_catalogs': []
    })
    
    # Get all country directories (excluding special directories)
    special_dirs = {'Africa', 'ASEAN', 'Caribbean', 'LatinAmerica', 'Oceania', 'World', 'Unknown', 'EU'}
    
    # Scan all YAML files
    for yaml_file in entities_dir.rglob('*.yaml'):
        # Skip special directories
        parts = yaml_file.parts
        if len(parts) > 2 and parts[1] in special_dirs:
            continue
        
        # Extract country code from path (first directory after entities)
        if len(parts) >= 2:
            country_code = parts[1]
            
            # Skip if it's a special directory
            if country_code in special_dirs:
                continue
            
            record = load_yaml_file(yaml_file)
            if not record:
                continue
            
            # Get country from record (more reliable)
            country_id = None
            owner = record.get('owner', {})
            owner_location = owner.get('location', {})
            owner_country = owner_location.get('country', {})
            country_id = owner_country.get('id')
            
            # Fallback to path-based country code
            if not country_id:
                country_id = country_code
            
            # Check if this is a national catalog
            if is_national_catalog(record):
                countries_data[country_id]['has_national'] = True
            
            # Store catalog info
            catalog_info = {
                'file': str(yaml_file),
                'name': record.get('name', 'Unknown'),
                'id': record.get('id', 'Unknown'),
                'owner_type': owner.get('type', 'Unknown'),
                'is_national': is_national_catalog(record),
                'is_federal': is_federal_or_central(record, yaml_file),
                'link': record.get('link', ''),
                'catalog_type': record.get('catalog_type', '')
            }
            
            countries_data[country_id]['catalogs'].append(catalog_info)
            
            if catalog_info['is_federal']:
                countries_data[country_id]['federal_catalogs'].append(catalog_info)
    
    # Find countries without national catalogs
    countries_without_national = {}
    for country, data in countries_data.items():
        if not data['has_national'] and len(data['catalogs']) > 0:
            countries_without_national[country] = data
    
    return countries_without_national, countries_data

def main():
    """Main function."""
    print("Analyzing catalogs...")
    countries_without_national, all_countries = analyze_catalogs()
    
    print(f"\n{'='*80}")
    print(f"COUNTRIES WITHOUT NATIONAL CATALOGS: {len(countries_without_national)}")
    print(f"{'='*80}\n")
    
    # Sort by number of federal catalogs (most promising first)
    sorted_countries = sorted(
        countries_without_national.items(),
        key=lambda x: len(x[1]['federal_catalogs']),
        reverse=True
    )
    
    for country, data in sorted_countries:
        print(f"\n{'─'*80}")
        print(f"Country: {country}")
        print(f"Total catalogs: {len(data['catalogs'])}")
        print(f"Federal/Central government catalogs: {len(data['federal_catalogs'])}")
        
        if data['federal_catalogs']:
            print(f"\n  SUGGESTED CATALOGS TO MARK AS NATIONAL:")
            for catalog in data['federal_catalogs']:
                print(f"    • {catalog['name']}")
                print(f"      ID: {catalog['id']}")
                print(f"      Type: {catalog['catalog_type']}")
                print(f"      Owner: {catalog['owner_type']}")
                print(f"      Link: {catalog['link']}")
                print(f"      File: {catalog['file']}")
                print()
        else:
            # Show all catalogs if no federal ones found
            print(f"\n  ALL CATALOGS (no federal/central ones found):")
            for catalog in data['catalogs'][:5]:  # Show first 5
                print(f"    • {catalog['name']} ({catalog['owner_type']})")
            if len(data['catalogs']) > 5:
                print(f"    ... and {len(data['catalogs']) - 5} more")
    
    # Summary statistics
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Total countries analyzed: {len(all_countries)}")
    print(f"Countries without national catalogs: {len(countries_without_national)}")
    
    countries_with_federal = sum(1 for d in countries_without_national.values() if d['federal_catalogs'])
    print(f"Countries with federal/central catalogs to mark: {countries_with_federal}")
    
    total_suggestions = sum(len(d['federal_catalogs']) for d in countries_without_national.values())
    print(f"Total catalogs suggested to mark as national: {total_suggestions}")

if __name__ == '__main__':
    main()

