#!/usr/bin/env python3
"""
Generate a detailed CSV report of countries without national catalogs
and their suggested catalogs to mark as national.
"""

import os
import yaml
import csv
from collections import defaultdict
from pathlib import Path

def load_yaml_file(file_path):
    """Load and parse a YAML file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
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
                'catalog_type': record.get('catalog_type', ''),
                'country_name': owner_country.get('name', country_id)
            }
            
            countries_data[country_id]['catalogs'].append(catalog_info)
            
            if catalog_info['is_federal']:
                countries_data[country_id]['federal_catalogs'].append(catalog_info)
    
    # Find countries without national catalogs
    countries_without_national = {}
    for country, data in countries_data.items():
        if not data['has_national'] and len(data['catalogs']) > 0:
            countries_without_national[country] = data
    
    return countries_without_national

def generate_csv_report():
    """Generate a CSV report of suggested catalogs."""
    countries_without_national = analyze_catalogs()
    
    # Sort by number of federal catalogs
    sorted_countries = sorted(
        countries_without_national.items(),
        key=lambda x: len(x[1]['federal_catalogs']),
        reverse=True
    )
    
    # Write CSV report
    with open('suggested_national_catalogs.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'country_code',
            'country_name',
            'catalog_name',
            'catalog_id',
            'catalog_type',
            'owner_type',
            'is_federal_path',
            'link',
            'file_path',
            'priority'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for country_code, data in sorted_countries:
            country_name = data['federal_catalogs'][0]['country_name'] if data['federal_catalogs'] else country_code
            
            # Determine priority
            num_federal = len(data['federal_catalogs'])
            if num_federal >= 10:
                priority = 'HIGH'
            elif num_federal >= 5:
                priority = 'MEDIUM'
            else:
                priority = 'LOW'
            
            for catalog in data['federal_catalogs']:
                is_federal_path = 'Yes' if 'Federal' in catalog['file'] else 'No'
                writer.writerow({
                    'country_code': country_code,
                    'country_name': country_name,
                    'catalog_name': catalog['name'],
                    'catalog_id': catalog['id'],
                    'catalog_type': catalog['catalog_type'],
                    'owner_type': catalog['owner_type'],
                    'is_federal_path': is_federal_path,
                    'link': catalog['link'],
                    'file_path': catalog['file'],
                    'priority': priority
                })
    
    print(f"CSV report generated: suggested_national_catalogs.csv")
    print(f"Total countries: {len(sorted_countries)}")
    print(f"Total catalogs: {sum(len(d[1]['federal_catalogs']) for d in sorted_countries)}")

if __name__ == '__main__':
    generate_csv_report()

