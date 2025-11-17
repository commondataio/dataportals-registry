#!/usr/bin/env python3
"""
Script to add missing France data portals from CSV to the registry
"""

import os
import sys
import json
import re
import yaml
from pathlib import Path
from urllib.parse import urlparse

# Add scripts directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

REPORT_FILE = Path(__file__).parent / "france_missing_portals_report.json"
ENTITIES_DIR = Path(__file__).parent.parent / "data" / "entities" / "FR"

# Region name to ISO 3166-2 code mapping
REGION_MAP = {
    "Auvergne-Rhône-Alpes": "FR-ARA",
    "Bourgogne-Franche-Comté": "FR-BFC",
    "Bretagne": "FR-BRE",
    "Centre-Val de Loire": "FR-CVL",
    "Corse": "FR-20R",  # Corsica
    "Grand Est": "FR-GES",
    "Guadeloupe": "FR-971",
    "Guyane": "FR-GF",  # French Guiana
    "Hauts-de-France": "FR-HDF",
    "Île-de-France": "FR-IDF",
    "La Réunion": "FR-974",
    "Martinique": "FR-972",
    "Normandie": "FR-NOR",
    "Nouvelle-Aquitaine": "FR-NAQ",
    "Nouvelle-Calédonie": "FR-NC",
    "Occitanie": "FR-OCC",
    "Pays de la Loire": "FR-PDL",
    "Polynésie": "FR-PF",  # French Polynesia
    "Provence-Alpes-Côte d'Azur": "FR-PAC",
}

# Technology to software ID mapping
SOFTWARE_MAP = {
    "ArcGIS": "arcgishub",
    "CKAN": "ckan",
    "OpenDataSoft": "opendatasoft",
    "GeoNetwork": "geonetwork",
    "uData": "udata",
    "Datafair": "datafair",
    "Onegeo Suite": "onegeo",
    "Isogéo": "isigeo",
    "GeoServer": "geoserver",
    "Geoportal Server": "geoportalserver",
    "GeoSource": "geosource",
    "WebDAV": "custom",
    "Typo3": "custom",
    "Wordpress": "wordpress",
    "Spécifique": "custom",
    "OD Gazette": "custom",
    "Mobapi": "custom",
    "Metaclic": "custom",
    "CMS.eolas": "custom",
    "Spip": "custom",
}

# Type to catalog_type mapping
CATALOG_TYPE_MAP = {
    "Géoportail": "Geoportal",
    "Portail": "Open data portal",
    "Site web": "Open data portal",
}

# Coverage to level mapping
COVERAGE_LEVEL_MAP = {
    "Régionale": 30,  # Regional level
    "Départementale": 40,  # Department level
    "Métropolitaine": 50,  # Metropolitan level
    "Intercommunale": 50,  # Intercommunal level
    "Communale": 60,  # Municipal level
    "Locale": 60,  # Local level
}

def generate_id_from_url(url):
    """Generate a unique ID from the portal URL."""
    # Remove protocol and www
    url = url.replace('https://', '').replace('http://', '').replace('www.', '')
    # Remove trailing slash
    url = url.rstrip('/')
    # Replace dots and special chars with nothing, keep only alphanumeric
    id_str = re.sub(r'[^a-zA-Z0-9]', '', url)
    return id_str.lower()

def get_software_id(techno):
    """Get software ID from technology name."""
    return SOFTWARE_MAP.get(techno, "custom")

def get_catalog_type(portal_type):
    """Get catalog type from portal type."""
    return CATALOG_TYPE_MAP.get(portal_type, "Open data portal")

def get_directory_path(region_code, coverage, catalog_type):
    """Determine the directory path for the portal."""
    base_dir = ENTITIES_DIR
    
    # For regional coverage, use region code directly
    if coverage == "Régionale":
        if region_code:
            subdir = region_code
        else:
            subdir = "Federal"
    # For other coverage levels, try to use region code if available
    elif region_code and region_code != "FR-20R":  # Exclude Corsica special case
        subdir = region_code
    else:
        subdir = "Federal"
    
    # Determine subdirectory based on catalog type
    if catalog_type == "Geoportal":
        type_dir = "geo"
    elif catalog_type == "Open data portal":
        type_dir = "opendata"
    else:
        type_dir = "opendata"
    
    return base_dir / subdir / type_dir

def create_portal_yaml(portal_data):
    """Create YAML entry for a portal."""
    name = portal_data['name']
    url = portal_data['url']
    region = portal_data['region']
    portal_type = portal_data['type']
    techno = portal_data['techno']
    coverage = portal_data['coverage']
    
    # Generate ID
    portal_id = generate_id_from_url(url)
    
    # Get mappings
    region_code = REGION_MAP.get(region)
    software_id = get_software_id(techno)
    catalog_type = get_catalog_type(portal_type)
    level = COVERAGE_LEVEL_MAP.get(coverage, 20)
    
    # Build YAML structure
    yaml_data = {
        'access_mode': ['open'],
        'api': False,
        'api_status': 'uncertain',
        'catalog_type': catalog_type,
        'content_types': ['dataset'],
        'coverage': [{
            'location': {
                'country': {
                    'id': 'FR',
                    'name': 'France'
                },
                'level': level,
                'macroregion': {
                    'id': '155',
                    'name': 'Western Europe'
                }
            }
        }],
        'id': portal_id,
        'langs': [{
            'id': 'FR',
            'name': 'French'
        }],
        'link': url,
        'name': name,
        'owner': {
            'link': None,
            'location': {
                'country': {
                    'id': 'FR',
                    'name': 'France'
                },
                'level': level
            },
            'name': name,
            'type': 'Local government'
        },
        'properties': {
            'has_doi': False
        },
        'rights': {
            'license_id': None,
            'license_name': None,
            'license_url': None,
            'privacy_policy_url': None,
            'rights_type': 'unknown',
            'tos_url': None
        },
        'software': {
            'id': software_id,
            'name': techno if techno != "Spécifique" else "Custom software"
        },
        'status': 'active',
        'tags': [
            'open data',
            'France'
        ]
    }
    
    # Add subregion if region code is available
    if region_code:
        yaml_data['coverage'][0]['location']['subregion'] = {
            'id': region_code,
            'name': region
        }
        yaml_data['owner']['location']['subregion'] = {
            'id': region_code,
            'name': region
        }
    
    # Add map_layer to content_types for geoportals
    if catalog_type == "Geoportal":
        yaml_data['content_types'].append('map_layer')
    
    # Add basic topics
    yaml_data['topics'] = [
        {
            'id': 'GOVE',
            'name': 'Government and public sector',
            'type': 'eudatatheme'
        }
    ]
    
    return yaml_data, portal_id

def main():
    print("=" * 80)
    print("Adding Missing France Data Portals to Registry")
    print("=" * 80)
    
    # Load missing portals from report
    if not REPORT_FILE.exists():
        print(f"Error: Report file not found: {REPORT_FILE}")
        return
    
    with open(REPORT_FILE, 'r', encoding='utf-8') as f:
        report = json.load(f)
    
    missing_portals = report.get('missing_portals', [])
    print(f"\nFound {len(missing_portals)} missing portals to add")
    
    created = []
    errors = []
    
    for portal_data in missing_portals:
        try:
            name = portal_data['name']
            url = portal_data['url']
            region = portal_data['region']
            
            print(f"\nProcessing: {name}")
            print(f"  URL: {url}")
            print(f"  Region: {region}")
            
            # Create YAML data
            yaml_data, portal_id = create_portal_yaml(portal_data)
            
            # Determine directory
            region_code = REGION_MAP.get(region)
            catalog_type = get_catalog_type(portal_data['type'])
            coverage = portal_data['coverage']
            dir_path = get_directory_path(region_code, coverage, catalog_type)
            
            # Create directory if needed
            dir_path.mkdir(parents=True, exist_ok=True)
            
            # Write YAML file
            yaml_file = dir_path / f"{portal_id}.yaml"
            
            if yaml_file.exists():
                print(f"  ⚠️  File already exists: {yaml_file}")
                errors.append(f"{name}: File already exists")
                continue
            
            with open(yaml_file, 'w', encoding='utf-8') as f:
                yaml.safe_dump(yaml_data, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
            
            print(f"  ✓ Created: {yaml_file}")
            created.append({
                'name': name,
                'id': portal_id,
                'file': str(yaml_file)
            })
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            errors.append(f"{portal_data.get('name', 'Unknown')}: {e}")
    
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"✓ Created: {len(created)} portals")
    print(f"✗ Errors: {len(errors)} portals")
    
    if errors:
        print("\nErrors:")
        for error in errors:
            print(f"  - {error}")
    
    print(f"\nSuccessfully added {len(created)} portals to the registry!")

if __name__ == '__main__':
    main()

