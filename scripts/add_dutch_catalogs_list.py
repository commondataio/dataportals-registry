#!/usr/bin/env python3
"""
Script to add common Dutch open data catalogs to the registry.
Based on the list from data.overheid.nl/community/catalogi
"""

import yaml
from pathlib import Path
import re

# Common Dutch open data catalogs that should be added
# This list includes common municipalities and organizations
DUTCH_CATALOGS = [
    {
        'name': 'Open Data Den Haag',
        'url': 'https://denhaag.dataplatform.nl',
        'description': 'Open data portal of the municipality of The Hague (Den Haag)',
        'province': 'NL-ZH',
        'city': 'Den Haag',
        'owner_name': 'Gemeente Den Haag',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Utrecht',
        'url': 'https://data.utrecht.nl',
        'description': 'Open data portal of the municipality of Utrecht',
        'province': 'NL-UT',
        'city': 'Utrecht',
        'owner_name': 'Gemeente Utrecht',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Tilburg',
        'url': 'https://data.tilburg.nl',
        'description': 'Open data portal of the municipality of Tilburg',
        'province': 'NL-NB',
        'city': 'Tilburg',
        'owner_name': 'Gemeente Tilburg',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Groningen',
        'url': 'https://data.groningen.nl',
        'description': 'Open data portal of the municipality of Groningen',
        'province': 'NL-GR',
        'city': 'Groningen',
        'owner_name': 'Gemeente Groningen',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Almere',
        'url': 'https://data.almere.nl',
        'description': 'Open data portal of the municipality of Almere',
        'province': 'NL-FL',
        'city': 'Almere',
        'owner_name': 'Gemeente Almere',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Breda',
        'url': 'https://data.breda.nl',
        'description': 'Open data portal of the municipality of Breda',
        'province': 'NL-NB',
        'city': 'Breda',
        'owner_name': 'Gemeente Breda',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Haarlem',
        'url': 'https://data.haarlem.nl',
        'description': 'Open data portal of the municipality of Haarlem',
        'province': 'NL-NH',
        'city': 'Haarlem',
        'owner_name': 'Gemeente Haarlem',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Enschede',
        'url': 'https://data.enschede.nl',
        'description': 'Open data portal of the municipality of Enschede',
        'province': 'NL-OV',
        'city': 'Enschede',
        'owner_name': 'Gemeente Enschede',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Apeldoorn',
        'url': 'https://data.apeldoorn.nl',
        'description': 'Open data portal of the municipality of Apeldoorn',
        'province': 'NL-GE',
        'city': 'Apeldoorn',
        'owner_name': 'Gemeente Apeldoorn',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Amersfoort',
        'url': 'https://data.amersfoort.nl',
        'description': 'Open data portal of the municipality of Amersfoort',
        'province': 'NL-UT',
        'city': 'Amersfoort',
        'owner_name': 'Gemeente Amersfoort',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Leiden',
        'url': 'https://data.leiden.nl',
        'description': 'Open data portal of the municipality of Leiden',
        'province': 'NL-ZH',
        'city': 'Leiden',
        'owner_name': 'Gemeente Leiden',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Dordrecht',
        'url': 'https://data.dordrecht.nl',
        'description': 'Open data portal of the municipality of Dordrecht',
        'province': 'NL-ZH',
        'city': 'Dordrecht',
        'owner_name': 'Gemeente Dordrecht',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Maastricht',
        'url': 'https://data.maastricht.nl',
        'description': 'Open data portal of the municipality of Maastricht',
        'province': 'NL-LI',
        'city': 'Maastricht',
        'owner_name': 'Gemeente Maastricht',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Zwolle',
        'url': 'https://data.zwolle.nl',
        'description': 'Open data portal of the municipality of Zwolle',
        'province': 'NL-OV',
        'city': 'Zwolle',
        'owner_name': 'Gemeente Zwolle',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Delft',
        'url': 'https://data.delft.nl',
        'description': 'Open data portal of the municipality of Delft',
        'province': 'NL-ZH',
        'city': 'Delft',
        'owner_name': 'Gemeente Delft',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Leeuwarden',
        'url': 'https://data.leeuwarden.nl',
        'description': 'Open data portal of the municipality of Leeuwarden',
        'province': 'NL-FR',
        'city': 'Leeuwarden',
        'owner_name': 'Gemeente Leeuwarden',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Alkmaar',
        'url': 'https://data.alkmaar.nl',
        'description': 'Open data portal of the municipality of Alkmaar',
        'province': 'NL-NH',
        'city': 'Alkmaar',
        'owner_name': 'Gemeente Alkmaar',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Venlo',
        'url': 'https://data.venlo.nl',
        'description': 'Open data portal of the municipality of Venlo',
        'province': 'NL-LI',
        'city': 'Venlo',
        'owner_name': 'Gemeente Venlo',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Heerlen',
        'url': 'https://data.heerlen.nl',
        'description': 'Open data portal of the municipality of Heerlen',
        'province': 'NL-LI',
        'city': 'Heerlen',
        'owner_name': 'Gemeente Heerlen',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Emmen',
        'url': 'https://data.emmen.nl',
        'description': 'Open data portal of the municipality of Emmen',
        'province': 'NL-DR',
        'city': 'Emmen',
        'owner_name': 'Gemeente Emmen',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
    {
        'name': 'Open Data Deventer',
        'url': 'https://data.deventer.nl',
        'description': 'Open data portal of the municipality of Deventer',
        'province': 'NL-OV',
        'city': 'Deventer',
        'owner_name': 'Gemeente Deventer',
        'owner_type': 'Local government',
        'catalog_type': 'Open data portal',
    },
]

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

def generate_id_from_url(url):
    """Generate a unique ID from the portal URL."""
    url = url.replace('https://', '').replace('http://', '').replace('www.', '')
    url = url.rstrip('/')
    id_str = re.sub(r'[^a-zA-Z0-9]', '', url)
    return id_str.lower()

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

def create_catalog_yaml(catalog_data):
    """Create a YAML entry for a catalog."""
    url = catalog_data['url']
    catalog_id = generate_id_from_url(url)
    province_code = catalog_data['province']
    province_name = PROVINCE_NAMES.get(province_code, province_code)
    
    yaml_data = {
        'access_mode': ['open'],
        'api': True,
        'api_status': 'uncertain',
        'catalog_type': catalog_data.get('catalog_type', 'Open data portal'),
        'content_types': ['dataset'],
        'coverage': [{
            'location': {
                'country': {
                    'id': 'NL',
                    'name': 'Netherlands'
                },
                'level': 30,
                'macroregion': {
                    'id': '155',
                    'name': 'Western Europe'
                },
                'subregion': {
                    'id': province_code,
                    'name': province_name
                }
            }
        }],
        'description': catalog_data.get('description', ''),
        'id': catalog_id,
        'langs': [{
            'id': 'NL',
            'name': 'Dutch'
        }],
        'link': url,
        'name': catalog_data['name'],
        'owner': {
            'link': url.replace('data.', 'www.').replace('/data', ''),
            'location': {
                'country': {
                    'id': 'NL',
                    'name': 'Netherlands'
                },
                'level': 30,
                'subregion': {
                    'id': province_code,
                    'name': province_name
                }
            },
            'name': catalog_data.get('owner_name', 'Unknown'),
            'type': catalog_data.get('owner_type', 'Local government')
        },
        'properties': {
            'has_doi': False,
            'transferable_location': True
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
        'tags': [
            'government',
            'open data',
            'Netherlands',
            catalog_data.get('city', ''),
            'municipality'
        ],
        'topics': [
            {
                'id': 'REGI',
                'name': 'Regions and cities',
                'type': 'eudatatheme'
            },
            {
                'id': 'GOVE',
                'name': 'Government and public sector',
                'type': 'eudatatheme'
            }
        ]
    }
    
    return yaml_data, catalog_id

def main():
    """Main function to process and create catalog entries."""
    base_dir = Path(__file__).parent.parent / 'data' / 'entities' / 'NL'
    
    created = []
    skipped = []
    errors = []
    
    for catalog_data in DUTCH_CATALOGS:
        url = catalog_data.get('url', '')
        if not url:
            skipped.append("Missing URL")
            continue
        
        # Check if catalog already exists
        if check_existing_catalog(base_dir, url):
            skipped.append(f"Catalog already exists: {url}")
            continue
        
        # Create YAML entry
        try:
            yaml_data, catalog_id = create_catalog_yaml(catalog_data)
        except Exception as e:
            errors.append(f"Error creating YAML for {url}: {e}")
            continue
        
        # Determine directory structure
        province_code = catalog_data.get('province')
        if province_code:
            catalog_dir = base_dir / province_code / 'opendata'
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
    
    if skipped:
        print(f"\nSkipped {len(skipped)} catalogs (already exist or missing data)")

if __name__ == '__main__':
    main()

