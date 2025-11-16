#!/usr/bin/env python3
"""
Script to add Indian state sub-portals from data.gov.in to the registry.
Fetches state portals from the API and creates YAML entries for each.
"""

import json
import requests
import yaml
import re
from pathlib import Path
from urllib.parse import urlparse

# ISO 3166-2 mapping for Indian states
# Mapping state names (various formats) to ISO 3166-2 codes
STATE_CODE_MAP = {
    'andhra pradesh': 'IN-AP',
    'arunachal pradesh': 'IN-AR',
    'assam': 'IN-AS',
    'bihar': 'IN-BR',
    'chhattisgarh': 'IN-CT',
    'goa': 'IN-GA',
    'gujarat': 'IN-GJ',
    'haryana': 'IN-HR',
    'himachal pradesh': 'IN-HP',
    'jammu and kashmir': 'IN-JK',
    'jammu & kashmir': 'IN-JK',
    'jharkhand': 'IN-JH',
    'karnataka': 'IN-KA',
    'kerala': 'IN-KL',
    'madhya pradesh': 'IN-MP',
    'maharashtra': 'IN-MH',
    'manipur': 'IN-MN',
    'meghalaya': 'IN-ML',
    'mizoram': 'IN-MZ',
    'nagaland': 'IN-NL',
    'odisha': 'IN-OR',
    'orissa': 'IN-OR',
    'punjab': 'IN-PB',
    'rajasthan': 'IN-RJ',
    'sikkim': 'IN-SK',
    'tamil nadu': 'IN-TN',
    'telangana': 'IN-TG',
    'tripura': 'IN-TR',
    'uttar pradesh': 'IN-UP',
    'uttarakhand': 'IN-UT',
    'west bengal': 'IN-WB',
    'delhi': 'IN-DL',
    'puducherry': 'IN-PY',
    'puducherry (pondicherry)': 'IN-PY',
    'chandigarh': 'IN-CH',
    'dadra and nagar haveli and daman and diu': 'IN-DH',
    'dadra & nagar haveli & daman & diu': 'IN-DH',
    'lakshadweep': 'IN-LD',
    'ladakh': 'IN-LA',
    'andaman and nicobar islands': 'IN-AN',
    'andaman & nicobar islands': 'IN-AN',
}

def normalize_state_name(name):
    """Normalize state name for lookup."""
    return name.lower().strip()

def get_state_code(state_name):
    """Get ISO 3166-2 code for a state name."""
    normalized = normalize_state_name(state_name)
    return STATE_CODE_MAP.get(normalized)

def generate_id_from_url(url):
    """Generate a unique ID from the portal URL."""
    # Remove protocol and www
    url = url.replace('https://', '').replace('http://', '').replace('www.', '')
    # Remove trailing slash
    url = url.rstrip('/')
    # Replace dots and special chars with nothing, keep only alphanumeric
    id_str = re.sub(r'[^a-zA-Z0-9]', '', url)
    return id_str.lower()

def get_known_state_portals():
    """Get a list of known state portals based on common patterns."""
    # Common state portal URL patterns
    states = [
        {'name': 'Odisha', 'url': 'https://odisha.data.gov.in'},
        {'name': 'Jammu and Kashmir', 'url': 'https://jk.data.gov.in'},
        {'name': 'Andhra Pradesh', 'url': 'https://ap.data.gov.in'},
        {'name': 'Arunachal Pradesh', 'url': 'https://ar.data.gov.in'},
        {'name': 'Assam', 'url': 'https://assam.data.gov.in'},
        {'name': 'Bihar', 'url': 'https://bihar.data.gov.in'},
        {'name': 'Chhattisgarh', 'url': 'https://cg.data.gov.in'},
        {'name': 'Goa', 'url': 'https://goa.data.gov.in'},
        {'name': 'Gujarat', 'url': 'https://gujarat.data.gov.in'},
        {'name': 'Haryana', 'url': 'https://haryana.data.gov.in'},
        {'name': 'Himachal Pradesh', 'url': 'https://hp.data.gov.in'},
        {'name': 'Jharkhand', 'url': 'https://jharkhand.data.gov.in'},
        {'name': 'Karnataka', 'url': 'https://karnataka.data.gov.in'},
        {'name': 'Kerala', 'url': 'https://kerala.data.gov.in'},
        {'name': 'Madhya Pradesh', 'url': 'https://mp.data.gov.in'},
        {'name': 'Maharashtra', 'url': 'https://maharashtra.data.gov.in'},
        {'name': 'Manipur', 'url': 'https://manipur.data.gov.in'},
        {'name': 'Meghalaya', 'url': 'https://meghalaya.data.gov.in'},
        {'name': 'Mizoram', 'url': 'https://mizoram.data.gov.in'},
        {'name': 'Nagaland', 'url': 'https://nagaland.data.gov.in'},
        {'name': 'Punjab', 'url': 'https://punjab.data.gov.in'},
        {'name': 'Rajasthan', 'url': 'https://rajasthan.data.gov.in'},
        {'name': 'Sikkim', 'url': 'https://sikkim.data.gov.in'},
        {'name': 'Tamil Nadu', 'url': 'https://tn.data.gov.in'},
        {'name': 'Telangana', 'url': 'https://telangana.data.gov.in'},
        {'name': 'Tripura', 'url': 'https://tripura.data.gov.in'},
        {'name': 'Uttar Pradesh', 'url': 'https://up.data.gov.in'},
        {'name': 'Uttarakhand', 'url': 'https://uk.data.gov.in'},
        {'name': 'West Bengal', 'url': 'https://wb.data.gov.in'},
        {'name': 'Delhi', 'url': 'https://delhi.data.gov.in'},
        {'name': 'Puducherry', 'url': 'https://puducherry.data.gov.in'},
        {'name': 'Chandigarh', 'url': 'https://chandigarh.data.gov.in'},
        {'name': 'Dadra and Nagar Haveli and Daman and Diu', 'url': 'https://dnhdd.data.gov.in'},
        {'name': 'Ladakh', 'url': 'https://ladakh.data.gov.in'},
        {'name': 'Lakshadweep', 'url': 'https://lakshadweep.data.gov.in'},
        {'name': 'Andaman and Nicobar Islands', 'url': 'https://an.data.gov.in'},
    ]
    return states

def verify_portal_exists(url):
    """Verify if a portal URL exists and is accessible."""
    try:
        response = requests.head(url, timeout=10, allow_redirects=True)
        if response.status_code == 200:
            return True
        # Also try GET for some servers that don't respond to HEAD
        response = requests.get(url, timeout=10, allow_redirects=True)
        return response.status_code == 200
    except:
        return False

def fetch_state_portals():
    """Fetch state portals - try API first, fallback to known list."""
    # Try API first
    url = "https://data.gov.in/backend/cmspublic/v1/states"
    params = {
        'filters[field_state_visibility]': 'true',
        'sort_by': 'created',
        'sort_order': 'DESC',
        'limit': 100
    }
    
    try:
        response = requests.get(url, params=params, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        if response.status_code == 200:
            data = response.json()
            if data.get('data'):
                return data
    except Exception as e:
        print(f"API fetch failed: {e}, using known list instead")
    
    # Fallback to known list
    known_states = get_known_state_portals()
    # Convert to API-like format
    return {
        'data': [
            {
                'field_state_name': state['name'],
                'field_state_url': state['url']
            }
            for state in known_states
        ]
    }

def create_portal_yaml(state_data, state_code):
    """Create a YAML entry for a state portal."""
    state_name = state_data.get('field_state_name', '')
    portal_url = state_data.get('field_state_url', '')
    
    if not portal_url:
        return None
    
    # Generate ID from URL
    portal_id = generate_id_from_url(portal_url)
    
    # Create YAML structure
    yaml_data = {
        'access_mode': ['open'],
        'api_status': 'uncertain',
        'catalog_type': 'Open data portal',
        'content_types': ['dataset'],
        'coverage': [{
            'location': {
                'country': {
                    'id': 'IN',
                    'name': 'India'
                },
                'level': 30,
                'macroregion': {
                    'id': '034',
                    'name': 'Southern Asia'
                },
                'subregion': {
                    'id': state_code,
                    'name': state_name
                }
            }
        }],
        'description': f'Open Government Data portal for {state_name} state, part of the data.gov.in network.',
        'id': portal_id,
        'langs': [{
            'id': 'EN',
            'name': 'English'
        }],
        'link': portal_url,
        'name': f'{state_name} Open Data Portal',
        'owner': {
            'location': {
                'country': {
                    'id': 'IN',
                    'name': 'India'
                },
                'level': 30,
                'subregion': {
                    'id': state_code,
                    'name': state_name
                }
            },
            'name': f'Government of {state_name}',
            'type': 'Regional government'
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
            'id': 'custom',
            'name': 'Custom software'
        },
        'status': 'active',
        'tags': [
            'government',
            'open data',
            'government data',
            'data catalog',
            state_name
        ],
        'topics': [
            {
                'id': 'GOVE',
                'name': 'Government and public sector',
                'type': 'eudatatheme'
            },
            {
                'id': 'REGI',
                'name': 'Regions and cities',
                'type': 'eudatatheme'
            },
            {
                'id': 'Location',
                'name': 'Location',
                'type': 'iso19115'
            }
        ]
    }
    
    return yaml_data, portal_id

def check_existing_portal(base_dir, portal_url):
    """Check if a portal URL already exists in the registry."""
    base_dir = Path(base_dir)
    if not base_dir.exists():
        return False
    
    normalized_url = portal_url.lower().rstrip('/')
    
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
    """Main function to process and create portal entries."""
    print("Fetching state portals from data.gov.in API...")
    data = fetch_state_portals()
    
    if not data:
        print("Failed to fetch data from API")
        return
    
    # Extract states from response
    states = data.get('data', [])
    if not states:
        print("No states found in API response")
        print(f"Response: {json.dumps(data, indent=2)}")
        return
    
    print(f"Found {len(states)} states")
    
    # Base directory for entities
    base_dir = Path(__file__).parent.parent / 'data' / 'entities' / 'IN'
    
    created = []
    skipped = []
    errors = []
    
    for state_data in states:
        state_name = state_data.get('field_state_name', '')
        portal_url = state_data.get('field_state_url', '')
        
        if not state_name or not portal_url:
            skipped.append(f"Missing data: {state_name or 'Unknown'}")
            continue
        
        # Check if portal already exists in registry
        if check_existing_portal(base_dir, portal_url):
            skipped.append(f"Portal already exists: {portal_url}")
            continue
        
        # Verify portal exists (optional, can be slow)
        # if not verify_portal_exists(portal_url):
        #     skipped.append(f"Portal not accessible: {portal_url}")
        #     continue
        
        # Get ISO 3166-2 code
        state_code = get_state_code(state_name)
        if not state_code:
            errors.append(f"Could not map state '{state_name}' to ISO 3166-2 code")
            continue
        
        # Create YAML entry
        result = create_portal_yaml(state_data, state_code)
        if not result:
            skipped.append(f"Could not create entry for {state_name}")
            continue
        
        yaml_data, portal_id = result
        
        # Determine directory structure: IN/{STATE_CODE}/opendata/
        state_dir = base_dir / state_code / 'opendata'
        state_dir.mkdir(parents=True, exist_ok=True)
        
        # Check if file already exists
        yaml_file = state_dir / f"{portal_id}.yaml"
        if yaml_file.exists():
            skipped.append(f"File already exists: {yaml_file}")
            continue
        
        # Write YAML file
        try:
            with open(yaml_file, 'w', encoding='utf-8') as f:
                yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
            created.append(f"{state_name} ({state_code}): {yaml_file}")
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
        print("\nSkipped:")
        for item in skipped:
            print(f"  - {item}")
    
    if errors:
        print("\nErrors:")
        for item in errors:
            print(f"  - {item}")

if __name__ == '__main__':
    main()

