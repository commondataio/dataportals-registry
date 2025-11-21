#!/usr/bin/env python3
"""
Script to move US-based files from scheduled/Unknown/geo to scheduled/US/geo
and update their metadata.
"""

import yaml
from pathlib import Path
import re
from urllib.parse import urlparse
import sys
import os

# Add scripts directory to path to import update functions
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from update_unknown_owners_us_geo import (
    extract_domain,
    update_record_metadata,
    parse_domain_for_owner
)

def is_us_domain(domain):
    """Check if domain appears to be US-based."""
    if not domain:
        return False
    domain_lower = domain.lower()
    
    # Check for .gov, .us, .mil domains (definitely US)
    if '.gov' in domain_lower or '.us' in domain_lower or '.mil' in domain_lower:
        return True
    
    # Check for common US TLDs with US state patterns
    if domain_lower.endswith('.org') or domain_lower.endswith('.com') or domain_lower.endswith('.net'):
        # Check for US state abbreviations
        us_states = ['al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'fl', 'ga', 'hi', 'id', 
                     'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi', 'mn', 'ms', 
                     'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 
                     'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 
                     'wi', 'wy', 'dc']
        for state in us_states:
            if f'.{state}.' in domain_lower or domain_lower.endswith(f'.{state}') or f'{state}.' in domain_lower or f'{state}' in domain_lower.split('.')[0]:
                return True
        
        # Check for state names
        state_names = ['alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 
                      'connecticut', 'delaware', 'florida', 'georgia', 'hawaii', 'idaho', 
                      'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana', 'maine', 
                      'maryland', 'massachusetts', 'michigan', 'minnesota', 'mississippi', 
                      'missouri', 'montana', 'nebraska', 'nevada', 'newhampshire', 'newjersey', 
                      'newmexico', 'newyork', 'northcarolina', 'northdakota', 'ohio', 
                      'oklahoma', 'oregon', 'pennsylvania', 'rhodeisland', 'southcarolina', 
                      'southdakota', 'tennessee', 'texas', 'utah', 'vermont', 'virginia', 
                      'washington', 'westvirginia', 'wisconsin', 'wyoming']
        for state_name in state_names:
            if state_name in domain_lower:
                return True
        
        # Check for common US city/county patterns
        us_patterns = ['county', 'city', 'state', 'town', 'municipal', 'parish']
        for pattern in us_patterns:
            if pattern in domain_lower:
                # Additional check: if it has these patterns and ends with .org/.com/.net, likely US
                return True
    
    return False

def main():
    unknown_dir = Path('data/scheduled/Unknown/geo')
    us_dir = Path('data/scheduled/US/geo')
    us_dir.mkdir(parents=True, exist_ok=True)
    
    moved_count = 0
    updated_count = 0
    error_count = 0
    
    print(f"Scanning {unknown_dir} for US-based files with Unknown owners...")
    
    for yaml_file in sorted(unknown_dir.glob('*.yaml')):
        try:
            # Read file
            with open(yaml_file, 'r', encoding='utf-8') as f:
                record = yaml.safe_load(f)
            
            if not record:
                continue
            
            # Check if owner.name is Unknown
            if record.get('owner', {}).get('name') != 'Unknown':
                continue
            
            # Extract domain
            link = record.get('link', '')
            domain = extract_domain(link) if link else ''
            
            # Also check endpoints
            if not domain and 'endpoints' in record:
                for endpoint in record['endpoints']:
                    endpoint_url = endpoint.get('url', '')
                    if endpoint_url:
                        domain = extract_domain(endpoint_url)
                        if domain:
                            break
            
            # Check if US-based
            if not is_us_domain(domain):
                continue
            
            # Move file to US/geo
            dest_file = us_dir / yaml_file.name
            if dest_file.exists():
                print(f"  Warning: {yaml_file.name} already exists in destination, skipping move")
                continue
            
            # Update metadata before moving
            changed = update_record_metadata(record, domain)
            
            # Write updated record to destination
            with open(dest_file, 'w', encoding='utf-8') as f:
                yaml.safe_dump(record, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
            
            # Remove source file
            yaml_file.unlink()
            
            moved_count += 1
            if changed:
                updated_count += 1
                owner_name = record.get('owner', {}).get('name', 'Unknown')
                print(f"  ✓ Moved and updated: {yaml_file.name} -> {owner_name}")
            else:
                print(f"  ✓ Moved: {yaml_file.name}")
        
        except Exception as e:
            error_count += 1
            print(f"  ✗ Error processing {yaml_file.name}: {e}")
    
    print(f"\nSummary:")
    print(f"  Files moved: {moved_count}")
    print(f"  Files updated: {updated_count}")
    print(f"  Errors: {error_count}")

if __name__ == "__main__":
    main()
