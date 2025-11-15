#!/usr/bin/env python3
"""
Analyze software mentioned in entities and scheduled directories
and find software not listed in software records.
"""

import os
import yaml
from pathlib import Path
from collections import defaultdict

# Directories
BASE_DIR = Path(__file__).parent
SOFTWARE_DIR = BASE_DIR / "data" / "software"
ENTITIES_DIR = BASE_DIR / "data" / "entities"
SCHEDULED_DIR = BASE_DIR / "data" / "scheduled"

def normalize_name(name):
    """Normalize software name for comparison (lowercase, remove extra spaces)."""
    if not name:
        return ''
    return ' '.join(name.lower().split())

def load_software_records():
    """Load all software records from the software directory."""
    software_records = {}
    software_ids = set()
    software_names = set()
    software_names_by_id = {}  # Map normalized name to ID
    
    for root, dirs, files in os.walk(SOFTWARE_DIR):
        for file in files:
            if file.endswith('.yaml') and not file.startswith('_'):
                filepath = os.path.join(root, file)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        record = yaml.safe_load(f)
                        if record and 'id' in record:
                            software_id = record['id']
                            software_name = record.get('name', '')
                            normalized_name = normalize_name(software_name)
                            software_records[software_id] = {
                                'id': software_id,
                                'name': software_name,
                                'normalized_name': normalized_name,
                                'file': filepath
                            }
                            software_ids.add(software_id)
                            software_names.add(normalized_name)
                            if normalized_name:
                                software_names_by_id[normalized_name] = software_id
                except Exception as e:
                    print(f"Error loading {filepath}: {e}")
    
    return software_records, software_ids, software_names, software_names_by_id

def extract_software_from_yaml_files(directory):
    """Extract software references from all YAML files in a directory."""
    software_refs = defaultdict(list)
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.yaml') or file.endswith('.yml'):
                filepath = os.path.join(root, file)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        record = yaml.safe_load(f)
                        if record and 'software' in record:
                            software = record['software']
                            if isinstance(software, dict):
                                software_id = software.get('id', '')
                                software_name = software.get('name', '')
                                if software_id or software_name:
                                    key = (software_id, software_name)
                                    software_refs[key].append({
                                        'file': filepath,
                                        'id': software_id,
                                        'name': software_name
                                    })
                except Exception as e:
                    print(f"Error reading {filepath}: {e}")
    
    return software_refs

def main():
    print("Loading software records from software directory...")
    software_records, software_ids, software_names, software_names_by_id = load_software_records()
    print(f"Found {len(software_records)} software records")
    
    print("\nExtracting software from entities...")
    entities_software = extract_software_from_yaml_files(ENTITIES_DIR)
    print(f"Found {len(entities_software)} unique software references in entities")
    
    print("\nExtracting software from scheduled...")
    scheduled_software = extract_software_from_yaml_files(SCHEDULED_DIR)
    print(f"Found {len(scheduled_software)} unique software references in scheduled")
    
    # Combine all software references
    all_software_refs = {}
    all_software_refs.update(entities_software)
    for key, refs in scheduled_software.items():
        if key in all_software_refs:
            all_software_refs[key].extend(refs)
        else:
            all_software_refs[key] = refs
    
    print(f"\nTotal unique software references: {len(all_software_refs)}")
    
    # Find missing software
    missing_by_id = []
    missing_by_name = []
    missing_both = []
    name_mismatches = []  # ID exists but name doesn't match
    
    for (software_id, software_name), refs in all_software_refs.items():
        normalized_ref_name = normalize_name(software_name)
        id_exists = software_id and software_id in software_ids
        name_exists = normalized_ref_name and normalized_ref_name in software_names
        
        # Check if name matches the ID's name
        name_matches_id = False
        if id_exists and software_id in software_records:
            record_name = software_records[software_id]['normalized_name']
            name_matches_id = normalized_ref_name == record_name
        
        if not id_exists and not name_exists:
            missing_both.append({
                'id': software_id,
                'name': software_name,
                'refs': refs
            })
        elif id_exists and not name_matches_id and normalized_ref_name:
            # ID exists but name doesn't match
            record_name = software_records[software_id].get('name', '') if id_exists else ''
            name_mismatches.append({
                'id': software_id,
                'name': software_name,
                'expected_name': record_name,
                'refs': refs
            })
        elif not id_exists and name_exists:
            # Name exists but ID doesn't - might be wrong ID
            correct_id = software_names_by_id.get(normalized_ref_name, '')
            missing_by_id.append({
                'id': software_id,
                'name': software_name,
                'suggested_id': correct_id,
                'refs': refs
            })
        elif id_exists and not normalized_ref_name:
            # ID exists but no name provided
            missing_by_name.append({
                'id': software_id,
                'name': software_name,
                'refs': refs
            })
    
    # Print results
    print("\n" + "="*80)
    print("MISSING SOFTWARE ANALYSIS")
    print("="*80)
    
    if missing_both:
        print(f"\n❌ Software missing by both ID and name ({len(missing_both)}):")
        for item in sorted(missing_both, key=lambda x: (x['id'] or '', x['name'] or '')):
            print(f"\n  ID: '{item['id']}' | Name: '{item['name']}'")
            print(f"  Referenced in {len(item['refs'])} file(s):")
            for ref in item['refs'][:5]:  # Show first 5
                rel_path = os.path.relpath(ref['file'], BASE_DIR)
                print(f"    - {rel_path}")
            if len(item['refs']) > 5:
                print(f"    ... and {len(item['refs']) - 5} more")
    
    if missing_by_id:
        print(f"\n⚠️  Software with missing ID but name exists ({len(missing_by_id)}):")
        for item in sorted(missing_by_id, key=lambda x: (x['id'] or '', x['name'] or '')):
            print(f"\n  ID: '{item['id']}' | Name: '{item['name']}'")
            print(f"  Referenced in {len(item['refs'])} file(s):")
            for ref in item['refs'][:3]:
                rel_path = os.path.relpath(ref['file'], BASE_DIR)
                print(f"    - {rel_path}")
            if len(item['refs']) > 3:
                print(f"    ... and {len(item['refs']) - 3} more")
    
    if name_mismatches:
        print(f"\n⚠️  Software with name mismatches (ID exists but name differs) ({len(name_mismatches)}):")
        for item in sorted(name_mismatches, key=lambda x: (x['id'] or '', x['name'] or '')):
            print(f"\n  ID: '{item['id']}' | Referenced name: '{item['name']}' | Expected name: '{item['expected_name']}'")
            print(f"  Referenced in {len(item['refs'])} file(s):")
            for ref in item['refs'][:3]:
                rel_path = os.path.relpath(ref['file'], BASE_DIR)
                print(f"    - {rel_path}")
            if len(item['refs']) > 3:
                print(f"    ... and {len(item['refs']) - 3} more")
    
    if missing_by_name:
        print(f"\n⚠️  Software with missing name but ID exists ({len(missing_by_name)}):")
        for item in sorted(missing_by_name, key=lambda x: (x['id'] or '', x['name'] or '')):
            print(f"\n  ID: '{item['id']}' | Name: '{item['name']}'")
            print(f"  Referenced in {len(item['refs'])} file(s):")
            for ref in item['refs'][:3]:
                rel_path = os.path.relpath(ref['file'], BASE_DIR)
                print(f"    - {rel_path}")
            if len(item['refs']) > 3:
                print(f"    ... and {len(item['refs']) - 3} more")
    
    if not missing_both and not missing_by_id and not missing_by_name and not name_mismatches:
        print("\n✅ All software references are found in software records!")
    
    # Summary statistics
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Software records: {len(software_records)}")
    print(f"Unique software in entities: {len(entities_software)}")
    print(f"Unique software in scheduled: {len(scheduled_software)}")
    print(f"Total unique software references: {len(all_software_refs)}")
    print(f"Missing software (both ID and name): {len(missing_both)}")
    print(f"Missing software (ID only): {len(missing_by_id)}")
    print(f"Missing software (name only): {len(missing_by_name)}")
    print(f"Name mismatches (ID exists, name differs): {len(name_mismatches)}")

if __name__ == "__main__":
    main()

