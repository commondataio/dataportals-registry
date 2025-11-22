#!/usr/bin/env python3
"""
Check consistency of software.id across all entity files.

This script checks:
1. All entities have software.id field
2. All software.id values match valid software definitions
3. software.id matches the expected name from software definition files
"""

import os
import yaml
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple

# Base directories
BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
SOFTWARE_DIR = BASE_DIR / "data" / "software"


def load_software_definitions() -> Dict[str, Dict]:
    """Load all software definitions from data/software directory."""
    software = {}
    
    for software_file in SOFTWARE_DIR.rglob("*.yaml"):
        if software_file.name == "_template.tmpl":
            continue
            
        try:
            with open(software_file, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                if data and "id" in data:
                    software[data["id"]] = {
                        "id": data["id"],
                        "name": data.get("name", ""),
                        "file": str(software_file.relative_to(BASE_DIR))
                    }
        except Exception as e:
            print(f"Error loading {software_file}: {e}")
    
    return software


def analyze_entities(software_defs: Dict[str, Dict]) -> Tuple[List, List, List]:
    """Analyze all entity files for software.id consistency."""
    missing_id = []
    invalid_id = []
    name_mismatch = []
    
    entity_files = list(ENTITIES_DIR.rglob("*.yaml"))
    print(f"Analyzing {len(entity_files)} entity files...")
    
    for entity_file in entity_files:
        try:
            with open(entity_file, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                
            if not data:
                continue
                
            if "software" not in data:
                missing_id.append((str(entity_file.relative_to(BASE_DIR)), "missing software field"))
                continue
                
            software_data = data["software"]
            
            # Check if id field exists
            if "id" not in software_data:
                missing_id.append((str(entity_file.relative_to(BASE_DIR)), "missing software.id"))
                continue
            
            software_id = software_data["id"]
            software_name = software_data.get("name", "")
            
            # Check if id is valid (exists in software definitions)
            if software_id not in software_defs:
                invalid_id.append((
                    str(entity_file.relative_to(BASE_DIR)),
                    software_id,
                    software_name
                ))
                continue
            
            # Check if name matches expected name from software definition
            expected_name = software_defs[software_id]["name"]
            if software_name != expected_name:
                name_mismatch.append((
                    str(entity_file.relative_to(BASE_DIR)),
                    software_id,
                    software_name,
                    expected_name
                ))
                
        except Exception as e:
            print(f"Error processing {entity_file}: {e}")
    
    return missing_id, invalid_id, name_mismatch


def main():
    print("Loading software definitions...")
    software_defs = load_software_definitions()
    print(f"Loaded {len(software_defs)} software definitions")
    
    print("\nAnalyzing entities...")
    missing_id, invalid_id, name_mismatch = analyze_entities(software_defs)
    
    # Report results
    print("\n" + "="*80)
    print("ANALYSIS RESULTS")
    print("="*80)
    
    print(f"\n1. Missing software.id: {len(missing_id)}")
    if missing_id:
        print("   Files with missing software.id:")
        for filepath, reason in missing_id[:20]:  # Show first 20
            print(f"     - {filepath} ({reason})")
        if len(missing_id) > 20:
            print(f"     ... and {len(missing_id) - 20} more")
    
    print(f"\n2. Invalid software.id: {len(invalid_id)}")
    if invalid_id:
        print("   Files with invalid software.id (not found in software definitions):")
        invalid_by_id = defaultdict(list)
        for filepath, sid, sname in invalid_id:
            invalid_by_id[sid].append((filepath, sname))
        
        for sid, files in sorted(invalid_by_id.items()):
            print(f"     - '{sid}' used in {len(files)} file(s):")
            for filepath, sname in files[:5]:  # Show first 5 per ID
                print(f"       {filepath} (name: {sname})")
            if len(files) > 5:
                print(f"       ... and {len(files) - 5} more")
    
    print(f"\n3. Name mismatches: {len(name_mismatch)}")
    if name_mismatch:
        print("   Files where software.name doesn't match software definition:")
        for filepath, sid, actual_name, expected_name in name_mismatch[:20]:  # Show first 20
            print(f"     - {filepath}")
            print(f"       ID: {sid}")
            print(f"       Actual name: '{actual_name}'")
            print(f"       Expected name: '{expected_name}'")
        if len(name_mismatch) > 20:
            print(f"     ... and {len(name_mismatch) - 20} more")
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    total_issues = len(missing_id) + len(invalid_id) + len(name_mismatch)
    if total_issues == 0:
        print("✓ All software.id fields are consistent!")
    else:
        print(f"✗ Found {total_issues} total issues:")
        print(f"  - {len(missing_id)} missing software.id")
        print(f"  - {len(invalid_id)} invalid software.id")
        print(f"  - {len(name_mismatch)} name mismatches")
    
    return total_issues


if __name__ == "__main__":
    exit(main())

