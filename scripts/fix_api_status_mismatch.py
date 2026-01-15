#!/usr/bin/env python
"""
Script to fix API_STATUS_MISMATCH issues by parsing the report file
and updating the YAML files accordingly.
"""

import os
import re
import yaml
from pathlib import Path
from typing import Dict, List, Tuple

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

# Get script directory and repository root
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)
REPORT_FILE = os.path.join(_REPO_ROOT, "dataquality", "rules", "API_STATUS_MISMATCH.txt")
ENTITIES_DIR = os.path.join(_REPO_ROOT, "data", "entities")


def parse_report_file(report_path: str) -> List[Tuple[str, str]]:
    """
    Parse the report file and extract file paths and field types.
    Returns list of tuples: (file_path, field_type)
    """
    issues = []
    
    with open(report_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Find where issues start
    issues_start = None
    for i, line in enumerate(lines):
        if line.strip() == "=== ISSUES ===":
            issues_start = i + 1
            break
    
    if issues_start is None:
        print("Could not find '=== ISSUES ===' section")
        return issues
    
    i = issues_start
    while i < len(lines):
        line = lines[i].strip()
        
        # Look for "File: " line
        if line.startswith("File: "):
            file_path = line.replace("File: ", "").strip()
            
            # Look ahead for Field line
            j = i + 1
            field_type = None
            while j < len(lines) and j < i + 10:  # Look up to 10 lines ahead
                field_line = lines[j].strip()
                if field_line.startswith("Field: "):
                    field_type = field_line.replace("Field: ", "").strip()
                    break
                j += 1
            
            if field_type:
                issues.append((file_path, field_type))
        
        i += 1
    
    return issues


def fix_yaml_file(file_path: str, field_type: str) -> bool:
    """
    Fix a single YAML file based on the field type.
    Returns True if file was modified, False otherwise.
    """
    full_path = os.path.join(ENTITIES_DIR, file_path)
    
    if not os.path.exists(full_path):
        print(f"  ✗ File not found: {full_path}")
        return False
    
    try:
        # Read YAML file
        with open(full_path, 'r', encoding='utf-8') as f:
            data = yaml.load(f, Loader=Loader)
        
        modified = False
        
        # Check if endpoints exist
        endpoints = data.get('endpoints', [])
        if not endpoints:
            print(f"  ⚠ No endpoints found, skipping")
            return False
        
        if field_type == "api_status":
            # Fix: change api_status from 'uncertain' or 'inactive' to 'active'
            current_status = data.get('api_status')
            if current_status in ['uncertain', 'inactive']:
                data['api_status'] = 'active'
                modified = True
                print(f"  ✓ Updated api_status: {current_status} -> active")
            elif current_status == 'active':
                print(f"  ⊙ Already active, no change needed")
            else:
                print(f"  ⚠ Unexpected api_status value: {current_status}")
        
        elif field_type == "api":
            # Fix: set api=True and api_status='active'
            current_api = data.get('api', False)
            if not current_api:
                data['api'] = True
                modified = True
                print(f"  ✓ Set api: False -> True")
            
            # Also ensure api_status is set to 'active'
            current_status = data.get('api_status')
            if current_status in [None, 'uncertain', 'inactive']:
                data['api_status'] = 'active'
                modified = True
                print(f"  ✓ Set api_status: {current_status} -> active")
            elif current_status == 'active':
                if modified:
                    print(f"  ✓ api_status already active")
            else:
                print(f"  ⚠ Unexpected api_status value: {current_status}")
        
        if modified:
            # Write YAML file back
            with open(full_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
            return True
        
        return False
    
    except Exception as e:
        print(f"  ✗ Error processing file: {e}")
        return False


def main():
    """Main function to fix all API_STATUS_MISMATCH issues."""
    print("Parsing report file...")
    issues = parse_report_file(REPORT_FILE)
    
    print(f"\nFound {len(issues)} issues to fix")
    print(f"Report file: {REPORT_FILE}")
    print(f"Entities directory: {ENTITIES_DIR}\n")
    
    fixed_count = 0
    skipped_count = 0
    error_count = 0
    
    for i, (file_path, field_type) in enumerate(issues, 1):
        print(f"[{i}/{len(issues)}] {file_path}")
        print(f"  Field: {field_type}")
        
        if fix_yaml_file(file_path, field_type):
            fixed_count += 1
        else:
            skipped_count += 1
    
    print(f"\n=== Summary ===")
    print(f"Total issues: {len(issues)}")
    print(f"Fixed: {fixed_count}")
    print(f"Skipped/No change: {skipped_count}")
    print(f"Errors: {error_count}")


if __name__ == "__main__":
    main()
