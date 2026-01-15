#!/usr/bin/env python
"""
Script to clear license info for records with INCONSISTENT_LICENSE issues
by parsing the report file and emptying license fields in YAML files.
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
REPORT_FILE = os.path.join(_REPO_ROOT, "dataquality", "rules", "INCONSISTENT_LICENSE.txt")
ENTITIES_DIR = os.path.join(_REPO_ROOT, "data", "entities")


def parse_report_file(report_path: str) -> List[str]:
    """
    Parse the report file and extract file paths.
    Returns list of file paths.
    """
    files = []
    
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
        return files
    
    i = issues_start
    while i < len(lines):
        line = lines[i].strip()
        
        # Look for "File: " line
        if line.startswith("File: "):
            file_path = line.replace("File: ", "").strip()
            if file_path not in files:
                files.append(file_path)
        
        i += 1
    
    return files


def clear_license_info(file_path: str) -> bool:
    """
    Clear license info (license_id, license_name, license_url) from a YAML file.
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
        
        rights = data.get('rights', {})
        if not isinstance(rights, dict):
            print(f"  ⚠ Rights is not a dict, skipping")
            return False
        
        # Check if there's any license info to clear
        license_id = rights.get('license_id')
        license_name = rights.get('license_name')
        license_url = rights.get('license_url')
        
        if license_id is None and license_name is None and license_url is None:
            print(f"  ⊙ License info already empty, no change needed")
            return False
        
        # Clear license fields
        rights['license_id'] = None
        rights['license_name'] = None
        rights['license_url'] = None
        
        data['rights'] = rights
        
        print(f"  ✓ Cleared license info (was: license_id={license_id}, license_name={license_name}, license_url={license_url})")
        
        # Write YAML file back
        with open(full_path, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
        
        return True
    
    except Exception as e:
        print(f"  ✗ Error processing file: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main function to clear license info for all INCONSISTENT_LICENSE issues."""
    print("Parsing report file...")
    files = parse_report_file(REPORT_FILE)
    
    print(f"\nFound {len(files)} files with inconsistent license info")
    print(f"Report file: {REPORT_FILE}")
    print(f"Entities directory: {ENTITIES_DIR}\n")
    
    fixed_count = 0
    skipped_count = 0
    error_count = 0
    
    for i, file_path in enumerate(files, 1):
        print(f"[{i}/{len(files)}] {file_path}")
        if clear_license_info(file_path):
            fixed_count += 1
        else:
            skipped_count += 1
    
    print(f"\n=== Summary ===")
    print(f"Total files: {len(files)}")
    print(f"Fixed: {fixed_count}")
    print(f"Skipped/No change: {skipped_count}")
    print(f"Errors: {error_count}")


if __name__ == "__main__":
    main()
