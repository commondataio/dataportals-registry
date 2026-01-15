#!/usr/bin/env python
"""
Script to remove INCOMPLETE_IDENTIFIER issues by parsing the report file
and removing incomplete identifiers from YAML files.
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
REPORT_FILE = os.path.join(_REPO_ROOT, "dataquality", "rules", "INCOMPLETE_IDENTIFIER.txt")
ENTITIES_DIR = os.path.join(_REPO_ROOT, "data", "entities")


def parse_report_file(report_path: str) -> List[Tuple[str, int]]:
    """
    Parse the report file and extract file paths and identifier indices.
    Returns list of tuples: (file_path, identifier_index)
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
            identifier_index = None
            while j < len(lines) and j < i + 10:  # Look up to 10 lines ahead
                field_line = lines[j].strip()
                
                if field_line.startswith("Field: "):
                    # Extract index from identifiers[0], identifiers[1], etc.
                    field_match = re.match(r'identifiers\[(\d+)\]', field_line.replace("Field: ", ""))
                    if field_match:
                        identifier_index = int(field_match.group(1))
                        break
                
                # Stop if we hit the next file entry
                if field_line.startswith("File: "):
                    break
                
                j += 1
            
            if identifier_index is not None:
                issues.append((file_path, identifier_index))
        
        i += 1
    
    return issues


def remove_identifier(file_path: str, identifier_index: int) -> bool:
    """
    Remove an incomplete identifier from a YAML file.
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
        
        identifiers = data.get('identifiers', [])
        if not isinstance(identifiers, list):
            print(f"  ⚠ Identifiers is not a list, skipping")
            return False
        
        if identifier_index >= len(identifiers):
            print(f"  ⚠ Identifier index {identifier_index} out of range (identifiers has {len(identifiers)} items)")
            return False
        
        identifier_to_remove = identifiers[identifier_index]
        print(f"  ✓ Removing identifier[{identifier_index}]: {identifier_to_remove}")
        
        # Remove the identifier
        identifiers.pop(identifier_index)
        
        # If identifiers list is now empty, remove the field entirely
        if len(identifiers) == 0:
            if 'identifiers' in data:
                del data['identifiers']
                print(f"  ✓ Removed empty identifiers field")
        else:
            data['identifiers'] = identifiers
        
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
    """Main function to remove all INCOMPLETE_IDENTIFIER issues."""
    print("Parsing report file...")
    issues = parse_report_file(REPORT_FILE)
    
    print(f"\nFound {len(issues)} issues to fix")
    print(f"Report file: {REPORT_FILE}")
    print(f"Entities directory: {ENTITIES_DIR}\n")
    
    fixed_count = 0
    skipped_count = 0
    error_count = 0
    
    # Group issues by file and process in reverse order to avoid index shifting
    file_issues = {}
    for file_path, identifier_index in issues:
        if file_path not in file_issues:
            file_issues[file_path] = []
        file_issues[file_path].append(identifier_index)
    
    # Sort indices in reverse order for each file
    for file_path in file_issues:
        file_issues[file_path].sort(reverse=True)
    
    total_processed = 0
    for file_path, identifier_indices in file_issues.items():
        print(f"[{total_processed + 1}/{len(file_issues)}] {file_path}")
        for identifier_index in identifier_indices:
            print(f"  Identifier[{identifier_index}]")
            if remove_identifier(file_path, identifier_index):
                fixed_count += 1
            else:
                skipped_count += 1
        total_processed += 1
    
    print(f"\n=== Summary ===")
    print(f"Total issues: {len(issues)}")
    print(f"Files processed: {len(file_issues)}")
    print(f"Fixed: {fixed_count}")
    print(f"Skipped/No change: {skipped_count}")
    print(f"Errors: {error_count}")


if __name__ == "__main__":
    main()
