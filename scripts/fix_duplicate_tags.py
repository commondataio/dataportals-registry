#!/usr/bin/env python
"""
Script to fix DUPLICATE_TAGS issues by parsing the report file
and removing duplicate tags from YAML files.
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
REPORT_FILE = os.path.join(_REPO_ROOT, "dataquality", "rules", "DUPLICATE_TAGS.txt")
ENTITIES_DIR = os.path.join(_REPO_ROOT, "data", "entities")


def parse_report_file(report_path: str) -> List[Tuple[str, int, str]]:
    """
    Parse the report file and extract file paths, tag indices, and current values.
    Returns list of tuples: (file_path, tag_index, current_value)
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
            
            # Look ahead for Field and Current Value lines
            j = i + 1
            tag_index = None
            current_value = None
            while j < len(lines) and j < i + 10:  # Look up to 10 lines ahead
                field_line = lines[j].strip()
                value_line = lines[j + 1].strip() if j + 1 < len(lines) else ""
                
                if field_line.startswith("Field: "):
                    # Extract index from tags[0], tags[1], etc.
                    field_match = re.match(r'tags\[(\d+)\]', field_line.replace("Field: ", ""))
                    if field_match:
                        tag_index = int(field_match.group(1))
                
                if value_line.startswith("Current Value: "):
                    current_value = value_line.replace("Current Value: ", "").strip()
                    break
                
                j += 1
            
            if tag_index is not None and current_value is not None:
                issues.append((file_path, tag_index, current_value))
        
        i += 1
    
    return issues


def fix_yaml_file(file_path: str, tag_index: int, current_value: str) -> bool:
    """
    Fix a single YAML file by removing the duplicate tag at the specified index.
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
        
        tags = data.get('tags', [])
        if not isinstance(tags, list):
            print(f"  ⚠ Tags is not a list, skipping")
            return False
        
        if tag_index >= len(tags):
            print(f"  ⚠ Tag index {tag_index} out of range (tags has {len(tags)} items)")
            return False
        
        # Verify this is actually a duplicate by checking if the same tag (case-insensitive) appears earlier
        tag_to_remove = tags[tag_index]
        tag_lower = tag_to_remove.lower().strip() if isinstance(tag_to_remove, str) else None
        
        if not isinstance(tag_to_remove, str):
            print(f"  ⚠ Tag[{tag_index}] is not a string: {tag_to_remove}")
            return False
        
        # Check for earlier occurrence (case-insensitive)
        found_earlier = False
        for i in range(tag_index):
            if isinstance(tags[i], str) and tags[i].lower().strip() == tag_lower:
                found_earlier = True
                print(f"  ✓ Found duplicate: tag[{i}]='{tags[i]}' and tag[{tag_index}]='{tag_to_remove}'")
                break
        
        if not found_earlier:
            print(f"  ⚠ Tag[{tag_index}]='{tag_to_remove}' doesn't appear to be a duplicate (no earlier occurrence found)")
            # Still remove it as the report says it's a duplicate
            print(f"  ✓ Removing tag[{tag_index}]='{tag_to_remove}' anyway (as per report)")
        
        # Remove the duplicate tag
        tags.pop(tag_index)
        data['tags'] = tags
        
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
    """Main function to fix all DUPLICATE_TAGS issues."""
    print("Parsing report file...")
    issues = parse_report_file(REPORT_FILE)
    
    print(f"\nFound {len(issues)} issues to fix")
    print(f"Report file: {REPORT_FILE}")
    print(f"Entities directory: {ENTITIES_DIR}\n")
    
    fixed_count = 0
    skipped_count = 0
    error_count = 0
    
    for i, (file_path, tag_index, current_value) in enumerate(issues, 1):
        print(f"[{i}/{len(issues)}] {file_path}")
        print(f"  Tag[{tag_index}]: '{current_value}'")
        if fix_yaml_file(file_path, tag_index, current_value):
            fixed_count += 1
            print(f"  ✓ Removed duplicate tag")
        else:
            skipped_count += 1
    
    print(f"\n=== Summary ===")
    print(f"Total issues: {len(issues)}")
    print(f"Fixed: {fixed_count}")
    print(f"Skipped/No change: {skipped_count}")
    print(f"Errors: {error_count}")


if __name__ == "__main__":
    main()
