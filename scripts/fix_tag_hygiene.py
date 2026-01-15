#!/usr/bin/env python
"""
Script to fix TAG_HYGIENE issues by parsing the report file
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
REPORT_FILE = os.path.join(_REPO_ROOT, "dataquality", "rules", "TAG_HYGIENE.txt")
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


def fix_tag(tag: str) -> List[str]:
    """
    Fix a single tag. Returns a list of fixed tags (may be empty if tag should be removed).
    """
    if not isinstance(tag, str):
        return []
    
    tag = tag.strip()
    
    # Remove empty tags (handle both empty string and None)
    if not tag or len(tag.strip()) == 0:
        return []
    
    # Remove tags that are clearly not real tags
    bad_patterns = [
        "Not specified in search result or metadata.",
        "not specified",
        "N/A",
        "n/a",
        "None",
        "none",
    ]
    tag_lower = tag.lower()
    for pattern in bad_patterns:
        if pattern.lower() in tag_lower:
            return []
    
    # Handle tags that are too short (< 3 characters)
    if len(tag) < 3:
        return []  # Remove short tags
    
    # Handle tags that are too long (> 40 characters)
    if len(tag) > 40:
        # Check if it contains multiple tags separated by commas
        if ',' in tag:
            # Split by comma and clean up
            parts = [p.strip() for p in tag.split(',')]
            fixed_tags = []
            for part in parts:
                if len(part) >= 3 and len(part) <= 40:
                    fixed_tags.append(part)
            return fixed_tags if fixed_tags else []
        else:
            # Too long but no commas - try to shorten or remove
            # For now, remove it as it's likely not a proper tag
            return []
    
    return [tag]


def fix_yaml_file(file_path: str, tag_indices_to_fix: List[int]) -> Tuple[bool, int]:
    """
    Fix a single YAML file by fixing all tags at the specified indices.
    Returns (modified, fixes_count) tuple.
    """
    full_path = os.path.join(ENTITIES_DIR, file_path)
    
    if not os.path.exists(full_path):
        print(f"  ✗ File not found: {full_path}")
        return False, 0
    
    try:
        # Read YAML file
        with open(full_path, 'r', encoding='utf-8') as f:
            data = yaml.load(f, Loader=Loader)
        
        tags = data.get('tags', [])
        if not isinstance(tags, list):
            print(f"  ⚠ Tags is not a list, skipping")
            return False, 0
        
        # Process tags in reverse order to avoid index shifting
        tag_indices_to_fix_sorted = sorted(set(tag_indices_to_fix), reverse=True)
        fixes_count = 0
        modified = False
        
        for tag_index in tag_indices_to_fix_sorted:
            if tag_index >= len(tags):
                print(f"  ⚠ Tag index {tag_index} out of range (tags has {len(tags)} items)")
                continue
            
            original_tag = tags[tag_index]
            fixed_tags = fix_tag(original_tag)
            
            if not fixed_tags:
                # Remove the tag
                tags.pop(tag_index)
                print(f"  ✓ Removed tag[{tag_index}]: '{original_tag}'")
                fixes_count += 1
                modified = True
            elif fixed_tags != [original_tag]:
                # Replace with fixed tags
                # Remove the old tag and insert new ones at the same position
                tags.pop(tag_index)
                for i, new_tag in enumerate(fixed_tags):
                    tags.insert(tag_index + i, new_tag)
                print(f"  ✓ Fixed tag[{tag_index}]: '{original_tag}' -> {fixed_tags}")
                fixes_count += 1
                modified = True
        
        if modified:
            data['tags'] = tags
            
            # Write YAML file back
            with open(full_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
        
        return modified, fixes_count
    
    except Exception as e:
        print(f"  ✗ Error processing file: {e}")
        import traceback
        traceback.print_exc()
        return False, 0


def main():
    """Main function to fix all TAG_HYGIENE issues."""
    print("Parsing report file...")
    issues = parse_report_file(REPORT_FILE)
    
    print(f"\nFound {len(issues)} issues to fix")
    print(f"Report file: {REPORT_FILE}")
    print(f"Entities directory: {ENTITIES_DIR}\n")
    
    fixed_count = 0
    skipped_count = 0
    error_count = 0
    
    # Group issues by file
    file_issues = {}
    for file_path, tag_index, current_value in issues:
        if file_path not in file_issues:
            file_issues[file_path] = []
        file_issues[file_path].append((tag_index, current_value))
    
    total_processed = 0
    for file_path, file_issue_list in file_issues.items():
        print(f"[{total_processed + 1}/{len(file_issues)}] {file_path}")
        tag_indices = [idx for idx, _ in file_issue_list]
        for tag_index, current_value in file_issue_list:
            print(f"  Tag[{tag_index}]: '{current_value}'")
        
        modified, fixes = fix_yaml_file(file_path, tag_indices)
        if modified:
            fixed_count += fixes
        else:
            skipped_count += len(file_issue_list)
        total_processed += 1
    
    print(f"\n=== Summary ===")
    print(f"Total issues: {len(issues)}")
    print(f"Files processed: {len(file_issues)}")
    print(f"Fixed: {fixed_count}")
    print(f"Skipped/No change: {skipped_count}")
    print(f"Errors: {error_count}")


if __name__ == "__main__":
    main()
