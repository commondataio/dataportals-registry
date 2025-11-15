#!/usr/bin/env python3
"""
Remove add_date and update_date fields from all YAML files in entities and scheduled directories.
"""

import os
import yaml
from pathlib import Path

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

# Paths
ENTITIES_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "entities")
SCHEDULED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "scheduled")

def remove_date_fields(filepath):
    """Remove add_date and update_date from a YAML file."""
    try:
        with open(filepath, "r", encoding="utf8") as f:
            record = yaml.load(f, Loader=Loader)
        
        if not record:
            return False, "File is empty"
        
        changed = False
        removed_fields = []
        
        if "add_date" in record:
            del record["add_date"]
            changed = True
            removed_fields.append("add_date")
        
        if "update_date" in record:
            del record["update_date"]
            changed = True
            removed_fields.append("update_date")
        
        if changed:
            # Write back the file
            with open(filepath, "w", encoding="utf8") as f:
                yaml.safe_dump(record, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
            return True, ", ".join(removed_fields)
        
        return False, None
        
    except yaml.YAMLError as e:
        return False, f"YAML error: {str(e)}"
    except Exception as e:
        return False, f"Error: {str(e)}"

def process_directory(directory, dir_name):
    """Process all YAML files in a directory."""
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist, skipping...")
        return 0, 0
    
    print(f"\nProcessing {dir_name} directory: {directory}")
    
    fixed_count = 0
    error_count = 0
    total_count = 0
    
    # Walk through all YAML files
    for root, dirs, files in os.walk(directory):
        yaml_files = [os.path.join(root, fi) for fi in files if fi.endswith((".yaml", ".yml"))]
        for filepath in yaml_files:
            total_count += 1
            fixed, message = remove_date_fields(filepath)
            if fixed:
                fixed_count += 1
                if fixed_count <= 10:  # Show first 10 examples
                    rel_path = os.path.relpath(filepath, directory)
                    print(f"  ✓ Removed from: {rel_path} ({message})")
            elif message and "error" in message.lower():
                error_count += 1
                if error_count <= 5:  # Show first 5 errors
                    rel_path = os.path.relpath(filepath, directory)
                    print(f"  ✗ Error: {rel_path} - {message}")
    
    print(f"  Total files: {total_count}, Fixed: {fixed_count}, Errors: {error_count}")
    return fixed_count, error_count

def main():
    """Main function."""
    print("Removing add_date and update_date fields from YAML files...\n")
    
    entities_fixed, entities_errors = process_directory(ENTITIES_DIR, "entities")
    scheduled_fixed, scheduled_errors = process_directory(SCHEDULED_DIR, "scheduled")
    
    print(f"\n{'='*60}")
    print("Summary:")
    print(f"  Entities directory:")
    print(f"    Files fixed: {entities_fixed}")
    print(f"    Errors: {entities_errors}")
    print(f"  Scheduled directory:")
    print(f"    Files fixed: {scheduled_fixed}")
    print(f"    Errors: {scheduled_errors}")
    print(f"  Total fixed: {entities_fixed + scheduled_fixed}")
    print(f"  Total errors: {entities_errors + scheduled_errors}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()

