#!/usr/bin/env python
"""Standalone script to validate all YAML files in entities, scheduled, or software directory"""

import os
import sys
import json
import yaml
import argparse

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

try:
    from cerberus import Validator
except ImportError:
    print("Error: cerberus module not found. Please install it with: pip install cerberus")
    sys.exit(1)

# Schema paths
SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "schemes")
CATALOG_SCHEMA = os.path.join(SCHEMAS_DIR, "catalog.json")
SOFTWARE_SCHEMA = os.path.join(SCHEMAS_DIR, "software.json")


def validate_yaml_files(directory="entities"):
    """Validate all YAML files in specified directory"""
    
    # Determine root directory
    ROOT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", directory)
    
    if not os.path.exists(ROOT_DIR):
        print(f"Error: Directory not found at {ROOT_DIR}")
        return 1
    
    # Select appropriate schema based on directory
    if directory == "software":
        SCHEMA_FILE = SOFTWARE_SCHEMA
    else:
        SCHEMA_FILE = CATALOG_SCHEMA
    
    # Load schema
    if not os.path.exists(SCHEMA_FILE):
        print(f"Error: Schema file not found at {SCHEMA_FILE}")
        return 1
    
    with open(SCHEMA_FILE, "r", encoding="utf8") as f:
        schema = json.load(f)
    
    # Create validator
    v = Validator(schema)
    
    errors = []
    total = 0
    valid = 0
    
    print(f"Validating YAML files in {directory} directory...")
    print(f"Schema: {SCHEMA_FILE}")
    print(f"Directory: {ROOT_DIR}\n")
    
    # Walk through all YAML files
    for root, dirs, files in os.walk(ROOT_DIR):
        yaml_files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        for filename in yaml_files:
            total += 1
            try:
                with open(filename, "r", encoding="utf8") as f:
                    record = yaml.load(f, Loader=Loader)
                
                if record is None:
                    errors.append((filename, "File is empty or invalid YAML"))
                    continue
                
                if not v.validate(record, schema):
                    record_id = record.get("id", "unknown")
                    errors.append((filename, f"{record_id}: {str(v.errors)}"))
                else:
                    valid += 1
                    
            except yaml.YAMLError as e:
                errors.append((filename, f"YAML parsing error: {str(e)}"))
            except Exception as e:
                record_id = "unknown"
                try:
                    with open(filename, "r", encoding="utf8") as f:
                        record = yaml.load(f, Loader=Loader)
                    if record:
                        record_id = record.get("id", "unknown")
                except:
                    pass
                errors.append((filename, f"{record_id}: {str(e)}"))
    
    # Print results
    print(f"\n{'='*60}")
    print(f"Validation complete:")
    print(f"  Total files: {total}")
    print(f"  Valid: {valid}")
    print(f"  Errors: {len(errors)}")
    print(f"{'='*60}\n")
    
    if errors:
        print("Errors found:\n")
        for filename, error in errors:
            rel_path = os.path.relpath(filename, ROOT_DIR)
            print(f"  {rel_path}")
            print(f"    {error}\n")
        return 1
    else:
        print("✓ All files are valid!")
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate YAML files in entities, scheduled, or software directory")
    parser.add_argument(
        "--directory",
        "-d",
        default="entities",
        choices=["entities", "scheduled", "software"],
        help="Directory to validate (default: entities)"
    )
    args = parser.parse_args()
    sys.exit(validate_yaml_files(args.directory))

