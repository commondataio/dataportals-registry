#!/usr/bin/env python3
"""
Fix inconsistencies in software.id across all entity files.

This script:
1. Fixes invalid software.id values
2. Updates software.name to match software definitions
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Tuple

# Base directories
BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
SOFTWARE_DIR = BASE_DIR / "data" / "software"

# Mapping for invalid IDs to correct IDs
INVALID_ID_MAPPING = {
    "arcgis": "arcgishub",  # ArcGIS Hub
    "d4c": "custom",  # Data4Citizen - no definition exists, map to custom
    "onegeo": "custom",  # Onegeo Suite - no definition exists, map to custom
}


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
                    }
        except Exception as e:
            print(f"Error loading {software_file}: {e}")
    
    return software


def fix_entity_file(filepath: Path, software_defs: Dict[str, Dict], dry_run: bool = False) -> Tuple[bool, str]:
    """Fix software.id and software.name in an entity file."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        
        if not data or "software" not in data:
            return False, "no software field"
        
        software_data = data["software"]
        if "id" not in software_data:
            return False, "no software.id"
        
        original_id = software_data["id"]
        original_name = software_data.get("name", "")
        changed = False
        changes = []
        
        # Fix invalid IDs
        if original_id in INVALID_ID_MAPPING:
            new_id = INVALID_ID_MAPPING[original_id]
            software_data["id"] = new_id
            changes.append(f"id: {original_id} -> {new_id}")
            changed = True
            # Update name if mapping to custom
            if new_id == "custom":
                software_data["name"] = "Custom software"
                if original_name != "Custom software":
                    changes.append(f"name: {original_name} -> Custom software")
        
        # Fix name mismatches for valid IDs
        current_id = software_data["id"]
        if current_id in software_defs:
            expected_name = software_defs[current_id]["name"]
            if software_data.get("name") != expected_name:
                software_data["name"] = expected_name
                changes.append(f"name: {original_name} -> {expected_name}")
                changed = True
        
        if changed and not dry_run:
            with open(filepath, "w", encoding="utf-8") as f:
                yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
        
        if changed:
            return True, "; ".join(changes)
        return False, "no changes needed"
        
    except Exception as e:
        return False, f"error: {e}"


def main(dry_run: bool = False):
    print("Loading software definitions...")
    software_defs = load_software_definitions()
    print(f"Loaded {len(software_defs)} software definitions")
    
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Fixing entity files...")
    entity_files = list(ENTITIES_DIR.rglob("*.yaml"))
    print(f"Processing {len(entity_files)} entity files...")
    
    fixed_count = 0
    error_count = 0
    
    for entity_file in entity_files:
        fixed, message = fix_entity_file(entity_file, software_defs, dry_run)
        if fixed:
            fixed_count += 1
            if fixed_count <= 20:  # Show first 20
                print(f"  ✓ {entity_file.relative_to(BASE_DIR)}: {message}")
        elif "error" in message:
            error_count += 1
            if error_count <= 10:  # Show first 10 errors
                print(f"  ✗ {entity_file.relative_to(BASE_DIR)}: {message}")
    
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Summary:")
    print(f"  - Fixed: {fixed_count} files")
    if error_count > 0:
        print(f"  - Errors: {error_count} files")
    
    if dry_run:
        print("\nRun without --dry-run to apply changes")
    else:
        print("\n✓ All fixes applied!")


if __name__ == "__main__":
    import sys
    dry_run = "--dry-run" in sys.argv or "-n" in sys.argv
    main(dry_run=dry_run)

