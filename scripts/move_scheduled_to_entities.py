#!/usr/bin/env python3
"""
Script to move records from scheduled to entities directory.

Criteria for moving:
1. Has country metadata (not "Unknown")
2. Has subregional metadata (subregion.id exists)
3. Has at least one endpoint
"""

import os
import yaml
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Import constants
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from constants import MAP_CATALOG_TYPE_SUBDIR

SCHEDULED_DIR = Path(__file__).parent.parent / "data" / "scheduled"
ENTITIES_DIR = Path(__file__).parent.parent / "data" / "entities"


def get_country(record: Dict) -> Optional[str]:
    """Extract country code from record."""
    # Check coverage first
    if "coverage" in record and isinstance(record["coverage"], list):
        for cov in record["coverage"]:
            if isinstance(cov, dict) and "location" in cov:
                loc = cov["location"]
                if isinstance(loc, dict) and "country" in loc:
                    country = loc["country"]
                    if isinstance(country, dict) and "id" in country:
                        country_id = country["id"]
                        if country_id and country_id != "Unknown":
                            return country_id
    
    # Check owner.location
    if "owner" in record and isinstance(record["owner"], dict):
        owner = record["owner"]
        if "location" in owner and isinstance(owner["location"], dict):
            loc = owner["location"]
            if "country" in loc and isinstance(loc["country"], dict):
                country_id = loc["country"].get("id")
                if country_id and country_id != "Unknown":
                    return country_id
    
    return None


def get_subregion(record: Dict) -> Optional[str]:
    """Extract subregion code from record."""
    # Check coverage first
    if "coverage" in record and isinstance(record["coverage"], list):
        for cov in record["coverage"]:
            if isinstance(cov, dict) and "location" in cov:
                loc = cov["location"]
                if isinstance(loc, dict) and "subregion" in loc:
                    subregion = loc["subregion"]
                    if isinstance(subregion, dict) and "id" in subregion:
                        subregion_id = subregion["id"]
                        if subregion_id:
                            return subregion_id
    
    # Check owner.location
    if "owner" in record and isinstance(record["owner"], dict):
        owner = record["owner"]
        if "location" in owner and isinstance(owner["location"], dict):
            loc = owner["location"]
            if "subregion" in loc and isinstance(loc["subregion"], dict):
                subregion_id = loc["subregion"].get("id")
                if subregion_id:
                    return subregion_id
    
    return None


def has_endpoints(record: Dict) -> bool:
    """Check if record has at least one endpoint."""
    if "endpoints" in record:
        endpoints = record["endpoints"]
        if isinstance(endpoints, list) and len(endpoints) > 0:
            return True
    return False


def get_category_dir(catalog_type: str) -> str:
    """Get category directory name from catalog type."""
    return MAP_CATALOG_TYPE_SUBDIR.get(catalog_type, "opendata")


def get_destination_path(record: Dict, country: str, subregion: str) -> Path:
    """Determine destination path for the record."""
    catalog_type = record.get("catalog_type", "Open data portal")
    category = get_category_dir(catalog_type)
    
    # Build path: entities/{COUNTRY}/{SUBREgION}/{CATEGORY}/
    dest_dir = ENTITIES_DIR / country / subregion / category
    return dest_dir


def process_file(filepath: Path) -> Tuple[bool, Optional[str]]:
    """Process a single YAML file. Returns (should_move, error_message)."""
    try:
        with open(filepath, "r", encoding="utf8") as f:
            record = yaml.safe_load(f)
        
        if not record:
            return False, "Empty record"
        
        # Check criteria
        country = get_country(record)
        if not country:
            return False, "No valid country metadata"
        
        subregion = get_subregion(record)
        if not subregion:
            return False, "No subregional metadata"
        
        if not has_endpoints(record):
            return False, "No endpoints"
        
        # All criteria met - prepare to move
        # Update status to active
        record["status"] = "active"
        
        # Determine destination
        dest_dir = get_destination_path(record, country, subregion)
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        dest_file = dest_dir / filepath.name
        
        # Check if destination file already exists
        if dest_file.exists():
            return False, f"Destination file already exists: {dest_file}"
        
        # Write updated record to destination
        with open(dest_file, "w", encoding="utf8") as f:
            yaml.safe_dump(record, f, allow_unicode=True, sort_keys=False)
        
        # Remove source file
        filepath.unlink()
        
        return True, None
        
    except Exception as e:
        return False, f"Error: {str(e)}"


def main():
    """Main function to process all scheduled files."""
    if not SCHEDULED_DIR.exists():
        print(f"Error: Scheduled directory not found: {SCHEDULED_DIR}")
        return
    
    moved_count = 0
    skipped_count = 0
    error_count = 0
    
    # Walk through all YAML files in scheduled directory
    for root, dirs, files in os.walk(SCHEDULED_DIR):
        for file in files:
            if file.endswith(('.yaml', '.yml')):
                filepath = Path(root) / file
                should_move, error = process_file(filepath)
                
                if should_move:
                    moved_count += 1
                    print(f"✓ Moved: {filepath.relative_to(SCHEDULED_DIR)}")
                elif error:
                    if "already exists" in error:
                        error_count += 1
                        print(f"✗ Error: {filepath.relative_to(SCHEDULED_DIR)} - {error}")
                    else:
                        skipped_count += 1
                        # Only print skipped files if they're close to matching
                        if "No valid country" not in error and "No subregional" not in error:
                            print(f"- Skipped: {filepath.relative_to(SCHEDULED_DIR)} - {error}")
    
    print(f"\nSummary:")
    print(f"  Moved: {moved_count}")
    print(f"  Skipped: {skipped_count}")
    print(f"  Errors: {error_count}")


if __name__ == "__main__":
    main()

