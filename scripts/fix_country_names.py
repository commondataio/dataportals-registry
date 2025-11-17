#!/usr/bin/env python3
"""
Script to find and fix country names and IDs in owner.location and coverage sections.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, List, Tuple
import sys

# Import COUNTRIES from constants
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from constants import COUNTRIES

ROOT_DIR = Path(__file__).parent.parent
ENTITIES_DIR = ROOT_DIR / "data" / "entities"
SCHEDULED_DIR = ROOT_DIR / "data" / "scheduled"


def check_and_fix_country(country: Dict, filepath: Path) -> Tuple[bool, str]:
    """
    Check if country id and name match, fix if needed.
    Returns (was_fixed, message)
    """
    if not country or "id" not in country:
        return False, ""
    
    country_id = country.get("id")
    country_name = country.get("name", "")
    
    # Skip if country_id is not in COUNTRIES mapping (e.g., Unknown, World, etc.)
    if country_id not in COUNTRIES:
        return False, ""
    
    expected_name = COUNTRIES[country_id]
    
    if country_name != expected_name:
        country["name"] = expected_name
        return True, f"Fixed: {country_id} name '{country_name}' -> '{expected_name}'"
    
    return False, ""


def fix_record(record: Dict, filepath: Path) -> List[str]:
    """
    Fix country names/IDs in a record.
    Returns list of fix messages.
    """
    fixes = []
    
    # Fix owner.location.country
    if "owner" in record and "location" in record["owner"]:
        owner_location = record["owner"]["location"]
        if "country" in owner_location:
            was_fixed, msg = check_and_fix_country(owner_location["country"], filepath)
            if was_fixed:
                fixes.append(f"owner.location: {msg}")
    
    # Fix coverage[].location.country
    if "coverage" in record and isinstance(record["coverage"], list):
        for idx, coverage_item in enumerate(record["coverage"]):
            if "location" in coverage_item and "country" in coverage_item["location"]:
                was_fixed, msg = check_and_fix_country(
                    coverage_item["location"]["country"], filepath
                )
                if was_fixed:
                    fixes.append(f"coverage[{idx}].location: {msg}")
    
    return fixes


def process_directory(directory: Path, dryrun: bool = False) -> Tuple[int, int]:
    """
    Process all YAML files in a directory.
    Returns (files_processed, files_fixed)
    """
    files_processed = 0
    files_fixed = 0
    
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if not filename.endswith(".yaml"):
                continue
            
            filepath = Path(root) / filename
            files_processed += 1
            
            try:
                with open(filepath, "r", encoding="utf8") as f:
                    record = yaml.safe_load(f)
                
                if not record:
                    continue
                
                fixes = fix_record(record, filepath)
                
                if fixes:
                    files_fixed += 1
                    print(f"\n{filepath}")
                    for fix_msg in fixes:
                        print(f"  - {fix_msg}")
                    
                    if not dryrun:
                        with open(filepath, "w", encoding="utf8") as f:
                            yaml.safe_dump(
                                record,
                                f,
                                allow_unicode=True,
                                default_flow_style=False,
                                sort_keys=False,
                            )
                        print(f"  ✓ Fixed and saved")
                    else:
                        print(f"  [DRYRUN] Would fix")
            
            except Exception as e:
                print(f"Error processing {filepath}: {e}", file=sys.stderr)
    
    return files_processed, files_fixed


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Find and fix country names and IDs in YAML records"
    )
    parser.add_argument(
        "--dryrun",
        action="store_true",
        help="Don't write changes, just report what would be fixed",
    )
    parser.add_argument(
        "--entities-only",
        action="store_true",
        help="Only process entities directory",
    )
    parser.add_argument(
        "--scheduled-only",
        action="store_true",
        help="Only process scheduled directory",
    )
    
    args = parser.parse_args()
    
    print("Scanning for country name/ID mismatches...")
    print(f"COUNTRIES mapping has {len(COUNTRIES)} entries")
    print()
    
    total_processed = 0
    total_fixed = 0
    
    if not args.scheduled_only:
        print("Processing entities directory...")
        processed, fixed = process_directory(ENTITIES_DIR, dryrun=args.dryrun)
        total_processed += processed
        total_fixed += fixed
        print(f"Entities: {processed} files processed, {fixed} files fixed")
    
    if not args.entities_only:
        print("\nProcessing scheduled directory...")
        processed, fixed = process_directory(SCHEDULED_DIR, dryrun=args.dryrun)
        total_processed += processed
        total_fixed += fixed
        print(f"Scheduled: {processed} files processed, {fixed} files fixed")
    
    print(f"\n{'='*60}")
    print(f"Total: {total_processed} files processed, {total_fixed} files fixed")
    if args.dryrun:
        print("(DRYRUN mode - no files were modified)")


if __name__ == "__main__":
    main()

