#!/usr/bin/env python3
"""
Fix all data quality issues from full_report.jsonl.

Reads dataquality/full_report.jsonl and fixes all issues for each record.
Groups issues by record_id and processes them using the IssueFixer class.
"""

import json
import re
import yaml
from pathlib import Path
from urllib.parse import urlparse
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict
import sys

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

# Import constants
sys.path.insert(0, str(Path(__file__).parent))
from constants import MAP_SOFTWARE_OWNER_CATALOG_TYPE, COUNTRIES

# Import IssueFixer from fix_all_priority_issues
from fix_all_priority_issues import IssueFixer

BASE_DIR = Path(__file__).parent.parent
FULL_REPORT_FILE = BASE_DIR / "dataquality" / "full_report.jsonl"
ENTITIES_DIR = BASE_DIR / "data" / "entities"


def read_full_report(file_path: Path) -> Dict[str, List[Dict[str, Any]]]:
    """Read full_report.jsonl and group issues by record_id."""
    records = defaultdict(lambda: {"file_path": None, "issues": []})
    
    if not file_path.exists():
        print(f"Error: File not found: {file_path}")
        return records
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                issue = json.loads(line)
                record_id = issue.get("record_id")
                file_path_str = issue.get("file_path")
                
                if not record_id:
                    continue
                
                if records[record_id]["file_path"] is None:
                    records[record_id]["file_path"] = file_path_str
                
                records[record_id]["issues"].append(issue)
            except json.JSONDecodeError as e:
                print(f"Warning: Failed to parse JSON at line {line_num}: {e}")
                continue
    
    return records


def fix_record(record_id: str, record_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Fix all issues for a single record."""
    file_path_str = record_data.get("file_path", "")
    if not file_path_str:
        return False, [f"No file_path for record {record_id}"]
    
    full_path = ENTITIES_DIR / file_path_str
    
    if not full_path.exists():
        return False, [f"File not found: {full_path}"]
    
    try:
        # Load YAML file
        with open(full_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        if not data:
            return False, ["Empty YAML file"]
        
        # Create fixer and fix all issues
        fixer = IssueFixer(data, full_path)
        issues = record_data.get("issues", [])
        
        fixed_count = 0
        for issue in issues:
            if fixer.fix_issue(issue):
                fixed_count += 1
        
        # Save if changes were made
        if fixer.changes:
            with open(full_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            return True, fixer.changes
        
        return False, ["No changes made"]
    
    except Exception as e:
        return False, [f"Error: {str(e)}"]


def main():
    """Main entry point."""
    print("Reading issues from full_report.jsonl...")
    records = read_full_report(FULL_REPORT_FILE)
    
    if not records:
        print("No records with issues found.")
        return
    
    print(f"Found {len(records)} records with issues to process.\n")
    
    fixed_count = 0
    skipped_count = 0
    error_count = 0
    total_issues = sum(len(r["issues"]) for r in records.values())
    
    for i, (record_id, record_data) in enumerate(sorted(records.items()), 1):
        file_path = record_data.get("file_path", "")
        issues_count = len(record_data.get("issues", []))
        
        print(f"[{i}/{len(records)}] Processing {record_id} ({file_path})")
        print(f"  Issues: {issues_count}")
        
        success, messages = fix_record(record_id, record_data)
        
        if success:
            fixed_count += 1
            print(f"  ✓ Fixed: {len(messages)} changes")
            for msg in messages[:5]:  # Show first 5 changes
                print(f"    - {msg}")
            if len(messages) > 5:
                print(f"    ... and {len(messages) - 5} more changes")
        elif messages and messages[0].startswith("Error"):
            error_count += 1
            print(f"  ✗ Error: {messages[0]}")
        else:
            skipped_count += 1
            print(f"  ○ Skipped: {messages[0] if messages else 'No changes needed'}")
        
        print()
    
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total records: {len(records)}")
    print(f"Total issues: {total_issues}")
    print(f"Fixed: {fixed_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Errors: {error_count}")


if __name__ == "__main__":
    main()
