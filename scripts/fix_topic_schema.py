#!/usr/bin/env python
"""
Script to fix TOPIC_SCHEMA_VIOLATION issues by converting bare string topics
to proper topic objects with type, id, and name fields.
"""

import os
import re
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

# Get script directory and repository root
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)
REPORT_FILE = os.path.join(_REPO_ROOT, "dataquality", "rules", "TOPIC_SCHEMA_VIOLATION.txt")
ENTITIES_DIR = os.path.join(_REPO_ROOT, "data", "entities")

# Mapping from bare topic strings to proper topic objects (id, name, type)
# Uses EU Data Theme and ISO19115 vocabularies where applicable
TOPIC_STRING_TO_OBJECT: Dict[str, Dict[str, str]] = {
    # Geoportal / spatial
    "Geospatial": {"id": "Location", "name": "Location", "type": "iso19115"},
    "Geography": {"id": "Boundaries", "name": "Boundaries", "type": "iso19115"},
    "Maps": {"id": "Imagery / Base Maps / Earth Cover", "name": "Imagery / Base Maps / Earth Cover", "type": "iso19115"},
    # Open data / government
    "Open Data": {"id": "GOVE", "name": "Government and public sector", "type": "eudatatheme"},
    "Government Data": {"id": "GOVE", "name": "Government and public sector", "type": "eudatatheme"},
    # Scientific / research
    "Research": {"id": "TECH", "name": "Science and technology", "type": "eudatatheme"},
    "Science": {"id": "TECH", "name": "Science and technology", "type": "eudatatheme"},
    "Academic": {"id": "TECH", "name": "Science and technology", "type": "eudatatheme"},
    # Indicators / statistics
    "Statistics": {"id": "SOCI", "name": "Population and society", "type": "eudatatheme"},
    "Indicators": {"id": "SOCI", "name": "Population and society", "type": "eudatatheme"},
    "Metrics": {"id": "SOCI", "name": "Population and society", "type": "eudatatheme"},
    # Microdata
    "Microdata": {"id": "SOCI", "name": "Population and society", "type": "eudatatheme"},
    "Surveys": {"id": "SOCI", "name": "Population and society", "type": "eudatatheme"},
    # Marketplace
    "Data Marketplace": {"id": "ECON", "name": "Economy and finance", "type": "eudatatheme"},
    "Commercial Data": {"id": "ECON", "name": "Economy and finance", "type": "eudatatheme"},
    "Third-party Data": {"id": "ECON", "name": "Economy and finance", "type": "eudatatheme"},
    # ML / AI
    "Machine Learning": {"id": "TECH", "name": "Science and technology", "type": "eudatatheme"},
    "AI": {"id": "TECH", "name": "Science and technology", "type": "eudatatheme"},
    "Data Science": {"id": "TECH", "name": "Science and technology", "type": "eudatatheme"},
    # Search / discovery
    "Data Search": {"id": "GOVE", "name": "Government and public sector", "type": "eudatatheme"},
    "Discovery": {"id": "GOVE", "name": "Government and public sector", "type": "eudatatheme"},
    # API
    "API": {"id": "TECH", "name": "Science and technology", "type": "eudatatheme"},
    "Web Services": {"id": "TECH", "name": "Science and technology", "type": "eudatatheme"},
    # Metadata
    "Metadata": {"id": "GOVE", "name": "Government and public sector", "type": "eudatatheme"},
    "Data Catalog": {"id": "GOVE", "name": "Government and public sector", "type": "eudatatheme"},
}


def string_to_topic(s: str) -> Dict[str, str]:
    """Convert a bare string topic to a proper topic object."""
    s = (s or "").strip()
    if not s:
        return None
    if s in TOPIC_STRING_TO_OBJECT:
        return TOPIC_STRING_TO_OBJECT[s].copy()
    # Fallback: use string as both id and name with eudatatheme type
    return {"id": s, "name": s, "type": "eudatatheme"}


def parse_report_file(report_path: str) -> List[str]:
    """
    Parse the report file and extract unique file paths.
    Returns list of file paths that need fixing.
    """
    file_paths = set()

    with open(report_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    issues_start = None
    for i, line in enumerate(lines):
        if line.strip() == "=== ISSUES ===":
            issues_start = i + 1
            break

    if issues_start is None:
        return list(file_paths)

    for i in range(issues_start, len(lines)):
        line = lines[i].strip()
        if line.startswith("File: "):
            file_path = line.replace("File: ", "").strip()
            file_paths.add(file_path)

    return sorted(file_paths)


def fix_topics_in_record(topics: List) -> Tuple[List, int]:
    """
    Convert any bare string topics to proper topic objects.
    Returns (fixed_topics_list, count_of_fixes).
    """
    if not topics or not isinstance(topics, list):
        return topics, 0

    fixed = []
    fixes_count = 0
    seen_keys = set()  # Deduplicate by (id, type)

    for item in topics:
        if isinstance(item, dict):
            # Already a proper topic object - keep if it has id/name/type
            if item.get("id") or item.get("name"):
                key = (item.get("id", ""), item.get("type", ""))
                if key not in seen_keys:
                    seen_keys.add(key)
                    fixed.append(item)
            else:
                fixes_count += 1
        elif isinstance(item, str):
            obj = string_to_topic(item)
            if obj:
                key = (obj.get("id", ""), obj.get("type", ""))
                if key not in seen_keys:
                    seen_keys.add(key)
                    fixed.append(obj)
                    fixes_count += 1
        # Skip non-dict, non-string (e.g. None)

    return fixed, fixes_count


def fix_yaml_file(file_path: str) -> Tuple[bool, int]:
    """
    Fix a single YAML file by converting bare string topics to proper objects.
    Returns (modified, fixes_count) tuple.
    """
    full_path = os.path.join(ENTITIES_DIR, file_path)

    if not os.path.exists(full_path):
        print(f"  ✗ File not found: {full_path}")
        return False, 0

    try:
        with open(full_path, "r", encoding="utf-8") as f:
            data = yaml.load(f, Loader=Loader)

        topics = data.get("topics", [])
        if not isinstance(topics, list):
            return False, 0

        fixed_topics, fixes_count = fix_topics_in_record(topics)
        if fixes_count == 0:
            return False, 0

        data["topics"] = fixed_topics

        with open(full_path, "w", encoding="utf-8") as f:
            yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)

        return True, fixes_count

    except Exception as e:
        print(f"  ✗ Error processing {file_path}: {e}")
        import traceback

        traceback.print_exc()
        return False, 0


def main():
    """Main function to fix all TOPIC_SCHEMA_VIOLATION issues."""
    print("Parsing report file...")
    file_paths = parse_report_file(REPORT_FILE)

    print(f"\nFound {len(file_paths)} files with topic schema violations")
    print(f"Report file: {REPORT_FILE}")
    print(f"Entities directory: {ENTITIES_DIR}\n")

    fixed_count = 0
    files_modified = 0

    for i, file_path in enumerate(file_paths):
        if (i + 1) % 500 == 0 or i == 0:
            print(f"[{i + 1}/{len(file_paths)}] Processing...")
        modified, fixes = fix_yaml_file(file_path)
        if modified:
            files_modified += 1
            fixed_count += fixes

    print(f"\n=== Summary ===")
    print(f"Files with issues: {len(file_paths)}")
    print(f"Files modified: {files_modified}")
    print(f"Topic strings converted: {fixed_count}")


if __name__ == "__main__":
    main()
