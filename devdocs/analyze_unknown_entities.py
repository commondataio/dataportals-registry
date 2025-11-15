#!/usr/bin/env python3
"""
Analyze YAML files in entities/Unknown directory to identify missing or fixable metadata.
"""

import os
import yaml
import json
from collections import defaultdict
from pathlib import Path

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

# Load schema to understand required fields
SCHEMA_FILE = "data/schemes/catalog.json"
UNKNOWN_DIR = "data/entities/Unknown"

def load_schema():
    """Load the catalog schema."""
    with open(SCHEMA_FILE, "r", encoding="utf8") as f:
        return json.load(f)

def analyze_file(filepath):
    """Analyze a single YAML file and return issues found."""
    issues = {
        "file": filepath,
        "missing_required": [],
        "unknown_country": False,
        "unknown_owner_country": False,
        "empty_topics": False,
        "null_owner_link": False,
        "generic_owner_name": False,
        "missing_description": False,
        "missing_identifiers": False,
        "missing_export_standard": False,
        "missing_dates": False,
        "empty_tags": False,
    }
    
    try:
        with open(filepath, "r", encoding="utf8") as f:
            record = yaml.load(f, Loader=Loader)
        
        if not record:
            return issues
        
        # Check for Unknown country in coverage
        if "coverage" in record and isinstance(record["coverage"], list):
            for cov in record["coverage"]:
                if isinstance(cov, dict) and "location" in cov:
                    loc = cov["location"]
                    if isinstance(loc, dict) and "country" in loc:
                        country = loc["country"]
                        if isinstance(country, dict) and country.get("id") == "Unknown":
                            issues["unknown_country"] = True
        
        # Check for Unknown country in owner
        if "owner" in record and isinstance(record["owner"], dict):
            owner = record["owner"]
            if "location" in owner and isinstance(owner["location"], dict):
                loc = owner["location"]
                if "country" in loc and isinstance(loc["country"], dict):
                    if loc["country"].get("id") == "Unknown":
                        issues["unknown_owner_country"] = True
            
            # Check for null owner link
            if owner.get("link") is None:
                issues["null_owner_link"] = True
            
            # Check for generic owner names
            owner_name = owner.get("name", "")
            if owner_name in ["Not provided in available content", "Unknown", ""]:
                issues["generic_owner_name"] = True
        
        # Check for empty topics
        if "topics" in record:
            if not record["topics"] or (isinstance(record["topics"], list) and len(record["topics"]) == 0):
                issues["empty_topics"] = True
        
        # Check for missing description
        if not record.get("description") or record.get("description", "").strip() == "":
            issues["missing_description"] = False  # Not required, but good to have
        
        # Check for missing identifiers
        if "identifiers" not in record or not record["identifiers"]:
            issues["missing_identifiers"] = True
        
        # Check for missing export_standard
        if "export_standard" not in record:
            issues["missing_export_standard"] = True
        
        # Check for missing dates
        if "add_date" not in record and "update_date" not in record:
            issues["missing_dates"] = True
        
        # Check for empty tags
        if "tags" not in record or not record["tags"]:
            issues["empty_tags"] = False  # Not required, but good to have
        
    except Exception as e:
        issues["error"] = str(e)
    
    return issues

def main():
    """Main analysis function."""
    schema = load_schema()
    
    # Find all YAML files in Unknown directory
    unknown_path = Path(UNKNOWN_DIR)
    yaml_files = list(unknown_path.rglob("*.yaml"))
    
    print(f"Analyzing {len(yaml_files)} YAML files in {UNKNOWN_DIR}...\n")
    
    all_issues = []
    stats = defaultdict(int)
    
    for yaml_file in yaml_files:
        issues = analyze_file(yaml_file)
        all_issues.append(issues)
        
        # Count issues
        if issues.get("unknown_country"):
            stats["unknown_country"] += 1
        if issues.get("unknown_owner_country"):
            stats["unknown_owner_country"] += 1
        if issues.get("empty_topics"):
            stats["empty_topics"] += 1
        if issues.get("null_owner_link"):
            stats["null_owner_link"] += 1
        if issues.get("generic_owner_name"):
            stats["generic_owner_name"] += 1
        if issues.get("missing_identifiers"):
            stats["missing_identifiers"] += 1
        if issues.get("missing_export_standard"):
            stats["missing_export_standard"] += 1
        if issues.get("missing_dates"):
            stats["missing_dates"] += 1
    
    # Print summary statistics
    print("=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    print(f"Total files analyzed: {len(yaml_files)}\n")
    
    print("Issues found:")
    print(f"  - Files with Unknown country in coverage: {stats['unknown_country']}")
    print(f"  - Files with Unknown country in owner: {stats['unknown_owner_country']}")
    print(f"  - Files with empty topics: {stats['empty_topics']}")
    print(f"  - Files with null owner.link: {stats['null_owner_link']}")
    print(f"  - Files with generic owner.name: {stats['generic_owner_name']}")
    print(f"  - Files missing identifiers: {stats['missing_identifiers']}")
    print(f"  - Files missing export_standard: {stats['missing_export_standard']}")
    print(f"  - Files missing dates: {stats['missing_dates']}")
    
    # Print detailed findings
    print("\n" + "=" * 80)
    print("DETAILED FINDINGS - Files that could be improved")
    print("=" * 80)
    
    # Group by issue type
    unknown_country_files = [i for i in all_issues if i.get("unknown_country")]
    unknown_owner_country_files = [i for i in all_issues if i.get("unknown_owner_country")]
    empty_topics_files = [i for i in all_issues if i.get("empty_topics")]
    null_owner_link_files = [i for i in all_issues if i.get("null_owner_link")]
    generic_owner_name_files = [i for i in all_issues if i.get("generic_owner_name")]
    missing_identifiers_files = [i for i in all_issues if i.get("missing_identifiers")]
    missing_export_standard_files = [i for i in all_issues if i.get("missing_export_standard")]
    missing_dates_files = [i for i in all_issues if i.get("missing_dates")]
    
    if unknown_country_files:
        print(f"\n1. FILES WITH UNKNOWN COUNTRY IN COVERAGE ({len(unknown_country_files)} files):")
        print("-" * 80)
        for issue in unknown_country_files[:20]:  # Show first 20
            rel_path = os.path.relpath(issue["file"])
            print(f"  - {rel_path}")
        if len(unknown_country_files) > 20:
            print(f"  ... and {len(unknown_country_files) - 20} more")
    
    if unknown_owner_country_files:
        print(f"\n2. FILES WITH UNKNOWN COUNTRY IN OWNER ({len(unknown_owner_country_files)} files):")
        print("-" * 80)
        for issue in unknown_owner_country_files[:20]:
            rel_path = os.path.relpath(issue["file"])
            print(f"  - {rel_path}")
        if len(unknown_owner_country_files) > 20:
            print(f"  ... and {len(unknown_owner_country_files) - 20} more")
    
    if empty_topics_files:
        print(f"\n3. FILES WITH EMPTY TOPICS ({len(empty_topics_files)} files):")
        print("-" * 80)
        for issue in empty_topics_files[:20]:
            rel_path = os.path.relpath(issue["file"])
            print(f"  - {rel_path}")
        if len(empty_topics_files) > 20:
            print(f"  ... and {len(empty_topics_files) - 20} more")
    
    if null_owner_link_files:
        print(f"\n4. FILES WITH NULL OWNER.LINK ({len(null_owner_link_files)} files):")
        print("-" * 80)
        for issue in null_owner_link_files[:20]:
            rel_path = os.path.relpath(issue["file"])
            print(f"  - {rel_path}")
        if len(null_owner_link_files) > 20:
            print(f"  ... and {len(null_owner_link_files) - 20} more")
    
    if generic_owner_name_files:
        print(f"\n5. FILES WITH GENERIC OWNER.NAME ({len(generic_owner_name_files)} files):")
        print("-" * 80)
        for issue in generic_owner_name_files[:20]:
            rel_path = os.path.relpath(issue["file"])
            print(f"  - {rel_path}")
        if len(generic_owner_name_files) > 20:
            print(f"  ... and {len(generic_owner_name_files) - 20} more")
    
    if missing_identifiers_files:
        print(f"\n6. FILES MISSING IDENTIFIERS ({len(missing_identifiers_files)} files):")
        print("-" * 80)
        for issue in missing_identifiers_files[:20]:
            rel_path = os.path.relpath(issue["file"])
            print(f"  - {rel_path}")
        if len(missing_identifiers_files) > 20:
            print(f"  ... and {len(missing_identifiers_files) - 20} more")
    
    if missing_export_standard_files:
        print(f"\n7. FILES MISSING EXPORT_STANDARD ({len(missing_export_standard_files)} files):")
        print("-" * 80)
        for issue in missing_export_standard_files[:20]:
            rel_path = os.path.relpath(issue["file"])
            print(f"  - {rel_path}")
        if len(missing_export_standard_files) > 20:
            print(f"  ... and {len(missing_export_standard_files) - 20} more")
    
    if missing_dates_files:
        print(f"\n8. FILES MISSING DATES ({len(missing_dates_files)} files):")
        print("-" * 80)
        for issue in missing_dates_files[:20]:
            rel_path = os.path.relpath(issue["file"])
            print(f"  - {rel_path}")
        if len(missing_dates_files) > 20:
            print(f"  ... and {len(missing_dates_files) - 20} more")
    
    # Save detailed report to file
    report_file = "unknown_entities_analysis_report.txt"
    with open(report_file, "w", encoding="utf8") as f:
        f.write("=" * 80 + "\n")
        f.write("DETAILED ANALYSIS REPORT - Unknown Entities\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Total files analyzed: {len(yaml_files)}\n\n")
        
        f.write("SUMMARY:\n")
        for key, value in stats.items():
            f.write(f"  - {key}: {value}\n")
        
        f.write("\n\nDETAILED FINDINGS:\n\n")
        
        # Write all files with issues
        for issue in all_issues:
            if any([issue.get("unknown_country"), issue.get("unknown_owner_country"), 
                   issue.get("empty_topics"), issue.get("null_owner_link"),
                   issue.get("generic_owner_name"), issue.get("missing_identifiers"),
                   issue.get("missing_export_standard"), issue.get("missing_dates")]):
                rel_path = os.path.relpath(issue["file"])
                f.write(f"\n{rel_path}:\n")
                if issue.get("unknown_country"):
                    f.write("  - Unknown country in coverage\n")
                if issue.get("unknown_owner_country"):
                    f.write("  - Unknown country in owner\n")
                if issue.get("empty_topics"):
                    f.write("  - Empty topics\n")
                if issue.get("null_owner_link"):
                    f.write("  - Null owner.link\n")
                if issue.get("generic_owner_name"):
                    f.write("  - Generic owner.name\n")
                if issue.get("missing_identifiers"):
                    f.write("  - Missing identifiers\n")
                if issue.get("missing_export_standard"):
                    f.write("  - Missing export_standard\n")
                if issue.get("missing_dates"):
                    f.write("  - Missing dates\n")
    
    print(f"\n\nDetailed report saved to: {report_file}")

if __name__ == "__main__":
    main()

