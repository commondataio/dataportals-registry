#!/usr/bin/env python3
"""
Analyze YAML files in entities and scheduled directories for:
- Duplicate IDs, names, and UIDs
- Anomalous and strange names
- Naming pattern issues
"""

import os
import re
import yaml
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

# Paths
ENTITIES_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "entities")
SCHEDULED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "scheduled")

def is_anomalous_name(name: str) -> Tuple[bool, List[str]]:
    """Check if a name is anomalous and return reasons."""
    if not name or not isinstance(name, str):
        return True, ["Empty or non-string name"]
    
    issues = []
    
    # Very long names (>200 chars)
    if len(name) > 200:
        issues.append(f"Very long ({len(name)} chars)")
    
    # Very short names (<3 chars)
    if len(name) < 3:
        issues.append(f"Very short ({len(name)} chars)")
    
    # Contains unusual special characters (not common punctuation)
    unusual_chars = re.findall(r'[^\w\s\-.,()\[\]{}:;/&@#%$*+=\'"]', name)
    if unusual_chars:
        issues.append(f"Unusual characters: {set(unusual_chars)}")
    
    # Multiple consecutive spaces
    if '  ' in name:
        issues.append("Multiple consecutive spaces")
    
    # Leading/trailing whitespace
    if name != name.strip():
        issues.append("Leading/trailing whitespace")
    
    # Only numbers or special characters
    if re.match(r'^[\d\s\-_.,()]+$', name):
        issues.append("Only numbers and punctuation")
    
    # Contains control characters
    if any(ord(c) < 32 and c not in '\n\r\t' for c in name):
        issues.append("Contains control characters")
    
    # Looks like a URL or path
    if name.startswith(('http://', 'https://', 'www.', '/', './')):
        issues.append("Looks like URL or path")
    
    # Contains file extension
    if re.search(r'\.(yaml|yml|json|xml|html|pdf|txt)$', name, re.IGNORECASE):
        issues.append("Contains file extension")
    
    # All uppercase (might be OK, but could be anomaly)
    if name.isupper() and len(name) > 10:
        issues.append("All uppercase (long)")
    
    # Mixed case issues (e.g., all lowercase for proper nouns)
    words = name.split()
    if len(words) > 3 and all(w.islower() for w in words):
        issues.append("All lowercase (might need capitalization)")
    
    return len(issues) > 0, issues

def is_anomalous_id(id_value: str) -> Tuple[bool, List[str]]:
    """Check if an ID is anomalous."""
    if not id_value or not isinstance(id_value, str):
        return True, ["Empty or non-string ID"]
    
    issues = []
    
    # Contains spaces
    if ' ' in id_value:
        issues.append("Contains spaces")
    
    # Contains uppercase (IDs are usually lowercase)
    if any(c.isupper() for c in id_value):
        issues.append("Contains uppercase letters")
    
    # Contains unusual special characters
    if not re.match(r'^[a-z0-9\-_.]+$', id_value):
        unusual = re.findall(r'[^a-z0-9\-_.]', id_value)
        issues.append(f"Unusual characters: {set(unusual)}")
    
    # Very long IDs (>100 chars)
    if len(id_value) > 100:
        issues.append(f"Very long ({len(id_value)} chars)")
    
    # Very short IDs (<3 chars)
    if len(id_value) < 3:
        issues.append(f"Very short ({len(id_value)} chars)")
    
    # Starts or ends with special characters
    if id_value.startswith(('-', '_', '.')) or id_value.endswith(('-', '_', '.')):
        issues.append("Starts or ends with special character")
    
    # Multiple consecutive special characters
    if re.search(r'[-_.]{2,}', id_value):
        issues.append("Multiple consecutive special characters")
    
    return len(issues) > 0, issues

def analyze_file(filepath: str, dir_type: str) -> Dict:
    """Analyze a single YAML file."""
    try:
        with open(filepath, "r", encoding="utf8") as f:
            record = yaml.load(f, Loader=Loader)
        
        if not record:
            return None
        
        result = {
            "filepath": filepath,
            "dir_type": dir_type,
            "id": record.get("id"),
            "name": record.get("name"),
            "uid": record.get("uid"),
            "name_issues": [],
            "id_issues": [],
        }
        
        # Check name
        if "name" in record:
            is_anomalous, issues = is_anomalous_name(record["name"])
            if is_anomalous:
                result["name_issues"] = issues
        
        # Check ID
        if "id" in record:
            is_anomalous, issues = is_anomalous_id(record["id"])
            if is_anomalous:
                result["id_issues"] = issues
        
        return result
        
    except yaml.YAMLError as e:
        return {"filepath": filepath, "error": f"YAML error: {str(e)}"}
    except Exception as e:
        return {"filepath": filepath, "error": f"Error: {str(e)}"}

def analyze_all_files():
    """Analyze all YAML files in entities and scheduled directories."""
    
    # Data structures for tracking duplicates
    id_to_files: Dict[str, List[str]] = defaultdict(list)
    name_to_files: Dict[str, List[str]] = defaultdict(list)
    uid_to_files: Dict[str, List[str]] = defaultdict(list)
    
    # Data structures for anomalies
    anomalous_names: List[Dict] = []
    anomalous_ids: List[Dict] = []
    errors: List[Dict] = []
    
    total_files = 0
    
    # Process entities directory
    print("Analyzing entities directory...")
    for root, dirs, files in os.walk(ENTITIES_DIR):
        for file in files:
            if file.endswith(('.yaml', '.yml')):
                total_files += 1
                filepath = os.path.join(root, file)
                result = analyze_file(filepath, "entities")
                
                if result and "error" in result:
                    errors.append(result)
                    continue
                
                if result:
                    # Track duplicates
                    if result.get("id"):
                        id_to_files[result["id"]].append(filepath)
                    if result.get("name"):
                        name_to_files[result["name"]].append(filepath)
                    if result.get("uid"):
                        uid_to_files[result["uid"]].append(filepath)
                    
                    # Track anomalies
                    if result.get("name_issues"):
                        anomalous_names.append(result)
                    if result.get("id_issues"):
                        anomalous_ids.append(result)
    
    # Process scheduled directory
    print("Analyzing scheduled directory...")
    for root, dirs, files in os.walk(SCHEDULED_DIR):
        for file in files:
            if file.endswith(('.yaml', '.yml')):
                total_files += 1
                filepath = os.path.join(root, file)
                result = analyze_file(filepath, "scheduled")
                
                if result and "error" in result:
                    errors.append(result)
                    continue
                
                if result:
                    # Track duplicates
                    if result.get("id"):
                        id_to_files[result["id"]].append(filepath)
                    if result.get("name"):
                        name_to_files[result["name"]].append(filepath)
                    if result.get("uid"):
                        uid_to_files[result["uid"]].append(filepath)
                    
                    # Track anomalies
                    if result.get("name_issues"):
                        anomalous_names.append(result)
                    if result.get("id_issues"):
                        anomalous_ids.append(result)
    
    # Filter duplicates (only keep those with >1 occurrence)
    duplicate_ids = {k: v for k, v in id_to_files.items() if len(v) > 1}
    duplicate_names = {k: v for k, v in name_to_files.items() if len(v) > 1}
    duplicate_uids = {k: v for k, v in uid_to_files.items() if len(v) > 1}
    
    return {
        "total_files": total_files,
        "duplicate_ids": duplicate_ids,
        "duplicate_names": duplicate_names,
        "duplicate_uids": duplicate_uids,
        "anomalous_names": anomalous_names,
        "anomalous_ids": anomalous_ids,
        "errors": errors,
    }

def generate_report(results: Dict) -> str:
    """Generate a comprehensive report."""
    report = []
    report.append("=" * 80)
    report.append("NAMING ISSUES ANALYSIS REPORT")
    report.append("=" * 80)
    report.append(f"\nTotal files analyzed: {results['total_files']}")
    report.append("\n")
    
    # Duplicate IDs
    report.append("=" * 80)
    report.append("DUPLICATE IDs")
    report.append("=" * 80)
    if results["duplicate_ids"]:
        report.append(f"\nFound {len(results['duplicate_ids'])} duplicate IDs:\n")
        for id_value, files in sorted(results["duplicate_ids"].items(), key=lambda x: len(x[1]), reverse=True):
            report.append(f"\nID: '{id_value}' ({len(files)} occurrences)")
            for filepath in files:
                report.append(f"  - {filepath}")
    else:
        report.append("\nNo duplicate IDs found.")
    
    # Duplicate Names
    report.append("\n" + "=" * 80)
    report.append("DUPLICATE NAMES")
    report.append("=" * 80)
    if results["duplicate_names"]:
        report.append(f"\nFound {len(results['duplicate_names'])} duplicate names:\n")
        for name, files in sorted(results["duplicate_names"].items(), key=lambda x: len(x[1]), reverse=True):
            report.append(f"\nName: '{name}' ({len(files)} occurrences)")
            for filepath in files:
                report.append(f"  - {filepath}")
    else:
        report.append("\nNo duplicate names found.")
    
    # Duplicate UIDs
    report.append("\n" + "=" * 80)
    report.append("DUPLICATE UIDs")
    report.append("=" * 80)
    if results["duplicate_uids"]:
        report.append(f"\nFound {len(results['duplicate_uids'])} duplicate UIDs:\n")
        for uid, files in sorted(results["duplicate_uids"].items(), key=lambda x: len(x[1]), reverse=True):
            report.append(f"\nUID: '{uid}' ({len(files)} occurrences)")
            for filepath in files:
                report.append(f"  - {filepath}")
    else:
        report.append("\nNo duplicate UIDs found.")
    
    # Anomalous Names
    report.append("\n" + "=" * 80)
    report.append("ANOMALOUS NAMES")
    report.append("=" * 80)
    if results["anomalous_names"]:
        report.append(f"\nFound {len(results['anomalous_names'])} files with anomalous names:\n")
        for item in results["anomalous_names"][:50]:  # Limit to first 50
            report.append(f"\nFile: {item['filepath']}")
            report.append(f"  Name: '{item['name']}'")
            report.append(f"  Issues: {', '.join(item['name_issues'])}")
        if len(results["anomalous_names"]) > 50:
            report.append(f"\n... and {len(results['anomalous_names']) - 50} more")
    else:
        report.append("\nNo anomalous names found.")
    
    # Anomalous IDs
    report.append("\n" + "=" * 80)
    report.append("ANOMALOUS IDs")
    report.append("=" * 80)
    if results["anomalous_ids"]:
        report.append(f"\nFound {len(results['anomalous_ids'])} files with anomalous IDs:\n")
        for item in results["anomalous_ids"][:50]:  # Limit to first 50
            report.append(f"\nFile: {item['filepath']}")
            report.append(f"  ID: '{item['id']}'")
            report.append(f"  Issues: {', '.join(item['id_issues'])}")
        if len(results["anomalous_ids"]) > 50:
            report.append(f"\n... and {len(results['anomalous_ids']) - 50} more")
    else:
        report.append("\nNo anomalous IDs found.")
    
    # Errors
    if results["errors"]:
        report.append("\n" + "=" * 80)
        report.append("ERRORS")
        report.append("=" * 80)
        report.append(f"\nFound {len(results['errors'])} files with errors:\n")
        for error in results["errors"][:20]:  # Limit to first 20
            report.append(f"  - {error['filepath']}: {error['error']}")
        if len(results["errors"]) > 20:
            report.append(f"\n... and {len(results['errors']) - 20} more")
    
    return "\n".join(report)

def generate_fix_guide(results: Dict) -> str:
    """Generate a guide on how to fix the issues."""
    guide = []
    guide.append("=" * 80)
    guide.append("HOW TO FIX NAMING ISSUES - GUIDE")
    guide.append("=" * 80)
    guide.append("\n")
    
    guide.append("1. DUPLICATE IDs")
    guide.append("-" * 80)
    guide.append("Problem: Multiple files share the same 'id' field value.")
    guide.append("Impact: IDs should be unique identifiers for each catalog.")
    guide.append("\nHow to fix:")
    guide.append("  a. Review each duplicate ID case")
    guide.append("  b. Determine if they represent the same catalog (merge) or different ones")
    guide.append("  c. If same catalog: Keep one file, remove others or merge their content")
    guide.append("  d. If different catalogs: Make IDs unique by:")
    guide.append("     - Adding location suffix (e.g., 'datagov-us', 'datagov-uk')")
    guide.append("     - Adding type suffix (e.g., 'datagov-opendata', 'datagov-geo')")
    guide.append("     - Using more specific domain-based IDs")
    guide.append("  e. Update the 'id' field in the YAML file")
    guide.append("\nExample fix:")
    guide.append("  Before: id: datagov")
    guide.append("  After:  id: datagovus (for US) or datagovuk (for UK)")
    guide.append("\n")
    
    guide.append("2. DUPLICATE NAMES")
    guide.append("-" * 80)
    guide.append("Problem: Multiple files share the same 'name' field value.")
    guide.append("Impact: Names can legitimately be the same, but duplicates might indicate:")
    guide.append("  - Same catalog listed multiple times")
    guide.append("  - Missing distinguishing information in the name")
    guide.append("\nHow to fix:")
    guide.append("  a. Review if the catalogs are actually the same")
    guide.append("  b. If same: Merge or remove duplicates")
    guide.append("  c. If different: Add distinguishing information:")
    guide.append("     - Add location: 'Data.gov (United States)' vs 'Data.gov (United Kingdom)'")
    guide.append("     - Add organization: 'Open Data Portal (City of Boston)'")
    guide.append("     - Add type: 'Geoportal (Federal)' vs 'Geoportal (State)'")
    guide.append("  d. Update the 'name' field in the YAML file")
    guide.append("\n")
    
    guide.append("3. DUPLICATE UIDs")
    guide.append("-" * 80)
    guide.append("Problem: Multiple files share the same 'uid' field value.")
    guide.append("Impact: UIDs should be unique identifiers (likely from an external system).")
    guide.append("\nHow to fix:")
    guide.append("  a. Check if UIDs come from an external system (e.g., CDI)")
    guide.append("  b. If UIDs are from external system: Contact system administrator")
    guide.append("  c. If UIDs are generated locally: Generate new unique UIDs")
    guide.append("  d. UID format should be consistent (e.g., 'cdi00000914')")
    guide.append("  e. Update the 'uid' field in the YAML file")
    guide.append("\n")
    
    guide.append("4. ANOMALOUS NAMES")
    guide.append("-" * 80)
    guide.append("Problem: Names contain unusual patterns, characters, or formatting issues.")
    guide.append("\nHow to fix by issue type:")
    guide.append("\n  a. Very long names (>200 chars):")
    guide.append("     - Shorten to essential information")
    guide.append("     - Move detailed description to 'description' field")
    guide.append("\n  b. Very short names (<3 chars):")
    guide.append("     - Expand with more descriptive information")
    guide.append("\n  c. Unusual characters:")
    guide.append("     - Remove or replace with standard characters")
    guide.append("     - Use Unicode normalization if needed")
    guide.append("\n  d. Multiple consecutive spaces:")
    guide.append("     - Replace with single space")
    guide.append("     - Use: name.replace('  ', ' ')")
    guide.append("\n  e. Leading/trailing whitespace:")
    guide.append("     - Use: name.strip()")
    guide.append("\n  f. Looks like URL or path:")
    guide.append("     - Extract actual catalog name from URL")
    guide.append("     - Use domain name or organization name instead")
    guide.append("\n  g. Contains file extension:")
    guide.append("     - Remove file extension from name")
    guide.append("\n  h. All uppercase (long):")
    guide.append("     - Convert to title case or sentence case")
    guide.append("     - Use: name.title() or name.capitalize()")
    guide.append("\n  i. All lowercase (might need capitalization):")
    guide.append("     - Apply proper capitalization")
    guide.append("     - Use: name.title() for title case")
    guide.append("\n")
    
    guide.append("5. ANOMALOUS IDs")
    guide.append("-" * 80)
    guide.append("Problem: IDs contain unusual patterns or don't follow conventions.")
    guide.append("\nHow to fix by issue type:")
    guide.append("\n  a. Contains spaces:")
    guide.append("     - Replace spaces with hyphens or underscores")
    guide.append("     - Use: id.replace(' ', '-')")
    guide.append("\n  b. Contains uppercase letters:")
    guide.append("     - Convert to lowercase")
    guide.append("     - Use: id.lower()")
    guide.append("\n  c. Unusual characters:")
    guide.append("     - Remove or replace with allowed characters (a-z, 0-9, -, _, .)")
    guide.append("     - Use: re.sub(r'[^a-z0-9\\-_.]', '', id.lower())")
    guide.append("\n  d. Very long IDs (>100 chars):")
    guide.append("     - Shorten while keeping uniqueness")
    guide.append("     - Use domain name or key words")
    guide.append("\n  e. Very short IDs (<3 chars):")
    guide.append("     - Expand with more context")
    guide.append("\n  f. Starts/ends with special characters:")
    guide.append("     - Remove leading/trailing special characters")
    guide.append("     - Use: id.strip('-_.')")
    guide.append("\n  g. Multiple consecutive special characters:")
    guide.append("     - Replace with single character")
    guide.append("     - Use: re.sub(r'[-_.]{2,}', '-', id)")
    guide.append("\n")
    
    guide.append("6. GENERAL BEST PRACTICES")
    guide.append("-" * 80)
    guide.append("  - IDs should be: lowercase, alphanumeric with hyphens/underscores, unique")
    guide.append("  - Names should be: descriptive, properly capitalized, no special formatting")
    guide.append("  - UIDs should be: unique, follow external system format if applicable")
    guide.append("  - Always validate YAML syntax after making changes")
    guide.append("  - Test that the file loads correctly: python -c \"import yaml; yaml.safe_load(open('file.yaml'))\"")
    guide.append("\n")
    
    guide.append("7. AUTOMATED FIXING SCRIPT")
    guide.append("-" * 80)
    guide.append("You can create a script to automatically fix common issues:")
    guide.append("  - Trim whitespace")
    guide.append("  - Normalize case")
    guide.append("  - Remove unusual characters")
    guide.append("  - Fix consecutive spaces/special characters")
    guide.append("\nHowever, manual review is recommended for:")
    guide.append("  - Duplicate IDs/names/UIDs (requires domain knowledge)")
    guide.append("  - Very long names (requires judgment on what to keep)")
    guide.append("  - Names that look like URLs (requires extracting proper name)")
    guide.append("\n")
    
    return "\n".join(guide)

if __name__ == "__main__":
    print("Starting analysis...")
    results = analyze_all_files()
    
    print("\nGenerating report...")
    report = generate_report(results)
    
    print("\nGenerating fix guide...")
    guide = generate_fix_guide(results)
    
    # Write reports to files
    report_file = os.path.join(os.path.dirname(__file__), "..", "naming_issues_report.txt")
    guide_file = os.path.join(os.path.dirname(__file__), "..", "naming_issues_fix_guide.txt")
    
    with open(report_file, "w", encoding="utf8") as f:
        f.write(report)
    
    with open(guide_file, "w", encoding="utf8") as f:
        f.write(guide)
    
    print(f"\nReport written to: {report_file}")
    print(f"Fix guide written to: {guide_file}")
    print("\nSummary:")
    print(f"  Total files: {results['total_files']}")
    print(f"  Duplicate IDs: {len(results['duplicate_ids'])}")
    print(f"  Duplicate names: {len(results['duplicate_names'])}")
    print(f"  Duplicate UIDs: {len(results['duplicate_uids'])}")
    print(f"  Anomalous names: {len(results['anomalous_names'])}")
    print(f"  Anomalous IDs: {len(results['anomalous_ids'])}")
    print(f"  Errors: {len(results['errors'])}")

