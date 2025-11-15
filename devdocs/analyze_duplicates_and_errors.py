#!/usr/bin/env python
"""Analyze all YAML records for duplicate uid's, id's, and critical errors"""

import os
import sys
import json
import yaml
from collections import defaultdict
from pathlib import Path

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

# Root directory
ROOT_DIR = os.path.join(os.path.dirname(__file__), "..")
DATA_DIR = os.path.join(ROOT_DIR, "data")

# Directories to analyze
DIRECTORIES = ["entities", "scheduled", "software"]


def analyze_all_records():
    """Analyze all YAML files for duplicates and errors"""
    
    # Trackers
    uid_to_files = defaultdict(list)
    id_to_files = defaultdict(list)
    errors = []
    empty_files = []
    parsing_errors = []
    missing_required = []
    empty_required = []
    filename_mismatches = []
    
    total_files = 0
    processed_files = 0
    
    print("Scanning YAML files...")
    
    for directory in DIRECTORIES:
        dir_path = os.path.join(DATA_DIR, directory)
        if not os.path.exists(dir_path):
            print(f"Warning: Directory {dir_path} does not exist")
            continue
            
        print(f"\nProcessing {directory}/...")
        
        for root, dirs, files in os.walk(dir_path):
            yaml_files = [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
            
            for filepath in yaml_files:
                total_files += 1
                rel_path = os.path.relpath(filepath, ROOT_DIR)
                
                try:
                    with open(filepath, "r", encoding="utf8") as f:
                        content = f.read().strip()
                        
                    if not content:
                        empty_files.append(rel_path)
                        continue
                    
                    record = yaml.load(content, Loader=Loader)
                    
                    if record is None:
                        empty_files.append(rel_path)
                        continue
                    
                    processed_files += 1
                    
                    # Extract uid and id
                    uid = record.get("uid")
                    record_id = record.get("id")
                    
                    # Track uid duplicates
                    if uid:
                        uid_to_files[uid].append(rel_path)
                    else:
                        missing_required.append({
                            "file": rel_path,
                            "field": "uid",
                            "type": directory
                        })
                    
                    # Track id duplicates
                    if record_id:
                        id_to_files[record_id].append(rel_path)
                    else:
                        missing_required.append({
                            "file": rel_path,
                            "field": "id",
                            "type": directory
                        })
                    
                    # Check for empty required fields
                    if uid == "":
                        empty_required.append({
                            "file": rel_path,
                            "field": "uid",
                            "type": directory
                        })
                    if record_id == "":
                        empty_required.append({
                            "file": rel_path,
                            "field": "id",
                            "type": directory
                        })
                    
                    # Check filename matches id (for entities and scheduled)
                    if directory in ["entities", "scheduled"]:
                        filename = os.path.splitext(os.path.basename(filepath))[0]
                        if record_id and record_id != filename:
                            filename_mismatches.append({
                                "file": rel_path,
                                "id": record_id,
                                "filename": filename
                            })
                    
                    # Check other critical required fields based on schema
                    if directory in ["entities", "scheduled"]:
                        required_fields = {
                            "name": "name",
                            "link": "link",
                            "status": "status",
                            "catalog_type": "catalog_type",
                            "access_mode": "access_mode",
                            "software": "software",
                            "owner": "owner"
                        }
                        
                        for field_key, field_name in required_fields.items():
                            if field_key not in record:
                                missing_required.append({
                                    "file": rel_path,
                                    "field": field_name,
                                    "type": directory
                                })
                            elif record[field_key] == "" or record[field_key] is None:
                                if field_key == "access_mode" and isinstance(record[field_key], list):
                                    if not record[field_key]:
                                        empty_required.append({
                                            "file": rel_path,
                                            "field": field_name,
                                            "type": directory
                                        })
                                elif field_key not in ["software", "owner"]:  # These are dicts, check separately
                                    empty_required.append({
                                        "file": rel_path,
                                        "field": field_name,
                                        "type": directory
                                    })
                    
                    elif directory == "software":
                        required_fields = {
                            "name": "name",
                            "type": "type",
                            "category": "category",
                            "datatypes": "datatypes",
                            "has_api": "has_api",
                            "has_bulk": "has_bulk",
                            "metadata_support": "metadata_support",
                            "pid_support": "pid_support",
                            "rights_management": "rights_management",
                            "storage_type": "storage_type"
                        }
                        
                        for field_key, field_name in required_fields.items():
                            if field_key not in record:
                                missing_required.append({
                                    "file": rel_path,
                                    "field": field_name,
                                    "type": directory
                                })
                
                except yaml.YAMLError as e:
                    parsing_errors.append({
                        "file": rel_path,
                        "error": str(e)
                    })
                except Exception as e:
                    errors.append({
                        "file": rel_path,
                        "error": str(e)
                    })
    
    # Find duplicates
    duplicate_uids = {uid: files for uid, files in uid_to_files.items() if len(files) > 1}
    duplicate_ids = {record_id: files for record_id, files in id_to_files.items() if len(files) > 1}
    
    # Generate report
    report = {
        "summary": {
            "total_files": total_files,
            "processed_files": processed_files,
            "empty_files": len(empty_files),
            "duplicate_uids": len(duplicate_uids),
            "duplicate_ids": len(duplicate_ids),
            "parsing_errors": len(parsing_errors),
            "other_errors": len(errors),
            "missing_required_fields": len(missing_required),
            "empty_required_fields": len(empty_required),
            "filename_mismatches": len(filename_mismatches)
        },
        "duplicate_uids": duplicate_uids,
        "duplicate_ids": duplicate_ids,
        "empty_files": empty_files,
        "parsing_errors": parsing_errors,
        "other_errors": errors,
        "missing_required_fields": missing_required,
        "empty_required_fields": empty_required,
        "filename_mismatches": filename_mismatches
    }
    
    return report


def write_report(report, output_dir):
    """Write analysis report to files"""
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Write JSON report
    json_path = os.path.join(output_dir, "duplicates_and_errors_report.json")
    with open(json_path, "w", encoding="utf8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\nJSON report written to: {json_path}")
    
    # Write markdown report
    md_path = os.path.join(output_dir, "duplicates_and_errors_report.md")
    with open(md_path, "w", encoding="utf8") as f:
        f.write("# Duplicate UID's, ID's and Critical Errors Analysis\n\n")
        f.write(f"Generated: {Path(__file__).stat().st_mtime}\n\n")
        
        # Summary
        f.write("## Summary\n\n")
        summary = report["summary"]
        f.write(f"- **Total files scanned**: {summary['total_files']}\n")
        f.write(f"- **Successfully processed**: {summary['processed_files']}\n")
        f.write(f"- **Empty files**: {summary['empty_files']}\n")
        f.write(f"- **Duplicate UID's**: {summary['duplicate_uids']}\n")
        f.write(f"- **Duplicate ID's**: {summary['duplicate_ids']}\n")
        f.write(f"- **YAML parsing errors**: {summary['parsing_errors']}\n")
        f.write(f"- **Other errors**: {summary['other_errors']}\n")
        f.write(f"- **Missing required fields**: {summary['missing_required_fields']}\n")
        f.write(f"- **Empty required fields**: {summary['empty_required_fields']}\n")
        f.write(f"- **Filename mismatches**: {summary['filename_mismatches']}\n\n")
        
        # Duplicate UIDs
        if report["duplicate_uids"]:
            f.write("## Duplicate UID's\n\n")
            f.write(f"Found **{len(report['duplicate_uids'])}** duplicate UID's:\n\n")
            for uid, files in sorted(report["duplicate_uids"].items()):
                f.write(f"### UID: `{uid}`\n\n")
                f.write(f"Found in {len(files)} file(s):\n\n")
                for file in files:
                    f.write(f"- `{file}`\n")
                f.write("\n")
        else:
            f.write("## Duplicate UID's\n\n")
            f.write("✓ No duplicate UID's found.\n\n")
        
        # Duplicate IDs
        if report["duplicate_ids"]:
            f.write("## Duplicate ID's\n\n")
            f.write(f"Found **{len(report['duplicate_ids'])}** duplicate ID's:\n\n")
            for record_id, files in sorted(report["duplicate_ids"].items()):
                f.write(f"### ID: `{record_id}`\n\n")
                f.write(f"Found in {len(files)} file(s):\n\n")
                for file in files:
                    f.write(f"- `{file}`\n")
                f.write("\n")
        else:
            f.write("## Duplicate ID's\n\n")
            f.write("✓ No duplicate ID's found.\n\n")
        
        # Empty files
        if report["empty_files"]:
            f.write("## Empty Files\n\n")
            f.write(f"Found **{len(report['empty_files'])}** empty files:\n\n")
            for file in report["empty_files"]:
                f.write(f"- `{file}`\n")
            f.write("\n")
        
        # Parsing errors
        if report["parsing_errors"]:
            f.write("## YAML Parsing Errors\n\n")
            f.write(f"Found **{len(report['parsing_errors'])}** files with YAML parsing errors:\n\n")
            for item in report["parsing_errors"]:
                f.write(f"### `{item['file']}`\n\n")
                f.write(f"Error: {item['error']}\n\n")
        
        # Other errors
        if report["other_errors"]:
            f.write("## Other Errors\n\n")
            f.write(f"Found **{len(report['other_errors'])}** files with other errors:\n\n")
            for item in report["other_errors"]:
                f.write(f"### `{item['file']}`\n\n")
                f.write(f"Error: {item['error']}\n\n")
        
        # Missing required fields
        if report["missing_required_fields"]:
            f.write("## Missing Required Fields\n\n")
            f.write(f"Found **{len(report['missing_required_fields'])}** records with missing required fields:\n\n")
            
            # Group by field
            by_field = defaultdict(list)
            for item in report["missing_required_fields"]:
                by_field[item["field"]].append(item)
            
            for field, items in sorted(by_field.items()):
                f.write(f"### Missing `{field}` ({len(items)} records)\n\n")
                for item in items[:20]:  # Show first 20
                    f.write(f"- `{item['file']}` (type: {item['type']})\n")
                if len(items) > 20:
                    f.write(f"- ... and {len(items) - 20} more\n")
                f.write("\n")
        
        # Empty required fields
        if report["empty_required_fields"]:
            f.write("## Empty Required Fields\n\n")
            f.write(f"Found **{len(report['empty_required_fields'])}** records with empty required fields:\n\n")
            
            # Group by field
            by_field = defaultdict(list)
            for item in report["empty_required_fields"]:
                by_field[item["field"]].append(item)
            
            for field, items in sorted(by_field.items()):
                f.write(f"### Empty `{field}` ({len(items)} records)\n\n")
                for item in items[:20]:  # Show first 20
                    f.write(f"- `{item['file']}` (type: {item['type']})\n")
                if len(items) > 20:
                    f.write(f"- ... and {len(items) - 20} more\n")
                f.write("\n")
        
        # Filename mismatches
        if report["filename_mismatches"]:
            f.write("## Filename Mismatches\n\n")
            f.write(f"Found **{len(report['filename_mismatches'])}** files where the `id` field doesn't match the filename:\n\n")
            for item in report["filename_mismatches"][:50]:  # Show first 50
                f.write(f"- `{item['file']}`\n")
                f.write(f"  - Filename: `{item['filename']}`\n")
                f.write(f"  - ID field: `{item['id']}`\n")
            if len(report["filename_mismatches"]) > 50:
                f.write(f"\n... and {len(report['filename_mismatches']) - 50} more\n")
            f.write("\n")
    
    print(f"Markdown report written to: {md_path}")
    
    # Write summary text file
    txt_path = os.path.join(output_dir, "duplicates_and_errors_summary.txt")
    with open(txt_path, "w", encoding="utf8") as f:
        f.write("DUPLICATE UID'S, ID'S AND CRITICAL ERRORS ANALYSIS\n")
        f.write("=" * 60 + "\n\n")
        
        summary = report["summary"]
        f.write(f"Total files scanned: {summary['total_files']}\n")
        f.write(f"Successfully processed: {summary['processed_files']}\n")
        f.write(f"Empty files: {summary['empty_files']}\n")
        f.write(f"Duplicate UID's: {summary['duplicate_uids']}\n")
        f.write(f"Duplicate ID's: {summary['duplicate_ids']}\n")
        f.write(f"YAML parsing errors: {summary['parsing_errors']}\n")
        f.write(f"Other errors: {summary['other_errors']}\n")
        f.write(f"Missing required fields: {summary['missing_required_fields']}\n")
        f.write(f"Empty required fields: {summary['empty_required_fields']}\n")
        f.write(f"Filename mismatches: {summary['filename_mismatches']}\n\n")
        
        if report["duplicate_uids"]:
            f.write("\nDUPLICATE UID'S:\n")
            f.write("-" * 60 + "\n")
            for uid, files in sorted(report["duplicate_uids"].items()):
                f.write(f"\nUID: {uid} ({len(files)} files)\n")
                for file in files:
                    f.write(f"  - {file}\n")
        
        if report["duplicate_ids"]:
            f.write("\n\nDUPLICATE ID'S:\n")
            f.write("-" * 60 + "\n")
            for record_id, files in sorted(report["duplicate_ids"].items()):
                f.write(f"\nID: {record_id} ({len(files)} files)\n")
                for file in files:
                    f.write(f"  - {file}\n")
    
    print(f"Summary text file written to: {txt_path}")


if __name__ == "__main__":
    print("Starting analysis of all YAML records...")
    report = analyze_all_records()
    
    output_dir = os.path.join(ROOT_DIR, "devdocs")
    write_report(report, output_dir)
    
    # Print summary to console
    print("\n" + "=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)
    summary = report["summary"]
    print(f"\nTotal files scanned: {summary['total_files']}")
    print(f"Successfully processed: {summary['processed_files']}")
    print(f"\nIssues found:")
    print(f"  - Duplicate UID's: {summary['duplicate_uids']}")
    print(f"  - Duplicate ID's: {summary['duplicate_ids']}")
    print(f"  - Empty files: {summary['empty_files']}")
    print(f"  - YAML parsing errors: {summary['parsing_errors']}")
    print(f"  - Other errors: {summary['other_errors']}")
    print(f"  - Missing required fields: {summary['missing_required_fields']}")
    print(f"  - Empty required fields: {summary['empty_required_fields']}")
    print(f"  - Filename mismatches: {summary['filename_mismatches']}")
    print("\nDetailed reports written to devdocs/ directory")

