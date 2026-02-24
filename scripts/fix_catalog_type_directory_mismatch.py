#!/usr/bin/env python3
"""
Fix CATALOG_TYPE_DIRECTORY_MISMATCH by moving files to the correct subdirectory.
Reads dataquality/full_report.jsonl and moves each file to match its catalog_type.
"""

import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
REPORT_FILE = BASE_DIR / "dataquality" / "full_report.jsonl"


def fix_one(rel_path: str, expected_subdir: str) -> tuple[bool, str]:
    """Move one YAML file to the correct subdir. Returns (success, message)."""
    full_path = ENTITIES_DIR / rel_path
    if not full_path.exists():
        return False, f"File not found: {rel_path}"

    parts = rel_path.replace("\\", "/").split("/")
    if len(parts) < 2:
        return False, f"Invalid path: {rel_path}"

    # Replace actual subdir (path_parts[-2]) with expected_subdir
    new_parts = parts[:-2] + [expected_subdir] + [parts[-1]]
    new_rel = "/".join(new_parts)
    new_full = ENTITIES_DIR / new_rel

    if new_full == full_path:
        return False, f"Already in correct dir: {rel_path}"

    if new_full.exists():
        return False, f"Target already exists: {new_rel}"

    new_full.parent.mkdir(parents=True, exist_ok=True)
    full_path.rename(new_full)
    return True, f"{rel_path} -> {new_rel}"


def main():
    if not REPORT_FILE.exists():
        print(f"Report not found: {REPORT_FILE}")
        print("Run: python scripts/builder.py analyze-quality")
        sys.exit(1)

    issues = []
    with open(REPORT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            if obj.get("issue_type") == "CATALOG_TYPE_DIRECTORY_MISMATCH":
                cv = obj.get("current_value", {})
                issues.append({
                    "file_path": obj.get("file_path", ""),
                    "expected_subdir": cv.get("expected_subdir", ""),
                })

    # Deduplicate by file_path (same file can appear in multiple country reports)
    seen = set()
    unique = []
    for i in issues:
        fp = i["file_path"]
        if fp and fp not in seen:
            seen.add(fp)
            unique.append(i)

    print(f"Found {len(unique)} CATALOG_TYPE_DIRECTORY_MISMATCH issues to fix.\n")

    fixed = 0
    errors = 0
    for item in unique:
        rel_path = item["file_path"]
        expected = item["expected_subdir"]
        if not expected:
            errors += 1
            print(f"  ✗ Missing expected_subdir: {rel_path}")
            continue
        ok, msg = fix_one(rel_path, expected)
        if ok:
            fixed += 1
            print(f"  ✓ {msg}")
        else:
            if "not found" in msg or "exists" in msg:
                errors += 1
            print(f"  - {msg}")

    print("\n" + "=" * 60)
    print(f"Fixed: {fixed}, Skipped/Errors: {len(unique) - fixed}")
    if fixed > 0:
        print("\nRun 'python scripts/builder.py analyze-quality' to verify.")


if __name__ == "__main__":
    main()
