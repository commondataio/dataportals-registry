#!/usr/bin/env python3
"""
Fix OWNER_SUBREGION_FEDERAL_DIRECTORY_MISMATCH issues.

Records with owner.location.subregion set but stored in the Federal directory
are moved to the correct subregion directory (e.g., US-CA, US-TX).

Usage:
  python scripts/fix_owner_subregion_federal_directory_mismatch.py [--dry-run]
"""

from __future__ import annotations

import argparse
import ast
import re
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
RULE_REPORT = BASE_DIR / "dataquality" / "rules" / "OWNER_SUBREGION_FEDERAL_DIRECTORY_MISMATCH.txt"


def parse_report(report_path: Path) -> list[tuple[str, str]]:
    """Parse report and return [(file_path, owner_subregion_id), ...]."""
    if not report_path.exists():
        return []

    text = report_path.read_text(encoding="utf-8")
    # Match: File: path\n...\nCurrent Value: {'owner_subregion_id': 'XX-YY', ...}
    pattern = (
        r"File: ([^\n]+)\n"
        r"Record ID: [^\n]+\n"
        r"Country: [^\n]+\n"
        r"Issue: OWNER_SUBREGION_FEDERAL_DIRECTORY_MISMATCH\n"
        r"Field: [^\n]+\n"
        r"Current Value: ([^\n]+)"
    )
    results: list[tuple[str, str]] = []
    for match in re.finditer(pattern, text):
        file_path = match.group(1).strip()
        current_value_str = match.group(2).strip()

        # Parse the dict-like string to extract owner_subregion_id
        owner_subregion_id = None
        try:
            # Handle single quotes in dict repr
            value = ast.literal_eval(current_value_str.replace("'", '"'))
            if isinstance(value, dict):
                owner_subregion_id = value.get("owner_subregion_id")
        except (ValueError, SyntaxError):
            # Fallback: regex for owner_subregion_id
            m = re.search(r"'owner_subregion_id':\s*'([^']+)'", current_value_str)
            if m:
                owner_subregion_id = m.group(1)

        if file_path and owner_subregion_id:
            results.append((file_path, owner_subregion_id))

    return results


def move_file(src_relative: str, subregion_id: str, dry_run: bool) -> bool:
    """Move file from Federal to subregion directory. Returns True if moved."""
    src = ENTITIES_DIR / src_relative
    if not src.exists():
        print(f"  Skip (not found): {src_relative}")
        return False

    # Replace Federal with subregion_id in path
    parts = src_relative.split("/")
    if len(parts) < 4 or parts[1] != "Federal":
        print(f"  Skip (invalid path): {src_relative}")
        return False

    # For EU/Federal files, destination country comes from subregion_id (e.g. FR-GES -> FR)
    if parts[0] == "EU" and "-" in subregion_id:
        dst_country = subregion_id.split("-")[0]
        dst_parts = [dst_country, subregion_id] + parts[2:]
    else:
        dst_parts = [parts[0], subregion_id] + parts[2:]
    dst_relative = "/".join(dst_parts)
    dst = ENTITIES_DIR / dst_relative

    if src == dst:
        return False

    if dst.exists():
        print(f"  Skip (destination exists): {dst_relative}")
        return False

    if dry_run:
        print(f"  Would move: {src_relative} -> {dst_relative}")
        return True

    dst.parent.mkdir(parents=True, exist_ok=True)
    src.rename(dst)
    print(f"  Moved: {src_relative} -> {dst_relative}")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Fix OWNER_SUBREGION_FEDERAL_DIRECTORY_MISMATCH")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without moving")
    args = parser.parse_args()

    if not RULE_REPORT.exists():
        print(f"Report not found: {RULE_REPORT}")
        return

    entries = parse_report(RULE_REPORT)
    print(f"Found {len(entries)} issues to fix")
    if args.dry_run:
        print("(dry-run mode - no files will be moved)\n")

    moved = 0
    for file_path, subregion_id in entries:
        if move_file(file_path, subregion_id, dry_run=args.dry_run):
            moved += 1

    print(f"\n{'Would move' if args.dry_run else 'Moved'}: {moved} files")


if __name__ == "__main__":
    main()
