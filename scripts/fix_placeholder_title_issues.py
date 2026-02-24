#!/usr/bin/env python3
"""
Fix PLACEHOLDER_TITLE issues from dataquality/rules/PLACEHOLDER_TITLE.txt.

Parses the report, extracts unique file paths, and applies fix_placeholder_title
to replace URL/domain-based titles with human-readable names.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

# Allow importing from scripts when run from repo root
sys.path.insert(0, str(Path(__file__).parent))
from fix_unknown_issues import apply_issue_fix

BASE_DIR = Path(__file__).parent.parent
PLACEHOLDER_TITLE_REPORT = BASE_DIR / "dataquality" / "rules" / "PLACEHOLDER_TITLE.txt"
PRIMARY_PRIORITY_FILE = BASE_DIR / "dataquality" / "primary_priority.jsonl"


def parse_placeholder_title_report(path: Path) -> set[str]:
    """Parse PLACEHOLDER_TITLE.txt and extract unique file paths."""
    if not path.exists():
        return set()
    text = path.read_text(encoding="utf-8")
    pattern = r"File: ([^\n]+)\nRecord ID: [^\n]+\nCountry: [^\n]+\nIssue: PLACEHOLDER_TITLE"
    return {m.group(1).strip() for m in re.finditer(pattern, text)}


def parse_primary_priority_for_placeholder_title(path: Path) -> set[str]:
    """Parse primary_priority.jsonl for records with PLACEHOLDER_TITLE issues."""
    files = set()
    if not path.exists():
        return files
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            file_path = (obj.get("file_path") or "").strip()
            if not file_path:
                continue
            for issue in obj.get("issues", []):
                if (issue or {}).get("issue_type") == "PLACEHOLDER_TITLE":
                    files.add(file_path)
                    break
    return files


def main() -> None:
    files = parse_placeholder_title_report(PLACEHOLDER_TITLE_REPORT)
    files |= parse_primary_priority_for_placeholder_title(PRIMARY_PRIORITY_FILE)
    files = sorted(files)

    if not files:
        print("No PLACEHOLDER_TITLE issues found.")
        return

    print(f"Found {len(files)} unique files with PLACEHOLDER_TITLE issues")
    fixed = 0
    skipped = 0
    for file_path in files:
        if apply_issue_fix(file_path, "PLACEHOLDER_TITLE"):
            fixed += 1
            print(f"  Fixed: {file_path}")
        else:
            skipped += 1

    print(f"\nTotal fixed: {fixed}")
    print(f"Total skipped: {skipped}")


if __name__ == "__main__":
    main()
