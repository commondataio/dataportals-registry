#!/usr/bin/env python3
"""
Fix SUBREGION_NAME_ID_MISMATCH issues: update subregion names to match
canonical ISO 3166-2 subdivision names.
"""
import json
import re
import yaml
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
SCHEDULED_DIR = BASE_DIR / "data" / "scheduled"
REPORT_FILE = BASE_DIR / "dataquality" / "rules" / "SUBREGION_NAME_ID_MISMATCH.txt"


def parse_report(report_path: Path):
    """Parse SUBREGION_NAME_ID_MISMATCH.txt and yield (file_path, field, expected)."""
    pattern = re.compile(
        r"^File: ([^\n]+)\n"
        r"Record ID: [^\n]+\n"
        r"Country: [^\n]+\n"
        r"Issue: SUBREGION_NAME_ID_MISMATCH\n"
        r"Field: ([^\n]+)\n"
        r"Current Value: (\{[^}]+\})",
        re.MULTILINE,
    )
    content = report_path.read_text(encoding="utf-8")
    for m in pattern.finditer(content):
        file_path = m.group(1).strip()
        field = m.group(2).strip()
        try:
            current_value = json.loads(m.group(3))
            expected = current_value.get("expected")
            if expected:
                yield file_path, field, expected
        except json.JSONDecodeError:
            continue


def fix_yaml_file(file_path: str, updates: list[tuple[str, str]]) -> bool:
    """Apply subregion name updates to a YAML file. Returns True if modified."""
    for base in (ENTITIES_DIR, SCHEDULED_DIR):
        full_path = base / file_path
        if full_path.exists():
            break
    else:
        return False

    with open(full_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not data:
        return False

    modified = False
    for field, expected in updates:
        if field == "owner.location.subregion.name":
            owner = data.get("owner") or {}
            loc = owner.get("location") or {}
            sr = loc.get("subregion")
            if sr and isinstance(sr, dict) and sr.get("name") != expected:
                sr["name"] = expected
                modified = True
        elif field == "owner.location.subdivision.name":
            owner = data.get("owner") or {}
            loc = owner.get("location") or {}
            sd = loc.get("subdivision")
            if sd and isinstance(sd, dict) and sd.get("name") != expected:
                sd["name"] = expected
                modified = True
        elif m := re.match(r"coverage\[(\d+)\]\.location\.subregion\.name", field):
            idx = int(m.group(1))
            cov = data.get("coverage") or []
            if idx < len(cov):
                loc = cov[idx].get("location") or {}
                sr = loc.get("subregion")
                if sr and isinstance(sr, dict) and sr.get("name") != expected:
                    sr["name"] = expected
                    modified = True
        elif m := re.match(r"coverage\[(\d+)\]\.location\.subdivision\.name", field):
            idx = int(m.group(1))
            cov = data.get("coverage") or []
            if idx < len(cov):
                loc = cov[idx].get("location") or {}
                sd = loc.get("subdivision")
                if sd and isinstance(sd, dict) and sd.get("name") != expected:
                    sd["name"] = expected
                    modified = True

    if modified:
        with open(full_path, "w", encoding="utf-8") as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
    return modified


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Fix SUBREGION_NAME_ID_MISMATCH issues")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fixed without writing")
    args = parser.parse_args()

    if not REPORT_FILE.exists():
        print(f"Report not found: {REPORT_FILE}")
        print("Run: python scripts/builder.py analyze-quality")
        return

    issues = list(parse_report(REPORT_FILE))
    # Group by file: (file_path -> [(field, expected), ...])
    by_file: dict[str, list[tuple[str, str]]] = {}
    for file_path, field, expected in issues:
        by_file.setdefault(file_path, [])
        if (field, expected) not in by_file[file_path]:
            by_file[file_path].append((field, expected))

    print(f"Found {len(issues)} issues in {len(by_file)} files")

    if args.dry_run:
        for fp, upds in list(by_file.items())[:5]:
            print(f"  {fp}: {upds}")
        if len(by_file) > 5:
            print(f"  ... and {len(by_file) - 5} more files")
        return

    fixed = 0
    for file_path, updates in by_file.items():
        if fix_yaml_file(file_path, updates):
            fixed += 1
            print(f"Fixed: {file_path}")

    print(f"\nFixed {fixed} files")


if __name__ == "__main__":
    main()
