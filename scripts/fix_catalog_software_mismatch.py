#!/usr/bin/env python3
"""
Fix CATALOG_SOFTWARE_MISMATCH by updating catalog_type and moving files to correct subdir.
Reads dataquality/rules/CATALOG_SOFTWARE_MISMATCH.txt and applies each suggested fix.
"""

import re
import sys
from pathlib import Path

import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

sys.path.insert(0, str(Path(__file__).parent))
from constants import MAP_CATALOG_TYPE_SUBDIR

BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
REPORT_FILE = BASE_DIR / "dataquality" / "rules" / "CATALOG_SOFTWARE_MISMATCH.txt"

# Open data portal and other types not in MAP_CATALOG_TYPE_SUBDIR
DEFAULT_SUBDIR = "opendata"


def catalog_type_to_subdir(catalog_type: str) -> str:
    """Return the subdirectory name for a catalog type."""
    return MAP_CATALOG_TYPE_SUBDIR.get(catalog_type, DEFAULT_SUBDIR)


def parse_report(report_path: Path):
    """Parse CATALOG_SOFTWARE_MISMATCH.txt; yield (file_path_rel, new_catalog_type)."""
    text = report_path.read_text(encoding="utf-8")
    # Pattern: File: SK/SK-BL/opendata/databratislavask.yaml then later Suggested Action: Update catalog_type to 'Geoportal' to match
    file_pattern = re.compile(r"^File:\s+(.+\.yaml)\s*$", re.MULTILINE)
    action_pattern = re.compile(r"Suggested Action:\s+Update catalog_type to '([^']+)' to match", re.MULTILINE)

    file_matches = list(file_pattern.finditer(text))
    action_matches = list(action_pattern.finditer(text))
    if len(file_matches) != len(action_matches):
        raise ValueError(f"Mismatch: {len(file_matches)} files vs {len(action_matches)} actions")
    for fm, am in zip(file_matches, action_matches):
        file_rel = fm.group(1).strip()
        new_type = am.group(1).strip()
        yield file_rel, new_type


def fix_one(rel_path: str, new_catalog_type: str) -> tuple[bool, str]:
    """Update one YAML file and move to correct subdir if needed. Returns (success, message)."""
    full_path = ENTITIES_DIR / rel_path
    if not full_path.exists():
        return False, f"File not found: {rel_path}"

    with open(full_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not data:
        return False, f"Empty YAML: {rel_path}"

    old_type = data.get("catalog_type", "")
    if old_type == new_catalog_type:
        return False, f"Already {new_catalog_type}: {rel_path}"

    data["catalog_type"] = new_catalog_type

    # Determine target subdir and current path parts (country / subregion? / subdir / file)
    parts = rel_path.replace("\\", "/").split("/")
    target_subdir = catalog_type_to_subdir(new_catalog_type)
    current_subdir = parts[-2] if len(parts) >= 2 else ""

    if current_subdir != target_subdir:
        # Build new path: same country (and subregion if present), new subdir, same filename
        if len(parts) >= 4:
            # e.g. US/US-OH/opendata/file.yaml -> US/US-OH/geo/file.yaml
            new_parts = parts[:-2] + [target_subdir] + [parts[-1]]
        else:
            # e.g. World/indicators/file.yaml -> World/opendata/file.yaml
            new_parts = [parts[0]] + [target_subdir] + [parts[-1]]
        new_rel = "/".join(new_parts)
        new_full = ENTITIES_DIR / new_rel
        new_full.parent.mkdir(parents=True, exist_ok=True)
        if new_full.exists() and new_full != full_path:
            return False, f"Target exists: {new_rel}"
        with open(new_full, "w", encoding="utf-8") as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        full_path.unlink()
        return True, f"{rel_path} -> catalog_type={new_catalog_type}, moved to {new_rel}"
    else:
        with open(full_path, "w", encoding="utf-8") as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        return True, f"{rel_path} -> catalog_type={new_catalog_type}"


def main():
    if not REPORT_FILE.exists():
        print(f"Report not found: {REPORT_FILE}")
        sys.exit(1)
    fixes = list(parse_report(REPORT_FILE))
    print(f"Found {len(fixes)} issues to fix.\n")
    fixed = 0
    skipped = 0
    errors = 0
    for rel_path, new_type in fixes:
        ok, msg = fix_one(rel_path, new_type)
        if ok:
            fixed += 1
            print(f"  ✓ {msg}")
        elif "not found" in msg or "Target exists" in msg or "Error" in msg:
            errors += 1
            print(f"  ✗ {msg}")
        else:
            skipped += 1
    print("\n" + "=" * 60)
    print(f"Fixed: {fixed}, Skipped: {skipped}, Errors: {errors}")


if __name__ == "__main__":
    main()
