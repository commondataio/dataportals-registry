#!/usr/bin/env python3
"""
Fix SUBREGION_UNK_PLACEHOLDER: move XX-UNK records to Federal/ and remove subregion.

For records in {country}/{country}-UNK/ or with owner.location.subregion XX-UNK,
moves to Federal/ and removes the placeholder subregion when we cannot infer
the real subregion. Use country-specific scripts (fix_es_unk_subregions.py, etc.)
for records where subregion can be inferred.
"""

from __future__ import annotations

from pathlib import Path

import yaml

BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"


def remove_unk_subregion(obj: dict) -> bool:
    """Remove subregion if it's XX-UNK. Returns True if changed."""
    if not isinstance(obj, dict):
        return False
    loc = obj.get("location") or {}
    if not isinstance(loc, dict):
        return False
    sr = loc.get("subregion") or loc.get("subdivision")
    if isinstance(sr, dict):
        sid = (sr.get("id") or "").strip()
        if sid and sid.endswith("-UNK"):
            if "subregion" in loc:
                del loc["subregion"]
            elif "subdivision" in loc:
                del loc["subdivision"]
            return True
    return False


def process_record(data: dict) -> bool:
    """Remove XX-UNK subregion from owner and coverage. Returns True if changed."""
    changed = False
    owner = data.get("owner") or {}
    if remove_unk_subregion(owner):
        changed = True
    for cov in data.get("coverage") or []:
        if isinstance(cov, dict) and remove_unk_subregion(cov):
            changed = True
    return changed


def main(dry_run: bool = True) -> None:
    moved = 0
    updated = 0

    for country_dir in sorted(ENTITIES_DIR.iterdir()):
        if not country_dir.is_dir():
            continue
        country_code = country_dir.name
        unk_dir = country_dir / f"{country_code}-UNK"
        if not unk_dir.exists():
            continue

        federal_dir = country_dir / "Federal"
        for subdir in ("opendata", "geo", "scientific", "indicators", "microdata", "api", "metadata", "other"):
            src_subdir = unk_dir / subdir
            if not src_subdir.exists():
                continue

            for yaml_path in sorted(src_subdir.glob("*.yaml")):
                try:
                    data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
                except Exception as e:
                    print(f"  SKIP (parse) {yaml_path}: {e}")
                    continue

                if not isinstance(data, dict):
                    continue

                changed = process_record(data)
                dst_subdir = federal_dir / subdir
                dst_path = dst_subdir / yaml_path.name

                if dst_path.exists() and dst_path.resolve() != yaml_path.resolve():
                    print(f"  SKIP (exists) {yaml_path.relative_to(ENTITIES_DIR)}")
                    continue

                if dry_run:
                    print(f"  MOVE {yaml_path.relative_to(ENTITIES_DIR)} -> {country_code}/Federal/{subdir}/")
                else:
                    dst_subdir.mkdir(parents=True, exist_ok=True)
                    dst_path.write_text(
                        yaml.dump(data, allow_unicode=True, default_flow_style=False, sort_keys=False),
                        encoding="utf-8",
                    )
                    yaml_path.unlink()

                moved += 1
                if changed:
                    updated += 1

    print(f"\n{'Would move' if dry_run else 'Moved'}: {moved} (updated subregion: {updated})")
    if dry_run and moved > 0:
        print("Run with --apply to perform moves.")


if __name__ == "__main__":
    import sys
    main(dry_run="--apply" not in sys.argv)
