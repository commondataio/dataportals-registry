#!/usr/bin/env python3
"""
Remove scheduled records that already exist in entities.

Walks data/scheduled/, loads each YAML, and deletes the file if its id or link
matches any record in data/entities/. Uses the same duplicate detection logic
as sync_ckan_ecosystem (ID match + normalized URL/domain match).

Usage:
  python scripts/remove_scheduled_duplicates.py           # Remove duplicates
  python scripts/remove_scheduled_duplicates.py --dry-run   # Preview only
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import typer
import yaml

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
SCHEDULED_DIR = BASE_DIR / "data" / "scheduled"

app = typer.Typer()


def normalize_url(url: str) -> str:
    """Normalize URL for duplicate detection."""
    if not url:
        return ""
    url = url.lower().strip()
    url = re.sub(r"^https?://", "", url)
    url = re.sub(r"^www\.", "", url)
    url = url.rstrip("/")
    return url


def normalize_domain(url: str) -> str:
    """Extract normalized domain from URL."""
    normalized = normalize_url(url)
    if not normalized:
        return ""
    domain = normalized.split("/")[0].split(":")[0]
    return domain


def load_entity_ids_and_urls() -> tuple[set[str], set[str]]:
    """Load all entity IDs and normalized URLs from data/entities/."""
    entity_ids = set()
    entity_urls = set()

    if not ENTITIES_DIR.exists():
        return entity_ids, entity_urls

    for root, _dirs, files in os.walk(ENTITIES_DIR):
        for filename in files:
            if not filename.endswith(".yaml"):
                continue
            filepath = os.path.join(root, filename)
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    record = yaml.load(f, Loader=Loader)
                if not isinstance(record, dict):
                    continue
                rid = record.get("id")
                link = record.get("link")
                if rid:
                    entity_ids.add(rid)
                if link:
                    entity_urls.add(normalize_url(link))
                    entity_urls.add(normalize_domain(link))
            except Exception:
                pass

    return entity_ids, entity_urls


def is_duplicate(record: dict, entity_ids: set[str], entity_urls: set[str]) -> bool:
    """Return True if record exists in entities (by id or link)."""
    rid = record.get("id")
    if rid and rid in entity_ids:
        return True

    link = record.get("link")
    if link:
        norm_url = normalize_url(link)
        norm_domain = normalize_domain(link)
        if norm_url in entity_urls or norm_domain in entity_urls:
            return True

    return False


@app.command()
def main(
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview only, do not delete files"),
) -> None:
    """Remove scheduled records that already exist in entities."""
    if dry_run:
        print("DRY RUN - no files will be deleted\n")

    if not SCHEDULED_DIR.exists():
        print(f"Scheduled directory not found: {SCHEDULED_DIR}")
        return

    print("Loading entity IDs and URLs...")
    entity_ids, entity_urls = load_entity_ids_and_urls()
    print(f"  Loaded {len(entity_ids)} entity IDs, {len(entity_urls)} entity URLs\n")

    removed = 0
    kept = 0
    errors: list[tuple[Path, str]] = []

    # Walk all subdirs (same pattern as builder.build_dataset)
    scheduled_files: list[Path] = []
    for root, _dirs, files in os.walk(SCHEDULED_DIR):
        for filename in files:
            if filename.endswith(".yaml"):
                scheduled_files.append(Path(root) / filename)

    for yaml_path in sorted(scheduled_files):
        try:
            data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        except Exception as e:
            errors.append((yaml_path, str(e)))
            continue

        if not isinstance(data, dict):
            errors.append((yaml_path, "Invalid YAML structure"))
            continue

        rid = data.get("id", "unknown")
        if is_duplicate(data, entity_ids, entity_urls):
            if dry_run:
                print(f"  [would remove] {rid} -> exists in entities")
            else:
                yaml_path.unlink()
                print(f"  [removed] {rid}")
            removed += 1
        else:
            kept += 1

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for path, err in errors[:10]:
            print(f"  {path}: {err}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")

    # Clean up empty subdirs (bottom-up) when files were removed
    dirs_removed = 0
    if not dry_run and removed > 0:
        for root, dirs, files in os.walk(SCHEDULED_DIR, topdown=False):
            dir_path = Path(root)
            if dir_path == SCHEDULED_DIR:
                continue
            # Remove if empty (no files, no subdirs with content)
            try:
                if not any(dir_path.iterdir()):
                    dir_path.rmdir()
                    dirs_removed += 1
            except OSError:
                pass

    print(f"\nRemoved: {removed}, kept: {kept}" + (f", empty dirs cleaned: {dirs_removed}" if dirs_removed else ""))

    if not dry_run and removed > 0:
        print("\nNext steps:")
        print("  python scripts/builder.py build")


if __name__ == "__main__":
    app()
