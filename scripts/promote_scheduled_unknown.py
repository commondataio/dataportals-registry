#!/usr/bin/env python
"""Promote scheduled Unknown/opendata entries to entities with proper country and status."""

import os
import sys

# Allow importing from scripts/
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEDULED_UNKNOWN = os.path.join(_REPO_ROOT, "data", "scheduled", "Unknown", "opendata")
ENTITIES_DIR = os.path.join(_REPO_ROOT, "data", "entities")

# URL/domain heuristics for country inference when coverage is Unknown
URL_COUNTRY_HINTS = {
    "brasilio": "BR",
    "brasil.io": "BR",
}

# Staging/dev sites - keep as inactive
STAGING_PATTERNS = ("staging", "dev.", "demo.", "test.", "derilinx.com", "klldev", "disldev")


def get_country_from_record(record: dict) -> tuple[str | None, str | None]:
    """Return (country_id, country_name) from coverage or owner. None if Unknown."""
    cov = record.get("coverage") or []
    if cov:
        loc = cov[0].get("location", {})
        country = loc.get("country", {})
        cid = country.get("id")
        cname = country.get("name")
        if cid and str(cid) != "Unknown":
            return (str(cid), cname or cid)

    owner = record.get("owner") or {}
    owner_loc = owner.get("location") or {}
    owner_country = owner_loc.get("country") or {}
    oid = owner_country.get("id")
    oname = owner_country.get("name")
    if oid and str(oid) != "Unknown":
        return (str(oid), oname or oid)

    # URL heuristics
    rid = record.get("id", "")
    link = (record.get("link") or "").lower()
    for hint, country_id in URL_COUNTRY_HINTS.items():
        if hint in rid or hint in link:
            return (country_id, None)

    return (None, None)


def get_subregion_from_record(record: dict) -> str | None:
    """Return subregion id (e.g. FR-974) if present, else None."""
    cov = record.get("coverage") or []
    if cov:
        loc = cov[0].get("location", {})
        sub = loc.get("subregion")
        if sub and isinstance(sub, dict):
            return sub.get("id")
        if isinstance(sub, str):
            return sub
    return None


def is_staging_or_dev(record: dict) -> bool:
    """True if record appears to be a staging/dev environment."""
    link = (record.get("link") or "").lower()
    rid = (record.get("id") or "").lower()
    desc = (record.get("description") or "").lower()
    for p in STAGING_PATTERNS:
        if p in link or p in rid or p in desc:
            return True
    return False


def main():
    if not os.path.isdir(SCHEDULED_UNKNOWN):
        print(f"Directory not found: {SCHEDULED_UNKNOWN}")
        return

    files = [f for f in os.listdir(SCHEDULED_UNKNOWN) if f.endswith(".yaml")]
    promoted = 0
    updated_coverage = 0
    skipped_unknown = 0
    skipped_staging = 0

    for filename in sorted(files):
        filepath = os.path.join(SCHEDULED_UNKNOWN, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            record = yaml.load(f, Loader=Loader)

        if not record:
            continue

        rid = record.get("id", "unknown")
        country_id, country_name = get_country_from_record(record)

        if not country_id or country_id == "Unknown":
            skipped_unknown += 1
            continue

        if is_staging_or_dev(record):
            skipped_staging += 1
            # Still move to entities but with inactive status
            record["status"] = "inactive"

        # Update coverage if it was Unknown but we inferred from owner/URL
        cov = record.get("coverage") or []
        if cov:
            loc = cov[0].get("location", {})
            curr_country = loc.get("country", {})
            if (curr_country.get("id") == "Unknown" or not curr_country.get("id")) and country_id:
                try:
                    from constants import COUNTRIES
                    cname = country_name or COUNTRIES.get(country_id, country_id)
                except ImportError:
                    cname = country_name or country_id
                cov[0]["location"]["country"] = {"id": country_id, "name": cname}
                updated_coverage += 1

        # Set status to active if not already set to inactive
        if record.get("status") == "scheduled" and not is_staging_or_dev(record):
            record["status"] = "active"

        subregion = get_subregion_from_record(record)
        admin_dir = subregion if subregion else "Federal"
        target_dir = os.path.join(ENTITIES_DIR, country_id, admin_dir, "opendata")
        target_path = os.path.join(target_dir, filename)

        os.makedirs(target_dir, exist_ok=True)

        with open(target_path, "w", encoding="utf-8") as f:
            yaml.dump(record, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

        os.remove(filepath)
        promoted += 1
        print(f"  {rid} -> {country_id}/{admin_dir}/opendata/ (status={record.get('status')})")

    # Remove empty Unknown/opendata dirs if any
    try:
        if os.path.isdir(SCHEDULED_UNKNOWN) and not os.listdir(SCHEDULED_UNKNOWN):
            os.rmdir(SCHEDULED_UNKNOWN)
        unknown_dir = os.path.dirname(SCHEDULED_UNKNOWN)
        if os.path.isdir(unknown_dir) and not os.listdir(unknown_dir):
            os.rmdir(unknown_dir)
    except OSError:
        pass

    print(f"\nPromoted: {promoted}, updated coverage: {updated_coverage}")
    print(f"Skipped (unknown country): {skipped_unknown}, skipped (staging): {skipped_staging}")


if __name__ == "__main__":
    main()
