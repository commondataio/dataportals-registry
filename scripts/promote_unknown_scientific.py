#!/usr/bin/env python3
"""
Review records in data/scheduled/Unknown/scientific, infer country from domain/link/owner,
update status, and move to proper subdir in data/entities/.

For each record:
- Use coverage/owner country if already set and not Unknown
- Infer country from TLD (.edu, .int, country TLDs), domain hints, owner
- Update coverage and owner with inferred country
- Ensure status is active
- Move to data/entities/{COUNTRY}/Federal/scientific/
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import urlparse

import yaml

BASE_DIR = Path(__file__).parent.parent
SCHEDULED_UNKNOWN_SCIENTIFIC = BASE_DIR / "data" / "scheduled" / "Unknown" / "scientific"
ENTITIES_DIR = BASE_DIR / "data" / "entities"

# Country names for display
COUNTRY_NAMES = {
    "US": "United States",
    "GB": "United Kingdom",
    "World": "World",
    "AU": "Australia",
    "CA": "Canada",
    "FR": "France",
    "DE": "Germany",
    "NL": "Netherlands",
    "BE": "Belgium",
    "ES": "Spain",
    "IT": "Italy",
    "SE": "Sweden",
    "DK": "Denmark",
    "PL": "Poland",
    "GE": "Georgia",
    "CH": "Switzerland",
    "AT": "Austria",
    "IE": "Ireland",
    "BR": "Brazil",
    "IN": "India",
    "CN": "China",
    "JP": "Japan",
    "KR": "South Korea",
    "ZA": "South Africa",
}

# Scientific-specific domain/id tokens -> (country_id, None for Federal)
SCIENTIFIC_DOMAIN_HINTS: dict[str, tuple[str, str | None]] = {
    # usegalaxy.* instances
    "usegalaxyde": ("DE", None),
    "usegalaxyse": ("SE", None),
    "usegalaxydk": ("DK", None),
    "usegalaxyit": ("IT", None),
    "usegalaxypasteurfr": ("FR", None),
    "usegalaxyvibbe": ("BE", None),
    "usegalaxynl": ("NL", None),
    "usegalaxypl": ("PL", None),
    "usegalaxyiuedu": ("US", None),
    # US universities
    "massiveucsdedu": ("US", None),
    "ucsd": ("US", None),
    # International / World
    "ecmwfint": ("World", None),
    "wwwecmwfint": ("World", None),
    "datafaangorg": ("World", None),
    "faang": ("World", None),
    "ifpri": ("World", None),
    "wwwifpriorg": ("World", None),
    "earthsystemgrid": ("World", None),
    "wwwearthsystemgridorg": ("World", None),
    # Regional
    "caucasusbarometer": ("GE", None),
    "caucasusbarometerorg": ("GE", None),
    # UK / Figshare (publisher instances default to UK when no owner country)
    "figsharecom": ("GB", None),
    "brillfigsharecom": ("GB", None),
    "springernaturefigsharecom": ("GB", None),
    "f1000figsharecom": ("GB", None),
    "frontiersinfigsharecom": ("GB", None),
    "dimensionsfigsharecom": ("GB", None),
    "eurfigsharecom": ("GB", None),
    "scielofigsharecom": ("BR", None),
    "cityfigsharecom": ("GB", None),
    "cellimagelibraryfigsharecom": ("GB", None),
    "iavifigsharecom": ("GB", None),
    "hirjibehedinresearchfigsharecom": ("GB", None),
    "uufigsharecom": ("NL", None),  # Utrecht University
    # US
    "discoverybiothingsio": ("US", None),
    "biothings": ("US", None),
    "moleculenetorg": ("US", None),
    "wwwpepnetorg": ("US", None),
    "wwwgseamsigdborg": ("US", None),
    "sleepdataorg": ("US", None),
    "cibmtrorg": ("US", None),
    "wwwciforicraforg": ("US", None),
    "wwwgainhealthorg": ("US", None),
    "openheritage3dorg": ("US", None),
    # International programs
    "ssdbiodporg": ("World", None),
    "iodp": ("World", None),
}


def infer_country_from_link(link: str) -> tuple[str, str | None] | None:
    """Infer country from domain TLD. Returns (country_id, subregion_id or None)."""
    if not link:
        return None
    try:
        parsed = urlparse(link)
        domain = (parsed.netloc or link).lower()
        domain = domain.replace("www.", "").split("/")[0]

        # .int -> World (international organizations)
        if domain.endswith(".int"):
            return ("World", None)

        # .edu -> US default (most .edu are US)
        if domain.endswith(".edu") and not any(
            domain.endswith(f".edu.{tld}")
            for tld in ["uk", "au", "ca", "nz", "sg", "in", "br", "mx", "ar", "jp", "kr", "cn", "tw"]
        ):
            return ("US", None)
        if domain.endswith(".edu.au"):
            return ("AU", None)
        if domain.endswith(".edu.uk"):
            return ("GB", None)
        if domain.endswith(".edu.ca"):
            return ("CA", None)
        if domain.endswith(".edu.nz"):
            return ("NZ", None)
        if domain.endswith(".edu.sg"):
            return ("SG", None)
        if domain.endswith(".edu.in"):
            return ("IN", None)
        if domain.endswith(".edu.br"):
            return ("BR", None)
        if domain.endswith(".edu.mx"):
            return ("MX", None)
        if domain.endswith(".edu.ar"):
            return ("AR", None)
        if domain.endswith(".edu.jp"):
            return ("JP", None)
        if domain.endswith(".edu.kr"):
            return ("KR", None)
        if domain.endswith(".edu.cn"):
            return ("CN", None)
        if domain.endswith(".edu.tw"):
            return ("TW", None)
        if domain.endswith(".ac.uk"):
            return ("GB", None)
        if domain.endswith(".ac.jp"):
            return ("JP", None)
        if domain.endswith(".ac.nz"):
            return ("NZ", None)
        if domain.endswith(".ac.be"):
            return ("BE", None)

        # Country code TLDs (2-letter)
        tld_2letter = domain.split(".")[-1] if "." in domain else ""
        tld_to_country = {
            "uk": "GB", "au": "AU", "ca": "CA", "nz": "NZ", "sg": "SG", "in": "IN",
            "br": "BR", "mx": "MX", "ar": "AR", "fr": "FR", "de": "DE", "nl": "NL",
            "be": "BE", "es": "ES", "it": "IT", "pl": "PL", "cz": "CZ", "at": "AT",
            "ch": "CH", "se": "SE", "no": "NO", "dk": "DK", "fi": "FI", "ie": "IE",
            "jp": "JP", "kr": "KR", "cn": "CN", "tw": "TW", "hk": "HK", "mo": "MO",
            "ru": "RU", "ua": "UA", "ge": "GE", "tr": "TR", "il": "IL",
            "eu": "EU",
        }
        if tld_2letter in tld_to_country:
            c = tld_to_country[tld_2letter]
            return ("World", None) if c == "EU" else (c, None)

    except Exception:
        pass
    return None


def infer_from_domain_hints(record: dict) -> tuple[str, str | None] | None:
    """Infer country from domain tokens in link, id, owner name."""
    link = (record.get("link") or "").strip()
    record_id = (record.get("id") or "").strip().lower()
    owner_name = ((record.get("owner", {}) or {}).get("name", "") or "").strip().lower()
    owner_link = ((record.get("owner", {}) or {}).get("link", "") or "").strip()

    combined = f" {record_id} {owner_name} "
    try:
        parsed = urlparse(link if "://" in link else f"https://{link}")
        host = (parsed.netloc or "").lower().replace("www.", "").split("/")[0]
        combined += f" {host} "
    except Exception:
        pass
    try:
        parsed = urlparse(owner_link if "://" in owner_link else f"https://{owner_link}")
        host = (parsed.netloc or "").lower().replace("www.", "").split("/")[0]
        combined += f" {host} "
    except Exception:
        pass

    combined_compact = re.sub(r"[^a-z0-9]", "", combined)

    for token, (country, subregion) in sorted(
        SCIENTIFIC_DOMAIN_HINTS.items(), key=lambda x: -len(x[0])
    ):
        if token in combined_compact or token in combined:
            return (country, subregion)

    return None


def get_country_from_record(record: dict) -> tuple[str | None, str | None]:
    """Return (country_id, country_name) from coverage or owner. None if Unknown."""
    cov = record.get("coverage") or []
    if cov:
        loc = cov[0].get("location", {})
        country = loc.get("country", {})
        cid = country.get("id")
        cname = country.get("name")
        if cid and str(cid) not in ("Unknown", ""):
            return (str(cid), cname or cid)

    owner = record.get("owner") or {}
    owner_loc = owner.get("location") or {}
    owner_country = owner_loc.get("country") or {}
    oid = owner_country.get("id")
    oname = owner_country.get("name")
    if oid and str(oid) not in ("Unknown", ""):
        return (str(oid), oname or oid)

    return (None, None)


def infer_country_and_subregion(record: dict) -> tuple[str, str | None]:
    """Infer (country_id, subregion_id or None). Default to World for unknown."""
    # 1. Existing coverage/owner country
    country_id, _ = get_country_from_record(record)
    if country_id:
        return (country_id, None)

    # 2. TLD from link
    result = infer_country_from_link(record.get("link") or "")
    if result:
        return result

    # 3. Domain hints
    result = infer_from_domain_hints(record)
    if result:
        return result

    # 4. Owner name hints for international
    owner_name = ((record.get("owner", {}) or {}).get("name", "") or "").lower()
    if any(
        x in owner_name
        for x in [
            "united nations",
            "international",
            "world",
            "global",
            "european centre",
            "ecmwf",
            "ifpri",
            "galaxy project community",
            "proteomexchange",
        ]
    ):
        return ("World", None)

    # 5. Description hints
    desc = (record.get("description") or "").lower()
    if any(
        x in desc
        for x in [
            "united nations",
            "international",
            "worldwide",
            "global",
            "intergovernmental",
        ]
    ):
        return ("World", None)

    # 6. Default: World for scientific repositories that couldn't be attributed
    return ("World", None)


def build_location(country_id: str, subregion_id: str | None) -> dict:
    """Build location dict for coverage/owner."""
    loc: dict = {
        "country": {
            "id": country_id,
            "name": COUNTRY_NAMES.get(country_id, country_id),
        },
        "level": 30 if subregion_id else 20,
    }
    if subregion_id:
        loc["subregion"] = {"id": subregion_id, "name": subregion_id}
    return loc


def get_target_path(country_id: str, subregion_id: str | None) -> Path:
    """Get target directory for entity file. Scientific uses Federal only."""
    if subregion_id:
        return ENTITIES_DIR / country_id / subregion_id / "scientific"
    return ENTITIES_DIR / country_id / "Federal" / "scientific"


def process_record(record: dict) -> bool:
    """Update record with inferred country and status. Returns True if changed."""
    changed = False
    country_id, subregion_id = infer_country_and_subregion(record)

    loc = build_location(country_id, subregion_id)

    # Update coverage
    coverage = record.get("coverage") or []
    if not coverage:
        record["coverage"] = [{"location": loc}]
        changed = True
    else:
        for cov in coverage:
            if isinstance(cov, dict) and "location" in cov:
                old_country = (cov.get("location") or {}).get("country") or {}
                if (old_country.get("id") or "").strip() != country_id:
                    cov["location"] = loc.copy()
                    changed = True
                elif subregion_id and (cov.get("location") or {}).get("subregion", {}).get("id") != subregion_id:
                    cov["location"] = loc.copy()
                    changed = True

    # Update owner location
    owner = record.get("owner") or {}
    owner_loc = owner.get("location") or {}
    old_country = (owner_loc.get("country") or {}).get("id") or ""
    if old_country != country_id or (
        subregion_id and (owner_loc.get("subregion") or {}).get("id") != subregion_id
    ):
        owner["location"] = loc.copy()
        record["owner"] = owner
        changed = True

    # Ensure status is active
    if (record.get("status") or "").strip() != "active":
        record["status"] = "active"
        changed = True

    return changed


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("DRY RUN - no files will be moved\n")

    if not SCHEDULED_UNKNOWN_SCIENTIFIC.exists():
        print(f"Directory not found: {SCHEDULED_UNKNOWN_SCIENTIFIC}")
        return

    yaml_files = sorted(SCHEDULED_UNKNOWN_SCIENTIFIC.glob("*.yaml"))
    if not yaml_files:
        print("No YAML files in data/scheduled/Unknown/scientific")
        return

    print(f"Processing {len(yaml_files)} records in data/scheduled/Unknown/scientific\n")

    by_target: dict[tuple[str, str | None], list[tuple[Path, dict]]] = {}
    errors: list[tuple[Path, str]] = []

    for yaml_path in yaml_files:
        try:
            data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        except Exception as e:
            errors.append((yaml_path, str(e)))
            continue
        if not isinstance(data, dict):
            errors.append((yaml_path, "Invalid YAML structure"))
            continue

        process_record(data)
        country_id, subregion_id = infer_country_and_subregion(data)
        key = (country_id, subregion_id)
        by_target.setdefault(key, []).append((yaml_path, data))

    # Summary
    for (country_id, subregion_id), items in sorted(
        by_target.items(), key=lambda x: (x[0][0], x[0][1] or "")
    ):
        target_dir = get_target_path(country_id, subregion_id)
        label = f"{country_id}/{subregion_id or 'Federal'}/scientific"
        print(f"  {label}: {len(items)} records")
        for path, rec in items[:5]:
            print(f"    - {path.name} -> {rec.get('name', '?')[:50]}")
        if len(items) > 5:
            print(f"    ... and {len(items) - 5} more")

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for path, err in errors[:10]:
            print(f"  {path.name}: {err}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")

    if dry_run or not yaml_files:
        print("\nRun without --dry-run to apply changes.")
        return

    # Move files
    moved = 0
    for (country_id, subregion_id), items in by_target.items():
        target_dir = get_target_path(country_id, subregion_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        for src_path, data in items:
            target_path = target_dir / src_path.name
            if target_path.exists():
                src_path.unlink()
                continue
            target_path.write_text(
                yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
                encoding="utf-8",
            )
            src_path.unlink()
            moved += 1

    print(f"\nMoved {moved} files to entities.")
    print("Run: python scripts/builder.py assign")
    print("Run: python scripts/builder.py validate-yaml")
    print("Run: python scripts/builder.py build")


if __name__ == "__main__":
    main()
