#!/usr/bin/env python3
"""
Review records in data/scheduled/, update status, and move to proper subdir in data/entities/.

For each scheduled file:
- Known country (EU, CN, FR, World, etc.): Move to entities/{country}/Federal/{type}/
- Unknown: Infer country from coverage, owner, link; update coverage/owner; move
- Update status to active (unless staging/dev)
"""

from __future__ import annotations

import os
import sys

# Allow importing from scripts/
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

import yaml

BASE_DIR = Path(__file__).parent.parent
SCHEDULED_DIR = BASE_DIR / "data" / "scheduled"
ENTITIES_DIR = BASE_DIR / "data" / "entities"

# Staging/dev sites - keep as inactive
STAGING_PATTERNS = ("staging", "dev.", "demo.", "test.", "derilinx.com", "klldev", "disldev")

# catalog_type -> subdir mapping (from constants.MAP_CATALOG_TYPE_SUBDIR + opendata default)
TYPE_TO_SUBDIR = {
    "Geoportal": "geo",
    "Open data portal": "opendata",
    "Scientific data repository": "scientific",
    "Indicators catalog": "indicators",
    "Microdata catalog": "microdata",
    "Machine learning catalog": "ml",
    "Metadata catalog": "metadata",
    "API Catalog": "api",
    "Data search engine": "search",
    "Data marketplace": "marketplace",
    "Other": "other",
    "Datasets list": "opendata",
    "General research repository": "scientific",
}


def get_subdir_from_path(rel_path: str) -> str:
    """Extract type subdir from scheduled path like EU/geo/ or Unknown/opendata/."""
    parts = Path(rel_path).parts
    if len(parts) >= 2:
        return parts[1]  # geo, opendata, scientific, etc.
    return "opendata"


def get_country_from_path(rel_path: str) -> str | None:
    """Extract country from scheduled path like EU/geo/ or Unknown/opendata/."""
    parts = Path(rel_path).parts
    if len(parts) >= 1:
        return parts[0]  # EU, CN, Unknown, World, etc.
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


def infer_country_from_link(link: str) -> str | None:
    """Infer country from domain TLD. Returns country_id or None."""
    if not link:
        return None
    try:
        parsed = urlparse(link if "://" in link else f"https://{link}")
        domain = (parsed.netloc or link).lower().replace("www.", "").split("/")[0]

        gov_tlds = {
            ".gov.uk": "GB", ".gov.au": "AU", ".gov.ca": "CA", ".gov.nz": "NZ",
            ".gov.sg": "SG", ".gov.in": "IN", ".gov.br": "BR", ".gov.mx": "MX",
            ".gov.ar": "AR", ".gov.fr": "FR", ".gov.de": "DE", ".gov.nl": "NL",
            ".gov.be": "BE", ".gov.es": "ES", ".gov.it": "IT", ".gov.pl": "PL",
            ".gov.cn": "CN", ".gov.jp": "JP", ".gov.kr": "KR", ".gov.ru": "RU",
            ".gov.ua": "UA", ".gov.tr": "TR", ".gov.gr": "GR", ".gov.pt": "PT",
            ".gov.fi": "FI", ".gov.se": "SE", ".gov.no": "NO", ".gov.dk": "DK",
            ".gov.ie": "IE", ".gov.at": "AT", ".gov.ch": "CH", ".gov.il": "IL",
            ".gov.za": "ZA", ".gov.ke": "KE", ".gov.pk": "PK", ".gov.bd": "BD",
            ".gov.vn": "VN", ".gov.id": "ID", ".gov.th": "TH", ".gov.my": "MY",
            ".gov.co": "CO", ".gov.cl": "CL", ".gov.pe": "PE", ".gov.ec": "EC",
            ".gov.mk": "MK", ".gov.me": "ME", ".gov.ge": "GE", ".gov.mn": "MN",
        }
        for suffix, cid in gov_tlds.items():
            if domain.endswith(suffix):
                return cid

        if domain.endswith(".gov") and not any(
            domain.endswith(f".gov.{tld}") for tld in ["uk", "au", "ca", "nz", "sg", "in", "br", "mx", "ar"]
        ):
            return "US"

        tld_2letter = domain.split(".")[-1] if "." in domain else ""
        tld_to_country = {
            "uk": "GB", "au": "AU", "ca": "CA", "nz": "NZ", "sg": "SG", "in": "IN",
            "br": "BR", "mx": "MX", "ar": "AR", "fr": "FR", "de": "DE", "nl": "NL",
            "be": "BE", "es": "ES", "it": "IT", "pl": "PL", "cz": "CZ", "at": "AT",
            "ch": "CH", "se": "SE", "no": "NO", "dk": "DK", "fi": "FI", "ie": "IE",
            "jp": "JP", "kr": "KR", "cn": "CN", "tw": "TW", "hk": "HK", "mo": "MO",
            "ru": "RU", "ua": "UA", "by": "BY", "kz": "KZ", "tr": "TR", "il": "IL",
            "gr": "GR", "pt": "PT", "ro": "RO", "hu": "HU", "bg": "BG", "hr": "HR",
            "si": "SI", "sk": "SK", "rs": "RS", "ba": "BA", "me": "ME", "mk": "MK",
            "al": "AL", "ee": "EE", "lv": "LV", "lt": "LT", "eu": "EU",
            "co": "CO", "cl": "CL", "pe": "PE", "ec": "EC", "uy": "UY", "py": "PY",
            "bo": "BO", "cr": "CR", "pa": "PA", "gt": "GT", "hn": "HN", "ni": "NI",
            "vn": "VN", "ph": "PH", "my": "MY", "id": "ID", "pk": "PK", "bd": "BD",
            "th": "TH", "kh": "KH", "la": "LA", "mm": "MM", "np": "NP", "lk": "LK",
            "sn": "SN", "ci": "CI", "gh": "GH", "ng": "NG", "eg": "EG", "ma": "MA",
            "tn": "TN", "dz": "DZ", "ly": "LY", "sd": "SD", "et": "ET", "tz": "TZ",
            "ug": "UG", "rw": "RW", "cm": "CM", "cd": "CD", "ao": "AO", "mz": "MZ",
            "zw": "ZW", "bw": "BW", "na": "NA", "ls": "LS", "mu": "MU", "sc": "SC",
            "km": "KM", "mg": "MG", "mv": "MV", "af": "AF", "uz": "UZ", "tm": "TM",
            "tj": "TJ", "kg": "KG", "ge": "GE", "am": "AM", "az": "AZ", "ir": "IR",
        }
        if tld_2letter in tld_to_country:
            return tld_to_country[tld_2letter]
    except Exception:
        pass
    return None


def infer_country_for_unknown(record: dict) -> str:
    """Infer country for Unknown records. Returns country_id, default World."""
    country_id, _ = get_country_from_record(record)
    if country_id:
        return country_id

    link = record.get("link") or ""
    cid = infer_country_from_link(link)
    if cid:
        return cid

    # URL/domain heuristics
    rid = (record.get("id") or "").lower()
    link_lower = link.lower()
    if "brasilio" in rid or "brasilio" in link_lower or "brasil.io" in link_lower:
        return "BR"
    if "axiell" in rid or "axiell" in link_lower:
        return "NL"  # Axiell/LDMax.nl
    if "dakar" in rid or "dakar" in link_lower or "inondationsdakar" in rid:
        return "SN"
    if "unesco" in link_lower or "unescwa" in link_lower:
        return "World"
    if "unhcr" in link_lower or "unhcrorg" in link_lower:
        return "World"
    if "icann" in link_lower:
        return "World"
    if "dhsprogram" in link_lower:
        return "US"
    if "sdsmt" in link_lower or "sdsmtedu" in link_lower:
        return "US"
    if "grandest" in link_lower or "data4citizen" in link_lower:
        return "FR"
    if "conaviopendata" in link_lower or "junar" in link_lower:
        return "CL"
    if "catalogriits" in link_lower:
        return "IT"
    if "redatam" in link_lower:
        return "World"
    if "africaopendata" in link_lower or "investigateafrica" in link_lower or "alcafricandatalab" in link_lower:
        return "World"
    if "opendatab40cities" in link_lower:
        return "World"
    if "opengeohub" in link_lower:
        return "World"
    if "marineregions" in link_lower:
        return "World"
    if "d4science" in link_lower:
        return "EU"
    if "opendatasoft" in link_lower and "zastrug" in rid:
        return "RS"  # or BA/ME - Balkan region

    return "World"


def is_staging_or_dev(record: dict) -> bool:
    """True if record appears to be a staging/dev environment."""
    link = (record.get("link") or "").lower()
    rid = (record.get("id") or "").lower()
    desc = (record.get("description") or "").lower()
    for p in STAGING_PATTERNS:
        if p in link or p in rid or p in desc:
            return True
    return False


def get_subdir_from_catalog_type(catalog_type: str, path_type: str) -> str:
    """Get target subdir from catalog_type or path fallback."""
    if catalog_type and catalog_type in TYPE_TO_SUBDIR:
        return TYPE_TO_SUBDIR[catalog_type]
    return path_type if path_type in TYPE_TO_SUBDIR.values() else "opendata"


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("DRY RUN - no files will be moved\n")

    if not SCHEDULED_DIR.exists():
        print(f"Directory not found: {SCHEDULED_DIR}")
        return

    # Collect all scheduled YAML files
    scheduled_files: list[tuple[Path, str, str]] = []  # (path, country_from_path, type_from_path)
    for yaml_path in sorted(SCHEDULED_DIR.rglob("*.yaml")):
        try:
            rel = yaml_path.relative_to(SCHEDULED_DIR)
            country = get_country_from_path(str(rel))
            subdir = get_subdir_from_path(str(rel))
            if country and subdir:
                scheduled_files.append((yaml_path, country, subdir))
        except ValueError:
            continue

    if not scheduled_files:
        print("No YAML files in data/scheduled/")
        return

    print(f"Processing {len(scheduled_files)} scheduled records\n")

    promoted = 0
    skipped_dup = 0
    updated_coverage = 0
    errors: list[tuple[Path, str]] = []

    for yaml_path, path_country, path_type in scheduled_files:
        try:
            data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        except Exception as e:
            errors.append((yaml_path, str(e)))
            continue
        if not isinstance(data, dict):
            errors.append((yaml_path, "Invalid YAML structure"))
            continue

        rid = data.get("id", "unknown")
        catalog_type = data.get("catalog_type", "")
        target_subdir = get_subdir_from_catalog_type(catalog_type, path_type)

        if path_country == "Unknown":
            country_id = infer_country_for_unknown(data)
            admin_dir = "Federal"

            # Update coverage if it was Unknown
            cov = data.get("coverage") or []
            if cov:
                loc = cov[0].get("location", {})
                curr_country = loc.get("country", {})
                curr_id = (curr_country.get("id") or "").strip()
                if curr_id in ("Unknown", "") and country_id:
                    try:
                        from constants import COUNTRIES
                        cname = COUNTRIES.get(country_id, country_id)
                    except ImportError:
                        cname = country_id
                    cov[0]["location"]["country"] = {"id": country_id, "name": cname}
                    updated_coverage += 1

            # Update owner location if Unknown
            owner = data.get("owner") or {}
            owner_loc = owner.get("location") or {}
            owner_country = owner_loc.get("country") or {}
            if (owner_country.get("id") or "").strip() in ("Unknown", "") and country_id:
                try:
                    from constants import COUNTRIES
                    cname = COUNTRIES.get(country_id, country_id)
                except ImportError:
                    cname = country_id
                owner["location"] = owner.get("location") or {}
                owner["location"]["country"] = {"id": country_id, "name": cname}
                data["owner"] = owner
        else:
            country_id = path_country
            admin_dir = "Federal"

        # Update status
        if is_staging_or_dev(data):
            data["status"] = "inactive"
        elif (data.get("status") or "").strip() == "scheduled":
            data["status"] = "active"

        target_dir = ENTITIES_DIR / country_id / admin_dir / target_subdir
        target_path = target_dir / yaml_path.name

        if target_path.exists() and target_path != yaml_path:
            # Duplicate - entity already exists, remove scheduled copy
            if not dry_run:
                yaml_path.unlink()
            skipped_dup += 1
            print(f"  [skip dup] {rid} -> already in entities")
            continue

        if dry_run:
            print(f"  {rid} -> {country_id}/{admin_dir}/{target_subdir}/ (status={data.get('status')})")
            promoted += 1
            continue

        target_dir.mkdir(parents=True, exist_ok=True)
        target_path.write_text(
            yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
        yaml_path.unlink()
        promoted += 1
        print(f"  {rid} -> {country_id}/{admin_dir}/{target_subdir}/ (status={data.get('status')})")

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for path, err in errors[:10]:
            print(f"  {path}: {err}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")

    print(f"\nPromoted: {promoted}, updated coverage: {updated_coverage}, skipped (dup): {skipped_dup}")

    if not dry_run and promoted > 0:
        print("\nNext steps:")
        print("  python scripts/builder.py assign")
        print("  python scripts/builder.py validate-yaml")
        print("  python scripts/builder.py build")


if __name__ == "__main__":
    main()
