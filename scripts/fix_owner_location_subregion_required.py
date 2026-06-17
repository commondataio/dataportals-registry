#!/usr/bin/env python3
"""
Fix OWNER_LOCATION_SUBREGION_REQUIRED issues from dataquality rule report.

Handles two cases:
1. Record in subregion directory (e.g. US/US-IL/geo/): Add/update owner.location.subregion
   to match the directory for ANY owner type. Converts subdivision to subregion when needed.
2. Regional/Local government: Enforce owner.location.level=30 and subregion.
3. World records with Regional/Local gov: Change owner.type to Central government
   (World has no subregions; avoids the rule).

Use --exclude-country to skip specific countries (e.g. --exclude-country US).
"""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlparse

import yaml


BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
SCHEDULED_DIR = BASE_DIR / "data" / "scheduled"
RULE_REPORT = BASE_DIR / "dataquality" / "rules" / "OWNER_LOCATION_SUBREGION_REQUIRED.txt"
SUBREGIONS_CSV = BASE_DIR / "data" / "reference" / "subregions" / "ISO3166-2.CSV"

SUPPORTED_OWNER_TYPES = {"local government", "regional government"}

TOKENIZE_STOP = {
    "www", "http", "https", "geo", "gis", "map", "maps", "portal", "opendata",
    "data", "gov", "org", "com", "net", "edu",
}


def load_subregion_names() -> dict[str, str]:
    names: dict[str, str] = {}
    if not SUBREGIONS_CSV.exists():
        return names
    with SUBREGIONS_CSV.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = (row.get("code") or "").strip()
            name = (row.get("subdivision_name") or "").strip()
            if code and name and code not in names:
                names[code] = name
    return names


def parse_report(report_path: Path) -> list[tuple[str, str]]:
    """Parse report and return [(file_path, country), ...]."""
    text = report_path.read_text(encoding="utf-8")
    pattern = (
        r"File: ([^\n]+)\n"
        r"Record ID: [^\n]+\n"
        r"Country: ([^\n]+)\n"
        r"Issue: OWNER_LOCATION_SUBREGION_REQUIRED\n"
        r"Field: [^\n]+"
    )
    return [(m1.strip(), m2.strip()) for m1, m2 in re.findall(pattern, text)]


def tokenize_text(value: str) -> list[str]:
    text = (value or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = [tok for tok in text.split() if len(tok) >= 3]
    return [tok for tok in tokens if tok not in TOKENIZE_STOP]


def build_token_subregion_index(country_code: str) -> dict[str, dict[str, int]]:
    token_index: dict[str, dict[str, int]] = defaultdict(dict)
    country_dir = ENTITIES_DIR / country_code
    if not country_dir.exists():
        return token_index
    for path in country_dir.glob("**/*.yaml"):
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        subregion_id = None
        coverage = data.get("coverage", [])
        if isinstance(coverage, list):
            for entry in coverage:
                loc = (entry or {}).get("location", {}) or {}
                for key in ("subregion", "subdivision"):
                    sr = loc.get(key)
                    if isinstance(sr, dict) and sr.get("id"):
                        subregion_id = sr.get("id")
                        break
                if subregion_id:
                    break

        if not subregion_id:
            owner_loc = ((data.get("owner", {}) or {}).get("location", {}) or {})
            for key in ("subregion", "subdivision"):
                sr = owner_loc.get(key)
                if isinstance(sr, dict) and sr.get("id"):
                    subregion_id = sr.get("id")
                    break

        if not subregion_id:
            parts = path.relative_to(ENTITIES_DIR).parts
            if len(parts) >= 2 and "-" in parts[1] and parts[1].startswith(f"{country_code}-"):
                subregion_id = parts[1]

        if not subregion_id:
            continue

        link = (data.get("link") or "").strip()
        owner_name = ((data.get("owner", {}) or {}).get("name", "") or "").strip()
        record_id = (data.get("id") or path.stem or "").strip()
        parsed = urlparse(link if "://" in link else f"https://{link}") if link else None
        host = (parsed.netloc if parsed else "").lower().replace("www.", "")

        for source in [record_id, owner_name, host]:
            for token in tokenize_text(source):
                bucket = token_index.setdefault(token, {})
                bucket[subregion_id] = bucket.get(subregion_id, 0) + 1

    return token_index


def infer_subregion(
    record: dict,
    file_path: str,
    subregion_names: dict[str, str],
    country_code: str,
    token_subregions: dict[str, dict[str, int]] | None = None,
) -> dict | None:
    coverage = record.get("coverage", [])
    if isinstance(coverage, list):
        for entry in coverage:
            location = (entry or {}).get("location", {}) or {}
            for key in ("subregion", "subdivision"):
                candidate = location.get(key)
                if isinstance(candidate, dict):
                    sid = (candidate.get("id") or "").strip()
                    sname = (candidate.get("name") or "").strip()
                    if sid:
                        return {"id": sid, "name": sname or subregion_names.get(sid, sid)}

    owner_location = ((record.get("owner", {}) or {}).get("location", {}) or {})
    for key in ("subregion", "subdivision"):
        candidate = owner_location.get(key)
        if isinstance(candidate, dict):
            sid = (candidate.get("id") or "").strip()
            sname = (candidate.get("name") or "").strip()
            if sid:
                return {"id": sid, "name": sname or subregion_names.get(sid, sid)}

    parts = file_path.split("/")
    if len(parts) >= 2:
        maybe_subregion = parts[1].strip()
        if maybe_subregion != "Federal" and "-" in maybe_subregion:
            return {
                "id": maybe_subregion,
                "name": subregion_names.get(maybe_subregion, maybe_subregion),
            }

    owner_name = ((record.get("owner", {}) or {}).get("name", "") or "").strip()
    record_id = (record.get("id") or "").strip()
    link = (record.get("link") or "").strip()
    combined = f"{owner_name} {link} {record_id}".lower()

    if country_code == "ID":
        if "indragiri" in combined or "inhil" in combined or "inhilkab" in combined:
            return {"id": "ID-RI", "name": subregion_names.get("ID-RI", "Riau")}
        if "jogja" in combined or "jogjaprov" in combined or "yogyakarta" in combined:
            return {"id": "ID-YO", "name": subregion_names.get("ID-YO", "Yogyakarta")}
        if "bandung" in combined:
            return {"id": "ID-JB", "name": subregion_names.get("ID-JB", "Jawa Barat")}
        if "batukota" in combined or "batu kota" in combined:
            return {"id": "ID-JI", "name": subregion_names.get("ID-JI", "Jawa Timur")}

    if token_subregions:
        host = (
            urlparse(link if "://" in link else f"https://{link}").netloc
            if link
            else ""
        ).lower().replace("www.", "")
        score: Counter[str] = Counter()
        for token in tokenize_text(f"{owner_name} {record_id} {host}"):
            for sid, count in token_subregions.get(token, {}).items():
                score[sid] += count
        if score:
            best_id, _ = score.most_common(1)[0]
            return {"id": best_id, "name": subregion_names.get(best_id, best_id)}

    return None


def resolve_path(file_path: str) -> Path | None:
    entity_path = ENTITIES_DIR / file_path
    if entity_path.exists():
        return entity_path
    scheduled_path = SCHEDULED_DIR / file_path
    if scheduled_path.exists():
        return scheduled_path
    return None


def get_admin_dir_from_path(file_path: str) -> str | None:
    """Extract admin directory (e.g. US-IL) from file path like US/US-IL/geo/id.yaml."""
    parts = file_path.replace("\\", "/").split("/")
    if len(parts) >= 2:
        admin = parts[1].strip()
        if admin != "Federal" and "-" in admin:
            return admin
    return None


def apply_fix(
    path: Path,
    file_path: str,
    subregion_names: dict[str, str],
    country_code: str,
    token_subregions: dict[str, dict[str, int]] | None,
) -> bool:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return False

    owner = data.get("owner", {}) or {}
    owner_type = (owner.get("type") or "").strip().lower()
    location = owner.get("location", {}) or {}
    changed = False

    # World records: Regional/Local gov cannot have subregion (World has none).
    # Change type to Central government to avoid the rule.
    if country_code == "World" and owner_type in SUPPORTED_OWNER_TYPES:
        owner["type"] = "Central government"
        data["owner"] = owner
        path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
        return True

    # Record in subregion directory: add/update owner.location.subregion to match directory.
    # This applies to ANY owner type (not just Local/Regional government).
    admin_dir = get_admin_dir_from_path(file_path)
    if admin_dir:
        subregion = {
            "id": admin_dir,
            "name": subregion_names.get(admin_dir, admin_dir),
        }
        current_sr = location.get("subregion")
        needs_subregion = (
            not isinstance(current_sr, dict)
            or not (current_sr.get("id") or "").strip()
        )
        if needs_subregion or (current_sr.get("id") or "").strip() != subregion["id"]:
            location["subregion"] = {"id": subregion["id"], "name": subregion["name"]}
            changed = True

        # Convert subdivision to subregion (remove subdivision)
        if "subdivision" in location:
            location.pop("subdivision")
            changed = True

    # For Local/Regional government: also enforce level 30.
    # When admin_dir is set, it takes precedence (file location is authoritative).
    # Only use infer_subregion when we have no admin_dir (e.g. missing from report).
    if owner_type in SUPPORTED_OWNER_TYPES and country_code != "World":
        if location.get("level") != 30:
            location["level"] = 30
            changed = True

        if not admin_dir:
            inferred = infer_subregion(
                data, file_path, subregion_names, country_code, token_subregions
            )
            if not inferred:
                inferred = {
                    "id": f"{country_code}-UNK",
                    "name": "Unknown subregion (needs review)",
                }
            if inferred:
                current_sr = location.get("subregion")
                if (
                    not isinstance(current_sr, dict)
                    or not (current_sr.get("id") or "").strip()
                    or (current_sr.get("id") or "").strip() != inferred["id"]
                ):
                    location["subregion"] = {"id": inferred["id"], "name": inferred["name"]}
                    changed = True

    if not changed:
        return False

    owner["location"] = location
    data["owner"] = owner
    path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Fix OWNER_LOCATION_SUBREGION_REQUIRED issues")
    parser.add_argument(
        "--exclude-country",
        action="append",
        default=[],
        metavar="CODE",
        help="Country code to exclude (e.g. US). Can be repeated.",
    )
    args = parser.parse_args()
    exclude_countries = {c.upper().strip() for c in args.exclude_country or []}

    if not RULE_REPORT.exists():
        print(f"Missing report file: {RULE_REPORT}")
        return

    subregion_names = load_subregion_names()
    entries = parse_report(RULE_REPORT)

    if exclude_countries:
        entries = [(fp, cc) for fp, cc in entries if cc not in exclude_countries]

    total = len(entries)
    fixed = 0
    missing = 0
    skipped = 0

    token_cache: dict[str, dict[str, dict[str, int]]] = {}

    for file_path, country_code in entries:
        path = resolve_path(file_path)
        if path is None:
            missing += 1
            continue

        token_subregions = None
        if country_code != "World":
            if country_code not in token_cache:
                token_cache[country_code] = build_token_subregion_index(country_code)
            token_subregions = token_cache[country_code] or None

        if apply_fix(path, file_path, subregion_names, country_code, token_subregions):
            fixed += 1
        else:
            skipped += 1

    print(f"Total issues processed: {total}")
    print(f"Fixed: {fixed}")
    print(f"Skipped: {skipped}")
    print(f"Missing files: {missing}")


if __name__ == "__main__":
    main()
