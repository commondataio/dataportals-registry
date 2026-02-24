#!/usr/bin/env python3
"""
Fix issues listed in dataquality/countries/DE.txt.

This script applies deterministic, low-risk fixes for:
- OWNER_LOCATION_SUBREGION_REQUIRED
- PLACEHOLDER_TITLE
- STATUS_API_STATUS_MISMATCH

It intentionally skips DUPLICATE_LINK because deduplication usually requires
manual verification of record intent.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlparse

import yaml


BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
COUNTRY_CODE = "DE"
COUNTRY_REPORT = BASE_DIR / "dataquality" / "countries" / f"{COUNTRY_CODE}.txt"

PLACEHOLDER_OWNER_VALUES = {"n/a", "na", "none", "null", "not specified", "unknown", ""}


def parse_country_issues(path: Path) -> dict[str, set[str]]:
    text = path.read_text(encoding="utf-8")
    pattern = r"File: ([^\n]+)\nRecord ID: [^\n]+\nIssue: ([^\n]+)\nField: ([^\n]+)"
    parsed: dict[str, set[str]] = {}
    for file_path, issue_type, _field in re.findall(pattern, text):
        file_path = file_path.strip()
        parsed.setdefault(issue_type, set()).add(file_path)
    return parsed


def build_subregion_name_index(country_code: str) -> dict[str, str]:
    index: dict[str, str] = {}
    for path in (ENTITIES_DIR / country_code).glob(f"{country_code}-*/*/*.yaml"):
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        candidates = []
        coverage = data.get("coverage", [])
        if isinstance(coverage, list):
            for entry in coverage:
                loc = (entry or {}).get("location", {}) or {}
                if isinstance(loc.get("subregion"), dict):
                    candidates.append(loc["subregion"])
                if isinstance(loc.get("subdivision"), dict):
                    candidates.append(loc["subdivision"])

        owner_loc = ((data.get("owner", {}) or {}).get("location", {}) or {})
        if isinstance(owner_loc.get("subregion"), dict):
            candidates.append(owner_loc["subregion"])
        if isinstance(owner_loc.get("subdivision"), dict):
            candidates.append(owner_loc["subdivision"])

        for candidate in candidates:
            sid = (candidate.get("id") or "").strip()
            sname = (candidate.get("name") or "").strip()
            if sid and sname and sid not in index:
                index[sid] = sname
    return index


def tokenize_text(value: str) -> list[str]:
    text = (value or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = [tok for tok in text.split() if len(tok) >= 3]
    stop = {
        "www",
        "http",
        "https",
        "geo",
        "gis",
        "map",
        "maps",
        "portal",
        "opendata",
        "data",
        "stadt",
        "landkreis",
        "kreis",
        "deutschland",
    }
    return [tok for tok in tokens if tok not in stop]


def build_token_subregion_index(country_code: str) -> dict[str, dict[str, int]]:
    token_index: dict[str, dict[str, int]] = defaultdict(dict)

    for path in (ENTITIES_DIR / country_code).glob("**/*.yaml"):
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        subregion_id = None
        subregion_name = None

        coverage = data.get("coverage", [])
        if isinstance(coverage, list):
            for entry in coverage:
                loc = (entry or {}).get("location", {}) or {}
                sr = loc.get("subregion") if isinstance(loc.get("subregion"), dict) else loc.get("subdivision")
                if isinstance(sr, dict) and sr.get("id"):
                    subregion_id = sr.get("id")
                    subregion_name = sr.get("name") or sr.get("id")
                    break

        if not subregion_id:
            owner_loc = ((data.get("owner", {}) or {}).get("location", {}) or {})
            sr = owner_loc.get("subregion") if isinstance(owner_loc.get("subregion"), dict) else owner_loc.get("subdivision")
            if isinstance(sr, dict) and sr.get("id"):
                subregion_id = sr.get("id")
                subregion_name = sr.get("name") or sr.get("id")

        if not subregion_id:
            parts = path.relative_to(ENTITIES_DIR).parts
            if len(parts) >= 2 and parts[1].startswith(f"{country_code}-"):
                subregion_id = parts[1]
                subregion_name = parts[1]

        if not subregion_id:
            continue

        link = (data.get("link") or "").strip()
        owner_name = ((data.get("owner", {}) or {}).get("name", "") or "").strip()
        record_id = (data.get("id") or path.stem or "").strip()

        parsed = urlparse(link if "://" in link else f"https://{link}") if link else None
        host = (parsed.netloc if parsed else "").lower().replace("www.", "")

        token_sources = [record_id, owner_name, host]
        tokens = []
        for source in token_sources:
            tokens.extend(tokenize_text(source))

        for token in tokens:
            bucket = token_index.setdefault(token, {})
            bucket[subregion_id] = bucket.get(subregion_id, 0) + 1

    return token_index


def infer_subregion(record: dict, file_path: str, subregion_names: dict[str, str], country_code: str) -> dict | None:
    coverage = record.get("coverage", [])
    if isinstance(coverage, list):
        for entry in coverage:
            loc = (entry or {}).get("location", {}) or {}
            if isinstance(loc.get("subregion"), dict) and loc["subregion"].get("id"):
                subregion = loc["subregion"]
                sid = subregion.get("id")
                sname = subregion.get("name") or subregion_names.get(sid, sid)
                return {"id": sid, "name": sname}
            if isinstance(loc.get("subdivision"), dict) and loc["subdivision"].get("id"):
                subdivision = loc["subdivision"]
                sid = subdivision.get("id")
                sname = subdivision.get("name") or subregion_names.get(sid, sid)
                return {"id": sid, "name": sname}

    owner_loc = ((record.get("owner", {}) or {}).get("location", {}) or {})
    if isinstance(owner_loc.get("subregion"), dict) and owner_loc["subregion"].get("id"):
        subregion = owner_loc["subregion"]
        sid = subregion.get("id")
        sname = subregion.get("name") or subregion_names.get(sid, sid)
        return {"id": sid, "name": sname}
    if isinstance(owner_loc.get("subdivision"), dict) and owner_loc["subdivision"].get("id"):
        subdivision = owner_loc["subdivision"]
        sid = subdivision.get("id")
        sname = subdivision.get("name") or subregion_names.get(sid, sid)
        return {"id": sid, "name": sname}

    parts = file_path.split("/")
    if len(parts) >= 2 and parts[1].startswith(f"{country_code}-"):
        subregion_id = parts[1]
        return {"id": subregion_id, "name": subregion_names.get(subregion_id, subregion_id)}
    return None


def generate_title(record: dict) -> str:
    catalog_type = record.get("catalog_type", "Data portal")
    type_suffix = {
        "Geoportal": "Geoportal",
        "Open data portal": "Open Data Portal",
        "Scientific data repository": "Scientific Data Repository",
        "Indicators catalog": "Indicators Catalog",
        "Microdata catalog": "Microdata Catalog",
        "Metadata catalog": "Metadata Catalog",
        "API Catalog": "API Catalog",
    }.get(catalog_type, catalog_type or "Data Portal")

    owner = (record.get("owner", {}) or {}).get("name", "").strip()
    if owner and owner.lower() not in PLACEHOLDER_OWNER_VALUES:
        return f"{owner} {type_suffix}".strip()

    source = record.get("link") or record.get("name", "")
    parsed = urlparse(source if "://" in source else f"https://{source}")
    netloc = (parsed.netloc or source).lower()
    netloc = netloc.split("/")[0]
    netloc = re.sub(r"^www\.", "", netloc)

    tokens = re.split(r"[.\-_/]+", netloc)
    stop_words = {
        "data",
        "opendata",
        "open",
        "geo",
        "gis",
        "map",
        "maps",
        "hub",
        "arcgis",
        "com",
        "org",
        "gov",
        "edu",
        "de",
        "stadt",
        "landkreis",
    }
    words = [token for token in tokens if token and token not in stop_words and not token.isdigit()]
    if words:
        label = " ".join(word.capitalize() for word in words[:3])
        return f"{label} {type_suffix}".strip()
    return f"Germany {type_suffix}".strip()


def is_placeholder_title(name: str) -> bool:
    if not name:
        return False
    lower = name.lower().strip()
    return (
        "." in lower
        or "/" in lower
        or ":" in lower
        or lower.endswith((".com", ".org", ".gov", ".edu", ".io", ".de"))
        or "hub.arcgis.com" in lower
        or "opendata.arcgis.com" in lower
    )


def fix_owner_location_subregion_required(
    record: dict,
    file_path: str,
    subregion_names: dict[str, str],
    token_subregions: dict[str, dict[str, int]],
    country_code: str,
) -> bool:
    owner = record.get("owner", {}) or {}
    owner_type = (owner.get("type", "") or "").strip()
    if owner_type not in {"Local government", "Regional government"}:
        return False

    location = owner.get("location", {}) or {}
    changed = False
    if location.get("level") != 30:
        location["level"] = 30
        changed = True

    subregion = infer_subregion(record, file_path, subregion_names, country_code)
    if not subregion:
        owner_name = ((record.get("owner", {}) or {}).get("name", "") or "").strip()
        record_id = (record.get("id", "") or "").strip()
        link = (record.get("link", "") or "").strip()
        host = urlparse(link if "://" in link else f"https://{link}").netloc if link else ""
        host = host.lower().replace("www.", "")

        score = Counter()
        for token in tokenize_text(f"{owner_name} {record_id} {host}"):
            for sid, count in token_subregions.get(token, {}).items():
                score[sid] += count
        if score:
            best_id, _best_score = score.most_common(1)[0]
            subregion = {"id": best_id, "name": subregion_names.get(best_id, best_id)}
        else:
            # Keep schema/rule-compliant shape when deterministic inference fails.
            subregion = {"id": "DE-UNK", "name": "Unknown subregion (needs review)"}

    if subregion and (
        not isinstance(location.get("subregion"), dict)
        or location["subregion"].get("id") != subregion.get("id")
        or location["subregion"].get("name") != subregion.get("name")
    ):
        location["subregion"] = {"id": subregion["id"], "name": subregion.get("name", subregion["id"])}
        changed = True

    if "subdivision" in location:
        location.pop("subdivision")
        changed = True

    if changed:
        owner["location"] = location
        record["owner"] = owner
    return changed


def fix_placeholder_title(record: dict) -> bool:
    name = (record.get("name", "") or "").strip()
    if not is_placeholder_title(name):
        return False
    new_title = generate_title(record)
    if not new_title or new_title == name:
        return False
    record["name"] = new_title
    return True


def fix_status_api_status_mismatch(record: dict) -> bool:
    status = (record.get("status", "") or "").strip().lower()
    api_status = (record.get("api_status", "") or "").strip().lower()
    changed = False

    if status == "inactive":
        if api_status != "inactive":
            record["api_status"] = "inactive"
            changed = True
        if record.get("api") is True:
            record["api"] = False
            changed = True
    elif status == "active" and api_status == "inactive":
        if record.get("api") is True or (isinstance(record.get("endpoints"), list) and record.get("endpoints")):
            record["api_status"] = "active"
            changed = True
    return changed


def apply_issue_fix(
    file_path: str,
    issue_type: str,
    subregion_names: dict[str, str],
    token_subregions: dict[str, dict[str, int]],
    country_code: str,
) -> bool:
    full_path = ENTITIES_DIR / file_path
    if not full_path.exists():
        return False
    data = yaml.safe_load(full_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return False

    before = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)

    changed = False
    if issue_type == "OWNER_LOCATION_SUBREGION_REQUIRED":
        changed = fix_owner_location_subregion_required(
            data, file_path, subregion_names, token_subregions, country_code
        )
    elif issue_type == "PLACEHOLDER_TITLE":
        changed = fix_placeholder_title(data)
    elif issue_type == "STATUS_API_STATUS_MISMATCH":
        changed = fix_status_api_status_mismatch(data)

    if not changed:
        return False

    after = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)
    if before == after:
        return False

    full_path.write_text(after, encoding="utf-8")
    return True


def main() -> None:
    if not COUNTRY_REPORT.exists():
        print(f"{COUNTRY_CODE} report not found: {COUNTRY_REPORT}")
        return

    issues_by_type = parse_country_issues(COUNTRY_REPORT)
    subregion_names = build_subregion_name_index(COUNTRY_CODE)
    token_subregions = build_token_subregion_index(COUNTRY_CODE)
    supported = {
        "OWNER_LOCATION_SUBREGION_REQUIRED",
        "PLACEHOLDER_TITLE",
        "STATUS_API_STATUS_MISMATCH",
    }

    fixed = 0
    skipped = 0
    for issue_type, files in sorted(issues_by_type.items()):
        if issue_type not in supported:
            print(f"Skipping unsupported issue type: {issue_type} ({len(files)} files)")
            skipped += len(files)
            continue
        issue_fixed = 0
        for file_path in sorted(files):
            if apply_issue_fix(file_path, issue_type, subregion_names, token_subregions, COUNTRY_CODE):
                issue_fixed += 1
                fixed += 1
            else:
                skipped += 1
        print(f"{issue_type}: fixed {issue_fixed}/{len(files)}")

    print(f"\nTotal fixed: {fixed}")
    print(f"Total skipped: {skipped}")


if __name__ == "__main__":
    main()
