#!/usr/bin/env python3
"""
Fix issues listed in dataquality/countries/US.txt.

This script applies deterministic, low-risk fixes for:
- MISSING_CONTENT_TYPES
- OWNER_LOCATION_SUBREGION_REQUIRED
- PLACEHOLDER_OWNER_NAME
- PLACEHOLDER_TITLE
- SHORT_DESCRIPTION
- SOFTWARE_EXPECTED_ENDPOINTS_MISSING

It intentionally skips DUPLICATE_LINK because that usually requires manual
deduplication decisions across records.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urlparse

import yaml


BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
US_REPORT = BASE_DIR / "dataquality" / "countries" / "US.txt"
PRIMARY_PRIORITY_FILE = BASE_DIR / "dataquality" / "primary_priority.jsonl"
FULL_REPORT_FILE = BASE_DIR / "dataquality" / "full_report.jsonl"


US_SUBREGION_NAMES = {
    "US-AL": "Alabama",
    "US-AK": "Alaska",
    "US-AZ": "Arizona",
    "US-AR": "Arkansas",
    "US-CA": "California",
    "US-CO": "Colorado",
    "US-CT": "Connecticut",
    "US-DE": "Delaware",
    "US-FL": "Florida",
    "US-GA": "Georgia",
    "US-HI": "Hawaii",
    "US-ID": "Idaho",
    "US-IL": "Illinois",
    "US-IN": "Indiana",
    "US-IA": "Iowa",
    "US-KS": "Kansas",
    "US-KY": "Kentucky",
    "US-LA": "Louisiana",
    "US-ME": "Maine",
    "US-MD": "Maryland",
    "US-MA": "Massachusetts",
    "US-MI": "Michigan",
    "US-MN": "Minnesota",
    "US-MS": "Mississippi",
    "US-MO": "Missouri",
    "US-MT": "Montana",
    "US-NE": "Nebraska",
    "US-NV": "Nevada",
    "US-NH": "New Hampshire",
    "US-NJ": "New Jersey",
    "US-NM": "New Mexico",
    "US-NY": "New York",
    "US-NC": "North Carolina",
    "US-ND": "North Dakota",
    "US-OH": "Ohio",
    "US-OK": "Oklahoma",
    "US-OR": "Oregon",
    "US-PA": "Pennsylvania",
    "US-RI": "Rhode Island",
    "US-SC": "South Carolina",
    "US-SD": "South Dakota",
    "US-TN": "Tennessee",
    "US-TX": "Texas",
    "US-UT": "Utah",
    "US-VT": "Vermont",
    "US-VA": "Virginia",
    "US-WA": "Washington",
    "US-WV": "West Virginia",
    "US-WI": "Wisconsin",
    "US-WY": "Wyoming",
    "US-DC": "District of Columbia",
    "US-PR": "Puerto Rico",
    "US-GU": "Guam",
    "US-VI": "U.S. Virgin Islands",
    "US-AS": "American Samoa",
    "US-MP": "Northern Mariana Islands",
}


PLACEHOLDER_OWNER_VALUES = {"n/a", "na", "none", "null", "not specified", "unknown", ""}


def parse_us_issues(path: Path) -> dict[str, set[str]]:
    text = path.read_text(encoding="utf-8")
    pattern = r"File: ([^\n]+)\nRecord ID: [^\n]+\nIssue: ([^\n]+)\nField: ([^\n]+)"
    parsed: dict[str, set[str]] = {}
    for file_path, issue_type, _field in re.findall(pattern, text):
        parsed.setdefault(issue_type, set()).add(file_path.strip())
    return parsed


def parse_us_issues_from_primary_priority(path: Path) -> dict[str, set[str]]:
    parsed: dict[str, set[str]] = {}
    if not path.exists():
        return parsed

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            if obj.get("country_code") != "US":
                continue

            file_path = (obj.get("file_path") or "").strip()
            if not file_path:
                continue

            for issue in obj.get("issues", []):
                issue_type = (issue or {}).get("issue_type")
                if not issue_type:
                    continue
                parsed.setdefault(issue_type, set()).add(file_path)

    return parsed


def parse_us_issues_from_full_report(path: Path) -> dict[str, set[str]]:
    parsed: dict[str, set[str]] = {}
    if not path.exists():
        return parsed

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("country_code") != "US":
                continue
            issue_type = (obj.get("issue_type") or "").strip()
            file_path = (obj.get("file_path") or "").strip()
            if not issue_type or not file_path:
                continue
            parsed.setdefault(issue_type, set()).add(file_path)
    return parsed


def get_base_url(link: str) -> str:
    if not link:
        return ""
    p = urlparse(link)
    scheme = p.scheme if p.scheme else "https"
    if p.netloc:
        return f"{scheme}://{p.netloc}"
    cleaned = link.replace("http://", "").replace("https://", "").strip("/")
    if "/" in cleaned:
        cleaned = cleaned.split("/")[0]
    return f"{scheme}://{cleaned}" if cleaned else ""


def infer_subregion_from_file_path(file_path: str) -> dict | None:
    parts = file_path.split("/")
    if len(parts) >= 2 and parts[1].startswith("US-"):
        subregion_id = parts[1]
        name = US_SUBREGION_NAMES.get(subregion_id)
        if name:
            return {"id": subregion_id, "name": name}
    return None


def infer_subregion(record: dict, file_path: str) -> dict | None:
    coverage = record.get("coverage", [])
    if coverage and isinstance(coverage, list):
        loc = (coverage[0] or {}).get("location", {}) or {}
        if isinstance(loc.get("subregion"), dict) and loc["subregion"].get("id"):
            return loc["subregion"]
        if isinstance(loc.get("subdivision"), dict) and loc["subdivision"].get("id"):
            return loc["subdivision"]

    owner = record.get("owner", {}) or {}
    owner_loc = owner.get("location", {}) or {}
    if isinstance(owner_loc.get("subregion"), dict) and owner_loc["subregion"].get("id"):
        return owner_loc["subregion"]
    if isinstance(owner_loc.get("subdivision"), dict) and owner_loc["subdivision"].get("id"):
        return owner_loc["subdivision"]

    return infer_subregion_from_file_path(file_path)


def generate_title(record: dict) -> str:
    catalog_type = record.get("catalog_type", "Data portal")
    type_suffix = {
        "Geoportal": "Geoportal",
        "Open data portal": "Open Data Portal",
        "Scientific data repository": "Scientific Data Repository",
        "Indicators catalog": "Indicators Catalog",
    }.get(catalog_type, catalog_type or "Data Portal")

    owner = (record.get("owner", {}) or {}).get("name", "").strip()
    if owner and owner.lower() not in PLACEHOLDER_OWNER_VALUES:
        return f"{owner} {type_suffix}".strip()

    source = record.get("link") or record.get("name", "")
    p = urlparse(source if "://" in source else f"https://{source}")
    netloc = (p.netloc or source).lower()
    netloc = netloc.split("/")[0]
    netloc = re.sub(r"^www\.", "", netloc)

    tokens = re.split(r"[.\-_/]+", netloc)
    stop = {
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
        "us",
    }
    words = [t for t in tokens if t and t not in stop and not t.isdigit()]
    if words:
        label = " ".join(w.capitalize() for w in words[:3])
        return f"{label} {type_suffix}".strip()

    return f"US {type_suffix}".strip()


def infer_owner_name(record: dict) -> str | None:
    owner = record.get("owner", {}) or {}
    owner_link = owner.get("link", "") or record.get("link", "")
    if owner_link:
        p = urlparse(owner_link if "://" in owner_link else f"https://{owner_link}")
        domain = (p.netloc or owner_link).lower()
        domain = re.sub(r"^www\.", "", domain)
        primary = domain.split(".")[0]
        primary = re.sub(r"^(data|gis|geo|opendata|portal|maps?)\-?", "", primary)
        primary = primary.replace("-", " ").replace("_", " ").strip()
        if primary and primary not in PLACEHOLDER_OWNER_VALUES:
            return " ".join(x.capitalize() for x in primary.split())
    return None


def infer_endpoints(record: dict) -> list[dict]:
    software_id = ((record.get("software", {}) or {}).get("id", "") or "").lower()
    link = record.get("link", "")
    base = get_base_url(link)
    if not base:
        return []

    endpoints: list[dict] = []
    if software_id in {"ckan", "dkan"}:
        endpoints.append(
            {"type": "ckan:api", "url": f"{base}/api/3/action/package_list", "version": "3.0"}
        )
    elif software_id == "arcgisserver":
        endpoints.append({"type": "arcgis:rest:info", "url": f"{base}/arcgis/rest/info?f=pjson"})
        endpoints.append({"type": "arcgis:rest:services", "url": f"{base}/arcgis/rest/services?f=pjson"})
    elif software_id == "dspace":
        endpoints.append({"type": "dspace:rest", "url": f"{base}/server/api"})
    elif software_id == "galaxy":
        endpoints.append({"type": "galaxy:api", "url": f"{base}/api/version"})
    elif software_id == "geoblacklight":
        endpoints.append({"type": "geoblacklight:catalog", "url": f"{base}/catalog.json"})

    if not endpoints:
        endpoints.append({"type": "sitemap", "url": f"{base}/sitemap.xml"})
    return endpoints


def fix_missing_content_types(record: dict) -> bool:
    cts = record.get("content_types", [])
    if cts:
        return False
    ctype = record.get("catalog_type", "")
    if ctype == "Geoportal":
        record["content_types"] = ["dataset", "map_layer"]
    elif ctype == "Scientific data repository":
        record["content_types"] = ["dataset", "document"]
    else:
        record["content_types"] = ["dataset"]
    return True


def fix_owner_location_subregion_required(record: dict, file_path: str) -> bool:
    owner = record.get("owner", {}) or {}
    owner_type = (owner.get("type", "") or "").strip()
    if owner_type not in {"Local government", "Regional government"}:
        return False

    location = owner.get("location", {}) or {}
    changed = False
    if location.get("level") != 30:
        location["level"] = 30
        changed = True

    subregion = infer_subregion(record, file_path)
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


def fix_placeholder_owner_name(record: dict) -> bool:
    owner = record.get("owner", {}) or {}
    current = (owner.get("name", "") or "").strip()
    if current.lower() not in PLACEHOLDER_OWNER_VALUES:
        return False

    inferred = infer_owner_name(record)
    if not inferred:
        inferred = (record.get("name", "") or "").strip()
    if not inferred:
        return False
    owner["name"] = inferred
    record["owner"] = owner
    return True


def is_placeholder_title(name: str) -> bool:
    if not name:
        return False
    lower = name.lower().strip()
    return (
        "." in lower
        or "/" in lower
        or lower.endswith((".com", ".org", ".gov", ".edu", ".io"))
        or "hub.arcgis.com" in lower
        or "opendata.arcgis.com" in lower
    )


def fix_placeholder_title(record: dict) -> bool:
    name = (record.get("name", "") or "").strip()
    if not is_placeholder_title(name):
        return False
    new_title = generate_title(record)
    if not new_title or new_title == name:
        return False
    record["name"] = new_title
    return True


def fix_short_description(record: dict) -> bool:
    desc = (record.get("description", "") or "").strip()
    if len(desc) >= 40:
        return False
    catalog_type = record.get("catalog_type", "Data portal")
    owner_name = ((record.get("owner", {}) or {}).get("name", "") or "").strip()
    link = record.get("link", "")
    base = get_base_url(link)
    owner_part = f" managed by {owner_name}" if owner_name else ""
    record["description"] = (
        f"{record.get('name', 'This portal')} is a {catalog_type.lower()}{owner_part}, "
        f"providing public access to datasets and related resources at {base or link}."
    )
    return True


def fix_expected_endpoints_missing(record: dict) -> bool:
    endpoints = record.get("endpoints", [])
    if isinstance(endpoints, list) and len(endpoints) > 0:
        return False
    inferred = infer_endpoints(record)
    if not inferred:
        return False
    record["endpoints"] = inferred
    return True


def fix_api_status_mismatch(record: dict) -> bool:
    endpoints = record.get("endpoints", [])
    if isinstance(endpoints, list) and len(endpoints) > 0:
        changed = False
        if record.get("api") is not True:
            record["api"] = True
            changed = True
        if record.get("api_status") != "active":
            record["api_status"] = "active"
            changed = True
        return changed
    return False


FIXERS = {
    "API_STATUS_MISMATCH": fix_api_status_mismatch,
    "MISSING_CONTENT_TYPES": fix_missing_content_types,
    "OWNER_LOCATION_SUBREGION_REQUIRED": fix_owner_location_subregion_required,
    "PLACEHOLDER_OWNER_NAME": fix_placeholder_owner_name,
    "PLACEHOLDER_TITLE": fix_placeholder_title,
    "SHORT_DESCRIPTION": fix_short_description,
    "SOFTWARE_EXPECTED_ENDPOINTS_MISSING": fix_expected_endpoints_missing,
}


def _get_fixer(issue_type: str):
    """Resolve fixer for issue type, including SOFTWARE_EXPECTED_ENDPOINTS_MISSING_* pattern."""
    if issue_type in FIXERS:
        return FIXERS[issue_type]
    if issue_type and issue_type.startswith("SOFTWARE_EXPECTED_ENDPOINTS_MISSING_"):
        return fix_expected_endpoints_missing
    return None


def apply_issue_fix(file_path: str, issue_type: str) -> bool:
    full_path = ENTITIES_DIR / file_path
    if not full_path.exists():
        return False

    data = yaml.safe_load(full_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return False

    before = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)

    fixer = _get_fixer(issue_type)
    if not fixer:
        return False
    if issue_type == "OWNER_LOCATION_SUBREGION_REQUIRED":
        changed = fixer(data, file_path)
    else:
        changed = fixer(data)
    if not changed:
        return False

    after = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)
    if before == after:
        return False

    full_path.write_text(after, encoding="utf-8")
    return True


def main() -> None:
    if not US_REPORT.exists():
        print(f"US report not found: {US_REPORT}")
        return

    issues_by_type = parse_us_issues(US_REPORT)
    primary_issues = parse_us_issues_from_primary_priority(PRIMARY_PRIORITY_FILE)
    for issue_type, files in primary_issues.items():
        issues_by_type.setdefault(issue_type, set()).update(files)
    full_issues = parse_us_issues_from_full_report(FULL_REPORT_FILE)
    for issue_type, files in full_issues.items():
        issues_by_type.setdefault(issue_type, set()).update(files)
    fixed = 0
    skipped = 0

    for issue_type, files in sorted(issues_by_type.items()):
        if _get_fixer(issue_type) is None:
            print(f"Skipping unsupported issue type: {issue_type} ({len(files)} files)")
            skipped += len(files)
            continue
        issue_fixed = 0
        for file_path in sorted(files):
            if apply_issue_fix(file_path, issue_type):
                issue_fixed += 1
                fixed += 1
            else:
                skipped += 1
        print(f"{issue_type}: fixed {issue_fixed}/{len(files)}")

    print(f"\nTotal fixed: {fixed}")
    print(f"Total skipped: {skipped}")


if __name__ == "__main__":
    main()
