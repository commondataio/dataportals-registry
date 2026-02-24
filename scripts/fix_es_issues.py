#!/usr/bin/env python3
"""
Fix issues listed in dataquality/countries/ES.txt.

This script applies deterministic, low-risk fixes for:
- OWNER_LOCATION_SUBREGION_REQUIRED
- PLACEHOLDER_OWNER_NAME
- PLACEHOLDER_TITLE
- SOFTWARE_EXPECTED_ENDPOINTS_MISSING

It intentionally skips DUPLICATE_LINK because that usually requires manual
deduplication decisions across records.
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse

import yaml


BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
ES_REPORT = BASE_DIR / "dataquality" / "countries" / "ES.txt"

PLACEHOLDER_OWNER_VALUES = {"n/a", "na", "none", "null", "not specified", "unknown", ""}


def parse_es_issues(path: Path) -> dict[str, set[str]]:
    text = path.read_text(encoding="utf-8")
    pattern = r"File: ([^\n]+)\nRecord ID: [^\n]+\nIssue: ([^\n]+)\nField: ([^\n]+)"
    parsed: dict[str, set[str]] = {}
    for file_path, issue_type, _field in re.findall(pattern, text):
        file_path = file_path.strip()
        parsed.setdefault(issue_type, set()).add(file_path)
    return parsed


def build_subregion_name_index() -> dict[str, str]:
    index: dict[str, str] = {}
    for path in (ENTITIES_DIR / "ES").glob("ES-*/*/*.yaml"):
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        candidates = []
        coverage = data.get("coverage", [])
        if isinstance(coverage, list) and coverage:
            loc = (coverage[0] or {}).get("location", {}) or {}
            if isinstance(loc.get("subregion"), dict):
                candidates.append(loc["subregion"])
            if isinstance(loc.get("subdivision"), dict):
                candidates.append(loc["subdivision"])

        owner_loc = ((data.get("owner", {}) or {}).get("location", {}) or {})
        if isinstance(owner_loc.get("subregion"), dict):
            candidates.append(owner_loc["subregion"])
        if isinstance(owner_loc.get("subdivision"), dict):
            candidates.append(owner_loc["subdivision"])

        for c in candidates:
            sid = (c.get("id") or "").strip()
            sname = (c.get("name") or "").strip()
            if sid and sname and sid not in index:
                index[sid] = sname
    return index


def infer_subregion(record: dict, file_path: str, subregion_names: dict[str, str]) -> dict | None:
    coverage = record.get("coverage", [])
    if isinstance(coverage, list) and coverage:
        for entry in coverage:
            loc = (entry or {}).get("location", {}) or {}
            if isinstance(loc.get("subregion"), dict) and loc["subregion"].get("id"):
                subregion = loc["subregion"]
                return {"id": subregion["id"], "name": subregion.get("name", subregion["id"])}
            if isinstance(loc.get("subdivision"), dict) and loc["subdivision"].get("id"):
                subdivision = loc["subdivision"]
                return {"id": subdivision["id"], "name": subdivision.get("name", subdivision["id"])}

    owner_loc = ((record.get("owner", {}) or {}).get("location", {}) or {})
    if isinstance(owner_loc.get("subregion"), dict) and owner_loc["subregion"].get("id"):
        subregion = owner_loc["subregion"]
        return {"id": subregion["id"], "name": subregion.get("name", subregion["id"])}
    if isinstance(owner_loc.get("subdivision"), dict) and owner_loc["subdivision"].get("id"):
        subdivision = owner_loc["subdivision"]
        return {"id": subdivision["id"], "name": subdivision.get("name", subdivision["id"])}

    parts = file_path.split("/")
    if len(parts) >= 2 and parts[1].startswith("ES-"):
        subregion_id = parts[1]
        return {"id": subregion_id, "name": subregion_names.get(subregion_id, subregion_id)}
    return None


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


def infer_owner_name(record: dict) -> str | None:
    owner = record.get("owner", {}) or {}
    owner_link = owner.get("link", "") or record.get("link", "")
    if not owner_link:
        return None
    p = urlparse(owner_link if "://" in owner_link else f"https://{owner_link}")
    domain = (p.netloc or owner_link).lower()
    domain = re.sub(r"^www\.", "", domain)
    primary = domain.split(".")[0]
    primary = re.sub(r"^(data|gis|geo|opendata|portal|maps?)\-?", "", primary)
    primary = primary.replace("-", " ").replace("_", " ").strip()
    if primary and primary not in PLACEHOLDER_OWNER_VALUES:
        return " ".join(x.capitalize() for x in primary.split())
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
        "es",
    }
    words = [t for t in tokens if t and t not in stop and not t.isdigit()]
    if words:
        label = " ".join(w.capitalize() for w in words[:3])
        return f"{label} {type_suffix}".strip()
    return f"Spain {type_suffix}".strip()


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
    elif software_id == "geonetwork":
        endpoints.append({"type": "geonetwork:api:records", "url": f"{base}/geonetwork/srv/api/records"})
    elif software_id == "geoserver":
        endpoints.append({"type": "ogc:wms", "url": f"{base}/geoserver/wms"})
        endpoints.append({"type": "ogc:wfs", "url": f"{base}/geoserver/wfs"})

    if not endpoints:
        endpoints.append({"type": "sitemap", "url": f"{base}/sitemap.xml"})
    return endpoints


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


def fix_owner_location_subregion_required(record: dict, file_path: str, subregion_names: dict[str, str]) -> bool:
    owner = record.get("owner", {}) or {}
    owner_type = (owner.get("type", "") or "").strip()
    if owner_type not in {"Local government", "Regional government"}:
        return False

    location = owner.get("location", {}) or {}
    changed = False
    if location.get("level") != 30:
        location["level"] = 30
        changed = True

    subregion = infer_subregion(record, file_path, subregion_names)
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


def fix_placeholder_title(record: dict) -> bool:
    name = (record.get("name", "") or "").strip()
    if not is_placeholder_title(name):
        return False
    new_title = generate_title(record)
    if not new_title or new_title == name:
        return False
    record["name"] = new_title
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
    if not isinstance(endpoints, list) or len(endpoints) == 0:
        return False
    changed = False
    if record.get("api") is not True:
        record["api"] = True
        changed = True
    if record.get("api_status") != "active":
        record["api_status"] = "active"
        changed = True
    return changed


def apply_issue_fix(file_path: str, issue_type: str, subregion_names: dict[str, str]) -> bool:
    full_path = ENTITIES_DIR / file_path
    if not full_path.exists():
        return False
    data = yaml.safe_load(full_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return False

    before = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)

    changed = False
    if issue_type == "OWNER_LOCATION_SUBREGION_REQUIRED":
        changed = fix_owner_location_subregion_required(data, file_path, subregion_names)
    elif issue_type == "PLACEHOLDER_OWNER_NAME":
        changed = fix_placeholder_owner_name(data)
    elif issue_type == "PLACEHOLDER_TITLE":
        changed = fix_placeholder_title(data)
    elif issue_type == "SOFTWARE_EXPECTED_ENDPOINTS_MISSING" or (
        issue_type and issue_type.startswith("SOFTWARE_EXPECTED_ENDPOINTS_MISSING_")
    ):
        changed = fix_expected_endpoints_missing(data)
    elif issue_type == "API_STATUS_MISMATCH":
        changed = fix_api_status_mismatch(data)

    if not changed:
        return False

    after = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)
    if before == after:
        return False

    full_path.write_text(after, encoding="utf-8")
    return True


def main() -> None:
    if not ES_REPORT.exists():
        print(f"ES report not found: {ES_REPORT}")
        return

    issues_by_type = parse_es_issues(ES_REPORT)
    subregion_names = build_subregion_name_index()
    supported = {
        "API_STATUS_MISMATCH",
        "OWNER_LOCATION_SUBREGION_REQUIRED",
        "PLACEHOLDER_OWNER_NAME",
        "PLACEHOLDER_TITLE",
        "SOFTWARE_EXPECTED_ENDPOINTS_MISSING",
    }

    def is_supported(t):
        return t in supported or (t and t.startswith("SOFTWARE_EXPECTED_ENDPOINTS_MISSING_"))

    fixed = 0
    skipped = 0
    for issue_type, files in sorted(issues_by_type.items()):
        if not is_supported(issue_type):
            print(f"Skipping unsupported issue type: {issue_type} ({len(files)} files)")
            skipped += len(files)
            continue
        issue_fixed = 0
        for file_path in sorted(files):
            if apply_issue_fix(file_path, issue_type, subregion_names):
                issue_fixed += 1
                fixed += 1
            else:
                skipped += 1
        print(f"{issue_type}: fixed {issue_fixed}/{len(files)}")

    print(f"\nTotal fixed: {fixed}")
    print(f"Total skipped: {skipped}")


if __name__ == "__main__":
    main()
