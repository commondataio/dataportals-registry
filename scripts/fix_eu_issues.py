#!/usr/bin/env python3
"""
Fix issues listed in dataquality/countries/EU.txt.

This script applies deterministic, low-risk fixes for:
- MISSING_ENDPOINTS
- OWNER_LOCATION_SUBREGION_REQUIRED
- PLACEHOLDER_OWNER_NAME
- PLACEHOLDER_TITLE
- SOFTWARE_EXPECTED_ENDPOINTS_MISSING
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse

import yaml


BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
EU_REPORT = BASE_DIR / "dataquality" / "countries" / "EU.txt"

PLACEHOLDER_OWNER_VALUES = {"n/a", "na", "none", "null", "not specified", "unknown", ""}


def parse_eu_issues(path: Path) -> dict[str, set[str]]:
    text = path.read_text(encoding="utf-8")
    pattern = r"File: ([^\n]+)\nRecord ID: [^\n]+\nIssue: ([^\n]+)\nField: ([^\n]+)"
    parsed: dict[str, set[str]] = {}
    for file_path, issue_type, _field in re.findall(pattern, text):
        file_path = file_path.strip()
        parsed.setdefault(issue_type, set()).add(file_path)
    return parsed


def get_base_url(link: str) -> str:
    if not link:
        return ""
    parsed = urlparse(link if "://" in link else f"https://{link}")
    if parsed.netloc:
        scheme = parsed.scheme or "https"
        return f"{scheme}://{parsed.netloc}"
    cleaned = link.replace("http://", "").replace("https://", "").strip("/")
    if "/" in cleaned:
        cleaned = cleaned.split("/")[0]
    return f"https://{cleaned}" if cleaned else ""


def build_subregion_name_index() -> dict[str, str]:
    index: dict[str, str] = {}
    for path in (ENTITIES_DIR / "EU").glob("EU-*/*/*.yaml"):
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        coverage = data.get("coverage", [])
        if isinstance(coverage, list):
            for entry in coverage:
                location = (entry or {}).get("location", {}) or {}
                for key in ("subregion", "subdivision"):
                    candidate = location.get(key)
                    if isinstance(candidate, dict):
                        sid = (candidate.get("id") or "").strip()
                        sname = (candidate.get("name") or "").strip()
                        if sid and sname and sid not in index:
                            index[sid] = sname

        owner_location = ((data.get("owner", {}) or {}).get("location", {}) or {})
        for key in ("subregion", "subdivision"):
            candidate = owner_location.get(key)
            if isinstance(candidate, dict):
                sid = (candidate.get("id") or "").strip()
                sname = (candidate.get("name") or "").strip()
                if sid and sname and sid not in index:
                    index[sid] = sname
    return index


def infer_subregion(record: dict, file_path: str, subregion_names: dict[str, str]) -> dict | None:
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
            return {"id": maybe_subregion, "name": subregion_names.get(maybe_subregion, maybe_subregion)}

    owner_name = ((record.get("owner", {}) or {}).get("name", "") or "").lower()
    link = (record.get("link") or "").lower()
    if "olomouc" in owner_name or "olomouc" in link:
        return {"id": "CZ-71", "name": "Olomoucky kraj"}
    if "region sud" in owner_name or "regionsud" in link:
        return {"id": "FR-PAC", "name": "Provence-Alpes-Cote d'Azur"}
    if "venice" in owner_name or "seastorms" in link:
        return {"id": "IT-34", "name": "Veneto"}
    if "adriatic ionian" in owner_name or "portodimare" in link:
        return {"id": "EUSAIR", "name": "Adriatic-Ionian Region"}

    return None


def infer_owner_name(record: dict) -> str | None:
    owner = record.get("owner", {}) or {}
    owner_link = owner.get("link", "") or record.get("link", "")
    if not owner_link:
        return None
    parsed = urlparse(owner_link if "://" in owner_link else f"https://{owner_link}")
    domain = (parsed.netloc or owner_link).lower()
    domain = re.sub(r"^www\.", "", domain)
    primary = domain.split(".")[0]
    primary = re.sub(r"^(data|gis|geo|opendata|portal|maps?)\-?", "", primary)
    primary = primary.replace("-", " ").replace("_", " ").strip()
    if primary and primary not in PLACEHOLDER_OWNER_VALUES:
        return " ".join(chunk.capitalize() for chunk in primary.split())
    return None


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

    owner_name = ((record.get("owner", {}) or {}).get("name", "") or "").strip()
    if owner_name and owner_name.lower() not in PLACEHOLDER_OWNER_VALUES:
        return f"{owner_name} {type_suffix}".strip()

    source = record.get("link") or record.get("name", "")
    parsed = urlparse(source if "://" in source else f"https://{source}")
    netloc = (parsed.netloc or source).lower().split("/")[0]
    netloc = re.sub(r"^www\.", "", netloc)
    tokens = re.split(r"[.\-_/]+", netloc)
    stop_tokens = {
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
        "eu",
    }
    words = [token for token in tokens if token and token not in stop_tokens and not token.isdigit()]
    if words:
        label = " ".join(word.capitalize() for word in words[:4])
        return f"{label} {type_suffix}".strip()
    return f"European Union {type_suffix}".strip()


def infer_endpoints(record: dict) -> list[dict]:
    software_id = ((record.get("software", {}) or {}).get("id", "") or "").lower()
    link = record.get("link", "")
    base = get_base_url(link)
    if not base:
        return []

    endpoints: list[dict] = []

    if software_id in {"ckan", "dkan"}:
        endpoints.append({"type": "ckan:package-list", "url": f"{base}/api/3/action/package_list", "version": "3"})
    elif software_id == "arcgisserver":
        endpoints.append({"type": "arcgis:rest:services", "url": f"{base}/rest/services?f=pjson"})
    elif software_id == "geoserver":
        endpoints.append(
            {
                "type": "wms130",
                "url": f"{base}/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities",
                "version": "1.3.0",
            }
        )
    elif software_id == "geonetwork":
        if "/geonetwork" in (record.get("link", "") or ""):
            endpoints.append({"type": "geonetwork:api:records", "url": f"{base}/srv/api/records"})
        else:
            endpoints.append({"type": "geonetwork:api:records", "url": f"{base}/geonetwork/srv/api/records"})
    elif software_id == "galaxy":
        endpoints.append({"type": "galaxy:api", "url": f"{base}/api"})
    elif software_id == "lizmap":
        endpoints.append({"type": "lizmap:service", "url": f"{base}/index.php/lizmap/service/"})
    elif software_id == "eurostat":
        endpoints.append({"type": "eurostat:json", "url": f"{base}/api/dissemination/statistics/1.0/data"})
    elif software_id == "sdmxri":
        endpoints.append({"type": "sdmxri:dataflow", "url": f"{base}/SDMX-WS/rest/dataflow"})
    elif software_id == "ecb":
        endpoints.append({"type": "sdmx:data", "url": f"{base}/service/data"})
    elif software_id == "fusionregistry":
        endpoints.append({"type": "fusionregistry:rest", "url": f"{base}/FusionRegistry/ws/rest"})
    elif software_id == "obibamica":
        endpoints.append({"type": "mica:api", "url": f"{base}/api"})

    if not endpoints:
        endpoints.append({"type": "sitemap", "url": f"{base}/sitemap.xml"})
    return endpoints


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
    elif issue_type in {"SOFTWARE_EXPECTED_ENDPOINTS_MISSING", "MISSING_ENDPOINTS"} or (
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
    if not EU_REPORT.exists():
        print(f"EU report not found: {EU_REPORT}")
        return

    issues_by_type = parse_eu_issues(EU_REPORT)
    subregion_names = build_subregion_name_index()
    supported = {
        "API_STATUS_MISMATCH",
        "MISSING_ENDPOINTS",
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
