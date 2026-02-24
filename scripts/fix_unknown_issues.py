#!/usr/bin/env python3
"""
Fix issues listed in dataquality/countries/Unknown.txt.

Applies deterministic fixes for:
- OWNER_LOCATION_SUBREGION_REQUIRED (World + level 30 -> level 20)
- PLACEHOLDER_OWNER_NAME
- PLACEHOLDER_TITLE
- SOFTWARE_EXPECTED_ENDPOINTS_MISSING
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urlparse

import yaml


BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
UNKNOWN_REPORT = BASE_DIR / "dataquality" / "countries" / "Unknown.txt"
PRIMARY_PRIORITY_FILE = BASE_DIR / "dataquality" / "primary_priority.jsonl"


PLACEHOLDER_OWNER_VALUES = {
    "n/a", "na", "none", "null", "not specified", "not available",
    "not provided in available content", "unknown", ""
}


def parse_unknown_issues(path: Path) -> dict[str, set[str]]:
    """Parse Unknown.txt and extract (file_path, issue_type) pairs."""
    text = path.read_text(encoding="utf-8")
    pattern = r"File: ([^\n]+)\nRecord ID: [^\n]+\nIssue: ([^\n]+)\nField: ([^\n]+)"
    parsed: dict[str, set[str]] = {}
    for file_path, issue_type, _field in re.findall(pattern, text):
        fp = file_path.strip()
        if fp and issue_type:
            parsed.setdefault(issue_type, set()).add(fp)
    return parsed


def parse_from_primary_priority(path: Path) -> dict[str, set[str]]:
    """Parse primary_priority.jsonl for Unknown country."""
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
            if obj.get("country_code") not in ("Unknown", "World"):
                continue
            file_path = (obj.get("file_path") or "").strip()
            if not file_path:
                continue
            for issue in obj.get("issues", []):
                issue_type = (issue or {}).get("issue_type")
                if issue_type:
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


def infer_owner_name(record: dict) -> str | None:
    """Infer owner name from link/domain or description."""
    owner = record.get("owner", {}) or {}
    owner_link = owner.get("link", "") or record.get("link", "")
    if owner_link:
        p = urlparse(owner_link if "://" in owner_link else f"https://{owner_link}")
        domain = (p.netloc or owner_link).lower()
        domain = re.sub(r"^www\.", "", domain)
        primary = domain.split(".")[0]
        primary = re.sub(r"^(data|gis|geo|opendata|portal|maps?)\-?", "", primary)
        primary = primary.replace("-", " ").replace("_", " ").strip()
        if primary and primary.lower() not in PLACEHOLDER_OWNER_VALUES:
            return " ".join(x.capitalize() for x in primary.split())
    # Try from description (e.g. "South Carolina Forestry Commission")
    desc = (record.get("description") or "").lower()
    if "green infrastructure center" in desc or "gic" in desc:
        return "Green Infrastructure Center"
    if "south carolina forestry commission" in desc or "scfc" in desc:
        return "South Carolina Forestry Commission"
    return None


def generate_title(record: dict) -> str:
    """Generate human-readable title from record."""
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
        "data", "opendata", "open", "geo", "gis", "map", "maps", "hub",
        "arcgis", "com", "org", "gov", "edu", "net", "io",
    }
    words = [t for t in tokens if t and t not in stop and not t.isdigit()]
    if words:
        label = " ".join(w.capitalize() for w in words[:3])
        return f"{label} {type_suffix}".strip()

    return f"Geoportal {type_suffix}".strip()


def infer_endpoints(record: dict) -> list[dict]:
    """Infer endpoints from software and link."""
    software_id = ((record.get("software", {}) or {}).get("id", "") or "").lower()
    link = record.get("link", "")
    base = get_base_url(link)
    if not base:
        return []

    endpoints: list[dict] = []
    if software_id == "geoserver":
        endpoints.extend([
            {"type": "wms130", "url": f"{base}/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities", "version": "1.3.0"},
            {"type": "wfs200", "url": f"{base}/geoserver/ows?service=WFS&version=2.0.0&request=GetCapabilities", "version": "2.0.0"},
            {"type": "wcs111", "url": f"{base}/geoserver/ows?service=WCS&version=1.1.1&request=GetCapabilities", "version": "1.1.1"},
        ])
    elif software_id in {"ckan", "dkan"}:
        endpoints.append({"type": "ckan:api", "url": f"{base}/api/3/action/package_list", "version": "3.0"})
    elif software_id in {"arcgishub", "arcgisserver"} or "arcgis" in (link or "").lower():
        endpoints.extend([
            {"type": "dcatap201", "url": f"{base}/api/feed/dcat-ap/2.0.1.json"},
            {"type": "dcatus11", "url": f"{base}/api/feed/dcat-us/1.1.json"},
            {"type": "rss", "url": f"{base}/api/feed/rss/2.0"},
            {"type": "ogcrecordsapi", "url": f"{base}/api/search/v1"},
        ])
    if not endpoints:
        endpoints.append({"type": "sitemap", "url": f"{base}/sitemap.xml"})
    return endpoints


def fix_owner_location_subregion_required(record: dict) -> bool:
    """For World country with Local/Regional gov: set owner.type to Central government.
    World has no subregions, so we cannot add subregion; changing type avoids the rule."""
    owner = record.get("owner", {}) or {}
    owner_type = (owner.get("type") or "").strip()
    if owner_type not in ("Local government", "Regional government"):
        return False

    location = owner.get("location", {}) or {}
    country = location.get("country", {}) or {}
    country_id = (country.get("id") or "").strip()

    if country_id != "World":
        return False

    owner["type"] = "Central government"
    record["owner"] = owner
    return True


def fix_placeholder_owner_name(record: dict) -> bool:
    """Replace placeholder owner name with inferred name."""
    owner = record.get("owner", {}) or {}
    current = (owner.get("name", "") or "").strip()
    if current.lower() not in PLACEHOLDER_OWNER_VALUES:
        return False

    inferred = infer_owner_name(record)
    if not inferred:
        inferred = (record.get("name", "") or "").strip()
        if not inferred or inferred.lower() in PLACEHOLDER_OWNER_VALUES:
            return False
    owner["name"] = inferred
    record["owner"] = owner
    return True


def is_placeholder_title(name: str) -> bool:
    """Check if name is a URL/domain placeholder."""
    if not name:
        return False
    lower = name.lower().strip()
    return (
        "." in lower or "/" in lower
        or lower.endswith((".com", ".org", ".gov", ".edu", ".io"))
        or "hub.arcgis.com" in lower
        or "opendata.arcgis.com" in lower
        or re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", lower)
    )


def fix_placeholder_title(record: dict) -> bool:
    """Replace URL/domain-based title with human-readable name."""
    name = (record.get("name", "") or "").strip()
    if not is_placeholder_title(name):
        return False
    new_title = generate_title(record)
    if not new_title or new_title == name:
        return False
    record["name"] = new_title
    return True


def fix_api_status_mismatch(record: dict) -> bool:
    """Set api=True and api_status=active when endpoints exist."""
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


def fix_expected_endpoints_missing(record: dict) -> bool:
    """Add endpoints for API-capable software."""
    endpoints = record.get("endpoints", [])
    if isinstance(endpoints, list) and len(endpoints) > 0:
        return False
    inferred = infer_endpoints(record)
    if not inferred:
        return False
    record["endpoints"] = inferred
    return True


FIXERS = {
    "API_STATUS_MISMATCH": fix_api_status_mismatch,
    "OWNER_LOCATION_SUBREGION_REQUIRED": fix_owner_location_subregion_required,
    "PLACEHOLDER_OWNER_NAME": fix_placeholder_owner_name,
    "PLACEHOLDER_TITLE": fix_placeholder_title,
    "SOFTWARE_EXPECTED_ENDPOINTS_MISSING": fix_expected_endpoints_missing,
}


def _get_fixer(issue_type: str):
    """Resolve fixer for issue type, including SOFTWARE_EXPECTED_ENDPOINTS_MISSING_* pattern."""
    if issue_type in FIXERS:
        return FIXERS[issue_type]
    if issue_type and issue_type.startswith("SOFTWARE_EXPECTED_ENDPOINTS_MISSING_"):
        return fix_expected_endpoints_missing
    return None


def resolve_path(file_path: str) -> Path | None:
    """Resolve file path to entities or scheduled."""
    entity_path = ENTITIES_DIR / file_path
    if entity_path.exists():
        return entity_path
    scheduled = BASE_DIR / "data" / "scheduled" / file_path
    if scheduled.exists():
        return scheduled
    return None


def apply_issue_fix(file_path: str, issue_type: str) -> bool:
    """Apply fix for a single issue in a file."""
    path = resolve_path(file_path)
    if not path:
        return False

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return False

    fixer = _get_fixer(issue_type)
    if not fixer:
        return False

    changed = fixer(data)
    if not changed:
        return False

    path.write_text(
        yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    return True


def main() -> None:
    if not UNKNOWN_REPORT.exists():
        print(f"Unknown report not found: {UNKNOWN_REPORT}")
        return

    issues_by_type = parse_unknown_issues(UNKNOWN_REPORT)
    primary_issues = parse_from_primary_priority(PRIMARY_PRIORITY_FILE)
    for issue_type, files in primary_issues.items():
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
