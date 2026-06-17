#!/usr/bin/env python
"""
Add STAC servers from dev/data/stac_servers.csv to data/entities as Geoportal entries.
Reads CSV (name, url, description), infers country from URL/name, skips duplicates,
writes YAML without uid (run: python scripts/builder.py assign).
"""

from __future__ import annotations

import csv
import os
import re
from pathlib import Path
from urllib.parse import urlparse

import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
ENTITIES_DIR = _REPO_ROOT / "data" / "entities"
STAC_CSV = _REPO_ROOT / "dev" / "data" / "stac_servers.csv"

# Country inference: (domain substring or host pattern) -> (country_code, subregion or None)
URL_COUNTRY_RULES = [
    (r"data\.geo\.admin\.ch", "CH", None),
    (r"terrascope\.be|stac\.terrascope\.be|services\.terrascope\.be", "BE", None),
    (r"services\.geo\.ca|datacube\.services\.geo\.ca", "CA", None),
    (r"gis\.ktn\.gv\.at", "AT", "AT-2"),  # Kärnten
    (r"api\.lantmateriet\.se", "SE", None),
    (r"dataspace\.copernicus\.eu|openeo\.dataspace\.copernicus\.eu", "EU", None),
    (r"eocat\.esa\.int", "EU", None),
    (r"destination-earth\.eu|destine\.eu|hda\.data\.destination-earth\.eu|earthdatahub\.destine\.eu", "EU", None),
    (r"fedeo\.ceos\.org", "World", None),
    (r"api\.fgdc\.gov|stac\.geoplatform\.gov|geoplatform\.gov", "US", None),
    (r"disasters-geoplatform\.hub\.arcgis\.com", "US", None),
    (r"ngda-transportation-geoplatform\.hub\.arcgis\.com", "US", None),
    (r"disaster-vallaris\.gistda\.or\.th|gistda\.or\.th|datacube\.gistda", "TH", None),
    (r"nz-coastal\.s3-|nz-elevation\.s3-|nz-imagery\.s3-|lantmateriet|toitū.*whenua", "NZ", None),
    (r"lgln\.niedersachsen\.de|dop\.stac\.lgln\.niedersachsen\.de", "DE", "DE-NI"),
    (r"eodata\.thuenen\.de", "DE", None),
    (r"stac\.teledetection\.fr|api\.stac\.teledetection\.fr", "FR", None),
    (r"panoramax\.xyz", "FR", None),
    (r"api\.iconem\.com", "FR", None),
    (r"stac\.cyverse\.org", "US", None),
    (r"stac\.worldpop\.org", "GB", None),
    (r"stac\.earthgenome\.org", "US", None),
    (r"digital-atlas\.s3\.amazonaws\.com", "Africa", None),
    (r"stac\.geobon\.org", "World", None),
    (r"ciesin\.github\.io", "US", None),
    (r"vims\.univ-nantes\.fr", "FR", None),
    (r"stac\.scitekno\.com\.br", "BR", None),
    (r"stac\.dataspace\.copernicus", "EU", None),
    (r"openeo\.dataspace\.copernicus", "EU", None),
    (r"eox\.at|eox\.pages\.at", "AT", None),
    (r"stac\.cyverse", "US", None),
    (r"hirondelle\.crim\.ca", "CA", None),
    (r"stac\.odse|wasabisys\.com.*odse", "EU", None),
    (r"openlandmap|wasabisys\.com.*openlandmap", "World", None),
    (r"esa-earthcode\.github\.io|esa\.int", "EU", None),
    (r"stac\.overturemaps\.org", "World", None),
    (r"radiantearth\.blob\.core\.windows\.net", "US", None),
    (r"noaadata\.apps\.nsidc\.org", "US", None),
    (r"worldbank|DECAT_Space2Stats", "World", None),
    (r"terradue\.com", "EU", None),
    (r"api\.hubocean\.earth", "World", None),
    (r"stac\.geoplatform\.gov", "US", None),
    (r"spved5ihrl\.execute-api\.us-west-2\.amazonaws\.com", "US", "US-KY"),
    (r"wyvern.*s3\.ca-central-1\.amazonaws\.com", "CA", None),
    (r"umbra-open-data-catalog.*s3\.us-west-2\.amazonaws\.com", "US", None),
    (r"dragonfly-open-data\.s3", "GB", None),
    (r"stac\.stac\.lgln\.niedersachsen\.de", "DE", "DE-NI"),
    (r"fairicube\.eu|stacapi\.eoxhub\.fairicube\.eu", "EU", None),
    (r"gpt\.geocloud\.com", "US", None),
    (r"data\.source\.coop", "World", None),
    (r"raw\.githubusercontent\.com.*Anna-leungtn.*STAC_CSDI", "HK", None),
    (r"api\.ellipsis-drive\.com", "NL", None),
]


def normalize_link(url: str) -> str:
    u = url.strip()
    if u and not u.startswith(("http://", "https://")):
        u = "https://" + u
    parsed = urlparse(u)
    # Drop fragment and optional trailing slash for comparison
    path = (parsed.path or "/").rstrip("/") or "/"
    return f"{parsed.scheme or 'https'}://{parsed.netloc.lower()}{path}"


def generate_id(url: str, existing_ids: set[str]) -> str:
    parsed = urlparse(url.strip())
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").strip("/")
    base = re.sub(r"[^a-z0-9]", "", host)
    if not base:
        base = "stac"
    candidate = base
    suffix = 0
    while candidate in existing_ids:
        suffix += 1
        # Add path-derived suffix for first collision, then numeric
        if suffix == 1 and path:
            path_slug = re.sub(r"[^a-z0-9]", "", path.replace("/", "")[:16])
            if path_slug:
                candidate = (base + path_slug)[:64]
            else:
                candidate = f"{base}{suffix}"
        else:
            candidate = f"{base}{suffix}"
    return candidate


def infer_country(url: str, name: str, description: str) -> tuple[str, str | None]:
    combined = f"{url} {name} {description}".lower()
    parsed = urlparse(url)
    path = (parsed.path or "").lower()
    host = (parsed.netloc or "").lower()
    # FGDC state STACs: api.fgdc.gov/states/ak -> US, US-AK
    if "api.fgdc.gov" in host and "/states/" in path:
        m = re.search(r"/states/([a-z]{2})", path)
        if m:
            return "US", f"US-{m.group(1).upper()}"
    for pattern, country, subregion in URL_COUNTRY_RULES:
        if re.search(pattern, combined, re.IGNORECASE):
            return country, subregion
    # Fallback from domain TLD
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    if ".gov" in host and "fgdc" in host:
        return "US", None
    if ".se" in host:
        return "SE", None
    if ".ch" in host:
        return "CH", None
    if ".at" in host:
        return "AT", None
    if ".be" in host:
        return "BE", None
    if ".ca" in host:
        return "CA", None
    if ".de" in host:
        return "DE", None
    if ".fr" in host:
        return "FR", None
    if ".eu" in host:
        return "EU", None
    if ".uk" in host or ".ac.uk" in host:
        return "GB", None
    if ".org" in host or ".com" in host or "amazonaws" in host or "github" in host:
        return "World", None
    return "World", None


def load_existing_entities() -> tuple[set[str], set[str]]:
    ids = set()
    links = set()
    for root, _dirs, files in os.walk(ENTITIES_DIR):
        for f in files:
            if not f.endswith(".yaml") and not f.endswith(".yml"):
                continue
            path = Path(root) / f
            try:
                with open(path, "r", encoding="utf-8") as fp:
                    data = yaml.safe_load(fp)
                if data:
                    if data.get("id"):
                        ids.add(data["id"])
                    if data.get("link"):
                        links.add(normalize_link(data["link"]))
            except Exception:
                pass
    return ids, links


def build_record(row: dict, record_id: str, country: str, subregion: str | None) -> dict:
    from constants import COUNTRIES

    name = (row.get("name") or "").strip() or "STAC Catalog"
    link = (row.get("link") or row.get("url") or "").strip()
    if link and not link.startswith(("http://", "https://")):
        link = "https://" + link
    description = (row.get("description") or "").strip() or None

    country_name = COUNTRIES.get(country, country)

    location = {
        "country": {"id": country, "name": country_name},
        "level": 20,
    }
    if subregion and country in ("US", "AT", "DE", "CA"):
        location["subregion"] = {"id": subregion, "name": subregion}

    coverage_item = {"location": dict(location)}
    if country == "EU":
        coverage_item["location"]["macroregion"] = {"id": "155", "name": "Western Europe"}

    owner_name = name
    if "Copernicus" in name:
        owner_name = "European Commission / Copernicus"
    elif "ESA" in name or "esa.int" in link.lower():
        owner_name = "European Space Agency"
    elif "NASA" in link or "nsidc" in link.lower():
        owner_name = "NASA / US Government"
    elif "World Bank" in name or "worldbank" in link.lower():
        owner_name = "World Bank"

    link_path = link.split("?")[0].lower()
    is_static_catalog = link_path.endswith(".json")
    if description:
        desc = description
    elif is_static_catalog:
        desc = "STAC (SpatioTemporal Asset Catalog) static catalog."
    else:
        desc = "STAC (SpatioTemporal Asset Catalog) API."
    if is_static_catalog:
        software = {"id": "stacbrowser", "name": "STAC Browser"}
        if "stac browser" not in desc.lower():
            desc = desc.rstrip() + " Powered by STAC Browser."
    else:
        software = {"id": "stacserver", "name": "Stac-server"}

    record = {
        "access_mode": ["open"],
        "api": True,
        "api_status": "active",
        "catalog_type": "Geoportal",
        "content_types": ["dataset", "map_layer"],
        "coverage": [coverage_item],
        "description": desc,
        "endpoints": [{"type": "stacserverapi", "url": link}],
        "id": record_id,
        "langs": [{"id": "EN", "name": "English"}],
        "link": link,
        "name": name,
        "owner": {
            "link": None,
            "location": location,
            "name": owner_name,
            "type": "Other",
        },
        "software": software,
        "status": "active",
        "tags": ["geospatial", "has_api", "STAC"],
    }
    return record


def main(dry_run: bool = False):
    import sys
    sys.path.insert(0, str(_SCRIPT_DIR))
    from constants import COUNTRIES, MAP_CATALOG_TYPE_SUBDIR

    if not STAC_CSV.exists():
        print(f"CSV not found: {STAC_CSV}")
        return

    existing_ids, existing_links = load_existing_entities()
    type_dir = MAP_CATALOG_TYPE_SUBDIR.get("Geoportal", "geo")

    added = 0
    skipped = 0
    with open(STAC_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        url = (row.get("url") or row.get("link") or "").strip()
        if not url:
            continue
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        norm = normalize_link(url)
        if norm in existing_links:
            skipped += 1
            continue
        record_id = generate_id(url, existing_ids)
        existing_ids.add(record_id)
        country, subregion = infer_country(
            url, row.get("name") or "", row.get("description") or ""
        )
        if country not in COUNTRIES:
            country = "World"
        record = build_record(row, record_id, country, subregion)
        record["link"] = url
        record["endpoints"][0]["url"] = url

        if subregion and country in ("US", "AT", "DE", "CA"):
            out_dir = ENTITIES_DIR / country / subregion / type_dir
        else:
            out_dir = ENTITIES_DIR / country / "Federal" / type_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{record_id}.yaml"

        if dry_run:
            print(f"Would add: {record_id} -> {out_path}")
            added += 1
            continue
        with open(out_path, "w", encoding="utf-8") as fp:
            yaml.dump(record, fp, default_flow_style=False, allow_unicode=True, sort_keys=False)
        print(f"Added: {record_id} -> {out_path}")
        added += 1

    print(f"Done. Added: {added}, Skipped (duplicate link): {skipped}")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="Print actions only")
    args = p.parse_args()
    main(dry_run=args.dry_run)
