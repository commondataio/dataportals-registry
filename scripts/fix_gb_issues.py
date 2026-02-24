#!/usr/bin/env python3
"""
Fix issues listed in dataquality/countries/GB.txt.

This script applies deterministic, low-risk fixes for:
- PLACEHOLDER_TITLE
- SOFTWARE_EXPECTED_ENDPOINTS_MISSING (infer standard endpoints from link + software)

It intentionally skips DUPLICATE_LINK (manual deduplication).
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse

import yaml


BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
GB_REPORT = BASE_DIR / "dataquality" / "countries" / "GB.txt"

PLACEHOLDER_OWNER_VALUES = {"n/a", "na", "none", "null", "not specified", "unknown", ""}

# Domain/host patterns to human-readable titles for GB catalogs
GB_TITLE_OVERRIDES: dict[str, str] = {
    "inspiredata.swansea.gov.uk": "Swansea Council INSPIRE Data Portal",
    "inspire.gwynedd.gov.uk": "Gwynedd Council INSPIRE Services",
    "researchdata.uwtsd.ac.uk": "University of Wales Trinity Saint David Research Data",
    "admin.opendatani.gov.uk": "Northern Ireland Open Data",
    "services.spatialni.gov.uk": "Spatial NI Services",
    "webservices.spatialni.gov.uk": "Spatial NI ArcGIS Web Services",
    "geonode.gis.qub.ac.uk": "Queen's University Belfast GeoNode",
    "preproduction.spatialni.gov.uk": "Spatial NI Pre-production",
    "statistics.gov.scot": "Scotland Statistics",
    "thredds.sams.ac.uk": "SAMS THREDDS Data Server",
    "gisdata-nlcmaps.opendata.arcgis.com": "North Lanarkshire Council Maps Open Data",
    "maps.eastdunbarton.gov.uk": "East Dunbartonshire Council Maps",
    "cagmap.snh.gov.uk": "Scottish Natural Heritage CAG Map Services",
    "mapping.moray.gov.uk": "Moray Council Mapping",
    "spatialdata.gov.scot": "Scotland Spatial Data",
    "navgisapp04.north-ayrshire.gov.uk": "North Ayrshire Council GIS",
    "maps.northlanarkshire.gov.uk": "North Lanarkshire Council Maps",
    "open-data-design-glasgowgis.hub.arcgis.com": "Glasgow Design Open Data",
    "spatial.stockport.gov.uk": "Stockport Council Spatial Data",
    "gis.herefordshire.gov.uk": "Herefordshire Council GIS",
    "inspire.redcar-cleveland.gov.uk": "Redcar and Cleveland Council INSPIRE",
    "opendata-cheshireeast.opendata.arcgis.com": "Cheshire East Open Data",
    "maps.cheshire.gov.uk": "Cheshire Council Maps",
    "inspire.northyorkmoors.org.uk": "North York Moors INSPIRE",
    "maps.whitehorsedc.gov.uk": "White Horse District Council Maps",
    "maps.cheshireeast.gov.uk": "Cheshire East Council Maps",
    "maps.runnymede.gov.uk": "Runnymede Council Maps",
    "spatialdata.communities.gov.uk": "Communities.gov.uk Spatial Data",
    "mapssouthoxon.gov.uk": "South Oxfordshire Council Maps",
    "inspire.worcester.gov.uk": "Worcester City Council INSPIRE",
    "wms.derbyshire.gov.uk": "Derbyshire Council WMS",
    "gis.northumberland.gov.uk": "Northumberland Council GIS",
    "gistfl.opendata.arcgis.com": "Telford and Wrekin GIS Open Data",
    "rchelmsford.gov.uk": "Chelmsford City Council",
    "sheffieldcitycouncil.cloud.esriuk.com": "Sheffield City Council GIS",
    "mapapps2.bgs.ac.uk": "British Geological Survey Map Apps",
    "maps.york.gov.uk": "City of York Council Maps",
    "gis.cumbria.gov.uk": "Cumbria Council GIS",
    "geoserver.oxfordarchaeology.com": "Oxford Archaeology GeoServer",
    "stedwards.figshare.com": "St Edward's University Figshare",
    "data.kent.ac.uk": "University of Kent Data",
    "archive.researchdata.leeds.ac.uk": "University of Leeds Research Data Archive",
    "ourworldindata.org": "Our World in Data",
    "plymouth.thedata.place": "Plymouth Data Place",
    "actionagainststunting.openfn.org": "Action Against Stunting Data",
    "icem-nesstar.data-archive.ac.uk": "ICEM Nesstar Data Archive",
    "opendatacommunities.org": "Open Data Communities",
    "datahub.io": "DataHub",
    "public.cdrc.ac.uk": "Consumer Data Research Centre",
    "cubeexplorer.csopenportal.co.uk": "CUBE Explorer",
    "itportal.decc.gov.uk": "DECC IT Portal",
    "ons-inspire.esriuk.com": "ONS INSPIRE Services",
    "maps1webserver2.croppermap.com": "Cropper Map Services",
    "hub.mapstand.com": "MapStand Hub",
    "locationmetadataeditor.data.gov.uk": "Location Metadata Editor",
    "gisdata.landmarkcloud.co.uk": "Landmark GIS Data",
    "geofeature.org": "GeoFeature",
    "developer.ons.gov.uk": "ONS Developer Hub",
    "brill.figshare.com": "Brill Figshare",
    "swat4hcls.figshare.com": "SWAT4HCLS Figshare Repository",
    "digitalscience.figshare.com": "Digital Science Figshare",
    "acs.figshare.com": "ACS Figshare",
    "shurda.shu.ac.uk": "Sheffield Hallam University Research Data",
    "plus.figshare.com": "PLUS Figshare",
    "brunel.figshare.com": "Brunel University Figshare",
    "orcid.figshare.com": "ORCID Figshare",
    "altmetric.figshare.com": "Altmetric Figshare",
    "liverpool-sdg-data.github.io": "Liverpool SDG Data",
    "birmingham-city-observatory.datopian.com": "Birmingham City Observatory",
    "discovery.closer.ac.uk": "CLOSER Discovery",
    "data.hounslow.gov.uk": "Hounslow Council Open Data",
    "alspac-explore.bristol.ac.uk": "ALSPAC Explore - University of Bristol",
    "data.ecosystem-modelling.pml.ac.uk": "PML Ecosystem Modelling Data",
    "gis1.westberks.gov.uk": "West Berkshire Council GIS",
    "inspire.sthelens.gov.uk": "St Helens Council INSPIRE",
}


def parse_gb_issues(path: Path) -> dict[str, set[str]]:
    text = path.read_text(encoding="utf-8")
    pattern = r"File: ([^\n]+)\nRecord ID: [^\n]+\nIssue: ([^\n]+)\nField: ([^\n]+)"
    parsed: dict[str, set[str]] = {}
    for file_path, issue_type, _field in re.findall(pattern, text):
        file_path = file_path.strip()
        if file_path.startswith("GB/") or file_path.startswith("World/"):
            parsed.setdefault(issue_type, set()).add(file_path)
    return parsed


def get_base_url(link: str) -> str:
    if not link:
        return ""
    p = urlparse(link if "://" in link else f"https://{link}")
    scheme = p.scheme or "https"
    netloc = (p.netloc or link).split("/")[0].split(":")[0]
    return f"{scheme}://{netloc}" if netloc else ""


def get_base_host(link: str) -> str:
    if not link:
        return ""
    p = urlparse(link if "://" in link else f"https://{link}")
    netloc = (p.netloc or link).lower().split("/")[0].split(":")[0]
    return re.sub(r"^www\.", "", netloc)


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

    link = record.get("link", "")
    name = record.get("name", "")

    # Check overrides by matching host/domain
    for pattern, title in GB_TITLE_OVERRIDES.items():
        if pattern in (link or "").lower() or pattern in (name or "").lower():
            if type_suffix not in title:
                return f"{title} - {type_suffix}"
            return title

    owner = (record.get("owner", {}) or {}).get("name", "").strip()
    if owner and owner.lower() not in PLACEHOLDER_OWNER_VALUES:
        return f"{owner} {type_suffix}".strip()

    source = link or name or ""
    netloc = get_base_host(source)
    if not netloc:
        return f"United Kingdom {type_suffix}".strip()

    tokens = re.split(r"[.\-_/]+", netloc)
    stop = {
        "data", "opendata", "open", "geo", "gis", "map", "maps", "hub",
        "arcgis", "com", "org", "gov", "govuk", "ac", "edu", "uk",
        "services", "www", "api",
    }
    words = [t for t in tokens if t and t not in stop and not t.isdigit()]
    if words:
        label = " ".join(w.capitalize() for w in words[:4])
        return f"{label} {type_suffix}".strip()
    return f"United Kingdom {type_suffix}".strip()


def is_placeholder_title(name: str) -> bool:
    if not name:
        return False
    lower = name.lower().strip()
    return (
        "." in lower
        or "/" in lower
        or ":" in lower
        or lower.endswith((".com", ".org", ".gov", ".govuk", ".edu", ".ac.uk", ".io"))
        or "hub.arcgis.com" in lower
        or "opendata.arcgis.com" in lower
    )


def infer_endpoints(record: dict) -> list[dict]:
    software_id = ((record.get("software", {}) or {}).get("id", "") or "").lower()
    link = record.get("link", "")
    base = get_base_url(link)
    if not base:
        return []

    endpoints: list[dict] = []
    if software_id == "arcgisserver":
        endpoints.append({"type": "arcgis:rest:info", "url": f"{base}/arcgis/rest/info?f=pjson"})
        endpoints.append({"type": "arcgis:rest:services", "url": f"{base}/arcgis/rest/services?f=pjson"})
    elif software_id == "geoserver":
        endpoints.append({"type": "wms111", "url": f"{base}/geoserver/ows?service=WMS&version=1.1.1&request=GetCapabilities", "version": "1.1.1"})
        endpoints.append({"type": "wfs110", "url": f"{base}/geoserver/ows?service=WFS&version=1.1.0&request=GetCapabilities", "version": "1.1.0"})
    elif software_id == "geonetwork":
        endpoints.append({"type": "geonetwork:api:records", "url": f"{base}/geonetwork/srv/api/records"})
        endpoints.append({"type": "csw202", "url": f"{base}/geonetwork/srv/eng/csw?service=CSW&version=2.0.2&request=GetCapabilities", "version": "2.0.2"})
    elif software_id == "figshare":
        endpoints.append({"type": "sitemap", "url": f"{base}/sitemap/siteindex.xml"})
    elif software_id == "ckan":
        endpoints.append({"type": "ckan:api", "url": f"{base}/api/3/action/package_list", "version": "3.0"})
    elif software_id == "nesstar":
        endpoints.append({"type": "sitemap", "url": f"{base}/sitemap.xml"})
    elif software_id == "haplo":
        endpoints.append({"type": "sitemap", "url": f"{base}/sitemap.xml"})
    elif software_id == "ipt":
        endpoints.append({"type": "sitemap", "url": f"{base}/sitemap.xml"})
    elif software_id == "invenio":
        endpoints.append({"type": "oaipmh20", "url": f"{base}/oai2d?verb=Identify", "version": "2.0"})
    elif software_id == "obibamica":
        endpoints.append({"type": "sitemap", "url": f"{base}/sitemap.xml"})
    elif software_id == "strapi":
        endpoints.append({"type": "sitemap", "url": f"{base}/sitemap.xml"})

    if not endpoints:
        endpoints.append({"type": "sitemap", "url": f"{base}/sitemap.xml"})
    return endpoints


def fix_software_expected_endpoints_missing(record: dict) -> bool:
    endpoints = record.get("endpoints", [])
    if isinstance(endpoints, list) and len(endpoints) > 0:
        return False
    inferred = infer_endpoints(record)
    if not inferred:
        return False
    record["endpoints"] = inferred
    if record.get("api") is not True:
        record["api"] = True
    if record.get("api_status") != "active":
        record["api_status"] = "active"
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


def apply_issue_fix(file_path: str, issue_type: str) -> bool:
    full_path = ENTITIES_DIR / file_path
    if not full_path.exists():
        return False
    data = yaml.safe_load(full_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return False

    before = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)

    changed = False
    if issue_type == "PLACEHOLDER_TITLE":
        changed = fix_placeholder_title(data)
    elif issue_type == "SOFTWARE_EXPECTED_ENDPOINTS_MISSING" or (
        issue_type and issue_type.startswith("SOFTWARE_EXPECTED_ENDPOINTS_MISSING_")
    ):
        changed = fix_software_expected_endpoints_missing(data)

    if not changed:
        return False

    after = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)
    if before == after:
        return False

    full_path.write_text(after, encoding="utf-8")
    return True


def main() -> None:
    if not GB_REPORT.exists():
        print(f"GB report not found: {GB_REPORT}")
        return

    issues_by_type = parse_gb_issues(GB_REPORT)
    supported = {"PLACEHOLDER_TITLE", "SOFTWARE_EXPECTED_ENDPOINTS_MISSING"}

    def is_supported(t):
        return t in supported or (t and t.startswith("SOFTWARE_EXPECTED_ENDPOINTS_MISSING_"))

    total = 0
    for issue_type in sorted(issues_by_type.keys()):
        if not is_supported(issue_type):
            continue
        files = issues_by_type.get(issue_type, set())
        if not files:
            continue
        fixed = 0
        for file_path in sorted(files):
            if apply_issue_fix(file_path, issue_type):
                fixed += 1
                print(f"Fixed {issue_type}: {file_path}")
        total += fixed
        if files:
            print(f"{issue_type}: fixed {fixed}/{len(files)}")
    print(f"\nTotal fixed: {total}")


if __name__ == "__main__":
    main()
