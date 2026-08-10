import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from builder import (
    check_owner_type_values,
    check_path_country_consistency,
    check_software_expected_endpoints,
    check_urls,
    choose_duplicate_keeper,
    get_priority_level,
    link_serves_as_api_endpoint,
    score_duplicate_keeper,
)


def test_link_serves_as_api_endpoint_geoserver():
    assert link_serves_as_api_endpoint(
        "geoserver", "https://maps.example.gov/geoserver/web/"
    )
    assert not link_serves_as_api_endpoint(
        "geoserver", "https://maps.example.gov/portal"
    )


def test_link_serves_as_api_endpoint_arcgis():
    assert link_serves_as_api_endpoint(
        "arcgisserver", "https://gis.example.gov/arcgis/rest/services"
    )
    assert link_serves_as_api_endpoint(
        "arcgisserver", "https://gis.example.gov/server/rest/services/Base/MapServer"
    )
    assert not link_serves_as_api_endpoint(
        "arcgisserver", "https://gis.example.gov/opendata"
    )


def test_software_expected_endpoints_skips_when_link_is_service_root(monkeypatch):
    monkeypatch.setattr(
        "builder.get_cached_software_map",
        lambda: {"geoserver": {"has_api": "Yes", "name": "GeoServer"}},
    )
    record = {
        "software": {"id": "geoserver", "name": "GeoServer"},
        "status": "active",
        "link": "https://example.org/geoserver",
        "endpoints": [],
    }
    assert check_software_expected_endpoints(record) is None


def test_software_expected_endpoints_flags_non_service_link(monkeypatch):
    monkeypatch.setattr(
        "builder.get_cached_software_map",
        lambda: {"ckan": {"has_api": "Yes", "name": "CKAN"}},
    )
    record = {
        "software": {"id": "ckan", "name": "CKAN"},
        "status": "active",
        "link": "https://data.example.gov",
        "endpoints": [],
    }
    issue = check_software_expected_endpoints(record)
    assert issue is not None
    assert issue["issue_type"] == "SOFTWARE_EXPECTED_ENDPOINTS_MISSING_CKAN"
    assert get_priority_level(issue["issue_type"]) == "MEDIUM"


def test_software_expected_endpoints_skips_when_api_true(monkeypatch):
    monkeypatch.setattr(
        "builder.get_cached_software_map",
        lambda: {"ckan": {"has_api": "Yes", "name": "CKAN"}},
    )
    record = {
        "software": {"id": "ckan", "name": "CKAN"},
        "status": "active",
        "api": True,
        "link": "https://data.example.gov",
        "endpoints": [],
    }
    assert check_software_expected_endpoints(record) is None


def test_owner_type_noncanonical_and_invalid(monkeypatch):
    monkeypatch.setattr(
        "builder._load_owner_type_vocab",
        lambda: (
            frozenset({"Academy", "Business"}),
            {"University": "Academy", "Company": "Business"},
        ),
    )
    assert check_owner_type_values({"owner": {"type": "Academy"}}) is None
    noncanon = check_owner_type_values({"owner": {"type": "University"}})
    assert noncanon["issue_type"] == "OWNER_TYPE_NONCANONICAL"
    invalid = check_owner_type_values({"owner": {"type": "Guild"}})
    assert invalid["issue_type"] == "INVALID_OWNER_TYPE"


def test_path_country_consistency_allowlist_and_mismatch():
    ok = check_path_country_consistency(
        {
            "_file_path": "EU/Federal/opendata/example.yaml",
            "owner": {"location": {"country": {"id": "FR", "name": "France"}}},
        }
    )
    assert ok is None
    mismatch = check_path_country_consistency(
        {
            "_file_path": "FR/Federal/opendata/example.yaml",
            "owner": {"location": {"country": {"id": "DE", "name": "Germany"}}},
            "coverage": [{"location": {"country": {"id": "DE", "name": "Germany"}}}],
        }
    )
    assert mismatch["issue_type"] == "PATH_COUNTRY_MISMATCH"


def test_check_urls_validates_catalog_export():
    record = {
        "link": "https://data.example.gov",
        "catalog_export": "not-a-url",
        "owner": {},
        "endpoints": [],
    }
    issues = check_urls(record)
    assert issues
    assert any(i["issue_type"] == "INVALID_CATALOG_EXPORT_URL" for i in issues)


def test_choose_duplicate_keeper_prefers_https_non_www():
    metas = [
        {
            "record_id": "wwwexample",
            "link": "http://www.example.gov/data",
            "file_path": "US/Federal/opendata/wwwexample.yaml",
        },
        {
            "record_id": "example",
            "link": "https://example.gov/data",
            "file_path": "US/Federal/opendata/example.yaml",
        },
    ]
    keeper = choose_duplicate_keeper(metas)
    assert keeper["record_id"] == "example"
    assert score_duplicate_keeper(metas[1]) > score_duplicate_keeper(metas[0])


def test_choose_duplicate_keeper_penalizes_unknown_path():
    metas = [
        {
            "record_id": "portal",
            "link": "https://portal.example.gov",
            "file_path": "Unknown/opendata/portal.yaml",
        },
        {
            "record_id": "portalus",
            "link": "https://portal.example.gov",
            "file_path": "US/Federal/opendata/portalus.yaml",
        },
    ]
    keeper = choose_duplicate_keeper(metas)
    assert keeper["record_id"] == "portalus"
