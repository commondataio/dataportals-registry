"""Tests for constants.py"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from constants import (
    reverse_langs,
    COUNTRIES_LANGS,
    DOMAIN_LOCATIONS,
    DEFAULT_LOCATION,
    MAP_CATALOG_TYPE_SUBDIR,
    MAP_SOFTWARE_OWNER_CATALOG_TYPE,
    CUSTOM_SOFTWARE_KEYS,
    ENTRY_TEMPLATE,
    COUNTRIES,
)


class TestReverseLangs:
    """Tests for reverse_langs function"""

    def test_reverse_langs_structure(self):
        """Test that reverse_langs returns correct structure"""
        reversed = reverse_langs()
        assert isinstance(reversed, dict)
        # Check that some known mappings exist
        assert "us" in reversed
        assert "uk" in reversed
        assert "de" in reversed

    def test_reverse_langs_values(self):
        """Test that reverse_langs returns correct language codes"""
        reversed = reverse_langs()
        assert reversed.get("us") == "EN"
        assert reversed.get("uk") == "EN"
        assert reversed.get("de") == "DE"
        assert reversed.get("fr") == "FR"


class TestConstants:
    """Tests for constant values"""

    def test_countries_langs_exists(self):
        """Test that COUNTRIES_LANGS is populated"""
        assert isinstance(COUNTRIES_LANGS, dict)
        assert len(COUNTRIES_LANGS) > 0
        assert "us" in COUNTRIES_LANGS
        assert COUNTRIES_LANGS["us"] == "EN"

    def test_domain_locations_structure(self):
        """Test that DOMAIN_LOCATIONS has correct structure"""
        assert isinstance(DOMAIN_LOCATIONS, dict)
        assert "us" in DOMAIN_LOCATIONS
        location = DOMAIN_LOCATIONS["us"]
        assert "location" in location
        assert "country" in location["location"]
        assert location["location"]["country"]["id"] == "US"

    def test_default_location(self):
        """Test DEFAULT_LOCATION structure"""
        assert isinstance(DEFAULT_LOCATION, dict)
        assert "location" in DEFAULT_LOCATION
        assert DEFAULT_LOCATION["location"]["country"]["id"] == "Unknown"

    def test_map_catalog_type_subdir(self):
        """Test MAP_CATALOG_TYPE_SUBDIR mappings"""
        assert isinstance(MAP_CATALOG_TYPE_SUBDIR, dict)
        # Test keys that actually exist in the dictionary
        assert "Geoportal" in MAP_CATALOG_TYPE_SUBDIR
        assert MAP_CATALOG_TYPE_SUBDIR["Geoportal"] == "geo"
        assert "Metadata catalog" in MAP_CATALOG_TYPE_SUBDIR
        assert MAP_CATALOG_TYPE_SUBDIR["Metadata catalog"] == "metadata"
        assert "Scientific data repository" in MAP_CATALOG_TYPE_SUBDIR
        assert MAP_CATALOG_TYPE_SUBDIR["Scientific data repository"] == "scientific"
        assert "Indicators catalog" in MAP_CATALOG_TYPE_SUBDIR
        assert MAP_CATALOG_TYPE_SUBDIR["Indicators catalog"] == "indicators"
        # Note: 'Open data portal' is not in MAP_CATALOG_TYPE_SUBDIR
        # It defaults to 'opendata' in builder.py when not found
        assert "Open data portal" not in MAP_CATALOG_TYPE_SUBDIR

    def test_map_software_owner_catalog_type(self):
        """Test MAP_SOFTWARE_OWNER_CATALOG_TYPE mappings"""
        assert isinstance(MAP_SOFTWARE_OWNER_CATALOG_TYPE, dict)
        assert "ckan" in MAP_SOFTWARE_OWNER_CATALOG_TYPE
        assert MAP_SOFTWARE_OWNER_CATALOG_TYPE["ckan"] == "Open data portal"
        assert MAP_SOFTWARE_OWNER_CATALOG_TYPE["arcgishub"] == "Geoportal"

    def test_custom_software_keys(self):
        """Test CUSTOM_SOFTWARE_KEYS list"""
        assert isinstance(CUSTOM_SOFTWARE_KEYS, list)
        assert "searchengines" in CUSTOM_SOFTWARE_KEYS
        assert "ml" in CUSTOM_SOFTWARE_KEYS

    def test_entry_template_structure(self):
        """Test ENTRY_TEMPLATE has required fields"""
        assert isinstance(ENTRY_TEMPLATE, dict)
        required_fields = [
            "id",
            "name",
            "link",
            "catalog_type",
            "access_mode",
            "content_types",
            "coverage",
            "owner",
            "software",
            "status",
        ]
        for field in required_fields:
            assert field in ENTRY_TEMPLATE

    def test_entry_template_defaults(self):
        """Test ENTRY_TEMPLATE default values"""
        assert ENTRY_TEMPLATE["api"] is False
        assert ENTRY_TEMPLATE["api_status"] == "uncertain"
        assert ENTRY_TEMPLATE["status"] == "scheduled"
        assert isinstance(ENTRY_TEMPLATE["access_mode"], list)
        assert isinstance(ENTRY_TEMPLATE["content_types"], list)
        assert isinstance(ENTRY_TEMPLATE["coverage"], list)

    def test_countries_dict(self):
        """Test COUNTRIES dictionary"""
        assert isinstance(COUNTRIES, dict)
        assert "US" in COUNTRIES
        assert COUNTRIES["US"] == "United States"
        assert "GB" in COUNTRIES
        assert COUNTRIES["GB"] == "United Kingdom"
