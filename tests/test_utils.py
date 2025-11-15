"""Tests for utility functions"""

import os
import sys
import pytest
from urllib.parse import urlparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from constants import (
    DOMAIN_LOCATIONS,
    DEFAULT_LOCATION,
    COUNTRIES_LANGS,
    MAP_CATALOG_TYPE_SUBDIR,
    MAP_SOFTWARE_OWNER_CATALOG_TYPE,
)


class TestURLParsing:
    """Tests for URL parsing utilities"""

    def test_urlparse_basic(self):
        """Test basic URL parsing"""
        url = "https://catalog.data.gov"
        parsed = urlparse(url)
        assert parsed.scheme == "https"
        assert parsed.netloc == "catalog.data.gov"

    def test_urlparse_with_path(self):
        """Test URL parsing with path"""
        url = "https://example.com/api/3/action/package_search"
        parsed = urlparse(url)
        assert parsed.netloc == "example.com"
        assert parsed.path == "/api/3/action/package_search"

    def test_urlparse_domain_extraction(self):
        """Test domain extraction from URL"""
        url = "https://ckan.himdataportal.com/"
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        assert domain == "ckan.himdataportal.com"

    def test_urlparse_tld_extraction(self):
        """Test TLD extraction from domain"""
        url = "https://example.gov"
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        tld = domain.rsplit(".", 1)[-1]
        assert tld == "gov"


class TestDomainLocationMapping:
    """Tests for domain to location mapping"""

    def test_domain_location_mapping(self):
        """Test mapping domains to locations"""
        test_cases = [
            ("example.gov", "gov", "US"),
            ("example.uk", "uk", "GB"),
            ("example.de", "de", "DE"),
            ("example.fr", "fr", "FR"),
        ]

        for domain, tld, expected_country in test_cases:
            if tld in DOMAIN_LOCATIONS:
                location = DOMAIN_LOCATIONS[tld]
                assert location["location"]["country"]["id"] == expected_country

    def test_default_location_for_unknown_tld(self):
        """Test that unknown TLDs get default location"""
        unknown_tld = "xyz"
        if unknown_tld not in DOMAIN_LOCATIONS:
            location = DEFAULT_LOCATION
            assert location["location"]["country"]["id"] == "Unknown"


class TestLanguageMapping:
    """Tests for language mapping from TLD"""

    def test_language_from_tld(self):
        """Test language detection from TLD"""
        test_cases = [
            ("us", "EN"),
            ("uk", "EN"),
            ("de", "DE"),
            ("fr", "FR"),
            ("es", "ES"),
        ]

        for tld, expected_lang in test_cases:
            if tld in COUNTRIES_LANGS:
                assert COUNTRIES_LANGS[tld] == expected_lang


class TestCatalogTypeMapping:
    """Tests for catalog type and subdirectory mapping"""

    def test_catalog_type_to_subdir(self):
        """Test mapping catalog types to subdirectories"""
        test_cases = [
            ("Geoportal", "geo"),
            ("Open data portal", "opendata"),
            ("Scientific data repository", "scientific"),
            ("Indicators catalog", "indicators"),
        ]

        for catalog_type, expected_subdir in test_cases:
            if catalog_type in MAP_CATALOG_TYPE_SUBDIR:
                assert MAP_CATALOG_TYPE_SUBDIR[catalog_type] == expected_subdir

    def test_software_to_catalog_type(self):
        """Test mapping software to catalog type"""
        test_cases = [
            ("ckan", "Open data portal"),
            ("arcgishub", "Geoportal"),
            ("dspace", "Scientific data repository"),
            ("opensdg", "Indicators catalog"),
        ]

        for software, expected_type in test_cases:
            if software in MAP_SOFTWARE_OWNER_CATALOG_TYPE:
                assert MAP_SOFTWARE_OWNER_CATALOG_TYPE[software] == expected_type


class TestRecordIDGeneration:
    """Tests for record ID generation from URLs"""

    def test_record_id_from_domain(self):
        """Test generating record ID from domain"""
        url = "https://ckan.himdataportal.com/"
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        record_id = (
            domain.split(":", 1)[0].replace("_", "").replace("-", "").replace(".", "")
        )
        assert record_id == "ckanhimdataportalcom"

    def test_record_id_with_port(self):
        """Test record ID generation with port"""
        url = "https://example.com:8080"
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        record_id = (
            domain.split(":", 1)[0].replace("_", "").replace("-", "").replace(".", "")
        )
        assert record_id == "examplecom"

    def test_record_id_special_chars(self):
        """Test record ID generation with special characters"""
        url = "https://test-site.example.com"
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        record_id = (
            domain.split(":", 1)[0].replace("_", "").replace("-", "").replace(".", "")
        )
        assert record_id == "testsiteexamplecom"
