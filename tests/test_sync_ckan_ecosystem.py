"""Tests for sync_ckan_ecosystem.py functions"""

import os
import json
import tempfile
import pytest
from unittest.mock import Mock, patch, MagicMock
from urllib.parse import urlparse

# Import sync_ckan_ecosystem functions
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from sync_ckan_ecosystem import (
    normalize_url,
    normalize_domain,
    parse_ckan_site_record,
    check_duplicate,
    enrich_metadata_from_web,
)


class TestNormalizeUrl:
    """Tests for normalize_url function"""

    def test_normalize_url_basic(self):
        """Test basic URL normalization"""
        assert normalize_url("https://example.com") == "example.com"
        assert normalize_url("http://example.com/") == "example.com"
        assert normalize_url("https://www.example.com") == "example.com"

    def test_normalize_url_with_path(self):
        """Test URL normalization with paths"""
        assert normalize_url("https://example.com/path") == "example.com/path"
        assert normalize_url("https://example.com/path/") == "example.com/path"

    def test_normalize_url_case_insensitive(self):
        """Test that normalization is case insensitive"""
        assert normalize_url("HTTPS://EXAMPLE.COM") == "example.com"

    def test_normalize_url_empty(self):
        """Test normalization of empty URL"""
        assert normalize_url("") == ""
        assert normalize_url(None) == ""


class TestNormalizeDomain:
    """Tests for normalize_domain function"""

    def test_normalize_domain_basic(self):
        """Test basic domain extraction"""
        assert normalize_domain("https://example.com") == "example.com"
        assert normalize_domain("http://example.com/path") == "example.com"

    def test_normalize_domain_with_port(self):
        """Test domain extraction with port"""
        assert normalize_domain("https://example.com:8080") == "example.com"

    def test_normalize_domain_with_subdomain(self):
        """Test domain extraction with subdomain"""
        assert normalize_domain("https://www.example.com") == "example.com"


class TestParseCkanSiteRecord:
    """Tests for parse_ckan_site_record function"""

    def test_parse_basic_record(self):
        """Test parsing a basic CKAN site record"""
        record = {
            "id": "test-site",
            "name": "test-site",
            "title": "Test Site",
            "url": "https://test.example.com",
            "notes": "A test CKAN site",
        }
        result = parse_ckan_site_record(record)
        assert result is not None
        assert result["url"] == "https://test.example.com"
        assert result["name"] == "Test Site"
        assert result["description"] == "A test CKAN site"

    def test_parse_record_without_url(self):
        """Test parsing a record without URL"""
        record = {
            "id": "test-site",
            "name": "test-site",
            "title": "Test Site",
        }
        result = parse_ckan_site_record(record)
        assert result is None

    def test_parse_record_url_variations(self):
        """Test parsing records with URL in different fields"""
        # Test with site_url
        record1 = {"site_url": "https://test1.com"}
        result1 = parse_ckan_site_record(record1)
        assert result1["url"] == "https://test1.com"

        # Test with link
        record2 = {"link": "https://test2.com"}
        result2 = parse_ckan_site_record(record2)
        assert result2["url"] == "https://test2.com"

        # Test with homepage
        record3 = {"homepage": "https://test3.com"}
        result3 = parse_ckan_site_record(record3)
        assert result3["url"] == "https://test3.com"

    def test_parse_record_adds_https(self):
        """Test that http:// is added if missing"""
        record = {"url": "test.example.com"}
        result = parse_ckan_site_record(record)
        assert result["url"] == "https://test.example.com"

    def test_parse_record_with_organization(self):
        """Test parsing record with organization info"""
        record = {
            "url": "https://test.com",
            "organization": {
                "title": "Test Organization",
                "type": "government",
            },
        }
        result = parse_ckan_site_record(record)
        assert result["owner_name"] == "Test Organization"
        assert result["owner_type"] == "government"

    def test_parse_record_with_country_tag(self):
        """Test parsing record with country tag"""
        record = {
            "url": "https://test.com",
            "tags": [{"name": "us"}, {"name": "opendata"}],
        }
        result = parse_ckan_site_record(record)
        assert result["country"] == "US"


class TestCheckDuplicate:
    """Tests for check_duplicate function"""

    def test_check_duplicate_by_url(self):
        """Test duplicate detection by URL"""
        existing_urls = {"example.com", "test.com"}
        existing_ids = set()
        url_to_id = {"example.com": "examplecom"}
        
        is_dup, existing_id = check_duplicate(
            "https://example.com", existing_urls, existing_ids, url_to_id
        )
        assert is_dup is True
        assert existing_id == "examplecom"

    def test_check_duplicate_by_domain(self):
        """Test duplicate detection by domain"""
        existing_urls = {"example.com"}
        existing_ids = set()
        url_to_id = {"example.com": "examplecom"}
        
        is_dup, existing_id = check_duplicate(
            "https://www.example.com/path", existing_urls, existing_ids, url_to_id
        )
        assert is_dup is True

    def test_check_duplicate_by_id(self):
        """Test duplicate detection by generated ID"""
        existing_urls = set()
        existing_ids = {"examplecom"}
        url_to_id = {}
        
        is_dup, existing_id = check_duplicate(
            "https://example.com", existing_urls, existing_ids, url_to_id
        )
        assert is_dup is True
        assert existing_id == "examplecom"

    def test_check_duplicate_not_found(self):
        """Test that non-duplicate URLs are not detected"""
        existing_urls = {"other.com"}
        existing_ids = {"othercom"}
        url_to_id = {}
        
        is_dup, existing_id = check_duplicate(
            "https://new-site.com", existing_urls, existing_ids, url_to_id
        )
        assert is_dup is False
        assert existing_id is None


class TestEnrichMetadataFromWeb:
    """Tests for enrich_metadata_from_web function"""

    @patch("sync_ckan_ecosystem.requests.get")
    @patch("sync_ckan_ecosystem.HAS_BS4", True)
    def test_enrich_with_meta_description(self, mock_get):
        """Test enriching metadata with meta description"""
        from bs4 import BeautifulSoup
        
        html = """
        <html>
        <head>
            <meta name="description" content="Test description">
            <title>Test Site</title>
        </head>
        <body></body>
        </html>
        """
        
        mock_response = Mock()
        mock_response.content = html.encode()
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        existing = {"url": "https://test.com"}
        result = enrich_metadata_from_web("https://test.com", existing)
        
        assert result["description"] == "Test description"
        mock_get.assert_called_once()

    @patch("sync_ckan_ecosystem.requests.get")
    @patch("sync_ckan_ecosystem.HAS_BS4", True)
    def test_enrich_with_og_description(self, mock_get):
        """Test enriching metadata with og:description"""
        from bs4 import BeautifulSoup
        
        html = """
        <html>
        <head>
            <meta property="og:description" content="OG description">
        </head>
        <body></body>
        </html>
        """
        
        mock_response = Mock()
        mock_response.content = html.encode()
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        existing = {"url": "https://test.com"}
        result = enrich_metadata_from_web("https://test.com", existing)
        
        assert result["description"] == "OG description"

    @patch("sync_ckan_ecosystem.requests.get")
    @patch("sync_ckan_ecosystem.HAS_BS4", False)
    def test_enrich_without_bs4(self, mock_get):
        """Test that enrichment skips if BeautifulSoup4 is not available"""
        existing = {"url": "https://test.com", "description": "Existing"}
        result = enrich_metadata_from_web("https://test.com", existing)
        
        assert result == existing
        mock_get.assert_not_called()

    @patch("sync_ckan_ecosystem.requests.get")
    @patch("sync_ckan_ecosystem.HAS_BS4", True)
    def test_enrich_handles_request_error(self, mock_get):
        """Test that enrichment handles request errors gracefully"""
        import requests
        mock_get.side_effect = requests.RequestException("Connection error")
        
        existing = {"url": "https://test.com", "description": "Existing"}
        result = enrich_metadata_from_web("https://test.com", existing)
        
        assert result == existing  # Should return unchanged on error


class TestIntegration:
    """Integration tests for the sync workflow"""

    @patch("sync_ckan_ecosystem.query_ckan_api")
    def test_fetch_ckan_ecosystem_dataset_mock(self, mock_query):
        """Test fetching CKAN ecosystem dataset with mocked API"""
        from sync_ckan_ecosystem import fetch_ckan_ecosystem_dataset
        
        # Mock package_show response
        mock_query.side_effect = [
            {
                "result": {
                    "name": "ckan-sites-metadata",
                    "resources": [
                        {
                            "url": "https://example.com/data.json",
                            "name": "CKAN Sites Data",
                        }
                    ],
                }
            },
            {
                "result": {
                    "results": [
                        {
                            "id": "site1",
                            "name": "site1",
                            "url": "https://site1.example.com",
                            "title": "Site 1",
                        },
                        {
                            "id": "site2",
                            "name": "site2",
                            "url": "https://site2.example.com",
                            "title": "Site 2",
                        },
                    ]
                }
            },
        ]
        
        records = fetch_ckan_ecosystem_dataset()
        assert len(records) == 2
        assert records[0]["id"] == "site1"
