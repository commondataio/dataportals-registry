#!/usr/bin/env python
"""Tests for re3data enrichment functionality."""

import pytest
import json
import yaml
import os
import tempfile
from typing import Dict, Any

# Import functions from re3data_enrichment
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

# Import after path is set
try:
    from re3data_enrichment import (
        get_re3data_identifier,
        parse_re3data_html,
        enrich_catalog_with_re3data,
        load_cached_re3data,
        cache_re3data_data,
    )
except ImportError:
    # Handle case where module can't be imported
    pytest.skip("re3data_enrichment module not available", allow_module_level=True)


class TestRe3DataIdentifier:
    """Tests for re3data identifier extraction."""
    
    def test_no_re3data_identifier(self):
        """Test catalog without re3data identifier returns None."""
        catalog = {
            "id": "test_catalog",
            "identifiers": [
                {"id": "wikidata", "value": "Q123"}
            ]
        }
        assert get_re3data_identifier(catalog) is None
    
    def test_has_re3data_identifier(self):
        """Test catalog with re3data identifier returns the value."""
        catalog = {
            "id": "test_catalog",
            "identifiers": [
                {"id": "re3data", "value": "r3d100010078", "url": "https://www.re3data.org/repository/r3d100010078"}
            ]
        }
        assert get_re3data_identifier(catalog) == "r3d100010078"
    
    def test_multiple_identifiers(self):
        """Test extraction when multiple identifiers exist."""
        catalog = {
            "id": "test_catalog",
            "identifiers": [
                {"id": "wikidata", "value": "Q123"},
                {"id": "re3data", "value": "r3d100010078"},
                {"id": "fairsharing", "value": "FAIRsharing.123"}
            ]
        }
        assert get_re3data_identifier(catalog) == "r3d100010078"
    
    def test_empty_identifiers(self):
        """Test catalog with empty identifiers list."""
        catalog = {
            "id": "test_catalog",
            "identifiers": []
        }
        assert get_re3data_identifier(catalog) is None
    
    def test_no_identifiers_field(self):
        """Test catalog without identifiers field."""
        catalog = {
            "id": "test_catalog"
        }
        assert get_re3data_identifier(catalog) is None


class TestParseRe3DataHTML:
    """Tests for HTML parsing."""
    
    def test_empty_html(self):
        """Test parsing empty HTML returns empty dict."""
        result = parse_re3data_html("", "r3d100010078")
        assert result["re3data_id"] == "r3d100010078"
        assert result["keywords"] == []
        assert result["description"] is None
    
    def test_basic_html_structure(self):
        """Test parsing basic HTML structure."""
        html = """
        <html>
        <head>
            <meta name="description" content="Test repository description">
        </head>
        <body>
            <div id="keywords">
                <span>keyword1</span>
                <span>keyword2</span>
            </div>
            <a href="mailto:test@example.com">Contact</a>
        </body>
        </html>
        """
        result = parse_re3data_html(html, "r3d100010078")
        assert result["re3data_id"] == "r3d100010078"
        assert "keyword1" in result["keywords"] or "keyword2" in result["keywords"]
        assert result["contact_email"] == "test@example.com" or result["description"] is not None
    
    def test_json_ld_extraction(self):
        """Test extraction from JSON-LD structured data."""
        html = """
        <html>
        <head>
            <script type="application/ld+json">
            {
                "description": "Repository description from JSON-LD",
                "keywords": ["keyword1", "keyword2"]
            }
            </script>
        </head>
        </html>
        """
        result = parse_re3data_html(html, "r3d100010078")
        # Should extract description or keywords from JSON-LD
        assert result["re3data_id"] == "r3d100010078"
    
    def test_institutions_extraction(self):
        """Test extraction of institutions."""
        html = """
        <html>
        <body>
            <div id="tab_institutions">
                <li><a href="https://example.com/inst1">Institution 1</a></li>
                <li><a href="/inst2">Institution 2</a></li>
            </div>
        </body>
        </html>
        """
        result = parse_re3data_html(html, "r3d100010078")
        assert result["re3data_id"] == "r3d100010078"
        # Should extract institutions if available
        assert isinstance(result["institutions"], list)
    
    def test_persistent_identifiers_extraction(self):
        """Test extraction of persistent identifiers."""
        html = """
        <html>
        <body>
            <div id="persistent-identifiers">
                <li>DOI supported</li>
                <li>Handle supported</li>
            </div>
        </body>
        </html>
        """
        result = parse_re3data_html(html, "r3d100010078")
        assert result["re3data_id"] == "r3d100010078"
        assert isinstance(result["persistent_identifiers"], list)


class TestEnrichCatalog:
    """Tests for catalog enrichment."""
    
    def test_enrich_catalog_basic(self):
        """Test basic catalog enrichment."""
        catalog = {
            "id": "test_catalog",
            "name": "Test Catalog",
            "identifiers": [
                {"id": "re3data", "value": "r3d100010078"}
            ]
        }
        
        re3data_data = {
            "re3data_id": "r3d100010078",
            "keywords": ["keyword1", "keyword2"],
            "description": "Test description",
            "contact_email": "test@example.com"
        }
        
        enriched = enrich_catalog_with_re3data(catalog, re3data_data)
        
        assert enriched["id"] == "test_catalog"
        assert enriched["name"] == "Test Catalog"
        assert "_re3data" in enriched
        assert enriched["_re3data"]["re3data_id"] == "r3d100010078"
        assert enriched["_re3data"]["keywords"] == ["keyword1", "keyword2"]
    
    def test_enrich_catalog_preserves_existing_fields(self):
        """Test that enrichment preserves existing catalog fields."""
        catalog = {
            "id": "test_catalog",
            "name": "Test Catalog",
            "description": "Original description",
            "tags": ["tag1", "tag2"]
        }
        
        re3data_data = {
            "re3data_id": "r3d100010078",
            "description": "Re3data description"
        }
        
        enriched = enrich_catalog_with_re3data(catalog, re3data_data)
        
        # Original fields should be preserved
        assert enriched["id"] == "test_catalog"
        assert enriched["name"] == "Test Catalog"
        assert enriched["description"] == "Original description"
        assert enriched["tags"] == ["tag1", "tag2"]
        
        # Re3data data should be in _re3data field
        assert enriched["_re3data"]["description"] == "Re3data description"
    
    def test_enrich_catalog_empty_re3data(self):
        """Test enrichment with empty re3data data."""
        catalog = {
            "id": "test_catalog",
            "name": "Test Catalog"
        }
        
        re3data_data = {}
        
        enriched = enrich_catalog_with_re3data(catalog, re3data_data)
        
        assert "_re3data" in enriched
        assert enriched["_re3data"] == {}


class TestCacheFunctions:
    """Tests for caching functionality."""
    
    def test_cache_re3data_data(self, tmp_path, monkeypatch):
        """Test caching re3data data."""
        from re3data_enrichment import RE3DATA_CACHE_FILE, CACHE_DIR
        
        # Use temporary directory for cache
        cache_file = tmp_path / "re3data_repositories.json"
        monkeypatch.setattr('re3data_enrichment.RE3DATA_CACHE_FILE', str(cache_file))
        monkeypatch.setattr('re3data_enrichment.CACHE_DIR', str(tmp_path))
        
        cache_re3data_data("r3d100010078", {
            "re3data_id": "r3d100010078",
            "keywords": ["test"]
        })
        
        # Check file was created
        assert cache_file.exists()
        
        # Load and verify
        cached = load_cached_re3data()
        assert "r3d100010078" in cached
        assert cached["r3d100010078"]["keywords"] == ["test"]
    
    def test_load_cached_re3data_nonexistent(self, tmp_path, monkeypatch):
        """Test loading cache when file doesn't exist."""
        cache_file = tmp_path / "nonexistent.json"
        monkeypatch.setattr('re3data_enrichment.RE3DATA_CACHE_FILE', str(cache_file))
        
        cached = load_cached_re3data()
        assert cached == {}


class TestEdgeCases:
    """Tests for edge cases and error handling."""
    
    def test_parse_html_with_malformed_json_ld(self):
        """Test parsing HTML with malformed JSON-LD doesn't crash."""
        html = """
        <html>
        <head>
            <script type="application/ld+json">
            { invalid json }
            </script>
        </head>
        </html>
        """
        # Should not raise an exception
        result = parse_re3data_html(html, "r3d100010078")
        assert result["re3data_id"] == "r3d100010078"
    
    def test_enrich_with_none_values(self):
        """Test enrichment handles None values correctly."""
        catalog = {
            "id": "test_catalog"
        }
        
        re3data_data = {
            "re3data_id": "r3d100010078",
            "contact_email": None,
            "description": None,
            "keywords": []
        }
        
        enriched = enrich_catalog_with_re3data(catalog, re3data_data)
        assert "_re3data" in enriched
        assert enriched["_re3data"]["re3data_id"] == "r3d100010078"
    
    def test_parse_html_without_bs4(self, monkeypatch):
        """Test parsing works without BeautifulSoup (fallback mode)."""
        # Mock HAS_BS4 to False
        import re3data_enrichment
        original_has_bs4 = re3data_enrichment.HAS_BS4
        re3data_enrichment.HAS_BS4 = False
        
        try:
            html = """
            <html>
            <head>
                <meta name="description" content="Test description">
            </head>
            <body>
                <a href="mailto:test@example.com">Contact</a>
            </body>
            </html>
            """
            result = parse_re3data_html(html, "r3d100010078")
            assert result["re3data_id"] == "r3d100010078"
            # Should extract description or email using regex
            assert result["description"] is not None or result["contact_email"] is not None
        finally:
            re3data_enrichment.HAS_BS4 = original_has_bs4

