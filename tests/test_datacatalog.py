"""Tests for pydantic DataCatalog model"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from cdiapi.data.datacatalog import DataCatalog
from cdiapi.data.shared import (
    Country,
    LocationBase,
    Location,
    Endpoint,
    Identifier,
    Topic,
    Software,
    Organization,
    SpokenLanguage,
)


class TestDataCatalog:
    """Tests for DataCatalog pydantic model"""

    def test_valid_catalog(self, sample_catalog_dict):
        """Test creating a valid DataCatalog instance"""
        catalog = DataCatalog(**sample_catalog_dict)
        assert catalog.id == "testcatalog"
        assert catalog.name == "Test Catalog"
        assert catalog.link == "https://example.com"
        assert catalog.catalog_type == "Open data portal"
        assert catalog.api is True
        assert catalog.status == "active"

    def test_catalog_required_fields(self):
        """Test that required fields are enforced"""
        from pydantic import ValidationError

        incomplete_data = {
            "id": "test",
            "name": "Test",
            # Missing required fields
        }
        with pytest.raises(ValidationError):
            DataCatalog(**incomplete_data)

    def test_catalog_optional_fields(self, sample_catalog_dict):
        """Test that optional fields work correctly"""
        # Remove optional fields
        sample_catalog_dict.pop("topics", None)
        sample_catalog_dict.pop("endpoints", None)
        sample_catalog_dict.pop("properties", None)

        catalog = DataCatalog(**sample_catalog_dict)
        assert catalog.topics == []
        assert catalog.endpoints == []

    def test_catalog_coverage(self, sample_catalog_dict):
        """Test coverage field structure"""
        catalog = DataCatalog(**sample_catalog_dict)
        assert len(catalog.coverage) > 0
        assert catalog.coverage[0].location.country.id == "US"
        assert catalog.coverage[0].location.country.name == "United States"

    def test_catalog_owner(self, sample_catalog_dict):
        """Test owner field structure"""
        catalog = DataCatalog(**sample_catalog_dict)
        assert catalog.owner.name == "Test Organization"
        assert catalog.owner.type == "Central government"
        assert catalog.owner.location.country.id == "US"

    def test_catalog_software(self, sample_catalog_dict):
        """Test software field structure"""
        catalog = DataCatalog(**sample_catalog_dict)
        assert catalog.software.id == "ckan"
        assert catalog.software.name == "CKAN"

    def test_catalog_langs(self, sample_catalog_dict):
        """Test langs field structure"""
        catalog = DataCatalog(**sample_catalog_dict)
        assert len(catalog.langs) > 0
        assert catalog.langs[0].id == "EN"
        assert catalog.langs[0].name == "English"

    def test_catalog_with_endpoints(self, sample_catalog_dict):
        """Test catalog with endpoints"""
        sample_catalog_dict["endpoints"] = [
            {"type": "ckan", "url": "https://example.com/api/3", "version": "3"}
        ]
        catalog = DataCatalog(**sample_catalog_dict)
        assert len(catalog.endpoints) == 1
        assert catalog.endpoints[0].type == "ckan"
        assert catalog.endpoints[0].url == "https://example.com/api/3"

    def test_catalog_with_identifiers(self, sample_catalog_dict):
        """Test catalog with identifiers"""
        sample_catalog_dict["identifiers"] = [
            {
                "id": "wikidata",
                "url": "https://www.wikidata.org/wiki/Q123",
                "value": "Q123",
            }
        ]
        catalog = DataCatalog(**sample_catalog_dict)
        assert len(catalog.identifiers) == 1
        assert catalog.identifiers[0].id == "wikidata"
        assert catalog.identifiers[0].value == "Q123"


class TestSharedModels:
    """Tests for shared pydantic models"""

    def test_country_model(self):
        """Test Country model"""
        country = Country(id="US", name="United States")
        assert country.id == "US"
        assert country.name == "United States"

    def test_location_base(self):
        """Test LocationBase model"""
        location = LocationBase(country=Country(id="US", name="United States"), level=1)
        assert location.country.id == "US"
        assert location.level == 1

    def test_location(self):
        """Test Location model"""
        location = Location(
            location=LocationBase(
                country=Country(id="US", name="United States"), level=1
            )
        )
        assert location.location.country.id == "US"

    def test_endpoint(self):
        """Test Endpoint model"""
        endpoint = Endpoint(type="ckan", url="https://example.com/api/3", version="3")
        assert endpoint.type == "ckan"
        assert endpoint.url == "https://example.com/api/3"
        assert endpoint.version == "3"

    def test_identifier(self):
        """Test Identifier model"""
        identifier = Identifier(
            id="wikidata", url="https://www.wikidata.org/wiki/Q123", value="Q123"
        )
        assert identifier.id == "wikidata"
        assert identifier.value == "Q123"

    def test_software(self):
        """Test Software model"""
        software = Software(id="ckan", name="CKAN")
        assert software.id == "ckan"
        assert software.name == "CKAN"

    def test_organization(self):
        """Test Organization model"""
        org = Organization(
            name="Test Org",
            type="Central government",
            location=LocationBase(
                country=Country(id="US", name="United States"), level=1
            ),
        )
        assert org.name == "Test Org"
        assert org.type == "Central government"
        assert org.location.country.id == "US"

    def test_spoken_language(self):
        """Test SpokenLanguage model"""
        lang = SpokenLanguage(id="EN", name="English")
        assert lang.id == "EN"
        assert lang.name == "English"
