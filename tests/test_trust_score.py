"""Tests for trust score calculation"""

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from calculate_trust_scores import (
    calculate_trust_score,
    calculate_owner_type_score,
    calculate_catalog_type_score,
    calculate_license_score,
    calculate_re3data_score,
    calculate_additional_factors_score,
    get_re3data_identifier,
)


class TestOwnerTypeScore:
    """Tests for owner type scoring"""

    def test_academy_score(self):
        """Test Academy owner type gets highest score"""
        catalog = {"owner": {"type": "Academy"}}
        assert calculate_owner_type_score(catalog) == 40

    def test_central_government_score(self):
        """Test Central government score"""
        catalog = {"owner": {"type": "Central government"}}
        assert calculate_owner_type_score(catalog) == 35

    def test_regional_government_score(self):
        """Test Regional government score"""
        catalog = {"owner": {"type": "Regional government"}}
        assert calculate_owner_type_score(catalog) == 30

    def test_local_government_score(self):
        """Test Local government score"""
        catalog = {"owner": {"type": "Local government"}}
        assert calculate_owner_type_score(catalog) == 25

    def test_international_score(self):
        """Test International organization score"""
        catalog = {"owner": {"type": "International"}}
        assert calculate_owner_type_score(catalog) == 30

    def test_civil_society_score(self):
        """Test Civil society score"""
        catalog = {"owner": {"type": "Civil society"}}
        assert calculate_owner_type_score(catalog) == 15

    def test_ngo_score(self):
        """Test NGO score"""
        catalog = {"owner": {"type": "NGO"}}
        assert calculate_owner_type_score(catalog) == 15

    def test_business_score(self):
        """Test Business score"""
        catalog = {"owner": {"type": "Business"}}
        assert calculate_owner_type_score(catalog) == 10

    def test_community_score(self):
        """Test Community score"""
        catalog = {"owner": {"type": "Community"}}
        assert calculate_owner_type_score(catalog) == 5

    def test_unknown_owner_type(self):
        """Test unknown owner type gets default score"""
        catalog = {"owner": {"type": "Unknown"}}
        assert calculate_owner_type_score(catalog) == 10

    def test_missing_owner(self):
        """Test missing owner gets default score"""
        catalog = {}
        assert calculate_owner_type_score(catalog) == 10


class TestCatalogTypeScore:
    """Tests for catalog type scoring"""

    def test_scientific_repository_score(self):
        """Test Scientific data repository gets highest score"""
        catalog = {"catalog_type": "Scientific data repository"}
        assert calculate_catalog_type_score(catalog) == 10

    def test_open_data_portal_score(self):
        """Test Open data portal score"""
        catalog = {"catalog_type": "Open data portal"}
        assert calculate_catalog_type_score(catalog) == 5

    def test_geoportal_score(self):
        """Test Geoportal score"""
        catalog = {"catalog_type": "Geoportal"}
        assert calculate_catalog_type_score(catalog) == 5

    def test_indicators_catalog_score(self):
        """Test Indicators catalog score"""
        catalog = {"catalog_type": "Indicators catalog"}
        assert calculate_catalog_type_score(catalog) == 5

    def test_microdata_catalog_score(self):
        """Test Microdata catalog score"""
        catalog = {"catalog_type": "Microdata catalog"}
        assert calculate_catalog_type_score(catalog) == 5

    def test_data_search_engines_penalty(self):
        """Test Data search engines (aggregators) get penalty"""
        catalog = {"catalog_type": "Data search engines"}
        assert calculate_catalog_type_score(catalog) == -10

    def test_data_marketplaces_penalty(self):
        """Test Data marketplaces get penalty"""
        catalog = {"catalog_type": "Data marketplaces"}
        assert calculate_catalog_type_score(catalog) == -5

    def test_machine_learning_catalog_neutral(self):
        """Test Machine learning catalog is neutral"""
        catalog = {"catalog_type": "Machine learning catalog"}
        assert calculate_catalog_type_score(catalog) == 0

    def test_unknown_catalog_type(self):
        """Test unknown catalog type gets default score"""
        catalog = {"catalog_type": "Unknown"}
        assert calculate_catalog_type_score(catalog) == 0

    def test_missing_catalog_type(self):
        """Test missing catalog type gets default score"""
        catalog = {}
        assert calculate_catalog_type_score(catalog) == 0


class TestLicenseScore:
    """Tests for license/rights scoring"""

    def test_has_license_id(self):
        """Test catalog with license_id gets points"""
        catalog = {"rights": {"license_id": "CC-BY-4.0"}}
        score = calculate_license_score(catalog)
        assert score >= 15  # Has license + rights_type if specified

    def test_has_license_name(self):
        """Test catalog with license_name gets points"""
        catalog = {"rights": {"license_name": "Creative Commons Attribution"}}
        score = calculate_license_score(catalog)
        assert score >= 15

    def test_has_license_url(self):
        """Test catalog with license_url gets points"""
        catalog = {"rights": {"license_url": "https://creativecommons.org/licenses/by/4.0/"}}
        score = calculate_license_score(catalog)
        assert score >= 15

    def test_has_all_license_info(self):
        """Test catalog with all license info gets maximum points"""
        catalog = {
            "rights": {
                "license_id": "CC-BY-4.0",
                "license_name": "Creative Commons Attribution 4.0",
                "license_url": "https://creativecommons.org/licenses/by/4.0/",
                "rights_type": "open"
            }
        }
        score = calculate_license_score(catalog)
        assert score == 20  # 15 for license + 5 for rights_type

    def test_missing_all_license_info(self):
        """Test catalog missing all license info gets penalty"""
        catalog = {}
        assert calculate_license_score(catalog) == -15

    def test_rights_type_unknown_penalty(self):
        """Test rights_type 'unknown' gets penalty"""
        catalog = {"rights": {"rights_type": "unknown"}}
        score = calculate_license_score(catalog)
        # Should have penalty for unknown, but no license info
        assert score < 0

    def test_rights_type_specified_bonus(self):
        """Test rights_type specified (not unknown) gets bonus"""
        catalog = {"rights": {"rights_type": "open"}}
        score = calculate_license_score(catalog)
        assert score == 5  # +5 for rights_type, but no license details

    def test_empty_rights(self):
        """Test empty rights dict gets penalty"""
        catalog = {"rights": {}}
        assert calculate_license_score(catalog) == -15


class TestRe3DataScore:
    """Tests for re3data scoring"""

    def test_no_re3data_identifier(self):
        """Test catalog without re3data identifier gets 0"""
        catalog = {}
        assert calculate_re3data_score(catalog) == 0

    def test_has_re3data_identifier(self):
        """Test catalog with re3data identifier gets points"""
        catalog = {
            "identifiers": [
                {"id": "re3data", "value": "r3d100010078", "url": "https://www.re3data.org/repository/r3d100010078"}
            ]
        }
        assert calculate_re3data_score(catalog) == 10

    def test_has_re3data_with_trust_seal(self):
        """Test catalog with re3data identifier and trust seal gets bonus"""
        catalog = {
            "identifiers": [
                {"id": "re3data", "value": "r3d100010078"}
            ]
        }
        trust_seals = {"r3d100010078": True}
        assert calculate_re3data_score(catalog, trust_seals) == 20

    def test_re3data_no_trust_seal(self):
        """Test catalog with re3data but no trust seal gets base points only"""
        catalog = {
            "identifiers": [
                {"id": "re3data", "value": "r3d100010078"}
            ]
        }
        trust_seals = {"r3d100010078": False}
        assert calculate_re3data_score(catalog, trust_seals) == 10

    def test_get_re3data_identifier(self):
        """Test extracting re3data identifier"""
        catalog = {
            "identifiers": [
                {"id": "wikidata", "value": "Q123"},
                {"id": "re3data", "value": "r3d100010078"}
            ]
        }
        assert get_re3data_identifier(catalog) == "r3d100010078"

    def test_get_re3data_identifier_missing(self):
        """Test extracting re3data identifier when missing"""
        catalog = {
            "identifiers": [
                {"id": "wikidata", "value": "Q123"}
            ]
        }
        assert get_re3data_identifier(catalog) is None


class TestAdditionalFactorsScore:
    """Tests for additional factors scoring"""

    def test_active_api(self):
        """Test catalog with active API gets points"""
        catalog = {"api": True, "api_status": "active"}
        assert calculate_additional_factors_score(catalog) == 10  # 5 for API + 5 for active

    def test_active_status(self):
        """Test catalog with active status gets points"""
        catalog = {"status": "active"}
        assert calculate_additional_factors_score(catalog) == 5

    def test_inactive_status_penalty(self):
        """Test catalog with inactive status gets penalty"""
        catalog = {"status": "inactive"}
        assert calculate_additional_factors_score(catalog) == -5

    def test_api_but_inactive_status(self):
        """Test catalog with API but inactive status"""
        catalog = {"api": True, "api_status": "inactive", "status": "inactive"}
        score = calculate_additional_factors_score(catalog)
        # API not active, status inactive = -5
        assert score == -5

    def test_no_api_no_status(self):
        """Test catalog with no API and no status"""
        catalog = {}
        assert calculate_additional_factors_score(catalog) == 0

    def test_uncertain_status(self):
        """Test catalog with uncertain status gets no points"""
        catalog = {"status": "uncertain"}
        assert calculate_additional_factors_score(catalog) == 0


class TestTrustScoreCalculation:
    """Tests for complete trust score calculation"""

    def test_high_trust_score(self):
        """Test calculation of high trust score"""
        catalog = {
            "owner": {"type": "Academy"},
            "catalog_type": "Scientific data repository",
            "rights": {
                "license_id": "CC-BY-4.0",
                "license_name": "Creative Commons Attribution 4.0",
                "rights_type": "open"
            },
            "identifiers": [
                {"id": "re3data", "value": "r3d100010078"}
            ],
            "api": True,
            "api_status": "active",
            "status": "active"
        }
        trust_seals = {"r3d100010078": True}
        score, components = calculate_trust_score(catalog, trust_seals)
        
        assert score == 100  # Clamped to 100
        assert components["owner_type_score"] == 40
        assert components["catalog_type_score"] == 10
        assert components["license_score"] == 20
        assert components["re3data_score"] == 20
        assert components["additional_factors_score"] == 10
        assert components["base_score"] == 100

    def test_moderate_trust_score(self):
        """Test calculation of moderate trust score"""
        catalog = {
            "owner": {"type": "Civil society"},
            "catalog_type": "Open data portal",
            "rights": {
                "rights_type": "open"
            },
            "status": "active"
        }
        score, components = calculate_trust_score(catalog)
        
        assert 0 <= score <= 100
        assert components["owner_type_score"] == 15
        assert components["catalog_type_score"] == 5
        assert components["license_score"] == 5
        assert components["re3data_score"] == 0
        assert components["additional_factors_score"] == 5

    def test_low_trust_score(self):
        """Test calculation of low trust score"""
        catalog = {
            "owner": {"type": "Community"},
            "catalog_type": "Data search engines",
            "status": "inactive"
        }
        score, components = calculate_trust_score(catalog)
        
        assert 0 <= score <= 100
        assert components["owner_type_score"] == 5
        assert components["catalog_type_score"] == -10
        assert components["license_score"] == -15
        assert components["re3data_score"] == 0
        assert components["additional_factors_score"] == -5

    def test_score_clamping(self):
        """Test that scores are clamped to 0-100 range"""
        # Create a catalog that would exceed 100
        catalog = {
            "owner": {"type": "Academy"},
            "catalog_type": "Scientific data repository",
            "rights": {
                "license_id": "CC-BY-4.0",
                "license_name": "CC-BY-4.0",
                "license_url": "https://creativecommons.org/licenses/by/4.0/",
                "rights_type": "open"
            },
            "identifiers": [
                {"id": "re3data", "value": "r3d100010078"}
            ],
            "api": True,
            "api_status": "active",
            "status": "active"
        }
        trust_seals = {"r3d100010078": True}
        score, _ = calculate_trust_score(catalog, trust_seals)
        
        assert score == 100  # Should be clamped

    def test_negative_score_clamping(self):
        """Test that negative scores are clamped to 0"""
        catalog = {
            "owner": {"type": "Community"},
            "catalog_type": "Data search engines",
            "status": "inactive"
        }
        score, components = calculate_trust_score(catalog)
        
        # Base score might be negative, but final should be >= 0
        assert score >= 0
        assert score <= 100

    def test_minimal_catalog(self):
        """Test score calculation with minimal catalog data"""
        catalog = {
            "owner": {"type": "Business"},
            "catalog_type": "Other",
            "status": "uncertain"
        }
        score, components = calculate_trust_score(catalog)
        
        assert 0 <= score <= 100
        assert "owner_type_score" in components
        assert "catalog_type_score" in components
        assert "license_score" in components
        assert "re3data_score" in components
        assert "additional_factors_score" in components
        assert "base_score" in components

    def test_missing_optional_fields(self):
        """Test score calculation with many missing optional fields"""
        catalog = {
            "owner": {"type": "Business"},
            "catalog_type": "Other"
        }
        score, components = calculate_trust_score(catalog)
        
        assert 0 <= score <= 100
        # Should still have all components
        assert len(components) == 6

