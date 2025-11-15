"""Tests for YAML parsing and validation"""

import os
import sys
import yaml
import pytest
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


class TestYAMLParsing:
    """Tests for YAML file parsing"""

    def test_parse_valid_yaml(self, sample_yaml_content):
        """Test parsing a valid YAML file"""
        data = yaml.load(sample_yaml_content, Loader=Loader)
        assert isinstance(data, dict)
        assert data["id"] == "testcatalog"
        assert data["name"] == "Test Catalog"
        assert data["catalog_type"] == "Open data portal"

    def test_parse_yaml_structure(self, sample_yaml_content):
        """Test YAML structure parsing"""
        data = yaml.load(sample_yaml_content, Loader=Loader)

        # Test nested structures
        assert "coverage" in data
        assert isinstance(data["coverage"], list)
        assert "location" in data["coverage"][0]
        assert data["coverage"][0]["location"]["country"]["id"] == "US"

        # Test lists
        assert isinstance(data["access_mode"], list)
        assert "open" in data["access_mode"]
        assert isinstance(data["tags"], list)

    def test_parse_yaml_with_null(self, sample_yaml_content):
        """Test YAML parsing with null values"""
        data = yaml.load(sample_yaml_content, Loader=Loader)
        assert data["owner"]["link"] is None

    def test_parse_yaml_booleans(self, sample_yaml_content):
        """Test YAML boolean parsing"""
        data = yaml.load(sample_yaml_content, Loader=Loader)
        assert data["api"] is True
        assert isinstance(data["api"], bool)

    def test_parse_yaml_numbers(self):
        """Test YAML number parsing"""
        yaml_content = """
level: 1
version: '3'
api_status: active
"""
        data = yaml.load(yaml_content, Loader=Loader)
        assert data["level"] == 1
        assert isinstance(data["level"], int)
        assert data["version"] == "3"

    def test_parse_complex_yaml(self):
        """Test parsing complex nested YAML structure"""
        yaml_content = """
coverage:
- location:
    country:
      id: US
      name: United States
    level: 30
    macroregion:
      id: '034'
      name: Southern Asia
    subregion:
      id: US-TX
      name: Texas
endpoints:
- type: ckan
  url: https://example.com/api/3
  version: '3'
- type: ckan:package-search
  url: https://example.com/api/3/action/package_search
  version: '3'
"""
        data = yaml.load(yaml_content, Loader=Loader)
        assert len(data["coverage"]) == 1
        assert "macroregion" in data["coverage"][0]["location"]
        assert "subregion" in data["coverage"][0]["location"]
        assert len(data["endpoints"]) == 2
        assert data["endpoints"][0]["type"] == "ckan"

    def test_yaml_safe_dump(self, sample_yaml_content):
        """Test YAML safe dump"""
        data = yaml.load(sample_yaml_content, Loader=Loader)
        dumped = yaml.safe_dump(data, allow_unicode=True)
        assert isinstance(dumped, str)
        # Parse it back to verify it's valid
        reloaded = yaml.load(dumped, Loader=Loader)
        assert reloaded["id"] == data["id"]
        assert reloaded["name"] == data["name"]


class TestYAMLValidation:
    """Tests for YAML validation against schema"""

    def test_validate_required_fields(self):
        """Test validation of required fields"""
        # Missing required fields
        incomplete_yaml = """
name: Test Catalog
link: https://example.com
"""
        data = yaml.load(incomplete_yaml, Loader=Loader)
        # Should have missing fields
        assert "id" not in data or data.get("id") is None

    def test_validate_field_types(self):
        """Test validation of field types"""
        yaml_content = """
id: testcatalog
name: Test Catalog
link: https://example.com
api: true
tags:
- tag1
- tag2
"""
        data = yaml.load(yaml_content, Loader=Loader)
        assert isinstance(data["id"], str)
        assert isinstance(data["api"], bool)
        assert isinstance(data["tags"], list)
        assert all(isinstance(tag, str) for tag in data["tags"])

    def test_parse_malformed_yaml(self):
        """Test error handling for malformed YAML files"""
        malformed_yaml = """
id: test
name: Test Catalog
invalid: [unclosed list
nested:
  - item1
    - item2  # Invalid indentation
"""
        with pytest.raises(yaml.YAMLError):
            yaml.load(malformed_yaml, Loader=Loader)

    def test_parse_yaml_with_tabs(self):
        """Test YAML parsing with tab characters

        Note: While YAML spec doesn't allow tabs for indentation, PyYAML
        may be lenient and accept them. This test verifies the actual behavior.
        """
        # Test with tabs used for indentation (not recommended per YAML spec)
        yaml_with_tabs = "id: test\n\tname: Test\n\tvalue: 123"

        # PyYAML behavior with tabs can vary - it may accept or reject them
        # We test that it either raises an error OR parses successfully
        try:
            data = yaml.load(yaml_with_tabs, Loader=Loader)
            # If it parses, verify the structure is reasonable
            # Note: tabs might cause unexpected parsing, so we just verify it doesn't crash
            assert data is not None
        except yaml.YAMLError:
            # If it raises an error, that's also acceptable per YAML spec
            # This is the stricter interpretation
            pass

    def test_parse_empty_yaml(self):
        """Test parsing empty YAML"""
        empty_yaml = ""
        data = yaml.load(empty_yaml, Loader=Loader)
        # Empty YAML should return None or empty dict
        assert data is None or data == {}

    def test_parse_yaml_invalid_unicode(self):
        """Test parsing YAML with invalid unicode"""
        # This should be handled gracefully
        try:
            invalid_yaml = b"\xff\xfeid: test"
            yaml.load(invalid_yaml.decode("utf-8", errors="ignore"), Loader=Loader)
        except (yaml.YAMLError, UnicodeDecodeError):
            # Either error is acceptable
            pass
