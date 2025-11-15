"""Pytest configuration and shared fixtures"""

import os
import json
import tempfile
import pytest
from pathlib import Path


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def sample_yaml_content():
    """Sample YAML content for testing"""
    return """access_mode:
- open
api: true
api_status: active
catalog_type: Open data portal
content_types:
- dataset
coverage:
- location:
    country:
      id: US
      name: United States
    level: 1
id: testcatalog
langs:
- id: EN
  name: English
link: https://example.com
name: Test Catalog
owner:
  link: null
  location:
    country:
      id: US
      name: United States
    level: 1
  name: Test Organization
  type: Central government
software:
  id: ckan
  name: CKAN
status: active
tags:
- government
- has_api
uid: cdi00000001
"""


@pytest.fixture
def sample_jsonl_content():
    """Sample JSONL content for testing"""
    return """{"id": "test1", "name": "Test Catalog 1", "link": "https://example.com/1"}
{"id": "test2", "name": "Test Catalog 2", "link": "https://example.com/2"}
{"id": "test3", "name": "Test Catalog 3", "link": "https://example.com/3"}
"""


@pytest.fixture
def sample_catalog_dict():
    """Sample catalog dictionary for testing"""
    return {
        "id": "testcatalog",
        "uid": "cdi00000001",
        "name": "Test Catalog",
        "link": "https://example.com",
        "catalog_type": "Open data portal",
        "api": True,
        "api_status": "active",
        "access_mode": ["open"],
        "langs": [{"id": "EN", "name": "English"}],
        "tags": ["government", "has_api"],
        "content_types": ["dataset"],
        "coverage": [
            {"location": {"country": {"id": "US", "name": "United States"}, "level": 1}}
        ],
        "endpoints": [],
        "identifiers": [],
        "owner": {
            "name": "Test Organization",
            "link": None,
            "type": "Central government",
            "location": {"country": {"id": "US", "name": "United States"}, "level": 1},
        },
        "software": {"id": "ckan", "name": "CKAN"},
        "status": "active",
        "topics": [],
    }


@pytest.fixture
def temp_jsonl_file(temp_dir, sample_jsonl_content):
    """Create a temporary JSONL file"""
    filepath = os.path.join(temp_dir, "test.jsonl")
    with open(filepath, "w", encoding="utf8") as f:
        f.write(sample_jsonl_content)
    return filepath


@pytest.fixture
def temp_yaml_file(temp_dir, sample_yaml_content):
    """Create a temporary YAML file"""
    filepath = os.path.join(temp_dir, "test.yaml")
    with open(filepath, "w", encoding="utf8") as f:
        f.write(sample_yaml_content)
    return filepath
