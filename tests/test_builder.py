"""Tests for builder.py functions"""

import os
import json
import tempfile
import pytest
import yaml
import types
from pathlib import Path

# Import builder functions
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from builder import load_jsonl, build_dataset, merge_datasets


class TestLoadJsonl:
    """Tests for load_jsonl function"""

    def test_load_jsonl_basic(self, temp_jsonl_file):
        """Test loading a basic JSONL file"""
        data = load_jsonl(temp_jsonl_file)
        assert len(data) == 3
        assert data[0]["id"] == "test1"
        assert data[1]["name"] == "Test Catalog 2"
        assert data[2]["link"] == "https://example.com/3"

    def test_load_jsonl_empty_file(self, temp_dir):
        """Test loading an empty JSONL file"""
        filepath = os.path.join(temp_dir, "empty.jsonl")
        with open(filepath, "w", encoding="utf8") as f:
            pass
        data = load_jsonl(filepath)
        assert data == []

    def test_load_jsonl_single_line(self, temp_dir):
        """Test loading a JSONL file with a single line"""
        filepath = os.path.join(temp_dir, "single.jsonl")
        with open(filepath, "w", encoding="utf8") as f:
            f.write('{"id": "single", "name": "Single Item"}\n')
        data = load_jsonl(filepath)
        assert len(data) == 1
        assert data[0]["id"] == "single"


class TestBuildDataset:
    """Tests for build_dataset function"""

    def test_build_dataset_from_yaml(self, temp_dir, sample_yaml_content, monkeypatch):
        """Test building a dataset from YAML files"""
        # Create a test directory with YAML files
        yaml_dir = os.path.join(temp_dir, "yaml_data")
        os.makedirs(yaml_dir, exist_ok=True)

        # Create test YAML files
        for i in range(3):
            filepath = os.path.join(yaml_dir, f"test{i}.yaml")
            with open(filepath, "w", encoding="utf8") as f:
                content = sample_yaml_content.replace("testcatalog", f"testcatalog{i}")
                f.write(content)

        # Mock DATASETS_DIR to use temp_dir
        datasets_dir = os.path.join(temp_dir, "datasets")
        os.makedirs(datasets_dir, exist_ok=True)

        import builder

        # Use monkeypatch to properly mock the module-level variable
        monkeypatch.setattr(builder, "DATASETS_DIR", datasets_dir)

        # Build dataset
        output_filename = "output.jsonl"
        build_dataset(yaml_dir, output_filename)

        # Verify output
        output_file = os.path.join(datasets_dir, output_filename)
        assert os.path.exists(output_file)
        data = load_jsonl(output_file)
        assert len(data) == 3
        assert all("id" in item for item in data)
        assert all("name" in item for item in data)

    def test_build_dataset_empty_directory(self, temp_dir, monkeypatch):
        """Test building dataset from empty directory"""
        yaml_dir = os.path.join(temp_dir, "empty_yaml")
        os.makedirs(yaml_dir, exist_ok=True)

        datasets_dir = os.path.join(temp_dir, "datasets")
        os.makedirs(datasets_dir, exist_ok=True)

        import builder

        monkeypatch.setattr(builder, "DATASETS_DIR", datasets_dir)

        output_filename = "empty_output.jsonl"
        build_dataset(yaml_dir, output_filename)

        output_file = os.path.join(datasets_dir, output_filename)
        # File should be created but empty
        assert os.path.exists(output_file)
        data = load_jsonl(output_file)
        assert len(data) == 0

    def test_build_dataset_nested_directories(
        self, temp_dir, sample_yaml_content, monkeypatch
    ):
        """Test building dataset from nested directory structure"""
        # Create nested directory structure
        base_dir = os.path.join(temp_dir, "nested")
        subdir1 = os.path.join(base_dir, "subdir1")
        subdir2 = os.path.join(base_dir, "subdir2")
        os.makedirs(subdir1, exist_ok=True)
        os.makedirs(subdir2, exist_ok=True)

        # Create YAML files in subdirectories
        for i, subdir in enumerate([subdir1, subdir2]):
            filepath = os.path.join(subdir, f"test{i}.yaml")
            with open(filepath, "w", encoding="utf8") as f:
                content = sample_yaml_content.replace("testcatalog", f"testcatalog{i}")
                f.write(content)

        datasets_dir = os.path.join(temp_dir, "datasets")
        os.makedirs(datasets_dir, exist_ok=True)

        import builder

        monkeypatch.setattr(builder, "DATASETS_DIR", datasets_dir)

        output_filename = "nested_output.jsonl"
        build_dataset(base_dir, output_filename)

        output_file = os.path.join(datasets_dir, output_filename)
        data = load_jsonl(output_file)
        assert len(data) == 2


class TestMergeDatasets:
    """Tests for merge_datasets function"""

    def test_merge_datasets(self, temp_dir, monkeypatch):
        """Test merging multiple JSONL files"""
        datasets_dir = os.path.join(temp_dir, "datasets")
        os.makedirs(datasets_dir, exist_ok=True)

        # Create test JSONL files
        file1 = os.path.join(datasets_dir, "file1.jsonl")
        file2 = os.path.join(datasets_dir, "file2.jsonl")

        with open(file1, "w", encoding="utf8") as f:
            f.write('{"id": "1", "name": "One"}\n')
            f.write('{"id": "2", "name": "Two"}\n')

        with open(file2, "w", encoding="utf8") as f:
            f.write('{"id": "3", "name": "Three"}\n')

        import builder

        monkeypatch.setattr(builder, "DATASETS_DIR", datasets_dir)

        output_filename = "merged.jsonl"
        merge_datasets(["file1.jsonl", "file2.jsonl"], output_filename)

        output_file = os.path.join(datasets_dir, output_filename)
        data = load_jsonl(output_file)
        assert len(data) == 3
        assert data[0]["id"] == "1"
        assert data[1]["id"] == "2"
        assert data[2]["id"] == "3"

    def test_merge_datasets_empty_files(self, temp_dir, monkeypatch):
        """Test merging empty JSONL files"""
        datasets_dir = os.path.join(temp_dir, "datasets")
        os.makedirs(datasets_dir, exist_ok=True)

        file1 = os.path.join(datasets_dir, "empty1.jsonl")
        file2 = os.path.join(datasets_dir, "empty2.jsonl")

        with open(file1, "w", encoding="utf8") as f:
            pass
        with open(file2, "w", encoding="utf8") as f:
            pass

        import builder

        monkeypatch.setattr(builder, "DATASETS_DIR", datasets_dir)

        output_filename = "merged_empty.jsonl"
        merge_datasets(["empty1.jsonl", "empty2.jsonl"], output_filename)

        output_file = os.path.join(datasets_dir, output_filename)
        data = load_jsonl(output_file)
        assert len(data) == 0

    def test_load_jsonl_invalid_json(self, temp_dir):
        """Test loading JSONL file with invalid JSON line"""
        filepath = os.path.join(temp_dir, "invalid.jsonl")
        with open(filepath, "w", encoding="utf8") as f:
            f.write('{"id": "1", "name": "Valid"}\n')
            f.write('{"id": "2", invalid json}\n')  # Invalid JSON
            f.write('{"id": "3", "name": "Valid"}\n')

        # Should raise JSON decode error
        with pytest.raises(json.JSONDecodeError):
            load_jsonl(filepath)

    def test_build_dataset_malformed_yaml(self, temp_dir, monkeypatch):
        """Test building dataset with malformed YAML file"""
        yaml_dir = os.path.join(temp_dir, "yaml_data")
        os.makedirs(yaml_dir, exist_ok=True)

        # Create a valid YAML file
        valid_file = os.path.join(yaml_dir, "valid.yaml")
        with open(valid_file, "w", encoding="utf8") as f:
            f.write("id: test\nname: Test\n")

        # Create a malformed YAML file
        invalid_file = os.path.join(yaml_dir, "invalid.yaml")
        with open(invalid_file, "w", encoding="utf8") as f:
            f.write("id: test\ninvalid: [unclosed\n")

        datasets_dir = os.path.join(temp_dir, "datasets")
        os.makedirs(datasets_dir, exist_ok=True)

        import builder

        monkeypatch.setattr(builder, "DATASETS_DIR", datasets_dir)

        # Should raise YAMLError when processing invalid file
        with pytest.raises(yaml.YAMLError):
            build_dataset(yaml_dir, "output.jsonl")


class TestBuilderApidetectIntegration:
    """Integration tests for builder -> apidetect invocation path."""

    def test_add_single_entry_calls_detect_single_scheduled(self, temp_dir, monkeypatch):
        import builder

        datasets_dir = os.path.join(temp_dir, "datasets")
        scheduled_dir = os.path.join(temp_dir, "scheduled")
        entries_dir = os.path.join(temp_dir, "entities")
        os.makedirs(datasets_dir, exist_ok=True)
        os.makedirs(scheduled_dir, exist_ok=True)
        os.makedirs(entries_dir, exist_ok=True)

        with open(os.path.join(datasets_dir, "software.jsonl"), "w", encoding="utf8") as f:
            f.write('{"id": "ckan", "name": "CKAN"}\n')

        monkeypatch.setattr(builder, "DATASETS_DIR", datasets_dir)
        monkeypatch.setattr(builder, "SCHEDULED_DIR", scheduled_dir)
        monkeypatch.setattr(builder, "ROOT_DIR", entries_dir)

        calls = []

        def _fake_detect_single(uniqid, dryrun=False, mode="entries", **kwargs):
            calls.append((uniqid, dryrun, mode))

        monkeypatch.setitem(
            sys.modules, "apidetect", types.SimpleNamespace(detect_single=_fake_detect_single)
        )

        builder._add_single_entry(
            url="https://catalog.example.org",
            software="ckan",
            country="US",
            scheduled=True,
            preloaded=[],
        )

        assert calls == [("catalogexampleorg", False, "scheduled")]

    def test_add_single_entry_calls_detect_single_entries(self, temp_dir, monkeypatch):
        import builder

        datasets_dir = os.path.join(temp_dir, "datasets")
        scheduled_dir = os.path.join(temp_dir, "scheduled")
        entries_dir = os.path.join(temp_dir, "entities")
        os.makedirs(datasets_dir, exist_ok=True)
        os.makedirs(scheduled_dir, exist_ok=True)
        os.makedirs(entries_dir, exist_ok=True)

        with open(os.path.join(datasets_dir, "software.jsonl"), "w", encoding="utf8") as f:
            f.write('{"id": "ckan", "name": "CKAN"}\n')

        monkeypatch.setattr(builder, "DATASETS_DIR", datasets_dir)
        monkeypatch.setattr(builder, "SCHEDULED_DIR", scheduled_dir)
        monkeypatch.setattr(builder, "ROOT_DIR", entries_dir)

        calls = []

        def _fake_detect_single(uniqid, dryrun=False, mode="entries", **kwargs):
            calls.append((uniqid, dryrun, mode))

        monkeypatch.setitem(
            sys.modules, "apidetect", types.SimpleNamespace(detect_single=_fake_detect_single)
        )

        builder._add_single_entry(
            url="https://catalog.example.org",
            software="ckan",
            country="US",
            scheduled=False,
            preloaded=[],
        )

        assert calls == [("catalogexampleorg", False, "entries")]
