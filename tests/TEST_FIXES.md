# Test Errors Found and Fixes Applied

## Summary

This document outlines the errors found in the test suite and the fixes that were applied.

## Issues Found

### 1. **Improper Mocking of Module-Level Variables**

**Problem**: Tests were manually modifying `builder.DATASETS_DIR` using try/finally blocks, which is error-prone and doesn't work well with pytest's fixture system.

**Location**: `tests/test_builder.py` - All `TestBuildDataset` and `TestMergeDatasets` methods

**Fix**: Replaced manual variable modification with `monkeypatch.setattr()` which is the proper pytest way to mock module-level variables.

**Before**:
```python
import builder
original_datasets_dir = builder.DATASETS_DIR
builder.DATASETS_DIR = datasets_dir
try:
    # test code
finally:
    builder.DATASETS_DIR = original_datasets_dir
```

**After**:
```python
import builder
monkeypatch.setattr(builder, 'DATASETS_DIR', datasets_dir)
# test code (no try/finally needed)
```

### 2. **Missing Error Handling Tests for Malformed YAML**

**Problem**: The test suite lacked tests for error handling when processing malformed YAML files, which is a critical edge case.

**Location**: `tests/test_yaml.py`

**Fix**: Added comprehensive error handling tests:
- `test_parse_malformed_yaml()` - Tests unclosed lists and invalid indentation
- `test_parse_yaml_with_tabs()` - Tests YAML with tab characters (invalid per spec)
- `test_parse_empty_yaml()` - Tests empty YAML input
- `test_parse_yaml_invalid_unicode()` - Tests invalid unicode handling

### 3. **Missing Error Handling Tests for Invalid JSON**

**Problem**: No tests for handling invalid JSON in JSONL files.

**Location**: `tests/test_builder.py`

**Fix**: Added `test_load_jsonl_invalid_json()` to test JSON decode error handling.

### 4. **Missing Error Handling Tests for Malformed YAML in build_dataset**

**Problem**: No test for how `build_dataset()` handles malformed YAML files.

**Location**: `tests/test_builder.py`

**Fix**: Added `test_build_dataset_malformed_yaml()` to verify that `build_dataset()` properly raises `YAMLError` when encountering invalid YAML.

## Test Coverage Improvements

The fixes add the following test coverage:

1. **Error Handling**:
   - Malformed YAML parsing
   - Invalid JSON in JSONL files
   - Empty YAML files
   - Invalid unicode in YAML
   - Tab characters in YAML (invalid per spec)

2. **Proper Test Isolation**:
   - Using `monkeypatch` ensures tests don't interfere with each other
   - No need for manual cleanup with try/finally blocks

## Running Tests

After applying these fixes, tests should run correctly. To run:

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_builder.py -v
pytest tests/test_yaml.py -v

# Run with coverage
pytest tests/ --cov=scripts --cov-report=html
```

## Expected Test Results

All tests should now:
- ✅ Pass with proper mocking using monkeypatch
- ✅ Test error handling for malformed inputs
- ✅ Test edge cases (empty files, invalid data)
- ✅ Be properly isolated from each other

## Notes

- The `monkeypatch` fixture is automatically provided by pytest - no need to import it
- Error handling tests use `pytest.raises()` to verify exceptions are raised correctly
- All test files have been validated for correct Python syntax

