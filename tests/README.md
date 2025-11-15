# Tests for dataportals-registry

This directory contains the test suite for the dataportals-registry project.

## Running Tests

### Install Dependencies

First, install the required dependencies including pytest:

```bash
pip install -r requirements.txt
```

### Run All Tests

From the repository root:

```bash
pytest tests/
```

Or from the scripts directory:

```bash
cd scripts
python -m pytest ../tests
```

### Run Specific Test Files

```bash
pytest tests/test_builder.py
pytest tests/test_constants.py
pytest tests/test_datacatalog.py
pytest tests/test_yaml.py
pytest tests/test_utils.py
```

### Run with Coverage

```bash
pytest tests/ --cov=scripts --cov-report=html
```

This will generate an HTML coverage report in `htmlcov/index.html`.

### Run Specific Test Classes or Methods

```bash
pytest tests/test_builder.py::TestLoadJsonl
pytest tests/test_builder.py::TestLoadJsonl::test_load_jsonl_basic
```

## Test Structure

- `conftest.py` - Shared fixtures and pytest configuration
- `test_builder.py` - Tests for builder.py functions (load_jsonl, build_dataset, merge_datasets)
- `test_constants.py` - Tests for constants.py utilities and mappings
- `test_datacatalog.py` - Tests for pydantic DataCatalog model and shared models
- `test_yaml.py` - Tests for YAML parsing and validation
- `test_utils.py` - Tests for utility functions (URL parsing, domain mapping, etc.)

## Test Coverage

The test suite covers:

1. **Data Loading**: JSONL file loading and parsing
2. **Dataset Building**: Converting YAML files to JSONL format
3. **Data Merging**: Combining multiple JSONL files
4. **Constants**: Validation of constant mappings and configurations
5. **Data Models**: Pydantic model validation for DataCatalog and related models
6. **YAML Parsing**: YAML file parsing and structure validation
7. **Utilities**: URL parsing, domain mapping, language detection

## Continuous Integration

Tests are automatically run on GitHub Actions for:
- Python 3.9, 3.10, 3.11, and 3.12
- On push to main/master/develop branches
- On pull requests

See `.github/workflows/tests.yml` for the CI configuration.

