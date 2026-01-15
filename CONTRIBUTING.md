# Contributing to dataportals-registry

Thank you for your interest in contributing to the dataportals-registry project! This document provides guidelines and instructions for contributing to the registry of data portals, catalogs, and repositories.

## Table of Contents

- [Getting Started](#getting-started)
- [Ways to Contribute](#ways-to-contribute)
- [Adding New Catalog Entries](#adding-new-catalog-entries)
- [YAML File Structure](#yaml-file-structure)
- [Validation](#validation)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Data Quality](#data-quality)
- [Code Style](#code-style)

## Getting Started

### Fork and Clone

1. Fork the repository on GitHub
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/dataportals-registry.git
   cd dataportals-registry
   ```
3. Add the upstream repository:
   ```bash
   git remote add upstream https://github.com/commondataio/dataportals-registry.git
   ```

### Development Environment Setup

1. Ensure you have Python 3.7+ installed
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Project Structure

- `data/entities/` - Verified data catalog records as individual YAML files
- `data/scheduled/` - Unverified (scheduled) data catalog records
- `data/schemes/` - JSON schema definitions for validation
- `data/software/` - Software/platform definitions
- `data/reference/` - Reference data and vocabularies
- `scripts/` - Python scripts for building, validating, and managing the registry
- `tests/` - Test files
- `devdocs/` - Development documentation and analysis tools

## Ways to Contribute

### Reporting Issues

If you find a bug, error, or have a suggestion:

1. Check if the issue already exists in the [Issues](https://github.com/commondataio/dataportals-registry/issues) page
2. If not, create a new issue with:
   - Clear description of the problem or suggestion
   - Steps to reproduce (if applicable)
   - Expected vs actual behavior
   - Any relevant file paths or examples

### Adding New Catalog Entries

The most common contribution is adding new data catalog entries. See the [Adding New Catalog Entries](#adding-new-catalog-entries) section below for detailed instructions.

### Improving Existing Entries

You can improve existing entries by:
- Adding missing information (endpoints, identifiers, descriptions)
- Correcting errors or outdated information
- Adding additional languages
- Improving metadata quality

### Documentation Improvements

Help improve documentation by:
- Fixing typos or unclear explanations
- Adding examples
- Improving structure and organization
- Translating documentation

### Code Contributions

Contributions to the codebase are welcome:
- Bug fixes
- New features
- Performance improvements
- Test coverage

## Adding New Catalog Entries

### Method 1: Using the Script (Recommended for Quick Additions)

The easiest way to add a new catalog entry is using the `add_single` command:

```bash
python scripts/builder.py add-single \
  --url "https://example.com/data" \
  --software "ckan" \
  --catalog-type "Open data portal" \
  --name "Example Data Portal" \
  --country "US" \
  --scheduled
```

**Parameters:**
- `--url` (required): The URL of the data catalog
- `--software`: Software/platform ID (e.g., "ckan", "dkan", "arcgishub")
- `--catalog-type`: Type of catalog (see catalog types below)
- `--name`: Display name of the catalog
- `--description`: Description of the catalog
- `--country`: ISO country code (e.g., "US", "GB", "FR")
- `--lang`: Language code (e.g., "EN", "ES", "FR")
- `--owner-name`: Name of the organization/owner
- `--owner-link`: URL of the owner organization
- `--owner-type`: Type of owner (e.g., "Central government", "Local government")
- `--scheduled`: Add to scheduled directory (default) or use `--no-scheduled` for entities

This will create a YAML file in the appropriate location based on the catalog type and country.

### Method 2: Manual YAML Creation

For more control or when adding multiple entries, create YAML files manually:

1. **Determine the file location:**
   - Files are organized by country/territory in `data/entities/COUNTRY_CODE/`
   - Within each country, files are organized by catalog type in subdirectories:
     - `opendata/` - Open data portals (default)
     - `geo/` - Geoportals
     - `scientific/` - Scientific data repositories
     - `microdata/` - Microdata catalogs
     - `indicators/` - Indicators catalogs
     - `ml/` - Machine learning catalogs
     - `search/` - Data search engines
     - `api/` - API Catalogs
     - `marketplace/` - Data marketplaces
     - `metadata/` - Metadata catalogs
     - `other/` - Other types

2. **Create the filename:**
   - Filename should match the `id` field (lowercase, no special characters)
   - Example: `catalogdatagov.yaml` for `id: catalogdatagov`

3. **Create the YAML file** with all required fields (see [YAML File Structure](#yaml-file-structure))

### Catalog Types

The following catalog types are supported:

- **Open data portal** - Default for most open data portals
- **Geoportal** - Geographic/spatial data portals
- **Scientific data repository** - Research data repositories
- **Indicators catalog** - Statistical indicators catalogs
- **Microdata catalog** - Microdata/survey data catalogs
- **Machine learning catalog** - ML datasets and models
- **Data search engines** - Search engines for datasets
- **API Catalog** - API directories and catalogs
- **Data marketplaces** - Commercial data marketplaces
- **Metadata catalog** - Metadata registries
- **Other** - Other types not listed above

### File Naming Convention

- Use lowercase letters and numbers only
- Remove special characters (dots, dashes, underscores) from the domain name
- Example: `https://catalog.data.gov` → `catalogdatagov.yaml`
- The `id` field in the YAML must match the filename (without `.yaml`)

## YAML File Structure

### Required Fields

Every catalog entry must include these required fields:

- `id` (string): Unique identifier matching the filename
- `uid` (string): Unique identifier (typically in format `cdi0000####`)
- `name` (string): Display name of the catalog
- `link` (string): URL of the catalog
- `catalog_type` (string): Type of catalog (see catalog types above)
- `access_mode` (list of strings): Access modes (e.g., `["open"]`, `["restricted"]`)
- `status` (string): Status of the catalog (e.g., `"active"`, `"inactive"`, `"scheduled"`)
- `software` (dict): Software/platform information
  - `id` (string): Software ID
  - `name` (string): Software name
- `owner` (dict): Owner/organization information
  - `name` (string): Owner name
  - `type` (string): Owner type (e.g., "Central government", "Local government", "NGO")
  - `location` (dict): Location information
    - `country` (dict): Country information
      - `id` (string): ISO country code
      - `name` (string): Country name
- `coverage` (list): Geographic coverage
  - Each item contains `location` with country information

### Optional Fields

- `description` (string): Description of the catalog
- `api` (boolean): Whether the catalog has an API
- `api_status` (string): API status (e.g., `"active"`, `"inactive"`, `"uncertain"`)
- `content_types` (list of strings): Types of content (e.g., `["dataset"]`, `["map_layer"]`)
- `endpoints` (list): API endpoints
  - `type` (string): Endpoint type (e.g., `"ckan"`, `"dcat:jsonld"`)
  - `url` (string): Endpoint URL
  - `version` (string): API version (optional)
- `identifiers` (list): External identifiers
  - `id` (string): Identifier type (e.g., `"wikidata"`, `"re3data"`)
  - `value` (string): Identifier value
  - `url` (string): URL to the identifier
- `_re3data` (dict, optional): Enriched metadata from re3data.org (automatically added by enrichment script)
  - Contains keywords, content types, contact email, description, persistent identifiers, software, versioning, institutions, repository type, and more
  - See [devdocs/re3data_enrichment.md](../devdocs/re3data_enrichment.md) for details
- `langs` (list): Supported languages
  - `id` (string): Language code (e.g., `"EN"`, `"ES"`)
  - `name` (string): Language name
- `tags` (list of strings): Tags/keywords
- `topics` (list): Topics/subjects
  - `type` (string): Topic type
  - `id` (string): Topic ID
  - `name` (string): Topic name
- `properties` (dict): Additional properties
  - `has_doi` (boolean): Whether datasets have DOIs
  - `is_national` (boolean): Whether it's a national catalog
  - `transferable_topics` (boolean): Whether topics can be transferred
  - `transferable_location` (boolean): Whether location can be transferred
  - `unfinished` (boolean): Whether the entry is incomplete
- `rights` (dict): Rights and licensing information
  - `license_id` (string): License identifier
  - `license_name` (string): License name
  - `license_url` (string): License URL
  - `rights_type` (string): Type of rights
  - `tos_url` (string): Terms of service URL
  - `privacy_policy_url` (string): Privacy policy URL
- `catalog_export` (string): Export standard (e.g., `"CKAN API"`)

### Example YAML File

```yaml
access_mode:
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
    level: 20
    macroregion:
      id: '021'
      name: Northern America
description: United States government's open data website providing access to datasets
  published by agencies across the federal government.
endpoints:
- type: ckan
  url: https://catalog.data.gov/api/3
  version: '3'
- type: ckan:package-search
  url: https://catalog.data.gov/api/3/action/package_search
  version: '3'
id: catalogdatagov
identifiers:
- id: wikidata
  url: https://www.wikidata.org/wiki/Q5227102
  value: Q5227102
langs:
- id: EN
  name: English
link: https://catalog.data.gov
name: The Home of the U.S. Government Open Data
owner:
  link: https://www.gsa.gov
  location:
    country:
      id: US
      name: United States
    level: 30
    subregion:
      id: US-DC
      name: District of Columbia
  name: GSA Technology Transformation Services
  type: Central government
properties:
  has_doi: false
  is_national: true
rights:
  rights_type: granular
software:
  id: ckan
  name: CKAN
status: active
tags:
- government
- has_api
- open data
- federal
- datasets
uid: cdi00001616
```

For a complete example, see: `data/entities/US/Federal/opendata/catalogdatagov.yaml`

### Schema Reference

The complete schema definition is available in `data/schemes/catalog.json`. This file defines all valid fields, their types, and requirements.

## Validation

### Running Validation

Before submitting your contribution, validate your YAML files:

```bash
python scripts/builder.py validate-yaml
```

This will:
- Check all YAML files in the `data/entities/` directory
- Validate against the schema in `data/schemes/catalog.json`
- Report any errors or missing required fields

### Common Validation Errors

1. **Missing required fields**: Ensure all required fields are present
2. **Empty required fields**: Required fields cannot be empty strings
3. **Invalid field types**: Check that fields match their expected types (string, list, dict, boolean)
4. **Filename mismatch**: The `id` field must match the filename (without `.yaml`)
5. **Invalid country codes**: Use valid ISO country codes
6. **Invalid language codes**: Use standard language codes (e.g., "EN", "ES", "FR")

### Fixing Validation Errors

1. Read the validation error message carefully
2. Check the schema file (`data/schemes/catalog.json`) for field requirements
3. Compare with example files in `data/entities/`
4. Fix the error and re-run validation

## Testing

### Running Tests

Run the test suite to ensure everything works:

```bash
pytest
```

Or run specific test files:

```bash
pytest tests/test_yaml.py
```

### Writing Tests

When contributing code changes, add tests:

1. Place test files in the `tests/` directory
2. Follow the naming convention: `test_*.py`
3. Use pytest for testing
4. Test both valid and invalid cases

## Pull Request Process

### Before Submitting

1. **Validate your changes:**
   ```bash
   python scripts/builder.py validate-yaml
   ```

2. **Run tests:**
   ```bash
   pytest
   ```

3. **Check for duplicates:**
   - Ensure the catalog doesn't already exist
   - Check both `data/entities/` and `data/scheduled/`

4. **Update documentation** if you've changed functionality

### Creating a Pull Request

1. **Create a branch:**
   ```bash
   git checkout -b add-catalog-example
   ```

2. **Make your changes:**
   - Add new YAML files
   - Or modify existing files
   - Follow the file structure guidelines

3. **Commit your changes:**
   ```bash
   git add data/entities/COUNTRY/type/example.yaml
   git commit -m "Add example data catalog"
   ```
   
   **Commit message guidelines:**
   - Use clear, descriptive messages
   - Start with a verb (Add, Fix, Update, Remove)
   - Reference issue numbers if applicable: `"Add example catalog (fixes #123)"`

4. **Push to your fork:**
   ```bash
   git push origin add-catalog-example
   ```

5. **Create a Pull Request:**
   - Go to the GitHub repository
   - Click "New Pull Request"
   - Select your branch
   - Fill in the PR description:
     - What catalog(s) you're adding/updating
     - Why (if not obvious)
     - Any relevant links or references

### PR Description Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] New catalog entry
- [ ] Update to existing entry
- [ ] Bug fix
- [ ] Documentation
- [ ] Code improvement

## Catalog(s) Added/Updated
- Catalog name: URL
- ...

## Validation
- [ ] Ran `python scripts/builder.py validate-yaml`
- [ ] All tests pass
- [ ] No duplicate entries

## Additional Notes
Any additional context or information
```

### Review Process

- Maintainers will review your PR
- They may request changes or ask questions
- Once approved, your PR will be merged
- Thank you for your contribution!

## Data Quality

### Running Data Quality Analysis

The project includes tools for analyzing data quality:

```bash
python devdocs/analyze_duplicates_and_errors.py
```

This generates reports on:
- Duplicate UID's and ID's
- Missing required fields
- Filename mismatches
- Empty files and parsing errors

Reports are saved in `devdocs/` and `dataquality/` directories.

### Best Practices for Data Entry

1. **Be accurate**: Verify all information before submitting
2. **Be complete**: Fill in as many fields as possible
3. **Use standard values**: 
   - Use ISO country codes
   - Use standard language codes
   - Use recognized software IDs
4. **Check for duplicates**: Ensure the catalog doesn't already exist
5. **Provide identifiers**: Add Wikidata, re3data, or other identifiers when available
6. **Re3data enrichment**: If a catalog has a re3data identifier, you can enrich it with metadata from re3data.org using:
   ```bash
   python scripts/re3data_enrichment.py enrich --id <re3data_id>
   ```
   See [devdocs/re3data_enrichment.md](../devdocs/re3data_enrichment.md) for more information.
7. **Include endpoints**: Add API endpoints if the catalog has an API
8. **Write clear descriptions**: Provide helpful descriptions for users

### Common Issues to Avoid

- **Duplicate entries**: Check existing entries before adding
- **Incorrect file location**: Place files in the correct country/type directory
- **Missing required fields**: Ensure all required fields are present
- **Invalid IDs**: Use valid software IDs and country codes
- **Outdated information**: Verify URLs and status are current

## Code Style

### Python Code

- Follow PEP 8 style guidelines
- Use meaningful variable names
- Add docstrings to functions and classes
- Keep functions focused and small

### YAML Files

- Use 2 spaces for indentation (no tabs)
- Use consistent formatting
- Keep lines under 100 characters when possible
- Use quotes for strings with special characters
- Use lists for multiple values

### Git

- Write clear, descriptive commit messages
- Make atomic commits (one logical change per commit)
- Keep PRs focused on a single topic

## Getting Help

If you need help or have questions:

1. Check existing [Issues](https://github.com/commondataio/dataportals-registry/issues)
2. Check the [README.md](README.md) for project overview
3. Check `devdocs/README.md` for development documentation
4. Open a new issue with your question

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (MIT for code, CC-BY 4.0 for data).

Thank you for contributing to dataportals-registry!

