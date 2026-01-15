# Project Context

## Purpose
The dataportals-registry is a comprehensive registry of data portals, catalogs, data repositories, and related data infrastructure. It serves as the first pillar of the open search engine project, which aims to create a unified discovery system for open data across the globe.

The project collects and maintains structured metadata about:
- Open data portals
- Geoportals
- Scientific data repositories
- Indicators catalogs
- Microdata catalogs
- Machine learning catalogs
- Data search engines
- API Catalogs
- Data marketplaces
- Other data infrastructure

The registry is organized as a collection of YAML files that will eventually be reorganized as a database with a publicly available open API and bulk data dumps.

## Tech Stack
- **Python 3.9-3.12**: Primary programming language
- **YAML**: Data storage format for catalog entries
- **JSON/JSONL**: Export formats for datasets
- **DuckDB**: Analytics-friendly database format
- **zstandard (zstd)**: Compression for JSONL exports
- **pytest**: Testing framework with coverage reporting
- **typer**: CLI framework for Python scripts
- **PyYAML**: YAML parsing and generation
- **Cerberus**: Schema validation
- **pydantic**: Data validation and settings management
- **pandas**: Data manipulation and analysis
- **requests**: HTTP client for API interactions
- **rich**: Terminal formatting and progress bars
- **beautifulsoup4**: HTML parsing for web scraping

## Project Conventions

### Code Style
- **Python**: Follow PEP 8 style guidelines
  - Use meaningful variable and function names
  - Add docstrings to functions and classes
  - Keep functions focused and small
  - Use type hints where appropriate
- **YAML Files**:
  - Use 2 spaces for indentation (no tabs)
  - Use consistent formatting
  - Keep lines under 100 characters when possible
  - Use quotes for strings with special characters
  - Use lists for multiple values
  - Filename must match the `id` field (lowercase, no special characters)
- **Git**:
  - Write clear, descriptive commit messages
  - Start commit messages with a verb (Add, Fix, Update, Remove)
  - Make atomic commits (one logical change per commit)
  - Keep PRs focused on a single topic

### Architecture Patterns
- **Data Organization**:
  - Catalog entries stored as individual YAML files in `data/entities/`
  - Files organized by country/territory: `data/entities/COUNTRY_CODE/`
  - Within each country, organized by catalog type subdirectories:
    - `opendata/` - Open data portals
    - `geo/` - Geoportals
    - `scientific/` - Scientific data repositories
    - `microdata/` - Microdata catalogs
    - `indicators/` - Indicators catalogs
    - `ml/` - Machine learning catalogs
    - `search/` - Data search engines
    - `api/` - API Catalogs
    - `marketplace/` - Data marketplaces
    - `other/` - Other types
- **Scripts**:
  - Main build script: `scripts/builder.py`
  - Validation scripts: Schema validation against `data/schemes/catalog.json`
  - Enrichment scripts: Re3data integration, software detection, etc.
- **Data Exports**:
  - JSONL format for line-delimited JSON
  - Compressed with zstd (`.zst` files)
  - DuckDB format for analytics
  - Parquet format for data analysis
  - Sliced exports by type and software in `data/datasets/bytype/` and `data/datasets/bysoftware/`

### Testing Strategy
- **Framework**: pytest with pytest-cov for coverage
- **Test Location**: All tests in `tests/` directory
- **Naming**: Test files follow `test_*.py` convention
- **Coverage**: Aim for comprehensive test coverage, especially for core builder functions
- **CI/CD**: Automated testing on push/PR via GitHub Actions
  - Tests run on Python 3.9, 3.10, 3.11, 3.12
  - Coverage reports uploaded to Codecov
- **Test Types**:
  - Unit tests for individual functions
  - Integration tests for data building pipeline
  - Validation tests for YAML schema compliance
  - Data quality tests for duplicates and errors

### Git Workflow
- **Branching**: Standard feature branch workflow
  - Create feature branches from main/master/develop
  - Branch names should be descriptive (e.g., `add-catalog-example`)
- **Pull Requests**:
  - PRs should include clear descriptions
  - Must pass validation and tests before merge
  - Use PR template for consistency
- **Commit Messages**:
  - Clear, descriptive messages
  - Start with verb (Add, Fix, Update, Remove)
  - Reference issue numbers when applicable: `"Add example catalog (fixes #123)"`
- **Protected Branches**: Main/master branches are protected, require PR reviews

## Domain Context
- **Data Catalogs**: Structured collections of dataset metadata
- **Catalog Types**: Different categories of data infrastructure (open data portals, scientific repositories, geoportals, etc.)
- **Software Platforms**: Common platforms include CKAN, DKAN, Dataverse, DSpace, ArcGIS Hub, GeoNetwork, PxWeb, etc.
- **Metadata Standards**: Support for various standards:
  - CKAN API
  - DCAT (Data Catalog Vocabulary)
  - OAI-PMH (Open Archives Initiative Protocol for Metadata Harvesting)
  - ISO 19115 (Geographic information metadata)
  - EU Data Theme taxonomy
- **Identifiers**: External identifiers from Wikidata, re3data, FAIRsharing, etc.
- **Geographic Coverage**: ISO country codes, UN M49 macroregions, subregions
- **Languages**: ISO language codes (EN, ES, FR, etc.)
- **Access Modes**: open, restricted, etc.
- **Status**: active, inactive, scheduled
- **Unique Identifiers**: 
  - `id`: Short identifier matching filename
  - `uid`: Unique identifier in format `cdi0000####` or `temp########`

## Important Constraints
- **YAML Schema Validation**: All entries must validate against `data/schemes/catalog.json`
- **File Naming**: Filename must exactly match the `id` field (lowercase, no special characters)
- **Required Fields**: Every catalog entry must include:
  - `id`, `uid`, `name`, `link`, `catalog_type`, `access_mode`, `status`, `software`, `owner`, `coverage`
- **Country Organization**: Files must be placed in correct country/type directory structure
- **No Duplicates**: Each catalog should have unique `id` and `uid`
- **Data Quality**: Regular validation and quality checks required
- **Python Version**: Must support Python 3.9-3.12
- **License**: Code under MIT, data under CC-BY 4.0

## External Dependencies
- **Re3Data API**: For enriching scientific data repositories with metadata
  - Used by `scripts/re3data_enrichment.py`
  - Adds `_re3data` field to entries with re3data identifiers
- **Various Data Sources**: The project aggregates data from multiple sources including:
  - STAC Catalogs (stacindex.org)
  - Dataverse Installations
  - Open Data Inception
  - CKAN Portals (datashades.info)
  - GeoNetwork Showcase
  - PxWeb examples
  - DKAN Community
  - Junar Clients
  - OpenSDG installations
  - MyCore Installations
  - Elsevier Pure installations
  - CoreTrustSeal Repositories
  - GeoOrchestra installations
  - EUDAT Repositories
  - Data.Europe.eu catalogues
  - And many others (see README.md for complete list)
- **Web APIs**: Scripts interact with various catalog APIs (CKAN, DCAT, OAI-PMH, etc.) for metadata extraction
- **Codecov**: For test coverage reporting in CI/CD
