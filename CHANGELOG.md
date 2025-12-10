# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.3.0] - 2025-12-10

### Added
- Zstandard-compressed exports for `catalogs.jsonl`, `software.jsonl`, `scheduled.jsonl`, and `full.jsonl` plus a `datasets.duckdb` snapshot for analytics-friendly queries
- New scientific and API catalogs across Switzerland, EU, France, Germany, Great Britain, and Italy (e.g., Agroportal, TechnoPortal HEVS, EarthPortal, W3C Linked Open Vocabularies, BiodivPortal, MATPortal, OLS4)
- New API registry entry for `api.gov.it` and additional international research repositories

### Changed
- Refreshed and expanded metadata for hundreds of catalog records across Americas, Europe, Asia, and Oceania
- Rebuilt JSONL/Parquet exports and type/software slices (12,281 catalogs; 134 software platforms; 749 scheduled sources; 13,030 combined records)
- Simplified CI test invocation to run from the repository root in `tests.yml`

## [1.2.0] - 2025-11-21

### Added
- **1,993 new data catalog records** across multiple countries and regions
- **1,515 ArcGIS Server instances** - massive expansion of geoportal coverage
- **293 World-level catalogs** - international and global data repositories
- **97 French data catalogs** - significant expansion of French open data coverage
- **Geospatial infrastructure expansion**:
  - 83 GeoServer instances
  - 37 GeoNode installations
  - 33 GeoNetwork catalogs
  - 8 Lizmap instances
  - 3 MapProxy instances
  - 2 MapBender instances
- **Open data platforms**:
  - 47 OpenDataSoft instances
  - 42 CKAN portals
  - 5 DKAN installations
- **Scientific data repositories**:
  - 38 Figshare-based repositories
  - 6 DSpace installations
  - 6 NADA microdata catalogs
- **Additional platforms**: 9 THREDDS servers, 5 Drupal-based catalogs, 3 DataFair instances

### Changed
- **363 records updated** with improved metadata
- Updated API endpoints for IPT-based data catalogs
- Enhanced metadata completeness across multiple records
- Improved catalog endpoints and identifiers
- Better geographic and administrative region coverage

### Fixed
- Multiple data errors and inconsistencies
- Metadata gaps in existing records
- Various catalog identifier issues
- Endpoint validation and corrections

### Statistics

#### Record Changes
- **New records**: 1,993
- **Modified records**: 363
- **Deleted records**: 0

#### Software Types (Top 15)
- ArcGIS Server: 1,515
- Custom/Unknown: 89
- GeoServer: 83
- OpenDataSoft: 47
- CKAN: 42
- Figshare: 38
- GeoNode: 37
- GeoNetwork: 33
- ArcGIS Hub: 26
- THREDDS: 9
- Lizmap: 8
- DSpace: 6
- NADA: 6
- Drupal: 5
- DKAN: 5

#### Catalog Types
- Geoportal: 1,726 (86.6%)
- Open data portal: 181 (9.1%)
- Scientific data repository: 68 (3.4%)
- Microdata catalog: 7
- Indicators catalog: 6
- Datasets list: 3
- Metadata catalog: 2

#### Geographic Coverage

**Countries (Top 20)**:
- United States: 1,472
- World-level: 293
- France: 97
- Netherlands: 11
- Unknown/Unspecified: 11
- Germany: 8
- Italy: 8
- South Africa: 8
- Uganda: 7
- United Kingdom: 6
- Belarus: 5
- Colombia: 5
- Hong Kong: 4
- Croatia: 4
- Iceland: 4
- Japan: 4
- Brazil: 3
- Spain: 3
- European Union: 3
- Thailand: 3

**United States - State Breakdown (Top 20)**:
- Minnesota: 54
- California: 51
- Wisconsin: 43
- Ohio: 42
- Texas: 39
- Florida: 34
- Oregon: 34
- Illinois: 26
- Washington: 26
- District of Columbia: 25
- North Carolina: 24
- Virginia: 23
- Pennsylvania: 20
- Utah: 19
- Colorado: 17
- Indiana: 17
- Michigan: 16
- Georgia: 15
- Missouri: 15
- North Dakota: 12

**Regional Coverage**:
- Federal-level records: 1,138
- US state-level records: 500+
- French regions (Île-de-France): 25
- Additional subregional coverage across multiple countries

## [1.1.0] - 2025-11-15

### Added
- Comprehensive data quality analysis tool (`devdocs/analyze_duplicates_and_errors.py`)
  - Detects duplicate UID's and ID's across all records
  - Identifies missing required fields
  - Finds filename mismatches (where `id` field doesn't match filename)
  - Reports empty files and YAML parsing errors
  - Generates detailed reports in JSON, Markdown, and text formats

### Changed
- Updated README.md with data quality and validation section
- Added documentation for analysis tools in `devdocs/` directory

### Fixed
- Identified 7 duplicate ID's (same ID in both entities and software directories)
- Identified 204 records missing required `uid` field
- Identified 63 files with filename mismatches
- Identified 1 empty file requiring attention

## [2024-04-13]

### Added
- Several scientific and geo data catalogs
- Changelog (History.md)

### Fixed
- Malawi geoportal uid
- API endpoint errors
- Schema mistakes and updated validation
- Various catalog identifiers and metadata

### Changed
- Major updates to Finnish data portals
- Updated many scientific data catalogs
- Updated API endpoints for multiple platforms

---

*Note: For detailed historical changes prior to 2024-04-13, see [History.md](History.md)*

