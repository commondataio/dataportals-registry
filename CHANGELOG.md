# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.0] - 2025-11-21

### Added
- **2,629 new data catalog records** across multiple countries and regions
- **Thousands of ArcGIS Server instances** in the United States
  - Major additions in US states: Minnesota (54), California (52), Wisconsin (42), Ohio (42), Texas (39), Oregon (34), Florida (34), Washington (26), Illinois (26)
- **Hundreds of IPT (Integrated Publishing Toolkit) data catalogs**
  - Added 78 new IPT-based scientific data repositories
  - Updated API endpoints for existing IPT-based catalogs
- **Regional data portals in India** (36 new records)
- **Hundreds of French data catalogs** (108 new records)
- **Netherlands data catalogs** (27 new records)
- **Multiple data catalogs in Spain** (279 new records)
- **World-level catalogs** (294 new records)
- **Additional records** from various countries including:
  - United Kingdom (19), South Africa (14), Brazil (13), Norway (12), Germany (12)
  - Colombia (10), Argentina (10), Japan (9), Russia (8), New Zealand (8)
  - And many other countries

### Changed
- Updated API endpoints for most IPT-based data catalogs
- Improved metadata for hundreds of scheduled-to-add data catalogs
- Enhanced metadata completeness across multiple records
- Updated catalog endpoints and identifiers

### Fixed
- Multiple data errors and inconsistencies
- Metadata gaps in existing records
- Various catalog identifier issues
- Endpoint validation and corrections

### Statistics
- **Total new records**: 2,629
- **Total modified records**: 260
- **Top software types added**:
  - Custom/Unknown: 258
  - IPT (Integrated Publishing Toolkit): 78
  - CKAN: 38
  - OpenDataSoft: 24
  - ArcGIS Hub: 24
  - GeoNetwork: 10
  - DKAN: 9
- **Top catalog types added**:
  - Open data portal: 324
  - Scientific data repository: 94
  - Geoportal: 62
  - Indicators catalog: 5
- **Geographic coverage**: Records added across 20+ countries with focus on US (1,525), World-level (294), Spain (279), France (108), and India (36)

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

