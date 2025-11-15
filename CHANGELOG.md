# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

