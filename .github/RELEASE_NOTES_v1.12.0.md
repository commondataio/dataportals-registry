# dataportals-registry v1.12.0

**Release date:** August 18, 2026

## Summary

This release adds 1,304 net new catalogs, expanding geoportal, open data, scientific, indicators, and microdata coverage across 116 countries, and adds 12 software platform definitions.

## What's in this release

### Added
- **1,304 net new catalog entries** (1,318 YAML files added; 14 removed).
- **166 Polish eWMAPA** county and city geoportals, **91 Swedish EntryScape** open data catalogs, and **62 Lizmap** geoportals (mostly French).
- **151 CKAN** open data portals, including **80 Thai** government catalogs and **34 Indonesian** Satu Data sites.
- **73 GBIF IPT** scientific catalogs, **59 Chinese InstDB** repositories, **32 Japanese data eye** portals, and **25 Chinese Inspur oPortal** sites.
- **101 indicators catalogs**, including Open Data for Africa / Knoema and Datawheel observatories; **26 microdata catalogs**, including 19 REDATAM sites.
- First entity roots for **Aruba (AW), Cayman Islands (KY), and Montserrat (MS)**; Kosovo ASKdata recategorized from Serbia.
- Scientific and geospatial coverage: WIS 2.0 in a box, DataONE, NASA GES DISC OPeNDAP/Hyrax, NextGIS Web, WEKO3, and rasdaman.
- **12 software definitions**: Micka, Knoema, REDATAM, Copernicus Data Stores, data eye, Gipuzkoa Irekia, Liferay, OGD Platform India, Inspur oPortal, Piveau, SEU-e, and Ensembl.

### Changed
- Recategorized misplaced catalogs in Lithuania, US territories, China, and Kosovo.
- Reassigned software IDs on existing records after new platform definitions (Liferay, Knoema, OGD Platform India, SEU-e, REDATAM, and others).
- Regenerated dataset exports: **16,276** catalog records; 148 software definitions; 1 scheduled.

### Fixed
- Quality regression baseline, Python 3.9 CI install/`test_schema_parity` collection, and extra catalog types for CKAN, WordPress, OpenDataSoft, and Drupal.

## Data exports (2026-08-18)

| Export | Count |
|--------|--------|
| `catalogs.jsonl` (+ `.zst`) | 16,276 catalog records |
| `software.jsonl` (+ `.zst`) | 148 software/platform definitions |
| `scheduled.jsonl` (+ `.zst`) | 1 scheduled source |
| `full.jsonl` (+ `.zst`) | 16,277 combined entities + scheduled |
| `full.parquet`, `datasets.duckdb` | Analytics-friendly exports |

## Full changelog

See [CHANGELOG.md](../CHANGELOG.md) for full history.
