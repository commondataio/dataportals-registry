# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Docusaurus documentation site (`website/`) publishing `docs/` to GitHub Pages at `https://datenoio.github.io/dataportals-registry/`, with internals docs for humans and agents (query, contribute, OpenSpec).
- Relocated working notes `geoseer-analysis.md`, `metadata-quality.md`, and `trust_score_methodology.md` from `docs/` to `devdocs/`.

### Changed
- Moved the GitHub repository from `commondataio/dataportals-registry` to `datenoio/dataportals-registry`. Old GitHub URLs redirect.

## [1.12.0] - 2026-08-18

**GitHub Release**: [v1.12.0](https://github.com/datenoio/dataportals-registry/releases/tag/v1.12.0) - Published August 18, 2026

### Added
- **1,304 net new catalog entries** (1,318 YAML files added; 14 removed); registry source now **16,276** entities (1 scheduled).
- **166 Polish eWMAPA** county and city geoportals, plus **91 Swedish EntryScape** open data catalogs and **62 Lizmap** geoportals (mostly French).
- **151 CKAN** open data portals, including **80 Thai** government catalogs and **34 Indonesian** Satu Data sites.
- **73 GBIF IPT** scientific catalogs, **59 Chinese InstDB** research-data repositories, **32 Japanese data eye** municipal portals, and **25 Chinese Inspur oPortal** sites.
- **101 indicators catalogs**, including **29 Open Data for Africa / Knoema** portals, **18 Datawheel** observatories, and additional national statistical systems.
- **26 microdata catalogs**, including **19 REDATAM** census/survey sites, with first entity roots for **Aruba (AW), Cayman Islands (KY), and Montserrat (MS)**; Kosovo (**XK**) ASKdata recategorized from Serbia.
- Scientific and geospatial coverage: **25 WMO WIS 2.0 in a box** nodes, **21 DataONE** repositories, **20 NASA GES DISC OPeNDAP/Hyrax** endpoints, **22 NextGIS Web** geoportals, **17 Japanese WEKO3** repositories, and **12 rasdaman** datacube services.
- **6 API catalogs** (Tallinn, Estonia RIHA and X-tee, Latvia VISS, Malaysia Kijang, Taiwan TDX) plus additional ArcGIS Hub/Server (50), OpenDataSoft (22), JKAN (14), and Piveau (8) sites.
- **12 software definitions**: Micka, Knoema, REDATAM, Copernicus Data Stores, data eye, Gipuzkoa Irekia, Liferay, OGD Platform India, Inspur oPortal, Piveau, SEU-e, and Ensembl.

### Changed
- Recategorized misplaced catalogs: Lithuania SDG ArcGIS hubs (`Unknown` → `LT/Federal`), American Samoa GIS (`US-AR` → `US-AS`), Bakersfield GIS (`US-DC` → `US-CA`), St. Petersburg stats (`US-TN` → `US-FL`), Guam geoportals (`US/Federal` → `US-GU`), Northern Mariana BECQ (`US-CA` → `US-MP`), Dazhou open data (`CN-NX` → `CN-SC`), and Kosovo ASKdata (`RS` → `XK`).
- Reassigned software IDs on existing records after new platform definitions: Liferay (104), Knoema (41), OGD Platform India (37), SEU-e (18), REDATAM (16), Data Fair (12), Gipuzkoa Irekia (7), oPortal (6), Micka (5), Piveau (4), and others.
- Refreshed metadata (names, links, endpoints, API status) across **327** existing catalogs, including large Spanish and Indian batches.
- Regenerated dataset exports: **16,276** catalog records (entities); 148 software definitions; 1 scheduled.

### Removed
- **14 catalog entries** removed (inactive, duplicate, or replaced), including misplaced WIS 2.0 nodes, US ArcGIS Hub copies, and retired Chinese open-data URLs.

### Fixed
- Quality regression baseline now matches current `analyze-quality` output after the v1.11.0 catalog additions.
- Pin `pyorc<0.11` on Python 3.9 so CI can install `iterabledata` without building dropped 3.9 wheels.
- Make `tests/test_schema_parity.py` collect on Python 3.9 (`from __future__ import annotations`).
- Allow CKAN, WordPress, OpenDataSoft, and Drupal to use additional catalog types they actually host (geoportals, scientific, indicators).

## [1.11.0] - 2026-08-17

**GitHub Release**: [v1.11.0](https://github.com/datenoio/dataportals-registry/releases/tag/v1.11.0) - Published August 17, 2026

### Added
- **444 net new catalog entries** (450 YAML files added; 5 existing records recategorized or replaced); registry source now **14,972** entities (1 scheduled).
- **64 THREDDS** scientific catalogs, including **48 ESGF** climate-data nodes (DKRZ, NASA NCCS, LLNL, CMCC, DIAS Japan, CEDA, and others).
- **49 indicators catalogs**, including national SDG portals, statistical databases, and central-bank, health, and finance indicator systems.
- New catalogs across 64 countries, including **CKAN** (28), **ArcGIS Hub/Server** (39), **Dataverse** (11), **GeoNetwork** (10), **OpenDataSoft** (10), and additional DKAN, Figshare, Pure, and ERDDAP sites.
- **6 metadata catalogs** (including HDA Belgium, I14Y Switzerland, LETZDATA Luxembourg) and **8 API catalogs** (including Datafordeler, Digitraffic, GUS API, Brønnøysund).
- **Apache Superset** software definition (`data/software/indicators/superset.yaml`) with catalog-type mapping in `scripts/constants.py`.

### Changed
- Recategorized or replaced five existing catalogs: Flanders VMM portal (`opendatawsevlaanderenbe` → `opendatawewisvlaanderenbe`), Olomouc geoportal (`EU/CZ-71` → `CZ/CZ-71`), Bordeaux Métropole (`opendatabordeauxmetropolefr` → `datahubbordeauxmetropolefr`), Incheon iMap (`KR-11` → `KR-28`), and Muntinlupa GIS (`muntinlupacitywebgis1com` → `cgismuntinlupacitygovph`).
- Refreshed metadata for selected Brazilian, Estonian, Italian, Korean, Liechtenstein, Maltese, Montenegrin, and Portuguese catalogs (URL/name updates; some status and software corrections).
- Regenerated dataset exports: **14,972** catalog records (entities); 136 software definitions; 1 scheduled.

## [1.10.0] - 2026-08-16

**GitHub Release**: [v1.10.0](https://github.com/datenoio/dataportals-registry/releases/tag/v1.10.0) - Published August 16, 2026

### Added
- **92 net new catalog entries** (95 YAML files added, 3 recategorized); registry source now **14,528** entities (0 scheduled).
- **39 Open Data for Africa** indicators catalogs (country portals plus the continental `dataportal.opendataforafrica.org`), covering many previously missing African countries.
- **15 REDATAM / RpWebEngine** microdata catalogs across Latin America and the Caribbean.
- National statistics and open data portals for countries and territories with little or no prior coverage, including first entity roots for **CF, DJ, ER, GQ, GW, MC, SD, SM, ST, SZ, TL, TM, VC** (plus new Andorra, Bhutan, Brunei, Hungary, Iran, Iraq, Jordan, Liechtenstein, Monaco, Mongolia, Oman open data, Palau indicators, Romania TEMPO, San Marino, Timor-Leste, Turkmenistan, and others).
- Caribbean OECS geoportal (`gis.oecs.int`) and Haiti data search engine (`ayitistats.org`).
- Language and geography reference support for new coverage: Turkmen (`TK`) in `data/reference/langs.csv` / `langs.tsv`; country entries and domain maps for Eritrea, Eswatini, Monaco, Timor-Leste, and Turkmenistan in `scripts/constants.py`.

### Changed
- Regenerated dataset exports: **14,528** catalog records (entities); 135 software definitions; 0 scheduled.
- Recategorized three existing catalogs: New Caledonia `data.gouv.nc` (`FR/FR-NC` → `NC/Federal`), OPT maps portal (`opendata` → `geo`), and NZ PAM geodata (`opendata` → `geo`).
- Refreshed metadata for Pacific SPREP country portals and selected Australian, Oman, Tajikistan, Samoa, and Minnesota entries (including HTTPS/name updates; Minnesota state portal marked deprecated; Samoa MNRE RIO portal marked inactive).
- Extended TLD-to-language defaults (`.sz` → English, `.mc` → French, `.tl` → Portuguese, `.tm` → Turkmen).

## [1.9.0] - 2026-08-10

**GitHub Release**: [v1.9.0](https://github.com/datenoio/dataportals-registry/releases/tag/v1.9.0) - Published August 10, 2026

### Added
- Canonical `owner.type` vocabulary (`data/reference/owner_types.yaml`) with synonym map and quality rules (`OWNER_TYPE_NONCANONICAL` / `INVALID_OWNER_TYPE`).
- Path/country consistency check (`PATH_COUNTRY_MISMATCH`) with allowlisted multinational roots.
- OpenSpec proposals for owner-type/path consistency and endpoint quality priority recalibration.

### Changed
- Regenerated dataset exports and quality reports after integrity cleanup: **14,436** catalog records (entities); 135 software definitions; 0 scheduled.
- Normalized **240** non-canonical `owner.type` values to the canonical vocabulary (e.g. `University` → `Academy`, `Private` → `Business`).
- Corrected path/country placement and metadata for misfiled catalogs (e.g. OpenSLR → `World/`, SoDaNet → `GR/`, SAERI → `FK/`, Gibraltar geoportal → `GI/`, New Caledonia SPREP portal → `NC/`, Italian cadastre geoportal → `IT/`, ITIE Sénégal → `SN/`; Esri China HK and Uruguay INE metadata aligned with path).
- Recalibrated quality priorities so integrity failures remain CRITICAL/IMPORTANT for CI while enrichment-track endpoint gaps stay MEDIUM/warning-only.
- Added missing runtime dependencies to `requirements.txt`; aligned catalog-type keys and re3data HTML parsing with tests.

### Fixed
- Cleared all **CRITICAL** and **IMPORTANT** integrity-track quality issues (remaining open issues are MEDIUM enrichment-track `SOFTWARE_EXPECTED_ENDPOINTS_MISSING_*` only).
- Resolved `DUPLICATE_RECORD_ID` collisions (kept one record or renamed distinct same-domain services such as GeoServer vs IPT).
- Resolved `DUPLICATE_LINK_NORMALIZED` pairs (29 groups): kept preferred keepers (https / non-www / non-Unknown), merged useful metadata, deleted www/duplicate copies.
- Fixed `PATH_COUNTRY_MISMATCH`, `OWNER_LOCATION_SUBREGION_REQUIRED`, `COVERAGE_NORMALIZATION`, and `API_STATUS_MISMATCH` findings.

### Removed
- Consolidated duplicate catalog YAML entries (duplicate ids and normalized-link twins), net reducing the registry from 14,470 to **14,436** entities.

## [1.8.0] - 2026-06-17

**GitHub Release**: [v1.8.0](https://github.com/datenoio/dataportals-registry/releases/tag/v1.8.0) - Published June 17, 2026

### Added
- **124 net new catalog entries** (560 added, 460 removed vs v1.7.0); export snapshot: **14,470** catalog records (entities).
- Quality regression guard (`tests/test_quality_regression.py`) and CI job to prevent quality-issue count regressions.
- Software taxonomy discovery guidance in `README.md` (`category`, `subtype` fields).
- Agent and governance documentation links (`llms.txt`, `DATASHEET.md`, `CITATION.cff`, `SECURITY.md`, `CODE_OF_CONDUCT.md`).
- Expanded `devdocs/quality-fix-workflow.md` and API detection regression tests.

### Changed
- **3,312 catalog entries updated** with refreshed metadata; regenerated datasets and quality reports.
- Export snapshots: 14,470 catalog records in `catalogs.jsonl` / `full.jsonl`; 135 software definitions; 0 scheduled.
- Builder, apidetect, enrichment, and fix scripts improved; scope boundary documented in `AGENTS.md`.

### Removed
- **460 catalog entries** removed (inactive, duplicate, or consolidated).

## [1.7.0] - 2026-02-24

**GitHub Release**: [v1.7.0](https://github.com/datenoio/dataportals-registry/releases/tag/v1.7.0) - Published February 24, 2026

### Added
- **1,647 new catalog entries** (net from v1.6.0); export snapshot: **14,346** catalog records (entities).

### Changed
- **3,432 catalog entries updated** with refreshed metadata; regenerated datasets and quality reports.
- Export snapshots: 14,346 catalog records in `catalogs.jsonl` / `full.jsonl`; 136 software definitions; 0 scheduled (all promoted or removed).

### Removed
- **3,472 catalog entries** removed (inactive, duplicate, or consolidated).

### Fixed
- Data quality rules and fixes (including API status mismatch handling).
- Subregion name/ID mismatch fixes (`fix_subregion_name_id_mismatch.py`).

## [1.6.0] - 2026-02-21

**GitHub Release**: [v1.6.0](https://github.com/datenoio/dataportals-registry/releases/tag/v1.6.0) - Published February 21, 2026

### Added
- **95 new catalog entries** (including Community Statistics Yukon — community-statistics.service.yukon.ca).

### Changed
- **156 catalog entries updated** with refreshed metadata and regenerated datasets and quality reports.
- Export snapshots: **12,699** catalog records (entities); 136 software definitions; combined entities + scheduled in `full.jsonl`.

### Removed
- **1 catalog entry** removed.

### Fixed
- Improved API detection reliability; added regression coverage for apidetect.

## [1.5.0] - 2026-02-12

**GitHub Release**: [v1.5.0](https://github.com/datenoio/dataportals-registry/releases/tag/v1.5.0) - Published February 12, 2026

### Changed
- Refreshed catalog metadata across entity YAML records and rebuilt generated dataset artifacts.
- Updated export snapshots in `README.md` to reflect the latest dataset counts (12,697 catalogs; 136 software definitions; 677 scheduled; 13,374 combined entities + scheduled records).
- Refined release documentation in `CHANGELOG.md` and `README.md`.

### Removed
- Removed legacy `History.md`; changelog history is maintained in `CHANGELOG.md`.

## [1.4.0] - 2026-02-09

**GitHub Release**: [v1.4.0](https://github.com/datenoio/dataportals-registry/releases/tag/v1.4.0) - Published February 9, 2026

### Added
- **208 new catalog entries** (12,489 total catalogs, up from 12,281)
- **Many new CKAN data catalogs** from ecosystem.ckan.org synchronization
- **Reference data files** for validation and consistency:
  - `data/reference/access_modes.yaml` - Standardized access mode values
  - `data/reference/catalog_types.yaml` - Allowed catalog type values
  - `data/reference/software_ids.yaml` - Comprehensive software ID mappings
  - `data/reference/status.yaml` - Status value definitions
- **New documentation**:
  - `devdocs/quality-fix-workflow.md` - Guide for fixing data quality issues
  - `devdocs/scheduled-to-entities.md` - Process for promoting scheduled entries to entities
  - `docs/metadata-quality.md` - Metadata quality standards and guidelines
- **OpenSpec proposal** for schema allowed values enhancement

### Changed
- **Schema validation enhanced** with allowed values validation for key fields (access_mode, catalog_type, software.id, status)
- **Raw JSONL files restored** - Both compressed (.zst) and uncompressed versions now available
- **Updated entity metadata** across multiple catalog entries
- Rebuilt JSONL/Parquet exports and type/software slices (12,489 catalogs; 134 software platforms; 758 scheduled sources; 12,623 combined records)
- **Documentation improvements**:
  - Enhanced AGENTS.md with OpenSpec workflow instructions
  - Expanded CONTRIBUTING.md with quality fix workflow and scheduled-to-entities process
  - Updated README.md with latest statistics and data export information

### Fixed
- Various metadata gaps and inconsistencies in catalog entries
- Improved data quality through enhanced validation rules

### Removed
- Legacy files cleaned up from repository

## [1.3.0] - 2025-12-10

### Added
- Zstandard-compressed exports for `catalogs.jsonl`, `software.jsonl`, `scheduled.jsonl`, and `full.jsonl` plus a `datasets.duckdb` snapshot for analytics-friendly queries
- New scientific and API catalogs across Switzerland, EU, France, Germany, Great Britain, and Italy (e.g., Agroportal, TechnoPortal HEVS, EarthPortal, W3C Linked Open Vocabularies, BiodivPortal, MATPortal, OLS4)
- New API registry entry for `api.gov.it` and additional international research repositories
- Generated data quality reports in `dataquality/` with helper scripts (`fix_*_issues.py`) for resolving flagged items

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

