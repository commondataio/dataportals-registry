# dataportals-registry v1.11.0

**Release date:** August 17, 2026

## Summary

This release adds 444 net new catalogs, expanding scientific THREDDS/ESGF climate-data coverage, national indicators systems, and open data, geo, metadata, and API catalogs across 64 countries.

## What's in this release

### Added
- **444 net new catalog entries** (450 YAML files added; 5 existing records recategorized or replaced).
- **64 THREDDS** scientific catalogs, including **48 ESGF** climate-data nodes (DKRZ, NASA NCCS, LLNL, CMCC, DIAS Japan, CEDA, and others).
- **49 indicators catalogs**, including national SDG portals, statistical databases, and central-bank, health, and finance indicator systems.
- New catalogs across 64 countries, including **CKAN** (28), **ArcGIS Hub/Server** (39), **Dataverse** (11), **GeoNetwork** (10), **OpenDataSoft** (10), and additional DKAN, Figshare, Pure, and ERDDAP sites.
- **6 metadata catalogs** (including HDA Belgium, I14Y Switzerland, LETZDATA Luxembourg) and **8 API catalogs** (including Datafordeler, Digitraffic, GUS API, Brønnøysund).
- **Apache Superset** software definition with catalog-type mapping.

### Changed
- Recategorized or replaced five existing catalogs: Flanders VMM portal, Olomouc geoportal, Bordeaux Métropole, Incheon iMap, and Muntinlupa GIS.
- Refreshed metadata for selected Brazilian, Estonian, Italian, Korean, Liechtenstein, Maltese, Montenegrin, and Portuguese catalogs.
- Regenerated dataset exports: **14,972** catalog records; 136 software definitions; 1 scheduled.

## Data exports (2026-08-17)

| Export | Count |
|--------|--------|
| `catalogs.jsonl` (+ `.zst`) | 14,972 catalog records |
| `software.jsonl` (+ `.zst`) | 136 software/platform definitions |
| `scheduled.jsonl` (+ `.zst`) | 1 scheduled source |
| `full.jsonl` (+ `.zst`) | 14,973 combined entities + scheduled |
| `full.parquet`, `datasets.duckdb` | Analytics-friendly exports |

## Full changelog

See [CHANGELOG.md](../CHANGELOG.md) for full history.
