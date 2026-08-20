# dataportals-registry v1.13.0

**Release date:** August 20, 2026

## Summary

This release adds 1,442 net new catalogs, expanding geoportal, open data, scientific, indicators, and microdata coverage across 83 countries, and adds 44 software platform definitions. The scheduled queue is now empty.

## What's in this release

### Added
- **1,442 net new catalog entries**; registry source now **17,718** entities (0 scheduled).
- **546 US catalogs**, including 213 geoportals, 129 scientific repositories, 121 indicators catalogs, and 79 open data portals.
- **197 Japanese catalogs**, including **166 わが街ガイド (`wagmap`)** geoportals and **15 GC Navi** viewers.
- **178 Chinese catalogs**, including **109** municipal and provincial open data portals and **48 Tianditu** geoportals.
- **98 German catalogs**, including Masterportal, cardo, NOL-IS, map.apps, and GENESIS-Online.
- **76 GeoMapFish** geoportals (mostly Swiss), **56 Turkish** municipal geoportals, **32 NADA** microdata catalogs, and **30 Greek** geoportals.
- **44 software definitions**, including wagmap, GC Navi, GeoMapFish, Tianditu, Masterportal, MapStore, SciCat, FAIRDOM-SEEK, LKOD, and GENESIS-Online.

### Changed
- Moved the GitHub repository from `commondataio/dataportals-registry` to `datenoio/dataportals-registry`.
- Cleared the scheduled queue (22 promoted, 31 removed).
- Recategorized **252** misplaced catalogs and refreshed metadata on **485** existing catalogs.
- Regenerated dataset exports: **17,718** catalog records; 192 software definitions; 0 scheduled.

### Fixed
- Rehomed four Ohio geoportals that were filed under the wrong place.

### Removed
- **16 catalog entries** removed (ArcGIS Hub templates and copies, Socrata demos, duplicate or inactive endpoints).
- Dropped **31 scheduled catalogs** that could not be verified as public catalogs.

## Data exports (2026-08-20)

| Export | Count |
|--------|--------|
| `catalogs.jsonl` (+ `.zst`) | 17,718 catalog records |
| `software.jsonl` (+ `.zst`) | 192 software/platform definitions |
| `scheduled.jsonl` (+ `.zst`) | 0 scheduled sources |
| `full.jsonl` (+ `.zst`) | 17,718 combined entities + scheduled |
| `full.parquet`, `datasets.duckdb` | Analytics-friendly exports |

## Full changelog

See [CHANGELOG.md](../CHANGELOG.md) for full history.
