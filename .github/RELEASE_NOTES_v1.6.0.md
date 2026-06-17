# dataportals-registry v1.6.0

**Release date:** February 21, 2026

## Summary

This release refreshes the registry and datasets, adds one new catalog, and improves API detection reliability.

## What's in this release

### Added
- **Community Statistics Yukon** (community-statistics.service.yukon.ca) catalog entry.

### Changed
- Refreshed registry entries and regenerated datasets and quality reports.
- Export snapshots: **13,877** catalog records; 136 software definitions; combined entities + scheduled in `full.jsonl`.

### Fixed
- Improved API detection reliability; added regression coverage for apidetect.

## Data exports (2026-02-21)

| Export | Count |
|--------|--------|
| `catalogs.jsonl` (+ `.zst`) | 13,877 catalog records |
| `software.jsonl` (+ `.zst`) | 136 software/platform definitions |
| `full.jsonl` (+ `.zst`) | 13,877 combined entities + scheduled |
| `full.parquet`, `datasets.duckdb` | Analytics-friendly exports |
| `bytype/`, `bysoftware/` | Sliced JSONL by catalog type or platform |

## Full changelog

See [CHANGELOG.md](../CHANGELOG.md) for full history.
