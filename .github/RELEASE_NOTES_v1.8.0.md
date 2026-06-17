# dataportals-registry v1.8.0

**Release date:** June 17, 2026

## Summary

This release refreshes the registry with 124 net new catalogs, updates over 3,300 entries, and adds CI quality regression guards.

## What's in this release

### Added
- **124 net new catalog entries** (560 added, 460 removed vs v1.7.0).
- Quality regression guard in CI to prevent quality-issue count regressions.
- Software taxonomy discovery guidance and agent/governance documentation links.

### Changed
- **3,312 catalog entries updated** with refreshed metadata; regenerated datasets and quality reports.
- Export snapshots: **14,470** catalog records; 135 software definitions; 0 scheduled.

### Removed
- **460 catalog entries** removed (inactive, duplicate, or consolidated).

## Data exports (2026-06-17)

| Export | Count |
|--------|--------|
| `catalogs.jsonl` (+ `.zst`) | 14,470 catalog records |
| `software.jsonl` (+ `.zst`) | 135 software/platform definitions |
| `full.jsonl` (+ `.zst`) | 14,470 combined entities + scheduled |
| `full.parquet`, `datasets.duckdb` | Analytics-friendly exports |
| `bytype/`, `bysoftware/` | Sliced JSONL by catalog type or platform |

## Full changelog

See [CHANGELOG.md](../CHANGELOG.md) for full history.
