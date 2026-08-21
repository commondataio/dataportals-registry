# dataportals-registry v1.14.0

**Release date:** August 21, 2026

## Summary

This release adds 702 net new catalogs and 19 software platform definitions. Coverage expands across geoportals, scientific repositories, metadata catalogs, DHIS2 health indicators, OpenAIRE search gateways, and biodiversity collection portals. The scheduled queue is empty. Dataset exports are rebuilt to match source YAML.

## What's in this release

### Added
- **702 net new catalog entries**; registry source now **18,420** entities (0 scheduled) across **217** country/territory folders, including a first entity root for **Grenada (GD)**.
- **19 software definitions**; software catalog now **211** platforms (MapServer, MapTiler Server, gvSIG Online, deegree, VertiGIS WebOffice, Cadenza, FAIR Data Point, Idra, CONTENTdm, Omeka S, Fedora, OPUS, RADAR, Symbiota, DHIS2, Yoda, OpenAIRE, IPUMS, and GeoMedia WebMap).
- **58 Symbiota** biodiversity portals, **33 OpenAIRE** Explore gateways, **33 FAIR Data Point** metadata catalogs, **20 DHIS2** national HMIS portals, **9 DSpace 7** repositories, **6 RADAR** and **4 Yoda** research-data platforms.
- **52 QWC2**, **45 MapServer**, **13 Mapbender**, and related geoportal products (WebOffice, Cadenza, deegree, gvSIG Online, NextGIS Web, MapTiler Server).
- Catalog discovery, harvest, apidetect, and liveness docs for humans and coding agents.

### Changed
- Cleared the scheduled queue (promoted FAIR Data Point, MapServer, Mapbender, NextGIS Web, then Symbiota and DSpace).
- Recategorized **199** misplaced catalogs (mostly out of `Unknown/` and `World/`) and refreshed metadata on **270** existing catalogs.
- Regenerated dataset exports: **18,420** catalog records; 211 software definitions; 0 scheduled.
- Quality regression baseline refreshed after the catalog additions.

### Removed
- **7 catalog entries** removed (placeholder or private Unknown/World geoportals).

## Data exports (2026-08-21)

| Export | Count |
|--------|--------|
| `catalogs.jsonl` (+ `.zst`) | 18,420 catalog records |
| `software.jsonl` (+ `.zst`) | 211 software/platform definitions |
| `scheduled.jsonl` (+ `.zst`) | 0 scheduled sources |
| `full.jsonl` (+ `.zst`) | 18,420 combined entities + scheduled |
| `full.parquet`, `datasets.duckdb` | Analytics-friendly exports |

## Full changelog

See [CHANGELOG.md](../CHANGELOG.md) for full history.
