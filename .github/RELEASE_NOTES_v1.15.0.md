# dataportals-registry v1.15.0

**Release date:** August 22, 2026

## Summary

This release adds 720 net new catalogs and 12 software platform definitions. Coverage expands across scientific repositories (bioinformatics, chemistry, agriculture, climate), geoportals (mviewer, Geocortex, Isogeo, openEO), machine-learning catalogs, UN/IGO indicators, and MetadataWorks catalogues. Dataset exports are rebuilt to match source YAML.

## What's in this release

### Added
- **720 net new catalog entries**; registry source now **19,140** entities (**17** scheduled) across **217** country/territory folders.
- **12 software definitions**; software catalog now **223** platforms: mviewer, Geocortex Essentials, Isogeo, QGIS Server, openEO, MapGIS IGServer, Breedbase, Tripal, VEuPathDB, MassBank, ioChem-BD, and ESGF.
- **471 scientific repositories**, including **19 Tripal**, **15 VEuPathDB**, **8 ESGF** Metagrid/CoG portals, **6 Breedbase**, **5 ioChem-BD**, and **4 MassBank** instances.
- **34 mviewer**, **15 Geocortex**, **10 Isogeo**, and related geoportals; **27** machine-learning catalogs; **59** indicators catalogs (Africa, Latin America, UN/IGO).
- **8 MetadataWorks Metadata Browser** catalogues in the UK. Polar/Arctic/Greenland sources sit in the scheduled queue (17 records).

### Changed
- Recategorized Embrapa GeoInfo from open data to geoportal. Retagged 25 existing catalogs onto new software IDs (openEO, Isogeo, QGIS Server, ESGF, ioChem-BD, VEuPathDB).
- Refreshed metadata on 871 existing catalogs; HTTP-verified endpoints on 716 records.
- Regenerated dataset exports: **19,140** catalog records; 223 software definitions; 17 scheduled (**19,157** in `full.jsonl`).
- Quality regression baseline refreshed (integrity CRITICAL/IMPORTANT remain zero).

### Removed
- **89 catalog entries** removed after v1.14.0 as duplicate recategorized records (mostly US Federal/Other geoportals and ArcGIS Hub copies).

## Data exports (2026-08-22)

| Export | Count |
|--------|--------|
| `catalogs.jsonl` (+ `.zst`) | 19,140 catalog records |
| `software.jsonl` (+ `.zst`) | 223 software/platform definitions |
| `scheduled.jsonl` (+ `.zst`) | 17 scheduled sources |
| `full.jsonl` (+ `.zst`) | 19,157 combined entities + scheduled |
| `full.parquet`, `datasets.duckdb` | Analytics-friendly exports |

## Full changelog

See [CHANGELOG.md](../CHANGELOG.md) for full history.
