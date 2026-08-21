# Catalog types

`catalog_type` is required and must be one of the schema-allowed values. The type folder on disk should match.

| `catalog_type` | Folder | Typical contents |
|----------------|--------|------------------|
| Open data portal | `opendata/` | Government and institutional open data |
| Geoportal | `geo/` | Spatial data, OGC services, map viewers |
| Scientific data repository | `scientific/` | Research data, CRIS, institutional repos |
| Indicators catalog | `indicators/` | Statistical indicators, SDMX, dashboards |
| Microdata catalog | `microdata/` | Survey / census microdata (NADA, Nesstar, …) |
| Machine learning catalog | `ml/` | ML datasets and models |
| Data search engine | `search/` | Cross-catalog search / aggregators |
| API Catalog | `api/` | API directories |
| Data marketplace | `marketplace/` | Commercial data markets |
| Metadata catalog | `metadata/` | Metadata registries / MDRs |
| Other | `other/` | Uncategorized |
| Datasets list | `other/` | Simple lists of datasets |
| General research repository | `scientific/` or `other/` | Broad research repos |

Controlled list: `data/reference/catalog_types.yaml` and `data/schemes/catalog.json`.

## Choosing a type

- Government CKAN / Socrata / OpenDataSoft / uData → **Open data portal**
- GeoNetwork, ArcGIS Hub (GIS-first), Lizmap, NextGIS → **Geoportal**
- Dataverse, DSpace, Invenio, Pure, GBIF IPT → **Scientific data repository**
- PxWeb, OpenSDG, Knoema, national indicator sites → **Indicators catalog**
- NADA, NESSTAR, REDATAM, Colectica → **Microdata catalog**
- Sites that only search other catalogs → **Data search engine** (aggregators score lower on [trust-score.md](trust-score.md))

If a site is both a geoportal and an open data portal, pick the **primary** user-facing product (map/CSW → geo; dataset catalog → opendata).

Finding catalogs that are not in the registry yet: [discovery.md](discovery.md). Platform-specific search queries: [discovery-opendata.md](discovery-opendata.md), [discovery-geoportals.md](discovery-geoportals.md), [discovery-scientific.md](discovery-scientific.md), [discovery-indicators.md](discovery-indicators.md).
