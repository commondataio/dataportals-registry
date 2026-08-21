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
- GeoNetwork, ArcGIS Hub (GIS-first), Lizmap, NextGIS, MapServer, MapTiler Server, gvSIG Online → **Geoportal**
- Dataverse, DSpace, Invenio, Pure, Esploro, GBIF IPT, OPUS, PHAIDRA → **Scientific data repository**
- PxWeb, OpenSDG, Knoema, national indicator sites → **Indicators catalog**
- NADA, NESSTAR, REDATAM, Colectica → **Microdata catalog**
- Sites that only search other catalogs → **Data search engine** (aggregators score lower on [trust-score.md](trust-score.md); Idra federations use `software.id: idra`)
- FAIR Data Point, Aristotle MDR, Fusion Registry, Metadata Browser → **Metadata catalog**
- OpenML-style ML dataset catalogs → **Machine learning catalog** when that is the primary product ([discovery-other.md](discovery-other.md))
- HTML tables / GitHub lists of files → **Datasets list**
- Broad IRs that are not Dataverse/DSpace/Invenio → **General research repository** (or Scientific if that is the primary product)

If a site is both a geoportal and an open data portal, pick the **primary** user-facing product (map/CSW → geo; dataset catalog → opendata).

Finding catalogs that are not in the registry yet: [discovery.md](discovery.md). Harvesting datasets from a catalog API: [harvest.md](harvest.md) ([scientific](harvest-scientific.md), [opendata](harvest-opendata.md), [geo](harvest-geoportals.md), [indicators](harvest-indicators.md), [metadata](harvest-metadata.md), [other](harvest-other.md), [protocols](harvest-protocols.md), [incremental](harvest-incremental.md), [earth observation](harvest-earthdata.md), [biodiversity](harvest-biodiversity.md), [viewers](harvest-viewers.md), [identifiers](harvest-identifiers.md), [output](harvest-output.md)). Platform-specific search queries: [discovery-opendata.md](discovery-opendata.md), [discovery-geoportals.md](discovery-geoportals.md), [discovery-scientific.md](discovery-scientific.md), [discovery-metadata.md](discovery-metadata.md), [discovery-indicators.md](discovery-indicators.md), [discovery-other.md](discovery-other.md).
