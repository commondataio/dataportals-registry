# Harvesting earth-observation and gridded data

THREDDS, ERDDAP, STAC, Open Data Cube, and similar catalogs list **coverages, collections, and datasetIDs** — not journal articles. The usual mistake is harvesting every NetCDF file, STAC **item**, or map tile as a dataset.

Overview: [harvest.md](harvest.md). OGC/STAC grain: [harvest-protocols.md](harvest-protocols.md). Finding installations: [discovery-geoportals.md](discovery-geoportals.md), [discovery-scientific.md](discovery-scientific.md). GET only. Stop on `401`/`403`.

## What to keep

| Keep | Drop |
|------|------|
| THREDDS `dataset` with an ID / OPeNDAP service | Nested **directories** and every file under `datasetScan` |
| ERDDAP `datasetID` row | `allDatasets` helper; every time-step query |
| STAC / ODC **collection** | Granules/items unless that is the product |
| Rasdaman **coverage** (WCS) | WCPS query results as extra datasets |
| Copernicus **collection** / product type | Individual scenes when a collection exists |
| DataONE MN **DATA** objects | CN-wide duplicates of nodes you already harvest |

Replace `https://host` with the catalog origin. Prefer `endpoints[]`.

## THREDDS (`thredds`)

```text
GET https://host/thredds/catalog.xml
```

Recurse `catalogRef`. Harvest `dataset` elements that expose OPeNDAP, WMS, or a durable ID. Prefer `thredds` over `opendap` on the same TDS. Detail also in [harvest-scientific.md](harvest-scientific.md#thredds-thredds).

## ERDDAP (`erddap`)

```text
GET https://host/erddap/info/index.json
```

Each `datasetID` is one dataset. Grid and table datasets are both in scope. Do not page `tabledap` rows as datasets.

## OPeNDAP (`opendap`) and PyDAP (`pydap`)

Hyrax/`catalog.xml` or a PyDAP directory. Harvest catalog **dataset nodes**, not every `.nc` URL. If THREDDS or ERDDAP is on the same host, use those recipes.

## STAC (`stacserver`, `stacbrowser`)

```text
GET https://host/collections
```

Default grain is **collections**. `/search` is a query API — set `limit` and follow `rel=next`. Items are granules. [harvest-geoportals.md](harvest-geoportals.md#stac-stacserver-stacbrowser).

## Open Data Cube (`opendatacube`) and Data Cube OWS (`datacubews`)

Prefer the public **STAC** or OWS `/collections` on that host. The explorer UI is not a dump. Do not harvest indexer/admin. If `software.id` is `stacserver` on the same cube, harvest STAC once.

## Rasdaman (`rasdaman`)

```text
GET https://host/rasdaman/ows?SERVICE=WCS&REQUEST=GetCapabilities
```

Each CoverageId is a dataset analog. Skip petascope HTML chrome and one-off WCPS plots.

## ncWMS (`ncwms`)

WMS GetCapabilities layers (Godiva is a viewer). If a parent THREDDS catalog lists the same datasets, harvest THREDDS.

## Copernicus DHuS (`copernicusdhus`)

Sentinel **DHuS** OData (`/odata/v1/Products` or documented search). Prefer **product types / collections** over every granule. Many nodes need a guest account — stop on `401`. Do not scrape the map.

## Copernicus CDS (`copernicuscds`)

Climate/Atmosphere Data Store. Harvest the public **dataset** catalogue (CDS dataset ids), not every retrieve job. API keys are common — stop on `401`. Do not clone cds.climate.copernicus.eu if you only needed the existing registry record.

## AODN (`aodn`)

Australian Ocean Data Network portal search (`/portal/search` or the API path in `endpoints[]`). Keep **dataset** hits. Drop individual file downloads and the national map chrome.

## DataONE (`dataone`)

Member-node search with `formatType=DATA`. Do not crawl the coordinating node for copies of MNs already in this registry unless asked.

## SciCat (`scicat`)

```text
GET https://host/api/v3/datasets
```

Facility datasets. Stop on `401` for complete metadata; a public list may still exist.

## IRI Data Library (`datalibrary`)

Harvest dataset nodes in `/SOURCES/` or catalog XML, not every maproom statistic view. [harvest-geoportals.md](harvest-geoportals.md#iri-data-library-datalibrary).

## WIS2 Box (`wis20box`) and pygeoapi

`/collections?f=json` — collections, not MQTT broker messages. [harvest-geoportals.md](harvest-geoportals.md).

## Related

- [harvest.md](harvest.md)
- [harvest-geoportals.md](harvest-geoportals.md)
- [harvest-scientific.md](harvest-scientific.md)
- [harvest-protocols.md](harvest-protocols.md)
- [harvest-incremental.md](harvest-incremental.md)
- [harvest-identifiers.md](harvest-identifiers.md)
- [harvest-output.md](harvest-output.md)
- [agents/harvest.md](agents/harvest.md)
