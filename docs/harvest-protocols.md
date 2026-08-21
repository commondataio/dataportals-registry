# Harvesting by protocol

Many catalogs share a **protocol**, not a product UI. Use this page when `endpoints[]` lists CSW, OAI-PMH, DCAT, STAC, SDMX, OGC, or ArcGIS REST and you need the harvest grain. Software-specific filters: [harvest-scientific.md](harvest-scientific.md), [harvest-opendata.md](harvest-opendata.md), [harvest-geoportals.md](harvest-geoportals.md), [harvest-indicators.md](harvest-indicators.md), [harvest-metadata.md](harvest-metadata.md).

GET public URLs only. Stop on `401`/`403`. Do not write dataset YAML into this repository.

## Choose the grain

| Protocol | Harvest as a dataset | Do not harvest as a dataset |
|----------|----------------------|-----------------------------|
| OAI-PMH | One **record** after a dataset `set` / `dc:type` filter | Every identifier in an unfiltered IR |
| CSW / ISO | `hierarchyLevel` **dataset** or **series** | `service`, `application`, harvested remote catalogs |
| DCAT | `dcat:Dataset` | `dcat:Distribution` (files), `dcat:Catalog` (the portal) |
| STAC | **Collection** (default) | Items/granules unless that is the product; tiles |
| SDMX | **Dataflow** | Codelists, DSDs, observations |
| OGC WMS/WFS/WCS | Named **Layer** / FeatureType / Coverage | GetMap/GetTile images |
| OGC API | `/collections` entry | `/conformance`, OpenAPI HTML |
| ArcGIS REST | Feature/Map/Image **service** | GPServer, geocode, print |
| CKAN Action | `package_search` **package** | Resource rows, harvest sources |
| SPARQL | Named **dataset** / graph the catalog documents | Every triple |

## OAI-PMH

1. `verb=Identify`
2. `verb=ListSets` — pick dataset / ResearchData / `doc-type:researchdata` sets when they exist
3. `verb=ListRecords&metadataPrefix=oai_dc` (or `oai_datacite`) with `set` and `resumptionToken`

Keep records whose `dc:type` or DataCite `resourceTypeGeneral` matches the [keep list](harvest.md#keep-vs-drop-shared-vocabulary). Drop articles and theses.

Common Identify paths: `/oai`, `/oai/request`, `/cgi/oai2`, `/oai2d`, `/ws/oai`, `/api/oai`, `/srv/eng/oaipmh`.

Incremental: `from=` / `until=` (ISO date). Do not page `ListIdentifiers` titles as datasets.

## CSW (OGC Catalog Service)

```text
GET https://host/geonetwork/srv/eng/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities
GET https://host/geonetwork/srv/eng/csw?service=CSW&version=2.0.2&request=GetRecords&resultType=results&outputSchema=http://www.isotc211.org/2005/gmd&typeNames=gmd:MD_Metadata&elementSetName=summary&maxRecords=50&startPosition=1
```

Page with `startPosition` / `nextRecord`. Keep ISO `hierarchyLevel` `dataset` or `series`. Drop `service` unless you index services separately.

Same pattern on GeoNode `/catalogue/csw`, pycsw `/csw`, Esri Geoportal `/csw`, deegree, Micka, smart.finder. Full GeoNetwork notes: [harvest-geoportals.md](harvest-geoportals.md#geonetwork-geonetwork-and-openwis-openwis).

## DCAT and data.json

```text
GET https://host/data.json
GET https://host/catalog.xml
GET https://host/catalog.json
```

US Project Open Data: `dataset` array in `/data.json`. DCAT-AP: `dcat:Dataset` only. One Dataset with five Distributions is **one** dataset.

Used by CKAN (sometimes), ArcGIS Hub (`/api/feed/dcat-us/1.1.json`), EntryScape, Piveau, LKOD, FAIR Data Point (RDF — [harvest-metadata.md](harvest-metadata.md)). Idra federations duplicate sources — prefer member catalogs ([harvest-opendata.md](harvest-opendata.md)).

## STAC

```text
GET https://host/
GET https://host/collections
```

Default: each **collection** is a dataset. Follow `links` `rel=next`. `/search` is a query API, not a dump — always set `limit` and follow next links. Items are granules; harvest them only when the catalog’s product is item-level. [harvest-geoportals.md](harvest-geoportals.md#stac-stacserver-stacbrowser).

## SDMX

List **dataflows** (REST 2.1 `/dataflow` or NSI `/rest/dataflow`). That is the dataset analog. Do not crawl every observation.

Official hubs (Eurostat, ECB, ILOSTAT, World Bank indicators, WHO GHO, BIS, UNICEF) use the same grain — recipes: [harvest-indicators.md](harvest-indicators.md#official-international-hubs). Structure registries (Fusion Registry) vs data portals (PxWeb, .Stat): [harvest-indicators.md](harvest-indicators.md), [harvest-metadata.md](harvest-metadata.md).

## OGC WMS, WFS, WCS

```text
GET https://host/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities
```

Named Layer / FeatureType / Coverage = one dataset-like object. Do not ingest the same name from WMS **and** WFS **and** WCS. Skip GetMap. If CSW/STAC exists on the same host, harvest the catalog, not every OWS layer.

MapServer, MapTiler Server, Lizmap, QWC2, and many municipal viewers only expose WMS — GetCapabilities is the harvest. Wagmap / EWMAPA often have no public GetCapabilities (`403`) — do not scrape tiles.

## OGC API Features / Records / pygeoapi

```text
GET https://host/collections?f=json
```

Each collection is a dataset. Same grain as STAC collections. WIS2 Box usually wraps this API.

## ArcGIS REST

```text
GET https://host/arcgis/rest/services?f=pjson
```

Walk folders. Keep `FeatureServer`, `MapServer`, `ImageServer`. Drop `GPServer`, geocode, print, geometry, NA. Hub sites: DCAT or `/api/search/v1` with data item types ([harvest-geoportals.md](harvest-geoportals.md#arcgis-server-arcgisserver)).

## CKAN Action API

```text
GET https://host/api/3/action/package_search?q=&rows=100&start=0
```

Packages, not resources. Optional `fq=dataset_type:dataset`. [harvest-opendata.md](harvest-opendata.md#ckan-ckan-and-dkan-dkan).

## SPARQL / linked data

Harvest the catalog’s **dataset list** (TriplyDB `/_api/facets/datasets`, a VoID/DCAT graph the site documents). Do not SELECT every triple. One public SPARQL endpoint is not automatically one dataset — list named graphs or DCAT Datasets first.

## Related

- [harvest.md](harvest.md)
- [harvest-scientific.md](harvest-scientific.md)
- [harvest-opendata.md](harvest-opendata.md)
- [harvest-geoportals.md](harvest-geoportals.md)
- [harvest-indicators.md](harvest-indicators.md)
- [harvest-metadata.md](harvest-metadata.md)
- [harvest-incremental.md](harvest-incremental.md)
- [harvest-earthdata.md](harvest-earthdata.md)
- [harvest-biodiversity.md](harvest-biodiversity.md)
- [harvest-viewers.md](harvest-viewers.md)
- [harvest-identifiers.md](harvest-identifiers.md)
- [harvest-output.md](harvest-output.md)
- [apidetect.md](apidetect.md)
