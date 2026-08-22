# Harvesting datasets from geoportals

Geoportals expose **layers, collections, and ISO metadata records**, not journal articles. You still must pick the right object: a CSW record, a STAC collection, or an ArcGIS service — not a map tile, a GetMap image, or a viewer theme.

Overview: [harvest.md](harvest.md). Finding installations: [discovery-geoportals.md](discovery-geoportals.md). CSW / STAC / OGC grain: [harvest-protocols.md](harvest-protocols.md). Replace `https://host` with the catalog origin. GET only. Stop on `401`/`403`. Prefer `endpoints[]`.

## What to keep

| Keep | Drop |
|------|------|
| ISO / DCAT **dataset** or **series** metadata | `hierarchyLevel` = `service` (unless you index services separately) |
| STAC **collection** (or items if that is the catalog grain) | Individual map tiles, GetMap/GetTile images |
| GeoNode **dataset** / layer | GeoNode **maps**, geoapps, user documents |
| ArcGIS Feature/Map/Image **service** | GPServer, geocode, print, geometry, NAServer |
| OGC API / pygeoapi **collection** | `/conformance`, OpenAPI UI, HTML themes |
| One record per published dataset | The same layer again via WMS *and* WFS *and* CSW |

If GeoNetwork (or CSW) and GeoServer share a host, harvest the **catalog** (CSW/STAC), not every OWS layer as a duplicate. Viewer-only stacks: [harvest-viewers.md](harvest-viewers.md).

## One catalog per host {#one-catalog-per-host}

Match discovery: [discovery.md](discovery.md#one-catalog-per-public-product).

| Stack | Harvest this | Skip as extra datasets |
|-------|----------------|------------------------|
| GeoNetwork + GeoServer | CSW `GetRecords` dataset/series | WMS layers already in CSW |
| Lizmap / QWC2 / mviewer + QGIS Server | Viewer config or the viewer’s WMS | A second crawl of `qgis_mapserv.fcgi` |
| ArcGIS Hub + Server | Hub DCAT or Hub search | REST services already listed as Hub items |
| STAC API + Browser | `/collections` on the API | Browser HTML as a second catalog |
| openEO | `/collections` | `/processes`, jobs, a parallel `stacserver` crawl |
| MapGIS IGServer | `/igs/rest/mrcs/docs` or `/igs/rest/services` | `/igs/manager`, tiles |

## GeoNetwork (`geonetwork`) {#geonetwork}

CSW is the portable harvest. Path may be `/geonetwork/srv/eng/csw` or `/srv/eng/csw`.

```text
GET https://host/geonetwork/srv/eng/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities
GET https://host/geonetwork/srv/eng/csw?service=CSW&version=2.0.2&request=GetRecords&resultType=results&outputSchema=http://www.isotc211.org/2005/gmd&typeNames=gmd:MD_Metadata&elementSetName=summary&maxRecords=50&startPosition=1
```

Page with `startPosition`. Keep records whose ISO `hierarchyLevel` is `dataset` or `series`. Drop `service`, `application`, and harvested **remote** catalogs listed as sources.

JSON search (GeoNetwork 3/4): `/srv/eng/q` (see `endpoints[]`) or `/srv/api/records`. Do not POST huge Elasticsearch bodies unless the user asked for GN4 search.

OAI: `/srv/eng/oaipmh?verb=Identify`.

## OpenWIS (`openwis`) {#openwis}

WMO OpenWIS catalogs share CSW with GeoNetwork. Harvest CSW `GetRecords` as above. Drop broker/admin HTML.

## GeoNode (`geonode`) {#geonode}

```text
GET https://host/api/datasets/?limit=100&offset=0
```

GeoNode 3 uses `/api/layers/` instead of `/api/datasets/`. Follow `meta.total_count`.

**Drop:** `/api/maps/` (compositions), `/api/geoapps/`, `/api/documents/` unless those documents are the data product, `/api/profiles/`. CSW at `/catalogue/csw` duplicates REST layers — pick one.

## GeoServer (`geoserver`) {#geoserver}

Register GeoServer only when it is the public catalog. Harvest **Layer** names from WMS GetCapabilities (or REST `/geoserver/rest/layers.json` if public).

```text
GET https://host/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities
```

One Layer (or LayerGroup) = one dataset-like object. Do not also ingest every WFS FeatureType and WCS Coverage of the same name. Skip `/geoserver/web` login. OGC API: `/geoserver/ogc/features/collections` and `/geoserver/ogc/stac/v1/collections` when those endpoints exist.

## ArcGIS Hub (`arcgishub`) {#arcgishub}

```text
GET https://host/api/search/v1
GET https://host/api/feed/dcat-us/1.1.json
```

Keep Feature Layer, Table, Shapefile, CSV, and similar **data** items. Drop Hub Site, StoryMap, Dashboard, Web Mapping Application, Domain, and people. DCAT-US `dataset` entries are the preferred grain. Same software as open data — see [harvest-opendata.md](harvest-opendata.md#arcgishub).

## ArcGIS Server (`arcgisserver`) {#arcgisserver}

```text
GET https://host/arcgis/rest/services?f=pjson
GET https://host/arcgis/rest/info?f=pjson
```

Walk folders. Keep `FeatureServer`, `MapServer`, `ImageServer` (and `SceneServer` if you index 3D). **Drop** `GPServer`, `GeometryServer`, `NAServer`, `GeocodeServer`, `IndexingServer`, `PrintingTools`. One service URL is one dataset-like object; do not explode every layer id unless the user wants layer-level records.

## STAC API (`stacserver`) {#stacserver}

```text
GET https://host/
GET https://host/collections
GET https://host/collections/{id}
```

Confirm STAC: landing JSON has `"conformsTo"` (or `stac_version`) and a `collections` link. Default grain: each **collection** is a dataset.

STAC **items** are granules/scenes. Harvest `/collections/{id}/items` only when the catalog’s product is item-level (small archives, not global satellite catalogs). Cap volume. `/search` is for filtered queries, not a full dump — always send `limit` and follow `links` rel `next`.

**Drop:** `conformance`, `queryables`, item assets (COGs, tiles) as datasets, and a second crawl of a STAC Browser on the same origin ([discovery.md](discovery.md#one-catalog-per-public-product)).

If openEO and STAC share a host, harvest [openEO](harvest-earthdata.md#openeo) `/collections` once.

## STAC Browser (`stacbrowser`) {#stacbrowser}

HTML UI over a STAC API. Harvest the **API** `href` from the catalog JSON the browser loads (`catalog.json` / `config.js`), not the Browser HTML. If the API is already registered as `stacserver` on that origin, do not harvest twice.

## pygeoapi (`pygeoapi`) {#pygeoapi}

OGC API Features / Records:

```text
GET https://host/collections?f=json
GET https://host/openapi
```

Each collection is a dataset. Protocol grain: [harvest-protocols.md](harvest-protocols.md).

## pycsw (`pycsw`) {#pycsw}

Prefer CSW `GetRecords` or `/collections?f=json` (same grain as [pygeoapi](#pygeoapi)). Skip installer HTML.

## WIS2 Box (`wis20box`) {#wis20box}

Often wraps pygeoapi. Harvest **collections**, not MQTT broker messages. Same `/collections?f=json` as [pygeoapi](#pygeoapi).

## Lizmap (`lizmap`) {#lizmap}

```text
GET https://host/index.php/lizmap/service?repository=REPO&project=PROJECT&SERVICE=WMS&REQUEST=GetCapabilities
```

Harvest **layers in published projects**. Skip `/admin.php`. One Lizmap site may have many repositories — use the catalog `link` repository, not every sibling.

## GeoMapFish (`geomapfish`) {#geomapfish}

```text
GET https://host/themes
```

Theme JSON lists layers. Keep data layers; drop background/basemap-only entries if the theme is a viewer chrome. Do not scrape MapFish print.

## QWC2 (`qwc2`) {#qwc2}

```text
GET https://host/themes.json
```

The theme/layer tree is the catalog. One theme is not automatically one dataset — harvest **layers** (or the documented QGIS Server WMS). Skip viewer HTML.

## Mapbender (`mapbender`) {#mapbender}

Harvest WMS GetCapabilities of **published applications**, not `/application/` admin. Named layers are the dataset analog.

## MapServer (`mapserver`) {#mapserver}

```text
GET https://host?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetCapabilities
```

Named layers. Do not harvest every CLASS as a dataset. If a parent CSW exists, prefer CSW.

## QGIS Server (`qgisserver`) {#qgisserver}

```text
GET https://host/cgi-bin/qgis_mapserv.fcgi?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetCapabilities
```

Named layers from the published QGIS project. Do not harvest every style/theme as a dataset. If Lizmap, QWC2, or mviewer on the same host is the public catalog, harvest that instead.

## mviewer (`mviewer`) {#mviewer}

Harvest the application config XML layer list (`/apps/*.xml`) or the WMS GetCapabilities those layers point at. One named layer = one dataset analog. Skip mviewerstudio admin. Viewer grain: [harvest-viewers.md](harvest-viewers.md).

## Isogeo (`isogeo`) {#isogeo}

```text
GET https://host/api
```

OpenAPI lists resources. Prefer ISO dataset/series records (or CSW `GetRecords` when present). Drop user accounts and empty workgroups. Distinct from IsiGéo (`isigeo`).

## Geocortex Essentials (`geocortex`) {#geocortex}

```text
GET https://host/Geocortex/Essentials/REST/sites?f=pjson
```

Each Essentials **site** is one application/catalog analog. Do not scrape Html5Viewer tiles or explode every layer unless the user asked for layer-level harvest. Distinct from VertiGIS WebOffice (`weboffice`). Viewer grain: [harvest-viewers.md](harvest-viewers.md).

## Esri Geoportal (`esrigeo`) {#esrigeo}

```text
GET https://host/rest/metadata/search
GET https://host/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities
```

Keep ISO dataset/series. OpenSearch `/opensearch?f=json` paginates with `from` / `size`. Drop service records.

## GET SDI Portal (`getsdiportal`) {#getsdiportal}

Often a GeoServer/MapStore stack. Harvest GeoServer OWS GetCapabilities or the portal CSW, not the MapStore UI. Same OWS grain as [GeoServer](#geoserver).

## Oskari (`oskari`) {#oskari}

```text
GET https://host/action?action_route=GetMapLayers&lang=en&epsg=EPSG:3067
```

Keep map layers. Hierarchical groups (`GetHierarchicalMapLayerGroups`) are folders, not extra datasets.

## IRI Data Library (`datalibrary`) {#datalibrary}

Ingrid/THREDDS-style climate catalogs. Harvest dataset nodes in the library tree (`/SOURCES/` or catalog XML), not every statistic view.

## Wagmap (`wagmap`) {#wagmap}

Japanese わが街ガイド viewers. Public GetCapabilities is often **missing or `403`**. Harvest only when a CSW/WMS/REST catalog is public. Do not scrape map tiles. Detail: [harvest-viewers.md](harvest-viewers.md#wagmap).

## EWMAPA (`ewmapa`) {#ewmapa}

Polish geoportal2.pl viewers. Same grain as [Wagmap](#wagmap): harvest only public CSW/WMS/REST. Do not scrape tiles. [harvest-viewers.md](harvest-viewers.md#ewmapa).

## gvSIG Online (`gvsigonline`) {#gvsigonline}

Municipal SDI over GeoServer (optional GeoNetwork). Harvest **published project layers** or GeoServer GetCapabilities on that host. Prefer CSW if GeoNetwork is public. Do not register/harvest a second GeoServer catalog for the same portal. Skip `/gvsigonline/` admin.

## Micka (`micka`) {#micka}

CSW GetRecords. Keep ISO `dataset` / `series`. Same grain as GeoNetwork ([harvest-protocols.md](harvest-protocols.md#csw)).

## deegree (`deegree`) {#deegree}

CSW and/or WFS GetCapabilities. Harvest metadata records or feature types that are published datasets. Skip installer/demo and xPlanBox admin HTML.

## ERDAS APOLLO (`erdasapollo`) {#erdasapollo}

```text
GET https://host/erdas-iws/ogc/wms/?service=WMS&request=GetCapabilities&version=1.3.0
```

Also CSW when listed in `endpoints[]`. Keep catalog/coverage records. Drop Image Manager login and the installer.

## NextGIS Web (`nextgisweb`) {#nextgisweb}

REST resource tree (`/api/resource/`). Keep vector/raster **layers**. Skip lookup tables, styles, and webmaps unless the user asked for maps as datasets.

## CoGIS (`cogis`) {#cogis}

ArcGIS-style REST under `/elitegis/rest/services`, `/arcgis3/rest/services`, or `/arcgisserver/rest/services` (`f=pjson`). Same keep/drop as [ArcGIS Server](#arcgisserver).

## eLiteGIS (`elitegis`) {#elitegis}

Same REST grain as [CoGIS](#cogis) when the branded viewer is eLiteGIS.

## Other geo platforms (short)

| `software.id` | List | Filter |
|---------------|------|--------|
| `mapstore` | GeoStore `/rest/geostore/` or backend CSW | Maps vs catalogs — keep catalog/dataset resources |
| `koordinates` | `/services/api/v1.x/data/` | Data sets, not tiles |
| `terria` | init catalog JSON | Catalog members typed as data, not Magda UI chrome |
| `opendatacube` | STAC or OWS collections | Datasets/cubes, not indexer admin |
| `rasdaman` | WCS GetCapabilities | Coverages |
| `ncwms` | WMS GetCapabilities | Layers (Godiva is a viewer) |
| `pycsw` | CSW GetRecords | Dataset metadata |
| `geoblacklight` | `/catalog.json` | Geospatial items; drop books/images if mixed |
| `opengeoportal` | search API / Solr | Layers, not institutions |
| `mapapps` | `/mapapps/` is a **viewer** | Harvest the CSW/ArcGIS backend if public |
| `geocortex` | `.../REST/sites?f=pjson` | Each Essentials **site** is one application; do not scrape Html5Viewer tiles or explode every layer |
| `qgisserver` | WMS GetCapabilities | Named layers; skip if Lizmap/QWC2/mviewer is the public catalog |
| `mviewer` | `/apps/*.xml` or WMS | Layers in the config, not tiles |
| `isogeo` | `/api` or CSW | ISO dataset/series, not workgroups |
| `openeo` | `/collections` | Collections, not process graphs or job results |
| `mapgisigserver` | `/igs/rest/mrcs/docs?f=json` or `/igs/rest/services?f=json` | Map documents / services; not tiles or `/igs/manager` |

Municipal viewers (cardo, NetGIS, GC Navi, NOL-IS, Masterportal, Tianditu, Wagmap, GiSoftGis, PopGIS, ActiveMap, Geonomics, ORBISMap): [harvest-viewers.md](harvest-viewers.md). SuperMap iServer/iPortal and MapGIS IGServer recipes are also on [harvest-viewers.md](harvest-viewers.md). MapProxy (`mapproxy`) is a cache — do not treat every cached layer as a new dataset if a parent SDI already lists it. Gridded EO (STAC, ODC, Rasdaman, Copernicus, ncWMS): [harvest-earthdata.md](harvest-earthdata.md). smart.finder: [harvest-viewers.md](harvest-viewers.md#smartfindersdi).

## Pagination and duplicates

- CSW: `maxRecords` + `startPosition` (or `nextRecord`).
- STAC / OGC API: `links` with `rel=next`.
- ArcGIS: folder recursion; do not follow `extent` queries as extra datasets.
- Deduplicate on fileIdentifier, layer name + host, or service URL plus catalog `uid` ([harvest-identifiers.md](harvest-identifiers.md)).

## Related

- [harvest.md](harvest.md)
- [harvest-opendata.md](harvest-opendata.md) (ArcGIS Hub as open data)
- [harvest-protocols.md](harvest-protocols.md)
- [harvest-viewers.md](harvest-viewers.md)
- [harvest-earthdata.md](harvest-earthdata.md)
- [harvest-incremental.md](harvest-incremental.md)
- [harvest-identifiers.md](harvest-identifiers.md)
- [harvest-output.md](harvest-output.md)
- [discovery-geoportals.md](discovery-geoportals.md)
- [apidetect.md](apidetect.md)
