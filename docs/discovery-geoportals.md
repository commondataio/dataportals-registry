# Discovering geoportals

How to find **geoportal** installations (`catalog_type: Geoportal`). Search-engine syntax: [discovery-search-tools.md](discovery-search-tools.md). If a site is both a map viewer and a dataset portal, pick the **primary** product ([catalog-types.md](catalog-types.md)).

## GeoNetwork (`geonetwork`)

ISO 19115 / CSW catalog. Gallery: [gallery-urls.csv](https://github.com/geonetwork/doc/blob/develop/source/annexes/gallery/gallery-urls.csv). European nodes also appear in the [INSPIRE geoportal](https://inspire-geoportal.ec.europa.eu/).

**Signals:** title “GeoNetwork”, path `/geonetwork` or `/srv/eng/catalog.search`, footer “GeoNetwork opensource”.

**Confirm:** `https://host/geonetwork/srv/eng/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities` (drop `/geonetwork` if the app is at the site root). Also `/srv/api` or `/srv/api/site`.

| Tool | Query |
|------|-------|
| Google | `intitle:"GeoNetwork" "opensource" -site:github.com` |
| Google | `inurl:/srv/eng/catalog.search` |
| Google | `inurl:geonetwork "CSW" site:.europa.eu` |
| Censys (web) | `web.endpoints.http.html_title: "GeoNetwork"` |
| Censys | `web.endpoints.http.body: "GeoNetwork opensource"` |
| Shodan | `http.title:"GeoNetwork"` |
| FOFA | `title="GeoNetwork"` |

**False positives:** documentation, GeoNetwork GitHub, harvested remote catalogs listed *inside* another GeoNetwork. Register the catalog root (`https://host/geonetwork` or `https://host/`), not a single metadata UUID.

OpenWIS (`openwis`) reuses GeoNetwork-style CSW paths; only set `openwis` when the product branding says OpenWIS.

## GeoNode (`geonode`)

Layer/map catalog, often with a bundled GeoServer.

**Confirm:** `/api/layers/` or `/api/datasets/` (GeoNode 4). CSW at `/catalogue/csw?service=CSW&version=2.0.2&request=GetCapabilities`.

| Tool | Query |
|------|-------|
| Google | `"GeoNode" (layers OR maps) inurl:/layers -site:geonode.org` |
| Google | `inurl:/api/layers/ geonode` |
| Censys | `web.endpoints.http.body: "GeoNode"` |
| Shodan | `http.html:"GeoNode"` |

Skip demo.geonode.org and the project docs.

## GeoServer (`geoserver`)

OGC service middleware. Register it when it is the **catalog** (layer list / GetCapabilities as the public product), not merely the backend behind GeoNode or ArcGIS.

**Confirm:** `https://host/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities` (sometimes `/ows` without `/geoserver`). Web admin title “GeoServer: Welcome”.

| Tool | Query |
|------|-------|
| Google | `intitle:"GeoServer: Welcome" OR inurl:/geoserver/web` |
| Google | `inurl:/geoserver/ows GetCapabilities` |
| Censys (hosts) | `host.services.software.product = "GeoServer"` |
| Shodan | `product:GeoServer` or `http.title:"GeoServer"` |
| FOFA | `app="GeoServer"` |

Do not register the `/geoserver/web` login as a catalog if a public WMS/WFS catalog is already represented by a parent GeoNode/GeoNetwork record on the same host. Prefer one record per public catalog UI.

## ArcGIS Hub (`arcgishub`)

Hub sites and Open Data sites on ArcGIS Online. Gallery: [hub.arcgis.com](https://hub.arcgis.com/). Hosts: `*.hub.arcgis.com`, `*opendata.arcgis.com`, plus custom domains.

**Confirm:** `/api/search/v1` or `/api/feed/dcat-us/1.1.json`. Map-first hubs stay `catalog_type: Geoportal`; dataset-first hubs may be Open data portal ([discovery-opendata.md](discovery-opendata.md#arcgis-hub-as-an-open-data-site-arcgishub)).

| Tool | Query |
|------|-------|
| Google | `site:hub.arcgis.com` |
| Google | `site:opendata.arcgis.com "{city or agency}"` |
| Google | `"ArcGIS Hub" "open data" -site:esri.com` |
| Censys | `web.names: "hub.arcgis.com"` |
| crt.sh | `%.hub.arcgis.com` |

## ArcGIS Server / Enterprise (`arcgisserver`)

REST services directory. **Confirm:** `https://host/arcgis/rest/info?f=pjson` or `/arcgis/rest/services?f=pjson` (path may be `/server/rest/services` or `/rest/services`).

| Tool | Query |
|------|-------|
| Google | `intitle:"Folder: /" "ArcGIS REST Services Directory"` |
| Google | `inurl:/arcgis/rest/services` |
| Censys | `host.services.software.product = "ArcGIS"` |
| Shodan | `http.html:"ArcGIS REST Services Directory"` |

Skip internal-only servers that return `401`/`403` for the services list. One record per public services root, not per map service.

## Lizmap (`lizmap`)

QGIS Server web client. **Signals:** `/index.php/view/`, `lizMap`, project list. Vendor: [lizmap.com](https://www.lizmap.com/en/).

| Tool | Query |
|------|-------|
| Google | `"Lizmap" (webgis OR geoportail OR "qgis") -site:github.com` |
| Google | `inurl:lizmap inurl:index.php/view` |
| Censys | `web.endpoints.http.body: "lizMap"` |

## NextGIS Web (`nextgisweb`)

**Signals:** `/resource/0`, NextGIS Web UI, `/api/resource/`.

| Tool | Query |
|------|-------|
| Google | `"NextGIS Web" OR inurl:/resource/0 "nextgis"` |
| Censys | `web.endpoints.http.body: "NextGIS"` |

## STAC (`stacserver`, `stacbrowser`, others)

Static catalogs (`catalog.json`) and STAC API. Index: [stacindex.org/catalogs](https://stacindex.org/catalogs).

**Confirm:** GET `https://host/stac` or `/catalog.json` with `"type": "Catalog"` or STAC API `"/conformance"`. Browser-only UIs that point at a known API should not be duplicated as a second catalog unless they are the public product.

| Tool | Query |
|------|-------|
| Google | `"stac" "catalog.json" OR inurl:/stac filetype:json` |
| Google | `"stac-browser" OR "radiantearth" catalog` |
| Censys | `web.endpoints.http.body: "stac_version"` |

## pygeoapi (`pygeoapi`)

OGC API Features / Records. **Confirm:** `/` or `/openapi` JSON with `pygeoapi` in generator/headers; `/collections`.

| Tool | Query |
|------|-------|
| Google | `"pygeoapi" (collections OR "ogc api") -site:github.com` |
| Censys | `web.endpoints.http.body: "pygeoapi"` |

## MapStore (`mapstore`)

GeoSolutions MapStore. **Signals:** `/mapstore`, `MapStore2`.

| Tool | Query |
|------|-------|
| Google | `"MapStore" geoportal OR inurl:/mapstore -site:github.com` |
| Censys | `web.endpoints.http.body: "MapStore"` |

## QWC2 (`qwc2`)

QGIS Web Client 2. **Signals:** `qwc2`, `qwc-services`, `/theme/` map UI.

| Tool | Query |
|------|-------|
| Google | `"QWC2" OR "QGIS Web Client" geoportal` |
| Censys | `web.endpoints.http.body: "qwc2"` |

## deegree (`deegree`)

OGC services and CSW. **Confirm:** GetCapabilities on `/deegree-webservices` or documented service paths.

| Tool | Query |
|------|-------|
| Google | `"deegree" (CSW OR WMS) GetCapabilities -site:github.com` |
| Censys | `web.endpoints.http.body: "deegree"` |

## Micka (`micka`)

Czech/Slovak metadata catalog. **Signals:** `/micka`, HSLayers, “Micka”.

| Tool | Query |
|------|-------|
| Google | `"Micka" (metadata OR geoportal OR CSW) site:.cz OR site:.sk` |
| Censys | `web.endpoints.http.body: "micka"` |

## GeoBlacklight (`geoblacklight`)

Library geoportals (often US universities). Showcase: [geoblacklight.org/showcase](https://geoblacklight.org/showcase/).

| Tool | Query |
|------|-------|
| Google | `"GeoBlacklight" OR inurl:/catalog geoblacklight site:.edu` |
| Censys | `web.endpoints.http.body: "geoblacklight"` |

## Oskari (`oskari`)

Finnish SDI map client. **Signals:** `Oskari`, `/Oskari/`, map full-screen UI.

| Tool | Query |
|------|-------|
| Google | `"Oskari" (geoportal OR kartta) -site:oskari.org` |
| Censys | `web.endpoints.http.body: "Oskari"` |

## Esri Geoportal Server (`esrigeo`)

Older Esri metadata catalog (not Hub). **Signals:** `/geoportal`, Geoportal Server, CSW.

| Tool | Query |
|------|-------|
| Google | `"Geoportal Server" Esri OR inurl:/geoportal/csw` |
| Censys | `web.endpoints.http.body: "Geoportal Server"` |

## Generic geospatial probes

On a **named** mapping-agency or city GIS host:

```text
/geonetwork/srv/eng/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities
/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities
/arcgis/rest/services?f=pjson
/rest/info?f=pjson
```

## disy Cadenza (`cadenza`)

German public-sector geoanalytics / geoportal (Cadenza Web and Cadenza Workbooks). Vendor: [disy.net](https://www.disy.net/en/products/disy-cadenza/overview/).

**Signals:** path `/cadenza/`, `/public/`, `/pages/map/`, or `/fachauswertungweb/`; HTML/JS contains `cadenza` and often `disy`; workbook navigator or JSF `*.xhtml` map pages; guest login before the public theme tree. Root may return HTTP 401 with an HTML login/guest page — that is still a public catalog if guest access exists.

**Confirm:** GET the catalog URL and match at least two of: `cadenza` in HTML, `disy` branding, Cadenza Web/Workbooks UI, or a working public map permalink. Do not add staff-only Cadenza (police, intranet Energieatlas, bathing-water ops tools). One record per public catalog UI, not per workbook or theme on the same host.

| Tool | Query |
|------|-------|
| Google | `"Cadenza Web" OR "disy Cadenza" (Umwelt OR Kartendienst OR Geoportal) site:.de` |
| Google | `inurl:/cadenza/ (UDO OR iDA OR Kartendienst)` |

## Generic geospatial probes

On a **named** mapping-agency or city GIS host:

```text
/geonetwork/srv/eng/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities
/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities
/arcgis/rest/services?f=pjson
/rest/info?f=pjson
```

Google patterns: `geoportal {agency}`, `INSPIRE {country}`, `IDE {country}` (infraestructura de datos espaciales), `GDI {land}`, `géoportail {région}`.

Regional GIS vendors (NetGIS, Sampaş, GiSoftGis, BelsisIMS, VertiGIS WebOffice, GeoMedia WebMap, disy Cadenza) have distinctive titles and paths listed in [discovery.md](discovery.md#identify-the-software) and [agents/discover.md](agents/discover.md). Search those titles with `site:` for the country TLD rather than inventing new probes.

## Related

- [discovery.md](discovery.md)
- [discovery-search-tools.md](discovery-search-tools.md)
- [discovery-opendata.md](discovery-opendata.md)
- [software-taxonomy.md](software-taxonomy.md)
