# Discovering geoportal SDI platforms

Shared catalog and service stacks (`catalog_type: Geoportal`). Overview and short probe table: [discovery-geoportals.md](discovery-geoportals.md). Regional / municipal viewers: [discovery-geoportals-viewers.md](discovery-geoportals-viewers.md). Search-engine syntax: [discovery-search-tools.md](discovery-search-tools.md).

Do not add dataset-level records (a single CSW UUID, a STAC item, an ArcGIS layer id). One public catalog UI = one registry record.

## GeoNetwork (`geonetwork`) {#geonetwork}

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

## GeoNode (`geonode`) {#geonode}

Layer/map catalog, often with a bundled GeoServer.

**Confirm:** `/api/layers/` or `/api/datasets/` (GeoNode 4). CSW at `/catalogue/csw?service=CSW&version=2.0.2&request=GetCapabilities`.

| Tool | Query |
|------|-------|
| Google | `"GeoNode" (layers OR maps) inurl:/layers -site:geonode.org` |
| Google | `inurl:/api/layers/ geonode` |
| Censys | `web.endpoints.http.body: "GeoNode"` |
| Shodan | `http.html:"GeoNode"` |

Skip demo.geonode.org and the project docs.

## GeoServer (`geoserver`) {#geoserver}

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

## ArcGIS Hub (`arcgishub`) {#arcgishub}

Hub sites and Open Data sites on ArcGIS Online. Gallery: [hub.arcgis.com](https://hub.arcgis.com/). Hosts: `*.hub.arcgis.com`, `*opendata.arcgis.com`, plus custom domains.

**Confirm:** `/api/search/v1` or `/api/feed/dcat-us/1.1.json`. Map-first hubs stay `catalog_type: Geoportal`; dataset-first hubs may be Open data portal ([discovery-opendata.md](discovery-opendata.md#arcgishub)).

| Tool | Query |
|------|-------|
| Google | `site:hub.arcgis.com` |
| Google | `site:opendata.arcgis.com "{city or agency}"` |
| Google | `"ArcGIS Hub" "open data" -site:esri.com` |
| Censys | `web.names: "hub.arcgis.com"` |
| crt.sh | `%.hub.arcgis.com` |

## ArcGIS Server / Enterprise (`arcgisserver`) {#arcgisserver}

REST services directory. **Confirm:** `https://host/arcgis/rest/info?f=pjson` or `/arcgis/rest/services?f=pjson` (path may be `/server/rest/services` or `/rest/services`).

| Tool | Query |
|------|-------|
| Google | `intitle:"Folder: /" "ArcGIS REST Services Directory"` |
| Google | `inurl:/arcgis/rest/services` |
| Censys | `host.services.software.product = "ArcGIS"` |
| Shodan | `http.html:"ArcGIS REST Services Directory"` |

Skip internal-only servers that return `401`/`403` for the services list. One record per public services root, not per map service.

## Lizmap (`lizmap`) {#lizmap}

QGIS Server web client. **Signals:** `/index.php/view/`, `lizMap`, project list. Vendor: [lizmap.com](https://www.lizmap.com/en/).

| Tool | Query |
|------|-------|
| Google | `"Lizmap" (webgis OR geoportail OR "qgis") -site:github.com` |
| Google | `inurl:lizmap inurl:index.php/view` |
| Censys | `web.endpoints.http.body: "lizMap"` |

## NextGIS Web (`nextgisweb`) {#nextgisweb}

**Signals:** `/resource/0`, NextGIS Web UI, `/api/resource/`.

| Tool | Query |
|------|-------|
| Google | `"NextGIS Web" OR inurl:/resource/0 "nextgis"` |
| Censys | `web.endpoints.http.body: "NextGIS"` |

## STAC API (`stacserver`) {#stacserver}

Static catalogs (`catalog.json`) and STAC API. Index: [stacindex.org/catalogs](https://stacindex.org/catalogs).

**Confirm:** GET `https://host/stac` or `/catalog.json` with `"type": "Catalog"` or STAC API `"/conformance"` plus `/collections`. Register the **API root**, not every collection.

| Tool | Query |
|------|-------|
| Google | `"stac" "catalog.json" OR inurl:/stac filetype:json` |
| Censys | `web.endpoints.http.body: "stac_version"` |

Do not add STAC **items** as catalogs.

## STAC Browser (`stacbrowser`) {#stacbrowser}

Radiant Earth STAC Browser (or a fork) as the **public catalog UI**. Confirm the HTML app title/footer mentions STAC Browser and that it points at a STAC API.

If that API is already registered as `stacserver` on the same host, do **not** add a second record unless the browser is the only public product (API is private or on another origin already listed). Prefer `stacserver` when both are public on the same origin.

| Tool | Query |
|------|-------|
| Google | `"stac-browser" OR "radiantearth" catalog` |
| Censys | `web.endpoints.http.body: "stac-browser"` |

## openEO (`openeo`) {#openeo}

EO cloud-processing API with a STAC-compatible collection catalog. Site: [openeo.org](https://openeo.org). Backends include Copernicus Data Space, VITO, EODC, mundialis Actinia, and the Google Earth Engine driver.

**Confirm:** GET the API landing page (often `/openeo/1.2/` or `/v1.0/`) JSON with `api_version` plus `endpoints` for `GET /collections` and `GET /processes`. `/.well-known/openeo` lists versions.

| Tool | Query |
|------|-------|
| Google | `"openeo" ("api_version" OR /collections OR /processes) -site:github.com -site:openeo.org` |
| Google | `inurl:/openeo/ (collections OR processes)` |
| Censys | `web.endpoints.http.body: "openeo"` |

Register the **backend API** root, not Hub HTML alone, unless Hub is the public product (`hub.openeo.org`). Prefer `openeo` over `stacserver` when `/processes` is part of the same API. Skip process-graph playgrounds with no collection list.

## pygeoapi (`pygeoapi`) {#pygeoapi}

OGC API Features / Records. **Confirm:** `/` or `/openapi` JSON with `pygeoapi` in generator/headers; `/collections`.

| Tool | Query |
|------|-------|
| Google | `"pygeoapi" (collections OR "ogc api") -site:github.com` |
| Censys | `web.endpoints.http.body: "pygeoapi"` |

## MapStore (`mapstore`) {#mapstore}

GeoSolutions MapStore. **Signals:** `/mapstore`, `MapStore2`.

| Tool | Query |
|------|-------|
| Google | `"MapStore" geoportal OR inurl:/mapstore -site:github.com` |
| Censys | `web.endpoints.http.body: "MapStore"` |

## QWC2 (`qwc2`) {#qwc2}

QGIS Web Client 2. **Signals:** `qwc2`, `qwc-services`, `/theme/` map UI.

| Tool | Query |
|------|-------|
| Google | `"QWC2" OR "QGIS Web Client" geoportal` |
| Censys | `web.endpoints.http.body: "qwc2"` |

## Mapbender (`mapbender`) {#mapbender}

Open-source geoportal framework (WhereGroup). **Signals:** Mapbender application UI, `/application/`, configurable map viewers on OGC services. Vendor: [mapbender.org](https://mapbender.org).

| Tool | Query |
|------|-------|
| Google | `"Mapbender" (geoportal OR Anwendung OR "map application") -site:github.com -site:mapbender.org` |
| Google | `inurl:/application/ mapbender` |
| Censys | `web.endpoints.http.body: "Mapbender"` |

Do not register a Mapbender app that is only a login shell with no public map list.

## MapTiler Server (`maptilerserver`) {#maptilerserver}

Self-hosted tile and map-style catalog. Default port **3650**; production sites often sit behind HTTPS on 443. **Signals:** HTML title `MapTiler Server`, `/admin` login, Next.js `pageProps.serverName` / `type` (`list` = public catalog, `logoOnly` = tile backend only).

**Confirm:** `GET https://host/api/maps/{mapId}/style.json` (common ids: `streets`, `basic`, `bright`). Bare `/api/maps` 404s on current versions. Register the public catalog root, not `/admin`. Skip `logoOnly` and staging hosts.

| Tool | Query |
|------|-------|
| Google | `intitle:"MapTiler Server" -site:maptiler.com -site:github.com` |
| Censys | `web.endpoints.http.html_title: "MapTiler Server"` |
| Censys | `host.services.endpoints.http.html_title: "MapTiler Server"` |
| Shodan | `http.title:"MapTiler Server"` |

## MapServer (`mapserver`) {#mapserver}

OGC service middleware (WMS/WFS/WCS from a mapfile). Register it when MapServer is the **public catalog** (GetCapabilities or a map list as the product), not merely the renderer behind Lizmap, QWC2, or GeoNetwork.

**Confirm:** WMS `GetCapabilities` whose service metadata mentions MapServer (`cgi-bin/mapserv`, `mapserv.exe`, or a `MapServer` keyword). Typical paths: `/cgi-bin/mapserv`, `/cgi-bin/mapserv.cgi`, or a named `.map` URL.

| Tool | Query |
|------|-------|
| Google | `inurl:cgi-bin/mapserv (WMS OR GetCapabilities)` |
| Google | `"MapServer" GetCapabilities -site:mapserver.org -site:github.com` |
| Censys | `web.endpoints.http.body: "MapServer"` |
| Shodan | `http.html:"MapServer"` |

Do not add a second record for MapServer on a host that already has a Lizmap, QWC2, or GeoNetwork catalog pointing at the same services.

## QGIS Server (`qgisserver`) {#qgisserver}

OGC service middleware from a QGIS project (WMS/WFS/WCS, OGC API). Register it when QGIS Server is the **public catalog** (GetCapabilities as the product), not merely the renderer behind Lizmap, QWC2, or mviewer.

**Confirm:** WMS `GetCapabilities` whose service metadata mentions QGIS Server (`qgis_mapserv.fcgi`, `MAP=` `.qgs` / `.qgz`, or a `QGIS` keyword). Typical paths: `/cgi-bin/qgis_mapserv.fcgi`, `/ows`, or a named `.qgs` URL.

| Tool | Query |
|------|-------|
| Google | `inurl:qgis_mapserv.fcgi (WMS OR GetCapabilities)` |
| Google | `"QGIS Server" GetCapabilities -site:qgis.org -site:github.com` |
| Censys | `web.endpoints.http.body: "QGIS Server"` |
| Shodan | `http.html:"QGIS Server"` |

Do not add a second record for QGIS Server on a host that already has a Lizmap, QWC2, or mviewer catalog pointing at the same services.

## mviewer (`mviewer`) {#mviewer}

GéoBretagne thematic map viewer (OpenLayers). Common in French régions, départements, and communes. Site: [mviewer.github.io](https://mviewer.github.io). Distinct from Lizmap (`lizmap`) and QWC2 (`qwc2`).

**Signals:** `mviewer` in HTML/JS; config XML under `/apps/` (often `default.xml`); optional mviewerstudio; GéoBretagne / Kartenn branding.

**Confirm:** GET the viewer URL and match mviewer JS plus a public layer/theme config. One record per public application (config), not per layer.

| Tool | Query |
|------|-------|
| Google | `"mviewer" (géoportail OR geoportail OR "openlayers") -site:github.com -site:mviewer.github.io` |
| Google | `inurl:mviewer (apps OR config.xml) site:.fr` |
| Censys | `web.endpoints.http.body: "mviewer"` |

Skip mviewerstudio admin and demo configs on mviewer.github.io unless the task is to record them.

## Isogeo (`isogeo`) {#isogeo}

French SaaS GIS metadata catalog (OpenCatalog / App). Vendor: [isogeo.com](https://www.isogeo.com). Distinct from IsiGéo (`isigeo`, Geomatika).

**Signals:** title or footer “Isogeo” / OpenCatalog; path `/api` OpenAPI; ISO 19115 inventory; often CSW.

**Confirm:** GET `/api` (OpenAPI mentioning Isogeo) or the public OpenCatalog search UI. One record per public catalog, not per metadata sheet.

| Tool | Query |
|------|-------|
| Google | `"Isogeo" (OpenCatalog OR géocatalogue OR "catalogue de données") site:.fr -site:isogeo.com` |
| Google | `"powered by Isogeo" OR "OpenCatalog Isogeo"` |
| Censys | `web.endpoints.http.body: "Isogeo"` |

Do not set `isigeo` (IsiGéo) for an Isogeo OpenCatalog.

## gvSIG Online (`gvsigonline`) {#gvsigonline}

Municipal / regional SDI built by the gvSIG Association. Demo and docs: [demo.gvsigonline.com](https://demo.gvsigonline.com/gvsigonline/core/documentation/). GeoServer is required underneath; optionally GeoNetwork.

**Signals:** path `/gvsigonline/`; public project picker `select_public_project`; title or footer “gvSIG Online”.

**Confirm:** GET `https://host/gvsigonline/` (or the catalog `link`) and match the gvSIG Online UI. Prefer the public viewer root, not `/geoserver/web`. Skip the demo unless the task is to record it.

| Tool | Query |
|------|-------|
| Google | `"gvSIG Online" (geoportal OR visor OR IDE) -site:gvsig.com -site:github.com` |
| Google | `inurl:/gvsigonline/ select_public_project` |
| Censys | `web.endpoints.http.body: "gvSIG Online"` |
| Censys | `web.endpoints.http.body: "select_public_project"` |

One record per public SDI UI. Do not also register the bundled GeoServer as a separate catalog on the same host.

## deegree (`deegree`) {#deegree}

Open-source Java SDI stack (WMS, WFS, WMTS, CSW, WPS, and deegree ogcapi). Used as INSPIRE service middleware.

**Confirm:** GetCapabilities on `/deegree-webservices`, `/services`, or a documented service path whose XML mentions deegree. CSW and OGC API Features are enough to treat it as a catalog when that is the public product.

| Tool | Query |
|------|-------|
| Google | `"deegree" (CSW OR WMS OR "ogcapi") GetCapabilities -site:github.com -site:deegree.org` |
| Censys | `web.endpoints.http.body: "deegree"` |
| Shodan | `http.html:"deegree"` |

## VertiGIS WebOffice (`weboffice`) {#weboffice}

Commercial web GIS (formerly SynerGIS WebOffice) on ArcGIS Enterprise. Vendor: [vertigis.com](https://www.vertigis.com). Multi-tenant hosts: `wo-hosting.vertigis.com`, `map.geoportal.at`.

**Signals:** `/synserver` or `/WebOffice/synserver`; HTML title `VertiGIS WebOffice`; `weboffice_packed.css`; core, flex, or mobile clients.

**Confirm:** GET the synserver URL and match the title plus `weboffice_packed.css`. One record per public client (tenant), not per map project.

| Tool | Query |
|------|-------|
| Google | `intitle:"VertiGIS WebOffice" OR inurl:/synserver WebOffice` |
| Google | `site:wo-hosting.vertigis.com OR site:map.geoportal.at` |
| Censys | `web.endpoints.http.html_title: "VertiGIS WebOffice"` |
| Censys | `web.endpoints.http.body: "weboffice_packed.css"` |

## Geocortex Essentials (`geocortex`) {#geocortex}

Commercial web GIS (Latitude Geographics, now VertiGIS Studio) on ArcGIS. Vendor: [geocortex.com](https://www.geocortex.com). Hosted tenants: `*.geocortex.com`. Distinct from VertiGIS WebOffice (`weboffice`).

**Signals:** title `Geocortex Essentials Sites Directory` or `Geocortex Viewer for HTML5`; path `/Geocortex/Essentials/` (sometimes `/ess/`) plus `/REST/sites`; `/Html5Viewer/`; footer “licensed Geocortex Essentials”.

**Confirm:** GET `https://host/Geocortex/Essentials/REST/sites?f=pjson` (instance path may include `/public/`, `/EXT/`, or a named instance). JSON `sites` array is the catalog. HTML5 viewers are the public map UI, not extra catalogs. One record per Essentials instance, not per site or viewer. Do not also register the bundled ArcGIS REST root as a second Geocortex catalog; keep an existing `arcgisserver` record on the same host if that is already the services directory.

| Tool | Query |
|------|-------|
| Google | `intitle:"Geocortex Essentials Sites Directory"` |
| Google | `intitle:"Geocortex Viewer for HTML5" -site:github.com` |
| Google | `inurl:/Geocortex/Essentials/REST/sites OR inurl:/Html5Viewer/` |
| Google | `site:geocortex.com Html5Viewer OR Essentials -www -shop -accounts` |
| Censys | `web.endpoints.http.html_title: "Geocortex Essentials Sites Directory"` |
| Censys | `web.endpoints.http.html_title: "Geocortex Viewer for HTML5"` |

Skip `gedemo.geocortex.com`, test hosts, and empty Sites Directories.

## GeoMedia WebMap (`geomediawebmap`) {#geomediawebmap}

Hexagon / Intergraph Geospatial Portal (GeoMedia WebMap Publisher Portal). Typical paths: `/geoportal01/`, `/cdngiportal/`, `/msip/Full.aspx`, `/Online_Mapping/`.

**Signals:** `Version:` and `Licensed to:` in the UI; `Intergraph.WebSolutions`; `$GP.` JavaScript; title may say Geospatial Portal or GeoMedia WebMap Publisher Portal.

**Confirm:** GET the portal URL and match at least two of those fingerprints. Skip staff-only intranet portals that require authentication for any map list.

| Tool | Query |
|------|-------|
| Google | `"Geospatial Portal" ("Licensed to" OR Intergraph) -site:hexagon.com` |
| Google | `"GeoMedia WebMap" (portal OR geoportal)` |
| Google | `inurl:/geoportal01/ OR inurl:/cdngiportal/ OR inurl:/msip/Full.aspx` |
| Censys | `web.endpoints.http.body: "Intergraph.WebSolutions"` |

## Micka (`micka`) {#micka}

Czech/Slovak metadata catalog. **Signals:** `/micka`, HSLayers, “Micka”.

| Tool | Query |
|------|-------|
| Google | `"Micka" (metadata OR geoportal OR CSW) site:.cz OR site:.sk` |
| Censys | `web.endpoints.http.body: "micka"` |

## GeoBlacklight (`geoblacklight`) {#geoblacklight}

Library geoportals (often US universities). Showcase: [geoblacklight.org/showcase](https://geoblacklight.org/showcase/).

| Tool | Query |
|------|-------|
| Google | `"GeoBlacklight" OR inurl:/catalog geoblacklight site:.edu` |
| Censys | `web.endpoints.http.body: "geoblacklight"` |

## Oskari (`oskari`) {#oskari}

Finnish SDI map client. **Signals:** `Oskari`, `/Oskari/`, map full-screen UI.

| Tool | Query |
|------|-------|
| Google | `"Oskari" (geoportal OR kartta) -site:oskari.org` |
| Censys | `web.endpoints.http.body: "Oskari"` |

## Esri Geoportal Server (`esrigeo`) {#esrigeo}

Older Esri metadata catalog (not Hub). **Signals:** `/geoportal`, Geoportal Server, CSW.

| Tool | Query |
|------|-------|
| Google | `"Geoportal Server" Esri OR inurl:/geoportal/csw` |
| Censys | `web.endpoints.http.body: "Geoportal Server"` |

## disy Cadenza (`cadenza`) {#cadenza}

German public-sector geoanalytics / geoportal (Cadenza Web and Cadenza Workbooks). Vendor: [disy.net](https://www.disy.net/en/products/disy-cadenza/overview/).

**Signals:** path `/cadenza/`, `/public/`, `/pages/map/`, or `/fachauswertungweb/`; HTML/JS contains `cadenza` and often `disy`; workbook navigator or JSF `*.xhtml` map pages; guest login before the public theme tree. Root may return HTTP 401 with an HTML login/guest page — that is still a public catalog if guest access exists.

**Confirm:** GET the catalog URL and match at least two of: `cadenza` in HTML, `disy` branding, Cadenza Web/Workbooks UI, or a working public map permalink. Do not add staff-only Cadenza (police, intranet Energieatlas, bathing-water ops tools). One record per public catalog UI, not per workbook or theme on the same host.

| Tool | Query |
|------|-------|
| Google | `"Cadenza Web" OR "disy Cadenza" (Umwelt OR Kartendienst OR Geoportal) site:.de` |
| Google | `inurl:/cadenza/ (UDO OR iDA OR Kartendienst)` |
| Censys | `web.endpoints.http.body: "cadenza"` |

## WIS 2.0 Box (`wis20box`) {#wis20box}

WMO WIS2 reference node for publishing meteorological and related geospatial data. Source: [wmo-im/wis2box](https://github.com/wmo-im/wis2box).

**Signals:** `wis2box` in HTML or API; pygeoapi / OGC API Features alongside WIS2 messaging; WMO WIS2 branding.

**Confirm:** GET the public discovery UI or OGC API landing page. Register the node catalog, not an individual dataset or MQTT topic.

| Tool | Query |
|------|-------|
| Google | `"wis2box" OR "WIS 2.0 Box" (pygeoapi OR "OGC API") -site:github.com` |
| Censys | `web.endpoints.http.body: "wis2box"` |

## GET SDI Portal (`getsdiportal`) {#getsdiportal}

Geospatial Enabling Technologies SDI client over GeoServer / GeoNetwork. Common in Greek municipal and regional SDIs.

**Signals:** tabbed UI (map, metadata, files, services); GET SDI / GETMAP branding; CSW plus WMS/WFS.

**Confirm:** GET the portal home and match the tabbed SDI UI. Do not also register the bundled GeoServer on the same host.

| Tool | Query |
|------|-------|
| Google | `"GET SDI Portal" OR "GETMAP" (geoportal OR CSW) -site:getmap.eu` |
| Censys | `web.endpoints.http.body: "GET SDI"` |

## MapProxy (`mapproxy`) {#mapproxy}

Open-source map cache/proxy. Register only when MapProxy is the **public catalog** (demo viewer + service list), not a silent cache behind another geoportal.

**Confirm:** GET `/demo/` or WMTS/WMS GetCapabilities whose service title mentions MapProxy.

| Tool | Query |
|------|-------|
| Google | `intitle:"MapProxy" (demo OR WMTS) -site:github.com -site:mapproxy.org` |
| Censys | `web.endpoints.http.body: "MapProxy"` |

## Terria (`terria`) {#terria}

Open-source catalog-driven map portal (TerriaJS). Site: [terria.io](https://terria.io).

**Signals:** TerriaJS / National Map-style catalog tree; `config.json` + `catalog.json`; Magda or CKAN-backed catalogs behind the viewer.

**Confirm:** GET the viewer and a working catalog JSON. If the same datasets are already a CKAN/Magda catalog on that host, prefer the dataset CMS unless the map is the primary product.

| Tool | Query |
|------|-------|
| Google | `"Terria" (catalog OR "National Map") -site:github.com` |
| Censys | `web.endpoints.http.body: "Terria"` |

## MapBiomas (`mapbiomas`) {#mapbiomas}

Land-cover collections and map viewers. Country nodes (Brazil, Indonesia, and others) share the MapBiomas web app. Site: [mapbiomas.org](https://mapbiomas.org).

**Confirm:** GET the country node and match MapBiomas collections UI. One record per public country/program portal.

| Tool | Query |
|------|-------|
| Google | `"MapBiomas" (coleções OR collections OR geoportal)` |
| Censys | `web.endpoints.http.body: "MapBiomas"` |

## ERDAS APOLLO (`erdasapollo`) {#erdasapollo}

Hexagon geospatial content management. Vendor: [hexagon.com](https://hexagon.com/products/erdas-apollo).

**Signals:** APOLLO Image Manager / Web Client; ERDAS APOLLO in HTML; WMS/WMTS/CSW from an APOLLO catalog.

**Confirm:** GET the public discovery client (not an intranet Image Manager). Skip login-only enterprise catalogs.

| Tool | Query |
|------|-------|
| Google | `"ERDAS APOLLO" (WMS OR catalog OR geoportal) -site:hexagon.com` |
| Censys | `web.endpoints.http.body: "ERDAS APOLLO"` |

## pycsw (`pycsw`) {#pycsw}

OGC CSW and OGC API – Records server. Site: [pycsw.org](https://pycsw.org/). Register when pycsw is the public catalog, not only the CSW backend of GeoNode/GeoNetwork.

**Confirm:** CSW `GetCapabilities` or OGC API Records landing page mentioning pycsw.

| Tool | Query |
|------|-------|
| Google | `"pycsw" (CSW OR "OGC API" Records) -site:github.com -site:pycsw.org` |
| Censys | `web.endpoints.http.body: "pycsw"` |

## Koordinates (`koordinates`) {#koordinates}

Cloud geospatial data platform. Hosts: `*.koordinates.com` plus custom government domains. Site: [koordinates.com](https://koordinates.com).

**Confirm:** GET the public data catalog (not a single layer). One record per tenant catalog.

| Tool | Query |
|------|-------|
| Google | `site:koordinates.com (data OR layers)` |
| Google | `"Powered by Koordinates" OR "koordinates" "open data"` |
| crt.sh | `%.koordinates.com` |

## IRI Data Library (`datalibrary`) {#datalibrary}

Climate / maproom portals (IRI Columbia and meteorological services). Site: [iridl.ldeo.columbia.edu](https://iridl.ldeo.columbia.edu).

**Signals:** Data Library / maproom; gridded download; IRI-style dataset URLs.

**Confirm:** GET a public maproom or dataset browser. One record per public library, not per maproom view.

| Tool | Query |
|------|-------|
| Google | `"Data Library" (maproom OR IRI) (climate OR geospatial) -site:columbia.edu` |
| Censys | `web.endpoints.http.body: "maproom"` |

## Rasdaman (`rasdaman`) {#rasdaman}

Array database with OGC WCS/WMS/WCPS. Site: [rasdaman.com](https://rasdaman.com). Register the **public service/catalog UI**, not a silent WCS behind another geoportal.

**Confirm:** GetCapabilities or WCPS endpoint that names rasdaman.

| Tool | Query |
|------|-------|
| Google | `"rasdaman" (WCS OR WCPS OR petascope) -site:github.com -site:rasdaman.com` |
| Censys | `web.endpoints.http.body: "rasdaman"` |

## Open Data Cube (`opendatacube`) {#opendatacube}

Earth-observation data cube. Site: [opendatacube.org](https://www.opendatacube.org). Often paired with STAC or `datacubews` (OWS). Prefer STAC/`stacserver` if that is the public catalog; use `opendatacube` when the cube explorer is the product.

**Confirm:** GET the explorer or ODC-indexed catalog UI.

| Tool | Query |
|------|-------|
| Google | `"Open Data Cube" (explorer OR datacube) -site:opendatacube.org -site:github.com` |
| Censys | `web.endpoints.http.body: "opendatacube"` |

## ncWMS (`ncwms`) {#ncwms}

WMS for NetCDF / multidimensional environmental data. Docs: [ncwms](https://reading-escience-centre.github.io/ncwms/). Register when ncWMS is the public map catalog, not a layer inside THREDDS.

**Confirm:** WMS GetCapabilities mentioning ncWMS / Godiva.

| Tool | Query |
|------|-------|
| Google | `"ncWMS" OR Godiva (WMS OR NetCDF) -site:github.com` |
| Censys | `web.endpoints.http.body: "ncWMS"` |

