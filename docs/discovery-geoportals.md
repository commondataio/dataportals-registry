# Discovering geoportals

How to find **geoportal** installations (`catalog_type: Geoportal`). Search-engine syntax: [discovery-search-tools.md](discovery-search-tools.md). If a site is both a map viewer and a dataset portal, pick the **primary** product ([catalog-types.md](catalog-types.md)).

This page covers shared SDI platforms with stable fingerprints, including high-volume regional products (Wagmap, EWMAPA, GeoMapFish, Tianditu, Masterportal, WIS2 Box). Also in `data/software/geo/`: MapTiler Server, MapServer, gvSIG Online, deegree, VertiGIS WebOffice, GeoMedia WebMap, disy Cadenza, and Mapbender. Turkish municipal GIS (NetGIS, Sampaş, GiSoftGis, BelsisIMS) stays in the compact table at the end.

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

## Mapbender (`mapbender`)

Open-source geoportal framework (WhereGroup). **Signals:** Mapbender application UI, `/application/`, configurable map viewers on OGC services. Vendor: [mapbender.org](https://mapbender.org).

| Tool | Query |
|------|-------|
| Google | `"Mapbender" (geoportal OR Anwendung OR "map application") -site:github.com -site:mapbender.org` |
| Google | `inurl:/application/ mapbender` |
| Censys | `web.endpoints.http.body: "Mapbender"` |

Do not register a Mapbender app that is only a login shell with no public map list.

## MapTiler Server (`maptilerserver`)

Self-hosted tile and map-style catalog. Default port **3650**; production sites often sit behind HTTPS on 443. **Signals:** HTML title `MapTiler Server`, `/admin` login, Next.js `pageProps.serverName` / `type` (`list` = public catalog, `logoOnly` = tile backend only).

**Confirm:** `GET https://host/api/maps/{mapId}/style.json` (common ids: `streets`, `basic`, `bright`). Bare `/api/maps` 404s on current versions. Register the public catalog root, not `/admin`. Skip `logoOnly` and staging hosts.

| Tool | Query |
|------|-------|
| Google | `intitle:"MapTiler Server" -site:maptiler.com -site:github.com` |
| Censys | `web.endpoints.http.html_title: "MapTiler Server"` |
| Censys | `host.services.endpoints.http.html_title: "MapTiler Server"` |
| Shodan | `http.title:"MapTiler Server"` |

## MapServer (`mapserver`)

OGC service middleware (WMS/WFS/WCS from a mapfile). Register it when MapServer is the **public catalog** (GetCapabilities or a map list as the product), not merely the renderer behind Lizmap, QWC2, or GeoNetwork.

**Confirm:** WMS `GetCapabilities` whose service metadata mentions MapServer (`cgi-bin/mapserv`, `mapserv.exe`, or a `MapServer` keyword). Typical paths: `/cgi-bin/mapserv`, `/cgi-bin/mapserv.cgi`, or a named `.map` URL.

| Tool | Query |
|------|-------|
| Google | `inurl:cgi-bin/mapserv (WMS OR GetCapabilities)` |
| Google | `"MapServer" GetCapabilities -site:mapserver.org -site:github.com` |
| Censys | `web.endpoints.http.body: "MapServer"` |
| Shodan | `http.html:"MapServer"` |

Do not add a second record for MapServer on a host that already has a Lizmap, QWC2, or GeoNetwork catalog pointing at the same services.

## gvSIG Online (`gvsigonline`)

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

## deegree (`deegree`)

Open-source Java SDI stack (WMS, WFS, WMTS, CSW, WPS, and deegree ogcapi). Used as INSPIRE service middleware.

**Confirm:** GetCapabilities on `/deegree-webservices`, `/services`, or a documented service path whose XML mentions deegree. CSW and OGC API Features are enough to treat it as a catalog when that is the public product.

| Tool | Query |
|------|-------|
| Google | `"deegree" (CSW OR WMS OR "ogcapi") GetCapabilities -site:github.com -site:deegree.org` |
| Censys | `web.endpoints.http.body: "deegree"` |
| Shodan | `http.html:"deegree"` |

## VertiGIS WebOffice (`weboffice`)

Commercial web GIS (formerly SynerGIS WebOffice) on ArcGIS Enterprise. Vendor: [vertigis.com](https://www.vertigis.com). Multi-tenant hosts: `wo-hosting.vertigis.com`, `map.geoportal.at`.

**Signals:** `/synserver` or `/WebOffice/synserver`; HTML title `VertiGIS WebOffice`; `weboffice_packed.css`; core, flex, or mobile clients.

**Confirm:** GET the synserver URL and match the title plus `weboffice_packed.css`. One record per public client (tenant), not per map project.

| Tool | Query |
|------|-------|
| Google | `intitle:"VertiGIS WebOffice" OR inurl:/synserver WebOffice` |
| Google | `site:wo-hosting.vertigis.com OR site:map.geoportal.at` |
| Censys | `web.endpoints.http.html_title: "VertiGIS WebOffice"` |
| Censys | `web.endpoints.http.body: "weboffice_packed.css"` |

## GeoMedia WebMap (`geomediawebmap`)

Hexagon / Intergraph Geospatial Portal (GeoMedia WebMap Publisher Portal). Typical paths: `/geoportal01/`, `/cdngiportal/`, `/msip/Full.aspx`, `/Online_Mapping/`.

**Signals:** `Version:` and `Licensed to:` in the UI; `Intergraph.WebSolutions`; `$GP.` JavaScript; title may say Geospatial Portal or GeoMedia WebMap Publisher Portal.

**Confirm:** GET the portal URL and match at least two of those fingerprints. Skip staff-only intranet portals that require authentication for any map list.

| Tool | Query |
|------|-------|
| Google | `"Geospatial Portal" ("Licensed to" OR Intergraph) -site:hexagon.com` |
| Google | `"GeoMedia WebMap" (portal OR geoportal)` |
| Google | `inurl:/geoportal01/ OR inurl:/cdngiportal/ OR inurl:/msip/Full.aspx` |
| Censys | `web.endpoints.http.body: "Intergraph.WebSolutions"` |

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

## disy Cadenza (`cadenza`)

German public-sector geoanalytics / geoportal (Cadenza Web and Cadenza Workbooks). Vendor: [disy.net](https://www.disy.net/en/products/disy-cadenza/overview/).

**Signals:** path `/cadenza/`, `/public/`, `/pages/map/`, or `/fachauswertungweb/`; HTML/JS contains `cadenza` and often `disy`; workbook navigator or JSF `*.xhtml` map pages; guest login before the public theme tree. Root may return HTTP 401 with an HTML login/guest page — that is still a public catalog if guest access exists.

**Confirm:** GET the catalog URL and match at least two of: `cadenza` in HTML, `disy` branding, Cadenza Web/Workbooks UI, or a working public map permalink. Do not add staff-only Cadenza (police, intranet Energieatlas, bathing-water ops tools). One record per public catalog UI, not per workbook or theme on the same host.

| Tool | Query |
|------|-------|
| Google | `"Cadenza Web" OR "disy Cadenza" (Umwelt OR Kartendienst OR Geoportal) site:.de` |
| Google | `inurl:/cadenza/ (UDO OR iDA OR Kartendienst)` |
| Censys | `web.endpoints.http.body: "cadenza"` |

## Wagmap / わが街ガイド (`wagmap`)

PASCO hosted public WebGIS for Japanese prefectures and municipalities. Vendor: [pasco.co.jp](https://www.pasco.co.jp/biz/app-soft/wagamachiguide/). Tenants usually live under `www2.wagmap.jp` plus a city path, or a city custom domain loading GeoAccessJS portal assets.

**Signals:** hostname `www2.wagmap.jp`; title or branding わが街ガイド / Wagmap; GeoAccessJS; optional open-data catalog alongside the map gallery.

**Confirm:** GET the tenant URL and match Wagmap / GeoAccessJS branding. One record per public tenant, not per map layer. Skip staff-only municipal GIS that requires login for any map list.

| Tool | Query |
|------|-------|
| Google | `site:www2.wagmap.jp` |
| Google | `"わが街ガイド" OR Wagmap (オープンデータ OR 地図) site:.jp` |
| Censys | `web.names: "www2.wagmap.jp"` |
| crt.sh | `%.wagmap.jp` |

## EWMAPA (`ewmapa`)

GEOBID GIS used for Polish cadastral, utility, and municipal map publication. Vendor: [geobid.pl](https://geobid.pl/). Many public viewers are hosted on `*.geoportal2.pl`.

**Signals:** `geoportal2.pl` host; EWMAPA / GEOBID branding; municipal SIP / geoportal UI.

**Confirm:** GET the public map catalog (not a single WMS layer URL). Duplicate-check the same gmina under GeoServer or ArcGIS before adding a second record.

| Tool | Query |
|------|-------|
| Google | `site:geoportal2.pl` |
| Google | `"EWMAPA" OR "GEOBID" (geoportal OR SIP) site:.pl` |
| Censys | `web.names: "geoportal2.pl"` |
| crt.sh | `%.geoportal2.pl` |

## GeoMapFish (`geomapfish`)

Open-source WebGIS (c2cgeoportal + ngeo). Common in Swiss cantons and other European public geoportals. Site: [geomapfish.org](https://geomapfish.org).

**Signals:** `ngeo` / `gmf-` CSS classes; `/themes` JSON; WMS/WMTS theme tree; `c2cgeoportal` in HTML or JS bundles.

**Confirm:** GET `/themes` (or the documented theme API) and a public map UI. One record per public geoportal, not per theme.

| Tool | Query |
|------|-------|
| Google | `"GeoMapFish" OR c2cgeoportal (geoportail OR geoportal) -site:github.com` |
| Google | `inurl:/themes ngeo OR geomapfish` |
| Censys | `web.endpoints.http.body: "c2cgeoportal"` |
| Censys | `web.endpoints.http.body: "gmf-"` |

## Tianditu (`tianditu`)

China National Geographic Information Public Service Platform (Map World). National, provincial, and municipal nodes share NGCC APIs and branding. Site: [tianditu.gov.cn](https://www.tianditu.gov.cn).

**Signals:** `tianditu` in hostname or HTML; 天地图 branding; Map World API keys / `tianditu.gov.cn` tile or widget hosts.

**Confirm:** GET the public node (province or city) and match 天地图 / Tianditu. One record per public node, not per map API key. Skip pure tile endpoints with no catalog UI.

| Tool | Query |
|------|-------|
| Google | `"天地图" (省 OR 市 OR 地理信息) -site:tianditu.gov.cn` |
| Google | `inurl:tianditu OR "Map World" 地理` |
| Censys | `web.endpoints.http.body: "tianditu"` |

## Masterportal (`masterportal`)

Hamburg LGV open-source map viewer used by German federal, state, and municipal agencies. Site: [masterportal.org](https://www.masterportal.org).

**Signals:** `Masterportal` in title or footer; `lgv-config` / `config.js` portal JSON; OGC WMS/WFS/CSW theme tree.

**Confirm:** GET the viewer URL and match Masterportal config plus a public layer tree. One record per public portal instance.

| Tool | Query |
|------|-------|
| Google | `"Masterportal" (Geoportal OR Kartendienst) site:.de -site:masterportal.org` |
| Censys | `web.endpoints.http.body: "Masterportal"` |
| Censys | `web.endpoints.http.body: "lgv-config"` |

## WIS 2.0 Box (`wis20box`)

WMO WIS2 reference node for publishing meteorological and related geospatial data. Source: [wmo-im/wis2box](https://github.com/wmo-im/wis2box).

**Signals:** `wis2box` in HTML or API; pygeoapi / OGC API Features alongside WIS2 messaging; WMO WIS2 branding.

**Confirm:** GET the public discovery UI or OGC API landing page. Register the node catalog, not an individual dataset or MQTT topic.

| Tool | Query |
|------|-------|
| Google | `"wis2box" OR "WIS 2.0 Box" (pygeoapi OR "OGC API") -site:github.com` |
| Censys | `web.endpoints.http.body: "wis2box"` |

## GET SDI Portal (`getsdiportal`)

Geospatial Enabling Technologies SDI client over GeoServer / GeoNetwork. Common in Greek municipal and regional SDIs.

**Signals:** tabbed UI (map, metadata, files, services); GET SDI / GETMAP branding; CSW plus WMS/WFS.

**Confirm:** GET the portal home and match the tabbed SDI UI. Do not also register the bundled GeoServer on the same host.

| Tool | Query |
|------|-------|
| Google | `"GET SDI Portal" OR "GETMAP" (geoportal OR CSW) -site:getmap.eu` |
| Censys | `web.endpoints.http.body: "GET SDI"` |

## MapProxy (`mapproxy`)

Open-source map cache/proxy. Register only when MapProxy is the **public catalog** (demo viewer + service list), not a silent cache behind another geoportal.

**Confirm:** GET `/demo/` or WMTS/WMS GetCapabilities whose service title mentions MapProxy.

| Tool | Query |
|------|-------|
| Google | `intitle:"MapProxy" (demo OR WMTS) -site:github.com -site:mapproxy.org` |
| Censys | `web.endpoints.http.body: "MapProxy"` |

## Terria (`terria`)

Open-source catalog-driven map portal (TerriaJS). Site: [terria.io](https://terria.io).

**Signals:** TerriaJS / National Map-style catalog tree; `config.json` + `catalog.json`; Magda or CKAN-backed catalogs behind the viewer.

**Confirm:** GET the viewer and a working catalog JSON. If the same datasets are already a CKAN/Magda catalog on that host, prefer the dataset CMS unless the map is the primary product.

| Tool | Query |
|------|-------|
| Google | `"Terria" (catalog OR "National Map") -site:github.com` |
| Censys | `web.endpoints.http.body: "Terria"` |

## MapBiomas (`mapbiomas`)

Land-cover collections and map viewers. Country nodes (Brazil, Indonesia, and others) share the MapBiomas web app. Site: [mapbiomas.org](https://mapbiomas.org).

**Confirm:** GET the country node and match MapBiomas collections UI. One record per public country/program portal.

| Tool | Query |
|------|-------|
| Google | `"MapBiomas" (coleções OR collections OR geoportal)` |
| Censys | `web.endpoints.http.body: "MapBiomas"` |

## ERDAS APOLLO (`erdasapollo`)

Hexagon geospatial content management. Vendor: [hexagon.com](https://hexagon.com/products/erdas-apollo).

**Signals:** APOLLO Image Manager / Web Client; ERDAS APOLLO in HTML; WMS/WMTS/CSW from an APOLLO catalog.

**Confirm:** GET the public discovery client (not an intranet Image Manager). Skip login-only enterprise catalogs.

| Tool | Query |
|------|-------|
| Google | `"ERDAS APOLLO" (WMS OR catalog OR geoportal) -site:hexagon.com` |
| Censys | `web.endpoints.http.body: "ERDAS APOLLO"` |

## pycsw (`pycsw`)

OGC CSW and OGC API – Records server. Site: [pycsw.org](https://pycsw.org/). Register when pycsw is the public catalog, not only the CSW backend of GeoNode/GeoNetwork.

**Confirm:** CSW `GetCapabilities` or OGC API Records landing page mentioning pycsw.

| Tool | Query |
|------|-------|
| Google | `"pycsw" (CSW OR "OGC API" Records) -site:github.com -site:pycsw.org` |
| Censys | `web.endpoints.http.body: "pycsw"` |

## Koordinates (`koordinates`)

Cloud geospatial data platform. Hosts: `*.koordinates.com` plus custom government domains. Site: [koordinates.com](https://koordinates.com).

**Confirm:** GET the public data catalog (not a single layer). One record per tenant catalog.

| Tool | Query |
|------|-------|
| Google | `site:koordinates.com (data OR layers)` |
| Google | `"Powered by Koordinates" OR "koordinates" "open data"` |
| crt.sh | `%.koordinates.com` |

## Other high-volume regional GIS

Use the same accept/reject rules. Search the product title with the country TLD; do not invent extra probes.

| `software.id` | Typical signals | Notes |
|---------------|-----------------|-------|
| `gcnavi` | `geocloud.jp/webgis/` (org subdomain), GC Navi / GeoCloud | Japanese local-government WebGIS (Informatix) |
| `nolis` | `maps.nol-is.de`, `static.nol-is.de`, NOL-IS | German municipal WebGIS |
| `cardo` | `/net3/public/`, cardo.Map | IDU IT public-sector geoportals |
| `netgisserver` | `/keos/`, `/Netgis7`, title `NetGIS Server 7` | Turkish municipal |
| `sampaswebgis` | `/KentrehberiApp/Index`, title `SAMPAŞ WEBGIS` | Turkish municipal |
| `gisoftgis` | `/GiSoftGis/#/cityguidepublic` | Turkish city guide SPA |
| `belsisims` | `ims.*/Projects/*/Pages/KRH.aspx` | Do not confuse with Netcad Netigma |

## Generic geospatial probes

On a **named** mapping-agency or city GIS host:

```text
/geonetwork/srv/eng/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities
/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities
/cgi-bin/mapserv?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetCapabilities
/gvsigonline/
/synserver
/cadenza/
/arcgis/rest/services?f=pjson
/rest/info?f=pjson
```

Google patterns: ``geoportal {agency}``, ``INSPIRE {country}``, ``IDE {country}`` (infraestructura de datos espaciales), ``GDI {land}``, ``géoportail {région}``.

Also try `/themes` (GeoMapFish), `/demo/` (MapProxy), and tenant hosts `www2.wagmap.jp`, `*.geoportal2.pl`, `*.geocloud.jp`.

## Related

- [discovery.md](discovery.md)
- [discovery-search-tools.md](discovery-search-tools.md)
- [discovery-opendata.md](discovery-opendata.md)
- [software-taxonomy.md](software-taxonomy.md)
