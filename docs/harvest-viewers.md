# Harvesting map viewers and tile caches

Many geoportals in this registry are **viewers** (QWC2, Masterportal, Lizmap, mviewer, Wagmap, Tianditu). The catalog of datasets is the **layer list** (GetCapabilities, `themes.json`, REST services) — not PNG tiles, print PDFs, or the basemap.

Use this page when `software.id` is a viewer or cache. Full SDI catalogs (GeoNetwork, GeoNode, ArcGIS Server): [harvest-geoportals.md](harvest-geoportals.md). Protocol grain: [harvest-protocols.md](harvest-protocols.md). GET only. Stop on `401`/`403`. Do not scrape tiles.

## Rule

1. If CSW, STAC, or ArcGIS REST exists on the same host, harvest **that** ([harvest-geoportals.md](harvest-geoportals.md)).
2. Else harvest WMS/WMTS/WFS **GetCapabilities** named layers, or the viewer’s JSON theme/layer tree.
3. One named layer (or published service) = one dataset analog. Do not ingest the same layer from WMS and WMTS.
4. Stop if GetCapabilities is `403` or missing — common for Wagmap and EWMAPA.

## Lizmap, QWC2, GeoMapFish, Mapbender, MapServer, QGIS Server, mviewer

Published project/theme **layers**. Recipes: [harvest-geoportals.md](harvest-geoportals.md) (`lizmap`, `qwc2`, `geomapfish`, `mapbender`, `mapserver`, `qgisserver`, `mviewer`). Skip `/admin.php`, mviewerstudio, and MapFish print.

## Masterportal (`masterportal`) {#masterportal}

Hamburg LGV viewer. Harvest `config.js` / portal JSON **layer tree** (or the WMS the config points at). One theme is not automatically one dataset. Do not scrape `lgv-config` tiles.

## MapStore (`mapstore`) {#mapstore}

GeoStore `/rest/geostore/` or backend CSW. Keep catalog/dataset resources. Drop saved **maps** and the MapStore UI chrome unless the user asked for maps.

## Terria (`terria`) {#terria}

Init `catalog.json` / `config.json` **members typed as data**. If Magda or CKAN on the same host already lists those datasets, harvest CKAN/Magda instead.

## GeoBlacklight (`geoblacklight`) {#geoblacklight}

```text
GET https://host/catalog.json
```

Geospatial items. Drop books/images when the Solr mix includes them. Page `start` / `rows` as in Blacklight.

## OpenGeoPortal (`opengeoportal`) {#opengeoportal}

Search/Solr **layers**, not institutions. Legacy paths vary — use `endpoints[]`.

## Koordinates (`koordinates`) {#koordinates}

```text
GET https://host/services/api/v1.x/data/
```

Data sets, not tile URLs.

## MapTiler Server (`maptilerserver`) {#maptilerserver}

```text
GET https://host/api
```

Harvest **maps / styles** the public catalog lists. Bare `/api/maps` 404s on some versions. Skip `/admin` and `logoOnly` tile backends. Do not harvest every XYZ tile.

## MapProxy (`mapproxy`) {#mapproxy}

WMTS/WMS GetCapabilities on the cache. Treat layers as datasets **only** if no parent SDI lists them. Most MapProxy instances duplicate GeoServer/MapServer — prefer the origin catalog.

## Tianditu (`tianditu`) {#tianditu}

Provincial/municipal 天地图 nodes. Harvest the node’s **layer/catalog API** if public. Skip pure tile hosts (`t0.tianditu.gov.cn` … `t7`), JS API keys (`tk=`), and `api.tianditu.gov.cn` token calls. One node = one harvest scope.

Backends vary; use `endpoints[]` when present. Common public catalogs:

```text
GET https://host/iserver/services.json
GET https://host/iportal/web/services.json
GET https://host/arcgis/rest/services?f=pjson
GET https://host/api/cityNode/queryByTree.json
```

Keep SuperMap services, iPortal maps/services, ArcGIS Map/Feature/Image services, or the city-node tree. Drop SSO, `console.tianditu.gov.cn` developer pages, and WMTS GetTile URLs.

## VertiGIS WebOffice (`weboffice`) {#weboffice}

Map UI first. Harvest CSW/WMS/REST when public. Do not scrape city-plan tiles.

## Geocortex Essentials (`geocortex`) {#geocortex}

List sites from the Essentials REST Sites Directory (`GET .../REST/sites?f=pjson`). Keep public sites as catalog applications. Drop Html5Viewer tiles, print PDFs, and per-layer identify results. If ArcGIS REST on the same host is already harvested, do not duplicate those services.

## Cadenza (`cadenza`) {#cadenza}

Map UI first. Harvest CSW/WMS/REST when public. Do not scrape workbook tiles.

## MangoMap (`mangomap`) {#mangomap}

Public MangoMap layer/catalog list if unauthenticated. Stop on `401`. Do not scrape map tiles.

## map.apps (`mapapps`) {#mapapps}

`/mapapps/` is a viewer — follow the backend catalog (CSW/ArcGIS). Do not scrape city-plan tiles.

## Wagmap (`wagmap`) {#wagmap}

GetCapabilities often missing or `403`. Harvest only a public CSW/WMS/REST catalog. Do not scrape わが街ガイド tiles.

## EWMAPA (`ewmapa`) {#ewmapa}

Polish geoportal2.pl. Same grain as [Wagmap](#wagmap): harvest only public CSW/WMS/REST. Do not scrape tiles.

## InGrid (`ingrid`) {#ingrid}

German InGrid. CSW:

```text
GET https://host/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities
GET https://host/interface/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities
```

Keep ISO dataset/series. [harvest-protocols.md](harvest-protocols.md#csw).

## IsiGéo (`isigeo`) {#isigeo}

Geomatika SDI. Harvest `/api` if it lists layers/datasets; otherwise WMS GetCapabilities on the published workspace.

## MetaGIS (`metagis`) {#metagis}

```text
GET https://host/ResultJSONGNServlet
```

JSON layer/search results. Skip HTML search chrome.

## smart.finder SDI (`smartfindersdi`) {#smartfindersdi}

CSW or finder search. Keep ISO dataset/series metadata. Skip admin and the installer.

## MapBiomas (`mapbiomas`) {#mapbiomas}

Harvest annual land-cover **collections** on the country/program node. Do not treat every map click or year slider state as a dataset.

## CARTO (`carto`) {#carto}

Government/org Builder tenants only. Public named maps/datasets if the SQL or Maps API is unauthenticated. Stop on API keys (`401`). Do not `SELECT` every table. Skip carto.com marketing.

## SuperMap iServer (`supermapiserver`) {#supermapiserver}

```text
GET https://host/services.json
GET https://host/iserver/services.json
```

Keep published **datasets/services**. Drop tiles and admin.

## SuperMap iPortal (`supermapiportal`) {#supermapiportal}

Same `services.json` grain as [iServer](#supermapiserver) when the public product is iPortal. Drop tiles and admin.

## MapGIS IGServer (`mapgisigserver`) {#mapgisigserver}

```text
GET https://host/igs/rest/mrcs/docs?f=json
GET https://host/igs/rest/services?f=json
```

Keep published **map documents** (IGS 1.0) or **services** (IGS 2.0). Drop tiles, `/igs/manager` admin, and GetMap images. IGS 2.0 `/igs/rest/services` looks like ArcGIS REST — harvest it as MapGIS when the path is `/igs/rest/`, not `/arcgis/rest/`.

## cardo (`cardo`) {#cardo}

Public UI under `/net3/public/`; WMS if published. Skip intranet cardo. Harvest GetCapabilities **layers** when that is the catalog.

## NetGIS Server (`netgisserver`) {#netgisserver}

`/Netgis7` or `/keos/`; optional `wms.ashx` GetCapabilities. Not Sampaş or GiSoftGis.

## GC Navi (`gcnavi`) {#gcnavi}

Tenant on `geocloud.jp/webgis/`. One municipality. Often no open GetCapabilities — stop rather than scraping tiles.

## NOL-IS (`nolis`) {#nolis}

Municipal WebGIS; harvest WMS/CSW if public.

## GiSoftGis (`gisoftgis`) {#gisoftgis}

Turkish city guide (`/GiSoftGis/`). Harvest WMS/REST if public. Do not treat the Angular hash router as a dataset list.

## Sampaş WebGIS (`sampaswebgis`) {#sampaswebgis}

`/KentrehberiApp/`. Same grain as [GiSoftGis](#gisoftgis).

## PopGIS (`popgis`) {#popgis}

SPC population/census GIS. Harvest the node’s **layer / table catalog**, not every map click. One country/territory node = one scope.

## ActiveMap (`activemapgis`) {#activemapgis}

Municipal map portal. Harvest the public layer tree or GetCapabilities. Skip Gradoservice / Panorama marketing.

## GIS WebServer SE (`giswebse`) {#giswebse}

Same grain as [ActiveMap](#activemapgis): public layer tree or GetCapabilities. Skip vendor marketing.

## Geonomics (`geonomics`) {#geonomics}

Viewer / local SDI. WMS or REST if public. Do not scrape Mapbox tiles.

## ORBISMap (`orbismap`) {#orbismap}

Same grain as [Geonomics](#geonomics).

## GeoPortal.rlp (`geoportalrlp`) {#geoportalrlp}

Open-source SDI (mrmap / Rheinland-Pfalz). Harvest **CSW** or published OWS **layers**, not the map HTML. Prefer CSW when both exist ([harvest-geoportals.md](harvest-geoportals.md), [harvest-protocols.md](harvest-protocols.md#csw)).

## GeoMedia WebMap (`geomediawebmap`) {#geomediawebmap}

Geospatial Portal under `/geoportal01/`, `/cdngiportal/`, or similar. Harvest WMS/WFS GetCapabilities or the portal’s layer list. Skip Intergraph marketing.

## mf-geoadmin3 (`mfgeoadmin3`) {#mfgeoadmin3}

Swiss geoadmin3 forks. Harvest `layersConfig` JSON (or WMS the config points at). Do not scrape map.geo.admin.ch tiles. Skip swisstopo marketing if you only needed an existing registry row.

## Re:Earth (`reearth`) {#reearth}

Cesium / PLATEAU VIEW. Harvest the public **catalog / scene dataset** API (CityGML or documented REST), not every 3D tile. One project = one harvest scope.

## GIS4Smart (`gis4smart`) {#gis4smart}

Municipal viewer (Y.Ge.P.). Harvest WMS/REST **layers** if public. Often no GetCapabilities — stop rather than scraping tiles.

## BelsisIMS (`belsisims`) {#belsisims}

KRH city guide. Same grain as [GIS4Smart](#gis4smart). Not NetGIS or Sampaş.

## GP Atlas (`gpatlas`) {#gpatlas}

Regional web GIS. Harvest the public **layer / catalog** JSON or WMS. Skip login editors and vendor marketing.

## Geometa (`geometa`) {#geometa}

Same grain as [GP Atlas](#gpatlas). Skip Agate document workflows unless those **are** the catalog.

## DATUM GIS (`datumgis`) {#datumgis}

Same grain as [GP Atlas](#gpatlas).

## EverGIS (`evergis`) {#evergis}

Same grain as [GP Atlas](#gpatlas).

## Ingeo (`ingeo`) {#ingeo}

Public GISOGD / layer list if any. Skip tiles and vendor marketing.

## Farvater GIS OGD (`farvatergisogd`) {#farvatergisogd}

Same grain as [Ingeo](#ingeo).

## Related

- [harvest.md](harvest.md)
- [harvest-geoportals.md](harvest-geoportals.md)
- [harvest-earthdata.md](harvest-earthdata.md)
- [harvest-protocols.md](harvest-protocols.md)
- [harvest-identifiers.md](harvest-identifiers.md)
- [harvest-output.md](harvest-output.md)
- [discovery-geoportals.md](discovery-geoportals.md)
- [agents/harvest.md](agents/harvest.md)
