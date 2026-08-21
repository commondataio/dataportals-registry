# Harvesting map viewers and tile caches

Many geoportals in this registry are **viewers** (QWC2, Masterportal, Lizmap, Wagmap, Tianditu). The catalog of datasets is the **layer list** (GetCapabilities, `themes.json`, REST services) — not PNG tiles, print PDFs, or the basemap.

Use this page when `software.id` is a viewer or cache. Full SDI catalogs (GeoNetwork, GeoNode, ArcGIS Server): [harvest-geoportals.md](harvest-geoportals.md). Protocol grain: [harvest-protocols.md](harvest-protocols.md). GET only. Stop on `401`/`403`. Do not scrape tiles.

## Rule

1. If CSW, STAC, or ArcGIS REST exists on the same host, harvest **that** ([harvest-geoportals.md](harvest-geoportals.md)).
2. Else harvest WMS/WMTS/WFS **GetCapabilities** named layers, or the viewer’s JSON theme/layer tree.
3. One named layer (or published service) = one dataset analog. Do not ingest the same layer from WMS and WMTS.
4. Stop if GetCapabilities is `403` or missing — common for Wagmap and EWMAPA.

## Lizmap, QWC2, GeoMapFish, Mapbender, MapServer

Published project/theme **layers**. Recipes: [harvest-geoportals.md](harvest-geoportals.md). Skip `/admin.php` and MapFish print.

## Masterportal (`masterportal`)

Hamburg LGV viewer. Harvest `config.js` / portal JSON **layer tree** (or the WMS the config points at). One theme is not automatically one dataset. Do not scrape `lgv-config` tiles.

## MapStore (`mapstore`)

GeoStore `/rest/geostore/` or backend CSW. Keep catalog/dataset resources. Drop saved **maps** and the MapStore UI chrome unless the user asked for maps.

## Terria (`terria`)

Init `catalog.json` / `config.json` **members typed as data**. If Magda or CKAN on the same host already lists those datasets, harvest CKAN/Magda instead.

## GeoBlacklight (`geoblacklight`)

```text
GET https://host/catalog.json
```

Geospatial items. Drop books/images when the Solr mix includes them. Page `start` / `rows` as in Blacklight.

## OpenGeoPortal (`opengeoportal`)

Search/Solr **layers**, not institutions. Legacy paths vary — use `endpoints[]`.

## Koordinates (`koordinates`)

```text
GET https://host/services/api/v1.x/data/
```

Data sets, not tile URLs.

## MapTiler Server (`maptilerserver`)

```text
GET https://host/api
```

Harvest **maps / styles** the public catalog lists. Bare `/api/maps` 404s on some versions. Skip `/admin` and `logoOnly` tile backends. Do not harvest every XYZ tile.

## MapProxy (`mapproxy`)

WMTS/WMS GetCapabilities on the cache. Treat layers as datasets **only** if no parent SDI lists them. Most MapProxy instances duplicate GeoServer/MapServer — prefer the origin catalog.

## Tianditu (`tianditu`)

Provincial/municipal 天地图 nodes. Harvest the node’s **layer/catalog API** if public. Skip pure tile hosts and API keys. One node = one harvest scope.

## VertiGIS WebOffice (`weboffice`), Cadenza (`cadenza`), MangoMap (`mangomap`), map.apps (`mapapps`)

Map UI first. Harvest CSW/WMS/REST when public. `mapapps` `/mapapps/` is a viewer — follow the backend catalog. Do not scrape city-plan tiles.

## Wagmap (`wagmap`) and EWMAPA (`ewmapa`)

GetCapabilities often missing or `403`. Harvest only a public CSW/WMS/REST catalog. Do not scrape わが街ガイド or geoportal2.pl tiles.

## InGrid (`ingrid`)

German InGrid. CSW:

```text
GET https://host/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities
GET https://host/interface/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities
```

Keep ISO dataset/series. [harvest-protocols.md](harvest-protocols.md#csw-ogc-catalog-service).

## IsiGéo (`isigeo`)

Geomatika SDI. Harvest `/api` if it lists layers/datasets; otherwise WMS GetCapabilities on the published workspace.

## MetaGIS (`metagis`)

```text
GET https://host/ResultJSONGNServlet
```

JSON layer/search results. Skip HTML search chrome.

## smart.finder SDI (`smartfindersdi`)

CSW or finder search. Keep ISO dataset/series metadata. Skip admin and the installer.

## MapBiomas (`mapbiomas`)

Harvest annual land-cover **collections** on the country/program node. Do not treat every map click or year slider state as a dataset.

## CARTO (`carto`)

Government/org Builder tenants only. Public named maps/datasets if the SQL or Maps API is unauthenticated. Stop on API keys (`401`). Do not `SELECT` every table. Skip carto.com marketing.

## SuperMap iServer / iPortal (`supermapiserver`, `supermapiportal`)

```text
GET https://host/services.json
GET https://host/iserver/services.json
```

Keep published **datasets/services**. Drop tiles and admin.

## cardo (`cardo`), NetGIS (`netgisserver`), GC Navi (`gcnavi`), NOL-IS (`nolis`)

Viewer + optional WMS. Harvest GetCapabilities **layers** when that is the catalog. Skip admin HTML. Product-specific paths:

- **cardo** (`cardo`): public UI under `/net3/public/`; WMS if published. Skip intranet cardo.
- **NetGIS Server** (`netgisserver`): `/Netgis7` or `/keos/`; optional `wms.ashx` GetCapabilities. Not Sampaş or GiSoftGis.
- **GC Navi** (`gcnavi`): tenant on `geocloud.jp/webgis/`. One municipality. Often no open GetCapabilities — stop rather than scraping tiles.
- **NOL-IS** (`nolis`): municipal WebGIS; harvest WMS/CSW if public.

## GiSoftGis (`gisoftgis`) and Sampaş (`sampaswebgis`)

Turkish city guides (`/GiSoftGis/`, `/KentrehberiApp/`). Harvest WMS/REST if public. Do not treat the Angular hash router as a dataset list.

## PopGIS (`popgis`)

SPC population/census GIS. Harvest the node’s **layer / table catalog**, not every map click. One country/territory node = one scope.

## ActiveMap (`activemapgis`), GIS WebServer SE (`giswebse`)

Municipal map portals. Harvest the public layer tree or GetCapabilities. Skip Gradoservice / Panorama marketing.

## Geonomics (`geonomics`) and ORBISMap (`orbismap`)

Viewer / local SDI. WMS or REST if public. Do not scrape Mapbox tiles.

## GeoPortal.rlp (`geoportalrlp`)

Open-source SDI (mrmap / Rheinland-Pfalz). Harvest **CSW** or published OWS **layers**, not the map HTML. Prefer CSW when both exist ([harvest-geoportals.md](harvest-geoportals.md), [harvest-protocols.md](harvest-protocols.md#csw-ogc-catalog-service)).

## GeoMedia WebMap (`geomediawebmap`) and mf-geoadmin3 (`mfgeoadmin3`)

- **GeoMedia WebMap:** Geospatial Portal under `/geoportal01/`, `/cdngiportal/`, or similar. Harvest WMS/WFS GetCapabilities or the portal’s layer list. Skip Intergraph marketing.
- **mf-geoadmin3:** Swiss geoadmin3 forks. Harvest `layersConfig` JSON (or WMS the config points at). Do not scrape map.geo.admin.ch tiles. Skip swisstopo marketing if you only needed an existing registry row.

## Re:Earth (`reearth`)

Cesium / PLATEAU VIEW. Harvest the public **catalog / scene dataset** API (CityGML or documented REST), not every 3D tile. One project = one harvest scope.

## GIS4Smart (`gis4smart`) and BelsisIMS (`belsisims`)

Municipal viewers (Y.Ge.P. / KRH city guide). Harvest WMS/REST **layers** if public. Often no GetCapabilities — stop rather than scraping tiles. BelsisIMS is not NetGIS or Sampaş.

## GP Atlas (`gpatlas`), Geometa (`geometa`), DATUM GIS (`datumgis`), EverGIS (`evergis`)

Regional Russian / CIS web GIS. Harvest the public **layer / catalog** JSON or WMS. Skip Agate document workflows unless those **are** the catalog, login editors, and vendor marketing.

## Ingeo (`ingeo`) and Farvater GIS OGD (`farvatergisogd`)

Public GISOGD / layer list if any. Skip tiles and vendor marketing. Zero-entity software ids may still appear on scheduled or future records — use the same grain.

## Related

- [harvest.md](harvest.md)
- [harvest-geoportals.md](harvest-geoportals.md)
- [harvest-earthdata.md](harvest-earthdata.md)
- [harvest-protocols.md](harvest-protocols.md)
- [harvest-identifiers.md](harvest-identifiers.md)
- [harvest-output.md](harvest-output.md)
- [discovery-geoportals.md](discovery-geoportals.md)
- [agents/harvest.md](agents/harvest.md)
