# Discovering geoportal viewers

Regional and municipal map viewers (`catalog_type: Geoportal`). These are **viewers**: harvest the layer list, not PNG tiles ([harvest-viewers.md](harvest-viewers.md)). Overview: [discovery-geoportals.md](discovery-geoportals.md). SDI catalogs: [discovery-geoportals-sdi.md](discovery-geoportals-sdi.md).

One record per public application (config / tenant), not per layer.

## Wagmap / わが街ガイド (`wagmap`) {#wagmap}

PASCO hosted public WebGIS for Japanese prefectures and municipalities. Vendor: [pasco.co.jp](https://www.pasco.co.jp/biz/app-soft/wagamachiguide/). Tenants usually live under `www2.wagmap.jp` plus a city path, or a city custom domain loading GeoAccessJS portal assets.

**Signals:** hostname `www2.wagmap.jp`; title or branding わが街ガイド / Wagmap; GeoAccessJS; optional open-data catalog alongside the map gallery.

**Confirm:** GET the tenant URL and match Wagmap / GeoAccessJS branding. One record per public tenant, not per map layer. Skip staff-only municipal GIS that requires login for any map list.

| Tool | Query |
|------|-------|
| Google | `site:www2.wagmap.jp` |
| Google | `"わが街ガイド" OR Wagmap (オープンデータ OR 地図) site:.jp` |
| Censys | `web.names: "www2.wagmap.jp"` |
| crt.sh | `%.wagmap.jp` |

## EWMAPA (`ewmapa`) {#ewmapa}

GEOBID GIS used for Polish cadastral, utility, and municipal map publication. Vendor: [geobid.pl](https://geobid.pl/). Many public viewers are hosted on `*.geoportal2.pl`.

**Signals:** `geoportal2.pl` host; EWMAPA / GEOBID branding; municipal SIP / geoportal UI.

**Confirm:** GET the public map catalog (not a single WMS layer URL). Duplicate-check the same gmina under GeoServer or ArcGIS before adding a second record.

| Tool | Query |
|------|-------|
| Google | `site:geoportal2.pl` |
| Google | `"EWMAPA" OR "GEOBID" (geoportal OR SIP) site:.pl` |
| Censys | `web.names: "geoportal2.pl"` |
| crt.sh | `%.geoportal2.pl` |

## GeoMapFish (`geomapfish`) {#geomapfish}

Open-source WebGIS (c2cgeoportal + ngeo). Common in Swiss cantons and other European public geoportals. Site: [geomapfish.org](https://geomapfish.org).

**Signals:** `ngeo` / `gmf-` CSS classes; `/themes` JSON; WMS/WMTS theme tree; `c2cgeoportal` in HTML or JS bundles.

**Confirm:** GET `/themes` (or the documented theme API) and a public map UI. One record per public geoportal, not per theme.

| Tool | Query |
|------|-------|
| Google | `"GeoMapFish" OR c2cgeoportal (geoportail OR geoportal) -site:github.com` |
| Google | `inurl:/themes ngeo OR geomapfish` |
| Censys | `web.endpoints.http.body: "c2cgeoportal"` |
| Censys | `web.endpoints.http.body: "gmf-"` |

## Tianditu (`tianditu`) {#tianditu}

China National Geographic Information Public Service Platform (Map World). National, provincial, and municipal nodes share NGCC APIs and branding. Site: [tianditu.gov.cn](https://www.tianditu.gov.cn).

**Signals:** `tianditu` in hostname or HTML; 天地图 branding; Map World API keys / `tianditu.gov.cn` tile or widget hosts.

**Confirm:** GET the public node (province or city) and match 天地图 / Tianditu. One record per public node, not per map API key. Skip pure tile endpoints with no catalog UI.

| Tool | Query |
|------|-------|
| Google | `"天地图" (省 OR 市 OR 地理信息) -site:tianditu.gov.cn` |
| Google | `inurl:tianditu OR "Map World" 地理` |
| Censys | `web.endpoints.http.body: "tianditu"` |

## Masterportal (`masterportal`) {#masterportal}

Hamburg LGV open-source map viewer used by German federal, state, and municipal agencies. Site: [masterportal.org](https://www.masterportal.org).

**Signals:** `Masterportal` in title or footer; `lgv-config` / `config.js` portal JSON; OGC WMS/WFS/CSW theme tree.

**Confirm:** GET the viewer URL and match Masterportal config plus a public layer tree. One record per public portal instance.

| Tool | Query |
|------|-------|
| Google | `"Masterportal" (Geoportal OR Kartendienst) site:.de -site:masterportal.org` |
| Censys | `web.endpoints.http.body: "Masterportal"` |
| Censys | `web.endpoints.http.body: "lgv-config"` |

## PopGIS (`popgis`) {#popgis}

Pacific Community (SPC) population / census GIS. Site: [spc.int PopGIS](https://www.spc.int/our-work/geospatial/popgis).

**Confirm:** GET the public map/layer catalog for a country or territory node.

| Tool | Query |
|------|-------|
| Google | `"PopGIS" (census OR geospatial) (Pacific OR SPC)` |
| Censys | `web.endpoints.http.body: "PopGIS"` |

## MangoMap (`mangomap`) {#mangomap}

Hosted map galleries. Tenants on `mangomap.com`. Site: [mangomap.com](https://mangomap.com).

**Confirm:** GET the organization portal. One record per tenant, not per map.

| Tool | Query |
|------|-------|
| Google | `site:mangomap.com` |
| crt.sh | `%.mangomap.com` |

## NetGIS Server (`netgisserver`) {#netgisserver}

Netcad GIS server, common in Turkish municipalities. Product: [NetGIS Server](https://www.netcad.com/tr/urunler/netgis-server).

**Signals:** `/Netgis7`, `/keos/` city guide, title `NetGIS Server 7`.

**Confirm:** GET the KEOS viewer or `/Netgis7` title page. Optional WMS: `wms.ashx` GetCapabilities. Do not confuse with Sampaş `/KentrehberiApp/` or GiSoftGis Angular city guides.

| Tool | Query |
|------|-------|
| Google | `intitle:"NetGIS Server 7" OR inurl:/Netgis7 OR inurl:/keos/` |
| Censys | `web.endpoints.http.html_title: "NetGIS Server"` |

## cardo (`cardo`) {#cardo}

IDU IT geospatial platform (Germany and neighbours). Site: [cardogis.com](https://cardogis.com).

**Signals:** `/net3/public/`, cardo.Map, `cardo` in HTML/JS.

**Confirm:** GET the public map/catalog UI under `/net3/public/` (or the branded geoportal home). Skip intranet-only cardo installs.

| Tool | Query |
|------|-------|
| Google | `"cardo.Map" OR inurl:/net3/public/` |
| Censys | `web.endpoints.http.body: "cardo.Map"` |

## GC Navi (`gcnavi`) {#gcnavi}

Informatix GeoCloud WebGIS for Japanese local governments. Product: [GC Navi](https://www.informatix.co.jp/gc/navi/).

**Signals:** `geocloud.jp/webgis/`, GC Navi, `bt=` / `p=` query parameters.

**Confirm:** GET the tenant WebGIS home (org subdomain on `geocloud.jp`). Distinct from internal GC Planets. One record per municipality tenant.

| Tool | Query |
|------|-------|
| Google | `"GC Navi" OR inurl:geocloud.jp/webgis/` |
| Censys | `web.names: "geocloud.jp"` |
| crt.sh | `%.geocloud.jp` |

## NOL-IS (`nolis`) {#nolis}

German municipal WebGIS. Site: [nol-is.de](https://www.nol-is.de).

**Signals:** assets from `maps.nol-is.de` or `static.nol-is.de`; NOL-IS / NOLIS branding.

**Confirm:** GET the public geoportal home. Skip vendor marketing pages.

| Tool | Query |
|------|-------|
| Google | `"NOL-IS" OR "NOLIS" Geoportal site:.de` |
| Censys | `web.names: "nol-is.de"` |

## GiSoftGis (`gisoftgis`) {#gisoftgis}

Turkish municipal Angular city guide. Path `/GiSoftGis/` with hash `#/cityguidepublic`.

**Signals:** `gi-ajax-loading-indicator`; meta “Kent Rehberi Uygulaması”.

**Confirm:** GET `/GiSoftGis/`. Distinct from NetGIS `/keos/` and Sampaş `/KentrehberiApp/`.

| Tool | Query |
|------|-------|
| Google | `inurl:/GiSoftGis/` |
| Censys | `web.endpoints.http.body: "GiSoftGis"` |

## Sampaş WebGIS (`sampaswebgis`) {#sampaswebgis}

AKOS municipal city-guide map. Typical path `/KentrehberiApp/Index`.

**Confirm:** GET that path; page title contains `SAMPAŞ WEBGIS`.

| Tool | Query |
|------|-------|
| Google | `"SAMPAŞ WEBGIS" OR inurl:/KentrehberiApp/` |
| Censys | `web.endpoints.http.html_title: "SAMPA"` |

## ActiveMap GIS (`activemapgis`) {#activemapgis}

Gradoservice municipal GIS (often Russian cities). Product: [ActiveMap](https://gradoservice.ru/products/activemap/).

**Confirm:** GET the public map portal home. Skip desktop-only marketing.

| Tool | Query |
|------|-------|
| Google | `"ActiveMap" GIS (портал OR Gradoservice)` |
| Censys | `web.endpoints.http.body: "ActiveMap"` |

## map.apps (`mapapps`) {#mapapps}

con terra WebGIS framework. Product: [map.apps](https://www.conterra.de/portfolio/mapapps). Often paired with smart.finder SDI (`smartfindersdi`).

**Signals:** `/mapapps/`; con terra / map.apps in HTML.

**Confirm:** GET the public `/mapapps/` viewer (not a login-only intranet). If smart.finder is the catalog UI, prefer `smartfindersdi` for that catalog.

| Tool | Query |
|------|-------|
| Google | `inurl:/mapapps/ (Geoportal OR "map.apps")` |
| Censys | `web.endpoints.http.body: "/mapapps/"` |

## CoGIS (`cogis`) {#cogis}

Data East geoportal stack. Site: [cogis.dataeast.com](https://cogis.dataeast.com). Map services may be CoGIS Server, eLiteGIS (`elitegis`), or ArcGIS Server — register the **public catalog UI**.

**Confirm:** GET CoGIS Portal home. Prefer `elitegis` only when that is the branded viewer with no CoGIS Portal.

| Tool | Query |
|------|-------|
| Google | `"CoGIS" (портал OR Portal OR geoportal) -site:dataeast.com` |
| Censys | `web.endpoints.http.body: "CoGIS"` |

## OpenGeoPortal (`opengeoportal`) {#opengeoportal}

Federated academic geoportal (Tufts and partners).

**Confirm:** GET the search/home UI that lists layers across institutions. Do not add a single layer preview URL.

| Tool | Query |
|------|-------|
| Google | `"OpenGeoPortal" OR "Open Geoportal" (layers OR geodata)` |
| Censys | `web.endpoints.http.body: "OpenGeoPortal"` |

## smart.finder SDI (`smartfindersdi`) {#smartfindersdi}

con terra metadata/search portal. Product: [smart.finder SDI](https://www.conterra.de/portfolio/smartfinder-sdi). Often sits next to `mapapps`.

**Confirm:** GET the public catalog search (CSW or finder UI). If only `/mapapps/` is public, use `mapapps`.

| Tool | Query |
|------|-------|
| Google | `"smart.finder SDI" OR "smart.finder" Geoportal site:.de` |
| Censys | `web.endpoints.http.body: "smart.finder"` |

## GIS WebServer SE (`giswebse`) {#giswebse}

KB Panorama web GIS. Site: [gisweb.ru](https://www.gisweb.ru).

**Confirm:** GET the public geoportal (layer tree / map). Skip desktop GIS marketing.

| Tool | Query |
|------|-------|
| Google | `"GIS WebServer SE" (геопортал OR geoportal)` |
| Censys | `web.endpoints.http.body: "GIS WebServer SE"` |

## MapGIS IGServer (`mapgisigserver`) {#mapgisigserver}

Zondy Cyber GIS server, common in Chinese government and natural-resources SDIs. Product: [MapGIS IGServer](https://www.mapgis.com/index.php?a=shows&catid=310&id=331). .NET installs often listen on **6163**; Java on **8089**.

**Signals:** `/igs/rest/` in the URL or HTML; title or footer “MapGIS IGServer”; IGS 1.0 `/igs/rest/mrcs/docs`, IGS 2.0 `/igs/rest/services`.

**Confirm:** GET `https://host/igs/rest/mrcs/docs?f=json` (IGS 1.0 map-document list) or `https://host/igs/rest/services?f=json` (IGS 2.0 service catalog). Register the public `/igs` root (or the node that exposes that REST), not `/igs/manager` admin. Skip MapGIS Desktop marketing.

**False positives:** hostnames containing `mapgis` that are actually ArcGIS Server (`/arcgis/rest/services`, e.g. some South Asian `mapgis.*` sites). IGS 2.0 REST resembles ArcGIS REST — still `mapgisigserver` when the path is `/igs/rest/`, not `/arcgis/rest/`. Do not also register a second ArcGIS Server record on the same IGServer host.

| Tool | Query |
|------|-------|
| Google | `"MapGIS IGServer" OR inurl:/igs/rest/mrcs/docs -site:mapgis.com -site:github.com` |
| Google | `inurl:/igs/rest/services "MapGIS"` |
| Censys | `web.endpoints.http.body: "/igs/rest/mrcs"` |
| FOFA | `body="/igs/rest/" && title="MapGIS"` |

## Other geoportal platforms

Search the product title with the country TLD. One record per public catalog UI.

| `software.id` | Signals / confirm | Typical query |
|---------------|-------------------|---------------|
| `gcnavi` | see above | |
| `nolis` | see above | |
| `cardo` | see above | |
| `netgisserver` | see above | |
| `sampaswebgis` | see above | |
| `gisoftgis` | see above | |
| `activemapgis` | see above | |
| `mapapps` | see above | |
| `belsisims` | `ims.*/Projects/*/Pages/KRH.aspx` | `KRH.aspx Belsis` |
| `orbismap` | ORBISMap Russian GIS | `"ORBISMap" геопортал` |
| `opengeoportal` | see above | |
| `geonomics` | Vue/Mapbox, geonomix.kz | `"Geonomics" OR geonomix геопортал` |
| `cogis` | see above | |
| `elitegis` | ArcGIS-compatible REST (Atemiko) | `"eLiteGIS" OR elitegis REST` |
| `smartfindersdi` | see above | |
| `giswebse` | see above | |
| `ingrid` | German InGrid CSW/OpenSearch | `"InGrid" (CSW OR Geoportal) site:.de` |
| `metagis` | MetaGIS (SE) | `"MetaGIS" geoportal site:.se` |
| `isigeo` | IsiGéo / Geomatika (not Isogeo SaaS) | `"IsiGéo" OR Isigeo géoportail` |
| `isogeo` | see above | |
| `mviewer` | see above | |
| `qgisserver` | see above | |
| `openeo` | see above | |
| `gis4smart` | GIS4Smart municipal | `"GIS4Smart" geoportal` |
| `geoportalrlp` | Rhineland-Palatinate stack | `geoportal.rlp.de` (do not re-add known nodes) |
| `copernicusdhus` | Copernicus DHuS | `"DHuS" Copernicus (catalogue OR odata)` |
| `popgis` | see above | |
| `ncwms` | see above | |
| `mangomap` | see above | |
| `opendatacube` | see above | |
| `datacubews` | ODC OWS WMS/WCS | `"datacube-ows" OR "datacube_ows"` |
| `supermapiserver` | SuperMap iServer REST | `"SuperMap iServer" rest` |
| `supermapiportal` | SuperMap iPortal | `"SuperMap iPortal"` |
| `mapgisigserver` | see above | |
| `reearth` | Re:Earth / PLATEAU VIEW | `"Re:Earth" OR "PLATEAU VIEW"` |
| `gpatlas` | GP Atlas | `"GP Atlas" GIS` |
| `geometa` | GeoMeta catalog | `"GeoMeta" geoportal` |
| `carto` | CARTO Builder / cloud maps | `site:carto.com` government tenants only |
| `mfgeoadmin3` | swisstopo geoadmin3 forks | `"geoadmin3" OR mf-geoadmin3` |
| `datumgis` | DATUM GIS | `"DATUM GIS" геопортал` |
| `evergis` | EverGIS / ЭверГИС | `"EverGIS" OR "ЭверГИС"` |
| `ingeo` | InGeo / ГИС ИнГео | `"ИнГео" GIS` |
| `farvatergisogd` | Farvater GIS OGD | `"Farvater" ГИСОГД` |

