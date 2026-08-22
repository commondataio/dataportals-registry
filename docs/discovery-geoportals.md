# Discovering geoportals

How to find **geoportal** installations (`catalog_type: Geoportal`). Search-engine syntax: [discovery-search-tools.md](discovery-search-tools.md). If a site is both a map viewer and a dataset portal, pick the **primary** product ([catalog-types.md](catalog-types.md)). One public catalog UI = one registry record — see [one catalog per host](discovery.md#one-catalog-per-public-product).

Fingerprints live on two pages so this overview stays short:

| Page | Use when |
|------|----------|
| [SDI platforms](discovery-geoportals-sdi.md) | GeoNetwork, GeoNode, GeoServer, ArcGIS, STAC, openEO, Lizmap, QGIS Server, mviewer, Isogeo, Geocortex, MapServer, and other catalog/service stacks |
| [Regional viewers](discovery-geoportals-viewers.md) | Wagmap, EWMAPA, Tianditu, Masterportal, GeoMapFish, NetGIS, cardo, MapGIS IGServer, and municipal GIS viewers |

All `software.id` values: [software-index.md](software-index.md). Harvest grain (layers vs tiles): [harvest-geoportals.md](harvest-geoportals.md), [harvest-viewers.md](harvest-viewers.md).

## Core SDI (short list)

Confirm with a GET on the candidate host only. Stop on `401`/`403`.

| If you see | `software.id` | Full fingerprints |
|------------|---------------|-------------------|
| `/srv/eng/csw` or `/srv/api` | `geonetwork` | [SDI](discovery-geoportals-sdi.md#geonetwork) |
| `/api/layers/` or `/api/datasets/` | `geonode` | [SDI](discovery-geoportals-sdi.md#geonode) |
| `/geoserver/ows` GetCapabilities | `geoserver` | [SDI](discovery-geoportals-sdi.md#geoserver) |
| Hub search / `opendata.arcgis.com` | `arcgishub` | [SDI](discovery-geoportals-sdi.md#arcgishub) |
| `/arcgis/rest/info?f=pjson` | `arcgisserver` | [SDI](discovery-geoportals-sdi.md#arcgisserver) |
| STAC `/collections` JSON | `stacserver` | [SDI](discovery-geoportals-sdi.md#stacserver) |
| STAC Browser HTML only | `stacbrowser` | [SDI](discovery-geoportals-sdi.md#stacbrowser) |
| `/.well-known/openeo` | `openeo` | [SDI](discovery-geoportals-sdi.md#openeo) |
| `qgis_mapserv.fcgi` as the public catalog | `qgisserver` | [SDI](discovery-geoportals-sdi.md#qgisserver) |
| mviewer `/apps/*.xml` | `mviewer` | [SDI](discovery-geoportals-sdi.md#mviewer) |
| Isogeo OpenCatalog `/api` | `isogeo` | [SDI](discovery-geoportals-sdi.md#isogeo) |
| `/Geocortex/Essentials/REST/sites` | `geocortex` | [SDI](discovery-geoportals-sdi.md#geocortex) |
| `/igs/rest/mrcs/docs` | `mapgisigserver` | [viewers](discovery-geoportals-viewers.md#mapgisigserver) |
| `www2.wagmap.jp` | `wagmap` | [viewers](discovery-geoportals-viewers.md#wagmap) |

## Generic geospatial probes

On a **named** mapping-agency or city GIS host:

```text
/geonetwork/srv/eng/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities
/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities
/cgi-bin/mapserv?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetCapabilities
/cgi-bin/qgis_mapserv.fcgi?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetCapabilities
/.well-known/openeo
/collections
/gvsigonline/
/synserver
/Geocortex/Essentials/REST/sites?f=pjson
/Html5Viewer/
/cadenza/
/arcgis/rest/services?f=pjson
/rest/info?f=pjson
/igs/rest/mrcs/docs?f=json
/igs/rest/services?f=json
```

Google patterns: ``geoportal {agency}``, ``INSPIRE {country}``, ``IDE {country}`` (infraestructura de datos espaciales), ``GDI {land}``, ``géoportail {région}``.

Also try `/themes` (GeoMapFish), `/demo/` (MapProxy), `/net3/public/` (cardo), `/mapapps/` (map.apps), and tenant hosts `www2.wagmap.jp`, `geoportal2.pl`, `geocloud.jp`.

## Related
- [discovery-geoportals-sdi.md](discovery-geoportals-sdi.md)
- [discovery-geoportals-viewers.md](discovery-geoportals-viewers.md)

- [discovery.md](discovery.md)
- [discovery-search-tools.md](discovery-search-tools.md)
- [discovery-opendata.md](discovery-opendata.md)
- [harvest-geoportals.md](harvest-geoportals.md)
- [harvest-viewers.md](harvest-viewers.md)
- [harvest-earthdata.md](harvest-earthdata.md)
- [software-taxonomy.md](software-taxonomy.md)
