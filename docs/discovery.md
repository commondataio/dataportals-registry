# Discovering data catalogs

Three different jobs:

| Job | What you want | Where to go |
|-----|----------------|-------------|
| Find catalogs **already in this registry** | Filter by country, type, software, or URL | [query-examples.md](query-examples.md), [agents/query.md](agents/query.md) |
| Find catalogs **not yet registered** | New portals, geoportals, repositories | This page, then [CONTRIBUTING.md](https://github.com/datenoio/dataportals-registry/blob/main/CONTRIBUTING.md) |
| List **datasets inside** a registered catalog | Remote catalog APIs, with type filters | [harvest.md](harvest.md) |

This page is for the second job: locating real catalog installations in the wild, checking they are not duplicates, and preparing them for a pull request. Coding agents should follow the shorter checklist in [agents/discover.md](agents/discover.md).

The registry records **catalogs** (portals, geoportals, repositories, and similar infrastructure). It does not store the datasets inside those catalogs. To crawl those datasets, see [harvest.md](harvest.md).

## Guides

| Guide | Use when |
|-------|----------|
| [Search engines and internet maps](discovery-search-tools.md) | Google, Censys, Shodan, FOFA, URLScan, crt.sh, and similar tools |
| [Agents, Cursor, ChatGPT](discovery-agent-tools.md) | Configure MCP, APIs, Custom GPTs, and LLM clients to use those tools |
| [Open data portals](discovery-opendata.md) | CKAN, DKAN, OpenDataSoft, Socrata, uData, Magda, JKAN, Junar, EntryScape, ArcGIS Hub, Idra, Liferay, POMOSAM, oPortal, OGD India, data eye, Piveau, Our Open Data, DataPress |
| [Geoportals](discovery-geoportals.md) | Overview; SDI stacks: [discovery-geoportals-sdi.md](discovery-geoportals-sdi.md); viewers: [discovery-geoportals-viewers.md](discovery-geoportals-viewers.md) |
| [Scientific repositories](discovery-scientific.md) | Institutional IRs; domain repos: [discovery-scientific-domain.md](discovery-scientific-domain.md) |
| [Metadata catalogs](discovery-metadata.md) | FAIR Data Point, Aristotle MDR, Fusion Registry, Metadata Browser |
| [Indicators and microdata](discovery-indicators.md) | PxWeb, OpenSDG, .Stat Suite, Knoema, SDMX-RI, GENESIS-Online, IBIS-PH, DHIS2, NADA, NESSTAR, REDATAM, Colectica, OBiBa Mica, IPUMS |
| [Search, ML, API, marketplaces](discovery-other.md) | Data search engines (Idra, OpenAIRE), ML catalogs, API directories, data marketplaces |

## Before you search

1. Confirm the candidate is a catalog: it lists or serves datasets, maps, indicators, or metadata — not a news site, a single spreadsheet, or a login-only intranet.
2. Search the registry first. Duplicate `link` values fail quality checks (`DUPLICATE_LINK` / `DUPLICATE_LINK_NORMALIZED`).
3. Prefer `data/scheduled/` for unverified finds. Promote later; see [scheduled.md](scheduled.md).

Duplicate check against the DuckDB export:

```sql
SELECT id, name, link, catalog_type, status
FROM catalogs
WHERE lower(link) LIKE '%example.gov%'
   OR id = 'examplegov';
```

Also search `data/scheduled/` if that directory is not empty. Do not walk every YAML file under `data/entities/` unless you are editing a specific record.

## Where catalogs usually live

- National open-data sites: `/data`, `/opendata`, `/datasets`, `data.gov.*`, `datos.*`, `donnees.*`
- Statistics offices, mapping agencies, environmental agencies, and universities
- Local government: city/region sites, `opendata.` subdomains, ArcGIS Hub sites
- Research: institutional repositories, Dataverse, DSpace, GBIF IPT, re3data-listed repos

Search with the local language (`datos abiertos`, `données ouvertes`, `offene daten`, `dados abertos`, `开放数据`) plus the country or city name. Restrict with `site:.gov`, `site:.gob.*`, or the national government TLD. Operators, Censys/Shodan queries, and other indexes: [discovery-search-tools.md](discovery-search-tools.md). Per-platform queries: the guides above.

## Existing lists (start here) {#existing-lists-start-here}

Many platforms publish installation galleries. Cross-check each URL against the registry before adding it. The README [data sources](https://github.com/datenoio/dataportals-registry/blob/main/README.md#data-sources) list is the full inventory; high-yield sources:

| Source | Typical catalogs |
|--------|------------------|
| [CKAN ecosystem](https://ecosystem.ckan.org/dataset/ckan-sites-metadata) | CKAN open-data portals (automated: `scripts/sync_ckan_ecosystem.py`) |
| [Datashades](https://datashades.info/) | CKAN and other portals |
| [data.europa.eu catalogues](https://data.europa.eu/data/catalogues) | EU and member-state catalogs |
| [GeoNetwork gallery](https://github.com/geonetwork/doc/blob/develop/source/annexes/gallery/gallery-urls.csv) | GeoNetwork geoportals |
| [INSPIRE geoportal](https://inspire-geoportal.ec.europa.eu/) | European spatial catalogs |
| [re3data](https://www.re3data.org/) | Research data repositories |
| [Dataverse installations](https://iqss.github.io/dataverse-installations/data/data.json) | Dataverse |
| [STAC Index](https://stacindex.org/catalogs) | STAC catalogs |
| [Open Data Inception](https://data.opendatasoft.com/explore/dataset/open-data-sources%40public/information/) | Open-data portals |
| [OpenSDG community](https://open-sdg.org/community) | SDG indicator sites |
| [GBIF IPT](https://www.gbif.org/ipt) | Biodiversity IPT nodes |
| [ArcGIS Hub](https://hub.arcgis.com/) | ArcGIS Hub sites |
| [EntryScape customers](https://entryscape.com/en/customers/) | EntryScape catalogs |
| [FAIR Data Point index](https://home.fairdatapoint.org) | FAIR Data Point metadata catalogs |
| [KOBV OPUS 4 references](https://www.kobv.de/entwicklung/software/opus-4/referenzen/) | OPUS institutional repositories |
| [About RADAR](https://radar.products.fiz-karlsruhe.de/en/radarabout/ueber-radar) | RADAR Cloud and RADAR Local research data repositories |
| [DHIS2](https://dhis2.org/) | National HMIS / public health indicator portals |
| [Yoda](https://www.uu.nl/en/research/yoda) | Dutch university research-data vaults |
| [OpenAIRE CONNECT](https://connect.openaire.eu/) | National and community research gateways |
| [IPUMS](https://www.ipums.org) | Harmonized census and survey microdata collections |
| [Symbiota portals](https://symbiota.org/symbiota-portals/) | Biodiversity collection CMS portals |
| [openEO](https://openeo.org) | EO cloud-processing backends |
| [Breedbase](https://breedbase.org) | Crop breeding information systems |
| [ROAR](http://roar.eprints.org) | Open-access repositories |

Vendor “customers” and “community” pages are useful but noisy: skip demos, marketing sites, and expired domains.

## Identify the software {#identify-the-software}

Choose `software.id` from `data/software/` (or `custom` if unknown). See [software-taxonomy.md](software-taxonomy.md) and [catalog-types.md](catalog-types.md).

| Platform | Typical signals | Probe (GET, timeout, public only) |
|----------|-----------------|-----------------------------------|
| CKAN | `/api/3/action/package_list`, “Powered by CKAN” | `https://host/api/3/action/status_show` |
| DKAN | CKAN-compatible API plus `/api/1/search` | same as CKAN, plus `/api/1/search` |
| Socrata | `/api/views`, `*.socrata.com` or SODA | `https://host/api/views.json?limit=1` |
| OpenDataSoft | `/api/explore/v2.1/catalog/datasets` | that path |
| GeoNetwork | `/srv/api`, `/srv/eng/csw` | CSW `GetCapabilities` |
| GeoNode | `/api/layers/`, `/api/datasets/` | `/api/layers/` |
| GeoServer | `/geoserver/ows`, `/geoserver/rest` | WMS `GetCapabilities` |
| MapTiler Server | Title `MapTiler Server`, `/admin`, port 3650 | `/api/maps/{id}/style.json` (e.g. `streets`) |
| MapServer | `/cgi-bin/mapserv`, WMS GetCapabilities mentions MapServer | WMS `GetCapabilities` |
| QGIS Server | `qgis_mapserv.fcgi`, GetCapabilities mentions QGIS Server | WMS `GetCapabilities` |
| mviewer | `mviewer` JS, `/apps/*.xml` config | Viewer URL plus public layer/theme config |
| Isogeo | OpenCatalog `/api` OpenAPI (not IsiGéo) | `/api` or public OpenCatalog home |
| openEO | landing `api_version` + `/collections` + `/processes` | `/.well-known/openeo` or API root |
| Mapbender | `/application/`, Mapbender viewer | HTML or title mentions Mapbender |
| gvSIG Online | `/gvsigonline/`, `select_public_project` | Page title or footer `gvSIG Online` |
| deegree | `/deegree-webservices`, CSW/WMS XML mentions deegree | OGC `GetCapabilities` |
| NetGIS Server | `/keos/`, `/Netgis7` | Page title `NetGIS Server 7`; optional `wms.ashx` GetCapabilities |
| Sampaş WebGIS | `/KentrehberiApp/Index` | Page title contains `SAMPAŞ WEBGIS` |
| GiSoftGis | `/GiSoftGis/#/cityguidepublic` | Angular SPA; `gi-ajax-loading-indicator`; meta “Kent Rehberi Uygulaması” |
| cardo | `/net3/public/`, cardo.Map | Public map/catalog under `/net3/public/` |
| GC Navi | `geocloud.jp/webgis/` | Tenant WebGIS home |
| NOL-IS | `maps.nol-is.de` / `static.nol-is.de` | Public municipal geoportal |
| map.apps | `/mapapps/` | con terra viewer; catalog UI may be `smartfindersdi` |
| CoGIS | CoGIS Portal | Portal home; `elitegis` only if that is the branded viewer |
| OpenGeoPortal | federated layer search | Geoportal home, not a single layer |
| Knoema | `*.knoema.com` or branded hub | Portal home only — not every dataset URL |
| SDMX-RI | `NSIWebService` / NSIStdV20Service | Public NSI/SDMX catalog |
| GENESIS-Online | `/genesis/online` | Table catalog (POST-heavy API) |
| IBIS-PH | `/ibisph-view/`, IBIS-Q | Public indicator home |
| DHIS2 | `/api/system/info`, `/dhis-web-commons/` | JSON `version` from system info; skip login-only HMIS |
| IPUMS | `*.ipums.org`, extract UI | Collection home (USA, International, CPS, …), not a single extract |
| OpenAIRE | `explore.openaire.eu`, `*.openaire.eu` CONNECT gateway | EXPLORE or national/community gateway home, not a single research product |
| Yoda | `yoda.`, public landing + DataCite | Institutional public dataset landing, not the login vault |
| Our Open Data | `/assets/cms/public.css` | Catalog home, not a numeric dataset page |
| DataPress | CKAN API plus DataPress chrome | Prefer `datapress` over `ckan` when branded |
| Haplo | Haplo research repository | Public outputs/data catalog |
| FAIRDOM-SEEK | FAIRDOMHub / WorkflowHub | Investigations/catalog home |
| RAMADDA | RAMADDA repository UI | Folder/catalog entry |
| ICAT | facility data catalog | Public search or documented REST/OAI |
| BelsisIMS KRH | `ims.*/Projects/*/Pages/KRH.aspx` | ASP.NET KRH city-guide; do not confuse with Netcad Netigma |
| VertiGIS WebOffice | `/synserver`, `/WebOffice/synserver`, `wo-hosting.vertigis.com`, `map.geoportal.at` | Page title `VertiGIS WebOffice`; `weboffice_packed.css`; core/flex/mobile clients |
| Geocortex Essentials | `/Geocortex/Essentials/REST/sites`, `/Html5Viewer/`, `*.geocortex.com` | Title `Geocortex Essentials Sites Directory` or `Geocortex Viewer for HTML5`; licensed Geocortex footer |
| GeoMedia WebMap / Geospatial Portal | `/geoportal01/`, `/cdngiportal/`, `/msip/Full.aspx`, `/Online_Mapping/` | `Version:` + `Licensed to:`; `Intergraph.WebSolutions` / `$GP.`; title may be Geospatial Portal or GeoMedia WebMap Publisher Portal |
| disy Cadenza | `/cadenza/`, `/public/`, `/pages/map/`, `/fachauswertungweb/` | `cadenza`/`disy` in HTML; Cadenza Web or Workbooks UI; guest login plus theme/workbook navigator |
| ArcGIS Server | `/rest/services`, `/arcgis/rest/services` | `/rest/info?f=pjson` |
| ArcGIS Hub | `/api/search/v1`, portal sharing REST | `/api/search/v1` |
| Idra | `/IdraPortal/`, `/Idra/api/v1/` | Federation UI or REST JSON; usually `catalog_type: Data search engine` |
| FAIR Data Point | RDF DCAT at `/`, `fdp-client`, `/swagger-ui` | GET with `Accept: text/turtle` or `application/ld+json` |
| Aristotle MDR | Aristotle registry of data elements / vocabularies | Public registry home or `/api/v4/` |
| Fusion Registry | SDMX structural metadata, Fusion Registry branding | SDMX REST catalog |
| Metadata Browser | MetadataWorks catalog UI | Public browser home |
| Wagmap / わが街ガイド | `www2.wagmap.jp`, GeoAccessJS | Tenant map gallery |
| EWMAPA | `*.geoportal2.pl`, GEOBID | Public municipal SIP |
| GeoMapFish | `ngeo` / `gmf-`, `/themes` JSON | Theme API plus map UI |
| Tianditu | 天地图 / `tianditu` | Public province or city node |
| MapGIS IGServer | `/igs/rest/mrcs/docs`, `/igs/rest/services` | IGS REST catalog; not ArcGIS on a `mapgis.*` host |
| Masterportal | Masterportal config / LGV viewer | Public layer tree |
| WIS2 Box | `wis2box`, pygeoapi | OGC API or discovery UI |
| Liferay (open data only) | RISP / datos abiertos module | Dataset listing, not a CMS homepage |
| oPortal | `/oportal/` | Government catalog home |
| OGD Platform India | `data.gov.in` tenant | Ministry/state catalog |
| Elsevier Digital Commons | bepress / Digital Commons | IR root + OAI |
| InstDB | FairStack / InstDB | Institutional node home |
| WEKO3 | WEKO3 IR | Repository root |
| OBiBa Mica | Mica / OBiBa study catalog | Public `/ws/` or study search |
| Dataverse | `/api/dataverses`, `/api/info/version` | `/api/info/version` |
| DSpace | `/oai/request?verb=Identify`, `/handle/` | OAI-PMH `Identify` |
| Invenio / Zenodo-like | `/api/records` | `/api/records?size=1` |
| OPUS | `/oai?verb=Identify`, OPUS 4 UI | repository-root `/oai?verb=Identify` |
| RADAR | `/radar/de/home`, `/oai/OAIHandler`, `/radar/api/datasets` | OAI `Identify` and datasets JSON with `totalHits` |
| Symbiota | `/collections/datasets/rsshandler.php`, “Powered by Symbiota” | Collection search and/or dataset RSS |
| CONTENTdm | `*.contentdm.oclc.org`, `/digital/api/collections` | that path and/or `/oai/oai.php?verb=Identify` |
| Omeka S | `/api` JSON-LD, `/api/items` | `/api/items` |
| Fedora Repository | `/fcrepo/rest` or `/rest` | Fedora version headers; prefer Hyrax/Islandora/PHAIDRA if that is the public UI |
| PHAIDRA | `/api/oai`, `/api/search/select` | OAI `Identify` |
| Esploro | `*.esploro.exlibrisgroup.com` or campus `/esploro` | Institutional research-outputs portal |
| PxWeb | `/api/v1/` | `/api/v1/` |
| NADA | `/index.php/api` or microdata UI | site home + API path |

Browser checks that help without an API:

- Page footer (“Powered by …”)
- HTML generator meta tags
- Network tab: calls to `/api/3`, `/srv/api`, `/rest/services`
- `robots.txt` and `/sitemap.xml` sometimes list API paths

Cross-check at least two signals before setting `software.id`. If nothing matches, use `custom` rather than guessing.

## URL patterns worth trying

Only request public URLs. Use a short timeout. Stop on `401`/`403` — do not attempt to bypass authentication.

**Open data**

- `/api/3`, `/api/3/action/package_search`
- `/data.json`, `/catalog.json`, `/catalog.xml` (DCAT)
- `/api/explore/v2.1/catalog/datasets` (OpenDataSoft)
- `/IdraPortal/` and `/Idra/api/v1/` (Idra)
- `/oportal/` (Inspur oPortal)
- `/openinf/` (Seoul Open Data Plaza)
- `/assets/cms/public.css` (Our Open Data)

**Geospatial**

- GeoNetwork: `/geonetwork/srv/eng/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities`
- GeoServer: `/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities`
- MapServer: `/cgi-bin/mapserv?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetCapabilities`
- QGIS Server: `/cgi-bin/qgis_mapserv.fcgi?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetCapabilities`
- mviewer: `/apps/` config XML (often `default.xml`)
- Isogeo: `/api` OpenAPI on an OpenCatalog host
- openEO: `/.well-known/openeo` or `/openeo/` landing JSON
- gvSIG Online: `/gvsigonline/`
- NetGIS Server: `/Netgis7` (version title) and `/keos/` (public KEOS viewer)
- Sampaş WebGIS: `/KentrehberiApp/Index` (title `SAMPAŞ WEBGIS`)
- GiSoftGis: `/GiSoftGis/` (city-guide hash `#/cityguidepublic`)
- BelsisIMS: `/Projects/.../Pages/KRH.aspx` on an `ims.` host
- cardo: `/net3/public/`
- GC Navi: tenant on `geocloud.jp/webgis/`
- map.apps: `/mapapps/`
- VertiGIS WebOffice: `/synserver` or `/WebOffice/synserver` (title `VertiGIS WebOffice`)
- Geocortex Essentials: `/Geocortex/Essentials/REST/sites?f=pjson` or `/Html5Viewer/` (title `Geocortex Essentials Sites Directory` / `Geocortex Viewer for HTML5`)
- disy Cadenza: `/cadenza/` or `/pages/map/default/index.xhtml` (Cadenza Web / Workbooks)
- ArcGIS: `/arcgis/rest/services?f=pjson`
- GeoMapFish: `/themes`
- MapGIS IGServer: `/igs/rest/mrcs/docs?f=json` or `/igs/rest/services?f=json`
- Wagmap: `https://www2.wagmap.jp/` plus tenant path
- WIS2 Box / pygeoapi: OGC API landing page

**Scientific and metadata**

- OAI-PMH: `/oai/request?verb=Identify` or `/oai?verb=Identify`
- Dataverse: `/api/info/version`
- RADAR: `/radar/api/datasets` and `/oai/OAIHandler?verb=Identify`
- Symbiota: `/collections/datasets/rsshandler.php`
- Breedbase: `/brapi/v2/serverinfo`
- ESGF: `/esg-search/search`
- CONTENTdm: `/digital/api/collections`
- Omeka S: `/api/items`
- FAIR Data Point: catalog root with `Accept: text/turtle`
- Digital Commons: repository root or `/do/oai/`
- Fusion Registry: SDMX REST catalog
- Knoema: branded hub home (not a dataset page)

**Generic**

- `/api`, `/api/docs`, `/swagger.json`, `/openapi.json`

After the record exists, `scripts/apidetect.py` can fill `endpoints[]` for known platforms:

```bash
python scripts/apidetect.py detect-single catalogdatagov --dryrun
python scripts/apidetect.py detect-software ckan --dryrun
```

Drop `--dryrun` only when you intend to write YAML. Prefer `--action insert` so existing endpoints are kept. Full command list: [apidetect.md](apidetect.md). URL reachability of `link` is a separate report: [liveness.md](liveness.md).

## Automated helpers in this repository

| Tool | Use |
|------|-----|
| `python scripts/sync_ckan_ecosystem.py --dry-run` | CKAN sites from ecosystem.ckan.org; then sync without `--dry-run` into `data/scheduled/` |
| `python scripts/builder.py add-single URL --scheduled` | Create one YAML from a verified URL |
| `python scripts/apidetect.py detect-single catalogdatagov` | Probe known API paths on an existing record ([apidetect.md](apidetect.md)) |
| `python scripts/check_liveness.py --sample 10` | Probe `link` reachability ([liveness.md](liveness.md)) |
| `python scripts/re3data_enrichment.py enrich --dry-run` | Fill `_re3data` when a re3data identifier is present |

CKAN sync details: [ckan-sync.md](ckan-sync.md). Re3Data: [re3data.md](re3data.md).

Do not write internet-wide scanners in this repository. Vendor/government lists, documented search-engine queries, and targeted GETs against candidate hosts are enough. How to query Google, Censys, and similar indexes: [discovery-search-tools.md](discovery-search-tools.md).

## One catalog per public product {#one-catalog-per-public-product}

The same hostname often runs several GIS products. Register **one YAML per public catalog UI**, not per backend.

| You see | Keep | Do not also add |
|---------|------|-----------------|
| GeoNetwork (or CSW) + GeoServer `/geoserver` | The **catalog** (GeoNetwork / CSW) | A second GeoServer record for the same layer list |
| Lizmap, QWC2, or mviewer + QGIS Server / MapServer | The **viewer** (`lizmap` / `qwc2` / `mviewer`) | `qgisserver` or `mapserver` on that host |
| ArcGIS Hub + ArcGIS Server REST on the same org | Hub if it is the public catalog; Server if REST is the product | Both for the same layer set |
| STAC API + STAC Browser | `stacserver` when the API is public | `stacbrowser` on the same origin |
| openEO + STAC on one API | `openeo` | A second `stacserver` row |
| MapGIS `/igs/rest/` on a host named `mapgis.*` | `mapgisigserver` | `arcgisserver` unless the path is `/arcgis/rest/` |
| GET SDI / GeoMapFish / CoGIS wrapping GeoServer | The **portal** software | Bundled GeoServer |

Duplicate-check `link` **and** the service origin before `add-single`. Harvest follows the same grain: [harvest-geoportals.md](harvest-geoportals.md#one-catalog-per-host).

## Verify before adding

1. Open the homepage. Confirm it is a catalog (search, dataset list, map layers, or metadata records).
2. Confirm `catalog_type` from [catalog-types.md](catalog-types.md). If a site is both a map viewer and an open-data portal, pick the primary product.
3. Confirm country and, for regional/local owners, the subregion folder (`US-CA/`, `FR-IDF/`, …).
4. Set `status: active` only if the site responds. Use `inactive` for dead sites you still want to record; use `scheduled` for unverified finds.
5. Record working `endpoints` when you have them. Leave `api: false` if you did not find a public API.
6. Add the record with `add-single` (recommended) or a YAML file whose filename matches `id`. Then follow [agents/contribute.md](agents/contribute.md) / CONTRIBUTING.md: `assign`, `validate-yaml --id {id}`.

## Conduct

- Respect `robots.txt` and site terms. Public catalog metadata only.
- Space out requests (about one to two seconds between hosts is enough for manual work).
- Do not collect personal data, credentials, or non-public APIs.
- Do not treat a single CSV download page as a catalog unless it is clearly a catalog UI or harvestable endpoint.

## Related

- [discovery-search-tools.md](discovery-search-tools.md) — Google, Censys, Shodan, FOFA, URLScan, crt.sh
- [discovery-agent-tools.md](discovery-agent-tools.md) — Cursor, ChatGPT, Claude, MCP, and API setup
- [discovery-opendata.md](discovery-opendata.md) / [discovery-geoportals.md](discovery-geoportals.md) ([SDI](discovery-geoportals-sdi.md), [viewers](discovery-geoportals-viewers.md)) / [discovery-scientific.md](discovery-scientific.md) ([domain](discovery-scientific-domain.md)) / [discovery-metadata.md](discovery-metadata.md) / [discovery-indicators.md](discovery-indicators.md) / [discovery-other.md](discovery-other.md)
- [software-index.md](software-index.md) — every `software.id` → recipe
- [apidetect.md](apidetect.md) / [liveness.md](liveness.md)
- [agents/discover.md](agents/discover.md) — agent checklist
- [agents/contribute.md](agents/contribute.md) — write YAML after a find
- [catalog-types.md](catalog-types.md)
- [software-taxonomy.md](software-taxonomy.md)
- [harvest.md](harvest.md) — datasets *inside* a registered catalog (not this discovery job)
- [cli.md](cli.md)
