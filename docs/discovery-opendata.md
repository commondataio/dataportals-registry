# Discovering open data portals

How to find **open data portal** installations (`catalog_type: Open data portal`) that are not yet in this registry. Search-engine syntax: [discovery-search-tools.md](discovery-search-tools.md). Overview and accept/reject rules: [discovery.md](discovery.md). Also covered here: Idra (`idra`), a DCAT-AP federation layer that is usually typed as a **Data search engine**; Piveau, Our Open Data, Gipuzkoa Irekia, DataPress, and Taiwan MODA.

Set `software.id` from `data/software/` only when a probe or page signal matches. Otherwise `custom`. After YAML exists: `python scripts/apidetect.py detect-single {id} --dryrun` (replace `{id}` with the catalog id).

## CKAN (`ckan`)

Most common self-hosted open-data CMS. Gallery: [CKAN ecosystem](https://ecosystem.ckan.org/dataset/ckan-sites-metadata) (automated: `python scripts/sync_ckan_ecosystem.py --dry-run`) and [Datashades](https://datashades.info/).

**Signals:** footer “Powered by CKAN”; `/dataset` or `/dataset/` listing; HTML includes `ckan.js` or `ckanext-`; cookie `ckan_`.

**Confirm (GET):** `https://host/api/3/action/status_show` and/or `/api/3/action/package_list`. JSON with `"success": true` is enough.

| Tool | Query |
|------|-------|
| Google | `"Powered by CKAN" inurl:/dataset -site:github.com -site:ckan.org` |
| Google | `inurl:/api/3/action/status_show` |
| Google | `"CKAN" "open data" site:.gov` |
| Censys (web) | `web.endpoints.http.body: "Powered by CKAN"` |
| Censys (web) | `web.endpoints.http.html_title: "CKAN"` |
| Shodan | `http.html:"Powered by CKAN"` |
| PublicWWW | `"Powered by CKAN"` or `"ckan.js"` |

**False positives:** ckan.org, docs, GitHub, demo.ckan.org, CKAN extensions that are not a portal, harvest *sources* listed inside another CKAN. Prefer the catalog homepage, not `/dataset/{slug}`.

**Paths:** `/dataset`, `/organization`, `/api/3`, `/data.json`, `/catalog.xml`. Some installs live under `/data` or `/opendata` — probe `https://host/data/api/3/action/status_show` as well.

## DKAN (`dkan`)

Drupal-based portal with a CKAN-compatible Action API plus DKAN’s own `/api/1/` routes. Community: [getdkan.org/community](https://getdkan.org/community).

**Confirm:** CKAN-style `/api/3/action/package_search` **and** `/api/1/search` or `/api/1/metastore`. Often `/data.json` (DCAT-US).

| Tool | Query |
|------|-------|
| Google | `"powered by DKAN" OR inurl:/api/1/metastore` |
| Google | `"DKAN" "open data" site:.gov` |
| Censys | `web.endpoints.http.body: "DKAN"` |
| Shodan | `http.html:"dkan"` |

Do not label a site `dkan` from the CKAN API alone — that is usually `ckan`.

## OpenDataSoft (`opendatasoft`)

SaaS and self-hosted Explore portals. Many hosts end in `*.opendatasoft.com` or use a custom domain with `/explore`.

**Confirm:** `https://host/api/explore/v2.1/catalog/datasets` (or legacy `/api/v2/catalog/datasets/`). UI path `/explore`.

| Tool | Query |
|------|-------|
| Google | `inurl:/explore "opendatasoft" -site:opendatasoft.com/blog` |
| Google | `site:opendatasoft.com/explore` |
| Google | `"Powered by OpenDataSoft" OR "ods-explore"` |
| Censys | `web.names: "opendatasoft.com"` |
| Censys | `web.endpoints.http.body: "OpenDataSoft"` |
| crt.sh | `%.opendatasoft.com` |

**False positives:** the vendor homepage, academy, and blog. Register the **portal** (`{org}.opendatasoft.com` or the city’s custom domain), not `www.opendatasoft.com`. List: [Open Data Inception](https://data.opendatasoft.com/explore/dataset/open-data-sources%40public/information/).

## Socrata (`socrata`)

Tyler / Socrata Open Data. UI often `/browse` or `/datasets`. SODA API under `/api/views`. Network: [opendatanetwork.com](https://www.opendatanetwork.com/search?q=).

**Confirm:** `https://host/api/views.json?limit=1` or `/api/views`. Many sites also serve `/data.json`. Headers may include `X-Socrata-*`.

| Tool | Query |
|------|-------|
| Google | `inurl:/browse "socrata" OR "open data network"` |
| Google | `"Powered by Socrata" OR inurl:/api/views` |
| Google | `site:*.socrata.com` (custom domains are more interesting) |
| Censys | `web.endpoints.http.body: "socrata"` |
| Shodan | `http.html:"X-Socrata" OR http.html:"soda.demo"` |

Skip `soda.demo.socrata.com` and Tyler marketing sites. Prefer the city’s production domain.

## uData (`udata`)

French-origin portal (data.gouv.fr lineage). Dataset UI `/datasets/`. API `/api/1/datasets/`.

**Confirm:** `https://host/api/1/datasets/?page_size=1` returns JSON with `data` / `total`.

| Tool | Query |
|------|-------|
| Google | `"opendata" inurl:/datasets site:.gouv.fr` |
| Google | `"udata" "jeux de données" OR inurl:/api/1/datasets` |
| Censys | `web.endpoints.http.body: "udata"` |
| Censys | `web.names: "data.gouv"` |

Local clones exist outside France. Do not assume every `/api/1/datasets` is uData — check the JSON shape.

## Magda (`magda`)

Search-centric catalog (data.gov.au and derivatives). API `/api/v0/search/datasets` or `/search/api/v0/search/datasets`.

**Confirm:** that search endpoint returns JSON datasets. UI often `/search` or `/dataset`.

| Tool | Query |
|------|-------|
| Google | `"magda" "data catalog" OR inurl:/api/v0/search/datasets` |
| Google | `inurl:/search/api/v0/search/datasets` |
| Censys | `web.endpoints.http.body: "magda"` |

## JKAN (`jkan`)

Jekyll + CKAN-like static portal. Often GitHub Pages. Datasets as Markdown in `/datasets`.

**Confirm:** HTML “JKAN” / `_config.yml` mentions; dataset list at `/datasets/`. No CKAN Action API.

| Tool | Query |
|------|-------|
| Google | `"JKAN" "open data" OR "jkan" inurl:/datasets` |
| Google | `site:github.io "JKAN"` |

Skip the [jkan.io](https://jkan.io) project site unless it is a real catalog instance.

## Junar (`junar`)

SaaS open-data CMS used in Latin America. Customer list: [junar.com/customers](https://junar.com/customers/). Often `/data.json`.

| Tool | Query |
|------|-------|
| Google | `"powered by Junar" OR "junar" "datos abiertos"` |
| Censys | `web.endpoints.http.body: "Junar"` |

## EntryScape (`entryscape`)

DCAT-AP catalogs, especially Sweden and Nordics. Customers: [entryscape.com/en/customers](https://entryscape.com/en/customers/). UI may be Blocks/Catalog; API under `/store/`.

| Tool | Query |
|------|-------|
| Google | `"EntryScape" (catalog OR "öppna data" OR dcat)` |
| Google | `inurl:/store "entryscape"` |
| Censys | `web.endpoints.http.body: "EntryScape"` |

## ArcGIS Hub as an open-data site (`arcgishub`)

Many Hub sites are **open data** first (dataset search, DCAT) rather than a map viewer. If the primary UI is a dataset catalog, use `catalog_type: Open data portal` and `software.id: arcgishub`. If it is a GIS hub / map gallery, use **Geoportal** — see [discovery-geoportals.md](discovery-geoportals.md#arcgis-hub-arcgishub).

**Confirm:** `/api/search/v1` or `/api/feed/dcat-us/1.1.json`. Hosts often `*.hub.arcgis.com` or `opendata.arcgis.com`.

```text
site:hub.arcgis.com "open data"
site:opendata.arcgis.com
inurl:hub.arcgis.com
```

crt.sh: `%.hub.arcgis.com`. Gallery: [hub.arcgis.com](https://hub.arcgis.com/).

## Idra (`idra`)

Open Data Federation Platform (FIWARE / Engineering Ingegneria Informatica). It harvests CKAN, DKAN, Socrata, OpenDataSoft, NGSI, and DCAT-AP sources into one search UI. Docs: [idra.readthedocs.io](https://idra.readthedocs.io). Source: [OPSILab/Idra](https://github.com/OPSILab/Idra).

Typical `catalog_type` is **Data search engine** (folder `search/`), not Open data portal: Idra is an aggregator over other ODMS catalogues.

**Signals:** path `/IdraPortal/`; REST under `/Idra/api/v1/`; SPARQL; DCAT-AP / DCAT-AP_IT branding.

**Confirm:** GET `https://host/IdraPortal/` (or `/Idra/api/v1/` JSON). Prefer a live production federation. The public demos (`idra.site`, `idra.opsilab.it`, `idra.eng.it`, sandbox hosts) are already registered and mostly **inactive** — do not re-add them.

| Tool | Query |
|------|-------|
| Google | `"Idra" ("Open Data Federation" OR IdraPortal OR "DCAT-AP_IT") -site:github.com -site:readthedocs.io` |
| Google | `inurl:/IdraPortal/ OR inurl:/Idra/api/v1/` |
| Censys | `web.endpoints.http.body: "IdraPortal"` |
| Censys | `web.endpoints.http.body: "Idra"` |

Do not register harvested source catalogs a second time as Idra. Duplicate-check the underlying CKAN/Socrata/OpenDataSoft `link` as well.

## Liferay (`liferay`)

Digital experience CMS. **Only** register when a public Open Data / RISP dataset listing exists (common on Spanish provincial sites), not a generic Liferay intranet.

**Signals:** Liferay portal paths (`/web/guest/`); “datos abiertos” / RISP module; Excel/XML/JSON/CSV dataset tables.

**Confirm:** GET the open-data page and verify a reusable dataset list. Skip city hall homepages that only mention open data in a news article.

| Tool | Query |
|------|-------|
| Google | `"datos abiertos" Liferay OR RISP (ayuntamiento OR diputación) site:.es` |
| Google | `inurl:/web/guest/ "datos abiertos"` |
| Censys | `web.endpoints.http.body: "Liferay"` |

## POMOSAM (`pomosam`)

CORA GEO municipal eGovernment / open-data publisher used by Slovak cities (contracts, invoices, orders, public datasets). Vendor: [pomosam.sk](http://www.pomosam.sk).

**Signals:** POMOSAM / CG eGOV branding; municipal zverejňovanie / open-data modules.

**Confirm:** GET the public dataset or disclosure catalog. One record per municipality tenant.

| Tool | Query |
|------|-------|
| Google | `"POMOSAM" OR "CG eGOV" (otvorené OR zverejňovanie) site:.sk` |
| Censys | `web.endpoints.http.body: "POMOSAM"` |

## oPortal (`oportal`)

Inspur Chinese government open-data product. Deployments share `/oportal/` catalogs, a developer center, and an application gallery.

**Signals:** path `/oportal/`; 浪潮 / Inspur; data-service / API gallery pages.

**Confirm:** GET `/oportal/` (or the documented catalog path) and match a dataset listing. One record per government tenant.

| Tool | Query |
|------|-------|
| Google | `inurl:/oportal/ (数据 OR 开放)` |
| Google | `"浪潮" 开放数据 oportal` |
| Censys | `web.endpoints.http.body: "/oportal/"` |

## OGD Platform India (`ogdindia`)

NIC SaaS on data.gov.in for ministries and states. Site: [data.gov.in](https://data.gov.in).

**Signals:** `data.gov.in` tenant host or path; OGD Platform India; CKAN-like catalog UI on NIC hosting.

**Confirm:** GET the ministry/state catalog home. Do not re-add the national portal if it is already registered; add only distinct tenant catalogs.

| Tool | Query |
|------|-------|
| Google | `site:data.gov.in (catalog OR dataset)` |
| Google | `"OGD Platform" OR "Open Government Data" site:.gov.in` |
| crt.sh | `%.data.gov.in` |

## data eye (`dataeye`)

Japanese municipal open-data SaaS (Data Cradle). Site: [dataeye.jp](https://dataeye.jp). Some tenants expose a CKAN-compatible metadata API.

**Confirm:** GET the prefecture/city catalog. One record per tenant (including joint prefecture-municipality group portals).

| Tool | Query |
|------|-------|
| Google | `site:dataeye.jp` |
| Google | `"data eye" オープンデータ (市 OR 県)` |
| crt.sh | `%.dataeye.jp` |

## Seoul Open Data Plaza (`seoulopendataplaza`)

Shared catalog used by Seoul Metropolitan Government district (`gu`) portals. Titles of the form 열린 데이터 광장; `/openinf/` JSP pages.

**Confirm:** GET the district plaza home. One record per `gu` tenant, plus the city portal if it is a distinct catalog.

| Tool | Query |
|------|-------|
| Google | `"열린 데이터 광장" site:.go.kr` |
| Google | `inurl:/openinf/ seoul` |
| Censys | `web.endpoints.http.body: "openinf"` |

## Data Fair (`datafair`)

Koumoul open-source data portals. Docs: [data-fair.github.io](https://data-fair.github.io/3/en/).

**Signals:** Data Fair / Koumoul; `/data-fair/` or dataset explorer APIs.

**Confirm:** GET the public portal and a dataset list API. Skip the vendor docs site.

| Tool | Query |
|------|-------|
| Google | `"Data Fair" (Koumoul OR datasets) -site:github.com` |
| Censys | `web.endpoints.http.body: "data-fair"` |

## Datawheel (`datawheel`)

Datawheel-hosted open-data / economic-complexity portals. Site: [datawheel.us](https://datawheel.us).

**Confirm:** GET the public data portal (not a marketing page). One record per government or international-organization catalog.

| Tool | Query |
|------|-------|
| Google | `"Datawheel" (open data OR "data portal") -site:datawheel.us` |
| Censys | `web.endpoints.http.body: "datawheel"` |

## SEU-e (`seue`)

Consorci AOC electronic office / transparency / open-data service for Catalan administrations. Hosts under `seu-e.cat`.

**Confirm:** GET the municipality’s open-data or transparency dataset listing on `seu-e.cat`. One record per public-administration tenant that publishes datasets.

| Tool | Query |
|------|-------|
| Google | `site:seu-e.cat (dades OR datasets OR "dades obertes")` |
| crt.sh | `%.seu-e.cat` |

## TriplyDB (`triplydb`)

Linked-data / knowledge-graph publishing with SPARQL. Site: [triplydb.com](https://triplydb.com).

**Confirm:** GET the public dataset catalog or SPARQL UI. One record per public instance, not per named graph.

| Tool | Query |
|------|-------|
| Google | `site:triplydb.com` |
| Google | `"TriplyDB" (SPARQL OR datasets) -site:triplydb.com` |
| crt.sh | `%.triplydb.com` |

## Drupal (`drupal`) and WordPress (`wordpress`)

Use these IDs only when the **public product is a dataset catalog** (DKAN-like Drupal open data, WordPress dataset plugins, CKAN-theme WP sites). Do not register ordinary CMS homepages.

**Confirm:** a queryable dataset list or harvestable API (`/jsonapi` for Drupal, a dataset plugin or CKAN proxy for WordPress). If the site is DKAN, use `dkan` instead of `drupal`.

| Tool | Query |
|------|-------|
| Google | `"powered by Drupal" ("open data" OR datasets) inurl:/data` |
| Google | `"open data" WordPress (CKAN OR dataset) -site:wordpress.org` |

## Piveau (`piveau`)

DCAT-AP microservice catalog (Fraunhofer FOKUS). Site: [piveau.de](https://www.piveau.de). Powers several European public-sector portals (including patterns used by data.europa.eu).

**Signals:** Piveau / DCAT-AP; Sparql or Hub-UI; `piveau` in HTML or API paths.

**Confirm:** GET the public catalog and a DCAT/search API. Do not re-add data.europa.eu if it is already registered.

| Tool | Query |
|------|-------|
| Google | `"Piveau" (DCAT-AP OR "open data") -site:github.com -site:piveau.de` |
| Censys | `web.endpoints.http.body: "piveau"` |

## LKOD (`lkod`)

Czech local DCAT-AP-CZ catalogs (Golemio / Operátor ICT). Harvests into NKOD. Site: [lkod.cz](https://lkod.cz).

**Confirm:** GET the municipal/local catalog (Next.js LKOD UI), not the national NKOD record twice.

| Tool | Query |
|------|-------|
| Google | `"LKOD" OR "lokální katalog otevřených dat" site:.cz` |
| Censys | `web.endpoints.http.body: "lkod"` |

## Aleph (`aleph`)

OCCRP investigative document/dataset search. Site: [aleph.occrp.org](https://aleph.occrp.org). Often `catalog_type: Data search engine` or Open data portal depending on whether it hosts datasets or searches collections.

**Confirm:** GET a public Aleph instance. Skip login-only investigations.

| Tool | Query |
|------|-------|
| Google | `"Aleph" OCCRP (datasets OR documents) -site:occrp.org` |
| Censys | `web.endpoints.http.body: "aleph"` |

## Our Open Data (`ouropendata`)

Japanese prefecture/city open-data CMS (Tokushima, Kagawa, Aomori, and others). Shared assets: `/assets/cms/public.css`, numeric `/dataset/` HTML pages, plus an application market and idea box.

**Confirm:** GET the catalog home (not a single dataset HTML page). Distinct from CKAN and data.go.jp.

| Tool | Query |
|------|-------|
| Google | `"Our Open Data" オープンデータ OR inurl:/assets/cms/public.css` |
| Censys | `web.endpoints.http.body: "assets/cms/public.css"` |

## Gipuzkoa Irekia (`gipuzkoairekia`)

Shared open-government / open-data platform for Gipuzkoa municipalities. Hub: [gipuzkoairekia.eus](https://www.gipuzkoairekia.eus).

**Confirm:** GET a **tenant** catalog (municipality or foral entity), not only the provincial hub if that hub is already registered. DCAT feeds are a plus.

| Tool | Query |
|------|-------|
| Google | `site:gipuzkoairekia.eus (datos OR datuak OR catalog)` |
| Google | `"Gipuzkoa Irekia" (opendata OR "datos abiertos")` |
| Censys | `web.names: "gipuzkoairekia.eus"` |

## DataPress (`datapress`)

Managed CKAN plus CMS. Site: [datapress.com](https://datapress.com). Prefer `datapress` when the public product is branded DataPress; otherwise `ckan` if only the CKAN API is visible.

**Confirm:** CKAN `status_show` **and** DataPress chrome (or vendor docs naming DataPress). Do not double-register the same host as both `ckan` and `datapress`.

| Tool | Query |
|------|-------|
| Google | `"DataPress" ("open data" OR CKAN)` |
| Censys | `web.endpoints.http.body: "datapress"` |

## MODA Open Data Platform (`modaopendata`)

Taiwan Nuxt/Vue open-data frontend (national data.gov.tw family plus local clones). Source: moda-gov-tw/opendata-frontend.

**Confirm:** GET a **tenant** catalog (city/ministry), not a duplicate of the national hub if that hub is already registered. Shared `_nuxt` stack plus catalog API.

| Tool | Query |
|------|-------|
| Google | `"data.gov.tw" OR inurl:_nuxt (opendata OR 開放資料) site:.tw` |
| Google | `"moda-gov-tw" opendata` |
| Censys | `web.names: "data.gov.tw"` |

## Other open-data platforms

| `software.id` | Signals | Typical query |
|---------------|---------|---------------|
| `ouropendata` | see above | |
| `gipuzkoairekia` | see above | |
| `datapress` | see above | |
| `modaopendata` | see above | |
| `bitrix` | 1C-Bitrix **dataset catalog only** | `"Битрикс" открытые данные` (skip ordinary CMS) |
| `jdop` | Japanese JDOP | `"JDOP" オープンデータ` |
| `publishmydata` | Linked-data publisher | `"PublishMyData" OR publishmydata` |
| `opendatareg` | OpenData.reg / regional IT | `"opendata.reg"` |
| `datagovmy` | Malaysia data.gov.my stack | `site:data.gov.my` (tenant catalogs only) |
| `copernicuscds` | Copernicus Climate/Atmosphere Data Store | `"Climate Data Store" Copernicus` (do not clone CDS) |
| `tablion` | Aristotle Tablion portal | `"Tablion" "data portal"` |
| `strapi` | Headless CMS **with a public dataset API** | `"Strapi" ("open data" OR datasets)` |
| `smw` | Semantic MediaWiki data catalog | `"Semantic MediaWiki" (dataset OR catalog)` |
| `d4science` | D4Science VRE / catalog | `"D4Science" (catalog OR "open data")` |

## Generic open-data URL patterns

Try these on a **named** government or city host only (not as an internet-wide scan):

- `/data`, `/opendata`, `/datasets`, `/catalog`, `/datos`, `/donnees`
- `/data.json`, `/catalog.json`, `/catalog.xml` (DCAT)
- `/api/3/action/status_show` (CKAN)
- `/api/explore/v2.1/catalog/datasets` (OpenDataSoft)
- `/IdraPortal/` and `/Idra/api/v1/` (Idra)
- `/oportal/` (Inspur oPortal)
- `/openinf/` (Seoul Open Data Plaza)
- `/assets/cms/public.css` (Our Open Data)

Search with local terms plus the city: `datos abiertos "Rosario"`, `offene Daten "Leipzig"`, `开放数据 市`.

## Related

- [discovery.md](discovery.md)
- [discovery-search-tools.md](discovery-search-tools.md)
- [discovery-metadata.md](discovery-metadata.md)
- [discovery-indicators.md](discovery-indicators.md)
- [discovery-other.md](discovery-other.md)
- [harvest-opendata.md](harvest-opendata.md)
- [harvest.md](harvest.md)
- [harvest-protocols.md](harvest-protocols.md)
- [apidetect.md](apidetect.md)
- [ckan-sync.md](ckan-sync.md)
- [catalog-types.md](catalog-types.md)
- [software-taxonomy.md](software-taxonomy.md)
