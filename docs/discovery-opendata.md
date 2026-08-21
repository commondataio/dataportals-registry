# Discovering open data portals

How to find **open data portal** installations (`catalog_type: Open data portal`) that are not yet in this registry. Search-engine syntax: [discovery-search-tools.md](discovery-search-tools.md). Overview and accept/reject rules: [discovery.md](discovery.md). Also covered here: Idra (`idra`), a DCAT-AP federation layer that is usually typed as a **Data search engine**.

Set `software.id` from `data/software/` only when a probe or page signal matches. Otherwise `custom`. After YAML exists: `python scripts/apidetect.py detect-single {id} --dryrun`.

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

## Generic open-data URL patterns

Try these on a **named** government or city host only (not as an internet-wide scan):

- `/data`, `/opendata`, `/datasets`, `/catalog`, `/datos`, `/donnees`
- `/data.json`, `/catalog.json`, `/catalog.xml` (DCAT)
- `/api/3/action/status_show` (CKAN)
- `/api/explore/v2.1/catalog/datasets` (OpenDataSoft)
- `/IdraPortal/` and `/Idra/api/v1/` (Idra)

Search with local terms plus the city: `datos abiertos "Rosario"`, `offene Daten "Leipzig"`, `开放数据 市`.

## Related

- [discovery.md](discovery.md)
- [discovery-search-tools.md](discovery-search-tools.md)
- [discovery-metadata.md](discovery-metadata.md)
- [ckan-sync.md](ckan-sync.md)
- [catalog-types.md](catalog-types.md)
- [software-taxonomy.md](software-taxonomy.md)
