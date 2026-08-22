# Harvesting datasets from open data portals

CKAN, OpenDataSoft, Socrata, and similar portals already treat **datasets** (packages, views) as the primary object. You rarely need the publication-type filters used for [scientific repositories](harvest-scientific.md). You still must avoid harvesting the wrong object (resources, showcases, harvest sources, individual files).

Overview: [harvest.md](harvest.md). Finding portals: [discovery-opendata.md](discovery-opendata.md). Shared DCAT/CKAN grain: [harvest-protocols.md](harvest-protocols.md).

Replace `https://host` with the catalog origin. GET only. Stop on `401`/`403`. Prefer URLs already in `endpoints[]`.

## What to keep

| Keep | Drop |
|------|------|
| Dataset / package / view / explore dataset | A file **resource** (CSV) as if it were a separate catalog record |
| The dataset landing API object | Harvest **source** metadata (the remote catalog CKAN is pulling from) |
| | Showcase, article, blog, app gallery, idea box |
| | Organization or group objects without a dataset list |

One dataset with five CSV resources is **one** dataset.

## CKAN (`ckan`) {#ckan}

**Search (preferred):**

```text
GET https://host/api/3/action/package_search?q=&rows=100&start=0
```

Use `start` += `rows` until `count` is reached. Each `results[]` element is a dataset.

**Optional filter** when the site mixes types:

```text
GET https://host/api/3/action/package_search?fq=dataset_type:dataset&rows=100
```

Drop `dataset_type:showcase` (ckanext-showcase), harvest objects, and `type:harvest`.

`package_list` returns names only and is painful on large sites — prefer `package_search`.

DataPress (`datapress`) is CKAN plus CMS — harvest `package_search`, not CMS pages. Do not also harvest the same host as `ckan`.

OpenAIRE Graph/CONNECT gateways are **data search engines**, not open-data CMSs. Recipe: [harvest-other.md](harvest-other.md#openaire).

## DKAN (`dkan`) {#dkan}

Same Action API as [CKAN](#ckan) when enabled; also `/api/1/search`. Confirm JSON `"success": true`. If only Drupal JSON:API is public, see [Drupal](#drupal) and prefer `dkan` when the product is DKAN.

## OpenDataSoft (`opendatasoft`) {#opendatasoft}

Already a dataset catalog:

```text
GET https://host/api/explore/v2.1/catalog/datasets?limit=100&offset=0
```

Follow `links` / offset until `total_count`. Do not harvest the vendor academy or `www.opendatasoft.com`.

## Socrata (`socrata`) {#socrata}

Views include charts, maps, files, and stories.

```text
GET https://host/api/catalog/v1?only=datasets&limit=100&offset=0
```

Legacy: `/api/views.json` mixes types — filter `viewType` / `displayType` to tabular datasets, or use the catalog API `only=datasets`. Drop `only=stories`, `only=filters`.

## uData (`udata`) {#udata}

```text
GET https://host/api/1/datasets/?page_size=100&page=1
```

Do not page `/api/1/reuses/` or `/api/1/posts/` as datasets.

## Magda (`magda`) {#magda}

Catalog search API from `endpoints[]`. Keep datasets, not portal chrome.

## JKAN (`jkan`) {#jkan}

Often static JSON in the repo — harvest the published `datasets.json` / equivalent, not GitHub issues.

## Junar (`junar`) {#junar}

Dataset API on the tenant, not junar.com marketing.

## EntryScape (`entryscape`) {#entryscape}

DCAT-AP. Harvest **Dataset** / `dcat:Dataset` only. Public DCAT/search API on the tenant.

## Piveau (`piveau`) {#piveau}

DCAT-AP search; skip the hub UI chrome. Keep `dcat:Dataset` only.

## Idra (`idra`) {#idra}

Federation of other catalogs (`catalog_type` is often Data search engine). Harvesting Idra duplicates member catalogs — prefer harvesting the **source** portals from this registry unless you need the federation view.

## ArcGIS Hub (`arcgishub`) as open data {#arcgishub}

```text
GET https://host/api/search/v1
```

Keep dataset / feature layer **items** that are public data. Drop StoryMaps, sites, and applications unless you have a separate apps index.

## Data Fair (`datafair`) {#datafair}

Koumoul portals. Typical list:

```text
GET https://host/data-fair/api/v1/datasets
```

Page the JSON dataset collection. Drop applications and remote-service catalog chrome. Paths vary — use `endpoints[]` when present.

## Datawheel (`datawheel`) {#datawheel}

Front-end data/economic-complexity sites. There is often **no** common `/api`. Harvest a documented JSON/CSV catalog if the portal publishes one; otherwise stop rather than scraping every visualization.

## TriplyDB (`triplydb`) {#triplydb}

```text
GET https://host/_api/facets/datasets
```

Keep **datasets**, not every named graph or SPARQL binding. One instance, not one graph per harvest record. See [harvest-protocols.md](harvest-protocols.md#sparql--linked-data).

## LKOD (`lkod`) {#lkod}

Czech local DCAT-AP-CZ. Harvest `dcat:Dataset` from the municipal LKOD UI/API. Do not also harvest NKOD for the same datasets.

## OGD Platform India (`ogdindia`) {#ogdindia}

Ministry/state tenants on data.gov.in. Catalog APIs often **require a registered key** — stop on `401`. If a public CKAN-style or HTML catalog lists datasets without a key, harvest that list. Do not re-harvest the national hub when you only needed a tenant.

## data eye (`dataeye`) {#dataeye}

Japanese municipal SaaS. Some tenants speak CKAN-compatible metadata — try `package_search` on the tenant host. Otherwise harvest the public catalog JSON if present. One tenant = one scope (`%.dataeye.jp`).

## Seoul Open Data Plaza (`seoulopendataplaza`) {#seoulopendataplaza}

`/openinf/` JSP catalogs. Open API developer space often needs a key. Harvest the public dataset listing / sitemap when unauthenticated access exists. One `gu` tenant = one scope.

## oPortal (`oportal`) {#oportal}

Inspur `/oportal/` catalogs. There is no verified anonymous default API on every tenant. Harvest `/oportal/` dataset listing or DCAT if public. Do not scrape the application gallery as datasets.

## Liferay (`liferay`) {#liferay}

Spanish RISP / datos abiertos modules on Liferay. Harvest only the **dataset list** (often a JSON/CSV/XML table or `/documents/` open-data folder).

**Keep:** rows that are datasets (title + landing or file URL). **Drop:** `/web/guest/` CMS homepages, news, and generic document libraries.

If the list is HTML-only with no machine table, stop. Do not crawl every Liferay page.

## POMOSAM (`pomosam`) {#pomosam}

Slovak municipal disclosure platforms. Harvest the open-data / dataset module. Skip procurement and contracts-only pages unless that module **is** the data catalog.

## SEU-e (`seue`) {#seue}

`seu-e.cat` e-office tenants. Harvest **dades obertes** listings only, not the whole electronic office.

## Drupal (`drupal`) {#drupal}

Only when the public product is a dataset catalog (not a news CMS).

```text
GET https://host/jsonapi/node/dataset
GET https://host/data.json
```

Bundle names vary (`dataset`, `open_data`, `ckan_dataset`). Inspect `/jsonapi` once for dataset-like bundles. **Keep** those nodes. **Drop** `article`, `page`, `media`, and user accounts.

DKAN on Drupal: use the [DKAN](#dkan) Action API when enabled — prefer `dkan` as `software.id`.

## WordPress (`wordpress`) {#wordpress}

`/wp-json/` only for a **datasets** custom post type (`/wp-json/wp/v2/dataset` or the type the catalog documents). Do not harvest `/wp/v2/posts` or media. Ordinary WordPress homepages are out of scope.

## Bitrix (`bitrix`) {#bitrix}

1C-Bitrix government portals. Harvest only a published **open-data / dataset** module (JSON/CSV/DCAT list). Skip the rest of the CMS, news, and `/bitrix/admin/`.

## DataPress (`datapress`) {#datapress}

CKAN plus CMS. Harvest `package_search` as in [CKAN](#ckan). Do not harvest CMS pages or double-count the host as `ckan`.

## Our Open Data (`ouropendata`) {#ouropendata}

Japanese Our Open Data: harvest the catalog home / numeric dataset list, not idea-box posts.

## Gipuzkoa Irekia (`gipuzkoairekia`) {#gipuzkoairekia}

Tenant DCAT. Keep datasets, not the rest of the Irekia CMS.

## MODA (`modaopendata`) {#modaopendata}

Tenant catalog API, not a second national data.gov.tw clone.

## PublishMyData (`publishmydata`) {#publishmydata}

Linked-data publishing (Swirrl). Harvest the **DCAT dataset list** or SPARQL that the catalog documents — named datasets, not every triple ([harvest-protocols.md](harvest-protocols.md#sparql--linked-data)).

## data.gov.my (`datagovmy`) {#datagovmy}

Malaysia national / agency tenants on the data.gov.my stack.

```text
GET https://api.data.gov.my/data-catalogue?id=fuelprice
```

`id` is required. That response is **observations** for one catalogue dataset — treat each `id` as one dataset analog; do not page rows as datasets. There is no verified anonymous GET that lists all catalogue ids; collect ids from the public catalogue UI or [developer.data.gov.my](https://developer.data.gov.my/). Drop dashboards and documentation pages. Agency sites (`open.dosm.gov.my`, `data.moh.gov.my`) are separate registry rows — harvest each tenant once.

## JDOP (`jdop`) {#jdop}

Zhejiang public-data open platform (`/jdop_front/`, `/dopServer/`). Harvest the tenant **dataset catalog** API when public. Skip the Zhejiang government homepage and login-only 数据开放 admin.

## Open Data Registry (`opendatareg`) {#opendatareg}

AWS Open Data Registry style catalogs (`catalog.json` / YAML dataset files, optional STAC). Keep **dataset** entries. Drop bucket listings and every STAC **item**. Skip cloning registry.opendata.aws if you only needed the existing registry row.

## D4Science (`d4science`) {#d4science}

Harvest **public catalogue items** on the VRE (gCat / documented dataset list). Drop workspace files, private VREs, and d4science.org marketing.

## Semantic MediaWiki (`smw`) {#smw}

```text
GET https://host/w/api.php?action=ask&query=[[Category:Dataset]]
```

Keep pages typed as Dataset (or the site’s equivalent category). Drop ordinary wiki articles. `api.php` without a dataset query is not a harvest.

## Strapi (`strapi`) {#strapi}

Public **dataset** content-type REST only (`/api/datasets` or the type the catalog documents). Drop posts, users, and admin.

## Tablion (`tablion`) {#tablion}

Aristotle’s data-portal product — harvest its public dataset API, not every MDR object ([harvest-metadata.md](harvest-metadata.md)).

Copernicus CDS (`copernicuscds`): [harvest-earthdata.md](harvest-earthdata.md#copernicuscds). Discovery fingerprints: [discovery-opendata.md](discovery-opendata.md).

## Portals without a dataset API

Liferay, POMOSAM, oPortal, OGD India, Seoul plaza, Drupal, and WordPress are covered above when a list exists. If there is still no machine-readable catalog, stop. Generic DCAT paths: `/catalog.xml`, `/data.json` ([harvest-protocols.md](harvest-protocols.md#dcat)).

## Related

- [harvest.md](harvest.md)
- [harvest-scientific.md](harvest-scientific.md)
- [harvest-geoportals.md](harvest-geoportals.md)
- [discovery-opendata.md](discovery-opendata.md)
- [harvest-protocols.md](harvest-protocols.md)
- [harvest-incremental.md](harvest-incremental.md)
- [harvest-earthdata.md](harvest-earthdata.md)
- [harvest-identifiers.md](harvest-identifiers.md)
- [harvest-output.md](harvest-output.md)
- [apidetect.md](apidetect.md)
- [agents/harvest.md](agents/harvest.md)
