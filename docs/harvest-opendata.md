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

## CKAN (`ckan`) and DKAN (`dkan`)

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

DKAN: same CKAN-style actions when enabled; also `/api/1/search`. Confirm JSON `"success": true`.

DataPress (`datapress`) is CKAN plus CMS — harvest `package_search`, not CMS pages. Do not also harvest the same host as `ckan`.

OpenAIRE Graph/CONNECT gateways are **data search engines**, not open-data CMSs. Recipe: [harvest-other.md](harvest-other.md#openaire-openaire).

## OpenDataSoft (`opendatasoft`)

Already a dataset catalog:

```text
GET https://host/api/explore/v2.1/catalog/datasets?limit=100&offset=0
```

Follow `links` / offset until `total_count`. Do not harvest the vendor academy or `www.opendatasoft.com`.

## Socrata (`socrata`)

Views include charts, maps, files, and stories.

```text
GET https://host/api/catalog/v1?only=datasets&limit=100&offset=0
```

Legacy: `/api/views.json` mixes types — filter `viewType` / `displayType` to tabular datasets, or use the catalog API `only=datasets`. Drop `only=stories`, `only=filters`.

## uData (`udata`)

```text
GET https://host/api/1/datasets/?page_size=100&page=1
```

Do not page `/api/1/reuses/` or `/api/1/posts/` as datasets.

## Magda (`magda`), JKAN (`jkan`), Junar (`junar`)

Use the dataset search/list endpoint from `endpoints[]`. Magda: catalog search API. JKAN is often static JSON in the repo — harvest the published `datasets.json` / equivalent, not GitHub issues. Junar: dataset API on the tenant, not junar.com marketing.

## EntryScape (`entryscape`), Piveau (`piveau`), Idra (`idra`)

DCAT-AP catalogs. Harvest **Dataset** / `dcat:Dataset` only.

- EntryScape: public DCAT/search API on the tenant.
- Piveau: DCAT-AP search; skip the hub UI chrome.
- Idra: federation of other catalogs (`catalog_type` is often Data search engine). Harvesting Idra duplicates member catalogs — prefer harvesting the **source** portals from this registry unless you need the federation view.

## ArcGIS Hub (`arcgishub`) as open data

```text
GET https://host/api/search/v1
```

Keep dataset / feature layer **items** that are public data. Drop StoryMaps, sites, and applications unless you have a separate apps index.

## Data Fair (`datafair`)

Koumoul portals. Typical list:

```text
GET https://host/data-fair/api/v1/datasets
```

Page the JSON dataset collection. Drop applications and remote-service catalog chrome. Paths vary — use `endpoints[]` when present.

## Datawheel (`datawheel`)

Front-end data/economic-complexity sites. There is often **no** common `/api`. Harvest a documented JSON/CSV catalog if the portal publishes one; otherwise stop rather than scraping every visualization.

## TriplyDB (`triplydb`)

```text
GET https://host/_api/facets/datasets
```

Keep **datasets**, not every named graph or SPARQL binding. One instance, not one graph per harvest record. See [harvest-protocols.md](harvest-protocols.md#sparql--linked-data).

## LKOD (`lkod`)

Czech local DCAT-AP-CZ. Harvest `dcat:Dataset` from the municipal LKOD UI/API. Do not also harvest NKOD for the same datasets.

## OGD Platform India (`ogdindia`)

Ministry/state tenants on data.gov.in. Catalog APIs often **require a registered key** — stop on `401`. If a public CKAN-style or HTML catalog lists datasets without a key, harvest that list. Do not re-harvest the national hub when you only needed a tenant.

## data eye (`dataeye`)

Japanese municipal SaaS. Some tenants speak CKAN-compatible metadata — try `package_search` on the tenant host. Otherwise harvest the public catalog JSON if present. One tenant = one scope (`%.dataeye.jp`).

## Seoul Open Data Plaza (`seoulopendataplaza`)

`/openinf/` JSP catalogs. Open API developer space often needs a key. Harvest the public dataset listing / sitemap when unauthenticated access exists. One `gu` tenant = one scope.

## oPortal (`oportal`)

Inspur `/oportal/` catalogs. There is no verified anonymous default API on every tenant. Harvest `/oportal/` dataset listing or DCAT if public. Do not scrape the application gallery as datasets.

## Liferay (`liferay`), POMOSAM (`pomosam`), SEU-e (`seue`)

CMS / eGovernment shells that sometimes host a dataset table.

- **Liferay:** harvest only the RISP / datos abiertos **dataset list** (CSV/JSON/XML tables). Skip `/web/guest/` homepages.
- **POMOSAM:** harvest the municipal disclosure/open-data module, not contracts-only pages unless those are the data catalog.
- **SEU-e:** harvest `seu-e.cat` tenant **dades obertes** listings, not the whole e-office.

If there is no machine list, do not scrape every CMS page from this repository’s workflows.

## Drupal (`drupal`) and WordPress (`wordpress`)

Only when the public product is a dataset catalog.

- Drupal JSON:API `/jsonapi/node/dataset` (or the site’s dataset bundle) — not every node.
- WordPress `/wp-json/` — only a datasets custom post type, not `/wp/v2/posts`.
- DKAN on Drupal: use [CKAN/DKAN](#ckan-ckan-and-dkan-dkan) actions when enabled.

Ordinary CMS homepages are out of scope.

## Bitrix (`bitrix`)

1C-Bitrix government portals. Harvest only a published **open-data / dataset** module (JSON/CSV/DCAT list). Skip the rest of the CMS, news, and `/bitrix/admin/`.

## DataPress (`datapress`)

CKAN plus CMS. Harvest `package_search` as in [CKAN](#ckan-ckan-and-dkan-dkan). Do not harvest CMS pages or double-count the host as `ckan`.

## Our Open Data (`ouropendata`), Gipuzkoa Irekia (`gipuzkoairekia`), MODA (`modaopendata`)

Japanese Our Open Data: harvest the catalog home / numeric dataset list, not idea-box posts. Gipuzkoa Irekia: tenant DCAT. MODA: tenant catalog API, not a second national data.gov.tw clone.

## PublishMyData (`publishmydata`)

Linked-data publishing (Swirrl). Harvest the **DCAT dataset list** or SPARQL that the catalog documents — named datasets, not every triple ([harvest-protocols.md](harvest-protocols.md#sparql--linked-data)).

## data.gov.my (`datagovmy`)

Malaysia national / agency tenants on the data.gov.my stack.

```text
GET https://api.data.gov.my/data-catalogue?id=fuelprice
```

`id` is required. That response is **observations** for one catalogue dataset — treat each `id` as one dataset analog; do not page rows as datasets. There is no verified anonymous GET that lists all catalogue ids; collect ids from the public catalogue UI or [developer.data.gov.my](https://developer.data.gov.my/). Drop dashboards and documentation pages. Agency sites (`open.dosm.gov.my`, `data.moh.gov.my`) are separate registry rows — harvest each tenant once.

## JDOP (`jdop`)

Zhejiang public-data open platform (`/jdop_front/`, `/dopServer/`). Harvest the tenant **dataset catalog** API when public. Skip the Zhejiang government homepage and login-only 数据开放 admin.

## Open Data Registry (`opendatareg`)

AWS Open Data Registry style catalogs (`catalog.json` / YAML dataset files, optional STAC). Keep **dataset** entries. Drop bucket listings and every STAC **item**. Skip cloning registry.opendata.aws if you only needed the existing registry row.

## D4Science (`d4science`)

Harvest **public catalogue items** on the VRE (gCat / documented dataset list). Drop workspace files, private VREs, and d4science.org marketing.

## Semantic MediaWiki (`smw`)

```text
GET https://host/w/api.php?action=ask&query=[[Category:Dataset]]
```

Keep pages typed as Dataset (or the site’s equivalent category). Drop ordinary wiki articles. `api.php` without a dataset query is not a harvest.

## Strapi (`strapi`) and Tablion (`tablion`)

Public **dataset** content-type REST only (`/api/datasets` or the type the catalog documents). Drop posts, users, and admin. Tablion is Aristotle’s data-portal product — harvest its public dataset API, not every MDR object ([harvest-metadata.md](harvest-metadata.md)).

Copernicus CDS (`copernicuscds`): [harvest-earthdata.md](harvest-earthdata.md#copernicus-cds-copernicuscds). Discovery fingerprints: [discovery-opendata.md](discovery-opendata.md).

## Portals without a dataset API

Liferay, POMOSAM, oPortal, OGD India, Seoul plaza, Drupal, and WordPress are covered above when a list exists. If there is still no machine-readable catalog, stop. Generic DCAT paths: `/catalog.xml`, `/data.json` ([harvest-protocols.md](harvest-protocols.md#dcat-and-datajson)).

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
