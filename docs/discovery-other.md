# Discovering search engines, ML catalogs, API directories, and marketplaces

How to find catalog types that do not have a dedicated high-volume software page: **Data search engine**, **Machine learning catalog**, **API Catalog**, and **Data marketplace**. Search-engine syntax: [discovery-search-tools.md](discovery-search-tools.md). Overview: [discovery.md](discovery.md). Type rules: [catalog-types.md](catalog-types.md).

These types are uncommon compared with open data, geo, and scientific repositories. Prefer an existing `software.id` when the product matches; otherwise use `custom`. Do not invent a new software ID for a one-off site.

## Data search engines (`search/`)

Sites whose **primary** product is search across other catalogs (aggregators). They score lower on [trust-score.md](trust-score.md).

**Idra** (`idra`) is a shared Open Data Federation Platform — fingerprints live in [discovery-opendata.md](discovery-opendata.md#idra). Typical `catalog_type` is **Data search engine**, not Open data portal.

**OpenAIRE** (`openaire`) is the other high-volume shared search stack: EXPLORE is the global Graph UI; CONNECT hosts national and community gateways (`netherlands.openaire.eu`, Canada.EXPLORE, and similar). Docs: [graph.openaire.eu/docs](https://graph.openaire.eu/docs/).

**Confirm OpenAIRE:** the UI searches the OpenAIRE Graph (publications, datasets, software, organisations). Register EXPLORE once and each distinct **national/community gateway**. Do not add a single research-product landing page.

| Tool | Query |
|------|-------|
| Google | `"OpenAIRE" (Explore OR CONNECT OR "research portal") -site:openaire.eu/about` |
| Google | `site:openaire.eu (Explore OR CONNECT)` |
| Censys | `web.names: "openaire.eu"` |

Other aggregators (national dataset search, harvested CKAN unions, commercial catalog search) are usually `software.id: custom`.

**Confirm:** the UI searches or harvests **other** catalogs. If the site hosts its own datasets as the main product, use Open data portal / Geoportal / Scientific instead.

| Tool | Query |
|------|-------|
| Google | `"open data" (search engine OR aggregator OR "dataset search")` plus a country name |
| Google | `"IdraPortal" OR "Open Data Federation"` |

Do not register harvested source catalogs a second time. Duplicate-check each underlying portal `link`.

## Machine learning catalogs (`ml/`)

Public catalogs of ML datasets or models (not a single Kaggle notebook, not a model card).

Shared software that sometimes maps here:

| `software.id` | When to use | Hunt notes |
|---------------|-------------|------------|
| `openmlorg` | OpenML instance | Site: [openml.org](https://www.openml.org). Confirm `/api/v1/` or the public dataset/task UI. Most national copies are already registered. |
| `galaxy` | Public Galaxy with data libraries | See [discovery-scientific.md](discovery-scientific.md). Prefer Scientific unless ML datasets are the primary product. |

Hugging Face, Kaggle, Papers with Code, and similar **global** hubs are usually already in the registry as single catalogs — do not add per-user spaces or per-dataset pages.

Regional challenge platforms (Zindi, AIcrowd, SIGNATE, Grand Challenge, CodaLab/Codabench, and national Chinese hubs such as Baidu AI Studio, BAAI, OpenXLab) are typically `software.id: custom`. Register **one catalog per hub**, not per competition or dataset.

| Tool | Query |
|------|-------|
| Google | `"OpenML" (datasets OR tasks) -site:openml.org -site:github.com` |
| Google | `"machine learning" ("data catalog" OR "dataset catalog")` plus an institution name |

## API catalogs (`api/`)

Directories of APIs (developer portals that list many APIs with docs and keys), not a CKAN Action API on an open-data site.

There is no high-volume shared `software.id` for this type in `data/software/`. Use `custom` unless a named platform definition already exists. A CKAN/Socrata/OpenDataSoft site stays **Open data portal** even if it has an API.

**Confirm:** a browsable list of APIs is the product. Skip a single REST endpoint with no catalog.

| Tool | Query |
|------|-------|
| Google | `"API catalog" OR "API directory" OR "developer portal" (datasets OR government)` plus a country name |

## Data marketplaces (`marketplace/`)

Commercial markets that sell or license datasets. Access is often `restricted`.

Use `custom` unless the vendor already has a software definition. Do not scrape prices or attempt paid-only listings.

**Confirm:** a public catalog of datasets for sale or licensed reuse. Skip procurement portals and app stores.

| Tool | Query |
|------|-------|
| Google | `"data marketplace" OR "data shop" (datasets OR geospatial)` plus a country name |

## Datasets lists (`other/` with type Datasets list)

Simple pages that list datasets without a full portal CMS (spreadsheet catalogs, HTML tables, GitHub data lists). Usually `software.id: custom`.

**Confirm:** a reusable list of multiple datasets with links or files. Skip a single CSV or a blog post.

| Tool | Query |
|------|-------|
| Google | `"list of datasets" OR "data inventory" (government OR open)` plus a country or city |

## General research repositories

Broad institutional research repos that are not clearly Dataverse/DSpace/Invenio. Prefer a named `software.id` from [discovery-scientific.md](discovery-scientific.md) (including Islandora, Samvera, Haplo, Worktribe). If none match, `custom` and `catalog_type: General research repository` or Scientific data repository per the **primary** UI.

## Custom software (`custom`) {#custom}

About one catalog in eight has no shared product ID. Use `custom` when two independent fingerprints do not match a `data/software/` definition. Hunt with the generic URL patterns in the type guides, not a software name.

**Decision tree**

1. Run the probe table in [discovery.md](discovery.md#identify-the-software) (plus the type guide for the primary UI).
2. If two signals match a named ID, use that ID — do not leave `custom`.
3. If the host is a viewer wrapping GeoServer/QGIS Server, register the **viewer** ([one catalog per public product](discovery.md#one-catalog-per-public-product)).
4. If nothing matches: `custom`, `api: false` unless you found a public list (`data.json`, DCAT, OAI, CSW, STAC).
5. Do not invent a new `software.id` for a one-off. Propose a software YAML only when several independent installations share a product ([software-taxonomy.md](software-taxonomy.md)).

Harvest: [harvest-other.md](harvest-other.md#custom).

## Related

- [discovery.md](discovery.md)
- [discovery-opendata.md](discovery-opendata.md) (Idra)
- [discovery-scientific.md](discovery-scientific.md) (OpenML, Galaxy)
- [harvest-other.md](harvest-other.md)
- [harvest.md](harvest.md)
- [catalog-types.md](catalog-types.md)
- [software-taxonomy.md](software-taxonomy.md)
