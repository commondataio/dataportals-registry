# Harvesting search engines, ML catalogs, and other types

Catalog types without a dedicated high-volume harvest page: **data search engines**, **ML catalogs**, **API directories**, **marketplaces**, **dataset lists**, and `software.id: custom`. Type rules: [catalog-types.md](catalog-types.md). Finding them: [discovery-other.md](discovery-other.md). Overview: [harvest.md](harvest.md).

GET public URLs only. Stop on `401`/`403`. Do not invent a new `software.id`.

## Data search engines

Aggregators search **other** catalogs (Idra, national dataset search, harvested unions).

**Prefer harvesting the source portals** listed in this registry. Harvesting the aggregator duplicates those datasets and loses the true `uid` of the source catalog.

If the user explicitly wants the federation view:

- **Idra** (`idra`): `/Idra/api/v1/` dataset search. Keep federated **datasets**. Record the source catalog URL when Idra provides it.
- **OpenAIRE** (`openaire`): Graph/CONNECT **datasets** only — [harvest-opendata.md](harvest-opendata.md#openaire-openaire). Prefer source IRs.
- **Aleph** (`aleph`): see [below](#aleph-aleph).
- **custom** search engines: use their documented search API; store `source` identifiers.

Do not add harvested member catalogs as new registry YAML (that is [discovery.md](discovery.md)).

## Aleph (`aleph`)

OCCRP-style investigative collections.

```text
GET https://host/api/2/collections
```

Keep **collections** that are dataset corpora. Drop entity/document search hits (`/api/2/entities`) as datasets. Page collection results; do not crawl every PDF. Skip aleph.occrp.org if you only needed the existing registry record.

## Machine learning catalogs

## OpenML (`openmlorg`)

```text
GET https://www.openml.org/api/v1/json/data/list/limit/100/offset/0
```

Keep **datasets** (`data`). Drop tasks, flows, runs, and setups. Do not harvest every OpenML task as a dataset. Skip cloning openml.org if you only needed the existing registry record — harvest **contents** when the user asked.

**Galaxy** (`galaxy`): public **data libraries** are the dataset catalog. Histories and workflow runs are not. Prefer [harvest-scientific.md](harvest-scientific.md) unless `catalog_type` is Machine learning catalog.

Hugging Face, Kaggle, and Papers with Code are usually **one** registered hub. Harvest their public dataset APIs if asked; do not add per-user spaces as catalogs.

## API catalogs

The product is a **list of APIs**, not datasets. Harvest API entries (name, docs URL, publisher) only when the user wants an API inventory. Store the API’s stable id + the catalog `uid` ([harvest-identifiers.md](harvest-identifiers.md)). A CKAN Action API stays an open-data harvest ([harvest-opendata.md](harvest-opendata.md)).

## Data marketplaces

Public catalog of datasets for sale or license. Harvest **public** listing APIs only (dataset id, title, landing URL). Stop on paywalls and `401`. Do not scrape prices, buyer lists, or sample files into this repository. `access_mode` on the **catalog** record is often `restricted` — still harvest metadata if it is public.

## Datasets lists

HTML tables, spreadsheets, GitHub inventories (`catalog_type: Datasets list`). One row / bullet with a dataset title + URL = one dataset. Skip the wrapping README as a dataset. No CMS API — parse the published file the catalog `link` points at, not a site-wide scrape.

## Custom software (`custom`)

About one catalog in eight. Do not guess CKAN or DSpace filters.

1. GET `endpoints[]` if any.
2. Try generic public lists: `/data.json`, `/catalog.json`, `/catalog.xml`, `/api`, `/api/docs`.
3. If the site is DCAT, keep `dcat:Dataset` only.
4. If nothing machine-readable exists, stop. Do not HTML-scrape the whole CMS from this repo’s workflows.

## Related

- [harvest.md](harvest.md)
- [harvest-opendata.md](harvest-opendata.md)
- [harvest-scientific.md](harvest-scientific.md)
- [harvest-protocols.md](harvest-protocols.md)
- [harvest-incremental.md](harvest-incremental.md)
- [harvest-identifiers.md](harvest-identifiers.md)
- [harvest-output.md](harvest-output.md)
- [discovery-other.md](discovery-other.md)
- [apidetect.md](apidetect.md)
- [agents/harvest.md](agents/harvest.md)
