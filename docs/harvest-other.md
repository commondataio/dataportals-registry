# Harvesting search engines, ML catalogs, and other types

Catalog types without a dedicated high-volume harvest page: **data search engines**, **ML catalogs**, **API directories**, **marketplaces**, **dataset lists**, and `software.id: custom`. Type rules: [catalog-types.md](catalog-types.md). Finding them: [discovery-other.md](discovery-other.md). Overview: [harvest.md](harvest.md).

GET public URLs only. Stop on `401`/`403`. Do not invent a new `software.id`.

## Data search engines

Aggregators search **other** catalogs (Idra, national dataset search, harvested unions).

**Prefer harvesting the source portals** listed in this registry. Harvesting the aggregator duplicates those datasets and loses the true `uid` of the source catalog.

If the user explicitly wants the federation view:

- **Idra** (`idra`): `/Idra/api/v1/` dataset search. Keep federated **datasets**. Record the source catalog URL when Idra provides it.
- **OpenAIRE** — [below](#openaire). Prefer source IRs.
- **Aleph** (`aleph`): see [below](#aleph).
- **custom** search engines: use their documented search API; store `source` identifiers.

Do not add harvested member catalogs as new registry YAML (that is [discovery.md](discovery.md)).

## OpenAIRE (`openaire`) {#openaire}

EXPLORE / CONNECT gateways over the OpenAIRE Graph. Filter exports on `software.id = 'openaire'`.

```text
GET https://api.openaire.eu/search/datasets
```

Keep Graph **datasets** (research products typed as dataset). Drop publications, software, and org units. For a **CONNECT** community portal, use that gateway’s search/API with the community filter — do not dump the whole European graph. Prefer harvesting **source** IRs from this registry when you need publisher-level ids. Stop on `401`.

## Aleph (`aleph`) {#aleph}

OCCRP-style investigative collections.

```text
GET https://host/api/2/collections
```

Keep **collections** that are dataset corpora. Drop entity/document search hits (`/api/2/entities`) as datasets. Page collection results; do not crawl every PDF. Skip aleph.occrp.org if you only needed the existing registry record.

## Machine learning catalogs

OpenML, Galaxy, and similar hubs list **datasets** (or data libraries), not tasks, runs, or user spaces. Regional challenge platforms (Zindi, AIcrowd, SIGNATE, Grand Challenge, CodaLab) are usually `custom` — harvest the hub’s public **dataset** list, not every competition submission.

## OpenML (`openmlorg`) {#openmlorg}

```text
GET https://www.openml.org/api/v1/json/data/list/limit/100/offset/0
```

Keep **datasets** (`data`). Drop tasks, flows, runs, and setups. Do not harvest every OpenML task as a dataset. Skip cloning openml.org if you only needed the existing registry record — harvest **contents** when the user asked.

## Galaxy (`galaxy`) {#galaxy}

Public **data libraries** are the dataset catalog. Histories and workflow runs are not. Prefer [harvest-scientific.md](harvest-scientific.md) unless `catalog_type` is Machine learning catalog.

Hugging Face, Kaggle, and Papers with Code are usually **one** registered hub. Harvest their public dataset APIs if asked; do not add per-user spaces as catalogs.

## API catalogs

The product is a **list of APIs**, not datasets. Harvest API entries (name, docs URL, publisher) only when the user wants an API inventory. Store the API’s stable id + the catalog `uid` ([harvest-identifiers.md](harvest-identifiers.md)). A CKAN Action API stays an open-data harvest ([harvest-opendata.md](harvest-opendata.md)).

## Data marketplaces

Public catalog of datasets for sale or license. Harvest **public** listing APIs only (dataset id, title, landing URL). Stop on paywalls and `401`. Do not scrape prices, buyer lists, or sample files into this repository. `access_mode` on the **catalog** record is often `restricted` — still harvest metadata if it is public.

## Datasets lists {#datasets-lists}

HTML tables, spreadsheets, GitHub inventories (`catalog_type: Datasets list`). One row / bullet with a dataset title + URL = one dataset. Skip the wrapping README as a dataset. No CMS API — parse the published file the catalog `link` points at, not a site-wide scrape.

## Custom software (`custom`) {#custom}

About one catalog in eight has no shared product ID. Do not guess CKAN, DSpace, or GeoNetwork filters.

**Decision tree**

1. Confirm `software.id` is `custom` in exports (or two independent fingerprints failed in [discovery.md](discovery.md)).
2. GET `endpoints[]` if any. Use those URLs first.
3. Protocol fallback (stop at the first public list):
   - DCAT / `data.json` / `/catalog.json` — keep `dcat:Dataset` only ([harvest-protocols.md](harvest-protocols.md#dcat))
   - OAI-PMH `Identify` → `ListSets` → dataset-named sets ([harvest-scientific.md](harvest-scientific.md#dspace))
   - CSW `GetRecords` / STAC `/collections` / OGC API `/collections?f=json` ([harvest-geoportals.md](harvest-geoportals.md), [harvest-protocols.md](harvest-protocols.md))
   - CKAN-shaped `/api/3/action/package_search` only if the JSON is actually CKAN (`"help"` + `"success"`) — then the record should not stay `custom`
4. If the site is an HTML table, spreadsheet, or GitHub inventory, parse that file only ([Datasets lists](#datasets-lists)).
5. **Stop** when none of those exist. Do not HTML-scrape the whole CMS from this repository’s workflows. Report `login` on `401`/`403`. A correct empty harvest is valid.

**Worked examples (high-count `custom` families)**

| Family | Registry grain | Harvest | Stop |
|--------|----------------|---------|------|
| EMBL-EBI resources (`www.ebi.ac.uk/pride`, `/ena`, `/gwas`, …) | One YAML per **resource**, not ebi.ac.uk | That resource’s public dataset/study list or documented REST | The EBI homepage, gene pages, every file |
| NCBI (`/sra`, `/geo`, `/datasets`, PubChem, …) | One YAML per **database** | That database’s public dataset/accession catalog | ncbi.nlm.nih.gov chrome, BLAST jobs |
| Hugging Face / Kaggle / Papers with Code | One hub catalog | Public **dataset** API/list | Notebooks, models, competitions, user spaces |
| Zindi / AIcrowd / SIGNATE | One challenge hub | Public **dataset** list | Submissions, leaderboards |
| Municipal Excel / HTML inventory | The file the `link` points at | Rows with title + URL | The wrapping CMS |

Do not invent a new `software.id` for a one-off.

## Related

- [harvest.md](harvest.md)
- [harvest-opendata.md](harvest-opendata.md)
- [harvest-scientific.md](harvest-scientific.md)
- [harvest-scientific-domain.md](harvest-scientific-domain.md)
- [harvest-protocols.md](harvest-protocols.md)
- [harvest-incremental.md](harvest-incremental.md)
- [harvest-identifiers.md](harvest-identifiers.md)
- [harvest-output.md](harvest-output.md)
- [discovery-other.md](discovery-other.md)
- [apidetect.md](apidetect.md)
- [agents/harvest.md](agents/harvest.md)
