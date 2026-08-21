# Harvesting datasets from scientific repositories

Institutional repositories and CRIS portals mix **publications, theses, software, and research data**. Harvest the public API, then **filter to datasets**. Overview and keep/drop vocabulary: [harvest.md](harvest.md). Finding installations: [discovery-scientific.md](discovery-scientific.md).

Replace `https://host` with the catalog `link` origin (no trailing slash unless the path needs it). GET only. Stop on `401`/`403`.

Use `endpoints[]` from the registry when present ([apidetect.md](apidetect.md)). Paths below are the defaults those maps probe.

## Mixed vs dataset-native

| Class | `software.id` (typical) | Filter needed? |
|-------|-------------------------|----------------|
| Mixed IR / CRIS | `dspace`, `dspacecris`, `invenio`, `inveniordm`, `eprints`, `hyrax`, `samvera`, `islandora`, `opus`, `mycore`, `phaidra`, `weko3`, `pure`, `esploro`, `elsevierdigitalcommons`, `figshare`, `haplo`, `worktribe`, `omegapsir`, `librecat`, `vufind` | **Yes** — publications dominate |
| Dataset-native | `dataverse`, `radar`, `scicat`, `dataone`, `thredds`, `erddap`, `opendap`, `ipt`, `seek`, `icat`, `instdb` | Little or none — still skip files, collections, and login-only rows |

## OAI-PMH fallback (any IR)

When REST search has no type filter, use OAI-PMH.

1. `GET https://host/oai/request?verb=Identify` (DSpace) or the Identify URL in `endpoints[]`.
2. `verb=ListSets` — keep `setSpec` values that mean data (`ResearchData`, `doc-type:researchdata`, `datasets`, `Dataset`, `Forschungsdaten`). Ignore `com_` / `col_` sets that are the whole repository.
3. `verb=ListRecords` with `metadataPrefix=oai_dc` and `set` equal to that `setSpec`. Follow `resumptionToken`.
4. If no dataset set exists, harvest `oai_dc` and **keep** records whose `dc:type` (or DataCite `resourceTypeGeneral`, or COAR URI) matches the [keep list](harvest.md#keep-vs-drop-shared-vocabulary).

Common Identify paths: `/oai?verb=Identify`, `/oai/request?verb=Identify`, `/cgi/oai2?verb=Identify`, `/oai2d`, `/ws/oai?verb=Identify`, `/api/oai?verb=Identify`.

Do not treat `ListIdentifiers` titles as datasets. Do not harvest `metadataPrefix=marc21` as a substitute for type.

## Dataverse (`dataverse`)

Native search already distinguishes objects. Prefer **datasets**, not files or sub-dataverses.

**List datasets:**

```text
GET https://host/api/search?q=*&type=dataset&per_page=100&start=0
```

Page with `start`. `total_count` is in the JSON envelope.

**Also useful:** `/api/info/version`, OAI `/oai?verb=Identify`.

**Drop:** `type=file` (file hits under a dataset), `type=dataverse` (collections), `/dataset.xhtml?persistentId=` as a crawl seed (that is one record). Harvest the installation root from the registry, then this search API.

Docs: [guides.dataverse.org](https://guides.dataverse.org).

## DSpace 7+ (`dspace`) and DSpace-CRIS (`dspacecris`)

DSpace items are publications, theses, and datasets in one index.

**Unfiltered (do not use as the crawl):** `/server/api/discover/search/objects`

**Filter to dataset entities** (DSpace-CRIS / configurable entities):

```text
GET https://host/server/api/discover/search/objects?dsoType=ITEM&f.entityType=Dataset,equals&size=100&page=0
```

Some campuses name the entity `ResearchData` or `Product`. Inspect facets once:

```text
GET https://host/server/api/discover/search/objects?dsoType=ITEM&size=0
```

Read `_embedded.searchResult.page` and facet values for `entityType` / `dc.type`. If there is no entity type, filter Solr-style:

```text
GET https://host/server/api/discover/search/objects?dsoType=ITEM&query=dc.type:Dataset&size=100
```

Try `Forschungsdaten`, `Research Data`, and `Dataset` — values are local.

**DSpace 6:** `/rest/items` has no reliable type filter. Use OAI `ListSets` + `ListRecords`, or skip 6.x hosts without a dataset collection.

**Drop:** `dsoType=COMMUNITY` / `COLLECTION`, researcher `Person` / `OrgUnit` / `Project` (CRIS), bitstream URLs.

## Invenio and InvenioRDM (`invenio`, `inveniordm`)

`/api/records` returns **all** record types (publication, dataset, software, poster, …).

**InvenioRDM / Zenodo-like:**

```text
GET https://host/api/records?q=metadata.resource_type.type:dataset&size=100&page=1
```

If that query returns zero but the UI has a Dataset facet, try:

```text
GET https://host/api/records?q=metadata.resource_type.id:dataset&size=100
GET https://host/api/records?type=dataset&size=100
```

Follow `links.next`. Inspect `hits.hits[].metadata.resource_type`.

**Drop:** `publication`, `presentation`, `poster`, `image`, `video`, `lesson`, `other` unless you explicitly want those corpora. `software` is not a dataset.

OAI is often `/oai2d`. Skip zenodo.org if you only need institutional instances already in the registry.

Docs: [inveniordm.docs.cern.ch](https://inveniordm.docs.cern.ch).

## EPrints (`eprints`)

Every eprint has a `type` (`article`, `thesis`, `dataset`, `monograph`, …).

**Browse/export by type:**

```text
GET https://host/cgi/exportview/type/dataset/JSON/dataset.js
```

**Search:** `/cgi/search/archive/advanced` with `type=dataset` (parameter names vary; confirm on one host).

**REST:** `/rest/eprint/` plus the numeric eprint id (`.xml`) is per-record. For a crawl, OAI is better:

```text
GET https://host/cgi/oai2?verb=ListSets
GET https://host/cgi/oai2?verb=ListRecords&metadataPrefix=oai_dc&set=DATASET_SET
```

If there is no dataset set, ListRecords and keep `dc:type` = `dataset` / `Dataset`.

**Drop:** `article`, `thesis`, `book`, `conference_item`, `exhibition`, `performance`.

## Hyrax / Samvera (`hyrax`, `samvera`)

Blacklight JSON catalog. Work types include GenericWork, Dataset, Etd, Image, FileSet.

```text
GET https://host/catalog.json?f[human_readable_type_sim][]=Dataset&per_page=100&page=1
```

If that facet is empty, try `f[resource_type_sim][]=Dataset` or `f[has_model_ssim][]=Dataset`. FileSets are files, not datasets.

Islandora (`islandora`) is Drupal+Fedora: harvest the public JSON:API or Solr only when a **dataset** content model / collection exists. Prefer Islandora over raw `fedora` `/fcrepo/rest`.

## OPUS (`opus`)

German IRs. The dataset document type is usually `researchdata` / `ResearchData`.

```text
GET https://host/oai?verb=ListSets
```

Look for `doc-type:researchdata` (spelling varies). Then:

```text
GET https://host/oai?verb=ListRecords&metadataPrefix=oai_dc&set=doc-type:researchdata
```

Solr UI often supports a doctype facet (`doctypefq=researchdata`). Thesis-only OPUS hosts have no dataset set — skip them for a data crawl (they can still be valid **catalog** records).

## MyCoRe (`mycore`)

```text
GET https://host/api/v2/objects
GET https://host/servlets/OAIDataProvider?verb=ListSets
```

Classification values are local (`mir_types`, `state`). Filter to data/Forschungsdaten classes after reading one object and `ListSets`. Unfiltered `/api/v2/objects` is the whole IR.

## PHAIDRA (`phaidra`)

```text
GET https://host/api/search/select?q=*:*&rows=0
GET https://host/api/oai?verb=Identify
```

Add a type constraint once you see stored fields (often `cmodel`, `dc_type`, or `object_type`). Example patterns to try: `cmodel:*Dataset*`, `dc_type:dataset`. Drop image/book/thesis cmodels.

## WEKO3 (`weko3`)

Item **type IDs are per instance**. The registry probe uses `type=` on `/api/records/` — that integer is **not** portable.

1. Open the public search UI or API and list item types.
2. Find the id for research data / 研究データ / Dataset.
3. Crawl `/api/records/?type=ITEM_TYPE_ID&page=1&size=20` (replace `ITEM_TYPE_ID`).

Without a resolved type id, you will ingest articles and reports.

## Elsevier Pure (`pure`)

The public **portal** lists `/en/datasets/` (locale prefix varies: `/de/datasets/`, `/da/datasets/`). Publications live under `/publications/` and `/persons/`.

**Prefer the datasets channel:**

```text
GET https://host/sitemap/datasets.xml
GET https://host/en/datasets/?search=&format=rss
```

OAI: `/ws/oai?verb=Identify` then `ListSets` for a datasets set.

Pure Web Services (`/ws/api/datasets`) often need an API key. If you get `401`, use the public portal/OAI/sitemap. Do not guess keys.

**Drop:** `/publications/`, activities, prizes, student theses unless typed as datasets.

## Esploro (`esploro`)

Research outputs include datasets as one resource type.

The registry map checks `/view/google/siteindex.xml` for `/dataset/` paths. Use that sitemap when it exists.

Otherwise use the public research search with a **datasets** facet (UI labels: Dataset, Research data). The SOAP/WADL probe `/esplorows/rest/research/simpleSearch` is a capability URL, not a full crawl.

**Drop:** articles, books, conference papers, ETDs in the same index.

## Elsevier Digital Commons (`elsevierdigitalcommons`)

Collections mix articles and data series. OAI: `/do/oai/?verb=ListSets`. Harvest only sets whose names are data/datasets/statistics — not the whole IR.

Sitemap `/sitemap/index` can list every series; still skip photograph and journal series.

## Figshare (`figshare`)

Institutional Figshare (not every figshare.com article). Item types are numeric.

| `item_type` | Meaning |
|-------------|---------|
| 3 | Dataset — **keep** |
| 4 | Fileset — **keep** (collection of files) |
| 9 / 18 | Code / software — not a dataset |
| 6, 8, 5, 7 | Paper, thesis, poster, presentation — **drop** |

GraphQL/search endpoints vary by tenant. Prefer the institution’s public API or sitemap entries under `/articles/dataset/`. Do not crawl `figshare.com/articles` globally.

## Haplo (`haplo`) and Worktribe (`worktribe`)

Output types include publications and datasets. Use the public catalog/OAI (`/oaiprovider?verb=Identify` on Worktribe) and keep records typed as dataset / research data. Skip grant and HR objects. Skip haplo.com / worktribe.com marketing hosts.

## Omega-PSIR (`omegapsir`)

CRIS with separate publications vs data modules when configured. Prefer URLs/APIs under a datasets/research-data listing. A global publication search is the wrong crawl.

## VuFind / LibreCat (`vufind`, `librecat`)

Discovery layers over mixed IRs. Add a format/type facet (`format:Dataset`, `document_type:dataset`) **before** paging. Unfiltered VuFind search is the library catalog, not a data catalog.

## Dataset-native platforms (short)

Little publication noise. Still skip non-dataset objects.

| Platform | List | Notes |
|----------|------|-------|
| Dataverse | see above | `type=dataset` only |
| RADAR (`radar`) | `/radar/api/datasets` | Already datasets; skip a single `/radar/de/dataset/` landing page as a seed |
| SciCat (`scicat`) | `/api/v3/datasets` or `/api/v3/Datasets` | Facility datasets; may require token for full metadata — stop on `401` |
| DataONE (`dataone`) | MN/CN search | `formatType=DATA` when the API supports it |
| THREDDS (`thredds`) | `/thredds/catalog.xml` | Catalogs of **data services**, not papers |
| ERDDAP (`erddap`) | `/erddap/info/index.json` | Datasets table |
| IPT (`ipt`) | `/inventory/dataset` | Darwin Core archives |
| FAIRDOM-SEEK (`seek`) | investigations / data files API | Keep data files / assays, not SOP-only pages |
| ICAT (`icat`) | documented REST/OAI | Facility catalog; skip icatproject.org itself |
| InstDB (`instdb`) | node home / API | Institutional research data |

Omeka S and CONTENTdm: only harvest when the catalog was accepted as a **dataset** site ([discovery-scientific.md](discovery-scientific.md)). Filter item classes to Dataset / DataCatalog; skip exhibit images.

## Pagination checklist

1. Read `total` / `nHits` / `page.totalPages` / OAI `resumptionToken` from the first response.
2. Cap page size; do not request `size=10000` on Solr-backed IRs.
3. Deduplicate on DOI, handle, or native id plus catalog `uid`.
4. Re-run with `from=` (OAI) or `updated` sort for incremental harvests when the API supports it.

## Related

- [harvest.md](harvest.md)
- [harvest-opendata.md](harvest-opendata.md)
- [harvest-geoportals.md](harvest-geoportals.md)
- [discovery-scientific.md](discovery-scientific.md)
- [apidetect.md](apidetect.md)
- [agents/harvest.md](agents/harvest.md)
