# Harvesting datasets from scientific repositories

Institutional repositories and CRIS portals mix **publications, theses, software, and research data**. Harvest the public API, then **filter to datasets**. Overview and keep/drop vocabulary: [harvest.md](harvest.md). Finding installations: [discovery-scientific.md](discovery-scientific.md). Domain stacks: [discovery-scientific-domain.md](discovery-scientific-domain.md).

Replace `https://host` with the catalog `link` origin (no trailing slash unless the path needs it). GET only. Stop on `401`/`403`.

Use `endpoints[]` from the registry when present ([apidetect.md](apidetect.md)). Paths below are the defaults those maps probe.

| Page | Use when |
|------|----------|
| This page | Institutional repositories and CRIS (Dataverse, DSpace, Invenio, EPrints, Pure, RADAR, Yoda, …) |
| [Domain repositories](harvest-scientific-domain.md) | IPT, Symbiota, THREDDS, ERDDAP, Breedbase, Tripal, VEuPathDB, MassBank, ioChem-BD, ESGF, ALA, SciCat-adjacent stacks |

All `software.id` values: [software-index.md](software-index.md).

## Mixed vs dataset-native

| Class | `software.id` (typical) | Filter needed? |
|-------|-------------------------|----------------|
| Mixed IR / CRIS | `dspace`, `dspacecris`, `invenio`, `inveniordm`, `eprints`, `hyrax`, `samvera`, `islandora`, `opus`, `mycore`, `phaidra`, `weko3`, `pure`, `esploro`, `elsevierdigitalcommons`, `figshare`, `haplo`, `worktribe`, `omegapsir`, `librecat`, `vufind` | **Yes** — publications dominate |
| Dataset-native | `dataverse`, `radar`, `yoda`, `instdb` on this page; IPT/THREDDS/Breedbase/ESGF and similar on [harvest-scientific-domain.md](harvest-scientific-domain.md) | Little or none — still skip files, occurrences, and login-only rows |

## OAI-PMH fallback (any IR)

When REST search has no type filter, use OAI-PMH ([harvest-protocols.md](harvest-protocols.md#oai-pmh)).

1. `GET https://host/oai/request?verb=Identify` (DSpace) or the Identify URL in `endpoints[]`.
2. `verb=ListSets` — keep `setSpec` values that mean data (`ResearchData`, `doc-type:researchdata`, `datasets`, `Dataset`, `Forschungsdaten`). Ignore `com_` / `col_` sets that are the whole repository.
3. `verb=ListRecords` with `metadataPrefix=oai_dc` and `set` equal to that `setSpec`. Follow `resumptionToken`.
4. If no dataset set exists, harvest `oai_dc` and **keep** records whose `dc:type` (or DataCite `resourceTypeGeneral`, or COAR URI) matches the [keep list](harvest.md#keep-vs-drop-shared-vocabulary).

Common Identify paths: `/oai?verb=Identify`, `/oai/request?verb=Identify`, `/cgi/oai2?verb=Identify`, `/oai2d`, `/ws/oai?verb=Identify`, `/api/oai?verb=Identify`.

Do not treat `ListIdentifiers` titles as datasets. Do not harvest `metadataPrefix=marc21` as a substitute for type.

## Dataverse (`dataverse`) {#dataverse}

Native search already distinguishes objects. Prefer **datasets**, not files or sub-dataverses.

**List datasets:**

```text
GET https://host/api/search?q=*&type=dataset&per_page=100&start=0
```

Page with `start`. `total_count` is in the JSON envelope.

**Also useful:** `/api/info/version`, OAI `/oai?verb=Identify`.

**Drop:** `type=file` (file hits under a dataset), `type=dataverse` (collections), `/dataset.xhtml?persistentId=` as a crawl seed (that is one record). Harvest the installation root from the registry, then this search API.

Docs: [guides.dataverse.org](https://guides.dataverse.org).

## DSpace 7+ (`dspace`) {#dspace}

DSpace items are publications, theses, and datasets in one index.

**Unfiltered (do not use as the crawl):** `/server/api/discover/search/objects`

**Worked example A — DSpace 7 entity type**

Filter to dataset entities (DSpace-CRIS / configurable entities):

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

**Worked example B — classic OAI ListSets (`dc.type`)**

DSpace 6 and 7 fallback when REST has no entity type:

```text
GET https://host/oai/request?verb=Identify
GET https://host/oai/request?verb=ListSets
GET https://host/oai/request?verb=ListRecords&metadataPrefix=oai_dc&set=col_123456789_4
```

Keep `setSpec` values whose name is dataset / research data / Forschungsdaten. Ignore `com_` community sets that are the whole repository. Then `ListRecords` with that `set`. If no dataset set exists, harvest `oai_dc` and keep records whose `dc:type` matches the [keep list](harvest.md#keep-vs-drop-shared-vocabulary).

**DSpace 6 REST:** `/rest/items` has no reliable type filter. Prefer OAI as above, or skip 6.x hosts without a dataset collection.

**Drop:** `dsoType=COMMUNITY` / `COLLECTION`, researcher `Person` / `OrgUnit` / `Project` (CRIS), bitstream URLs.

## DSpace-CRIS (`dspacecris`) {#dspacecris}

Same REST/OAI as [DSpace](#dspace). Prefer `f.entityType=Dataset,equals` (or the campus ResearchData entity). Drop CRIS `Person`, `OrgUnit`, and `Project` objects.

## Invenio (`invenio`) {#invenio}

Classic Invenio (not RDM). `/api/records` returns **all** record types. Filter to datasets the same way as [InvenioRDM](#inveniordm), then confirm the UI is not InvenioRDM-branded.

**Drop:** `publication`, `presentation`, `poster`, `image`, `video`, `lesson`, `other`. `software` is not a dataset. OAI is often `/oai2d`.

## InvenioRDM (`inveniordm`) {#inveniordm}

`/api/records` returns **all** record types (publication, dataset, software, poster, …).

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

## EPrints (`eprints`) {#eprints}

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

## Hyrax (`hyrax`) {#hyrax}

Blacklight JSON catalog. Work types include GenericWork, Dataset, Etd, Image, FileSet.

```text
GET https://host/catalog.json?f[human_readable_type_sim][]=Dataset&per_page=100&page=1
```

If that facet is empty, try `f[resource_type_sim][]=Dataset` or `f[has_model_ssim][]=Dataset`. FileSets are files, not datasets.

## Samvera (`samvera`) {#samvera}

Same Blacklight harvest as [Hyrax](#hyrax) when the UI is Samvera without Hyrax branding.

Islandora (`islandora`) is Drupal+Fedora: harvest the public JSON:API or Solr only when a **dataset** content model / collection exists. Prefer Islandora over raw `fedora` `/fcrepo/rest`. See [Islandora](#islandora).

## OPUS (`opus`) {#opus}

German IRs. The dataset document type is usually `researchdata` / `ResearchData`.

```text
GET https://host/oai?verb=ListSets
```

Look for `doc-type:researchdata` (spelling varies). Then:

```text
GET https://host/oai?verb=ListRecords&metadataPrefix=oai_dc&set=doc-type:researchdata
```

Solr UI often supports a doctype facet (`doctypefq=researchdata`). Thesis-only OPUS hosts have no dataset set — skip them for a data crawl (they can still be valid **catalog** records).

## MyCoRe (`mycore`) {#mycore}

```text
GET https://host/api/v2/objects
GET https://host/servlets/OAIDataProvider?verb=ListSets
```

Classification values are local (`mir_types`, `state`). Filter to data/Forschungsdaten classes after reading one object and `ListSets`. Unfiltered `/api/v2/objects` is the whole IR.

## PHAIDRA (`phaidra`) {#phaidra}

```text
GET https://host/api/search/select?q=*:*&rows=0
GET https://host/api/oai?verb=Identify
```

Add a type constraint once you see stored fields (often `cmodel`, `dc_type`, or `object_type`). Example patterns to try: `cmodel:*Dataset*`, `dc_type:dataset`. Drop image/book/thesis cmodels.

## WEKO3 (`weko3`) {#weko3}

Item **type IDs are per instance**. The registry probe uses `type=` on `/api/records/` — that integer is **not** portable.

1. Open the public search UI or API and list item types.
2. Find the id for research data / 研究データ / Dataset.
3. Crawl `/api/records/?type=ITEM_TYPE_ID&page=1&size=20` (replace `ITEM_TYPE_ID`).

Without a resolved type id, you will ingest articles and reports.

## Elsevier Pure (`pure`) {#pure}

The public **portal** lists `/en/datasets/` (locale prefix varies: `/de/datasets/`, `/da/datasets/`). Publications live under `/publications/` and `/persons/`.

**Prefer the datasets channel:**

```text
GET https://host/sitemap/datasets.xml
GET https://host/en/datasets/?search=&format=rss
```

OAI: `/ws/oai?verb=Identify` then `ListSets` for a datasets set.

Pure Web Services (`/ws/api/datasets`) often need an API key. If you get `401`, use the public portal/OAI/sitemap. Do not guess keys.

**Drop:** `/publications/`, activities, prizes, student theses unless typed as datasets.

## Esploro (`esploro`) {#esploro}

Research outputs include datasets as one resource type.

The registry map checks `/view/google/siteindex.xml` for `/dataset/` paths. Use that sitemap when it exists.

Otherwise use the public research search with a **datasets** facet (UI labels: Dataset, Research data). The SOAP/WADL probe `/esplorows/rest/research/simpleSearch` is a capability URL, not a full crawl.

**Drop:** articles, books, conference papers, ETDs in the same index.

## Elsevier Digital Commons (`elsevierdigitalcommons`) {#elsevierdigitalcommons}

Collections mix articles and data series. OAI: `/do/oai/?verb=ListSets`. Harvest only sets whose names are data/datasets/statistics — not the whole IR.

Sitemap `/sitemap/index` can list every series; still skip photograph and journal series.

## Figshare (`figshare`) {#figshare}

Institutional Figshare (not every figshare.com article). Item types are numeric.

| `item_type` | Meaning |
|-------------|---------|
| 3 | Dataset — **keep** |
| 4 | Fileset — **keep** (collection of files) |
| 9 / 18 | Code / software — not a dataset |
| 6, 8, 5, 7 | Paper, thesis, poster, presentation — **drop** |

GraphQL/search endpoints vary by tenant. Prefer the institution’s public API or sitemap entries under `/articles/dataset/`. Do not crawl `figshare.com/articles` globally.

## Haplo (`haplo`) {#haplo}

Output types include publications and datasets. Use the public catalog/OAI and keep records typed as dataset / research data. Skip grant and HR objects. Skip haplo.com marketing hosts.

## Worktribe (`worktribe`) {#worktribe}

Public catalog/OAI (`/oaiprovider?verb=Identify`). Keep dataset / research data. Skip grant/HR objects and worktribe.com marketing.

## Omega-PSIR (`omegapsir`) {#omegapsir}

CRIS with separate publications vs data modules when configured. Prefer URLs/APIs under a datasets/research-data listing. A global publication search is the wrong crawl.

## VuFind (`vufind`) {#vufind}

Discovery layer over mixed IRs. Add a format/type facet (`format:Dataset`, `document_type:dataset`) **before** paging. Unfiltered VuFind search is the library catalog, not a data catalog.

## LibreCat (`librecat`) {#librecat}

Same facet-first harvest as [VuFind](#vufind) when the public UI is LibreCat.

## InstDB (`instdb`) {#instdb}

FairStack institutional research-data nodes. Harvest the public dataset/API list on the node (`/api` when present). Skip fairstack.cn marketing and per-file URLs.

## NYU Data Catalog (`nyudatacatalog`) {#nyudatacatalog}

Medical-library dataset catalog (schema.org DataCatalog JSON-LD on listing pages). Harvest **Dataset** objects from JSON-LD or the public search listing. Drop expert/person pages. Drupal JSON:API only if a dataset bundle exists.

## DataLad (`datalad`) {#datalad}

Harvest the published **catalog** dataset list (`catalog.json` or the catalog site’s dataset pages), not git-annex keys.

## GIN (`gin`) {#gin}

Gogs `/api/v1/repos/search` — each **repository** can be a dataset; do not harvest git objects. Stop on `401`.

## HUBzero (`hubzero`) {#hubzero}

Scientific gateway. Harvest public **resources** typed as datasets/databases. Drop tools, tickets, and login-only groups.

## LinkAhead (`linkahead`) {#linkahead}

CaosDB REST (`/api/v1/`). Query Record types that are datasets/collections. Drop files and properties as extra datasets.

## Fedora (`fedora`) {#fedora}

Use Fedora LDP `/fcrepo/rest` (or `/rest`) **only** when Fedora is the public catalog. Prefer Hyrax/Islandora/PHAIDRA recipes on the same host.

## Islandora (`islandora`) {#islandora}

Drupal+Fedora. Harvest Solr/REST with a Dataset content model — not every Drupal node. Prefer Islandora over raw [Fedora](#fedora).

## CONTENTdm (`contentdm`) {#contentdm}

Only when the site was accepted as a **dataset** catalog ([discovery-scientific.md](discovery-scientific.md)). `/digital/api/collections` plus OAI; keep statistical/climate collections, skip photo exhibits.

## Omeka S (`omekas`) {#omekas}

Only when accepted as a dataset catalog. `/api/items` filtered to Dataset / DataCatalog classes; skip exhibit images.

## OSF (`osf`) {#osf}

Harvest **institution** or named project catalogs only (`https://api.osf.io/v2/`). Keep nodes/registrations that are data. Do not crawl all of osf.io. Stop on `401`.

## Converis (`converis`) {#converis}

Clarivate CRIS. Same publication-vs-data problem as Pure: harvest **datasets**, not publications or persons. Public OAI/listing if present; stop on `/ws` keys.

## Djehuty (`djehuty`) {#djehuty}

4TU.ResearchData stack. Harvest the public dataset search (Invenio-like `resource_type` filter when exposed).

## RADAR (`radar`) {#radar}

FIZ Karlsruhe research data repositories (RADAR Cloud and RADAR Local). Filter exports on `software.id = 'radar'`.

```text
GET https://host/radar/api/datasets
GET https://host/oai/OAIHandler?verb=Identify
```

Already datasets (`totalHits` in the JSON). Page the API; keep dataset ids/DOIs. Skip a single `/radar/de/dataset/` landing page as a seed and the FIZ marketing site. OAI is a fallback. Discovery: [discovery-scientific.md](discovery-scientific.md#radar).

## Yoda (`yoda`) {#yoda}

Utrecht / SURF research-data vault on iRODS. Filter exports on `software.id = 'yoda'`. Harvest **published** vault datasets (DataCite DOI landing pages or the public catalog API in `endpoints[]`). Drop `/research/` collaboration collections and iRODS tickets. Stop on `401`. Do not list every file in a vault package.

## dLibra (`dlibra`) {#dlibra}

Polish digital library. Use OAI-PMH with a dataset / dane `set` or `dc:type` filter ([harvest-protocols.md](harvest-protocols.md#oai-pmh)). Skip manuscript/photo libraries that were never accepted as dataset catalogs.

## Dataset-native platforms (short)

Little publication noise. Still skip non-dataset objects.

| Platform | List | Notes |
|----------|------|-------|
| Dataverse | [above](#dataverse) | `type=dataset` only |
| SciCat (`scicat`) | [harvest-earthdata.md](harvest-earthdata.md#scicat) | Facility datasets; stop on `401` |
| RADAR (`radar`) | [above](#radar) | Already datasets; skip marketing and single landings |
| Yoda (`yoda`) | [above](#yoda) | Published datasets only; skip the authenticated vault |

Domain stacks (IPT, THREDDS, Breedbase, ESGF, …): [harvest-scientific-domain.md](harvest-scientific-domain.md). Omeka S and CONTENTdm: sections above.

## Pagination checklist

1. Read `total` / `nHits` / `page.totalPages` / OAI `resumptionToken` from the first response.
2. Cap page size; do not request `size=10000` on Solr-backed IRs.
3. Deduplicate on DOI, handle, or native id plus catalog `uid` ([harvest-identifiers.md](harvest-identifiers.md)). Emit [output records](harvest-output.md).
4. Re-run with `from=` (OAI) or `updated` sort for incremental harvests when the API supports it ([harvest-incremental.md](harvest-incremental.md)).

## Related

- [harvest.md](harvest.md)
- [harvest-opendata.md](harvest-opendata.md)
- [harvest-geoportals.md](harvest-geoportals.md)
- [harvest-scientific-domain.md](harvest-scientific-domain.md)
- [discovery-scientific.md](discovery-scientific.md)
- [harvest-protocols.md](harvest-protocols.md)
- [harvest-biodiversity.md](harvest-biodiversity.md)
- [harvest-earthdata.md](harvest-earthdata.md)
- [harvest-incremental.md](harvest-incremental.md)
- [harvest-identifiers.md](harvest-identifiers.md)
- [harvest-output.md](harvest-output.md)
- [apidetect.md](apidetect.md)
- [agents/harvest.md](agents/harvest.md)
