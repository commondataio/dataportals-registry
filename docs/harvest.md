# Harvesting datasets from catalog APIs

This registry stores **catalogs** (portals, geoportals, repositories). It does **not** store the datasets inside those catalogs. To list or index datasets, harvest the catalog’s public API.

Two different jobs share the word *harvest*:

| Job | What you want | Where to go |
|-----|----------------|-------------|
| List **catalogs** in this registry | Country, type, `software.id`, `endpoints[]` | [query-examples.md](query-examples.md), [agents/query.md](agents/query.md) |
| List **datasets** inside a catalog | Records from the remote API, filtered to datasets | This page, then the platform guides |
| Find catalogs **not yet registered** | New portal URLs | [discovery.md](discovery.md) |

Coding agents: [agents/harvest.md](agents/harvest.md). Production harvesting for the Dateno stack lives in [reaper](https://github.com/datenoio/reaper) — these pages are the human/API recipes, not a crawler in this repository.

## Guides

| Guide | Use when |
|-------|----------|
| [Scientific repositories](harvest-scientific.md) | DSpace, Invenio, EPrints, Pure, Esploro, and other IRs that mix **publications, theses, software, and datasets** |
| [Open data portals](harvest-opendata.md) | CKAN, OpenDataSoft, Socrata, and similar — packages vs resources |
| [Geoportals](harvest-geoportals.md) | GeoNetwork CSW, GeoNode, ArcGIS, STAC, OGC API — layers vs services vs tiles |
| [Indicators and microdata](harvest-indicators.md) | PxWeb tables, SDMX dataflows, OpenSDG indicators, NADA studies |
| [Metadata catalogs](harvest-metadata.md) | FAIR Data Point DCAT, Aristotle MDR, Fusion Registry structure |
| [Search, ML, API, marketplaces](harvest-other.md) | Aggregators, OpenML, API directories, marketplaces, `custom` |
| [Protocols](harvest-protocols.md) | OAI-PMH, CSW, DCAT, STAC, SDMX, OGC, ArcGIS REST — grain that is shared across products |
| [Incremental harvests](harvest-incremental.md) | `from=`, `metadata_modified`, STAC `datetime`, checkpoints |
| [Earth observation](harvest-earthdata.md) | THREDDS, ERDDAP, STAC collections, Open Data Cube, Copernicus |
| [Biodiversity and genomics](harvest-biodiversity.md) | IPT, Symbiota, ALA, GBIF datasets, Ensembl species |
| [Map viewers](harvest-viewers.md) | QWC2, Masterportal, Lizmap, MapProxy — layers not tiles |
| [Dataset identifiers](harvest-identifiers.md) | Native id + catalog `uid`; DOI/handle; do not mint `cdi########` for datasets |
| [Harvest output](harvest-output.md) | JSON record shape, skip counts, empty-harvest checklist |

**Pick a guide:** mixed IR → scientific. CKAN/Socrata-like → opendata. CSW/STAC/ArcGIS catalog → geoportals. Map UI only → viewers. Gridded EO → earthdata. IPT/Symbiota/GBIF → biodiversity. Tables/dataflows → indicators. FDP/MDR → metadata. `from=` / checkpoints → incremental. Shared OAI/CSW/DCAT grain → protocols. Aggregators/ML/`custom` → other. JSON records / empty results → output.

Do not apply IR publication filters to WMS or PxWeb. Do not treat CSW **service** records or STAC **items** as datasets unless that is the catalog grain.

## Workflow

1. **Pick catalogs from exports**, not by walking YAML. Prefer `status = active`, `api = true`, and a known `software.id`.
2. **Read `endpoints[]`** on the record. Those URLs were probed for this registry. If empty, use the platform default paths in the guides — still GET only, on that host.
3. **Identify**, then **filter to datasets** (server-side query or OAI set), then **paginate**.
4. **Store dataset identifiers** (native id, DOI/handle when present) plus the catalog `uid` ([harvest-identifiers.md](harvest-identifiers.md)). Emit the [output record](harvest-output.md). Do not write dataset YAML into this repository.
5. **Stop** on `401` / `403`. Do not guess API keys or follow login forms.
6. Later runs: reuse the same filter with a date/token checkpoint ([harvest-incremental.md](harvest-incremental.md)).

```sql
SELECT id, uid, name, link,
       json_extract_string(software, '$.id') AS software_id,
       endpoints
FROM catalogs
WHERE catalog_type = 'Scientific data repository'
  AND status = 'active'
  AND json_extract_string(software, '$.id') IN (
    'dspace', 'dspacecris', 'invenio', 'inveniordm', 'eprints',
    'hyrax', 'pure', 'esploro', 'opus', 'elsevierdigitalcommons',
    'weko3', 'phaidra', 'figshare', 'haplo', 'worktribe', 'mycore',
    'ipt', 'thredds', 'erddap'
  )
LIMIT 50;
```

Filter `software.id` only with values that exist in `data/software/` (published catalog: **205** definitions). Recipes for RADAR, Yoda, DHIS2, IPUMS, OpenAIRE, and Symbiota still apply by **hostname** — those records are often `software.id: custom` in current exports. Do not write an unpublished id onto catalog YAML.

For open data or geo, change `catalog_type` and the `software.id` list (`ckan`, `geonetwork`, `stacserver`, …). Nested `software` / `endpoints` are JSON **strings** in DuckDB ([ai-consumers.md](ai-consumers.md)).

## Why scientific repositories need extra filters

Open-data portals (CKAN, Socrata) list datasets as the primary object. Institutional repositories and CRIS portals list **research outputs**: journal articles, theses, presentations, code, and — sometimes — datasets.

If you page `/api/records` or OAI `ListRecords` with no type filter, most hits are publications. The scientific guide is the filter cookbook.

**Prefer server-side filters** (search `q=`, facet, OAI `setSpec`). Client-side `dc:type` matching is the fallback when the API has no type parameter. Vocabularies differ per campus — always `ListSets` / inspect one sample record before a full crawl.

## Keep vs drop (shared vocabulary)

Keep a record when its type is clearly research **data** (including data papers only if they deposit data files — prefer the dataset record).

| Keep (examples) | Drop (examples) |
|-----------------|-----------------|
| Dataset, DataSet, Research Data, Forschungsdaten, ResearchData | Article, Journal Article, Review |
| Data collection, DataCollection, Database | Thesis, Dissertation, Doctoral thesis, Master thesis |
| Tabular data, Geospatial data, Census data | Conference paper, Presentation, Poster, Lecture |
| COAR `c_ddb1` (dataset) | COAR `c_6501` (journal article), `c_46ec` (thesis) |
| DataCite `resourceTypeGeneral=Dataset` | DataCite `Text`, `Image`, `Audiovisual`, `Other` |
| Figshare `item_type=3` (dataset), `4` (fileset) | Figshare paper, poster, presentation, thesis |

Also drop: user accounts, projects, org units, researcher profiles, harvest-source records, individual **files** when a parent **dataset** record exists (Dataverse `type=file` vs `type=dataset`).

Other catalog types use a different grain: CSW **dataset/series** not services; STAC **collections** not items; CKAN **packages** not resources; PxWeb **tables** not folders; IPT **archives** not occurrences ([harvest-protocols.md](harvest-protocols.md)).

Software/code and models are not datasets unless the catalog types them as data. Index them separately if you need them.

## Politeness

- GET public URLs only. Short timeout. One or two probes to learn paging, then polite page size (`10`–`100`).
- Honor `Retry-After` and back off on `429`.
- Do not write internet-wide scanners in this repository. Harvest **named catalogs** from the registry.
- Prefer OAI-PMH `ListRecords` with a `set` and resumption tokens over scraping HTML.
- After a catalog is registered here, `python scripts/apidetect.py detect-single CATALOG_ID --dryrun` can fill `endpoints[]` — that is not a dataset crawl.

## What not to store here

- Dataset-level YAML, CKAN packages, Dataverse studies, STAC items
- Copies of harvested JSON in `data/entities/`
- API keys, cookies, or session tokens

## Related

- [harvest-scientific.md](harvest-scientific.md)
- [harvest-opendata.md](harvest-opendata.md)
- [harvest-geoportals.md](harvest-geoportals.md)
- [harvest-indicators.md](harvest-indicators.md)
- [harvest-metadata.md](harvest-metadata.md)
- [harvest-other.md](harvest-other.md)
- [harvest-protocols.md](harvest-protocols.md)
- [harvest-incremental.md](harvest-incremental.md)
- [harvest-earthdata.md](harvest-earthdata.md)
- [harvest-biodiversity.md](harvest-biodiversity.md)
- [harvest-viewers.md](harvest-viewers.md)
- [harvest-identifiers.md](harvest-identifiers.md)
- [harvest-output.md](harvest-output.md)
- [agents/harvest.md](agents/harvest.md)
- [apidetect.md](apidetect.md)
- [when-to-use.md](when-to-use.md)
