# Agent guide: harvesting datasets from catalogs

List **datasets inside** registered catalogs via their public APIs. Human narrative: [harvest.md](../harvest.md). Per type: [scientific IRs](../harvest-scientific.md), [domain scientific](../harvest-scientific-domain.md), [opendata](../harvest-opendata.md), [geoportals](../harvest-geoportals.md), [indicators](../harvest-indicators.md), [metadata](../harvest-metadata.md), [other](../harvest-other.md). Shared protocols: [harvest-protocols.md](../harvest-protocols.md). Incremental: [harvest-incremental.md](../harvest-incremental.md). Identifiers: [harvest-identifiers.md](../harvest-identifiers.md). EO: [harvest-earthdata.md](../harvest-earthdata.md). Biodiversity: [harvest-biodiversity.md](../harvest-biodiversity.md). Viewers: [harvest-viewers.md](../harvest-viewers.md). Output: [harvest-output.md](../harvest-output.md).

This is **not** catalog discovery ([discover.md](discover.md)) and **not** registry query ([query.md](query.md)).

## Goal

For catalogs the user named (or a scoped DuckDB selection), produce dataset identifiers with:

- catalog `uid` / `id` / `link`
- `software.id`
- native dataset id and/or DOI/handle ([harvest-identifiers.md](../harvest-identifiers.md))
- the type filter you applied
- skip counts (publications, files, showcases)

Do not write dataset YAML into this repository. Do not invent `uid` for datasets.

## Before calling APIs

1. Read [llms.txt](https://github.com/datenoio/dataportals-registry/blob/main/llms.txt) if you have not already.
2. Resolve catalogs from **exports** (`datasets.duckdb` / `full.parquet`). Use `endpoints[]` when present.
3. Confirm `software.id`. If `custom`, do not guess a CKAN/DSpace filter — inspect the live API once or stop. Do not filter exports on a `software.id` that is missing from `data/software/`.

```sql
SELECT id, uid, name, link,
       json_extract_string(software, '$.id') AS software_id,
       endpoints
FROM catalogs
WHERE id = 'examplegov'
   OR lower(link) LIKE '%example.gov%';
```

## Order of work

1. **Identify** (OAI Identify, CKAN `status_show`, Dataverse `info/version`, DSpace `/server/api`).
2. **List type vocabularies** (OAI `ListSets`, search facets, CKAN `fq` types).
3. **Apply the platform dataset filter** from the harvest guides. Do not page an unfiltered IR search.
4. **Paginate** with the documented cursor (`start`, `page`, `resumptionToken`). Small page size. Incremental later: [harvest-incremental.md](../harvest-incremental.md).
5. **Drop** publications, theses, files-under-datasets, showcases, harvest sources ([keep vs drop](../harvest.md#keep-vs-drop-shared-vocabulary)).
6. **Emit** one JSON record per kept dataset plus skip counts ([harvest-output.md](../harvest-output.md)).

## Platform shortcuts

Open the harvest heading from [software-index.md](../software-index.md). Do not invent filters for `custom`.

| If `software.id` is | Dataset filter (then follow the harvest page) |
|---------------------|-----------------------------------------------|
| `dataverse` | `/api/search?q=*&type=dataset` |
| `dspace` / `dspacecris` | `f.entityType=Dataset` or OAI `ListSets` |
| `invenio` / `inveniordm` | `/api/records?q=metadata.resource_type.type:dataset` |
| `ckan` / `dkan` | `package_search` (packages, not resources) |
| `opendatasoft` | `/api/explore/v2.1/catalog/datasets` |
| `socrata` | `/api/catalog/v1?only=datasets` |
| `geonetwork` | CSW `GetRecords`; `hierarchyLevel` dataset/series |
| `stacserver` | `/collections` (items only if that is the grain) |
| `arcgisserver` | `/arcgis/rest/services?f=pjson` — not GPServer |
| `pxweb` | `/api/v1/` tables (`type: t`), not folders |
| `custom` | [harvest-other.md](../harvest-other.md#custom) decision tree |

If the filter returns zero hits, inspect **one** unfiltered sample and `ListSets` / facets before concluding the catalog has no data. Local labels include Forschungsdaten, Research Data, and numeric WEKO3 item types.

## Accept / reject (dataset records)

**Accept** when the API object is a dataset (or data collection) with a stable id.

**Reject:** article, thesis, poster, presentation, person, project, org unit, CKAN resource row, Dataverse `type=file`, showcase, harvest source, login-only metadata, WMS tiles, ArcGIS GPServer, STAC items when collections are the grain, PxWeb folders, SDMX codelists-as-datasets, aggregator duplicates of source portals, IPUMS extracts, DHIS2 analytics cells, OpenAIRE publications.

## Do not

- Walk `data/entities/**/*.yaml` to find catalogs
- Add harvested datasets as registry YAML
- Bypass `401`/`403` or guess API keys (Pure `/ws/api/` often needs a key — use public OAI/sitemap instead)
- Unscoped crawls or HTML scrapers for the whole web
- Treat Idra/federations as a substitute for harvesting member catalogs unless the user asked for the federation view

## Related

- [harvest.md](../harvest.md)
- [harvest-scientific.md](../harvest-scientific.md)
- [harvest-scientific-domain.md](../harvest-scientific-domain.md)
- [harvest-opendata.md](../harvest-opendata.md)
- [harvest-geoportals.md](../harvest-geoportals.md)
- [harvest-indicators.md](../harvest-indicators.md)
- [harvest-metadata.md](../harvest-metadata.md)
- [harvest-other.md](../harvest-other.md)
- [harvest-protocols.md](../harvest-protocols.md)
- [harvest-incremental.md](../harvest-incremental.md)
- [harvest-earthdata.md](../harvest-earthdata.md)
- [harvest-biodiversity.md](../harvest-biodiversity.md)
- [harvest-viewers.md](../harvest-viewers.md)
- [harvest-identifiers.md](../harvest-identifiers.md)
- [harvest-output.md](../harvest-output.md)
- [software-index.md](../software-index.md)
- [apidetect.md](../apidetect.md)
- [query.md](query.md)
- [discover.md](discover.md)
