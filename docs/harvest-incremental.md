# Incremental harvesting

A **full** harvest lists every public dataset. An **incremental** harvest lists only records that appeared or changed since the last successful run. Use this page after you have a first snapshot from a [type](harvest.md) or [protocol](harvest-protocols.md) guide.

Store checkpoints **outside** this repository (your index, object store, or harvest DB). Do not write dataset YAML here.

## What to persist per catalog

| Field | Why |
|-------|-----|
| Catalog `uid` / `id` | Join key back to this registry |
| Native dataset id (and DOI/handle if present) | Dedup — [harvest-identifiers.md](harvest-identifiers.md) |
| `software.id` and the filter you used | Replay |
| Last successful timestamp or token | Next incremental |
| Skip counts | Spot a broken filter |

Do not invent `cdi########` ids for datasets.

## Prefer server-side “since”

| Protocol / API | Incremental hook |
|----------------|------------------|
| OAI-PMH | `from=` / `until=` (ISO date) on `ListRecords` |
| CKAN | `fq=metadata_modified:[SINCE TO *]` or sort `metadata_modified desc` and stop |
| DSpace 7 | `lastModified` / discover query with a date facet |
| InvenioRDM | `q=updated:>=SINCE` (ISO) |
| Dataverse | `sort=dateSort` / `fq=dateSort:[SINCE TO *]` |
| CSW | `Modified` / `RevisionDate` PropertyIsGreaterThan in `GetRecords` |
| STAC | `datetime=SINCE/..` on `/search` (collections still preferred for grain) |
| Socrata | `updated_at` on `/api/catalog/v1` |
| OpenDataSoft | `modified` on explore API |
| ERDDAP | Compare `info/index.json` `datasetID` set; some servers expose `dataTimestamp` |
| ArcGIS Hub | `modified` on search |
| PxWeb | No standard “since” — re-walk tables; diff table ids |
| SDMX dataflow list | Re-list `/dataflow`; diff ids. Do not incremental-page observation cubes |
| World Bank / GHO indicators | Re-list indicator APIs; diff indicator ids |
| openEO | Re-list `/collections`; diff collection ids. Do not incremental-page `/jobs` |
| Breedbase BrAPI | Re-list `/brapi/v2/studies` (and trials); diff study ids |
| ESGF esg-search | `from`/`to` on Solr when documented; else re-query and diff `dataset_id` |

If the API has **no** date filter, harvest identifiers only (cheap list), then GET metadata for ids not in your store. Do not re-download every observation cube.

## Tokens and paging

- OAI `resumptionToken` is for **one** ListRecords session. Do not save it across days; save `from=` instead.
- STAC / OGC API: follow `rel=next`. Do not invent page numbers.
- CKAN `start` is an offset; if the catalog mutates mid-crawl, prefer `metadata_modified` sort.
- Honor `Retry-After` and back off on `429`. Cap page size (`10`–`100`).

## First run vs later runs

1. **First run:** apply the dataset-type filter from the platform guide, page to completion, store ids + checkpoint time (use the server’s clock from `Date` / OAI `responseDate` when possible).
2. **Later runs:** same filter plus `from=` / `updated>=`. Union new ids; update changed ids; do not delete missing ids unless the user asked for a tombstone pass.
3. **Tombstones (optional):** a rare full harvest to mark disappeared datasets. IRs often keep withdrawn records as `unavailable` — treat that as a status, not a new dataset.

## Failures

- `401` / `403`: stop. Do not rotate keys.
- Empty incremental: inspect **one** unfiltered sample; the clock format may be wrong (`YYYY-MM-DD` vs full ISO).
- Huge “everything changed”: the server may ignore `from=`. Fall back to a full harvest once, then fix the filter.

## Related

- [harvest.md](harvest.md)
- [harvest-protocols.md](harvest-protocols.md)
- [harvest-identifiers.md](harvest-identifiers.md)
- [harvest-output.md](harvest-output.md)
- [agents/harvest.md](agents/harvest.md)
- [apidetect.md](apidetect.md)
