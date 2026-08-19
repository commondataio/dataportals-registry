# Agent guide: discovering catalogs

Find catalog installations that are **not yet in this registry**, then hand off to [contribute.md](contribute.md). Human narrative: [discovery.md](../discovery.md).

This is **not** the query workflow. To look up existing records, use [query.md](query.md).

## Goal

Produce a short list of verified candidate URLs with:

- `name`, `link`
- proposed `catalog_type` and `software.id`
- ISO country (and subregion when the owner is regional/local)
- whether the site already exists in exports

Do not invent `uid`. Do not add dataset-level records. Do not implement production search APIs here.

## Before probing the web

1. Read [llms.txt](https://github.com/datenoio/dataportals-registry/blob/main/llms.txt) if you have not already.
2. Duplicate-check **exports** (`data/datasets/datasets.duckdb` or `full.parquet`), then `data/scheduled/` if present.
3. If the user named a URL or domain, search that first and stop if it is already registered.

```sql
SELECT id, uid, name, link, catalog_type, status,
       json_extract_string(software, '$.id') AS software_id
FROM catalogs
WHERE lower(link) LIKE '%example.gov%'
   OR id = 'examplegov';
```

Match on hostname, not display name. `id` is not a URL.

## Discovery order

1. **Vendor and government lists** in [discovery.md](../discovery.md) and the README data-sources section — highest yield, fewest false positives.
2. **CKAN ecosystem sync** (preview only until the user wants files written):

   ```bash
   python scripts/sync_ckan_ecosystem.py --dry-run
   ```

3. **Targeted search** the user asked for (one country, one software, one city). Use local-language open-data terms and government TLDs.
4. **Endpoint probes** on the candidate host only (table below). GET, short timeout, public URLs.

Do not run internet-wide scanners, Shodan/Censys sweeps, or recursive crawls unless the user explicitly asks and the scope is a named set of hosts.

## Software probes

Set `software.id` only when a probe or page signal matches. Otherwise `custom`. Definitions: `data/software/` and [software-taxonomy.md](../software-taxonomy.md).

| If you see | `software.id` (typical) | `catalog_type` (typical) |
|------------|-------------------------|--------------------------|
| `/api/3/action/package_list` or `status_show` | `ckan` | Open data portal |
| `/api/explore/v2.1/catalog/datasets` | `opendatasoft` | Open data portal |
| `/api/views` (SODA) | `socrata` | Open data portal |
| `/srv/eng/csw` or `/srv/api` | `geonetwork` | Geoportal |
| `/api/layers/` | `geonode` | Geoportal |
| `/geoserver/ows` GetCapabilities | `geoserver` | Geoportal |
| `/rest/info?f=pjson` ArcGIS Server | `arcgisserver` | Geoportal |
| ArcGIS Hub search API / hub site | `arcgishub` | Geoportal or Open data portal (primary UI) |
| `/api/info/version` Dataverse | `dataverse` | Scientific data repository |
| OAI-PMH `Identify` + DSpace UI | `dspace` | Scientific data repository |
| `/api/v1/` PxWeb | `pxweb` | Indicators catalog |

Types: [catalog-types.md](../catalog-types.md). If a site is both a map viewer and a dataset portal, pick the **primary** product.

After a YAML file exists, optional endpoint fill:

```bash
python scripts/apidetect.py detect-single {id} --dryrun
```

## Accept / reject

**Accept** when all are true:

- Public HTTP(S) catalog UI or harvestable API
- Not a duplicate of `link` / same host catalog already in entities or scheduled
- Country (and subregion) can be determined from the owner
- Software is known or explicitly `custom`

**Reject** (do not add):

- Demo, template, or documentation-only sites
- Single file downloads with no catalog
- Sites that require authentication for any catalog listing
- Dataset records, CKAN packages, STAC items (out of scope)
- Guessed software IDs

## After a valid find

1. `python scripts/builder.py add-single --url … --scheduled` (preferred) or write YAML per [contribute.md](contribute.md).
2. `python scripts/builder.py assign`
3. `python scripts/builder.py validate-yaml --id {id}`
4. Cite `id` + `link` in the reply. List skipped duplicates with their existing `id`.

## Do not

- Walk `data/entities/**/*.yaml` to search; use exports
- Hand-edit `data/datasets/`
- Bypass `401`/`403`, guess API keys, or follow login forms
- Flood a host; one or two GETs per path is enough
- Commit generated dumps unless the user asked for a rebuild

## Related

- [discovery.md](../discovery.md)
- [contribute.md](contribute.md)
- [query.md](query.md)
- [cli.md](../cli.md)
