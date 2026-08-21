# Agent guide: harvesting datasets from catalogs

List **datasets inside** registered catalogs via their public APIs. Human narrative: [harvest.md](../harvest.md). Per type: [scientific](../harvest-scientific.md), [opendata](../harvest-opendata.md), [geoportals](../harvest-geoportals.md), [indicators](../harvest-indicators.md), [metadata](../harvest-metadata.md), [other](../harvest-other.md). Shared protocols: [harvest-protocols.md](../harvest-protocols.md). Incremental: [harvest-incremental.md](../harvest-incremental.md). Identifiers: [harvest-identifiers.md](../harvest-identifiers.md). EO: [harvest-earthdata.md](../harvest-earthdata.md). Biodiversity: [harvest-biodiversity.md](../harvest-biodiversity.md). Viewers: [harvest-viewers.md](../harvest-viewers.md). Output: [harvest-output.md](../harvest-output.md).

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
3. Confirm `software.id`. If `custom`, do not guess a CKAN/DSpace filter — inspect the live API once or stop. Apply a platform recipe by **hostname** when the catalog is still `custom` (IPUMS, OpenAIRE EXPLORE, RADAR, Yoda, DHIS2, Symbiota). Do not filter exports on a `software.id` that is missing from `data/software/`.

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

| If `software.id` is | Dataset filter |
|---------------------|----------------|
| `dataverse` | `/api/search?q=*&type=dataset` |
| `dspace` / `dspacecris` | `/server/api/discover/search/objects?dsoType=ITEM&f.entityType=Dataset,equals` (or `dc.type` / OAI set) |
| `invenio` / `inveniordm` | `/api/records?q=metadata.resource_type.type:dataset` |
| `eprints` | OAI or `/cgi/exportview/type/dataset/JSON/dataset.js` |
| `hyrax` | `/catalog.json?f[human_readable_type_sim][]=Dataset` |
| `opus` | OAI set `doc-type:researchdata` |
| `pure` | `/sitemap/datasets.xml` or `/en/datasets/` — not `/publications/` |
| `ipt` | `/inventory/dataset` or `/rss.do` — Darwin Core archives, not occurrences |
| `thredds` | `/thredds/catalog.xml` — recurse `catalogRef`; harvest dataset nodes |
| `erddap` | `/erddap/info/index.json` — `datasetID` rows |
| `ckan` / `dkan` / `datapress` | `package_search` (packages, not resources) |
| `opendatasoft` | `/api/explore/v2.1/catalog/datasets` |
| `socrata` | `/api/catalog/v1?only=datasets` |
| `datafair` | `/data-fair/api/v1/datasets` |
| `triplydb` | `/_api/facets/datasets` |
| `scicat` | Native datasets API — little extra filtering |
| `seek` | `/data_files.json` / assays — not SOP-only pages |
| `geonetwork` | CSW `GetRecords`; keep `hierarchyLevel` dataset/series |
| `geonode` | `/api/datasets/` (v4) or `/api/layers/` (v3) — not maps |
| `stacserver` | `/collections` (items only if that is the grain) |
| `arcgisserver` | `/arcgis/rest/services?f=pjson` — Feature/Map/Image, not GPServer |
| `pygeoapi` / `wis20box` | `/collections?f=json` |
| `lizmap` / `qwc2` / `mapserver` | WMS GetCapabilities layers |
| `geomapfish` | `/themes` JSON layers |
| `oskari` | `GetMapLayers` action |
| `esrigeo` | `/rest/metadata/search` or CSW |
| `masterportal` / `mapstore` / `terria` | Theme/catalog JSON layers — not tiles ([harvest-viewers.md](../harvest-viewers.md)) |
| `pxweb` | Walk `/api/v1/` — tables (`type: t`), not folders |
| `eurostat` / `ecb` / `ilostat` | SDMX **dataflow** list (`eurostat` dissemination API, `data-api.ecb.europa.eu`, `sdmx.ilo.org`) — not observation `/data` |
| `dataworldbankorg` | `/v2/indicator` — not country time-series queries |
| `whoint` | GHO `Indicator` list — not observation rows |
| `databisorg` | `stats.bis.org` SDMX `/dataflow` — not POST `/api/v0/search` |
| `datauniceforg` | `sdmx.data.unicef.org` dataflows — not country profiles |
| `datagovmy` | Catalogue **ids** from the UI/docs — `/data-catalogue?id=` is observations, not a list |
| `opensdg` | Indicator JSON under `/data/` |
| `nada` | `/index.php/api/catalog/search` — studies, not videos |
| `ifremercatalog` | SEANOE OAI `ListRecords` — datasets, not every file |
| `fairdatapoint` | RDF catalog root; follow `dcat:dataset` |
| `fusionregistry` | SDMX dataflows, not every codelist |
| `openmlorg` | OpenML `data/list` — not tasks/runs |
| `idra` | Federation search only if asked; prefer source catalogs |
| `aleph` | `/api/2/collections` — collections, not every document |
| `gvsigonline` | Published project layers or GeoServer GetCapabilities |

**By hostname** (often still `custom` in exports — do not filter DuckDB on these ids until they exist in `data/software/`):

| Product | Dataset filter |
|---------|----------------|
| RADAR (`/radar/api/datasets`) | Dataset JSON / OAI — [harvest-scientific.md](../harvest-scientific.md#radar-radar) |
| Yoda public landing | Published vault DOIs — not `/research/` |
| DHIS2 | `/api/dataSets.json` / indicators — not orgUnits |
| IPUMS (`*.ipums.org`) | Collection/sample metadata — not a finished extract |
| OpenAIRE EXPLORE/CONNECT | Graph `search/datasets` — not publications |
| Symbiota | `/collections/datasets/rsshandler.php` — not occurrences |

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
- [apidetect.md](../apidetect.md)
- [query.md](query.md)
- [discover.md](discover.md)
