# dataportals-registry
Registry of data portals, catalogs, data repositories and related data infrastructure.

This is the first pillar of the open search engine project. Other pillars include:
* **registry of all catalogs** (this one)
* datasets raw metadata database
* unified dataset search index and search engine
* datasets backup and file cache

Please take a look at [project mindmap](/assets/commondataindex.png) to see its goals and structure.

Source of truth is YAML under `data/entities/`. Consumers should use the exported JSONL, Parquet, and DuckDB datasets. Production search APIs live in [dateno-api](https://github.com/datenoio/dateno-api); this repository does not host a query API or MCP server.

## Documentation

Published internals for humans and coding agents:

- Site (GitHub Pages): <https://datenoio.github.io/dataportals-registry/>
- Source markdown: [`docs/`](docs/)
- Agent index: [`llms.txt`](llms.txt) (also served at `/dataportals-registry/llms.txt`)
- Local preview: `cd website && npm install && npm run start`

Working notes and one-off analyses live in [`devdocs/`](devdocs/), not on the site.

## What kind of data catalogs collected?

This registry includes description of the following data catalogs:
* Open data portals
* Geoportals
* Scientific data repositories
* Indicators catalogs
* Microdata catalogs
* Machine learning catalogs
* Data search engines
* API Catalogs
* Data marketplaces
* Metadata catalogs
* Other 



## Inspiration

This project is inspired by Re3Data and FAIRsharing. The key difference is the focus on open data as a broad topic, not just open research data.

## How this repository is organized

Catalog YAML lives under `data/entities/{COUNTRY}/{Federal|SUBREGION}/{type}/{id}.yaml`. Layout, field rules, and software IDs: [docs/directory-layout.md](docs/directory-layout.md), [docs/data-model.md](docs/data-model.md), [docs/software-taxonomy.md](docs/software-taxonomy.md).

### Example

FAA Open Data Portal (`data/entities/US/Federal/opendata/catalogdatafaagov.yaml`):

```yaml
access_mode:
- open
api: true
api_status: active
catalog_type: Open data portal
id: catalogdatafaagov
link: https://catalog.data.faa.gov
name: Federal Aviation Administration Open Data Portal
owner:
  name: Federal Aviation Administration
  type: Central government
  location:
    country:
      id: US
      name: United States
    level: 20
software:
  id: ckan
  name: CKAN
status: active
uid: cdi00005263
```

### Datasets and code

Generated exports live in `data/datasets/`. Rebuild from the repository root:

```bash
python scripts/builder.py build
```

Do not hand-edit `data/datasets/`.

## Data exports

Last published snapshot (**v1.13.0**, 2026-08-20):

- `data/datasets/catalogs.jsonl` (+ `.zst`): 17,718 catalog records
- `data/datasets/software.jsonl` (+ `.zst`): 192 software/platform definitions
- `data/datasets/scheduled.jsonl` (+ `.zst`): 0 scheduled sources to crawl
- `data/datasets/full.jsonl` (+ `.zst`): 17,718 combined entities + scheduled records
- `data/datasets/full.parquet`, `data/datasets/datasets.duckdb`: analytics-friendly exports

Current source YAML (2026-08-21, not yet rebuilt into exports):

- `data/entities/`: **18,208** catalog records
- `data/scheduled/`: **88** unverified records (mostly FAIR Data Point and MapServer)
- `data/software/`: **205** platform definitions
- **217** country/territory folders

Run `python scripts/builder.py build` to refresh JSONL, Parquet, and DuckDB to match source. All `.zst` files can be decompressed with `unzstd file.zst` (zstd). Filter by catalog type or software in DuckDB rather than looking for pre-sliced dumps.

## Discovery

How to find catalogs **already in this registry**:

**By geography**  
- Entity YAMLs live under `data/entities/COUNTRY_CODE/` (e.g. `US`, `FR`, `BR`).  
- Use `Federal/` for federal-level catalogs and subregion codes for states/regions (e.g. `US-CA`, `US-VA`, `BR-SP`).  
- One YAML per catalog; filename is the catalog `id`.

**By catalog type**  
- Under each country (or `scheduled/`), type folders: `opendata/`, `geo/`, `scientific/`, `microdata/`, `indicators/`, `ml/`, `search/`, `api/`, `marketplace/`, `other/`.

**By software taxonomy**  
- Software definitions in `data/software/` include:
  - `category`: domain family (Open data portal, Geoportal, Scientific data repository, etc.)
  - `subtype`: product form/deployment model (for example `data_portal_platform`, `managed_saas_service`, `protocol_or_api_server`)
- Use `subtype` for cross-category comparisons such as self-hosted platforms vs managed SaaS vs protocol-first components.

**From export artifacts**  
- **catalogs.jsonl** / **full.jsonl**: line-delimited JSON (entities only, or entities + scheduled).  
- **full.parquet**, **data/datasets/datasets.duckdb**: for analytics; query with DuckDB or pandas.

Example DuckDB query (all CKAN catalogs in the US from the full export). The built DuckDB store normalizes nested fields to JSON strings, so filter on the `software` and `coverage` string columns:

```sql
SELECT id, name, link
FROM catalogs
WHERE software LIKE '%"id":"ckan"%'
  AND coverage LIKE '%"id":"US"%';
```

How to find catalogs **not yet in this registry** (search lists, identify software, avoid duplicates): [docs/discovery.md](docs/discovery.md) for humans and [docs/agents/discover.md](docs/agents/discover.md) for coding agents. Search-engine recipes (Google, Censys, Shodan, FOFA): [docs/discovery-search-tools.md](docs/discovery-search-tools.md). Configure those tools in Cursor, ChatGPT, and other LLM clients: [docs/discovery-agent-tools.md](docs/discovery-agent-tools.md). Per-platform queries: [open data](docs/discovery-opendata.md), [geoportals](docs/discovery-geoportals.md), [scientific](docs/discovery-scientific.md), [metadata](docs/discovery-metadata.md), [indicators and microdata](docs/discovery-indicators.md), [search / ML / API / marketplaces](docs/discovery-other.md). Endpoint fill: [docs/apidetect.md](docs/apidetect.md). URL liveness: [docs/liveness.md](docs/liveness.md).

## Data Quality and Validation

The repository includes tools for analyzing and validating data quality:

- **Duplicate Detection**: Exact and normalized URL duplicates (`DUPLICATE_LINK` / `DUPLICATE_LINK_NORMALIZED`) plus same-id collisions (`DUPLICATE_RECORD_ID`), with a preferred keeper (https, non-www, non-Unknown path)
- **Path and owner consistency**: File-path country vs owner/coverage country (`PATH_COUNTRY_MISMATCH`); regional/local owners require `owner.location.level=30` and matching subregion directory
- **Owner type vocabulary**: Canonical values and synonyms in `data/reference/owner_types.yaml` (`OWNER_TYPE_NONCANONICAL` / `INVALID_OWNER_TYPE`)
- **Schema Validation**: Validation against JSON schemas in `data/schemes/`
- **Data Quality Reports**: Analysis reports written to the `dataquality/` directory (by rule, priority, and country)

CI guards integrity-track regressions via `dataquality/baseline_counts.json`.

To run data quality analysis:

```bash
python scripts/builder.py analyze-quality
```

Reports are written to `dataquality/` (e.g. `full_report.txt`, `primary_priority.jsonl`, and per-country/per-priority breakouts).

See [devdocs/quality-fix-workflow.md](devdocs/quality-fix-workflow.md), [docs/metadata-quality.md](docs/metadata-quality.md), and `dataquality/full_report.txt` for current findings. Helper scripts (`scripts/fix_*_issues.py`) can apply automated fixes based on reported priorities.

## Agent and governance documentation

- [llms.txt](llms.txt) — concise index for LLM agents
- [DATASHEET.md](DATASHEET.md) — dataset characteristics, bias, and limitations
- [CITATION.cff](CITATION.cff) — academic citation metadata
- [SECURITY.md](SECURITY.md) — vulnerability reporting
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) — community standards

## Re3Data enrichment

Catalogs with a re3data identifier can be enriched into `_re3data`. Preview with `python scripts/re3data_enrichment.py enrich --dry-run`. Full workflow: [docs/re3data.md](docs/re3data.md).

## CKAN ecosystem synchronization

Discover CKAN sites from [ecosystem.ckan.org](https://ecosystem.ckan.org/dataset/ckan-sites-metadata). Preview with `python scripts/sync_ckan_ecosystem.py --dry-run`. Full workflow: [docs/ckan-sync.md](docs/ckan-sync.md).

## How to contribute?

If you find any mistake or you have an additional data catalog to add, please generate [pull request](https://github.com/datenoio/dataportals-registry/pulls) or write an [issue](https://github.com/datenoio/dataportals-registry/issues).

## Data sources

Following data sources used:

* Stac Catalogs	https://stacindex.org/catalogs - done
* Dataverse Installations	https://iqss.github.io/dataverse-installations/data/data.json - done
* Open Data Inception	https://data.opendatasoft.com/explore/dataset/open-data-sources%40public/information/ - done
* CKAN Portals across the world	https://datashades.info/ - done
* CKAN Ecosystem Sites	https://ecosystem.ckan.org/dataset/ckan-sites-metadata - done (automated sync)
* Geonetwork Showcase	https://github.com/geonetwork/doc/blob/develop/source/annexes/gallery/gallery-urls.csv - done
* PxWeb examples	https://www.scb.se/en/services/statistical-programs-for-px-files/px-web/pxweb-examples/ - done
* DKAN Community	https://getdkan.org/community - done
* Junar Clients	https://junar.com/customers/ - done
* Datashades data portals list	https://datashades.info/api/portal/list - done
* OpenSDG installations	https://open-sdg.org/community - done
* MyCore Installations	https://www.mycore.de/site/applications/list/ - done
* Elsevier Pure installations - https://www.elsevier.com/solutions/pure/pure-in-action - done
* CoreTrustSeal Repositories https://amt.coretrustseal.org/certificates - done
* GeoOrchestra installations https://www.georchestra.org/community.html - done
* CKAN Ecosystem https://ecosystem.ckan.org
* EUDAT Repositories	https://b2find.eudat.eu/organization/
* Data.Europe.eu catalogues	https://data.europa.eu/data/catalogues?locale=en
* Re3Data	https://www.re3data.org/
* RISources	https://risources.dfg.de
* Spanish opendata initiatives https://datos.gob.es/en/accessible-initiatives
* INSPIRE Country catalogs	https://inspire-geoportal.ec.europa.eu/overview.html?view=thematicEuOverview&theme=none
* Socrata OpenDataNetwork	https://www.opendatanetwork.com/search?q= - done
* ArcGIS Hub search	https://hub.arcgis.com/ - done
* Brazilian Catalogs of geodata metadata https://inde.gov.br/Estatisticas/CatalogosMetadados
* Open Data Monitor (outdated, but useful) https://www.opendatamonitor.eu
* List of French open data catalogs https://airtable.com/shrWxHPi2XjLu9xtM/tblwklJPsyayeH5lX
* Brazilian local government (state and municipal) open data portals https://github.com/augusto-herrmann/transparencia-dados-abertos-brasil/blob/main/data/valid/brazilian-transparency-and-open-data-portals.csv
* Russian and CIS countries data catalogs https://datacatalogs.ru
* EntryScape customers (Sweden) https://entryscape.com/en/customers/ - done
* Geolode, catalog of open geodata websites https://geolode.org
* WebCommons Dataset subset http://webdatacommons.org/structureddata/2022-12/stats/schema_org_subsets.html
* Major Smart Cities with Open Data (updated 2019) https://rlist.io/l/major-smart-cities-with-open-data-portals
* Registry of Open Access Repositories http://roar.eprints.org
* IPT: Integrated Publishing Toolkit installations - https://www.gbif.org/ipt
* Geoblacklight showcase - https://geoblacklight.org/showcase/ - done

## License

Source code licensed under MIT license
Data licensed under CC-BY 4.0 license