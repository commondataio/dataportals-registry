# Datasheet for dataportals-registry

This document follows the spirit of [Datasheets for Datasets](https://arxiv.org/abs/1803.09010) (Gebru et al.) and describes the registry as a dataset for downstream users and agents.

## Motivation

### For what purpose was the dataset created?

The dataportals-registry catalogs metadata about open data portals, geoportals, scientific data repositories, indicators catalogs, and related data infrastructure. It supports discovery, mapping the global open-data landscape, and feeding unified search/indexing projects (Common Data Index / open search engine pillar).

### Who created the dataset and on behalf of which entity?

Maintained by the [Common Data Index](https://github.com/commondataio) community. Individual catalog records are contributed via pull requests and automated enrichment pipelines.

## Composition

### What do the instances represent?

Each record describes one data catalog or repository: name, URL, owner, geographic coverage, software platform, API status, identifiers (Wikidata, Re3Data, etc.), and optional enrichment fields.

### How many instances?

Approximately **14,470** verified entity records (early 2026), plus scheduled/unverified entries in `data/scheduled/`. Export counts are listed in [README.md](README.md#data-exports).

### What data does each instance consist of?

YAML source files validated against `data/schemes/catalog.json`. Required fields include `id`, `uid`, `name`, `link`, `catalog_type`, `access_mode`, `status`, `software`, `owner`, and `coverage`.

## Collection process

### How was the data associated with each instance acquired?

- Manual curation and community contributions
- Bulk imports from public lists (CKAN ecosystem, STAC index, Dataverse installations, INSPIRE geoportals, etc.; see README data sources)
- Automated sync scripts (`sync_ckan_ecosystem.py`, enrichment from Re3Data)
- Quality checks and fix scripts (`scripts/builder.py analyze-quality`, `scripts/fix_*_issues.py`)

### Over what timeframe was the data collected?

Ongoing since 2023. Records include `status` (active/inactive/scheduled) but not uniform `last_verified_at` timestamps across all entries.

## Preprocessing / labeling

### Was any preprocessing or cleaning done?

Yes. Schema validation, software normalization, subregion ISO 3166-2 checks, URL format validation, duplicate detection, and priority-based fix scripts. Quality reports live in `dataquality/`.

## Distribution

### How is the dataset distributed?

- Git repository (YAML source)
- JSONL, Parquet, DuckDB, and zstd-compressed exports under `data/datasets/`
- License: **CC-BY 4.0** for data; MIT for code

### Maintenance and update cadence

- Continuous via pull requests
- Exports regenerated with `python scripts/builder.py build`
- Quality analysis on demand and in CI (`analyze-quality` + regression baseline)
- URL liveness probes weekly (report-only phase)

## Recommended uses

- Catalog discovery and landscape analysis
- Training or evaluating metadata extraction / classification systems
- Joining with external registries (Re3Data, Wikidata) via `identifiers`
- Building downstream search indexes (with awareness of biases below)

## Known limitations and bias

### Geographic coverage bias

Coverage is **uneven by country**. United States records are heavily over-represented (~31% of entity records), followed by France, Spain, United Kingdom, Canada, and Germany. Many countries have sparse or no coverage.

### Metadata completeness

Not all records have descriptions, endpoints, tags, or verified API status. Scheduled entries are explicitly unverified.

### URL liveness

`link` fields are format-validated but not guaranteed reachable. Use `dataquality/liveness_report.jsonl` for probe results; many portals block automated requests (classified as `inconclusive`, not `dead`).

### Software detection

Software platform is inferred or contributor-supplied and may be outdated after migrations.

### Scope

This repository is **reference data only**. Runtime query APIs and MCP servers are maintained in separate repositories.

## Citation

See [CITATION.cff](CITATION.cff).

## Feedback

Report issues via [GitHub Issues](https://github.com/commondataio/dataportals-registry/issues). Security concerns: [SECURITY.md](SECURITY.md).
