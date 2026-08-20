# Exports

Generated artifacts live in `data/datasets/`. Rebuild with `python scripts/builder.py build`. Never hand-edit this directory.

## Primary dumps

| File | Contents |
|------|----------|
| `catalogs.jsonl` (+ `.zst`) | Verified entities only |
| `scheduled.jsonl` (+ `.zst`) | Unverified scheduled records (may be empty) |
| `full.jsonl` (+ `.zst`) | Entities + scheduled |
| `software.jsonl` (+ `.zst`) | Software / platform definitions |
| `full.parquet` | Analytics table of `full.jsonl` |
| `datasets.duckdb` | Tables `catalogs` and `software` |
| `catalogs.jsonld` | Optional; `build --jsonld` |

August 2026 snapshot: **17,718** catalog records, **192** software definitions.

Filter by catalog type or software in DuckDB / Parquet (see [query-examples.md](query-examples.md)); there are no pre-sliced `bytype/` or `bysoftware/` dumps.

Incidental files such as `software_stats.csv` or `fulldbreg.parquet` may appear in `data/datasets/` from older tooling — prefer the primary dumps above.

## Compression

`.zst` files are [zstandard](https://facebook.github.io/zstd/). Decompress with `unzstd file.zst` or stream them in Python via `zstandard`.

## DuckDB columns

`datasets.duckdb` table `catalogs` (August 2026 build). Nested objects are `VARCHAR` JSON strings; `api` is `BOOLEAN`.

| Column | JSONL type | DuckDB |
|--------|------------|--------|
| `id`, `uid`, `name`, `link`, `catalog_type`, `status`, `api_status`, `description` | string | VARCHAR |
| `api` | boolean | BOOLEAN |
| `access_mode`, `content_types`, `coverage`, `endpoints`, `identifiers`, `langs`, `owner`, `properties`, `rights`, `software`, `tags`, `topics`, `_re3data` | list/object | VARCHAR (JSON text) |

Table `software` keeps scalars as VARCHAR (including `has_api` / `has_bulk` as `Yes`/`No` strings). Nested `datatypes`, `metadata_support`, `owner`, `license` are JSON text.

`trust_score` is optional on YAML and may be absent from a given DuckDB build if no records in the snapshot have the field.

## JSON-LD / DCAT

`data/schemes/catalog.context.jsonld` maps fields to DCAT-AP, Dublin Core, schema.org, and the `cdi:` namespace. Emit a framed dump with:

```bash
python scripts/builder.py build --jsonld
```

| Catalog field | JSON-LD term |
|---------------|--------------|
| (type) | `dcat:DataCatalog` |
| `name` | `dct:title` |
| `description` | `dct:description` |
| `link` | `dcat:landingPage` |
| `owner` | `dct:publisher` |
| `rights` | `dct:rights` |
| `access_mode` | `dct:accessRights` |
| `identifier` | `dct:identifier` |
| `id` | `cdi:id` |
| `uid` | `cdi:uid` |
| `catalog_type` | `cdi:catalogType` |
| `status` | `cdi:status` |
| `software` | `cdi:software` |
| `coverage` | `cdi:coverage` |
| `endpoints` | `cdi:endpoints` |
| `identifiers` | `cdi:identifiers` |
| `api` | `cdi:hasApi` |
| `api_status` | `cdi:apiStatus` |
| `tags` | `cdi:tags` |
| `topics` | `cdi:topics` |
| `langs` | `cdi:langs` |
| `content_types` | `cdi:contentTypes` |
| `trust_score` | `cdi:trustScore` |
| `properties` | `cdi:properties` |
| `catalog_export` | `cdi:catalogExport` |
| `_re3data` | `cdi:re3dataEnrichment` |

`cdi:` is `https://commondata.io/ns/dataportals-registry#`.
