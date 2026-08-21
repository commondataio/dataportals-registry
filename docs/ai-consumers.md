# AI consumer guide

Consumption contract for LLM agents, enrichment pipelines, and programmatic integrators. Installation and contribution: [README.md](https://github.com/datenoio/dataportals-registry/blob/main/README.md).

## Scope

### In scope

Metadata about **catalogs** (portals, geoportals, repositories, and related infrastructure):

- Identity: `id`, `uid`, `name`, `link`, `status`
- Classification: `catalog_type`, `software`, `tags`, `topics`, `content_types`
- Geography: `owner.location`, `coverage[]` (country, subregion, macroregion, level)
- Access: `access_mode`, `api`, `api_status`, `endpoints[]`, `rights`
- Crosswalks: `identifiers[]` (wikidata, re3data, fairsharing, …)
- Optional enrichment: `_re3data`, `trust_score`

### Out of scope

Do **not** expect:

- Dataset-level records (the contents of each catalog)
- A production HTTP search API or MCP server in this repository (use [dateno-api](https://github.com/datenoio/dateno-api) for search)
- Uniform `last_verified_at` timestamps on every record
- Balanced geographic coverage (US records are over-represented)

## Preferred access paths

| Method | Path |
|--------|------|
| DuckDB (preferred in-repo) | `data/datasets/datasets.duckdb` table `catalogs` |
| Parquet | `data/datasets/full.parquet` |
| JSONL entities | `data/datasets/catalogs.jsonl` (+ `.zst`) |
| JSONL + scheduled | `data/datasets/full.jsonl` |
| Software definitions | `data/datasets/software.jsonl` and DuckDB table `software` |
| YAML source | `data/entities/**/*.yaml` — **authoring only** |

Prefer DuckDB or Parquet over parsing thousands of YAML files.

## Join keys

| Entity | Primary key | Also useful |
|--------|-------------|-------------|
| Catalog | `uid` (`cdi########`) | `id` (filename stem), `link` |
| Software | `software.id` | join to `data/software/**/{id}.yaml` |
| Country coverage | `coverage[].location.country.id` | ISO alpha-2; some records use `World` / numeric M49 |
| Owner country | `owner.location.country.id` | should match path country |
| External registry | `identifiers[].id` + `identifiers[].value` | `wikidata`, `re3data`, `fairsharing` |

`id` is unique among files; `uid` is the stable identifier across exports. Do not join on `name`.

## Nested fields in DuckDB / Parquet

The builder stores nested objects as **JSON strings** so DuckDB type inference does not break. Filter with `LIKE` or parse JSON:

```sql
SELECT id, name, link
FROM catalogs
WHERE software LIKE '%"id":"ckan"%'
  AND coverage LIKE '%"id":"FR"%';
```

```sql
SELECT id, json_extract_string(software, '$.id') AS software_id
FROM catalogs
WHERE json_extract_string(software, '$.id') = 'geonetwork';
```

Column inventory: [exports.md](exports.md#duckdb-columns). Identifier types: [vocabularies.md](vocabularies.md#identifiers).

## Status and access

| Field | Values | Notes |
|-------|--------|--------|
| `status` | `active`, `inactive`, `scheduled`, `deprecated` | curated, not a live probe |
| `access_mode` | list; prefer `open` / `restricted` | schema also allows `limited`, `public`, `protected`, `closed`, `private` |
| `api` | boolean | if `true`, `api_status` should be set |
| `api_status` | `active`, `inactive`, `uncertain` | |

## Versioning

Exports are regenerated with `python scripts/builder.py build`. There is no per-table `_meta` identity like internacia-db; treat git commit / GitHub release as the snapshot version. Record counts are listed in [README.md](https://github.com/datenoio/dataportals-registry/blob/main/README.md#data-exports).

## Known limitations

- Geographic bias: see DATASHEET.md
- Many records lack `description`, `endpoints`, or `topics`
- Scheduled entries (when present) are unverified; as of 21 August 2026 there are 88 in `data/scheduled/`
- DuckDB/Parquet exports lag source YAML until the next `build` (v1.13.0 exports: 17,718 catalogs / 192 software)
- `software` may be `custom` / `unknown` when the platform is undetected

## Related

- [data-model.md](data-model.md)
- [exports.md](exports.md)
- [vocabularies.md](vocabularies.md)
- [query-examples.md](query-examples.md)
- [agents/query.md](agents/query.md)
- [agents/discover.md](agents/discover.md)
- [agents/harvest.md](agents/harvest.md)
- [harvest.md](harvest.md)
- [discovery.md](discovery.md)
- [discovery-agent-tools.md](discovery-agent-tools.md)
