# Getting started

dataportals-registry is a **reference-data registry** of open data portals, geoportals, scientific repositories, and related data infrastructure. Source records are YAML; consumers should prefer the exported datasets. High-volume platforms include CKAN, GeoNetwork, Dataverse, ArcGIS, **openEO**, **mviewer**, and **DHIS2** — full map: [software-index.md](software-index.md). Code is MIT; data and documentation are [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

Record counts (published snapshot vs current source): [exports.md](exports.md#record-counts).

## Fastest path (analytics)

Query the DuckDB export. Nested objects are stored as JSON strings, so filter with `LIKE` or `json_extract`:

```bash
duckdb data/datasets/datasets.duckdb \
  -c "SELECT id, name, link FROM catalogs WHERE software LIKE '%\"id\":\"ckan\"%' LIMIT 10;"
```

```python
import duckdb

con = duckdb.connect("data/datasets/datasets.duckdb")
con.execute(
    """
    SELECT id, name, link
    FROM catalogs
    WHERE catalog_type = 'Open data portal'
      AND coverage LIKE '%"id":"US"%'
    LIMIT 10
    """
).fetchall()
```

Parquet is interchangeable:

```python
import duckdb

con = duckdb.connect()
con.execute("SELECT count(*) FROM 'data/datasets/full.parquet'").fetchone()
```

## Fastest path (spreadsheet / JSONL)

- `data/datasets/catalogs.jsonl` — one JSON object per verified catalog
- `data/datasets/full.parquet` — same records, analytics-friendly

Decompress `.zst` files with `unzstd file.zst`. Filter by type or software with DuckDB ([query-examples.md](query-examples.md)).

## Authoring path (YAML)

Edit source YAML only when adding or correcting a catalog:

1. Place the file at `data/entities/{COUNTRY}/{Federal|SUBREGION}/{type}/{id}.yaml` (see [directory-layout.md](directory-layout.md))
2. Match `id` to the filename (lowercase letters and digits only)
3. Run `python scripts/builder.py assign` if `uid` is missing
4. Validate with `python scripts/builder.py validate-yaml --id {id}`

Do **not** hand-edit `data/datasets/`.

## Citation

See [CITATION.cff](https://github.com/datenoio/dataportals-registry/blob/main/CITATION.cff) and [DATASHEET.md](https://github.com/datenoio/dataportals-registry/blob/main/DATASHEET.md).

```
dataportals-registry: A global registry of open data portals and catalogs
(Common Data Index, 2026). CC-BY-4.0.
https://github.com/datenoio/dataportals-registry
```

## Next steps

| Goal | Doc |
|------|-----|
| Scope and when not to use this repo | [when-to-use.md](when-to-use.md) |
| Pipeline diagram | [architecture.md](architecture.md) |
| CLI | [cli.md](cli.md) |
| Find catalogs not yet registered | [discovery.md](discovery.md) |
| Google, Censys, and other search tools | [discovery-search-tools.md](discovery-search-tools.md) |
| Configure search tools in Cursor / ChatGPT | [discovery-agent-tools.md](discovery-agent-tools.md) |
| Open data / geo / scientific / metadata / indicators / other types | [discovery-opendata.md](discovery-opendata.md), [discovery-geoportals.md](discovery-geoportals.md) ([SDI](discovery-geoportals-sdi.md), [viewers](discovery-geoportals-viewers.md)), [discovery-scientific.md](discovery-scientific.md) ([domain](discovery-scientific-domain.md)), [discovery-metadata.md](discovery-metadata.md), [discovery-indicators.md](discovery-indicators.md), [discovery-other.md](discovery-other.md) |
| Harvest datasets from catalog APIs | [harvest.md](harvest.md), [harvest-scientific.md](harvest-scientific.md) ([domain](harvest-scientific-domain.md)), [harvest-opendata.md](harvest-opendata.md), [harvest-geoportals.md](harvest-geoportals.md), [harvest-indicators.md](harvest-indicators.md), [harvest-metadata.md](harvest-metadata.md), [harvest-other.md](harvest-other.md), [harvest-protocols.md](harvest-protocols.md), [harvest-incremental.md](harvest-incremental.md), [harvest-earthdata.md](harvest-earthdata.md), [harvest-biodiversity.md](harvest-biodiversity.md), [harvest-viewers.md](harvest-viewers.md), [harvest-identifiers.md](harvest-identifiers.md), [harvest-output.md](harvest-output.md) |
| Endpoint detection / URL liveness | [apidetect.md](apidetect.md), [liveness.md](liveness.md) |
| Field reference | [data-model.md](data-model.md) |
| Vocabularies (levels, identifiers, endpoints) | [vocabularies.md](vocabularies.md) |
| Catalog types | [catalog-types.md](catalog-types.md) |
| Software IDs → discovery/harvest recipe | [software-index.md](software-index.md) |
| Software IDs and new platform YAML | [software-taxonomy.md](software-taxonomy.md) |
| Join keys and DuckDB columns | [ai-consumers.md](ai-consumers.md) |
| Quality issue codes | [quality-rules.md](quality-rules.md) |
| Verified SQL | [query-examples.md](query-examples.md) |
| Agent query workflow | [agents/query.md](agents/query.md) |
| Agent discovery workflow | [agents/discover.md](agents/discover.md) |
| Agent harvest workflow | [agents/harvest.md](agents/harvest.md) |
| Add or edit YAML | [agents/contribute.md](agents/contribute.md) |
