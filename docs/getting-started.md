# Getting started

dataportals-registry is a **reference-data registry** of open data portals, geoportals, scientific repositories, and related data infrastructure. Source records are YAML; consumers should prefer the exported datasets. Code is MIT; data and documentation are [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

Latest snapshot (August 2026): **16,896** verified catalog entities and **148** software definitions.

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
- `data/datasets/bytype/` and `data/datasets/bysoftware/` — pre-sliced JSONL

Decompress `.zst` files with `unzstd file.zst`.

## Authoring path (YAML)

Edit source YAML only when adding or correcting a catalog:

1. Place the file at `data/entities/{COUNTRY}/{type}/{id}.yaml`
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
| Field reference | [data-model.md](data-model.md) |
| Join keys and gotchas | [ai-consumers.md](ai-consumers.md) |
| Verified SQL | [query-examples.md](query-examples.md) |
| Agent query workflow | [agents/query.md](agents/query.md) |
| Add or edit YAML | [agents/contribute.md](agents/contribute.md) |
