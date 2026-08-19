# Query examples (DuckDB)

Verified patterns against `data/datasets/datasets.duckdb` (table `catalogs`) or `data/datasets/full.parquet`. Nested fields are JSON strings — see [ai-consumers.md](ai-consumers.md).

## Connect

```python
import duckdb

con = duckdb.connect("data/datasets/datasets.duckdb")
```

```python
import duckdb

con = duckdb.connect()
con.execute("SELECT count(*) FROM 'data/datasets/full.parquet'").fetchone()
```

## Counts by catalog type

```sql
SELECT catalog_type, count(*) AS n
FROM catalogs
GROUP BY 1
ORDER BY n DESC;
```

## CKAN portals in the United States

```sql
SELECT id, name, link
FROM catalogs
WHERE software LIKE '%"id":"ckan"%'
  AND coverage LIKE '%"id":"US"%'
  AND status = 'active'
ORDER BY name
LIMIT 50;
```

## Active catalogs with an API

```sql
SELECT id, name, catalog_type, json_extract_string(software, '$.id') AS software_id
FROM catalogs
WHERE api = true
  AND api_status = 'active'
ORDER BY name
LIMIT 50;
```

## Geoportals by software

```sql
SELECT json_extract_string(software, '$.id') AS software_id, count(*) AS n
FROM catalogs
WHERE catalog_type = 'Geoportal'
GROUP BY 1
ORDER BY n DESC;
```

## External identifiers (Wikidata)

```sql
SELECT id, name, identifiers
FROM catalogs
WHERE identifiers LIKE '%"id":"wikidata"%'
LIMIT 20;
```

## Scientific repositories with re3data

```sql
SELECT id, name, link
FROM catalogs
WHERE catalog_type = 'Scientific data repository'
  AND identifiers LIKE '%"id":"re3data"%'
ORDER BY name
LIMIT 50;
```

## Is this URL already registered?

```sql
SELECT id, name, link, catalog_type, status
FROM catalogs
WHERE lower(link) LIKE '%example.gov%';
```

Use this before adding a catalog. Full workflow: [discovery.md](discovery.md).

## Software table

```sql
SELECT id, name, category
FROM software
ORDER BY name;
```

## JSONL without DuckDB

```python
import json

with open("data/datasets/catalogs.jsonl", encoding="utf-8") as fh:
    for line in fh:
        rec = json.loads(line)
        if rec.get("software", {}).get("id") == "ckan":
            print(rec["id"], rec["link"])
```

## Polars / Parquet

```python
import polars as pl

df = pl.read_parquet("data/datasets/full.parquet")
ckan = df.filter(pl.col("software").str.contains('"id":"ckan"'))
print(ckan.select(["id", "name", "link"]).head())
```
