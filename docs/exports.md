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

August 2026 snapshot: **16,896** catalog records, **148** software definitions.

## Slices

- `data/datasets/bytype/` — JSONL per catalog type
- `data/datasets/bysoftware/` — JSONL per `software.id`

## Compression

`.zst` files are [zstandard](https://facebook.github.io/zstd/). Decompress with `unzstd file.zst` or stream them in Python via `zstandard`.

## DuckDB notes

Nested objects (`software`, `owner`, `coverage`, `endpoints`, …) are serialized as JSON strings so mixed types do not break table creation. See [ai-consumers.md](ai-consumers.md) and [query-examples.md](query-examples.md).

## JSON-LD / DCAT

`data/schemes/catalog.context.jsonld` maps fields to DCAT and schema.org. Emit a framed dump with:

```bash
python scripts/builder.py build --jsonld
```
