# Agent guide: querying the registry

Platform-neutral workflow for looking up catalogs by geography, software, and type.
Works with Cursor, Claude Code, Copilot, Codex, and any agent with file access.

## Before querying

1. Read [llms.txt](https://github.com/datenoio/dataportals-registry/blob/main/llms.txt) for layout and gotchas.
2. Use **exports** — do not walk `data/entities/**/*.yaml` unless authoring.
3. Full contract: [ai-consumers.md](../ai-consumers.md).
4. Recipes: [query-examples.md](../query-examples.md).
5. If the user wants catalogs that are **not** in the registry yet, switch to [discover.md](discover.md) (client setup: [discovery-agent-tools.md](../discovery-agent-tools.md)).
6. If the user wants **datasets inside** a catalog, switch to [harvest.md](../harvest.md).

## Access paths

| Method | Path |
|--------|------|
| DuckDB | `data/datasets/datasets.duckdb` table `catalogs` |
| Parquet | `data/datasets/full.parquet` |
| JSONL | `data/datasets/catalogs.jsonl` |
| Software | DuckDB table `software` or `data/datasets/software.jsonl` |

## Join keys

- Catalog: `uid` (stable) or `id` (filename)
- Software: `software.id` (JSON string in DuckDB)
- Country: `coverage` JSON contains `"id":"XX"`
- External: `identifiers` JSON contains `"id":"wikidata"` / `"re3data"`

## Scope

**In:** catalog identity, owner, coverage, software, endpoints, identifiers.

**Out:** dataset records inside a portal; production search API/MCP in this repo ([dateno-api](https://github.com/datenoio/dateno-api)). To crawl those datasets, switch to [harvest.md](harvest.md).

## Canonical queries

See [query-examples.md](../query-examples.md). Minimal check that exports exist:

```sql
SELECT count(*) FROM catalogs;
SELECT count(*) FROM software;
```

## Gotchas

- Nested fields are JSON **strings** in DuckDB/Parquet.
- `id` is not a URL. Reconstruct nothing from `id`; use `link`.
- `status` is curated. Liveness is a separate report (`dataquality/liveness_report.jsonl`).
- Geographic coverage is biased toward the United States — do not treat counts as a complete global census.
- `software.id` may be `custom` when the platform is unknown.

## After answering

Cite `uid` or `id` + `link`. If the user wants to **edit** a record, switch to [contribute.md](contribute.md). If they want catalogs that are missing from the registry, switch to [discover.md](discover.md). If they want **datasets inside** a catalog, switch to [harvest.md](../harvest.md) (agent checklist: [agents/harvest.md](harvest.md)).
