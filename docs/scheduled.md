# Scheduled entries

Unverified catalogs live under `data/scheduled/` with `status: scheduled` and `uid` values like `temp########`. They are included in `full.jsonl` / `full.parquet` / DuckDB, not in `catalogs.jsonl`.

As of 21 August 2026 the queue holds **88** records, mostly FAIR Data Point (`fairdatapoint`) metadata catalogs and MapServer (`mapserver`) geoportals, plus Mapbender and NextGIS Web finds. After a release the queue may be empty.

Prefer `--scheduled` when adding finds you have not fully reviewed: [discovery.md](discovery.md), [agents/contribute.md](agents/contribute.md).

## Promote with the script

```bash
python scripts/promote_scheduled.py --dry-run
python scripts/promote_scheduled.py
python scripts/builder.py assign
python scripts/builder.py validate-yaml
```

`scripts/promote_scheduled.py`:

- Moves each YAML to `data/entities/{country}/Federal/{type}/`
- Sets `status` to `active`, or `inactive` when the host looks like staging/demo
- Infers country for `Unknown/` paths from coverage, owner, or URL
- Deletes the scheduled copy if the same `id` already exists as an entity

After promotion, place regional/local catalogs in a subregion folder (`US-CA/`, …) with `owner.location.level` 30+ if the script left them under `Federal/`.

## Manual promotion

1. Review the YAML (`link`, `catalog_type`, `software.id`, owner, coverage).
2. Move it to `data/entities/{COUNTRY}/{Federal|SUBREGION}/{type}/{id}.yaml`.
3. Filename must match `id`.
4. Run `assign` then `validate-yaml --id {id}`.

Without options, `validate-yaml` validates `data/entities/` only; `--id` also searches `data/scheduled/`. Rebuild exports after a batch: `python scripts/builder.py build`.

Duplicates: `python scripts/remove_scheduled_duplicates.py`.
