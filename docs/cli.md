# CLI reference

All commands run from the repository root unless noted.

```bash
pip install -r requirements.txt
python scripts/builder.py --help
```

Python **3.9–3.12**. Test layout: [tests/README.md](https://github.com/datenoio/dataportals-registry/blob/main/tests/README.md).

## Essential commands

| Command | Purpose |
|---------|---------|
| `python scripts/builder.py build` | Rebuild JSONL, zstd, Parquet, and DuckDB from YAML |
| `python scripts/builder.py build --jsonld` | Also emit `data/datasets/catalogs.jsonld` |
| `python scripts/builder.py validate-yaml` | Validate entity YAML against the Cerberus schema |
| `python scripts/builder.py validate-yaml --id catalogdatafaagov` | Validate one catalog id |
| `python scripts/builder.py validate-yaml --file path/to/file.yaml` | Validate one file |
| `python scripts/builder.py validate` | Validate built `full.jsonl` against the same schema |
| `python scripts/builder.py validate-software` | Software YAML coverage/profile checks |
| `python scripts/builder.py assign` | Assign missing `cdi########` UIDs in entities (`--dryrun` to preview) |
| `python scripts/builder.py assign --mode scheduled` | Assign `temp########` UIDs in scheduled |
| `python scripts/builder.py analyze-quality` | Write `dataquality/` reports |
| `python scripts/builder.py quality-control` | Terminal completeness metrics (`--mode full` or `catalogs`) |
| `pytest` | Test suite with coverage |

## Adding catalogs

```bash
python scripts/builder.py add-single "https://example.com/data" \
  --software ckan \
  --catalog-type "Open data portal" \
  --name "Example Data Portal" \
  --country US \
  --scheduled
```

Use `--no-scheduled` to write under `data/entities/`. After adding files, run `assign` then `validate-yaml`.

| Command | Purpose |
|---------|---------|
| `add-single` | One URL |
| `add-list FILENAME` | One URL per line |
| `add-opendatasoft-catalog FILENAME` | Prepared OpenDataSoft JSONL |
| `add-socrata-catalog FILENAME` | Prepared Socrata JSONL |
| `add-arcgishub-catalog FILENAME` | Prepared ArcGIS Hub JSONL (writes entities; `--force` to overwrite) |
| `add-legacy` | Maintainer: ingest `UNPROCESSED` `.txt` lists |

Finding catalogs: [discovery.md](discovery.md), [agents/discover.md](agents/discover.md). CKAN bulk import: [ckan-sync.md](ckan-sync.md). Promote scheduled: [scheduled.md](scheduled.md).

## Enrichment and monitoring

```bash
python scripts/re3data_enrichment.py enrich --dry-run
python scripts/sync_ckan_ecosystem.py --dry-run
python scripts/apidetect.py detect-single catalogdatagov --dryrun
python scripts/check_liveness.py --sample 10
python scripts/calculate_trust_scores.py --dry-run
python scripts/promote_scheduled.py --dry-run
```

Re3Data: [re3data.md](re3data.md). Endpoint maps: [apidetect.md](apidetect.md) (`detect-software`, `detect-country`; dry-run first). URL reachability: [liveness.md](liveness.md) (weekly workflow, report-only JSONL; does not change YAML `status`). Probe APIs only after a catalog YAML exists.

## Quality helpers

```bash
python scripts/fix_critical_issues.py
python scripts/fix_important_issues.py
python scripts/update_quality_baseline.py
python scripts/builder.py fix
python scripts/generate_cursor_commands.py
```

`builder.py fix` drives `cursor-agent` against `dataquality/primary_priority.jsonl` (requires the Cursor CLI). Issue codes: [quality-rules.md](quality-rules.md). Workflow: [metadata-quality.md](metadata-quality.md).

## Reports and dumps

| Command | Purpose |
|---------|---------|
| `python scripts/builder.py export` | Flattened CSV (`export.csv`) |
| `python scripts/builder.py stats` | Country × software TSV (`country_software.csv`) |
| `python scripts/builder.py report` | Legacy incomplete-field scan on `full.jsonl` |
| `python scripts/builder.py country-report` | Per-country counts from Parquet |
| `python scripts/builder.py get-countries` | Print a `COUNTRIES` map snippet |
| `python scripts/builder.py validate-typing` | Optional pydantic check (needs `cdiapi`) |
| `python scripts/builder.py build-docs` | Software stub markdown for the sibling `cdi-docs` repo |

## Tests

```bash
pytest
pytest tests/test_builder.py -v
pytest -m unit
pytest --no-cov
```

CI (`.github/workflows/tests.yml`) runs `validate-yaml`, pytest on Python 3.9–3.12, and the quality regression guard.
