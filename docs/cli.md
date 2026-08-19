# CLI reference

All commands run from the repository root unless noted.

```bash
pip install -r requirements.txt
python scripts/builder.py --help
```

## Essential commands

| Command | Purpose |
|---------|---------|
| `python scripts/builder.py build` | Rebuild JSONL, zstd, Parquet, and DuckDB from YAML |
| `python scripts/builder.py build --jsonld` | Also emit `data/datasets/catalogs.jsonld` |
| `python scripts/builder.py validate-yaml` | Validate all entity YAML against Cerberus schema |
| `python scripts/builder.py validate-yaml --id catalogdatafaagov` | Validate one catalog |
| `python scripts/builder.py validate-yaml --file path/to/file.yaml` | Validate one file |
| `python scripts/builder.py assign` | Assign missing `cdi########` UIDs |
| `python scripts/builder.py analyze-quality` | Write `dataquality/` reports |
| `python scripts/builder.py quality-control` | Terminal completeness metrics |
| `pytest` | Test suite with coverage |

## Adding catalogs

```bash
python scripts/builder.py add-single \
  --url "https://example.com/data" \
  --software ckan \
  --catalog-type "Open data portal" \
  --name "Example Data Portal" \
  --country US \
  --scheduled
```

Use `--no-scheduled` to write directly under `data/entities/`. After adding files, run `assign` then `validate-yaml`.

## Enrichment and sync

```bash
python scripts/re3data_enrichment.py enrich --dry-run
python scripts/re3data_enrichment.py enrich
python scripts/sync_ckan_ecosystem.py --dry-run
python scripts/sync_ckan_ecosystem.py
```

## Quality helpers

```bash
python scripts/fix_critical_issues.py
python scripts/update_quality_baseline.py
python scripts/check_liveness.py --sample 10
python scripts/calculate_trust_scores.py --dry-run
```

See [metadata-quality.md](metadata-quality.md) and [devdocs/quality-fix-workflow.md](https://github.com/datenoio/dataportals-registry/blob/main/devdocs/quality-fix-workflow.md).

## Tests

```bash
pytest
pytest tests/test_builder.py -v
pytest -m unit
pytest --no-cov
```

CI (`.github/workflows/tests.yml`) runs `validate-yaml`, pytest on Python 3.9–3.12, and the quality regression guard.
