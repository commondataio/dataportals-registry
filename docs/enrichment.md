# Enrichment and quality-fix scripts

Maintainer tools that **edit existing YAML**. They are not discovery scanners. Find catalogs with [discovery.md](discovery.md); fill APIs with [apidetect.md](apidetect.md); probe `link` with [liveness.md](liveness.md).

Most contributors only need `validate-yaml`, `assign`, and `analyze-quality`. The commands below assume you already have a named batch of records and a dry-run.

## Endpoint inference (`endpoints_infer.py`)

Not a CLI. Quality-fix scripts import `infer_endpoints()`, which GETs [apidetect.md](apidetect.md) URL maps and returns only endpoints that respond. Do not guess CKAN/GeoServer paths for `custom` or `NO_STANDARD_PROBE` software.

## Re3Data and CKAN sync

| Script | Doc |
|--------|-----|
| `scripts/re3data_enrichment.py` | [re3data.md](re3data.md) |
| `scripts/sync_ckan_ecosystem.py` | [ckan-sync.md](ckan-sync.md) |

## Trust scores

```bash
python scripts/calculate_trust_scores.py --dry-run
```

Semantics: [trust-score.md](trust-score.md). Optional; not required for a contribution.

## Quality fix loop

```bash
python scripts/builder.py analyze-quality
python scripts/fix_critical_issues.py
python scripts/fix_important_issues.py
python scripts/fix_medium_issues.py
python scripts/builder.py validate-yaml
python scripts/builder.py analyze-quality
python scripts/update_quality_baseline.py   # only after an intentional baseline change
```

Country helpers (`fix_us_issues.py`, `fix_es_issues.py`, `fix_de_issues.py`, …) apply the same rules to one country folder. `fix_all_issues.py` / `fix_all_priority_issues.py` walk every priority.

`python scripts/generate_cursor_commands.py` writes agent prompts from `dataquality/primary_priority.jsonl`.

`python scripts/builder.py fix` runs `cursor-agent` against that JSONL when the Cursor CLI is installed.

Working notes: `devdocs/quality-fix-workflow.md`. Issue codes: [quality-rules.md](quality-rules.md).

## Legacy bulk enrich (`enrich.py`, `enrich_ai.py`, `enrich_soft.py`)

These Typer apps predate the quality loop. Paths inside them often assume `../data/` (run from `scripts/` if you must). **Do not run them as part of adding a catalog.**

| Script | Role |
|--------|------|
| `enrich.py` | One-off bulk edits (topics, countries, identifiers, catalog_type). Many subcommands. Dry-run first. |
| `enrich_ai.py` | Optional LLM descriptions into `data/enriched/` — not merged automatically. Needs an API key; out of scope for normal PRs. |
| `enrich_soft.py` | Rebuild/update `data/software/` from historical CSV. Prefer editing software YAML directly ([software-taxonomy.md](software-taxonomy.md)). |

## Related

- [cli.md](cli.md)
- [architecture.md](architecture.md)
- [metadata-quality.md](metadata-quality.md)
- [apidetect.md](apidetect.md)
- [liveness.md](liveness.md)
