# Metadata quality

Recommended (non-required) fields that improve discoverability. Required fields are in [data-model.md](data-model.md). The analyzer writes findings to `dataquality/`.

## Recommended fields

| Field | Purpose |
|-------|---------|
| `description` | Human-readable summary. Missing or very short text is flagged. |
| `endpoints` | Harvestable APIs (`type` + `url`) |
| `identifiers` | wikidata, re3data, fairsharing, … |
| `langs` | `{id, name}` (e.g. `EN` / `English`) |
| `tags` | Keywords (`government`, `has_api`) |
| `topics` | EU data themes or ISO 19115 |
| `owner.link` | Owning organization URL |
| `owner.location` | Country and, when relevant, subregion |
| `api_status` | Required in practice when `api: true` (`active`, `inactive`, `uncertain`) |

## Reports

```bash
python scripts/builder.py analyze-quality
```

| Path | Contents |
|------|----------|
| `dataquality/full_report.txt` | Human-readable summary |
| `dataquality/full_report.jsonl` | Machine-readable issues (join on `uid`) |
| `dataquality/primary_priority.jsonl` | CRITICAL + IMPORTANT |
| `dataquality/baseline_counts.json` | CI regression baseline |
| `dataquality/rules/` | Per-rule breakdowns |
| `dataquality/priorities/` | By CRITICAL / IMPORTANT / MEDIUM / LOW |
| `dataquality/countries/` | Per-country |

## Integrity vs enrichment

| Track | Examples | CI |
|-------|----------|----|
| Integrity | `INVALID_*`, `DUPLICATE_*`, `MISSING_ENDPOINTS` when `api: true`, path/country mismatches | Hard fail if CRITICAL/IMPORTANT counts grow |
| Enrichment | missing topics/tags, short description, expected software endpoints | Warning only by default |

Maintainer notes: [devdocs/quality-fix-workflow.md](https://github.com/datenoio/dataportals-registry/blob/main/devdocs/quality-fix-workflow.md). Script map: [enrichment.md](enrichment.md). Issue codes: [quality-rules.md](quality-rules.md). Vocabularies: [vocabularies.md](vocabularies.md).

## Fix workflow

```bash
python scripts/builder.py analyze-quality
python scripts/fix_critical_issues.py
python scripts/fix_important_issues.py
python scripts/builder.py validate-yaml
python scripts/builder.py analyze-quality
python scripts/update_quality_baseline.py   # only after an intentional baseline change
```

Agent-driven loop: `python scripts/generate_cursor_commands.py` then the generated prompts, or `python scripts/builder.py fix` if `cursor-agent` is installed.

Liveness probes (`scripts/check_liveness.py`, weekly workflow) write `dataquality/liveness_report.jsonl`. They do not update YAML `status`.
