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

## Vocabularies

Under `data/reference/`:

- `catalog_types.yaml`
- `software_ids.yaml`
- `status.yaml`
- `access_modes.yaml` — prefer `open` / `restricted` for new entries
- `owner_types.yaml` — canonical values plus synonym map

## Fix workflow

See [devdocs/quality-fix-workflow.md](https://github.com/datenoio/dataportals-registry/blob/main/devdocs/quality-fix-workflow.md). Helper scripts: `scripts/fix_critical_issues.py`, `scripts/fix_important_issues.py`, and related `fix_*` tools.
