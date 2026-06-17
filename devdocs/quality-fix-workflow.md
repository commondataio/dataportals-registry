# Quality fix workflow

This document describes how to fix data quality issues reported by the registry's quality analysis, including using Cursor or other agentic IDEs.

## 1. Generate the quality report

From the repository root:

```bash
python scripts/builder.py analyze-quality
```

Output is written to `dataquality/`:

- **full_report.txt** – Human-readable summary and issues by type
- **primary_priority.jsonl** – One JSON object per record with issues (used by fix automation)
- **countries/** – Per-country breakdowns
- **priorities/** – CRITICAL, IMPORTANT, MEDIUM, LOW
- **rules/** – Per-rule type details

## 2. Inspect issues

- Open `dataquality/full_report.txt` for an overview.
- For scripted or agent-driven fixes, use `dataquality/primary_priority.jsonl`: each line is a JSON object with `record_id`, `file_path`, and `issues` (list of issue objects with `issue_type`, `field`, `current_value`, `suggested_action`, etc.).

## 3. Apply fixes

**Option A – Priority-based fix scripts**

Run the existing fix scripts by priority (see `scripts/`):

- `fix_critical_issues.py`
- `fix_important_issues.py`
- `fix_medium_issues.py`
- `fix_low_issues.py`

Or run all: `fix_all_issues.py` (may use Cursor automation).

**Option B – Cursor / agent-driven fixes**

1. Generate prompts or commands from the primary priority file:
   ```bash
   python scripts/generate_cursor_commands.py
   ```
   This produces `scripts/update_all_issues.sh` and `scripts/all_issues_prompts.txt`.

2. Use the prompts or run the generated script with Cursor (or another agent) to apply edits to the YAML files referenced in `primary_priority.jsonl`.

**Option C – Manual edits**

Edit the YAML files listed in the report according to the `suggested_action` for each issue.

## 4. Verify

After making changes:

1. Validate schema:
   ```bash
   python scripts/builder.py validate-yaml
   ```

2. Re-run quality analysis to confirm issues are resolved or reduced:
   ```bash
   python scripts/builder.py analyze-quality
   ```

3. Run the test suite:
   ```bash
   pytest tests -v
   ```

## URL normalization rules (duplicate checks)

`analyze-quality` detects duplicate portals both by exact URL and by canonicalized URL (`DUPLICATE_LINK_NORMALIZED`).
Canonicalization rules:

- lower-case scheme and host
- remove leading `www.`
- drop default ports (`:80` for HTTP, `:443` for HTTPS)
- trim trailing `/` from path
- keep query string when present

## Quality regression baseline

CI guards against increases in high-priority quality issues using `dataquality/baseline_counts.json`.

Baseline format:

```json
{
  "generated_at": "2026-06-17T10:00:00Z",
  "source": "dataquality/full_report.jsonl",
  "total_records_analyzed": 14470,
  "by_priority": {
    "CRITICAL": 0,
    "IMPORTANT": 295,
    "MEDIUM": 2344,
    "LOW": 0
  },
  "by_issue_type": { "...": 0 }
}
```

After intentional bulk fixes that reduce issue counts, refresh the baseline:

```bash
python scripts/builder.py analyze-quality
python scripts/update_quality_baseline.py
```

Commit both updated `dataquality/full_report.jsonl` (and related reports if tracked) and `dataquality/baseline_counts.json` in the same PR.

Regression rules:

- `CRITICAL` and `IMPORTANT` counts must not increase compared to baseline.
- `MEDIUM` and `LOW` changes should also update the baseline when accepted intentionally.

## Catalog liveness monitoring

Weekly HTTP probes complement static URL format checks. Phase 1 is report-only; YAML schema fields are planned for phase 2.

Run locally:

```bash
python scripts/check_liveness.py --sample 10
python scripts/check_liveness.py --country US --sample 50
```

Status meanings:

| Status | Meaning |
|--------|---------|
| `live` | HTTP 2xx response |
| `redirect` | HTTP 3xx response (reachable) |
| `inconclusive` | Bot protection or client errors (e.g. 403, 401) — not counted as dead |
| `dead` | 404, 5xx after retries, or connection/timeout failures |
| `error` | Other transport or parsing failures |

Probe policy: HEAD with GET fallback, 10s timeout, 2 retries on timeout/5xx/429.

Output: `dataquality/liveness_report.jsonl` with `uid`, `link`, `liveness_status`, `http_code`, `checked_at`.

Scheduled workflow: `.github/workflows/liveness.yml` (weekly cron + manual dispatch).

Phase 2 (future): optional `liveness_status` and `last_verified_at` fields on catalog records.

## Summary

| Step | Command / action |
|------|------------------|
| Generate report | `python scripts/builder.py analyze-quality` |
| Inspect | `dataquality/full_report.txt`, `dataquality/primary_priority.jsonl` |
| Fix | `scripts/fix_*_issues.py` or `generate_cursor_commands.py` + Cursor |
| Validate | `python scripts/builder.py validate-yaml` |
| Re-check quality | `python scripts/builder.py analyze-quality` |
