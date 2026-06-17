# Change: Refactor quality reporting pipeline

## Why

The Genspark audit found that `dataquality/full_report.txt` may not reflect all active quality rules, and `devdocs/quality-rules-analysis-report.md` documents checks that are implemented but disconnected from the pipeline. Deprecated stub checks (`return None`) remain registered, creating maintenance noise and misleading rule counts.

## What Changes

- Audit and fix `analyze-quality` aggregation so `full_report.txt`, `full_report.jsonl`, `primary_priority.jsonl`, and `dataquality/rules/*.txt` are consistent.
- Remove deprecated stub checks from the execution list: `check_path_country_consistency`, `check_id_host_correlation`, `check_owner_coverage_coherence`.
- Add a post-run consistency assertion: total issues in `full_report.jsonl` MUST match the sum of per-rule report counts.
- Update `devdocs/quality-rules-analysis-report.md` to reflect the live check registry.

## Impact

- Affected specs: `data-quality-reporting` (new capability)
- Affected code:
  - `scripts/builder.py` (`analyze_quality`, `generate_full_report`, checks list)
  - `devdocs/quality-rules-analysis-report.md`
- Data: Regenerated `dataquality/*` outputs after pipeline fix
- No breaking changes to catalog YAML schema
