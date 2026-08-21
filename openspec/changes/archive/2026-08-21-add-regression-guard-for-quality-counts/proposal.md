# Change: Add regression guard for quality counts

## Why

The pytest suite validates code but not data quality regressions. At 14k+ records, a PR can silently increase CRITICAL/IMPORTANT issue counts. The Genspark audit recommends a CI guard preventing growth in `primary_priority.jsonl` line count.

## What Changes

- Add a baseline file `dataquality/baseline_counts.json` with per-priority and per-issue-type counts.
- Add CI step (or pytest test) that runs after `analyze-quality` and fails if CRITICAL or IMPORTANT counts exceed baseline by more than a configurable threshold.
- Document baseline update procedure in `devdocs/quality-fix-workflow.md`.

## Impact

- Affected specs: `quality-regression-guard` (new capability)
- Affected code:
  - `.github/workflows/tests.yml` or new workflow
  - `tests/test_quality_regression.py` (new)
  - `dataquality/baseline_counts.json` (new)
- Depends on: `refactor-quality-reporting-pipeline` (stable reporting)
- No breaking changes to catalog YAML schema
