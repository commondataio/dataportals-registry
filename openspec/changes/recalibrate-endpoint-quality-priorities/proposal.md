# Change: Recalibrate endpoint quality priorities

## Why

`SOFTWARE_EXPECTED_ENDPOINTS_MISSING_*` accounts for ~76% of open IMPORTANT issues and dominates the CI regression guard. Many findings are enrichment debt (API-capable software without a listed endpoint), not integrity failures. Treating them the same as INVALID_* / directory mismatches hides real regressions and blocks useful completeness work.

## What Changes

- Classify quality issue types into **integrity** vs **enrichment** tracks.
- Move software-expected-endpoint findings (when `api` is not explicitly true and link is not a service root) to MEDIUM enrichment priority, or a dedicated non-blocking track.
- Keep integrity failures (INVALID_*, DUPLICATE_RECORD_ID, CATALOG_SOFTWARE_MISMATCH, required fields) as CRITICAL/IMPORTANT for CI.
- Extend the regression baseline to support per-track or per-issue-type ceilings so enrichment backlog does not mask integrity growth.
- Document the priority model in `devdocs/quality-fix-workflow.md`.

## Impact

- Affected specs: `data-quality-reporting`, `quality-regression-guard`
- Affected code: `scripts/builder.py` (`ISSUE_PRIORITY_MAP`, `get_priority_level`), `scripts/quality_regression.py`, `tests/test_quality_regression.py`, `dataquality/baseline_counts.json`
- Data: Baseline refresh after priority recalibration (IMPORTANT counts expected to drop)
- No breaking changes to catalog YAML schema
