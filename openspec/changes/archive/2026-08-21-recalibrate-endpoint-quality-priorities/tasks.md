## 1. Priority model

- [x] 1.1 Define integrity vs enrichment issue-type sets in `scripts/constants.py` or builder maps
- [x] 1.2 Move `SOFTWARE_EXPECTED_ENDPOINTS_MISSING_*` to MEDIUM (enrichment) unless `api: true`
- [x] 1.3 Keep `MISSING_ENDPOINTS` when `api: true` as IMPORTANT (integrity/coherence)
- [x] 1.4 Update `RULE_DESCRIPTIONS` and quality docs for the new model

## 2. Regression guard

- [x] 2.1 Extend baseline schema with optional `integrity` / `enrichment` priority buckets or per-type allowlists
- [x] 2.2 Update `compare_to_baseline` to fail on integrity growth; report enrichment growth as warning-only unless configured
- [x] 2.3 Update `tests/test_quality_regression.py` for the new semantics

## 3. Validation

- [x] 3.1 Re-run `analyze-quality` and refresh `dataquality/baseline_counts.json`
- [x] 3.2 Confirm IMPORTANT integrity count is stable and endpoint enrichment no longer blocks CI by default
- [x] 3.3 Run `pytest tests/test_quality_regression.py tests/test_quality_rule_improvements.py -v`
