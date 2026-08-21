## 1. Baseline

- [x] 1.1 After quality pipeline fix, run `analyze-quality` and capture counts by priority and issue type
- [x] 1.2 Add `dataquality/baseline_counts.json` with `critical`, `important`, `medium`, `low`, and optional per-type breakdown
- [x] 1.3 Document baseline format in `devdocs/quality-fix-workflow.md`

## 2. Regression test

- [x] 2.1 Add `tests/test_quality_regression.py` that reads `primary_priority.jsonl` and compares to baseline
- [x] 2.2 Fail when CRITICAL or IMPORTANT counts exceed baseline (zero tolerance or small configurable delta)
- [x] 2.3 Allow MEDIUM/LOW increases only with explicit baseline update in PR

## 3. CI integration

- [x] 3.1 Add CI step to run quality regression test on PRs that touch `data/entities/` or `data/scheduled/`
- [x] 3.2 Document how maintainers update baseline after intentional bulk fixes

## 4. Verification

- [x] 4.1 Run `openspec validate add-regression-guard-for-quality-counts --strict`
- [x] 4.2 Run pytest including new regression test
