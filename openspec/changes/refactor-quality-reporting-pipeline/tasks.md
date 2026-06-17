## 1. Pipeline audit

- [x] 1.1 Trace each registered check function to its output destinations (`full_report.txt`, `full_report.jsonl`, `rules/*.txt`, `primary_priority.jsonl`)
- [x] 1.2 Identify and fix any checks whose issues are dropped or double-counted during aggregation
- [x] 1.3 Add a post-run consistency check that compares aggregate issue counts across output files

## 2. Deprecated check cleanup

- [x] 2.1 Remove `check_owner_coverage_coherence` from the `analyze_quality` checks list
- [x] 2.2 Delete or archive stub implementations for `check_path_country_consistency`, `check_id_host_correlation`, `check_owner_coverage_coherence` if no longer referenced
- [x] 2.3 Remove corresponding entries from `ISSUE_PRIORITY_MAP` if present

## 3. Documentation

- [x] 3.1 Update `devdocs/quality-rules-analysis-report.md` with the current registered check list and status
- [x] 3.2 Confirm `check_subregion_iso3166_2` is documented as registered and active

## 4. Verification

- [x] 4.1 Run `python scripts/builder.py analyze-quality` and verify multiple rule families appear in `full_report.txt`
- [x] 4.2 Run `openspec validate refactor-quality-reporting-pipeline --strict`
- [ ] 4.3 Run pytest to ensure no regressions *(full suite currently has pre-existing unrelated failures in `tests/test_re3data_enrichment.py` and `tests/test_trust_score.py`; `tests/test_builder.py` passes)*
