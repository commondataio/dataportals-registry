## 1. Liveness script

- [x] 1.1 Create `scripts/check_liveness.py` with CLI: `--sample N`, `--country XX`, `--output PATH`
- [x] 1.2 Probe primary `link` with HEAD, GET fallback, 10s timeout, 2 retries
- [x] 1.3 Classify results: `live`, `dead`, `redirect`, `inconclusive`, `error`
- [x] 1.4 Write `dataquality/liveness_report.jsonl` (uid, link, status, http_code, checked_at)

## 2. GitHub Actions workflow

- [x] 2.1 Add `.github/workflows/liveness.yml` with weekly cron and `workflow_dispatch`
- [x] 2.2 Configure concurrency limit and job timeout appropriate for full catalog
- [x] 2.3 Upload `liveness_report.jsonl` as workflow artifact

## 3. Documentation

- [x] 3.1 Document probe semantics and status meanings in `devdocs/quality-fix-workflow.md`
- [x] 3.2 Note phase 2 plan for `liveness_status` / `last_verified_at` schema fields

## 4. Verification

- [x] 4.1 Run script in `--sample 10` mode against known live and dead URLs
- [x] 4.2 Run `openspec validate add-liveness-monitoring-workflow --strict`
- [x] 4.3 Add unit tests for status classification logic (mocked HTTP)
