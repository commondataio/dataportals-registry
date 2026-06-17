# Change: Add liveness monitoring workflow

## Why

`check_urls` validates URL format only; many open-data portals become unreachable without schema-level signal. The Genspark audit recommends scheduled HTTP probes writing `liveness_status` and `last_verified_at` per record.

## What Changes

- Add `scripts/check_liveness.py` to probe catalog URLs (HEAD with GET fallback).
- Add weekly GitHub Actions workflow with rate limiting, timeouts, and retry policy.
- Write results to `dataquality/liveness_report.jsonl` (report layer first; schema fields optional in phase 2).
- Document probe semantics and false-positive handling (redirects, bot protection).

## Impact

- Affected specs: `catalog-liveness` (new capability)
- Affected code:
  - `scripts/check_liveness.py` (new)
  - `.github/workflows/liveness.yml` (new)
  - `dataquality/liveness_report.jsonl` (generated)
- Depends on: stable quality baseline (wave 1)
- No breaking changes in phase 1 (report-only output)
