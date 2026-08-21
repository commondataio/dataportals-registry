# Change: Add owner-type vocabulary and path consistency rules

## Why

Owner types currently have 38+ free-text variants with common synonyms (Academy/University/Higher education, Business/Private/Company). Path-vs-country and id-vs-host checks exist only as deprecated stubs, so directory placement and filename/host drift go undetected until manual review.

## What Changes

- Add `data/reference/owner_types.yaml` with canonical owner types and synonym map.
- Add quality rules: `INVALID_OWNER_TYPE`, `OWNER_TYPE_NONCANONICAL` (synonym → canonical suggestion).
- Re-implement `check_path_country_consistency` with allowlists for EU/World/International; start at MEDIUM.
- Do **not** enforce id/host correlation (`ID_HOST_MISMATCH`) — historical ids must remain compatible with past releases.
- Optionally revive a cautious `check_owner_coverage_coherence` (owner country vs coverage countries) as LOW/MEDIUM advisory.
- Document vocabularies in `docs/metadata-quality.md`.

## Impact

- Affected specs: `owner-type-vocabulary` (new), `path-consistency-checks` (new)
- Affected code: `scripts/builder.py`, `scripts/constants.py`, tests under `tests/`
- Data: New reference YAML; new issue types may appear after enablement
- No breaking schema change initially (quality-layer enforcement first; schema enum optional later)
