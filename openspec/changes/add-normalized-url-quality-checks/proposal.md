# Change: Add normalized URL quality checks

## Why

Duplicate link detection in `analyze-quality` compares exact `link` strings only. Variants such as `http://x.gov/` vs `https://x.gov` vs `https://x.gov/` pass as distinct records. The Genspark audit also noted that `identifiers[].url`, rights URLs, and `catalog_export` need consistent URL validation beyond format checks.

## What Changes

- Add URL canonicalization utility (scheme normalization, trailing slash, default ports, host lowercasing).
- Extend duplicate detection to flag `DUPLICATE_LINK_NORMALIZED` when canonical forms match.
- Ensure `check_identifier_urls`, `check_rights_urls`, and related checks use the same canonicalization rules.
- Document normalization rules in `devdocs/quality-fix-workflow.md`.

## Impact

- Affected specs: `url-quality-checks` (new capability)
- Affected code: `scripts/builder.py` (duplicate detection, URL check functions)
- Data: New issue type `DUPLICATE_LINK_NORMALIZED` may appear in quality reports
- No breaking changes to catalog YAML schema
