## 1. URL canonicalization

- [x] 1.1 Add `canonicalize_url(url: str) -> str` helper in `scripts/builder.py` or `scripts/url_utils.py`
- [x] 1.2 Normalize: scheme to https where both exist, lowercase host, remove default ports, strip trailing slash on path
- [x] 1.3 Add unit tests for canonicalization edge cases

## 2. Duplicate detection

- [x] 2.1 Build `canonical_link -> [record_ids]` map alongside exact `link` map in `analyze_quality`
- [x] 2.2 Emit `DUPLICATE_LINK_NORMALIZED` when canonical forms match but exact strings differ
- [x] 2.3 Register issue type in `ISSUE_PRIORITY_MAP` (IMPORTANT tier)

## 3. Extended URL coverage

- [ ] 3.1 Apply canonicalization in cross-record duplicate checks for `identifiers[].url` where applicable
- [x] 3.2 Verify `check_identifier_urls`, `check_rights_urls` share canonicalization with duplicate logic
- [x] 3.3 Document normalization rules in `devdocs/quality-fix-workflow.md`

## 4. Verification

- [ ] 4.1 Run `analyze-quality` and confirm new issue type appears for known http/https pairs
- [x] 4.2 Run `openspec validate add-normalized-url-quality-checks --strict`
- [x] 4.3 Run pytest
