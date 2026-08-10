## 1. Owner-type vocabulary

- [x] 1.1 Draft canonical owner types from current frequency analysis
- [x] 1.2 Create `data/reference/owner_types.yaml` with synonyms → canonical mapping
- [x] 1.3 Implement `check_owner_type_values` emitting `INVALID_OWNER_TYPE` / `OWNER_TYPE_NONCANONICAL`
- [x] 1.4 Register check, priorities, and rule descriptions
- [x] 1.5 Document vocabulary in `docs/metadata-quality.md`

## 2. Path consistency

- [x] 2.1 Re-implement `check_path_country_consistency` with allowlists (EU, World, International, UNKNOWN staging)
- [x] 2.2 Register path-country check at MEDIUM priority
- [x] 2.3 Add unit tests for allowlist and false-positive cases
- [x] 2.4 Explicitly omit id/host correlation to preserve historical id compatibility

## 3. Validation

- [x] 3.1 Run `analyze-quality` on a sample/full set and review false-positive rates
- [x] 3.2 Adjust synonym map / allowlists based on findings
- [x] 3.3 Update baseline if IMPORTANT counts change after any priority promotion
