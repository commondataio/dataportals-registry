## 1. Reference data

- [x] 1.1 Evaluate debian `iso-codes` vs Wikidata export for ISO 3166-2 completeness
- [x] 1.2 Add `data/reference/subregions/iso3166-2.csv` (code, name, parent country) generated from chosen source
- [x] 1.3 Document refresh command in `devdocs/` or script `scripts/refresh_subregion_reference.py`

## 2. Loader migration

- [x] 2.1 Update `SUBREGIONS_CSV` constant and loader in `scripts/builder.py`
- [x] 2.2 Update `scripts/enrich.py`, `scripts/enrich_ai.py`, `scripts/fix_owner_location_subregion_required.py`
- [x] 2.3 Add deprecation notice for `IP2LOCATION-ISO3166-2.CSV` references

## 3. Validation and tests

- [x] 3.1 Add `tests/test_subregion_reference.py` with known-valid codes (FR-75, US-PR, BE-BRU, etc.)
- [x] 3.2 Run `analyze-quality` and record before/after `SUBREGION_INVALID_ISO3166_2` counts
- [x] 3.3 Update `devdocs/SUBREGION_INVALID_ISO3166_2_analysis.md` with migration results

## 4. Verification

- [x] 4.1 Run `openspec validate update-subregion-reference-source --strict`
- [x] 4.2 Run pytest
