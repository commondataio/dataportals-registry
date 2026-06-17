# Change: Update subregion reference source

## Why

Subregion validation uses `data/reference/subregions/IP2LOCATION-ISO3166-2.CSV`, which is incomplete. Analysis in `devdocs/SUBREGION_INVALID_ISO3166_2_analysis.md` found ~82% of flagged `SUBREGION_INVALID_ISO3166_2` issues are false positives because valid ISO 3166-2 codes are missing from the reference file.

## What Changes

- Replace or supplement the IP2Location CSV with a more complete ISO 3166-2 reference (debian `iso-codes` package data or Wikidata-derived export).
- Update `check_subregion_iso3166_2` and all scripts that load subregion reference data to use the new source.
- Add a reference-data refresh procedure and a test asserting coverage of known-valid codes (FR departments, US territories, BE regions).
- Update `devdocs/SUBREGION_INVALID_ISO3166_2_analysis.md` with post-migration false-positive rate.

## Impact

- Affected specs: `subregion-validation` (new capability)
- Affected code:
  - `scripts/builder.py` (`check_subregion_iso3166_2`, `SUBREGIONS_CSV` loader)
  - `scripts/enrich.py`, `scripts/enrich_ai.py`, `scripts/fix_owner_location_subregion_required.py`
  - `data/reference/subregions/` (new or updated reference file)
- Data: Re-run `analyze-quality` after migration; expect significant reduction in `SUBREGION_INVALID_ISO3166_2` count
- No breaking changes to catalog YAML schema
