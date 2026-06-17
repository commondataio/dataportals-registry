## ADDED Requirements

### Requirement: Authoritative ISO 3166-2 Reference Data
The registry MUST validate subregion codes against a complete ISO 3166-2 reference dataset.

#### Scenario: Valid ISO 3166-2 code is present in record
- **WHEN** a catalog record contains `owner.location.subregion.id` or `coverage[].location.subregion.id` with a code listed in the official ISO 3166-2 registry
- **THEN** `check_subregion_iso3166_2` does not flag `SUBREGION_INVALID_ISO3166_2`
- **AND** the reference file includes the code

#### Scenario: Invalid subregion code
- **WHEN** a record contains a subregion id not present in the reference dataset
- **THEN** `check_subregion_iso3166_2` flags `SUBREGION_INVALID_ISO3166_2`
- **AND** the suggested action references the canonical reference file path

### Requirement: Reproducible Reference Refresh
The project MUST provide a documented procedure to refresh subregion reference data.

#### Scenario: Maintainer refreshes reference data
- **WHEN** a maintainer runs the documented refresh command
- **THEN** `data/reference/subregions/iso3166-2.csv` is regenerated from the pinned upstream source
- **AND** `tests/test_subregion_reference.py` passes against the updated file

### Requirement: False Positive Reduction
Migration to the new reference source MUST materially reduce false-positive subregion validation errors.

#### Scenario: Post-migration quality analysis
- **WHEN** `analyze-quality` runs after reference migration
- **THEN** the count of `SUBREGION_INVALID_ISO3166_2` issues decreases by at least 50% compared to the pre-migration baseline documented in `devdocs/SUBREGION_INVALID_ISO3166_2_analysis.md`
