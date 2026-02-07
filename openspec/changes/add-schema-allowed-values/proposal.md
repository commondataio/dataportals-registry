# Change: Add allowed values to catalog schema (catalog_type, status, access_mode)

## Why

The catalog JSON schema (Cerberus) in `data/schemes/catalog.json` does not restrict `catalog_type`, `status`, or `access_mode` to canonical values. CONTRIBUTING and reference data list them in prose/YAML only, so typos and drift can be introduced. Adding Cerberus `allowed` constraints keeps the registry consistent and makes validation failures actionable.

## What Changes

- In `data/schemes/catalog.json`:
  - Add `allowed` to `catalog_type` with the list from `data/reference/catalog_types.yaml`.
  - Add `allowed` to `status` with values: `active`, `inactive`, `scheduled`.
  - Add `allowed` to the list item schema of `access_mode` with values: `open`, `restricted`.
- Reference vocabularies already exist in `data/reference/` (catalog_types.yaml, status.yaml, access_modes.yaml).

## Impact

- Affected specs: None (schema-only change).
- Affected code: `data/schemes/catalog.json` only.
- Data: Any existing YAML that uses a value outside these lists will fail `validate-yaml` until corrected. Current data is expected to use only these values.
- **BREAKING**: Validation will reject previously accepted values that are not in the allowed lists. Run `python scripts/builder.py validate-yaml` after merge to catch and fix any outliers.
