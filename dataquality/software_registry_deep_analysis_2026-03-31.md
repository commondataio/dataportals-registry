# Deep Analysis: `data/software` Quality

Date: 2026-03-31  
Scope: all YAML records under `data/software/` (excluding `types.yaml` dictionary file)

## Executive findings

- The registry has broad coverage: **134 software definitions** across 6 categories.
- Core technical fields are strong: `export_formats` (97.0%), `website` (98.5%), `last_updated` (98.5%).
- Main quality gaps are still metadata depth fields:
  - `license`: **8.1%** coverage
  - `version`: **25.9%** coverage
  - `release_date`: **0.0%** coverage
  - `plugins`: **0.0%** coverage
- Cross-file consistency issues remain:
  - `data/reference/software_ids.yaml` references `searchengine`, but no definition exists.
  - Catalog usage contains `othergeo`, but `data/software/geo/othergeo.yaml` is deleted.
- Validation logic mismatch exists between `data/schemes/software.json` and real records (many valid operational fields are not in schema).

## Inventory and structure

Software definitions by `category`:

- Scientific data repository: 44
- Geoportal: 39
- Open data portal: 26
- Indicators catalog: 18
- Microdata catalog: 4
- Metadata catalog: 3

Notes:

- 1 dictionary file (`types.yaml`) is not a software record and should stay excluded from record-quality checks.
- `custom.yaml` is a real software definition and is included in metrics.

## Completeness analysis

Global optional field coverage (134 records):

- `version`: 35/134 (25.9%)
- `license`: 11/134 (8.1%)
- `repository_url`: 78/134 (57.8%)
- `documentation_url`: 94/134 (69.6%)
- `changelog_url`: 73/134 (54.1%)
- `release_date`: 0/134 (0.0%)
- `last_updated`: 133/134 (98.5%)
- `capabilities`: 97/134 (71.9%)
- `export_formats`: 131/134 (97.0%)
- `website`: 133/134 (98.5%)

### By category (selected fields)

#### Scientific data repository (44)
- `version`: 29.5%
- `license`: 9.1%
- `repository_url`: 81.8%
- `documentation_url`: 88.6%
- `changelog_url`: 77.3%

#### Geoportal (39)
- `version`: 33.3%
- `license`: 7.7%
- `repository_url`: 61.5%
- `documentation_url`: 82.1%
- `changelog_url`: 61.5%

#### Open data portal (26)
- `version`: 23.1%
- `license`: 15.4%
- `repository_url`: 42.3%
- `documentation_url`: 50.0%
- `changelog_url`: 30.8%

#### Indicators catalog (18)
- `version`: 11.1%
- `license`: 0.0%
- `repository_url`: 11.1%
- `documentation_url`: 27.8%
- `changelog_url`: 11.1%

#### Microdata catalog (4)
- `version`: 25.0%
- `license`: 0.0%
- `repository_url`: 75.0%
- `documentation_url`: 75.0%
- `changelog_url`: 75.0%

#### Metadata catalog (3)
- `version`: 0.0%
- `license`: 0.0%
- `repository_url`: 66.7%
- `documentation_url`: 66.7%
- `changelog_url`: 66.7%
- `export_formats`: 0.0%

## Consistency and validation findings

### 1) Profile validation issues (current builder logic)

The software profile checks in `scripts/builder.py` identify **5 version-format issues**:

- `data/software/microdata/nada.yaml`: `V5.5`
- `data/software/indicators/pxweb.yaml`: `2025.v1`
- `data/software/geo/ncwms.yaml`: `ncwms-2.5.2`
- `data/software/scientific/dspace.yaml`: `dspace-9.2`
- `data/software/scientific/hyrax.yaml`: `hyrax-chart-3.7.2`

Also, one schema-enum conflict exists:

- `data/software/geo/opendatacube.yaml` uses `has_bulk: Compatible`, but `software.json` allows only `Yes/No/Uncertain`.

### 2) Reference/definition drift

- In `data/reference/software_ids.yaml`, `searchengine` is still listed, but `data/software/opendata/searchengine.yaml` is deleted.
- Catalog usage in `data/datasets/catalogs.jsonl` includes `othergeo`, but `data/software/geo/othergeo.yaml` is deleted.

### 3) Schema drift against actual records

`data/schemes/software.json` appears stricter than real-world records and does not include fields commonly present in software files (`description`, `owner`, `logo_url`).  
This creates a maintenance risk: schema checks and builder behavior are not aligned.

## What is available vs what is missing

### Available (strong parts)

- Rich protocol matrix in `metadata_support` across all records (near-complete population).
- Strong availability of:
  - `website`
  - `last_updated`
  - `export_formats`
  - `capabilities` (moderately strong)
- Good repository/documentation coverage in scientific and geo categories.

### Missing / weak

- Licensing intelligence is shallow (11 records with `license` object).
- Version intelligence is sparse and partially non-normalized.
- `release_date` is unused across all software records.
- Indicators and metadata categories are under-documented (repo/docs/changelog/version coverage low).
- ID map and usage drift (`searchengine`, `othergeo`) can break downstream analytics and software-ID checks.

## Prioritized improvement plan

## P0 (consistency and correctness)

1. Resolve ID drift immediately:
   - Remove `searchengine` from `data/reference/software_ids.yaml` (or restore definition).
   - Replace `othergeo` in catalogs with a valid software ID, or restore `othergeo` definition.
2. Fix enum mismatch:
   - Normalize `has_bulk` in `opendatacube.yaml` to an allowed value (`Yes`, `No`, or `Uncertain`).

## P1 (high-value quality upgrades)

3. Normalize the 5 non-standard version values to a canonical format:
   - Prefer semver-like (`X.Y.Z`) or date (`YYYY-MM-DD`) according to current builder rules.
4. Improve `license` coverage to at least 50% in first pass:
   - Start with open-source platforms where license info is easiest to verify from repo metadata.
5. Lift weakest categories first:
   - `Indicators catalog`: fill `repository_url`, `documentation_url`, `changelog_url`, `version`.
   - `Metadata catalog`: ensure `export_formats` and `version`.

## P2 (model/schema hardening)

6. Align schema and actual data model:
   - Either add `description`, `owner`, `logo_url` to `data/schemes/software.json`, or remove them from records and preserve elsewhere.
7. Add a dedicated `validate-software` command in `scripts/builder.py` using `software.json` + profile checks.
8. Add CI quality gates for software records:
   - No unresolved IDs
   - No invalid enum values
   - Minimum coverage thresholds for `license`, `version`, and docs/repo links by category

## Suggested measurable targets (next iteration)

- `license`: 11/134 -> **>= 60/134**
- `version`: 35/134 -> **>= 80/134**
- `repository_url`: 78/134 -> **>= 100/134**
- `documentation_url`: 94/134 -> **>= 110/134**
- `changelog_url`: 73/134 -> **>= 95/134**
- `release_date`: 0/134 -> **>= 40/134** (or formally deprecate field)

## Operational checklist

- Run and review:
  - `python scripts/builder.py build`
  - `python scripts/check_software_id.py`
- Add software-focused quality report generation to CI with per-category trends.
- Keep `data/reference/software_ids.yaml` generated from actual files only (single source of truth).

## Conclusion

The software registry is now structurally comprehensive and mostly standardized for core access/protocol fields, but still lacks depth in provenance and lifecycle metadata (version, license, release data).  
The highest-impact improvements are: **ID consistency fixes**, **enum normalization**, and **targeted enrichment of indicators/metadata software records**.
