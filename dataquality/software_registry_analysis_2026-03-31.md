# Software Registry Deep Analysis

Date: 2026-03-31  
Scope: `data/software` software profile records and cross-reference with `data/entities` + `data/scheduled`

## Executive Findings

1. The software registry has broad coverage but uneven depth: 134 software records are present, with strong core fields but sparse governance metadata (license/version/repository links).
2. One active software ID is missing from definitions: `othergeo` is referenced in entities but its definition file was removed.
3. Metadata quality is weakest in `indicators` and `metadata` categories, with high "Unknown/Uncertain" density.
4. Several highest-impact software profiles (used by thousands of catalogs) are under-documented.
5. Category vocabulary is slightly inconsistent (`Metadata` vs `Metadata catalog`) and should be normalized.

## Inventory and Coverage

- YAML files in `data/software`: **135**
- Actual software records (`type: Software`): **134**
- Non-record helper files: `data/software/types.yaml`
- Definitions by directory:
  - `scientific`: 44
  - `geo`: 39
  - `opendata`: 25
  - `indicators`: 18
  - `microdata`: 4
  - `metadata`: 3
  - root-level software profile: `custom`

## Completeness Snapshot

Presence across 134 software profiles:

- `description`: 133
- `owner`: 133
- `logo_url`: 133
- `website`: 133
- `export_formats`: 131
- `last_updated`: 133
- `capabilities`: 97
- `documentation_url`: 94
- `repository_url`: 78
- `changelog_url`: 73
- `version`: 38
- `license`: 11

Interpretation: baseline descriptive fields are almost complete, but provenance and maintainability fields are often missing.

## Cross-Reference with Catalog Usage

- Software definitions: **134**
- Software IDs used in entities/scheduled: **131**
- Missing definitions for used IDs: **1**
  - `othergeo` (used in `data/entities/DE/DE-TH/geo/wwwgeoportalthde.yaml`)
- Unused software definitions: **4**
  - `dlibra`
  - `icat`
  - `opendatareg`
  - `samvera`

## High-Impact Profiles with Low Metadata

Profiles with very high usage and low metadata depth (score based on presence of repo/docs/changelog/license/version/capabilities):

- `arcgisserver` used by 3274 catalogs, low metadata score
- `arcgishub` used by 2548 catalogs, low metadata score
- `custom` used by 1552 catalogs, no rich metadata
- `opendatasoft` used by 421 catalogs, low metadata score
- `socrata` used by 219 catalogs, low metadata score

These should be first in line for enrichment because each record influences large portions of the registry.

## Data Certainty Analysis

Average unknown/uncertain ratio over structured capability fields:

- Overall: **0.327**
- By category:
  - `indicators`: **0.675** (highest uncertainty)
  - `metadata`: **0.603**
  - `opendata`: 0.280
  - `geo`: 0.280
  - `scientific`: 0.245
  - `microdata`: 0.058

Fields with highest unknown rates:

- `metadata_support.opensearch`: 74.6%
- `metadata_support.schema-org`: 68.7%
- `metadata_support.openaire`: 40.3%
- `metadata_support.ogcrecords`: 39.6%
- `metadata_support.dcat`: 38.1%

Interpretation: many records are structurally valid but semantically weak due to uncertain capability claims.

## Consistency Issues

1. Category term mismatch in metadata software records:
   - Files in `data/software/metadata/*.yaml` use `category: Metadata catalog`
   - Canonical mapping in `types.yaml` uses `Metadata`
2. `custom` profile is intentionally generic but currently missing:
   - `description`
   - `owner`
   - `logo_url`
   - website/docs/repo/changelog/license/version
3. `last_updated` is generally good (133/134 present), but one profile (`mwmb`) is missing it.

## What Is Missing

### Missing software record

- Re-add a profile for `othergeo` (or update the referencing catalog to a valid existing software ID).

### Missing metadata dimensions at scale

- Licensing metadata is missing for 123/134 software records.
- Version metadata is missing for 96/134 records.
- Source/repository links are missing for 56/134 records.
- Changelog links are missing for 61/134 records.

### Missing quality governance

- No explicit quality score or confidence field per software profile.
- No automated stale-record threshold policy (for example, "review if last_updated > 365 days").
- No normalization layer for category vocabulary variants.

## Prioritized Improvement Plan

### P0 (Immediate, correctness)

1. Restore `othergeo` software definition or remap affected catalogs.
2. Normalize category values for metadata profiles (`Metadata` vs `Metadata catalog`) and enforce one canonical term.

### P1 (High impact, low effort)

1. Enrich top-used software (`arcgisserver`, `arcgishub`, `custom`, `opendatasoft`, `socrata`) with:
   - documentation URL
   - repository URL (if public)
   - license
   - version/update channel
2. Add missing `last_updated` for `mwmb`.
3. Add minimum description/owner/logo for `custom` profile, even if generic.

### P2 (Systematic quality uplift)

1. Define a minimum metadata baseline for every software profile:
   - required: id, name, type, category, description, owner, website, last_updated
   - strongly recommended: documentation_url, license.type, capabilities
2. Introduce confidence scoring for uncertain capabilities (for example, `confidence: low|medium|high`).
3. Add a periodic script to report:
   - records with high unknown ratio (>0.60)
   - records missing license
   - records missing docs/repo
   - records not updated in >365 days

## Suggested Automation Additions

1. Add a software-focused checker script (or builder subcommand) producing `dataquality/software_report.jsonl`.
2. Add CI gate rules:
   - fail on referenced but undefined software IDs
   - warn on missing `license` for software used by more than N catalogs
   - warn on category vocabulary mismatch
3. Add deterministic quality score output per software profile for triage dashboards.

## Proposed KPI Targets (Next Iteration)

- Missing software definitions: 1 -> 0
- License coverage: 11/134 -> at least 60/134
- Version coverage: 38/134 -> at least 80/134
- Unknown ratio in `indicators`: 0.675 -> below 0.45
- Top-5 most used software records: all have docs + license + last_updated

## Notes

- This analysis intentionally separated helper taxonomy files from `type: Software` records.
- Usage impact was calculated from all YAML entries in `data/entities` and `data/scheduled`.
- "Unknown ratio" was computed over structured status-like fields (`datatypes`, `has_api`, `has_bulk`, `metadata_support`, `pid_support`, `rights_management`, `storage_type`).
