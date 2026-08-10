# Data Quality Rules Analysis Report

**Updated:** 2026-08-10  
**Scope:** `analyze-quality` command in `scripts/builder.py`  
**Purpose:** Document the live rule inventory, current backlog shape, and follow-up work.

> Note: The February 2026 edition of this report listed many enum/directory/URL gaps
> that have since been implemented. This file reflects the **current** pipeline.

---

## 1. Executive Summary

`analyze-quality` runs **41 per-record check functions** plus cross-record checks
(exact/normalized duplicate links, duplicate record ids) on `data/entities/`.

As of the 2026-06-18 baseline (14,470 records):

| Priority  | Count |
|-----------|------:|
| CRITICAL  | 0     |
| IMPORTANT | 280   |
| MEDIUM    | 0     |
| LOW       | 0     |

Open IMPORTANT issues are dominated by:

1. `SOFTWARE_EXPECTED_ENDPOINTS_MISSING_*` (~76%)
2. `DUPLICATE_LINK_NORMALIZED` (~22%)
3. Other (`API_STATUS_MISMATCH`, `COVERAGE_NORMALIZATION`) (~2%)

Completeness rules (topics/tags/langs/description) are largely saturated on entities.

---

## 2. Live Check Inventory

### 2.1 Per-record checks (registered in `analyze_quality`)

| Check | Issue types (examples) | Priority band |
|-------|------------------------|---------------|
| `check_missing_topics` | MISSING_TOPICS | LOW |
| `check_missing_tags` | MISSING_TAGS | LOW |
| `check_missing_description` | MISSING_DESCRIPTION | MEDIUM |
| `check_missing_langs` | MISSING_LANGS | MEDIUM |
| `check_missing_endpoints` | MISSING_ENDPOINTS | MEDIUM |
| `check_software_expected_endpoints` | SOFTWARE_EXPECTED_ENDPOINTS_MISSING_* | IMPORTANT |
| `check_owner_info` | MISSING_OWNER_*, OWNER_*_DIRECTORY_*, PLACEHOLDER_OWNER_NAME | CRITICAL/IMPORTANT/LOW |
| `check_coverage` | MISSING_COVERAGE | IMPORTANT |
| `check_placeholder_values` | PLACEHOLDER_* | IMPORTANT |
| `check_urls` | INVALID_URL, INVALID_OWNER_URL, INVALID_ENDPOINT_URL, INVALID_CATALOG_EXPORT_URL | CRITICAL/MEDIUM |
| `check_required_fields` | MISSING_REQUIRED_FIELD | CRITICAL |
| `check_identifiers` | INCOMPLETE_IDENTIFIER | IMPORTANT |
| `check_license_completeness` | INCONSISTENT_LICENSE | IMPORTANT |
| `check_api_status_coherence` | MISSING_API_STATUS, API_STATUS_MISMATCH | IMPORTANT |
| `check_content_types_access_mode` | MISSING_CONTENT_TYPES, MISSING_ACCESS_MODE | MEDIUM |
| `check_language_validation` | INVALID_LANGUAGE | MEDIUM |
| `check_coverage_normalization` | COVERAGE_NORMALIZATION, DUPLICATE_COVERAGE | IMPORTANT/LOW |
| `check_software_normalization` | SOFTWARE_ID_UNKNOWN, SOFTWARE_NAME_MISMATCH | IMPORTANT |
| `check_catalog_software_coherence` | CATALOG_SOFTWARE_MISMATCH | CRITICAL |
| `check_tag_topic_hygiene` | TAG_HYGIENE, DUPLICATE_TAGS, TOPIC_INCOMPLETE, TOPIC_SCHEMA_VIOLATION | MEDIUM/LOW |
| `check_description_quality` | SHORT_DESCRIPTION | MEDIUM |
| `check_uid_id_consistency` | INVALID_UID, INVALID_ID | CRITICAL |
| `check_contact_info` | MISSING_CONTACT_INFO | LOW |
| `check_status_directory_uid_consistency` | STATUS_DIRECTORY_MISMATCH | IMPORTANT |
| `check_status_api_status_coherence_extended` | STATUS_API_STATUS_MISMATCH | MEDIUM |
| `check_title_quality` | PLACEHOLDER_TITLE | MEDIUM |
| `check_rights_completeness` | RIGHTS_INCOMPLETE | MEDIUM |
| `check_subregion_unk_placeholder` | SUBREGION_UNK_PLACEHOLDER | IMPORTANT |
| `check_subregion_iso3166_2` | SUBREGION_INVALID_ISO3166_2 | IMPORTANT |
| `check_access_mode_values` | INVALID_ACCESS_MODE | IMPORTANT |
| `check_catalog_type_values` | INVALID_CATALOG_TYPE | IMPORTANT |
| `check_status_values` | INVALID_STATUS | IMPORTANT |
| `check_api_status_values` | INVALID_API_STATUS | MEDIUM |
| `check_trust_score_bounds` | TRUST_SCORE_OUT_OF_BOUNDS | MEDIUM |
| `check_identifier_urls` | INVALID_IDENTIFIER_URL | MEDIUM |
| `check_rights_urls` | INVALID_RIGHTS_URL | MEDIUM |
| `check_catalog_type_directory` | CATALOG_TYPE_DIRECTORY_MISMATCH | IMPORTANT |
| `check_country_codes` | INVALID_COUNTRY_CODE | MEDIUM |
| `check_country_subregion_name_consistency` | COUNTRY_NAME_ID_MISMATCH, SUBREGION_NAME_ID_MISMATCH | MEDIUM |
| `check_unknown_country_macroregion` | UNKNOWN_COUNTRY_OR_MACROREGION | IMPORTANT |

### 2.2 Cross-record checks

| Check | Issue type | Priority |
|-------|------------|----------|
| Exact link duplicates | DUPLICATE_LINK | IMPORTANT |
| Canonical link duplicates | DUPLICATE_LINK_NORMALIZED | IMPORTANT |
| Same id in multiple paths | DUPLICATE_RECORD_ID | CRITICAL |

### 2.3 Intentionally not in pipeline

- `check_owner_coverage_coherence` (deprecated stub)
- `check_id_host_correlation` / `ID_HOST_MISMATCH` (omitted; historical ids must stay compatible)

---

## 3. Recent Changes (2026-08)

- **Link-as-endpoint exemption:** GeoServer/ArcGIS Server links that already point at
  service roots do not raise `SOFTWARE_EXPECTED_ENDPOINTS_MISSING_*`.
- **access_modes.yaml aligned** with schema/`ACCESS_MODE_ALLOWED` (7 values).
- **`catalog_export` URL validation** via `INVALID_CATALOG_EXPORT_URL`.
- **Duplicate keeper hints** on exact/normalized link duplicates.
- **`DUPLICATE_RECORD_ID`** for same id across multiple file paths.
- **Integrity vs enrichment tracks:** software-expected endpoints are MEDIUM enrichment;
  CI fails only on integrity CRITICAL/IMPORTANT growth.
- **Owner-type vocabulary:** `data/reference/owner_types.yaml` +
  `INVALID_OWNER_TYPE` / `OWNER_TYPE_NONCANONICAL`.
- **Path consistency:** `PATH_COUNTRY_MISMATCH` (MEDIUM).
- **Not enforced:** id/host correlation (`ID_HOST_MISMATCH`) — omitted to preserve
  compatibility with historical catalog ids.

---

## 4. Follow-up Work

Remaining themes (not yet proposed/implemented):

1. Semantic checks against `langs.csv`, `data_themes.yaml`, `endpoint_types.yaml`
2. Optional scheduled-directory scan and liveness advisory import
3. Modularize quality engine out of `builder.py`
4. Remediate `DUPLICATE_RECORD_ID` pairs

---

## 5. References

- `scripts/builder.py` – check functions and `analyze_quality`
- `scripts/constants.py` – allowed-value sets
- `data/schemes/catalog.json` – Cerberus schema
- `data/reference/` – vocabularies
- `dataquality/baseline_counts.json` – CI regression baseline
- `devdocs/quality-fix-workflow.md` – fix workflow
