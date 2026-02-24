# Data Quality Rules Analysis Report

**Generated:** 2026-02-24  
**Scope:** `analyze-quality` command in `scripts/builder.py`  
**Purpose:** Identify existing rules and suggest missing rules to improve catalog record quality.

---

##  1. Executive Summary

The `analyze-quality` command runs **27 quality check functions** plus **1 cross-record check** (duplicate links) on catalog YAML files. This report analyzes the current rule coverage, identifies gaps, and recommends new rules to improve data quality.

**Key findings:**

- One implemented check (`check_subregion_iso3166_2`) is **not registered** in the analyze-quality pipeline
- Several schema-constrained fields lack **value validation** rules
- Multiple schema fields have **no quality checks** at all
- Three checks are **deprecated** (return `None`) but remain in the pipeline

---

## 2. Existing Rules Inventory

###  2.1 Active Check Functions (27)


| Check Function                               | Issue Types                                                                                                                                                                               | Priority           | Description                             |
| -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ | --------------------------------------- |
| `check_missing_topics`                       | MISSING_TOPICS                                                                                                                                                                            | LOW                | Topics field missing or empty           |
| `check_missing_tags`                         | MISSING_TAGS                                                                                                                                                                              | LOW                | Tags field missing or empty             |
| `check_missing_description`                  | MISSING_DESCRIPTION                                                                                                                                                                       | MEDIUM             | Placeholder or empty description        |
| `check_missing_langs`                        | MISSING_LANGS                                                                                                                                                                             | MEDIUM             | Langs field missing or empty            |
| `check_missing_endpoints`                    | MISSING_ENDPOINTS                                                                                                                                                                         | MEDIUM             | API records without endpoints           |
| `check_software_expected_endpoints`          | SOFTWARE_EXPECTED_ENDPOINTS_MISSING_*                                                                                                                                                     | IMPORTANT          | API-capable software without endpoints  |
| `check_owner_info`                           | MISSING_OWNER_NAME, PLACEHOLDER_OWNER_NAME, MISSING_OWNER_TYPE, MISSING_OWNER_LINK, MISSING_OWNER_LOCATION, OWNER_LOCATION_SUBREGION_REQUIRED, OWNER_SUBREGION_FEDERAL_DIRECTORY_MISMATCH | CRITICAL/IMPORTANT | Owner completeness and consistency      |
| `check_coverage`                             | MISSING_COVERAGE                                                                                                                                                                          | IMPORTANT          | Coverage field missing or empty         |
| `check_placeholder_values`                   | PLACEHOLDER_CATALOG_TYPE, PLACEHOLDER_STATUS, PLACEHOLDER_SOFTWARE                                                                                                                        | IMPORTANT          | Placeholder values in key fields        |
| `check_urls`                                 | INVALID_URL, INVALID_OWNER_URL, INVALID_ENDPOINT_URL                                                                                                                                      | CRITICAL           | URL format validation                   |
| `check_required_fields`                      | MISSING_REQUIRED_FIELD                                                                                                                                                                    | CRITICAL           | Missing id, uid, name, link, etc.       |
| `check_identifiers`                          | INCOMPLETE_IDENTIFIER                                                                                                                                                                     | IMPORTANT          | Identifier missing id or value          |
| `check_license_completeness`                 | INCONSISTENT_LICENSE                                                                                                                                                                      | IMPORTANT          | Inconsistent license fields             |
| `check_api_status_coherence`                 | MISSING_API_STATUS, API_STATUS_MISMATCH                                                                                                                                                   | IMPORTANT          | API status consistency                  |
| `check_content_types_access_mode`            | MISSING_CONTENT_TYPES, MISSING_ACCESS_MODE                                                                                                                                                | MEDIUM             | Missing content_types or access_mode    |
| `check_language_validation`                  | INVALID_LANGUAGE                                                                                                                                                                          | MEDIUM             | Lang entries without id and name        |
| `check_coverage_normalization`               | COVERAGE_NORMALIZATION, DUPLICATE_COVERAGE                                                                                                                                                | IMPORTANT/LOW      | Coverage level, macroregion, duplicates |
| `check_software_normalization`               | SOFTWARE_ID_UNKNOWN, SOFTWARE_NAME_MISMATCH                                                                                                                                               | IMPORTANT          | Software ID/name validation             |
| `check_catalog_software_coherence`           | CATALOG_SOFTWARE_MISMATCH                                                                                                                                                                 | CRITICAL           | catalog_type vs software.id             |
| `check_tag_topic_hygiene`                    | TAG_HYGIENE, DUPLICATE_TAGS, TOPIC_INCOMPLETE                                                                                                                                             | MEDIUM/LOW         | Tag length, duplicates; topic structure |
| `check_description_quality`                  | SHORT_DESCRIPTION                                                                                                                                                                         | MEDIUM             | Description < 40 chars                  |
| `check_uid_id_consistency`                   | INVALID_UID, INVALID_ID                                                                                                                                                                   | CRITICAL           | UID format, ID alphanumeric             |
| `check_contact_info`                         | MISSING_CONTACT_INFO                                                                                                                                                                      | LOW                | Active+restricted without owner link    |
| `check_status_directory_uid_consistency`     | STATUS_DIRECTORY_MISMATCH                                                                                                                                                                 | IMPORTANT          | Scheduled in entities/                  |
| `check_status_api_status_coherence_extended` | STATUS_API_STATUS_MISMATCH                                                                                                                                                                | MEDIUM             | status vs api_status coherence          |
| `check_title_quality`                        | PLACEHOLDER_TITLE                                                                                                                                                                         | MEDIUM             | Short, generic, or URL-like names       |
| `check_rights_completeness`                  | RIGHTS_INCOMPLETE                                                                                                                                                                         | MEDIUM             | Rights with only 1 of 3 license fields  |
| `check_subregion_unk_placeholder`            | SUBREGION_UNK_PLACEHOLDER                                                                                                                                                                 | IMPORTANT          | XX-UNK placeholder subregions           |


### 2.2 Cross-Record Check (Post-Scan)


| Check                    | Issue Type     | Priority  | Description                        |
| ------------------------ | -------------- | --------- | ---------------------------------- |
| Duplicate link detection | DUPLICATE_LINK | IMPORTANT | Multiple records sharing same link |


### 2.3 Implemented but NOT Registered


| Check Function              | Issue Type                  | Status                                                                                                                                                     |
| --------------------------- | --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `check_subregion_iso3166_2` | SUBREGION_INVALID_ISO3166_2 | **Defined and in ISSUE_PRIORITY_MAP but NOT in the checks list** – subregion codes validated against `data/reference/subregions/IP2LOCATION-ISO3166-2.CSV` |


**Recommendation:** Add `check_subregion_iso3166_2` to the `checks` list in `analyze_quality()`.

### 2.4 Deprecated / Unused Checks


| Check Function                   | In Pipeline | Original Purpose                                             |
| -------------------------------- | ----------- | ------------------------------------------------------------ |
| `check_owner_coverage_coherence` | Yes         | Owner vs coverage coherence – returns `None` (deprecated)    |
| `check_path_country_consistency` | No          | Path vs metadata country – defined but not in checks list    |
| `check_id_host_correlation`      | No          | ID vs link host correlation – defined but not in checks list |


`check_owner_coverage_coherence` is in the pipeline but always returns `None`. Consider removing it or re-implementing if still desired.

---

## 3. Schema Fields Without Quality Rules

Based on `data/schemes/catalog.json` and the catalog structure:


| Field                             | Schema Constraints                                                     | Current Check                           | Gap                                                                                                      |
| --------------------------------- | ---------------------------------------------------------------------- | --------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `access_mode`                     | List of: open, restricted, limited, public, protected, closed, private | MISSING_ACCESS_MODE only                | **No validation of values** – invalid strings (e.g. "free", "open_access") not flagged                   |
| `api_status`                      | String                                                                 | MISSING_API_STATUS, API_STATUS_MISMATCH | **No allowed-value validation** – schema has no allowed list; common values: active, inactive, uncertain |
| `catalog_type`                    | 13 allowed values                                                      | PLACEHOLDER_CATALOG_TYPE (Unknown/null) | **No validation of invalid values** – typos or non-standard values not caught                            |
| `status`                          | active, inactive, scheduled, deprecated                                | PLACEHOLDER_STATUS (Unknown/null)       | **No validation of invalid values**                                                                      |
| `trust_score`                     | 0–100                                                                  | None                                    | **No bounds check** – values outside 0–100 not flagged                                                   |
| `identifiers[].url`               | URL string                                                             | None                                    | **No URL validation** – identifier URLs not checked                                                      |
| `rights.tos_url`                  | URL                                                                    | None                                    | **No URL validation**                                                                                    |
| `rights.privacy_policy_url`       | URL                                                                    | None                                    | **No URL validation**                                                                                    |
| `rights.license_url`              | URL                                                                    | Checked for presence in license logic   | **No format validation** when present                                                                    |
| `catalog_export`                  | String (URL)                                                           | None                                    | **No validation**                                                                                        |
| `owner.type`                      | String                                                                 | MISSING_OWNER_TYPE                      | **No allowed-value validation** – schema expects owner type but no enum                                  |
| `langs[].id`                      | String                                                                 | Structure check (id+name)               | **No ISO 639-1 format validation**                                                                       |
| `coverage[].location.country.id`  | String/integer                                                         | Presence only                           | **No ISO 3166-1 country code validation**                                                                |
| `coverage[].location.macroregion` | id, name                                                               | COVERAGE_NORMALIZATION (missing)        | **No macroregion id format validation** – UN M49 codes expected                                          |
| `topics`                          | List of {type, id, name}                                               | TOPIC_INCOMPLETE                        | **No schema validation** – topics can be strings (e.g. "Open Data") vs dicts per schema                  |
| `file path vs catalog_type`       | Convention                                                             | None                                    | **No check** – file in `geo/` but catalog_type "Open data portal"                                        |


---

## 4. Suggested New Rules

### 4.1 High Priority (Immediate Impact)


| Rule ID                        | Suggested Check                             | Description                                       | Priority  |
| ------------------------------ | ------------------------------------------- | ------------------------------------------------- | --------- |
| **REGISTER_SUBREGION_ISO3166** | Add `check_subregion_iso3166_2` to pipeline | Fix: register existing check                      | —         |
| **INVALID_ACCESS_MODE**        | `check_access_mode_values`                  | Validate values against schema allowed list       | IMPORTANT |
| **INVALID_CATALOG_TYPE**       | `check_catalog_type_values`                 | Validate catalog_type against allowed values      | IMPORTANT |
| **INVALID_STATUS**             | `check_status_values`                       | Validate status against allowed values            | IMPORTANT |
| **INVALID_API_STATUS**         | `check_api_status_values`                   | Validate api_status (active, inactive, uncertain) | MEDIUM    |
| **TRUST_SCORE_OUT_OF_BOUNDS**  | `check_trust_score_bounds`                  | trust_score must be 0–100 when present            | MEDIUM    |


### 4.2 Medium Priority (Data Consistency)


| Rule ID                             | Suggested Check                  | Description                                                                    | Priority  |
| ----------------------------------- | -------------------------------- | ------------------------------------------------------------------------------ | --------- |
| **INVALID_IDENTIFIER_URL**          | Extend `check_urls` or new check | Validate identifiers[].url when present                                        | MEDIUM    |
| **INVALID_RIGHTS_URLS**             | `check_rights_urls`              | Validate tos_url, privacy_policy_url, license_url format                       | MEDIUM    |
| **CATALOG_TYPE_DIRECTORY_MISMATCH** | `check_catalog_type_directory`   | File in geo/ but catalog_type "Open data portal" (use MAP_CATALOG_TYPE_SUBDIR) | IMPORTANT |
| **INVALID_COUNTRY_CODE**            | `check_country_codes`            | Validate owner/coverage country.id against ISO 3166-1                          | MEDIUM    |
| **TOPIC_SCHEMA_VIOLATION**          | Extend `check_tag_topic_hygiene` | Topics as bare strings vs dict {type, id, name}                                | LOW       |


### 4.3 Lower Priority (Nice to Have)


| Rule ID                          | Suggested Check                    | Description                                             | Priority |
| -------------------------------- | ---------------------------------- | ------------------------------------------------------- | -------- |
| **INVALID_LANG_CODE**            | Extend `check_language_validation` | Validate langs[].id as ISO 639-1 (2–3 chars)            | LOW      |
| **DUPLICATE_IDENTIFIERS**        | `check_duplicate_identifiers`      | Same identifier id repeated                             | LOW      |
| **DESCRIPTION_EXCESSIVE_LENGTH** | Extend `check_description_quality` | Description > 2000 chars (unreasonable)                 | LOW      |
| **INVALID_OWNER_TYPE**           | `check_owner_type_values`          | Validate against known owner types if vocabulary exists | LOW      |
| **DEPRECATED_STATUS_ADVISORY**   | `check_deprecated_status`          | Records with status=deprecated – advisory for review    | LOW      |


### 4.4 Optional / Future (External or Heavy)


| Rule ID                                | Suggested Check                               | Description                                     | Notes                                            |
| -------------------------------------- | --------------------------------------------- | ----------------------------------------------- | ------------------------------------------------ |
| **LINK_REACHABILITY**                  | HTTP HEAD/GET check                           | Link returns 4xx/5xx                            | Requires network; consider optional/separate job |
| **ENDPOINT_REACHABILITY**              | HTTP check on endpoints                       | Endpoint URLs return errors                     | Same as above                                    |
| **RE-ENABLE_OWNER_COVERAGE_COHERENCE** | Re-implement `check_owner_coverage_coherence` | Owner country vs coverage countries consistency | Was deprecated; could be revived                 |


---

## 5. Summary Table: Rules to Add


| #   | Rule                                 | Effort  | Impact |
| --- | ------------------------------------ | ------- | ------ |
| 1   | Register `check_subregion_iso3166_2` | Trivial | High   |
| 2   | INVALID_ACCESS_MODE                  | Low     | Medium |
| 3   | INVALID_CATALOG_TYPE                 | Low     | Medium |
| 4   | INVALID_STATUS                       | Low     | Medium |
| 5   | INVALID_API_STATUS                   | Low     | Low    |
| 6   | TRUST_SCORE_OUT_OF_BOUNDS            | Low     | Low    |
| 7   | INVALID_IDENTIFIER_URL               | Low     | Medium |
| 8   | INVALID_RIGHTS_URLS                  | Low     | Medium |
| 9   | CATALOG_TYPE_DIRECTORY_MISMATCH      | Medium  | High   |
| 10  | INVALID_COUNTRY_CODE                 | Medium  | Medium |
| 11  | TOPIC_SCHEMA_VIOLATION               | Low     | Low    |


---

## 6. Implementation Notes

1. **Value validation:** Reuse schema allowed lists from `catalog.json` where possible, or define constants in `scripts/constants.py`.
2. **File path checks:** Use `record["_file_path"]` and `MAP_CATALOG_TYPE_SUBDIR`; extend the map to include "Open data portal" → "opendata", "Other" → "other", etc.
3. **Country codes:** Use `data/reference/` or a standard ISO 3166-1 list; consider existing `COUNTRIES` in constants.
4. **Priority assignment:** Add new issue types to `ISSUE_PRIORITY_MAP` and `RULE_DESCRIPTIONS` for consistent reporting.

---

## 7. References

- `scripts/builder.py` – check functions (lines ~1242–2595), `analyze_quality` (lines ~3517–3910)
- `data/schemes/catalog.json` – schema and allowed values
- `scripts/constants.py` – `MAP_CATALOG_TYPE_SUBDIR`, `MAP_SOFTWARE_OWNER_CATALOG_TYPE`, `COUNTRIES`
- `AGENTS.md` – catalog types and directory structure
- `devdocs/quality-fix-workflow.md` – quality workflow

