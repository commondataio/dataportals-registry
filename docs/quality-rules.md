# Quality issue types

`python scripts/builder.py analyze-quality` emits one row per finding. Codes live in `ISSUE_PRIORITY_MAP` in `scripts/builder.py`. Enrichment-track codes are listed in `ENRICHMENT_ISSUE_TYPES` / `ENRICHMENT_ISSUE_PREFIXES` in `scripts/constants.py` — they are reported but do not fail the CI regression guard.

Integrity-track CRITICAL and IMPORTANT counts must not grow (`dataquality/baseline_counts.json`). How to fix reports: [metadata-quality.md](metadata-quality.md).

## CRITICAL (integrity)

| Code | Fix |
|------|-----|
| `MISSING_REQUIRED_FIELD` | Add the core schema field (`id`, `uid`, `name`, `link`, `catalog_type`, `status`, `software`, `owner`). Missing `access_mode` and `coverage` are reported separately as `MISSING_ACCESS_MODE` / `MISSING_COVERAGE`. |
| `INVALID_URL` | Set `link` to a valid `http`/`https` URL |
| `INVALID_OWNER_URL` | Fix `owner.link` |
| `INVALID_ENDPOINT_URL` | Fix `endpoints[].url` |
| `INVALID_UID` | Run `python scripts/builder.py assign`; do not invent UIDs |
| `INVALID_ID` | Make `id` match the filename (lowercase letters and digits) |
| `CATALOG_SOFTWARE_MISMATCH` | Align `catalog_type` with `software.category`, or retag `software.id` |
| `DUPLICATE_RECORD_ID` | Merge or rename colliding `id` values |

## IMPORTANT (integrity)

| Code | Fix |
|------|-----|
| `MISSING_OWNER_NAME` | Set `owner.name` |
| `MISSING_OWNER_TYPE` | Set canonical `owner.type` from `data/reference/owner_types.yaml` |
| `INVALID_OWNER_TYPE` | Replace illegal `owner.type` with a canonical value |
| `MISSING_OWNER_LOCATION` | Set `owner.location.country.{id,name}` |
| `OWNER_LOCATION_SUBREGION_REQUIRED` | Regional/local owners: `owner.location.level` 30+ and a subregion |
| `OWNER_SUBREGION_FEDERAL_DIRECTORY_MISMATCH` | Move regional/local catalogs out of `Federal/` into a subregion folder |
| `MISSING_COVERAGE` | Add at least one `coverage[].location.country` |
| `COVERAGE_NORMALIZATION` | Normalize country / macroregion ids and names |
| `PLACEHOLDER_CATALOG_TYPE` | Replace placeholder `catalog_type` |
| `PLACEHOLDER_STATUS` | Replace placeholder `status` |
| `PLACEHOLDER_SOFTWARE` | Replace placeholder software with a real `software.id` or `custom` |
| `INCOMPLETE_IDENTIFIER` | Each identifier needs `id` and `value` |
| `INCONSISTENT_LICENSE` | Align `rights.license_id` / `license_name` / `license_url` |
| `API_STATUS_MISMATCH` | If `api` is true, set a coherent `api_status` |
| `MISSING_API_STATUS` | Set `api_status` when `api: true` |
| `MISSING_ENDPOINTS` | Add at least one `endpoints[]` entry when `api: true` |
| `SOFTWARE_ID_UNKNOWN` | Use an id from `data/software/` or `custom` |
| `SOFTWARE_NAME_MISMATCH` | Match `software.name` to the software definition |
| `STATUS_DIRECTORY_MISMATCH` | Scheduled records belong under `data/scheduled/`; verified under `data/entities/` |
| `SUBREGION_INVALID_ISO3166_2` | Use a valid ISO 3166-2 subregion id |
| `SUBREGION_UNK_PLACEHOLDER` | Replace `UNK` / placeholder subregion codes |
| `UNKNOWN_COUNTRY_OR_MACROREGION` | Replace `Unknown` country or macroregion |
| `INVALID_ACCESS_MODE` | Use values from `data/reference/access_modes.yaml` |
| `INVALID_CATALOG_TYPE` | Use values from `data/reference/catalog_types.yaml` |
| `INVALID_STATUS` | Use `active`, `inactive`, `scheduled`, or `deprecated` |
| `CATALOG_TYPE_DIRECTORY_MISMATCH` | Move the file into the type folder that matches `catalog_type` |
| `DUPLICATE_LINK` | Keep one record for the exact same `link` |
| `DUPLICATE_LINK_NORMALIZED` | Keep one record after URL canonicalization (https, no www, no trailing slash) |

## MEDIUM

| Code | Track | Fix |
|------|-------|-----|
| `MISSING_DESCRIPTION` | Enrichment | Add a short `description` |
| `SHORT_DESCRIPTION` | Enrichment | Expand descriptions shorter than the quality threshold |
| `MISSING_LANGS` | Enrichment | Add `langs` as `{id, name}` (e.g. `EN` / `English`) |
| `INVALID_LANGUAGE` | Integrity | Fix `langs` entries that lack `id` and `name` |
| `MISSING_CONTENT_TYPES` | Enrichment | Add `content_types` (e.g. `dataset`) |
| `MISSING_ACCESS_MODE` | Integrity | Set `access_mode` (prefer `open` / `restricted`) |
| `TAG_HYGIENE` | Enrichment | Normalize messy tags |
| `TOPIC_INCOMPLETE` | Enrichment | Complete `topics[].{type,id,name}` |
| `STATUS_API_STATUS_MISMATCH` | Integrity | Inactive catalogs should not claim an active API |
| `RIGHTS_INCOMPLETE` | Enrichment | Add license / ToS / privacy URLs when known |
| `PLACEHOLDER_TITLE` | Enrichment | Replace placeholder `name` |
| `PLACEHOLDER_OWNER_NAME` | Integrity | Replace placeholder `owner.name` |
| `INVALID_API_STATUS` | Integrity | Use `active`, `inactive`, or `uncertain` |
| `TRUST_SCORE_OUT_OF_BOUNDS` | Integrity | Recalculate `trust_score` (0–100) |
| `INVALID_IDENTIFIER_URL` | Integrity | Fix `identifiers[].url` |
| `INVALID_RIGHTS_URL` | Integrity | Fix `rights` URLs |
| `INVALID_CATALOG_EXPORT_URL` | Integrity | Fix `catalog_export` if it is a URL |
| `INVALID_COUNTRY_CODE` | Integrity | Use ISO 3166-1 alpha-2 or an allowed special root |
| `COUNTRY_NAME_ID_MISMATCH` | Integrity | Match country `id` to the reference name |
| `SUBREGION_NAME_ID_MISMATCH` | Integrity | Match subregion `id` to the reference name |
| `OWNER_TYPE_NONCANONICAL` | Enrichment | Map synonyms (`NGO` → `Civil society`, `University` → `Academy`) |
| `PATH_COUNTRY_MISMATCH` | Integrity | Directory country must match owner/coverage country |
| `SOFTWARE_EXPECTED_ENDPOINTS_MISSING_*` | Enrichment | Add the harvest endpoints typical for that `software.id` |

## LOW (enrichment unless noted)

| Code | Fix |
|------|-----|
| `MISSING_TOPICS` | Add EU data themes or ISO 19115 topics |
| `MISSING_TAGS` | Add keywords (`government`, `has_api`, …) |
| `MISSING_OWNER_LINK` | Set `owner.link` |
| `DUPLICATE_TAGS` | Deduplicate `tags` |
| `DUPLICATE_COVERAGE` | Remove repeated coverage entries (same country, level, and subregion) |
| `MISSING_CONTACT_INFO` | Add contact when the catalog publishes it |
| `TOPIC_SCHEMA_VIOLATION` | Fix `topics` shape (`type`, `id`, `name`) |

## Software definition issues

`python scripts/builder.py validate-software` emits its own codes for `data/software/` records (they are not part of `analyze-quality` priorities):

| Code | Fix |
|------|-----|
| `SOFTWARE_SUBTYPE_MISSING` | Set a `subtype` from the allowed list |
| `SOFTWARE_SUBTYPE_INVALID` | Use a `subtype` from `data/schemes/software.json` |
| `SOFTWARE_SUBTYPE_CATEGORY_MISMATCH` | Pick a subtype compatible with `category` |
| `SOFTWARE_VERSION_FORMAT` | Use a valid `version` string |
| `SOFTWARE_INVALID_URL` | Fix `website` / `documentation_url` / `repository_url` |
| `SOFTWARE_INVALID_DATE` / `SOFTWARE_INVALID_DATETIME` | Fix date-formatted fields |
| `SOFTWARE_INVALID_PLUGINS` / `SOFTWARE_INVALID_PLUGIN_ITEM` / `SOFTWARE_PLUGIN_MISSING_NAME` | Fix the `plugins` list shape |
| `SOFTWARE_INVALID_CAPABILITIES` / `SOFTWARE_INVALID_CAPABILITY_ITEM` | Fix the `capabilities` list shape |
| `SOFTWARE_INVALID_EXPORT_FORMATS` / `SOFTWARE_INVALID_EXPORT_FORMAT_ITEM` | Fix the `export_formats` list shape |
| `SOFTWARE_INVALID_LICENSE` / `SOFTWARE_LICENSE_MISSING_TYPE` | Give `license` a valid `type` |

## Related

- [metadata-quality.md](metadata-quality.md)
- [vocabularies.md](vocabularies.md)
- [cli.md](cli.md)
- Maintainer notes: [devdocs/quality-fix-workflow.md](https://github.com/datenoio/dataportals-registry/blob/main/devdocs/quality-fix-workflow.md)
