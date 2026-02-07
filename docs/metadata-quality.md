# Metadata quality and recommended fields

This document lists **recommended** (non-required) fields that improve catalog discoverability and consistency. The schema in `data/schemes/catalog.json` defines required fields; the quality analysis in `dataquality/` flags missing or inconsistent values.

## Recommended fields

Fill these when possible so that catalogs are easier to discover and maintain:

| Field | Purpose |
|-------|--------|
| **description** | Short human-readable description of the catalog. Quality rules flag missing or very short descriptions. |
| **endpoints** | API endpoints (e.g. `type: ckan`, `url: ...`) so crawlers and tools can access catalog data. |
| **identifiers** | External IDs (e.g. wikidata, re3data, fairsharing) for linking and enrichment. |
| **langs** | Supported languages (e.g. `id: EN`, `name: English`). |
| **tags** | Keywords for search and filtering (e.g. `government`, `has_api`, `open data`). |
| **topics** | Thematic classification (e.g. EU data themes: `type: eudatatheme`, `id: GOVE`, `name: Government and public sector`). |
| **owner.link** | URL of the owning organization. |
| **owner.location** | Full location (country and, when relevant, subregion/macroregion) for geographic discovery. |
| **api_status** | When `api: true`, set `api_status` to `active`, `inactive`, or `uncertain`. |

## Quality rules and reports

The quality pipeline (`python scripts/builder.py analyze-quality`) writes reports under **dataquality/**.

- **dataquality/rules/** – Per-rule breakdowns (e.g. `MISSING_DESCRIPTION.txt`, `MISSING_ENDPOINTS.txt`, `MISSING_API_STATUS.txt`, `MISSING_LANGS.txt`, `MISSING_TAGS.txt`, `MISSING_TOPICS.txt`, `MISSING_OWNER_LINK.txt`, `MISSING_OWNER_LOCATION.txt`, `COVERAGE_NORMALIZATION.txt`, `TAG_HYGIENE.txt`, `SHORT_DESCRIPTION.txt`).
- **dataquality/priorities/** – Issues grouped by priority (CRITICAL, IMPORTANT, MEDIUM, LOW).
- **dataquality/full_report.txt** – Summary and full list of issues.
- **dataquality/primary_priority.jsonl** – Machine-readable list of records with issues (for fix scripts and Cursor/agent workflows).

See [devdocs/quality-fix-workflow.md](devdocs/quality-fix-workflow.md) for how to fix issues and re-run validation.

## Reference vocabularies

Controlled values for key fields are maintained under **data/reference/**:

- **catalog_types.yaml** – Allowed `catalog_type` values.
- **software_ids.yaml** – Canonical `software.id` values (aligned with `data/software/`).
- **status.yaml** – Allowed `status` values (`active`, `inactive`, `scheduled`).
- **access_modes.yaml** – Allowed `access_mode` list values (`open`, `restricted`).

Use these when editing YAMLs or building tooling so that metadata stays consistent.
