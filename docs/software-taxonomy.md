# Software taxonomy

Each catalog points at a software definition via `software.id` / `software.name`. Definitions live in `data/software/` and export to `data/datasets/software.jsonl`.

## Identity

| Field | Rule |
|-------|------|
| `software.id` | Lowercase, matches filename (`ckan.yaml` → `ckan`) |
| `software.name` | Display name (`CKAN`) |
| Unknown / bespoke sites | `id: custom` (see `data/software/custom.yaml`) |

Canonical IDs: `data/reference/software_ids.yaml`.

## Category and subtype

Software records declare:

- `category` — domain family, aligned with catalog types (Open data portal, Geoportal, Scientific data repository, …)
- `subtype` — product form, for cross-category comparison

Typical subtypes include `data_portal_platform`, `managed_saas_service`, and `protocol_or_api_server`. Use `subtype` to compare self-hosted platforms vs managed SaaS vs protocol-first components.

Human-readable category notes: `data/software/types.yaml`.

## Layout

```
data/software/
├── opendata/ckan.yaml
├── geo/geonetwork.yaml
├── scientific/dataverse.yaml
├── indicators/opensdg.yaml
├── microdata/nada.yaml
└── custom.yaml
```

## What a software record contains

Beyond `id` / `name` / `category`, records may include:

- `has_api`, `has_bulk`, `datatypes`
- `metadata_support` (CKAN API, DCAT, CSW, OAI-PMH, STAC, SDMX, …)
- `pid_support` (DOI)
- `rights_management`
- `repository_url`, `documentation_url`, `website`

Quality checks flag catalogs whose software implies endpoints that are missing (`SOFTWARE_EXPECTED_ENDPOINTS_MISSING_*`) on the enrichment track — they do not fail CI by themselves.

## Querying software usage

```sql
SELECT json_extract_string(software, '$.id') AS sid, count(*) AS n
FROM catalogs
GROUP BY 1
ORDER BY n DESC
LIMIT 20;
```

Or read `data/datasets/bysoftware/` for pre-sliced JSONL.

Matching a live site to a `software.id`: [discovery.md](discovery.md).
