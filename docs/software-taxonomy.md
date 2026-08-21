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

Typical subtypes:

| `subtype` | Use for |
|-----------|---------|
| `data_portal_platform` | Self-hosted open-data CMS (CKAN, DKAN, uData, Idra) |
| `scientific_repository_platform` | Dataverse, DSpace, Invenio, Figshare, OPUS, Omeka S, Fedora, Esploro, PHAIDRA |
| `geospatial_catalog_platform` | GeoNetwork, GeoNode, ArcGIS Hub, gvSIG Online, VertiGIS WebOffice, GeoMedia WebMap, disy Cadenza, Mapbender |
| `microdata_catalog_platform` | NADA, NESSTAR, REDATAM |
| `indicators_data_platform` | PxWeb, OpenSDG, Knoema |
| `metadata_registry_platform` | FAIR Data Point, Aristotle MDR |
| `protocol_or_api_server` | STAC, THREDDS, OPeNDAP, SPARQL endpoints |
| `geospatial_service_middleware` | GeoServer, MapServer, MapTiler Server, deegree, rasdaman |
| `cms_or_app_framework` | WordPress, Drupal, Liferay used as a catalog |
| `managed_saas_service` | Socrata, OpenDataSoft, CONTENTdm, vendor-hosted Hub |
| `domain_data_infrastructure` | Domain-specific stacks (GBIF IPT, SciCat) |
| `general_software` | Catch-all, including `custom` |

Human-readable category notes: `data/software/types.yaml`. Allowed list: `data/schemes/software.json`.

## Layout

```
data/software/
├── opendata/ckan.yaml
├── opendata/idra.yaml
├── geo/geonetwork.yaml
├── geo/mapserver.yaml
├── scientific/dataverse.yaml
├── scientific/opus.yaml
├── indicators/opensdg.yaml
├── microdata/nada.yaml
├── metadata/fairdatapoint.yaml
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

## Adding a software definition

1. Confirm the product is shared (not a one-off site). One-off sites keep `software.id: custom`.
2. Pick `id`: lowercase letters/digits, matching the filename (`data/software/{category}/{id}.yaml`).
3. Set `type: Software`, `name`, `category` (aligned with catalog types), and `subtype` from the table above.
4. Fill `has_api`, `metadata_support`, `website`, and `documentation_url` when known. See `data/software/opendata/ckan.yaml`.
5. Add the id to `data/reference/software_ids.yaml` if that list is maintained in the same change.
6. Run `python scripts/builder.py validate-software` and `python scripts/builder.py build`.

Do not create a new software id for a single catalog unless several independent installations exist or are expected.

When the product is a discovery target (shared platform with live installations), add fingerprints and search queries to the matching guide: [discovery-opendata.md](discovery-opendata.md), [discovery-geoportals.md](discovery-geoportals.md), [discovery-scientific.md](discovery-scientific.md), [discovery-metadata.md](discovery-metadata.md), [discovery-indicators.md](discovery-indicators.md), or [discovery-other.md](discovery-other.md).

## Querying software usage

```sql
SELECT json_extract_string(software, '$.id') AS sid, count(*) AS n
FROM catalogs
GROUP BY 1
ORDER BY n DESC
LIMIT 20;
```

Matching a live site to a `software.id`: [discovery.md](discovery.md). Per-platform Google and Censys queries: [discovery-opendata.md](discovery-opendata.md), [discovery-geoportals.md](discovery-geoportals.md), [discovery-scientific.md](discovery-scientific.md), [discovery-metadata.md](discovery-metadata.md), [discovery-indicators.md](discovery-indicators.md), [discovery-other.md](discovery-other.md). After YAML exists: [apidetect.md](apidetect.md).
