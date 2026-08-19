# Data model

Each catalog is one YAML document validated against `data/schemes/catalog.json` (Cerberus). JSON Schema with descriptions: `data/schemes/catalog.schema.json`. DCAT/schema.org mappings: `data/schemes/catalog.context.jsonld`.

## Required fields

| Field | Type | Notes |
|-------|------|--------|
| `id` | string | Filename stem; lowercase letters and digits |
| `uid` | string | `cdi########` for entities; assigned by `builder.py assign` |
| `name` | string | Display name |
| `link` | string | Catalog URL |
| `catalog_type` | string | See [catalog-types.md](catalog-types.md) |
| `access_mode` | list of string | Prefer `open` or `restricted` |
| `status` | string | `active`, `inactive`, `scheduled`, or `deprecated` |
| `software` | object | `{id, name}` — `id` should exist under `data/software/` |
| `owner` | object | `{name, type, location.country.{id,name}}` |
| `coverage` | list | At least one `{location.country.{id,name}}` |

## Recommended fields

| Field | Purpose |
|-------|---------|
| `description` | Short human-readable summary |
| `endpoints` | Harvestable APIs (`type`, `url`, optional `version`) |
| `identifiers` | `{id, value, url}` for wikidata / re3data / fairsharing |
| `langs` | `{id, name}` e.g. `EN` / `English` |
| `tags` | Keywords (`government`, `has_api`, …) |
| `topics` | `{type, id, name}` — EU data themes or ISO 19115 |
| `api` / `api_status` | Set together when an API exists |
| `owner.link` | Owning organization URL |
| `content_types` | e.g. `dataset`, `map_layer` |
| `rights` | license / ToS / privacy URLs |

## Owner

Canonical `owner.type` values (see `data/reference/owner_types.yaml`):

`Local government`, `Central government`, `Regional government`, `Federal government`, `Academy`, `Business`, `Civil society`, `International`, `Community`, `Other`.

Synonyms such as `University` → `Academy` are accepted by quality checks but new entries should use canonical values.

`owner.location.level` is a geographic level code (commonly `20` national, `30` subnational). Regional/local owners should use level `30` and a matching subregion directory.

## Coverage location

```yaml
coverage:
- location:
    country:
      id: US
      name: United States
    level: 20
    macroregion:
      id: '021'
      name: Northern America
    subregion:
      id: US-CA
      name: California
```

`level` is numeric. Subregion `id` uses ISO 3166-2 style when the catalog is not national.

## Endpoints

```yaml
endpoints:
- type: ckan
  url: https://catalog.data.faa.gov/api/3
  version: '3'
- type: dcatus11
  url: https://catalog.data.faa.gov/data.json
```

Common `type` values include `ckan`, `ckan:package-search`, `dcatap201`, `dcatus11`, `geonetwork:csw`, `oaipmh`, `socrata:opendata`, `stac`, `sparql`. Use the types already present for the same `software.id`.

## Example (verified entity)

```yaml
access_mode:
- open
api: true
api_status: active
catalog_type: Open data portal
id: catalogdatafaagov
link: https://catalog.data.faa.gov
name: Federal Aviation Administration Open Data Portal
owner:
  name: Federal Aviation Administration
  type: Central government
  location:
    country:
      id: US
      name: United States
    level: 20
software:
  id: ckan
  name: CKAN
status: active
uid: cdi00005263
```

Full file: `data/entities/US/Federal/opendata/catalogdatafaagov.yaml`.

## Software records

Software YAML under `data/software/{category}/{id}.yaml` includes `id`, `name`, `category`, `subtype`, API/metadata support flags, and documentation URLs. See [software-taxonomy.md](software-taxonomy.md).
