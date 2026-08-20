# Vocabularies

Controlled values for catalog YAML. Source files live under `data/reference/`. Catalog types: [catalog-types.md](catalog-types.md). Software IDs: [software-taxonomy.md](software-taxonomy.md). Owner types: [data-model.md](data-model.md).

## Geographic levels

`coverage[].location.level` and `owner.location.level` are numeric. **Higher numbers are more local.** New records should use `20` (national) or `30` (first-level subnational) unless a finer level is clearly justified.

| Level | Meaning | Typical path |
|------:|---------|--------------|
| `0` | Unknown / unspecified | Avoid on new records |
| `10` | Supranational, global, or multi-country | `World/`, `EU/`, `Africa/` |
| `20` | National | `{CC}/Federal/` |
| `30` | First-level subnational (state, region, province) | `{CC}/{CC-XX}/` |
| `40` | County / district | Still under the first-level ISO folder |
| `50` | City | Same |
| `60` | Commune / local authority | Same |

Regional and local government owners must have `owner.location.level` of **30 or higher** and a matching subregion directory (`US-CA/`, `GB-SCT/`, …), not `Federal/`.

Country folders use ISO 3166-1 alpha-2. Special roots (`World`, `EU`, `Africa`, `ASEAN`, …) are listed in `PATH_COUNTRY_ALLOWLIST` in `scripts/constants.py`. Subregion ids use ISO 3166-2 style. Macroregion ids are UN M49 numeric codes (`021` Northern America).

## Identifiers

`identifiers[]` is a list of `{id, value, url}`. Prefer these `id` values on new records:

| `id` | Registry | Example `value` |
|------|----------|-----------------|
| `wikidata` | Wikidata | `Q5227102` |
| `re3data` | re3data.org | `r3d100010078` |
| `fairsharing` | FAIRsharing | `FAIRsharing.6069e1` |
| `opendoar` | OpenDOAR | numeric / slug as published |
| `roar` | ROAR | as published |
| `datacite` | DataCite | as published |
| `ror` | ROR | `https://ror.org/…` or ROR id |
| `doi` | DOI | `10.…` |

Also present in historical records (do not add for new catalogs unless there is no better key): `dataportals.org`, `url`, `domain`, `github`, `esri`, `arcgis`. Each entry needs both `id` and `value` (`INCOMPLETE_IDENTIFIER` otherwise).

## Endpoint types

`endpoints[].type` names the protocol or API family. Prefer types already used for the same `software.id`. Common values:

| Type | Typical software |
|------|------------------|
| `ckan`, `ckan:package-search`, `ckan:package-list`, `ckan:status-show` | CKAN |
| `dcatus11`, `dcatap201`, `dcatap21`, `dcat:jsonld` | DCAT portals |
| `socrata:opendata` | Socrata |
| `opendatasoft`, `opendatasoftapi` | OpenDataSoft |
| `geonetwork:csw`, `csw202`, `csw300` | GeoNetwork / CSW |
| `oaipmh`, `oaipmh20` | OAI-PMH |
| `stac` | STAC |
| `sparql` | SPARQL |
| `arcgis:rest:services`, `arcgis:rest:info` | ArcGIS Server / Hub |
| `dataverseapi` | Dataverse |
| `ogc:features`, `wms`, `wfs` | OGC services |

The observed inventory (150+ values, including vendor-specific types) is `data/reference/endpoint_types.yaml`. That file is generated from current YAML, not a closed allow-list.

## Topics

`topics[]` entries are `{type, id, name}`.

| `type` | Vocabulary | Source |
|--------|------------|--------|
| EU data theme | `AGRI`, `ECON`, `EDUC`, `ENER`, `ENVI`, `GOVE`, `HEAL`, `INTR`, `JUST`, `REGI`, `SOCI`, `TECH`, `TRAN` | `data/reference/data_themes.yaml` |
| ISO 19115 | Biota, Boundaries, Elevation, Oceans, … | `data/reference/iso19115.yaml` |

Open-data portals usually use EU data themes. Geoportals may use ISO 19115. Incomplete `{type,id,name}` triples are `TOPIC_INCOMPLETE`.

## Languages

`langs[]` is `{id, name}` with ISO 639-1 style codes (`EN` / `English`). Codes and names: `data/reference/langs.csv`.

## Other reference files

| File | Used for |
|------|----------|
| `catalog_types.yaml` | `catalog_type` |
| `software_ids.yaml` | `software.id` |
| `status.yaml` | `status` |
| `access_modes.yaml` | `access_mode` (prefer `open` / `restricted`) |
| `owner_types.yaml` | canonical `owner.type` plus synonym map |
| `countries.csv` | country id/name |
| `macroregion_countries.csv` | UN M49 membership |
