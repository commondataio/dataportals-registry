# Harvest output and empty results

After you apply a [type](harvest.md) or [protocol](harvest-protocols.md) filter, emit **one record per kept dataset** plus a short skip report. Identifier rules: [harvest-identifiers.md](harvest-identifiers.md). Incremental checkpoints: [harvest-incremental.md](harvest-incremental.md).

This page is an **agent recipe**. It is not the production harvest-record schema used by [reaper](https://github.com/datenoio/reaper). Downstream harvesters MAY use the shape below; they MUST NOT treat it as a runtime contract of this registry.

Do not write this JSON into `data/entities/`. Store it in your index or harvest DB.

## Record shape

```json
{
  "catalog_uid": "cdi00001616",
  "catalog_id": "catalogdatagov",
  "catalog_link": "https://catalog.data.gov",
  "software_id": "ckan",
  "native_id": "00000000-0000-0000-0000-000000000000",
  "persistent_id": "10.example/abc",
  "landing_url": "https://catalog.data.gov/dataset/example",
  "title": "Example dataset",
  "type_filter": "package_search fq=dataset_type:dataset",
  "modified": "2026-08-01T00:00:00Z"
}
```

`catalog_uid` is this registry’s catalog id. `native_id` is required. `persistent_id` is DOI/handle/ARK when present. Omit empty fields rather than inventing values.

## JSON Schema (recipe)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://datenoio.github.io/dataportals-registry/harvest-record.schema.json",
  "title": "Harvest dataset record (agent recipe)",
  "type": "object",
  "additionalProperties": true,
  "required": ["catalog_uid", "catalog_id", "software_id", "native_id"],
  "properties": {
    "catalog_uid": { "type": "string", "pattern": "^(cdi|temp)[0-9]{8}$" },
    "catalog_id": { "type": "string" },
    "catalog_link": { "type": "string", "format": "uri" },
    "software_id": { "type": "string" },
    "native_id": { "type": "string", "minLength": 1 },
    "persistent_id": { "type": "string" },
    "landing_url": { "type": "string", "format": "uri" },
    "title": { "type": "string" },
    "type_filter": { "type": "string" },
    "modified": { "type": "string" }
  }
}
```

This schema is documentation-only. It is not published under `data/schemes/` and is not validated in CI.

## Skip report

Count rejects so a zero-dataset harvest is diagnosable:

| Reason | Typical cause |
|--------|----------------|
| publication | Unfiltered IR search |
| file_or_resource | CKAN resource / Dataverse file |
| service_or_tile | CSW service, WMS GetMap, STAC item |
| folder | PxWeb `type: l`, THREDDS directory |
| occurrence | IPT/Symbiota/ALA row-level |
| observation | SDMX/WDI/GHO cube cells; World Bank country-indicator queries |
| login | `401` / `403` |
| aggregator_dup | Idra/OpenAIRE copy of a source already harvested |

## Empty harvest checklist

1. Confirm `software.id` from exports. If `custom`, do not apply a CKAN/DSpace filter.
2. GET **one** unfiltered sample. If the sample is publications or tiles, the filter is missing — not “no data”.
3. `ListSets` / facets / `fq` vocabularies differ per campus (Forschungsdaten, numeric WEKO3 types).
4. `401` / `403`: stop. Record `login` skips; do not guess keys.
5. Wrong grain: STAC items vs collections, CSW service vs dataset, CKAN resources vs packages ([harvest-protocols.md](harvest-protocols.md)).
6. Viewer with no GetCapabilities: [harvest-viewers.md](harvest-viewers.md) — do not scrape tiles.

A catalog can be in-scope for this registry and still have **zero public datasets** after a correct filter. Report that, with the skip counts.

## Related

- [harvest.md](harvest.md)
- [harvest-identifiers.md](harvest-identifiers.md)
- [harvest-incremental.md](harvest-incremental.md)
- [harvest-protocols.md](harvest-protocols.md)
- [agents/harvest.md](agents/harvest.md)
