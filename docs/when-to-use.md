# When to use this registry

Use dataportals-registry when you need **stable metadata about data catalogs** — not the datasets inside those catalogs.

## Use this repository for

- Finding portals, geoportals, repositories, metadata catalogs, and related infrastructure by country, type, or software
- Joining catalogs to Wikidata, re3data, FAIRsharing, and similar identifiers
- Landscape analysis (how many CKAN sites in France, which geoportals expose CSW, …)
- Training or evaluating catalog-metadata classifiers, with the biases in [DATASHEET.md](https://github.com/datenoio/dataportals-registry/blob/main/DATASHEET.md)
- Feeding a downstream search index that crawls `endpoints` listed on each record

## Do not use this repository for

- Querying dataset records inside a portal (CKAN packages, Dataverse studies, STAC items)
- Production search APIs or MCP servers — this repository is reference data only. Search: [dateno-api](https://github.com/datenoio/dateno-api). Catalog browse UI: [dataportals-web](https://github.com/datenoio/dataportals-web). Harvesting catalog contents: [reaper](https://github.com/datenoio/reaper).
- Real-time uptime SLAs — liveness probes are report-only
- Exhaustive coverage of every country; the United States is heavily over-represented

## Quick decision guide

1. “Is there an open data portal for city X / country Y?”  
   Use this registry: filter `catalog_type` and `coverage`. If nothing matches, follow [discovery.md](discovery.md) (search engines: [discovery-search-tools.md](discovery-search-tools.md); platforms: [opendata](discovery-opendata.md), [geoportals](discovery-geoportals.md), [scientific](discovery-scientific.md), [metadata](discovery-metadata.md), [indicators](discovery-indicators.md), [other types](discovery-other.md)) before adding a record.

2. “Which catalogs run CKAN / GeoNetwork / Dataverse?”  
   Use this registry: filter `software.id` in DuckDB / Parquet ([query-examples.md](query-examples.md)).

3. “Give me the API URL to harvest.”  
   Use this registry: read `endpoints[]` and `api_status`. Then harvest the remote catalog ([harvest.md](harvest.md)). Scientific IRs: [harvest-scientific.md](harvest-scientific.md). Geo: [harvest-geoportals.md](harvest-geoportals.md). Indicators: [harvest-indicators.md](harvest-indicators.md).

4. “Search for a specific dataset titled …”  
   Out of scope for this repo’s data. Harvest the catalog ([harvest.md](harvest.md)) or use a downstream search index.

5. “Is this URL currently up?”  
   Optional signal only: `dataquality/liveness_report.jsonl`. Status in YAML is curated, not a live probe.

## Related projects

| Need | Where |
|------|--------|
| Catalog metadata (this repo) | YAML + DuckDB/Parquet exports |
| Production catalog/dataset search API | [dateno-api](https://github.com/datenoio/dateno-api) |
| Harvest datasets listed in catalogs | [reaper](https://github.com/datenoio/reaper) (production); recipes: [harvest.md](harvest.md) |
| Public catalog website | [dataportals-web](https://github.com/datenoio/dataportals-web) |
| Country and organization reference data | [internacia-db](https://github.com/datenoio/internacia-db) |
| Unified file reading/writing | [iterabledata](https://github.com/datenoio/iterabledata) |
