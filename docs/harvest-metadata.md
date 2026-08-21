# Harvesting metadata catalogs

Metadata catalogs publish **DCAT/RDF catalogs, data-element registries, or SDMX structure**. Harvest catalog and dataset **metadata**, not file bytes and not every code list as a dataset unless that is the product.

Overview: [harvest.md](harvest.md). Finding installations: [discovery-metadata.md](discovery-metadata.md). GET only. Stop on `401`/`403`.

## What to keep

| Keep | Drop |
|------|------|
| FAIR Data Point **Catalog** and child **Dataset** IRIs | Binary distributions as extra datasets (they are files) |
| Aristotle **stewarded** public objects the user asked for (datasets vs data elements) | Staff-only workflow items |
| Fusion Registry **dataflows** (dataset analog) | Codelists and DSDs unless harvesting structure |
| Metadata Browser public **dataset** records | Terminology-only hits if you want datasets |

If the live product is CKAN, GeoNetwork, or Dataverse, use those harvest guides instead of this page.

## FAIR Data Point (`fairdatapoint`)

FDP is RDF DCAT. Start at the catalog root with RDF Accept headers.

```text
GET https://host/
Accept: text/turtle
```

Also try `application/ld+json`. Follow `dcat:dataset` (and nested `dcat:catalog`) IRIs. Harvest each Dataset resource once. **Drop** `dcat:Distribution` as a separate dataset (keep URLs as files on the parent). Cap recursion through child FDPs so you do not crawl the whole federation.

HTML `fdp-client` alone is not a harvest. Swagger `/swagger-ui` documents the API — use it to find catalog/dataset paths. Register/harvest the FDP **root**, not a single dataset IRI as the catalog.

Docs: [docs.fairdatapoint.org](https://docs.fairdatapoint.org). Public index: [home.fairdatapoint.org](https://home.fairdatapoint.org) (do not re-harvest the index as if it were every child FDP).

## Aristotle MDR (`aristotlemdr`)

```text
GET https://host/api/v4/
GET https://host/api/v4/metadata/
```

This is a **metadata registry** (object classes, data elements, value domains). Those are not research-data files.

- If the user wants **datasets**: keep Aristotle types that represent a dataset/distribution (names vary by stewardship model) and skip data-element noise.
- If the user wants **metadata objects**: page `/api/v4/metadata/` and record native ids — still do not write them into this registry’s YAML.

Skip login-only stewardship UIs.

## Fusion Registry (`fusionregistry`)

SDMX structural metadata.

```text
GET https://host/ws/public/sdmxapi/rest
GET https://host/ws/rest
```

List **dataflows** as the harvest grain for “datasets”. Harvest DSDs/codelists only when the job is a structure crawl. Do not confuse this with PxWeb/.Stat **observation** APIs ([harvest-indicators.md](harvest-indicators.md)).

## Metadata Browser (`mwmb`)

Public MetadataWorks UI. There may be no stable open list API. Harvest the public dataset/standard listing if a JSON/search endpoint exists in `endpoints[]`. Skip terminology-only pages when the user asked for datasets. One deployment = one catalog harvest scope.

## DCAT without an FDP

Many open-data sites expose `/catalog.xml`, `/data.json`, or DCAT-AP. That harvest belongs with [harvest-opendata.md](harvest-opendata.md) (`dcat:Dataset` only). Protocol details: [harvest-protocols.md](harvest-protocols.md#dcat-and-datajson). Use this page when `software.id` is `fairdatapoint`, `aristotlemdr`, `fusionregistry`, or `mwmb`.

## Related

- [harvest.md](harvest.md)
- [harvest-opendata.md](harvest-opendata.md)
- [harvest-indicators.md](harvest-indicators.md)
- [harvest-protocols.md](harvest-protocols.md)
- [harvest-incremental.md](harvest-incremental.md)
- [harvest-identifiers.md](harvest-identifiers.md)
- [harvest-output.md](harvest-output.md)
- [discovery-metadata.md](discovery-metadata.md)
- [apidetect.md](apidetect.md)
- [agents/harvest.md](agents/harvest.md)
