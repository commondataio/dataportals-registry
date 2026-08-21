# Discovering metadata catalogs

How to find **metadata catalog** installations (`catalog_type: Metadata catalog`). Search-engine syntax: [discovery-search-tools.md](discovery-search-tools.md). Overview: [discovery.md](discovery.md).

These sites publish **catalog/dataset metadata** (often RDF / DCAT or SDMX structural metadata), not a full open-data CMS and not a research-data file store. If the public product is CKAN, Dataverse, or GeoNetwork, use those `software.id` values and types instead.

## FAIR Data Point (`fairdatapoint`)

Open-source REST API and web client for FAIR metadata as RDF (DCAT + the [FAIR Data Point specification](https://specs.fairdatapoint.org/)). Docs: [docs.fairdatapoint.org](https://docs.fairdatapoint.org). Index of public points: [home.fairdatapoint.org](https://home.fairdatapoint.org).

**Signals:** HTML client `fdp-client`; JSON-LD or Turtle DCAT at the catalog root; paths such as `/catalog`, `/dataset`, `/swagger-ui`; hostname often `fdp.` or path `/fairdatapoint`.

**Confirm:** GET the catalog URL with `Accept: text/turtle` or `application/ld+json` and check for DCAT `Catalog` / FAIR Data Point metadata. The HTML UI alone is not enough if it is only a marketing page.

| Tool | Query |
|------|-------|
| Google | `"FAIR Data Point" OR "fairdatapoint" (catalog OR DCAT) -site:github.com` |
| Google | `inurl:fairdatapoint OR intitle:"FAIR Data Point"` |
| Censys | `web.endpoints.http.body: "fairdatapoint"` |
| Censys | `web.endpoints.http.body: "fdp-client"` |
| crt.sh | `fdp.%` |

Start from the public index, then fill gaps with search. Skip points that require login for any catalog listing. Register the FDP root, not a single dataset IRI. Do not duplicate the index (`home.fairdatapoint.org`) if it is already in the registry.

## Aristotle Metadata Registry (`aristotlemdr`)

Open-source metadata registry for models and controlled vocabularies. Site: [aristotlemetadata.com](https://www.aristotlemetadata.com) (product branding varies by deployment).

**Signals:** Aristotle MDR; `/api/v4/` or browsable registry of object classes / data elements; stewardship workflows.

**Confirm:** GET the public registry home and an API listing when unauthenticated access exists. Skip staff-only stewardship tools.

| Tool | Query |
|------|-------|
| Google | `"Aristotle" ("Metadata Registry" OR MDR) (vocabulary OR "data element") -site:github.com` |
| Censys | `web.endpoints.http.body: "Aristotle"` |

## Fusion Metadata Registry (`fusionregistry`)

SDMX-native structural metadata registry (code lists, DSDs, REST). Often branded Fusion Registry / FMR.

**Signals:** Fusion Registry; SDMX REST (`/sdmx/v2/` or `/ws/public/sdmxapi/`); structural metadata browser.

**Confirm:** GET the public registry or SDMX REST catalog. Do not confuse with a PxWeb/.Stat **data** portal — those stay `pxweb` / `statsuite` under indicators.

| Tool | Query |
|------|-------|
| Google | `"Fusion Registry" OR "Fusion Metadata Registry" SDMX -site:github.com` |
| Google | `inurl:/sdmx/v2/ "Fusion"` |
| Censys | `web.endpoints.http.body: "Fusion Registry"` |

## Metadata Browser (`mwmb`)

MetadataWorks catalog UI for datasets, standards, and terminologies. Site: [metadataworks.ai](https://metadataworks.ai/metadata-browser).

**Signals:** Metadata Browser / MetadataWorks; federated metadata search.

**Confirm:** GET the public browser. One record per public deployment.

| Tool | Query |
|------|-------|
| Google | `"Metadata Browser" MetadataWorks (catalog OR terminology)` |
| Censys | `web.endpoints.http.body: "MetadataWorks"` |

## Related

- [discovery.md](discovery.md)
- [discovery-search-tools.md](discovery-search-tools.md)
- [discovery-opendata.md](discovery-opendata.md) (DCAT portals that are not FDP)
- [discovery-indicators.md](discovery-indicators.md) (Fusion Registry vs PxWeb/.Stat)
- [harvest-metadata.md](harvest-metadata.md)
- [discovery-scientific.md](discovery-scientific.md)
- [software-taxonomy.md](software-taxonomy.md)
- [catalog-types.md](catalog-types.md)
