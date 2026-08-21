# Discovering metadata catalogs

How to find **metadata catalog** installations (`catalog_type: Metadata catalog`). Search-engine syntax: [discovery-search-tools.md](discovery-search-tools.md). Overview: [discovery.md](discovery.md).

These sites publish **catalog/dataset metadata** (often RDF / DCAT), not a full open-data CMS and not a research-data file store. If the public product is CKAN, Dataverse, or GeoNetwork, use those `software.id` values and types instead.

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

## Related

- [discovery.md](discovery.md)
- [discovery-search-tools.md](discovery-search-tools.md)
- [discovery-opendata.md](discovery-opendata.md) (DCAT portals that are not FDP)
- [discovery-scientific.md](discovery-scientific.md)
- [software-taxonomy.md](software-taxonomy.md)
- [catalog-types.md](catalog-types.md)
