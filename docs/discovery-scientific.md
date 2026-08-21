# Discovering scientific data repositories

How to find **scientific data repository** installations (`catalog_type: Scientific data repository`). Search-engine syntax: [discovery-search-tools.md](discovery-search-tools.md). Cross-check [re3data](https://www.re3data.org/) and the Dataverse installations JSON before adding a well-known platform — many are already registered.

Also in `data/software/scientific/`: OPUS, CONTENTdm, Omeka S, Fedora Repository, PHAIDRA, and Esploro. Do not add dataset-level records (a single Dataverse dataset, a Zenodo deposition, a STAC item).

## Dataverse (`dataverse`)

Installations JSON: [dataverse-installations data.json](https://iqss.github.io/dataverse-installations/data/data.json). Branding is often ``{Org} Dataverse``.

**Confirm:** `https://host/api/info/version` and/or `/api/search?q=*&type=dataset`. OAI-PMH: `/oai?verb=Identify`.

| Tool | Query |
|------|-------|
| Google | `"Dataverse" "Harvard" OR "IQSS" -site:harvard.edu` (then drop Harvard to find others) |
| Google | `inurl:/dataverse.xhtml OR inurl:/dataverse/` |
| Google | `"API" "info/version" dataverse` |
| Censys | `web.endpoints.http.html_title: "Dataverse"` |
| Censys | `web.endpoints.http.body: "dataverse"` |
| Shodan | `http.title:"Dataverse"` |
| crt.sh | `dataverse.%` |

**False positives:** guides.dataverse.org, the Harvard demo, individual dataset landing pages (`/dataset.xhtml?persistentId=`). Register the installation root.

## DSpace (`dspace`) and DSpace-CRIS (`dspacecris`)

Institutional repositories. DSpace 7+ API at `/server/api`. Older 6.x: `/rest/items`, XMLUI/JSPUI. OAI-PMH: `/oai/request?verb=Identify`.

Set `dspacecris` only when the UI is DSpace-CRIS (researcher profiles, CRIS entities), not vanilla DSpace.

| Tool | Query |
|------|-------|
| Google | `"DSpace" (repository OR "handle") site:.edu` |
| Google | `inurl:/xmlui OR inurl:/jspui "DSpace"` |
| Google | `inurl:/server/api/discover/search/objects` |
| Google | `"DSpace-CRIS" OR "dspace-cris"` |
| Censys | `web.endpoints.http.body: "DSpace"` |
| Shodan | `http.html:"generator\" content=\"DSpace"` |

ROAR ([roar.eprints.org](http://roar.eprints.org)) lists many DSpace hosts; still duplicate-check this registry.

## Invenio and InvenioRDM (`invenio`, `inveniordm`)

Zenodo-like research data repositories. **Confirm:** `/api/records?size=1` JSON. OAI-PMH often `/oai2d`.

Use `inveniordm` for RDM branding / InvenioRDM; `invenio` for classic Invenio.

| Tool | Query |
|------|-------|
| Google | `"InvenioRDM" OR "invenio-rdm" repository` |
| Google | `inurl:/api/records "invenio"` |
| Censys | `web.endpoints.http.body: "invenio"` |

Skip zenodo.org itself if already registered; look for **institutional** RDM instances.

## EPrints (`eprints`)

**Confirm:** `/eprint` URLs, “Powered by EPrints”, OAI-PMH `/cgi/oai2?verb=Identify`. Directory: [ROAR](http://roar.eprints.org).

| Tool | Query |
|------|-------|
| Google | `"Powered by EPrints" -site:eprints.org` |
| Google | `inurl:/cgi/oai2 eprints` |
| Censys | `web.endpoints.http.body: "EPrints"` |

## Hyrax / Samvera (`hyrax`)

Rails institutional repo. **Confirm:** `/catalog.json` or Blacklight `/catalog`. Branding “Hyrax”, “Samvera”, “Nurax”.

Hyrax (and Islandora, PHAIDRA) often sit on **Fedora Repository** as the preservation backend. Set `software.id` from the **public catalog UI**, not the storage layer. Use `fedora` only when Fedora’s REST/LDP API is the public product — see [Fedora](#fedora-repository-fedora).

| Tool | Query |
|------|-------|
| Google | `"Hyrax" (repository OR "research data") site:.edu` |
| Google | `"Powered by Hyrax" OR "Samvera"` |
| Censys | `web.endpoints.http.body: "hyrax"` |

## GBIF IPT (`ipt`)

Integrated Publishing Toolkit for biodiversity data. List: [gbif.org/ipt](https://www.gbif.org/ipt).

**Confirm:** `/rss.do`, `/inventory/dataset`, or the IPT homepage with installation name.

| Tool | Query |
|------|-------|
| Google | `"Integrated Publishing Toolkit" IPT GBIF` |
| Google | `inurl:/ipt "GBIF"` |
| Censys | `web.endpoints.http.body: "Integrated Publishing Toolkit"` |

Prefer GBIF’s official installation list, then fill gaps with search.

## Figshare (`figshare`)

Institutional Figshare (not figshare.com itself). **Signals:** `{org}.figshare.com` or a custom domain with Figshare UI.

| Tool | Query |
|------|-------|
| Google | `site:figshare.com -site:figshare.com/articles "{university}"` |
| Google | `"figshare" "institutional repository"` |
| crt.sh | `%.figshare.com` |

Register the **institution** instance, not individual article URLs.

## THREDDS (`thredds`)

Scientific data servers (often climate/ocean). **Confirm:** `/thredds/catalog.html` or `/thredds/catalog.xml`.

| Tool | Query |
|------|-------|
| Google | `inurl:/thredds/catalog.html` |
| Google | `"THREDDS Data Server" catalog` |
| Censys | `web.endpoints.http.body: "THREDDS"` |
| Shodan | `http.html:"THREDDS Data Server"` |

## ERDDAP (`erddap`)

NOAA-style tabular/gridded data server. **Confirm:** `/erddap/index.html` or `/erddap/info/index.json`.

| Tool | Query |
|------|-------|
| Google | `inurl:/erddap "ERDDAP"` |
| Censys | `web.endpoints.http.body: "ERDDAP"` |

## MyCoRe (`mycore`)

German institutional repos. List: [mycore.de applications](https://www.mycore.de/site/applications/list/).

| Tool | Query |
|------|-------|
| Google | `"MyCoRe" (repositorium OR repository) site:.de` |
| Censys | `web.endpoints.http.body: "MyCoRe"` |

## OPUS (`opus`)

German (and some Austrian) institutional repositories. Official instance list: [KOBV OPUS 4 references](https://www.kobv.de/entwicklung/software/opus-4/referenzen/). Many hosts share `opus4.kobv.de` or `opus.bsz-bw.de` with a per-institution path.

Prefer instances that publish research data (`doc-type:ResearchData` in OAI-PMH `ListSets`, or a Forschungsdaten collection), not thesis-only publication servers.

**Confirm:** `{base}/oai?verb=Identify` and `{base}/oai?verb=ListSets` (replace `{base}` with the repository root). Search UI often under `/solrsearch/` or `/home`.

| Tool | Query |
|------|-------|
| Google | `"OPUS 4" (Forschungsdaten OR "research data" OR Repositorium) site:.de` |
| Google | `inurl:opus4.kobv.de OR inurl:opus.bsz-bw.de` |
| Censys | `web.endpoints.http.body: "OPUS 4"` |

Skip intranet-only thesis portals (Hochschulnetz / account required). Register the repository root, not a single document frontdoor.

## CONTENTdm (`contentdm`)

OCLC hosted digital collections. Hosts are often `*.contentdm.oclc.org`. Docs: [OCLC CONTENTdm](https://help.oclc.org/Metadata_Services/CONTENTdm).

Register a CONTENTdm site only when it publishes **research datasets, statistical series, or a data collection**, not a photo/manuscript exhibit with no dataset catalog. Stats NZ Digital Library and climate-data collections are in scope; typical campus image libraries are not.

**Confirm:** `https://host/digital/api/collections` and/or `/oai/oai.php?verb=Identify`. Website API: `/digital/bl/dmwebservices/index.php?q=dmGetCollectionList/json`.

| Tool | Query |
|------|-------|
| Google | `site:contentdm.oclc.org (dataset OR "research data" OR statistics OR climate)` |
| Google | `"CONTENTdm" ("digital collections" OR dataset) -site:oclc.org` |
| Censys | `web.names: "contentdm.oclc.org"` |
| crt.sh | `%.contentdm.oclc.org` |

Register the collection or library root that lists datasets, not a single item URL.

## Omeka S (`omekas`)

Cultural-heritage and research publishing platform. JSON-LD REST API; modules include Linked Data Sets and OAI-PMH Repository. Docs: [omeka.org/s](https://omeka.org/s).

Set `omekas` when the public product is a **dataset catalog** (schema.org DataCatalog, SPARQL, or a datasets/items API), not an exhibit-only museum site.

**Confirm:** `https://host/api` or `/api/items` returns JSON-LD. Optional: `/.well-known/datacatalog`, `/oai`.

| Tool | Query |
|------|-------|
| Google | `"Omeka S" (dataset OR datacatalog OR SPARQL OR "linked open data") -site:omeka.org` |
| Google | `inurl:/api/items omeka` |
| Censys | `web.endpoints.http.body: "Omeka S"` |
| Censys | `web.endpoints.http.body: "o:item"` |

Omeka Classic is a different product — do not label it `omekas` without the S API (`/api` JSON-LD).

## Fedora Repository (`fedora`)

Preservation repository with a Linked Data Platform REST API. Public catalogs usually wrap it with Hyrax, Islandora, or PHAIDRA.

**Confirm:** `GET https://host/fcrepo/rest` or `/rest` with Fedora version headers or RDF. Use `fedora` only if that API (or a thin Fedora HTML) is what users treat as the catalog.

| Tool | Query |
|------|-------|
| Google | `"Fedora Repository" OR inurl:/fcrepo/rest (research OR dataset)` |
| Google | `"fcrepo" "research data" -site:github.com` |
| Censys | `web.endpoints.http.body: "Fedora Repository"` |

Prefer `hyrax`, `islandora`, or `phaidra` when those UIs are the public product on the same host.

## PHAIDRA (`phaidra`)

University of Vienna institutional repository (and clones). List and docs: [phaidra.org](https://phaidra.org). OAI-PMH and REST search are common.

**Confirm:** `/api/oai?verb=Identify` or `/api/search/select`. Hostnames often `phaidra.{university}`.

| Tool | Query |
|------|-------|
| Google | `"PHAIDRA" (repository OR Forschungsdaten OR "research data") -site:univie.ac.at` |
| Google | `inurl:phaidra (oai OR repository)` |
| Censys | `web.endpoints.http.body: "PHAIDRA"` |

## Elsevier Pure (`pure`)

CRIS / research portal. Showcase: [Pure in action](https://www.elsevier.com/solutions/pure/pure-in-action). Portal paths often `/portal` or `/en/`.

| Tool | Query |
|------|-------|
| Google | `"Pure" "research portal" Elsevier OR "pure.elsevier"` |
| Google | `inurl:/portal/en/ persons datasets` |
| Censys | `web.endpoints.http.body: "Elsevier Pure"` |

Skip marketing pages. The public research **portal** is the catalog, not the admin Pure backend.

## Esploro (`esploro`)

Clarivate / Ex Libris research information management and repository. Vendor: [Esploro](https://exlibrisgroup.com/products/esploro-research-services-platform/). Hosts often `*.esploro.exlibrisgroup.com` or a campus custom domain with `/esploro` or research-outputs views.

Register the institutional research portal that lists datasets, not a single output URL. Skip Ex Libris marketing pages.

| Tool | Query |
|------|-------|
| Google | `"Esploro" ("research portal" OR "research outputs" OR datasets) -site:exlibrisgroup.com` |
| Google | `site:esploro.exlibrisgroup.com` |
| Censys | `web.names: "esploro.exlibrisgroup.com"` |
| crt.sh | `%.esploro.exlibrisgroup.com` |

## Generic scientific probes

On a **named** university or lab host:

```text
/oai/request?verb=Identify
/oai?verb=Identify
/api/info/version
/api/records?size=1
/server/api
/digital/api/collections
/api/items
```

Google: ``"research data repository" {university}``, ``"repositorio de datos" {universidad}``, ``Forschungsdaten {hochschule}``. re3data.org advanced search by country and software is usually faster than Google for this class.

## Related

- [discovery.md](discovery.md)
- [discovery-search-tools.md](discovery-search-tools.md)
- [discovery-metadata.md](discovery-metadata.md)
- [re3data.md](re3data.md)
- [software-taxonomy.md](software-taxonomy.md)
