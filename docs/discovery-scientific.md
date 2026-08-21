# Discovering scientific data repositories

How to find **scientific data repository** installations (`catalog_type: Scientific data repository`). Search-engine syntax: [discovery-search-tools.md](discovery-search-tools.md). Cross-check [re3data](https://www.re3data.org/) and the Dataverse installations JSON before adding a well-known platform — many are already registered.

Do not add dataset-level records (a single Dataverse dataset, a Zenodo deposition, a STAC item).

## Dataverse (`dataverse`)

Installations JSON: [dataverse-installations data.json](https://iqss.github.io/dataverse-installations/data/data.json). Branding is often “{Org} Dataverse”.

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

## Elsevier Pure (`pure`)

CRIS / research portal. Showcase: [Pure in action](https://www.elsevier.com/solutions/pure/pure-in-action). Portal paths often `/portal` or `/en/`.

| Tool | Query |
|------|-------|
| Google | `"Pure" "research portal" Elsevier OR "pure.elsevier"` |
| Google | `inurl:/portal/en/ persons datasets` |
| Censys | `web.endpoints.http.body: "Elsevier Pure"` |

Skip marketing pages. The public research **portal** is the catalog, not the admin Pure backend.

## Generic scientific probes

On a **named** university or lab host:

```text
/oai/request?verb=Identify
/oai?verb=Identify
/api/info/version
/api/records?size=1
/server/api
```

Google: `"research data repository" {university}`, `"repositorio de datos" {universidad}`, `Forschungsdaten {hochschule}`. re3data.org advanced search by country and software is usually faster than Google for this class.

## Related

- [discovery.md](discovery.md)
- [discovery-search-tools.md](discovery-search-tools.md)
- [re3data.md](re3data.md)
- [software-taxonomy.md](software-taxonomy.md)
