# Discovering scientific data repositories

How to find **scientific data repository** installations (`catalog_type: Scientific data repository`). Search-engine syntax: [discovery-search-tools.md](discovery-search-tools.md). Cross-check [re3data](https://www.re3data.org/) and the Dataverse installations JSON before adding a well-known platform — many are already registered.

Do not add dataset-level records (a single Dataverse dataset, a Zenodo deposition, a STAC item).

| Page | Use when |
|------|----------|
| This page | Institutional repositories and CRIS (Dataverse, DSpace, Invenio, EPrints, OPUS, RADAR, Yoda, …) |
| [Domain repositories](discovery-scientific-domain.md) | IPT, Symbiota, THREDDS, ERDDAP, Breedbase, Tripal, VEuPathDB, MassBank, ioChem-BD, ESGF, ALA, SciCat |

All `software.id` values: [software-index.md](software-index.md). Harvest filters: [harvest-scientific.md](harvest-scientific.md), [harvest-scientific-domain.md](harvest-scientific-domain.md).

## Dataverse (`dataverse`) {#dataverse}

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

## DSpace (`dspace`) {#dspace}

Institutional repositories. DSpace 7+ API at `/server/api`. Older 6.x: `/rest/items`, XMLUI/JSPUI. OAI-PMH: `/oai/request?verb=Identify`.

| Tool | Query |
|------|-------|
| Google | `"DSpace" (repository OR "handle") site:.edu` |
| Google | `inurl:/xmlui OR inurl:/jspui "DSpace"` |
| Google | `inurl:/server/api/discover/search/objects` |
| Censys | `web.endpoints.http.body: "DSpace"` |
| Shodan | `http.html:"generator\" content=\"DSpace"` |

ROAR ([roar.eprints.org](http://roar.eprints.org)) lists many DSpace hosts; still duplicate-check this registry. Harvest type filters: [harvest-scientific.md](harvest-scientific.md#dspace).

## DSpace-CRIS (`dspacecris`) {#dspacecris}

Set `dspacecris` only when the UI is DSpace-CRIS (researcher profiles, CRIS entities), not vanilla DSpace. Same OAI/REST probes as [DSpace](#dspace).

| Tool | Query |
|------|-------|
| Google | `"DSpace-CRIS" OR "dspace-cris"` |

## Invenio (`invenio`) {#invenio}

Classic Invenio (not RDM). **Confirm:** `/api/records?size=1` JSON and Invenio branding without InvenioRDM. OAI-PMH often `/oai2d`.

Use `inveniordm` when the product is InvenioRDM. Skip zenodo.org itself if already registered.

| Tool | Query |
|------|-------|
| Google | `inurl:/api/records "invenio" -InvenioRDM` |
| Censys | `web.endpoints.http.body: "invenio"` |

## InvenioRDM (`inveniordm`) {#inveniordm}

Zenodo-like research data repositories. **Confirm:** `/api/records?size=1` JSON plus InvenioRDM / RDM branding. OAI-PMH often `/oai2d`. Look for **institutional** RDM instances.

| Tool | Query |
|------|-------|
| Google | `"InvenioRDM" OR "invenio-rdm" repository` |
| Google | `inurl:/api/records "invenio"` |
| Censys | `web.endpoints.http.body: "invenio"` |

## EPrints (`eprints`) {#eprints}

**Confirm:** `/eprint` URLs, “Powered by EPrints”, OAI-PMH `/cgi/oai2?verb=Identify`. Directory: [ROAR](http://roar.eprints.org).

| Tool | Query |
|------|-------|
| Google | `"Powered by EPrints" -site:eprints.org` |
| Google | `inurl:/cgi/oai2 eprints` |
| Censys | `web.endpoints.http.body: "EPrints"` |

## Hyrax (`hyrax`) {#hyrax}

Rails institutional repo. **Confirm:** `/catalog.json` or Blacklight `/catalog`. Branding “Hyrax”, “Samvera”, “Nurax”.

Hyrax (and Islandora, PHAIDRA) often sit on **Fedora Repository** as the preservation backend. Set `software.id` from the **public catalog UI**, not the storage layer. Use `fedora` only when Fedora’s REST/LDP API is the public product — see [Fedora](#fedora).

| Tool | Query |
|------|-------|
| Google | `"Hyrax" (repository OR "research data") site:.edu` |
| Google | `"Powered by Hyrax" OR "Samvera"` |
| Censys | `web.endpoints.http.body: "hyrax"` |

## Figshare (`figshare`) {#figshare}

Institutional Figshare (not figshare.com itself). **Signals:** `{org}.figshare.com` or a custom domain with Figshare UI.

| Tool | Query |
|------|-------|
| Google | `site:figshare.com -site:figshare.com/articles "{university}"` |
| Google | `"figshare" "institutional repository"` |
| crt.sh | `%.figshare.com` |

Register the **institution** instance, not individual article URLs.

## MyCoRe (`mycore`) {#mycore}

German institutional repos. List: [mycore.de applications](https://www.mycore.de/site/applications/list/).

| Tool | Query |
|------|-------|
| Google | `"MyCoRe" (repositorium OR repository) site:.de` |
| Censys | `web.endpoints.http.body: "MyCoRe"` |

## OPUS (`opus`) {#opus}

German (and some Austrian) institutional repositories. Official instance list: [KOBV OPUS 4 references](https://www.kobv.de/entwicklung/software/opus-4/referenzen/). Many hosts share `opus4.kobv.de` or `opus.bsz-bw.de` with a per-institution path.

Prefer instances that publish research data (`doc-type:ResearchData` in OAI-PMH `ListSets`, or a Forschungsdaten collection), not thesis-only publication servers.

**Confirm:** `{base}/oai?verb=Identify` and `{base}/oai?verb=ListSets` (replace `{base}` with the repository root). Search UI often under `/solrsearch/` or `/home`.

| Tool | Query |
|------|-------|
| Google | `"OPUS 4" (Forschungsdaten OR "research data" OR Repositorium) site:.de` |
| Google | `inurl:opus4.kobv.de OR inurl:opus.bsz-bw.de` |
| Censys | `web.endpoints.http.body: "OPUS 4"` |

Skip intranet-only thesis portals (Hochschulnetz / account required). Register the repository root, not a single document frontdoor.

## RADAR (`radar`) {#radar}

FIZ Karlsruhe research data repositories (RADAR Cloud and RADAR Local). Official instance notes: [About RADAR](https://radar.products.fiz-karlsruhe.de/en/radarabout/ueber-radar). re3data lists them under software **RADAR**.

**Confirm:** `{base}/radar/en/home` or `{base}/radar/de/home` titled RADAR / the local brand, `{base}/oai/OAIHandler?verb=Identify`, and `{base}/radar/api/datasets` JSON with `totalHits` > 0. Prefer instances that already publish datasets. Use `software.id: radar`.

Register distinct **Local** hosts (KonDATA, WueData, Datathek, FoDaSi, OstData, RADAR-BB). Do **not** add `radar.kit.edu` or NFDI branded subdomains (`radar4chem.radar-service.eu`, `radar4culture.radar-service.eu`, `radar4memory.radar-service.eu`) — those share the already-registered Cloud catalog at `www.radar-service.eu`.

| Tool | Query |
|------|-------|
| Google | `"RADAR" (Forschungsdaten OR "research data") (repositorium OR repository) site:.de` |
| Google | `inurl:/radar/de/home OR inurl:/radar/en/home` |
| Censys | `web.endpoints.http.html_title: "Forschungsdaten"` plus RADAR body hints |
| re3data | software filter **RADAR** |

Skip the FIZ product/marketing site (`radar.products.fiz-karlsruhe.de`) and dataset landing pages (`/radar/de/dataset/{id}`).

## Yoda (`yoda`) {#yoda}

Dutch research-data management platform (Utrecht University / Yoda Consortium, often hosted by SURF) on iRODS. Docs: [utrechtuniversity.github.io/yoda](https://utrechtuniversity.github.io/yoda/). Public catalogs are institutional **publication landings** (DataCite-indexed datasets), not the authenticated vault.

**Confirm:** a public dataset landing or portal titled Yoda / Your Data for that institution. Hostnames often `public.yoda.*`, `portal.yoda.*`, or `*-landing.irods.surfsara.nl`. Use `software.id: yoda`. Skip login-only workspaces and SURF marketing pages.

| Tool | Query |
|------|-------|
| Google | `"Yoda" ("research data" OR "data publication") (university OR SURF) site:.nl` |
| Google | `inurl:yoda. "data" (portal OR public)` |
| Censys | `web.names: "yoda."` |

Register one catalog per **institution** public landing. Do not add every DataCite DOI.

## CONTENTdm (`contentdm`) {#contentdm}

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

## Omeka S (`omekas`) {#omekas}

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

## Fedora Repository (`fedora`) {#fedora}

Preservation repository with a Linked Data Platform REST API. Public catalogs usually wrap it with Hyrax, Islandora, or PHAIDRA.

**Confirm:** `GET https://host/fcrepo/rest` or `/rest` with Fedora version headers or RDF. Use `fedora` only if that API (or a thin Fedora HTML) is what users treat as the catalog.

| Tool | Query |
|------|-------|
| Google | `"Fedora Repository" OR inurl:/fcrepo/rest (research OR dataset)` |
| Google | `"fcrepo" "research data" -site:github.com` |
| Censys | `web.endpoints.http.body: "Fedora Repository"` |

Prefer `hyrax`, `islandora`, or `phaidra` when those UIs are the public product on the same host.

## PHAIDRA (`phaidra`) {#phaidra}

University of Vienna institutional repository (and clones). List and docs: [phaidra.org](https://phaidra.org). OAI-PMH and REST search are common.

**Confirm:** `/api/oai?verb=Identify` or `/api/search/select`. Hostnames often `phaidra.{university}`.

| Tool | Query |
|------|-------|
| Google | `"PHAIDRA" (repository OR Forschungsdaten OR "research data") -site:univie.ac.at` |
| Google | `inurl:phaidra (oai OR repository)` |
| Censys | `web.endpoints.http.body: "PHAIDRA"` |

## Elsevier Pure (`pure`) {#pure}

CRIS / research portal. Showcase: [Pure in action](https://www.elsevier.com/solutions/pure/pure-in-action). Portal paths often `/portal` or `/en/`.

| Tool | Query |
|------|-------|
| Google | `"Pure" "research portal" Elsevier OR "pure.elsevier"` |
| Google | `inurl:/portal/en/ persons datasets` |
| Censys | `web.endpoints.http.body: "Elsevier Pure"` |

Skip marketing pages. The public research **portal** is the catalog, not the admin Pure backend.

## Esploro (`esploro`) {#esploro}

Clarivate / Ex Libris research information management and repository. Vendor: [Esploro](https://exlibrisgroup.com/products/esploro-research-services-platform/). Hosts often `*.esploro.exlibrisgroup.com` or a campus custom domain with `/esploro` or research-outputs views.

Register the institutional research portal that lists datasets, not a single output URL. Skip Ex Libris marketing pages.

| Tool | Query |
|------|-------|
| Google | `"Esploro" ("research portal" OR "research outputs" OR datasets) -site:exlibrisgroup.com` |
| Google | `site:esploro.exlibrisgroup.com` |
| Censys | `web.names: "esploro.exlibrisgroup.com"` |
| crt.sh | `%.esploro.exlibrisgroup.com` |

## Elsevier Digital Commons (`elsevierdigitalcommons`) {#elsevierdigitalcommons}

Hosted institutional repository (bepress / Elsevier). Hosts often `*.bepress.com`, `digitalcommons.` plus a campus domain, or `dc.` plus a campus host.

**Signals:** Digital Commons branding; `/do/oai/` or bepress OAI; article/dataset collections.

**Confirm:** GET the IR home and OAI Identify when public. Register the repository root, not a single series. Skip exhibit-only faculty profile sites with no dataset or research-output catalog.

| Tool | Query |
|------|-------|
| Google | `"Digital Commons" (bepress OR Elsevier) (datasets OR repository) -site:elsevier.com` |
| Google | `site:bepress.com OR inurl:digitalcommons` |
| Censys | `web.endpoints.http.body: "bepress"` |
| crt.sh | `%.bepress.com` |

## InstDB (`instdb`) {#instdb}

FairStack institutional research-data repository (CAS / CNIC). Site: [fairstack.cn](https://fairstack.cn/product/software/InstDB).

**Signals:** InstDB / FairStack branding; Chinese Academy of Sciences data-center portals; DOI/CSTR assignment UI.

**Confirm:** GET the public catalog home. One record per institutional node, not per dataset.

| Tool | Query |
|------|-------|
| Google | `"InstDB" OR "FairStack" (数据仓储 OR repository) -site:fairstack.cn` |
| Censys | `web.endpoints.http.body: "InstDB"` |

## WEKO3 (`weko3`) {#weko3}

NII / RCOS open-source institutional repository. Docs: [weko3.readthedocs.io](https://weko3.readthedocs.io).

**Signals:** WEKO3 / WEKO branding; Invenio-like item types; `/api` search; OAI-PMH.

**Confirm:** GET the repository home and `/oai?verb=Identify` when exposed. Register the IR root.

| Tool | Query |
|------|-------|
| Google | `"WEKO3" OR "WEKO 3" (repository OR 機関リポジトリ) -site:github.com` |
| Censys | `web.endpoints.http.body: "WEKO3"` |

## Omega-PSIR (`omegapsir`) {#omegapsir}

Polish university CRIS + repository. Site: [omegapsir.io](https://www.omegapsir.io).

**Signals:** Omega-PSIR / “Baza Wiedzy”; researcher profiles plus research-data records.

**Confirm:** GET the public CRIS/repository home. One record per university instance.

| Tool | Query |
|------|-------|
| Google | `"Omega-PSIR" OR "Baza Wiedzy" (repozytorium OR CRIS) site:.pl` |
| Censys | `web.endpoints.http.body: "Omega-PSIR"` |

## Islandora (`islandora`) {#islandora}

Public Drupal + Fedora repository UI. Prefer `islandora` over `fedora` when users see an Islandora collection browser (not a generic Drupal site).

| Tool | Query |
|------|-------|
| Google | `"Islandora" (repository OR collections) -site:github.com` |

## Samvera (`samvera`) {#samvera}

Samvera collection UI without Hyrax branding. Use [Hyrax](#hyrax) when that is the branded product. Prefer these IDs over `fedora`. **Confirm:** `/catalog` JSON.

| Tool | Query |
|------|-------|
| Google | `"Samvera" (repository OR "research data") -site:samvera.org` |

## Haplo (`haplo`) {#haplo}

Research information / repository platform. Site: [haplo.com](https://www.haplo.com).

**Confirm:** GET the public research-outputs or dataset catalog (not a staff-only CRIS). Skip haplo.com marketing.

| Tool | Query |
|------|-------|
| Google | `"Haplo" (repository OR "research outputs" OR "research data") -site:haplo.com` |
| Censys | `web.endpoints.http.body: "haplo"` |

## Worktribe (`worktribe`) {#worktribe}

University research hub. Site: [worktribe.com](https://worktribe.com).

**Confirm:** GET a public repository or research-data listing. Skip grant-admin-only tenants.

| Tool | Query |
|------|-------|
| Google | `"Worktribe" (repository OR "research data") -site:worktribe.com` |
| Censys | `web.endpoints.http.body: "worktribe"` |

## FAIRDOM-SEEK (`seek`) {#seek}

Catalog for datasets, models, SOPs, and workflows. Site: [seek4science.org](https://seek4science.org). Includes WorkflowHub / FAIRDOMHub-style Rails apps.

**Confirm:** GET the public SEEK home or `/investigations` listing. JSON API is a plus. Do not add a single assay page.

| Tool | Query |
|------|-------|
| Google | `"FAIRDOM-SEEK" OR "FAIRDOMHub" OR WorkflowHub (datasets OR catalog)` |
| Censys | `web.endpoints.http.body: "fairdom"` |

## RAMADDA (`ramadda`) {#ramadda}

Repository for Archiving, Managing and Accessing Diverse Data. Site: [ramadda.org](https://www.ramadda.org).

**Confirm:** GET the repository entry page (folder/catalog UI). Skip a single file download.

| Tool | Query |
|------|-------|
| Google | `"RAMADDA" (repository OR catalog OR "data portal") -site:github.com` |
| Censys | `web.endpoints.http.body: "RAMADDA"` |

## Other scientific platforms

| `software.id` | Signals | Typical query |
|---------------|---------|---------------|
| `haplo` | see above | |
| `worktribe` | see above | |
| `seek` | see above | |
| `ramadda` | see above | |
| `nyudatacatalog` | NYU Data Catalog forks | `"NYU Data Catalog" OR "data-catalog" medical library` |
| `icat` | see above | |
| `ensembl` | Ensembl genome browsers | `site:ensembl.org` taxon portals only (do not clone www) |
| `datalad` | DataLad catalogs / GIN-like | `"DataLad" (catalog OR datasets)` |
| `hubzero` | HUBzero scientific gateway | `"HUBzero" (resources OR database)` |
| `linkahead` | LinkAhead / CaosDB | `"LinkAhead" OR CaosDB repository` |
| `vufind` | VuFind **data/repo discovery** | `"VuFind" (research data OR datasets)` |
| `mytardis` | MyTardis | `"MyTardis" (data OR repository)` |
| `librecat` | LibreCat | `"LibreCat" repository` |
| `gin` | G-Node gin (gogs+git-annex) | `site:gin.g-node.org` or `"GIN" g-node` |
| `symbiota` | see above | |
| `breedbase` | BrAPI crop breeding instances | `"Breedbase" OR CassavaBase BrAPI` |
| `tripal` | Tripal/Chado genome databases | `"Powered by Tripal"` |
| `veupathdb` | EuPathDB WDK organism sites | `VEuPathDB OR PlasmoDB` |
| `massbank` | MassBank / MoNA instances | `"MassBank" spectra database` |
| `iochembd` | ioChem-BD nodes + Find index | `"ioChem-BD" repository` |
| `esgf` | Metagrid / esg-search (not TDS) | `"esg-search" OR Metagrid ESGF` |
| `yoda` | see above | |
| `gbifplatform` | GBIF.org itself | do not re-add; use `ipt` for publisher IPTs |
| `converis` | Clarivate Converis CRIS | `"Converis" (research OR repository)` |
| `aodn` | AODN Portal | `"AODN" portal` |
| `osf` | OSF institutions | `site:osf.io` **institution** catalogs only |
| `ifremercatalog` | SEANOE | `"SEANOE" IFREMER` |
| `pydap` | PyDAP OPeNDAP | `"PyDAP" OPeNDAP` (else use `opendap`) |
| `djehuty` | 4TU Djehuty | `"Djehuty" (repository OR 4TU)` |
| `dlibra` | dLibra digital library **with datasets** | `"dLibra" (dane OR dataset)` |

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

- [discovery-scientific-domain.md](discovery-scientific-domain.md)
- [discovery.md](discovery.md)
- [discovery-search-tools.md](discovery-search-tools.md)
- [discovery-metadata.md](discovery-metadata.md)
- [discovery-other.md](discovery-other.md)
- [re3data.md](re3data.md)
- [harvest.md](harvest.md) — crawl datasets from repository APIs (filter publications vs data)
- [harvest-scientific.md](harvest-scientific.md), [harvest-scientific-domain.md](harvest-scientific-domain.md), [harvest-biodiversity.md](harvest-biodiversity.md), [harvest-earthdata.md](harvest-earthdata.md)
- [software-taxonomy.md](software-taxonomy.md)
