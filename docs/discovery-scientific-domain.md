# Discovering domain scientific repositories

Biodiversity, facility, crop, chemistry, and earth-system repositories (`catalog_type: Scientific data repository`). Institutional IRs: [discovery-scientific.md](discovery-scientific.md). Search-engine syntax: [discovery-search-tools.md](discovery-search-tools.md). Harvest: [harvest-scientific-domain.md](harvest-scientific-domain.md).

One portal / node = one registry record. Do not add gene pages, occurrences, or ESGF data nodes as extra catalogs.

## GBIF IPT (`ipt`) {#ipt}

Integrated Publishing Toolkit for biodiversity data. List: [gbif.org/ipt](https://www.gbif.org/ipt).

**Confirm:** `/rss.do`, `/inventory/dataset`, or the IPT homepage with installation name.

| Tool | Query |
|------|-------|
| Google | `"Integrated Publishing Toolkit" IPT GBIF` |
| Google | `inurl:/ipt "GBIF"` |
| Censys | `web.endpoints.http.body: "Integrated Publishing Toolkit"` |

Prefer GBIF’s official installation list, then fill gaps with search.

## Symbiota (`symbiota`) {#symbiota}

Open-source biodiversity collections CMS. Official portal directory: [symbiota.org/symbiota-portals](https://symbiota.org/symbiota-portals/). Docs: [docs.symbiota.org](https://docs.symbiota.org/).

Theme-based portals (SEINet, MyCoPortal, CCH2, Ecdysis, and others) publish specimen occurrences, images, checklists, and Darwin Core datasets. Register **one catalog per portal**, not per collection or GBIF IPT mirror. Use `software.id: symbiota`.

**Confirm:** public collection search (`/collections/index.php` or `/portal/collections/`) and/or dataset RSS at `/collections/datasets/rsshandler.php`. Page signals include “Symbiota”, `collid=`, and “Search Collections”. Skip login-only portals and the vendor homepage.

| Tool | Query |
|------|-------|
| Google | `"Powered by Symbiota" OR "Symbiota portal" (collections OR occurrences) -site:symbiota.org -site:github.com` |
| Google | `inurl:/collections/datasets/rsshandler.php` |
| Censys | `web.endpoints.http.body: "Symbiota"` |

## THREDDS (`thredds`) {#thredds}

Scientific data servers (often climate/ocean). **Confirm:** `/thredds/catalog.html` or `/thredds/catalog.xml`.

| Tool | Query |
|------|-------|
| Google | `inurl:/thredds/catalog.html` |
| Google | `"THREDDS Data Server" catalog` |
| Censys | `web.endpoints.http.body: "THREDDS"` |
| Shodan | `http.html:"THREDDS Data Server"` |

## ERDDAP (`erddap`) {#erddap}

NOAA-style tabular/gridded data server. **Confirm:** `/erddap/index.html` or `/erddap/info/index.json`.

| Tool | Query |
|------|-------|
| Google | `inurl:/erddap "ERDDAP"` |
| Censys | `web.endpoints.http.body: "ERDDAP"` |

## OPeNDAP (`opendap`) {#opendap}

Remote subsetting protocol and Hyrax/THREDDS-style servers. Site: [opendap.org](https://www.opendap.org). Use `opendap` when the public catalog is an OPeNDAP/Hyrax directory, not when OPeNDAP is only a download option on THREDDS (`thredds`) or ERDDAP (`erddap`).

**Confirm:** GET a catalog XML or Hyrax/OPeNDAP directory listing.

| Tool | Query |
|------|-------|
| Google | `"OPeNDAP" (Hyrax OR "catalog.xml") -site:opendap.org -site:github.com` |
| Censys | `web.endpoints.http.body: "OPeNDAP"` |

## DataONE (`dataone`) {#dataone}

Earth-science member-node network. Site: [dataone.org](https://www.dataone.org). Prefer the **member node** catalog URL, not every harvested dataset.

**Confirm:** GET the member-node home. Duplicate-check before adding nodes already in re3data / this registry.

| Tool | Query |
|------|-------|
| Google | `"DataONE" ("member node" OR MN) repository` |
| Censys | `web.endpoints.http.body: "DataONE"` |

## Galaxy (`galaxy`) {#galaxy}

Usable-analysis platform that sometimes publishes public data libraries. Site: [usegalaxy.org](https://usegalaxy.org). Register **public Galaxy instances with a data library / shared histories catalog**, not every private analysis server.

**Confirm:** GET the instance and a public data-library or toolshed-adjacent dataset listing.

| Tool | Query |
|------|-------|
| Google | `"Galaxy" ("data libraries" OR usegalaxy) -site:galaxyproject.org` |
| Censys | `web.endpoints.http.body: "usegalaxy"` |

## Atlas of Living Australia (`ala`) {#ala}

Biodiversity occurrence catalogs (ALA and national living-atlas forks). Site: [ala.org.au](https://www.ala.org.au).

**Confirm:** GET the public occurrence/search portal. One record per national atlas, not per collection.

| Tool | Query |
|------|-------|
| Google | `"Atlas of Living Australia" OR "Living Atlas" (occurrences OR biocache)` |
| Censys | `web.endpoints.http.body: "biocache"` |

## SciCat (`scicat`) {#scicat}

Metadata catalogue for photon/neutron facilities. Docs: [scicatproject.github.io](https://scicatproject.github.io).

**Signals:** SciCat Angular UI; `/api/v3/` or dataset DOI landing pages (PSI, ESS, MAX IV).

**Confirm:** GET the public dataset search. One record per facility catalogue.

| Tool | Query |
|------|-------|
| Google | `"SciCat" (dataset OR catalogue) (ESS OR PSI OR "MAX IV") -site:github.com` |
| Censys | `web.endpoints.http.body: "scicat"` |

## Axiom Data Science Portal (`axiomportal`) {#axiomportal}

IOOS-style ocean observing explorer (Axiom). Distinct from ERDDAP/THREDDS backends.

**Signals:** Axiom portal chrome; sensor time series; compiled data views.

**Confirm:** GET the public portal home. Do not also register the bundled ERDDAP as a second catalog unless it is a separate public product.

| Tool | Query |
|------|-------|
| Google | `"Axiom" ("Data Science" OR IOOS) portal` |
| Censys | `web.endpoints.http.body: "axiomdatascience"` |

## OntoPortal (`ontoportal`) {#ontoportal}

Ontology repositories (BioPortal-style). Site: [ontoportal.org](https://ontoportal.org).

**Confirm:** GET the public ontology browser / REST. One record per public OntoPortal appliance.

| Tool | Query |
|------|-------|
| Google | `"OntoPortal" OR "BioPortal" (ontology repository) -site:bioontology.org` |
| Censys | `web.endpoints.http.body: "ontoportal"` |

## Breedbase (`breedbase`) {#breedbase}

Crop breeding information systems. Site: [breedbase.org](https://breedbase.org). Instances include CassavaBase, MusaBase, YamBase, SweetPotatoBase, Sol Genomics Network, and Triticeae Toolbox (T3).

**Signals:** Breedbase chrome; `/brapi/v2/serverinfo`; crop “Base” branding.

**Confirm:** GET `https://host/brapi/v2/serverinfo` JSON, or the public trial/search UI. One record per crop instance, not per trial.

| Tool | Query |
|------|-------|
| Google | `"Breedbase" OR CassavaBase OR MusaBase OR YamBase OR SweetPotatoBase (breeding OR BrAPI)` |
| Google | `inurl:/brapi/v2/serverinfo` |
| Censys | `web.endpoints.http.body: "Breedbase"` |

## Tripal (`tripal`) {#tripal}

GMOD Tripal genome databases (Drupal + Chado). Site: [tripal.info](https://tripal.info).

**Signals:** “Powered by Tripal”; `/web-services/`; Chado/Tripal footer.

**Confirm:** GET the public organism/dataset home or Tripal web services. Skip generic Drupal sites without Chado biological content. Prefer Tripal over `drupal` when the catalog is a genome database.

| Tool | Query |
|------|-------|
| Google | `"Powered by Tripal" OR "Tripal" (genome OR germplasm OR Chado) -site:tripal.info -site:github.com` |
| Censys | `web.endpoints.http.body: "Tripal"` |

## VEuPathDB (`veupathdb`) {#veupathdb}

EuPathDB WDK organism sites. Hub: [veupathdb.org](https://veupathdb.org). Component sites include PlasmoDB, FungiDB, VectorBase, and TriTrypDB.

**Signals:** VEuPathDB / EuPathDB chrome; search-strategy UI; `/webservices/`.

**Confirm:** GET the public search home. One record per organism portal (plus the hub if it is a distinct catalog UI). Do not add every gene page.

| Tool | Query |
|------|-------|
| Google | `"VEuPathDB" OR EuPathDB OR PlasmoDB OR FungiDB OR VectorBase OR TriTrypDB (genome OR "data set")` |
| Censys | `web.endpoints.http.body: "VEuPathDB"` |

## MassBank (`massbank`) {#massbank}

Community reference mass-spectral databases. Instances: MassBank Europe, MassBank Japan, MoNA.

**Signals:** MassBank record IDs; `/MassBank/` UI; MoNA `/rest/spectra`.

**Confirm:** GET the public spectral search. One record per instance, not per spectrum.

| Tool | Query |
|------|-------|
| Google | `"MassBank" (spectra OR "mass spectral") (database OR repository) -site:github.com` |
| Google | `"MassBank of North America" OR MoNA spectra` |
| Censys | `web.endpoints.http.body: "MassBank"` |

## ioChem-BD (`iochembd`) {#iochembd}

Distributed computational-chemistry repository. Site: [iochem-bd.org](https://www.iochem-bd.org). Browse modules are DSpace-based; use `iochembd` (not `dspace`) when the product is ioChem-BD.

**Signals:** ioChem-BD branding; `/rest/items`; `/oai/request?verb=Identify`; CML datasets.

**Confirm:** GET the public Browse collections or OAI Identify. Register each **public node** and the central Find index as distinct catalogs. Skip Create-only private workspaces.

| Tool | Query |
|------|-------|
| Google | `"ioChem-BD" (repository OR "computational chemistry") -site:github.com` |
| Censys | `web.endpoints.http.body: "ioChem-BD"` |

## ESGF (`esgf`) {#esgf}

Earth System Grid Federation **search/index** (Metagrid, esg-search). Site: [esgf.llnl.gov](https://esgf.llnl.gov).

**Signals:** Metagrid UI; `/esg-search/search`; CMIP dataset index.

**Confirm:** GET a working esg-search query or the public Metagrid home. Use `thredds` for ESGF **data nodes** that expose `/thredds/catalog.xml`. Do not clone every data node as `esgf`.

| Tool | Query |
|------|-------|
| Google | `"ESGF" OR Metagrid ("esg-search" OR CMIP) (catalog OR search)` |
| Censys | `web.endpoints.http.body: "esg-search"` |

## ICAT (`icat`) {#icat}

Facility scientific catalog. Site: [icatproject.org](https://icatproject.org).

**Confirm:** GET the public dataset search UI or documented ICAT REST/OAI. Skip facility login-only stores. Do not clone icatproject.org itself.

| Tool | Query |
|------|-------|
| Google | `"ICAT" (facility OR "data catalog" OR "scientific data") -site:icatproject.org -site:github.com` |
| Censys | `web.endpoints.http.body: "icat"` |

## Related

- [discovery-scientific.md](discovery-scientific.md)
- [discovery.md](discovery.md)
- [discovery-search-tools.md](discovery-search-tools.md)
- [harvest-scientific-domain.md](harvest-scientific-domain.md)
- [harvest-biodiversity.md](harvest-biodiversity.md)
- [harvest-earthdata.md](harvest-earthdata.md)
- [software-index.md](software-index.md)

