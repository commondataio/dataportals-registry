# Discovering indicators and microdata catalogs

How to find **indicators catalogs** (`catalog_type: Indicators catalog`) and **microdata catalogs** (`catalog_type: Microdata catalog`). Search-engine syntax: [discovery-search-tools.md](discovery-search-tools.md).

Statistical offices, central banks, SDG reporting sites, and survey archives are the usual owners. Search the agency name plus the local word for “statistics” / “indicators” / “microdata”, then confirm the platform. High-count stacks with their own recipes: PxWeb, OpenSDG, .Stat Suite, Knoema (portal homes only), SDMX-RI, GENESIS-Online, IBIS-PH, DHIS2, NADA, NESSTAR, REDATAM, Colectica, OBiBa Mica, IPUMS.

## PxWeb (`pxweb`)

PC-Axis web tables, widely used by Nordic and other NSOs. Examples: [SCB PxWeb examples](https://www.scb.se/en/services/statistical-programs-for-px-files/px-web/pxweb-examples/).

**Confirm:** `https://host/api/v1/` (language segment may be `/api/v1/en/` or `/api/v1/{lang}/`). UI often `/pxweb/` or titled “PxWeb”.

| Tool | Query |
|------|-------|
| Google | `intitle:PxWeb OR inurl:/pxweb` |
| Google | `inurl:/api/v1 "px" statistics` |
| Google | `"PxWeb" (statistik OR statistics OR tilastot)` |
| Censys | `web.endpoints.http.html_title: "PxWeb"` |
| Censys | `web.endpoints.http.body: "PxWeb"` |
| Shodan | `http.title:"PxWeb"` |

**False positives:** documentation for PX files, desktop PC-Axis, a single `.px` download page. Need the **table tree** UI or `/api/v1/`.

## OpenSDG (`opensdg`)

Static SDG reporting sites (often GitHub Pages). Community: [open-sdg.org/community](https://open-sdg.org/community).

**Signals:** `/reporting-status`, indicator pages `/\{goal\}-\{target\}-\{indicator\}`, “Open SDG” in footer or `open-sdg` JS.

| Tool | Query |
|------|-------|
| Google | `"Open SDG" OR "open-sdg" indicators` |
| Google | `inurl:reporting-status "sustainable development"` |
| Google | `site:github.io "Open SDG"` |
| Censys | `web.endpoints.http.body: "open-sdg"` |

Start from the community list; use Google for national translations (`indicadores ODS`, `indicateurs ODD`).

## .Stat Suite (`statsuite`)

SIS-CC / OECD .Stat. **Confirm:** `/api/search` or SDMX endpoints; UI “.Stat Suite” / Data Explorer.

| Tool | Query |
|------|-------|
| Google | `".Stat Suite" OR "SIS-CC" "data explorer"` |
| Google | `inurl:/nsi OR "DotStat" SDMX` |
| Censys | `web.endpoints.http.body: ".Stat"` |

## Knoema (`knoema`)

Commercial indicator portals and country hubs. Site: [knoema.com](https://knoema.com). Ministries and banks often run a branded hub on a `knoema.com` subdomain or a custom domain.

**Signals:** Knoema chrome; `/atlas` or dataset explorer; REST under `/api/1.0/` or `/api/3.0/`.

**Confirm:** GET the **portal home** (a catalog of datasets). Do **not** add every Knoema dataset URL. Skip the global knoema.com hub if it is already registered; add only distinct institutional sites.

| Tool | Query |
|------|-------|
| Google | `site:knoema.com (atlas OR "data portal")` |
| Google | `"powered by Knoema" OR "Knoema" (statistics OR indicators) -site:knoema.com` |
| Censys | `web.names: "knoema.com"` |
| crt.sh | `%.knoema.com` |

## SDMX-RI (`sdmxri`)

Eurostat SDMX Reference Infrastructure (NSI web service). Site: [sdmx.org](https://sdmx.org/?page_id=4666).

**Signals:** `NSIWebService`, SDMX-RI; `/NSIStdV20Service` or SDMX REST 2.1.

**Confirm:** GET a working SDMX query or the public NSI page that lists dataflows. If PxWeb or .Stat is the human UI, register that catalog instead of a raw SOAP URL.

| Tool | Query |
|------|-------|
| Google | `"SDMX-RI" OR "NSI Web Service" OR NSIStdV20Service` |
| Censys | `web.endpoints.http.body: "NSIWebService"` |

## GENESIS-Online (`genesisonline`)

Destatis / Länder statistical database. Example: [www-genesis.destatis.de](https://www-genesis.destatis.de). Table retrieval is often **POST-only** — do not invent GET API paths.

**Signals:** GENESIS-Online; `genesisclient`; `/genesis/online`.

**Confirm:** GET the public table catalog. One record per statistical-office instance (Bund vs Land).

| Tool | Query |
|------|-------|
| Google | `"GENESIS-Online" (Statistik OR Destatis) site:.de` |
| Google | `inurl:/genesis/online` |
| Censys | `web.endpoints.http.body: "GENESIS-Online"` |

## IBIS-PH (`ibisph`)

US state public-health indicator system. Community: [Adopt IBIS](https://ibis.utah.gov/ibisph-view/resource/AdoptIBIS.html).

**Signals:** IBIS-PH / IBIS-Q; `/ibisph-view/`; XML-driven indicator pages.

**Confirm:** GET a public indicator home or query module. Skip login-only health department tools.

| Tool | Query |
|------|-------|
| Google | `"IBIS-PH" OR "IBIS PH" (indicators OR "public health") site:.gov` |
| Censys | `web.endpoints.http.body: "ibisph"` |

## DHIS2 (`dhis2`)

Open-source health management information system (HISP / University of Oslo). More than 70 ministries run national HMIS instances. Docs: [docs.dhis2.org](https://docs.dhis2.org). Public FlexiPortal front-ends also count when they publish indicators from a DHIS2 backend.

`software.id: dhis2` is **not** in the published software catalog. Register finds as `custom` until that definition ships. Do not label a CKAN health document site DHIS2 from a tag alone.

**Signals:** `/dhis-web-commons/`, `/dhis-web-dashboard/`, login chrome “DHIS 2”; REST `/api/system/info`.

**Confirm:** `GET https://host/api/system/info` JSON with a `version` field, or a public portal that is documented as DHIS2. Skip staff-only logins with no public indicator catalog.

| Tool | Query |
|------|-------|
| Google | `"DHIS2" OR "DHIS 2" (HMIS OR "health information" OR portal) -site:dhis2.org -site:github.com` |
| Google | `inurl:/dhis-web-commons OR inurl:/api/system/info` |
| Censys | `web.endpoints.http.body: "dhis-web-commons"` |
| Censys | `web.endpoints.http.html_title: "DHIS 2"` |

## Other indicator platforms

| `software.id` | Where to look | Typical query |
|---------------|---------------|---------------|
| `superstar` | Census table browsers | `"SuperSTAR" (census OR "table builder")` |
| `statsuite` | see above | |
| `stattech` | SIS-CC .Stat technology / SDMX APIs | `"Stat Technology" OR "SIS-CC" SDMX` |
| `oracleapex` | Oracle APEX **indicator apps** | `"Oracle APEX" (statistika OR indicators)` (skip generic APEX sites) |
| `datavavt` | Data VAVT economic indicators | `"data.vavt.ru"` |
| `superset` | Apache Superset **public indicator dashboards** | `"Apache Superset" (open data OR indicators)` |
| `ibmcognos` | Cognos **public stat portals** | `"Cognos" (statistics OR open data)` |
| `bicontour` | BI Contour dashboards | `"Contour BI" OR "BI Contour" portal` |
| `whoint` | WHO data hub | do not re-add who.int; add only distinct regional hubs |
| `eurostat` | Eurostat | do not re-add the EU hub |
| `ecb` | ECB Data Portal | do not re-add data.ecb.europa.eu |
| `dataworldbankorg` | World Bank Data | do not re-add data.worldbank.org |
| `datauniceforg` | UNICEF data | do not re-add data.unicef.org |
| `ilostat` | ILOSTAT | do not re-add ilostat.ilo.org |
| `databisorg` | BIS Data Portal | do not re-add data.bis.org |
| `datainsight` | Veritas Data Insight **as a public catalog** | rare; skip enterprise-only |

National statistical office homepages often link “database”, “statbank”, “PC-Axis”, “SDMX”. Follow those links rather than guessing software from the NSO CMS.

## NADA (`nada`)

IHSN National Data Archive for survey microdata. Site: [nada.ihsn.org](https://nada.ihsn.org). UI: study catalog, often `/index.php/catalog`.

**Confirm:** `https://host/index.php/api/catalog/search` (JSON) or the public catalog listing without login.

| Tool | Query |
|------|-------|
| Google | `"NADA" "microdata" OR "national data archive" IHSN` |
| Google | `inurl:/index.php/catalog "microdata"` |
| Google | `"Powered by NADA" OR "nada" "survey catalog"` |
| Censys | `web.endpoints.http.body: "NADA"` |
| Censys | `web.endpoints.http.body: "IHSN"` |

**False positives:** nada.ihsn.org itself (the software site), WordPress blogs named NADA. Need a **study list** with DDI-style metadata.

## IPUMS (`ipums`)

University of Minnesota extract platform for harmonized census and survey microdata. Collections share one API and extract engine: IPUMS USA, International, CPS, DHS, NHIS, Higher Ed, PMA, MICS, Time Use, plus geographic NHGIS and IHGIS. Developer docs: [developer.ipums.org](https://developer.ipums.org).

`software.id: ipums` is **not** in the published software catalog. Register collection homes as `custom` until that definition ships.

**Signals:** `*.ipums.org` or `idhsdata.org` / `nhgis.org`; extract-system UI; “IPUMS” branding.

**Confirm:** GET the **collection home** (variable/sample selector), not a completed extract download. One registry record per public collection. Do not add every extract or variable page.

| Tool | Query |
|------|-------|
| Google | `"IPUMS" (extract OR microdata OR census) site:.org -site:github.com` |
| Google | `site:ipums.org (USA OR International OR CPS OR NHIS)` |
| Censys | `web.names: "ipums.org"` |

## NESSTAR (`nesstar`)

Older microdata publisher. Many instances are inactive; still record working public catalogs.

| Tool | Query |
|------|-------|
| Google | `"Nesstar" (microdata OR "webview")` |
| Google | `inurl:/webview nesstar` |
| Censys | `web.endpoints.http.body: "Nesstar"` |

## REDATAM (`redatam`)

ECLAC census/survey online processing. Site: [redatam.org](https://www.redatam.org).

| Tool | Query |
|------|-------|
| Google | `"REDATAM" (censos OR census OR "en línea")` |
| Google | `inurl:redatam OR "Redatam Web Server"` |
| Censys | `web.endpoints.http.body: "REDATAM"` |

## Colectica (`colectica`)

DDI metadata catalogs / portals.

| Tool | Query |
|------|-------|
| Google | `"Colectica" (portal OR repository OR DDI)` |
| Censys | `web.endpoints.http.body: "Colectica"` |

## OBiBa Mica (`obibamica`)

Epidemiological / population-health study catalog (OBiBa). Often paired with Opal; register the **public Mica** discovery UI.

**Signals:** Mica / OBiBa branding; study and network search; `/ws/` REST.

**Confirm:** GET the public study catalog. Skip login-only research networks.

| Tool | Query |
|------|-------|
| Google | `"Mica" OBiBa (studies OR catalog) -site:github.com` |
| Censys | `web.endpoints.http.body: "obiba"` |
| Censys | `web.endpoints.http.body: "Mica"` |

## Survey Solutions (`surveysolutions`)

World Bank survey suite. Register only a **public Data Browser** of microdata, not a data-collection server.

| Tool | Query |
|------|-------|
| Google | `"Survey Solutions" ("data browser" OR microdata) -site:mysurvey.solutions` |

## Generic statistics-office patterns

```text
site:.gov {country} (statbank OR "statistical database" OR pxweb OR sdmx)
"microdata" (catalog OR archive OR "data archive") {NSO name}
"DDI" "survey catalog" {country}
```

Central banks (`indicators` more often than microdata): `site:{bank-domain} (statistics OR SDMX OR "statistical warehouse")`. Only add a catalog when there is a queryable database, not a PDF publications page.

## Related

- [discovery.md](discovery.md)
- [discovery-search-tools.md](discovery-search-tools.md)
- [discovery-metadata.md](discovery-metadata.md)
- [harvest-indicators.md](harvest-indicators.md)
- [harvest.md](harvest.md)
- [harvest-protocols.md](harvest-protocols.md)
- [catalog-types.md](catalog-types.md)
- [software-taxonomy.md](software-taxonomy.md)
