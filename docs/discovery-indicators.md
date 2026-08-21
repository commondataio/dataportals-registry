# Discovering indicators and microdata catalogs

How to find **indicators catalogs** (`catalog_type: Indicators catalog`) and **microdata catalogs** (`catalog_type: Microdata catalog`). Search-engine syntax: [discovery-search-tools.md](discovery-search-tools.md).

Statistical offices, central banks, SDG reporting sites, and survey archives are the usual owners. Search the agency name plus the local word for “statistics” / “indicators” / “microdata”, then confirm the platform.

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

## Other indicator platforms

| `software.id` | Where to look | Typical query |
|---------------|---------------|---------------|
| `knoema` | Knoema-branded country hubs | `site:knoema.com "{country}"` (only add distinct portals, not every dataset) |
| `superstar` | Census table browsers | `"SuperSTAR" (census OR "table builder")` |
| `genesisonline` | Destatis GENESIS and clones | `"GENESIS-Online" site:.de` |
| `sdmxri` | SDMX-RI / NSI web | `"SDMX-RI" OR "NSI Web Service"` |
| `ibisph` | US state public-health indicators | `"IBIS-PH" OR "IBIS PH" health indicators` |
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
- [catalog-types.md](catalog-types.md)
- [software-taxonomy.md](software-taxonomy.md)
