# Harvesting indicators and microdata

Statistical catalogs list **tables, dataflows, indicators, or survey studies**. Harvest those objects — not every observation, PDF yearbook, or news page.

Overview: [harvest.md](harvest.md). Finding catalogs: [discovery-indicators.md](discovery-indicators.md). GET only. Stop on `401`/`403`. Prefer `endpoints[]`.

## What to keep

| Keep | Drop |
|------|------|
| PxWeb **table** (leaf in the subject tree) | Subject **folders** and the API root |
| SDMX **dataflow** | Codelists, concept schemes, DSDs as if they were data (unless you harvest structural metadata on purpose — [harvest-metadata.md](harvest-metadata.md)) |
| OpenSDG **indicator** JSON | Static about/reporting HTML |
| NADA / NESSTAR **study** | Videos, documents, and news items in the same catalog |
| Mica **study** / dataset | Network chrome, person records |
| DHIS2 **data set** / public indicator | Org-unit trees, user accounts, login-only analytics |
| IPUMS **sample** / collection metadata | Completed extract files and variable pages as catalogs |
| Knoema **dataset** on a portal | Individual time-series points and knoema.com global search hits |

Do not download full observation cubes unless the user asked for data files. Catalog harvest = identifiers + title + URL + period.

## PxWeb (`pxweb`)

Walk the JSON tree. Language segment is often `en`, `sv`, `fi`, `da`.

```text
GET https://host/api/v1/
GET https://host/api/v1/en/
```

Each JSON object with `type: t` (table) is a dataset. `type: l` is a folder — recurse. Do not treat a POST of table cells as a new dataset. Cap depth; some NSOs have thousands of tables.

## OpenSDG (`opensdg`)

Each SDG indicator is one dataset. List from reporting status or `data/` JSON.

```text
GET https://host/reporting-status
GET https://host/data/1-1-1.json
```

Language prefixes (`/en/data/…`) vary. Harvest every indicator id the site publishes, not only `1-1-1`. Drop goal/target **pages** without a data file.

## .Stat Suite (`statsuite`) and Stat Technology (`stattech`)

```text
GET https://host/api/search
```

SDMX REST: list **dataflows** (the dataset analog). Pair with the Data Explorer UI only to confirm labels. If PxWeb is the public UI on the same office, harvest one catalog — do not double-count the same table.

## Knoema (`knoema`)

Portal REST (`/api/1.0/` or `/api/3.0/`) lists **datasets** for that hub. Page the dataset catalog. Do not crawl every resource URL or the global knoema.com search. One portal = one harvest scope.

## SDMX-RI (`sdmxri`)

List dataflows from the NSI REST/SOAP endpoint in `endpoints[]` (`/rest/dataflow` or documented NSI path). Keep dataflows. Drop structure-only resources. If the human catalog is PxWeb/.Stat, harvest that UI’s table list instead of raw SOAP. REST grain: [harvest-protocols.md](harvest-protocols.md#sdmx).

## GENESIS-Online (`genesisonline`)

Table **retrieval** is often POST-only. There is no reliable public GET “list all tables” API. Harvest the public research/catalog UI identifiers if documented; do not invent GET paths. Stop on login walls.

## IBIS-PH (`ibisph`)

Indicator pages and IBIS-Q query modules. Harvest public **indicator** home records (XML/HTML indicator ids). Skip query-builder sessions and PDF fact sheets as separate datasets.

## DHIS2 (`dhis2`)

Not a published `software.id` yet. Match DHIS2 hosts in exports as `custom` (or by `link`).

National HMIS / public health indicator portals.

```text
GET https://host/api/system/info
GET https://host/api/dataSets.json?fields=id,displayName&pageSize=50
GET https://host/api/indicators.json?fields=id,displayName&pageSize=50
```

Keep **data sets** and public **indicators**. Drop user accounts, org-unit trees as datasets, and login-only analytics. Stop on `401`/`403`. Many ministries expose no anonymous API — then harvest only the public portal’s documented indicator list. Skip dhis2.org marketing.

## NADA (`nada`)

Survey microdata catalog.

```text
GET https://host/index.php/api/catalog/search
```

Page the JSON study list. Keep survey / microdata / geospatial studies. **Drop** `dtype` values that are document, video, or news when present. CSV export (`/index.php/catalog/export/csv`) is a bulk study list — still one row per study, not per file.

## IPUMS (`ipums`)

Not a published `software.id` yet. Match IPUMS collection homes in exports as `custom` (or by `link`).

One harvest scope per **collection** (USA, International, CPS, …). Use the IPUMS API metadata endpoints ([developer.ipums.org](https://developer.ipums.org)) with the collection name. Keep samples / datasets in that collection. **Drop** completed extract files, variable codebooks as separate catalogs, and the IPUMS marketing homepage. Do not download person-level microdata.

## NESSTAR (`nesstar`)

```text
GET https://host/webview/
GET https://host/api
```

Harvest the **study** list in WebView. Many instances are dead — skip `401`/`404`. Do not scrape the vendor site.

## REDATAM (`redatam`)

```text
GET https://host/redbin/RpWebEngine.exe/Portal
```

HTML census/survey portals with little REST. Harvest the published **database/project** names from the portal home. Do not run interactive tabulations as a crawl.

## Colectica (`colectica`)

DDI repository. Public probe is often `/swagger/ui` or `/swagger/v1/swagger.json`. Search may be POST and/or authenticated — stop on `401`. Harvest **StudyUnit** / dataset items when a public API exists, not every DDI fragment (variables, questions) as a dataset.

## OBiBa Mica (`obibamica`)

```text
GET https://host/studies
GET https://host/api/studies
```

Keep studies and Mica **datasets**. Drop networks, persons, and collected-dataset-empty shells. Docs: [micadoc.obiba.org](https://micadoc.obiba.org/en/latest/rest/).

## Survey Solutions (`surveysolutions`)

Headquarters survey catalogs are often login-only. Harvest only a **public** questionnaire/data listing. Stop on `401`.

## SuperSTAR (`superstar`)

Census table-builder catalogs. Harvest the published **table / database** list, not every cube cell. Skip marketing and login-only builders.

## Official international hubs

These `software.id` values are **one registered catalog each**. Harvest **contents** when the user asked for that hub. Do not add them again as new registry YAML.

Grain is still **dataflow / indicator**, not observation cells. SDMX protocol: [harvest-protocols.md](harvest-protocols.md#sdmx). Prefer `endpoints[]` on the live record when present.

## Eurostat (`eurostat`)

```text
GET https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/latest
```

Each **dataflow** is one dataset analog. The registered observation URL (`/api/dissemination/statistics/1.0/data`) is **not** a catalog list — do not page cubes as datasets.

## ECB (`ecb`)

```text
GET https://data-api.ecb.europa.eu/service/dataflow
```

List **dataflows**. The UI host `data.ecb.europa.eu` is not the SDMX root; `/service/data` is observations. The registered observation endpoint is not a catalog list.

## World Bank (`dataworldbankorg`)

```text
GET https://api.worldbank.org/v2/indicator?format=json&per_page=1000
GET https://api.worldbank.org/v2/sources?format=json
```

Keep **indicators** (or **sources** if the user asked for catalogs-of-catalogs). Drop country pages, WDI observation queries (`/v2/country/.../indicator/...`), and data.worldbank.org marketing.

## WHO GHO (`whoint`)

```text
GET https://ghoapi.azureedge.net/api/Indicator
```

Keep **indicators**. Drop Dimension / country lists and every GHO observation row.

## ILOSTAT (`ilostat`)

```text
GET https://sdmx.ilo.org/rest/dataflow
```

Keep **dataflows**. `www.ilo.org/sdmx/` is often Cloudflare-blocked from scripts — use `sdmx.ilo.org`. Drop ilostat.ilo.org article pages.

## BIS (`databisorg`)

```text
GET https://stats.bis.org/api/v1/dataflow
```

Keep SDMX **dataflows**. The registered `https://data.bis.org/api/v0/search` is **POST** (not a GET list) and is not a dataset catalog. Drop help HTML and observation queries.

## UNICEF (`datauniceforg`)

```text
GET https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/dataflow
```

Keep **dataflows**. Do not treat every country profile on data.unicef.org as a dataset. The HTML site may be Cloudflare-blocked; SDMX is the harvest.

## Oracle APEX (`oracleapex`)

Public statistical **apps** that list indicators or tables. Harvest the documented public REST/ORDS feed if it returns a dataset list. Skip generic APEX sites, login builders, and `/apex/f?p=` session URLs as identifiers.

## Apache Superset (`superset`) and IBM Cognos (`ibmcognos`)

Public BI that sometimes **is** the indicator catalog.

- **Superset:** harvest public **datasets** / charts the catalog documents. Drop internal dashboards and login-only `/superset/dashboard/`.
- **Cognos:** harvest published **packages / reports** that are statistical tables. Drop intranet Cognos.

Stop on `401`. Do not scrape every dashboard tile.

## Other indicator IDs

| `software.id` | Harvest | Skip |
|---------------|---------|------|
| `datavavt` | VA/VT public indicator tables | Intranet |
| `bicontour` | Public contour/indicator catalog | Viewer-only |
| `datainsight` | Public insight **datasets** | Internal BI |

## Related

- [harvest.md](harvest.md)
- [harvest-metadata.md](harvest-metadata.md) (SDMX **structure** vs dataflows)
- [harvest-protocols.md](harvest-protocols.md)
- [harvest-incremental.md](harvest-incremental.md)
- [harvest-identifiers.md](harvest-identifiers.md)
- [harvest-output.md](harvest-output.md)
- [discovery-indicators.md](discovery-indicators.md)
- [apidetect.md](apidetect.md)
- [agents/harvest.md](agents/harvest.md)
