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

List dataflows from the NSI REST/SOAP endpoint in `endpoints[]` (`/rest/dataflow` or documented NSI path). Keep dataflows. Drop structure-only resources. If the human catalog is PxWeb/.Stat, harvest that UI’s table list instead of raw SOAP.

## GENESIS-Online (`genesisonline`)

Table **retrieval** is often POST-only. There is no reliable public GET “list all tables” API. Harvest the public research/catalog UI identifiers if documented; do not invent GET paths. Stop on login walls.

## IBIS-PH (`ibisph`)

Indicator pages and IBIS-Q query modules. Harvest public **indicator** home records (XML/HTML indicator ids). Skip query-builder sessions and PDF fact sheets as separate datasets.

## NADA (`nada`)

Survey microdata catalog.

```text
GET https://host/index.php/api/catalog/search
```

Page the JSON study list. Keep survey / microdata / geospatial studies. **Drop** `dtype` values that are document, video, or news when present. CSV export (`/index.php/catalog/export/csv`) is a bulk study list — still one row per study, not per file.

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

## Other indicator IDs

| `software.id` | Harvest | Skip |
|---------------|---------|------|
| `superstar` | Census table builder catalogs | Marketing |
| `oracleapex` | Public indicator apps only | Generic APEX sites |
| `superset` | Public dashboard **datasets** | Internal BI |
| `ibmcognos` | Public stat packages | Intranet Cognos |
| `eurostat`, `ecb`, `whoint`, `dataworldbankorg`, `ilostat`, `databisorg`, `datauniceforg` | Official dataflow/indicator APIs if the user asked to harvest **that** hub | Do not clone them as new registry catalogs; they are already registered |

## Related

- [harvest.md](harvest.md)
- [harvest-metadata.md](harvest-metadata.md) (SDMX **structure** vs dataflows)
- [discovery-indicators.md](discovery-indicators.md)
- [apidetect.md](apidetect.md)
