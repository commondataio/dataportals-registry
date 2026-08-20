# Discovering data catalogs

Two different jobs share the word *discovery*:

| Job | What you want | Where to go |
|-----|----------------|-------------|
| Find catalogs **already in this registry** | Filter by country, type, software, or URL | [query-examples.md](query-examples.md), [agents/query.md](agents/query.md) |
| Find catalogs **not yet registered** | New portals, geoportals, repositories | This page, then [CONTRIBUTING.md](https://github.com/datenoio/dataportals-registry/blob/main/CONTRIBUTING.md) |

This page is for the second job: locating real catalog installations in the wild, checking they are not duplicates, and preparing them for a pull request. Coding agents should follow the shorter checklist in [agents/discover.md](agents/discover.md).

The registry records **catalogs** (portals, geoportals, repositories, and similar infrastructure). It does not store the datasets inside those catalogs.

## Before you search

1. Confirm the candidate is a catalog: it lists or serves datasets, maps, indicators, or metadata — not a news site, a single spreadsheet, or a login-only intranet.
2. Search the registry first. Duplicate `link` values fail quality checks (`DUPLICATE_LINK` / `DUPLICATE_LINK_NORMALIZED`).
3. Prefer `data/scheduled/` for unverified finds. Promote later; see [scheduled.md](scheduled.md).

Duplicate check against the DuckDB export:

```sql
SELECT id, name, link, catalog_type, status
FROM catalogs
WHERE lower(link) LIKE '%example.gov%'
   OR id = 'examplegov';
```

Also search `data/scheduled/` if that directory is not empty. Do not walk every YAML file under `data/entities/` unless you are editing a specific record.

## Where catalogs usually live

- National open-data sites: `/data`, `/opendata`, `/datasets`, `data.gov.*`, `datos.*`, `donnees.*`
- Statistics offices, mapping agencies, environmental agencies, and universities
- Local government: city/region sites, `opendata.` subdomains, ArcGIS Hub sites
- Research: institutional repositories, Dataverse, DSpace, GBIF IPT, re3data-listed repos

Search with the local language (`datos abiertos`, `données ouvertes`, `offene daten`, `dados abertos`, `开放数据`) plus the country or city name. Restrict with `site:.gov`, `site:.gob.*`, or the national government TLD.

## Existing lists (start here)

Many platforms publish installation galleries. Cross-check each URL against the registry before adding it. The README [data sources](https://github.com/datenoio/dataportals-registry/blob/main/README.md#data-sources) list is the full inventory; high-yield sources:

| Source | Typical catalogs |
|--------|------------------|
| [CKAN ecosystem](https://ecosystem.ckan.org/dataset/ckan-sites-metadata) | CKAN open-data portals (automated: `scripts/sync_ckan_ecosystem.py`) |
| [Datashades](https://datashades.info/) | CKAN and other portals |
| [data.europa.eu catalogues](https://data.europa.eu/data/catalogues) | EU and member-state catalogs |
| [GeoNetwork gallery](https://github.com/geonetwork/doc/blob/develop/source/annexes/gallery/gallery-urls.csv) | GeoNetwork geoportals |
| [INSPIRE geoportal](https://inspire-geoportal.ec.europa.eu/) | European spatial catalogs |
| [re3data](https://www.re3data.org/) | Research data repositories |
| [Dataverse installations](https://iqss.github.io/dataverse-installations/data/data.json) | Dataverse |
| [STAC Index](https://stacindex.org/catalogs) | STAC catalogs |
| [Open Data Inception](https://data.opendatasoft.com/explore/dataset/open-data-sources%40public/information/) | Open-data portals |
| [OpenSDG community](https://open-sdg.org/community) | SDG indicator sites |
| [GBIF IPT](https://www.gbif.org/ipt) | Biodiversity IPT nodes |
| [ArcGIS Hub](https://hub.arcgis.com/) | ArcGIS Hub sites |
| [EntryScape customers](https://entryscape.com/en/customers/) | EntryScape catalogs |
| [ROAR](http://roar.eprints.org) | Open-access repositories |

Vendor “customers” and “community” pages are useful but noisy: skip demos, marketing sites, and expired domains.

## Identify the software

Choose `software.id` from `data/software/` (or `custom` if unknown). See [software-taxonomy.md](software-taxonomy.md) and [catalog-types.md](catalog-types.md).

| Platform | Typical signals | Probe (GET, timeout, public only) |
|----------|-----------------|-----------------------------------|
| CKAN | `/api/3/action/package_list`, “Powered by CKAN” | `https://host/api/3/action/status_show` |
| DKAN | CKAN-compatible API plus `/api/1/search` | same as CKAN, plus `/api/1/search` |
| Socrata | `/api/views`, `*.socrata.com` or SODA | `https://host/api/views.json?limit=1` |
| OpenDataSoft | `/api/explore/v2.1/catalog/datasets` | that path |
| GeoNetwork | `/srv/api`, `/srv/eng/csw` | CSW `GetCapabilities` |
| GeoNode | `/api/layers/`, `/api/datasets/` | `/api/layers/` |
| GeoServer | `/geoserver/ows`, `/geoserver/rest` | WMS `GetCapabilities` |
| NetGIS Server | `/keos/`, `/Netgis7` | Page title `NetGIS Server 7`; optional `wms.ashx` GetCapabilities |
| Sampaş WebGIS | `/KentrehberiApp/Index` | Page title contains `SAMPAŞ WEBGIS` |
| GiSoftGis | `/GiSoftGis/#/cityguidepublic` | Angular SPA; `gi-ajax-loading-indicator`; meta “Kent Rehberi Uygulaması” |
| BelsisIMS KRH | `ims.*/Projects/*/Pages/KRH.aspx` | ASP.NET KRH city-guide; do not confuse with Netcad Netigma |
| ArcGIS Server | `/rest/services`, `/arcgis/rest/services` | `/rest/info?f=pjson` |
| ArcGIS Hub | `/api/search/v1`, portal sharing REST | `/api/search/v1` |
| Dataverse | `/api/dataverses`, `/api/info/version` | `/api/info/version` |
| DSpace | `/oai/request?verb=Identify`, `/handle/` | OAI-PMH `Identify` |
| Invenio / Zenodo-like | `/api/records` | `/api/records?size=1` |
| PxWeb | `/api/v1/` | `/api/v1/` |
| NADA | `/index.php/api` or microdata UI | site home + API path |

Browser checks that help without an API:

- Page footer (“Powered by …”)
- HTML generator meta tags
- Network tab: calls to `/api/3`, `/srv/api`, `/rest/services`
- `robots.txt` and `/sitemap.xml` sometimes list API paths

Cross-check at least two signals before setting `software.id`. If nothing matches, use `custom` rather than guessing.

## URL patterns worth trying

Only request public URLs. Use a short timeout. Stop on `401`/`403` — do not attempt to bypass authentication.

**Open data**

- `/api/3`, `/api/3/action/package_search`
- `/data.json`, `/catalog.json`, `/catalog.xml` (DCAT)
- `/api/explore/v2.1/catalog/datasets` (OpenDataSoft)

**Geospatial**

- GeoNetwork: `/geonetwork/srv/eng/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities`
- GeoServer: `/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities`
- NetGIS Server: `/Netgis7` (version title) and `/keos/` (public KEOS viewer)
- Sampaş WebGIS: `/KentrehberiApp/Index` (title `SAMPAŞ WEBGIS`)
- GiSoftGis: `/GiSoftGis/` (city-guide hash `#/cityguidepublic`)
- BelsisIMS: `/Projects/{NAME}/Pages/KRH.aspx` on an `ims.` host
- ArcGIS: `/arcgis/rest/services?f=pjson`

**Scientific**

- OAI-PMH: `/oai/request?verb=Identify`
- Dataverse: `/api/info/version`

**Generic**

- `/api`, `/api/docs`, `/swagger.json`, `/openapi.json`

After the record exists, `scripts/apidetect.py` can fill `endpoints[]` for known platforms:

```bash
python scripts/apidetect.py detect-single {id} --dryrun
python scripts/apidetect.py detect-software ckan --dryrun
```

Drop `--dryrun` only when you intend to write YAML. Prefer `--action insert` so existing endpoints are kept.

## Automated helpers in this repository

| Tool | Use |
|------|-----|
| `python scripts/sync_ckan_ecosystem.py --dry-run` | CKAN sites from ecosystem.ckan.org; then sync without `--dry-run` into `data/scheduled/` |
| `python scripts/builder.py add-single --url … --scheduled` | Create one YAML from a verified URL |
| `python scripts/apidetect.py detect-single {id}` | Probe known API paths on an existing record |
| `python scripts/re3data_enrichment.py enrich --dry-run` | Fill `_re3data` when a re3data identifier is present |

CKAN sync details: [ckan-sync.md](ckan-sync.md). Re3Data: [re3data.md](re3data.md).

Do not write internet-wide scanners in this repository. Targeted GETs against candidate hosts, plus vendor/government lists, are enough.

## Verify before adding

1. Open the homepage. Confirm it is a catalog (search, dataset list, map layers, or metadata records).
2. Confirm `catalog_type` from [catalog-types.md](catalog-types.md). If a site is both a map viewer and an open-data portal, pick the primary product.
3. Confirm country and, for regional/local owners, the subregion folder (`US-CA/`, `FR-IDF/`, …).
4. Set `status: active` only if the site responds. Use `inactive` for dead sites you still want to record; use `scheduled` for unverified finds.
5. Record working `endpoints` when you have them. Leave `api: false` if you did not find a public API.
6. Add the record with `add-single` (recommended) or a YAML file whose filename matches `id`. Then follow [agents/contribute.md](agents/contribute.md) / CONTRIBUTING.md: `assign`, `validate-yaml --id {id}`.

## Conduct

- Respect `robots.txt` and site terms. Public catalog metadata only.
- Space out requests (about one to two seconds between hosts is enough for manual work).
- Do not collect personal data, credentials, or non-public APIs.
- Do not treat a single CSV download page as a catalog unless it is clearly a catalog UI or harvestable endpoint.

## Related

- [agents/discover.md](agents/discover.md) — agent checklist
- [agents/contribute.md](agents/contribute.md) — write YAML after a find
- [catalog-types.md](catalog-types.md)
- [software-taxonomy.md](software-taxonomy.md)
- [cli.md](cli.md)
