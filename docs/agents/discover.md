# Agent guide: discovering catalogs

Find catalog installations that are **not yet in this registry**, then hand off to [contribute.md](contribute.md). Human narrative: [discovery.md](../discovery.md). Search-engine and per-platform queries: [discovery-search-tools.md](../discovery-search-tools.md).

This is **not** the query workflow. To look up existing records, use [query.md](query.md).

## Goal

Produce a short list of verified candidate URLs with:

- `name`, `link`
- proposed `catalog_type` and `software.id`
- ISO country (and subregion when the owner is regional/local)
- whether the site already exists in exports

Do not invent `uid`. Do not add dataset-level records. Do not implement production search APIs here.

## Before probing the web

1. Read [llms.txt](https://github.com/datenoio/dataportals-registry/blob/main/llms.txt) if you have not already.
2. Duplicate-check **exports** (`data/datasets/datasets.duckdb` or `full.parquet`), then `data/scheduled/` if present.
3. If the user named a URL or domain, search that first and stop if it is already registered.

```sql
SELECT id, uid, name, link, catalog_type, status,
       json_extract_string(software, '$.id') AS software_id
FROM catalogs
WHERE lower(link) LIKE '%example.gov%'
   OR id = 'examplegov';
```

Match on hostname, not display name. `id` is not a URL.

## Discovery order

1. **Vendor and government lists** in [discovery.md](../discovery.md) and the README data-sources section — highest yield, fewest false positives.
2. **CKAN ecosystem sync** (preview only until the user wants files written):

   ```bash
   python scripts/sync_ckan_ecosystem.py --dry-run
   ```

3. **Targeted search** the user asked for (one country, one software, one city). Use local-language open-data terms and government TLDs. Query recipes: [discovery-search-tools.md](../discovery-search-tools.md) and the platform guides ([opendata](../discovery-opendata.md), [geoportals](../discovery-geoportals.md), [scientific](../discovery-scientific.md), [metadata](../discovery-metadata.md), [indicators](../discovery-indicators.md)).
4. **Endpoint probes** on the candidate host only (table below). GET, short timeout, public URLs.

You MAY run documented Google / Censys / Shodan / FOFA queries when the user asked to discover catalogs and the scope is a country, software, city, or TLD. Do not write internet-wide scanners, recursive crawlers, or unscoped sweeps in this repository. Still duplicate-check exports before probing live hosts.

## Software probes

Set `software.id` only when a probe or page signal matches. Otherwise `custom`. Definitions: `data/software/` and [software-taxonomy.md](../software-taxonomy.md).

| If you see | `software.id` (typical) | `catalog_type` (typical) |
|------------|-------------------------|--------------------------|
| `/api/3/action/package_list` or `status_show` | `ckan` | Open data portal |
| `/api/explore/v2.1/catalog/datasets` | `opendatasoft` | Open data portal |
| `/api/views` (SODA) | `socrata` | Open data portal |
| `/srv/eng/csw` or `/srv/api` | `geonetwork` | Geoportal |
| `/api/layers/` | `geonode` | Geoportal |
| `/geoserver/ows` GetCapabilities | `geoserver` | Geoportal |
| `/cgi-bin/mapserv` WMS GetCapabilities mentions MapServer | `mapserver` | Geoportal |
| `/application/` Mapbender viewer | `mapbender` | Geoportal |
| `/gvsigonline/` titled gvSIG Online (`select_public_project`) | `gvsigonline` | Geoportal |
| deegree GetCapabilities / CSW / ogcapi | `deegree` | Geoportal |
| `/keos/` city guide plus `/Netgis7` titled NetGIS Server 7 | `netgisserver` | Geoportal |
| `/KentrehberiApp/` titled SAMPAŞ WEBGIS | `sampaswebgis` | Geoportal |
| `/GiSoftGis/` Angular city guide (`gi-ajax-loading-indicator`) | `gisoftgis` | Geoportal |
| `ims.*/Projects/*/Pages/KRH.aspx` (BelsisIMS KRH) | `belsisims` | Geoportal |
| `/synserver` titled VertiGIS WebOffice (`weboffice_packed.css`, core/flex client) | `weboffice` | Geoportal |
| Title `MapTiler Server`, `/admin`, `/api/maps/{id}/style.json` (often port 3650) | `maptilerserver` | Geoportal |
| `/cadenza/`, Cadenza Web/Workbooks (`disy`, guest login, workbook navigator) | `cadenza` | Geoportal |
| Geospatial Portal UI (`Version:`/`Licensed to:`, `Intergraph.WebSolutions`, `$GP.`) e.g. `/geoportal01/`, `/cdngiportal/`, `/msip/Full.aspx`, `/Online_Mapping/` | `geomediawebmap` | Geoportal |
| `/rest/info?f=pjson` ArcGIS Server | `arcgisserver` | Geoportal |
| ArcGIS Hub search API / hub site | `arcgishub` | Geoportal or Open data portal (primary UI) |
| `/IdraPortal/` or `/Idra/api/v1/` | `idra` | Data search engine |
| FAIR Data Point RDF DCAT (`fdp-client`, Turtle/JSON-LD at `/`) | `fairdatapoint` | Metadata catalog |
| Aristotle MDR public registry / data elements | `aristotlemdr` | Metadata catalog |
| Fusion Registry SDMX structural metadata | `fusionregistry` | Metadata catalog |
| MetadataWorks Metadata Browser | `mwmb` | Metadata catalog |
| `www2.wagmap.jp` / わが街ガイド / GeoAccessJS | `wagmap` | Geoportal |
| `geoportal2.pl` / EWMAPA / GEOBID | `ewmapa` | Geoportal |
| GeoMapFish `ngeo` / `/themes` | `geomapfish` | Geoportal |
| 天地图 / Tianditu node | `tianditu` | Geoportal |
| Masterportal / LGV viewer | `masterportal` | Geoportal |
| wis2box / WIS2 node | `wis20box` | Geoportal |
| `/oportal/` Inspur catalog | `oportal` | Open data portal |
| `data.gov.in` OGD tenant | `ogdindia` | Open data portal |
| Liferay RISP / datos abiertos listing (not a CMS homepage) | `liferay` | Open data portal |
| Digital Commons / bepress IR | `elsevierdigitalcommons` | Scientific data repository |
| InstDB / FairStack node | `instdb` | Scientific data repository |
| WEKO3 institutional repository | `weko3` | Scientific data repository |
| OPeNDAP / Hyrax directory (not THREDDS-only) | `opendap` | Scientific data repository |
| OBiBa Mica study catalog | `obibamica` | Microdata catalog |
| `/api/info/version` Dataverse | `dataverse` | Scientific data repository |
| OAI-PMH `Identify` + DSpace UI | `dspace` | Scientific data repository |
| OPUS 4 `/oai?verb=Identify` | `opus` | Scientific data repository |
| CONTENTdm `/digital/api/collections` (dataset collections only) | `contentdm` | Scientific data repository or Indicators catalog |
| Omeka S `/api/items` JSON-LD (dataset catalogs, not exhibit-only) | `omekas` | Scientific data repository or Open data portal |
| Fedora `/fcrepo/rest` as the public catalog (else use Hyrax/Islandora/PHAIDRA) | `fedora` | Scientific data repository |
| PHAIDRA `/api/oai` or `/api/search/select` | `phaidra` | Scientific data repository |
| Esploro research-outputs portal (`esploro.exlibrisgroup.com`) | `esploro` | Scientific data repository |
| `/api/v1/` PxWeb | `pxweb` | Indicators catalog |

Types: [catalog-types.md](../catalog-types.md). If a site is both a map viewer and a dataset portal, pick the **primary** product.

After a YAML file exists, optional endpoint fill:

```bash
python scripts/apidetect.py detect-single catalogdatagov --dryrun
```

See [apidetect.md](../apidetect.md). Do not run `apidetect_urlmaps_draft.py` as a CLI.

## Accept / reject

**Accept** when all are true:

- Public HTTP(S) catalog UI or harvestable API
- Not a duplicate of `link` / same host catalog already in entities or scheduled
- Country (and subregion) can be determined from the owner
- Software is known or explicitly `custom`

**Reject** (do not add):

- Demo, template, or documentation-only sites
- Single file downloads with no catalog
- Sites that require authentication for any catalog listing
- Dataset records, CKAN packages, STAC items (out of scope)
- Guessed software IDs

## After a valid find

1. `python scripts/builder.py add-single URL --scheduled` (preferred) or write YAML per [contribute.md](contribute.md).
2. `python scripts/builder.py assign`
3. `python scripts/builder.py validate-yaml --id` for that catalog id
4. Cite `id` + `link` in the reply. List skipped duplicates with their existing `id`.

## Do not

- Walk `data/entities/**/*.yaml` to search; use exports
- Hand-edit `data/datasets/`
- Bypass `401`/`403`, guess API keys, or follow login forms
- Flood a host; one or two GETs per path is enough
- Commit generated dumps unless the user asked for a rebuild

## Related

- [discovery.md](../discovery.md)
- [discovery-search-tools.md](../discovery-search-tools.md)
- [discovery-agent-tools.md](../discovery-agent-tools.md)
- [discovery-opendata.md](../discovery-opendata.md) / [discovery-geoportals.md](../discovery-geoportals.md) / [discovery-scientific.md](../discovery-scientific.md) / [discovery-metadata.md](../discovery-metadata.md) / [discovery-indicators.md](../discovery-indicators.md) / [discovery-other.md](../discovery-other.md)
- [apidetect.md](../apidetect.md) / [liveness.md](../liveness.md)
- [contribute.md](contribute.md)
- [query.md](query.md)
- [cli.md](../cli.md)
