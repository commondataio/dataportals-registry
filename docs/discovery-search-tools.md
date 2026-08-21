# Search engines and internet maps

Use these tools **after** vendor and government lists ([discovery.md](discovery.md#existing-lists-start-here)). They find catalog installations that never appear on a gallery page: city CKAN sites, municipal GeoNetwork nodes, university Dataverse hosts.

This page is for **public catalog discovery** for the registry. It is not a scanner playbook. Do not write internet-wide crawlers in this repository. Query existing search indexes, then confirm each candidate with one or two public GETs.

Agent checklist: [agents/discover.md](agents/discover.md). Platform fingerprints: [opendata](discovery-opendata.md), [geoportals](discovery-geoportals.md), [scientific](discovery-scientific.md), [indicators and microdata](discovery-indicators.md).

## Workflow

1. Scope the search: one country, one city, one `software.id`, or one TLD. Unscoped queries produce more noise than this registry can review.
2. Duplicate-check exports (`datasets.duckdb` / `full.parquet`) and `data/scheduled/` **before** opening dozens of tabs. Match on hostname, not display name.
3. Run a **title / URL** query first (Google `intitle:` / `inurl:`, Censys `html_title`). Then a **body / snippet** query (`"Powered by CKAN"`, HTTP body).
4. Restrict with `site:.gov`, a national TLD, or a Censys `location.country_code`.
5. Confirm the live site with the probe table for that platform. Set `software.id` only when two signals match.
6. Add verified finds with `add-single --scheduled`. Skip demos, docs, GitHub repos, and login-only sites.

## What counts as a hit

Keep a URL when all of these are true:

- A public catalog UI or harvestable API (dataset list, map catalog, repository search, indicator tables)
- Country (and subregion for local owners) can be determined from the owner
- Software is known or explicitly `custom`

Discard documentation sites, vendor marketing, software forges, single-file download pages, expired domains, and anything that returns `401`/`403` for the catalog listing.

## Google Search

[Google](https://www.google.com) is the highest-yield first pass for named cities and government TLDs. Use [Bing](https://www.bing.com) or [DuckDuckGo](https://duckduckgo.com) with the same operators when Google rate-limits.

### Operators that matter

| Operator | Meaning | Catalog example |
|----------|---------|-----------------|
| `"exact phrase"` | Words in that order | `"Powered by CKAN"` |
| `intitle:` | Words in the HTML title | `intitle:"GeoNetwork opensource"` |
| `inurl:` | Path or host fragment | `inurl:/dataset site:.gov` |
| `intext:` | Words in the page body | `intext:"Powered by OpenDataSoft"` |
| `site:` | Host or TLD | `site:.gouv.fr données ouvertes` |
| `OR` | Either term | `"open data" OR "datos abiertos"` |
| `-term` | Exclude | `"CKAN" -site:github.com -site:ckan.org` |
| `filetype:` | File extension | `filetype:xml inurl:GetCapabilities CSW` |
| `after:` / `before:` | Date filter | `opendata after:2024-01-01` |
| `AROUND(n)` | Terms near each other | `datos AROUND(3) abiertos` |

Combine operators. A useful pattern is **phrase + path + TLD + exclusions**:

```text
"Powered by CKAN" inurl:/dataset site:.gov -site:github.com -site:ckan.org
```

### Language and TLD filters

Search in the local language. Restrict to the government or national TLD so you do not harvest every blog post about open data.

| Language | Catalog phrases | Typical `site:` |
|----------|-----------------|-----------------|
| English | open data, data portal, geoportal, data catalog | `.gov`, `.gov.uk`, `.gov.au` |
| Spanish | datos abiertos, catálogo de datos, geoportal | `.gob.*`, `.gob.es`, `.gob.mx`, `.gob.ar` |
| French | données ouvertes, catalogue de données | `.gouv.fr`, `.gc.ca` |
| German | offene daten, datenportal, geoportal | `.de`, `.gv.at`, `.admin.ch` |
| Portuguese | dados abertos, portal de dados | `.gov.br`, `.gov.pt` |
| Italian | dati aperti, catalogo dati | `.gov.it` |
| Dutch | open data, dataportaal | `.overheid.nl` |
| Russian | открытые данные, геопортал | `.gov.ru`, `.рф` |
| Chinese | 开放数据, 数据开放, 政务数据 | `.gov.cn` |
| Arabic | البيانات المفتوحة | `.gov.sa`, `.gov.eg` |
| Japanese | オープンデータ | `.go.jp`, `.lg.jp` |

City and agency names beat generic “open data” queries. Example: `datos abiertos "municipalidad" site:.gob.pe`.

### Query recipes

Copy and replace the TLD or place name. Platform-specific queries live on the platform pages; these are generic starters.

**Open data**

```text
("open data" OR opendata OR "data portal") (catalog OR datasets) site:.gov
inurl:/opendata OR inurl:/data OR inurl:/datasets site:.gouv.fr
"datos abiertos" (ayuntamiento OR municipio OR gobernación) site:.gob.mx
```

**Geoportals**

```text
(geoportal OR "geo portal" OR "spatial data" OR INSPIRE) (catalog OR metadata) site:.europa.eu
intitle:geoportal (WMS OR CSW OR GeoNetwork) site:.de
```

**Scientific repositories**

```text
("research data" OR "data repository" OR dataverse OR dspace) (datasets OR "dataverse") site:.edu
"institutional repository" (data OR research) site:.ac.uk
```

**Host patterns**

Google `site:` does not treat `data.*` as a DNS wildcard. Use `inurl:` for prefixes, or Certificate Transparency ([crt.sh](#certificate-transparency-and-dns)) for `opendata.` names:

```text
inurl:data. "open data"
inurl:opendata. OR inurl:geoportal.
inurl:hub.arcgis.com
inurl:opendatasoft.com
```

### Noise to exclude

Add these exclusions once you see the same junk in the first page of results:

```text
-site:github.com -site:gitlab.com -site:sourceforge.net
-site:stackoverflow.com -site:reddit.com -site:wikipedia.org
-site:ckan.org -site:docs.ckan.org -site:opendatasoft.com/blog
-"getting started" -documentation -"quick start" -tutorial
```

Google often ranks **dataset pages** and **news articles** above the catalog homepage. Open the site, then walk up to `/`, `/data`, `/dataset`, or `/geonetwork` until you have the catalog root. Register the catalog URL, not a single dataset.

## Censys

[Censys Platform](https://platform.censys.io) indexes hosts, certificates, and web properties. It is useful when Google does not list a site (no inbound links, robots-blocked HTML, IP-only services).

Create a free or research account. Use the **web properties** dataset for catalogs (they are websites). Use **hosts** when you need a product fingerprint on a port (GeoServer, ArcGIS Server). Use **certificates** for hostname patterns such as `opendata.*`.

Do not export huge unscoped result sets. Filter by country or software, then review hostnames one by one.

### Query language (Platform)

Censys Platform uses CenQL. `:` is tokenized full-text search. `=` is an exact match. Prefer **web properties** for catalog UIs:

| Goal | Field (web properties) | Field (hosts) |
|------|------------------------|---------------|
| HTML title | `web.endpoints.http.html_title` | `host.services.endpoints.http.html_title` |
| HTML body (first 64 KB) | `web.endpoints.http.body` | `host.services.endpoints.http.body` |
| Software product | `web.software.product` | `host.services.software.product` |
| Country | `web.location.country_code` | `host.location.country_code` |
| Hostname / name | `web.names` | `host.dns.names` |

Official syntax: [Censys Query Language](https://docs.censys.com/docs/censys-query-language). Field names change between Legacy Search and Platform; if a query returns a parse error, switch the dataset tab or check the field browser in the UI.

### Starter queries (Platform)

**Web properties — titles and body snippets**

```text
web.endpoints.http.html_title: "CKAN"
web.endpoints.http.body: "Powered by CKAN"
web.endpoints.http.html_title: "GeoNetwork"
web.endpoints.http.body: "GeoNetwork opensource"
web.endpoints.http.html_title: "Socrata"
web.endpoints.http.body: "OpenDataSoft"
web.software.product: "GeoServer"
web.names: "opendata"
```

**Hosts — products that listen on a port**

```text
host.services.software.product = "GeoServer"
host.services.software.product = "ArcGIS"
host.services: (software.product = "GeoServer" and endpoints.http.html_title: "GeoServer")
host.location.country_code = "FR" and host.services.endpoints.http.html_title: "CKAN"
```

**Certificates — hostname patterns**

```text
cert.parsed.names: "opendata"
cert.parsed.names: "geoportal"
cert.parsed.names: "data.gov"
```

Intersect with country whenever the UI allows it. Example: French CKAN-like titles:

```text
web.location.country_code = "FR" and web.endpoints.http.html_title: "données"
web.names: ".gouv.fr" and web.endpoints.http.body: "ckan"
```

### Legacy Search (older UI)

If you still have access to [search.censys.io](https://search.censys.io) Legacy Search, the equivalent fields are `services.http.response.html_title`, `services.http.response.body`, and `services.software.product`:

```text
services.http.response.html_title: "CKAN"
services.http.response.body: "Powered by CKAN"
services.software.product: GeoServer
location.country_code: FR
```

### How to turn a Censys hit into a registry URL

1. Copy the hostname (prefer the certificate or web-property name, not a raw IP).
2. Try `https://{hostname}/` first, then the platform path (`/dataset`, `/geonetwork`, `/dataverse`).
3. Duplicate-check the hostname in DuckDB.
4. Probe the public API path from the platform guide.
5. Skip hosts that only serve a login form, a default web-server page, or an internal dashboard.

Censys records IPs that may host **many** vhosts. Always confirm the catalog URL in a browser or with a GET that includes a `Host` header / HTTPS name. Do not register a bare IP as `link`.

## Shodan

[Shodan](https://www.shodan.io) is the other large internet map. Filters that help:

| Filter | Use |
|--------|-----|
| `http.title:` | HTML title |
| `http.html:` | Body snippet |
| `http.component:` | Detected component |
| `product:` | Service product |
| `org:` / `ssl:` | Organisation or cert CN |
| `country:` | ISO country |
| `hostname:` | Reverse DNS / vhost |

```text
http.title:"CKAN" country:DE
http.html:"Powered by CKAN" -http.title:"documentation"
http.title:"GeoNetwork" country:FR
product:GeoServer country:ES
http.html:"ArcGIS REST Services Directory"
http.title:"Dataverse" hostname:edu
ssl.cert.subject.CN:opendata
```

Same review rules as Censys: hostname over IP, public catalog UI, no auth bypass.

## FOFA, ZoomEye, and similar maps

These indexes are strong for East Asian and some European hosts that Google ranks poorly.

**[FOFA](https://en.fofa.info)** (example syntax):

```text
title="CKAN"
body="Powered by CKAN"
title="GeoNetwork"
app="GeoServer"
title="数据开放" && country="CN"
host="opendata"
```

**[ZoomEye](https://www.zoomeye.org)**:

```text
title:"CKAN"
http.body:"Powered by CKAN"
app:"GeoServer"
```

**[Netlas](https://app.netlas.io)** and **[Onyphe](https://www.onyphe.io)** expose similar `title` / `body` / `country` filters. Translate the same phrases; do not expect identical field names.

## URLScan, PublicWWW, and page-source search

**[urlscan.io](https://urlscan.io/search/)** searches recently crawled pages (good for new city portals):

```text
page.title:"CKAN"
page.title:"GeoNetwork"
page.url:"opendata" AND page.title:"data"
page.url:"/api/3/action" AND filename:json
```

**[PublicWWW](https://publicwww.com)** and **[nerdydata](https://www.nerdydata.com)** search HTML source across the web. They catch footer strings that Google tokenizes away:

```text
"Powered by CKAN"
"ckan.js"
"GeoNetwork opensource"
"ods-theme"
"soda.demo.socrata.com"   # exclude this; look for soda. hosts instead
"dataverse.js"
```

Export hostnames, then duplicate-check. These services are noisy: always open the live catalog.

## Certificate Transparency and DNS

Catalogs often sit on predictable names. Search Certificate Transparency rather than brute-forcing DNS.

**[crt.sh](https://crt.sh)** (SQL-like `%` wildcards):

```text
%.opendata.%
opendata.%
data.%.gov.%
geoportal.%
geonetwork.%
%.hub.arcgis.com
%.opendatasoft.com
```

**[Censys certificates](https://platform.censys.io)** and **[Cloudflare Radar / CT](https://radar.cloudflare.com)** can list the same names.

Useful hostname prefixes: `data.`, `opendata.`, `datos.`, `donnees.`, `geo.`, `geoportal.`, `metadata.`, `catalog.`, `ckan.`, `dkan.`, `gis.`, `maps.`, `indicators.`, `stats.`, `microdata.`, `nada.`.

A certificate name is not a catalog. Resolve it, then confirm a catalog UI.

## Common Crawl and web archives

When a site is gone from Google but you need the catalog root:

- [Common Crawl CDX](https://index.commoncrawl.org) — URL patterns such as `*.ckan.*`, `*/geonetwork/*`
- [Internet Archive CDX](https://web.archive.org) — `https://web.archive.org/cdx/search/cdx?url=data.example.gov/*&output=json`
- [Web Data Commons](http://webdatacommons.org/structureddata/) — `schema.org/Dataset` / DCAT pages (noisy; use as a lead list)

Prefer the live URL for `link`. Use archives only to recover a name or to mark `status: inactive`.

## Technology lookup (verify, not hunt)

Once you have a hostname, these tools confirm `software.id`. They are weak for hunting unknown sites.

| Tool | What it tells you |
|------|-------------------|
| [Wappalyzer](https://www.wappalyzer.com) / browser extension | JS frameworks, CMS, sometimes CKAN / Socrata |
| [BuiltWith](https://builtwith.com) | Similar, plus historical tech |
| Browser **View source** / Network tab | `/api/3`, `/srv/api`, `/api/explore`, `/arcgis/rest` |
| `https://host/robots.txt` and `/sitemap.xml` | Hidden API or catalog paths |
| HTTP headers | `X-Socrata-*`, `Server:`, cookies named `ckan` / `geonetwork` |

Cross-check at least two signals before setting `software.id`. If nothing matches, use `custom`.

## Official and community lists (still first)

Search engines miss less when you start from a list. Highest yield:

| Source | Typical software |
|--------|------------------|
| [CKAN ecosystem](https://ecosystem.ckan.org/dataset/ckan-sites-metadata) | `ckan` (also `scripts/sync_ckan_ecosystem.py`) |
| [Datashades](https://datashades.info/) | CKAN and others |
| [data.europa.eu catalogues](https://data.europa.eu/data/catalogues) | National EU catalogs |
| [GeoNetwork gallery](https://github.com/geonetwork/doc/blob/develop/source/annexes/gallery/gallery-urls.csv) | `geonetwork` |
| [INSPIRE geoportal](https://inspire-geoportal.ec.europa.eu/) | European SDI catalogs |
| [re3data](https://www.re3data.org/) | Scientific repositories |
| [Dataverse installations](https://iqss.github.io/dataverse-installations/data/data.json) | `dataverse` |
| [STAC Index](https://stacindex.org/catalogs) | STAC |
| [ArcGIS Hub](https://hub.arcgis.com/) | `arcgishub` |
| [Open Data Inception](https://data.opendatasoft.com/explore/dataset/open-data-sources%40public/information/) | Mixed open data |
| [ROAR](http://roar.eprints.org) | Repositories (`eprints`, `dspace`, …) |
| [GBIF IPT](https://www.gbif.org/ipt) | `ipt` |

Full inventory: README [data sources](https://github.com/datenoio/dataportals-registry/blob/main/README.md#data-sources).

## Duplicate check (do this constantly)

```sql
SELECT id, uid, name, link, catalog_type, status,
       json_extract_string(software, '$.id') AS software_id
FROM catalogs
WHERE lower(link) LIKE '%example.gov%'
   OR id = 'examplegov';
```

Match `www` vs bare host, `http` vs `https`, and `/data` vs `/`. `DUPLICATE_LINK` / `DUPLICATE_LINK_NORMALIZED` fail quality checks.

## Conduct

- Public catalog metadata only. Stop on `401`/`403`. Do not follow login forms or guess API keys.
- Space out live GETs (about one to two seconds between hosts). Search-engine queries do not hit the catalog until you verify.
- Respect `robots.txt` and site terms when you fetch the candidate itself.
- Do not collect personal data or non-public APIs.
- Do not add internet-wide scanners, mass port scans, or recursive crawlers to this repository.

## Related

- [discovery.md](discovery.md) — overview and accept/reject rules
- [discovery-opendata.md](discovery-opendata.md)
- [discovery-geoportals.md](discovery-geoportals.md)
- [discovery-scientific.md](discovery-scientific.md)
- [discovery-indicators.md](discovery-indicators.md)
- [agents/discover.md](agents/discover.md)
- [discovery-agent-tools.md](discovery-agent-tools.md)
