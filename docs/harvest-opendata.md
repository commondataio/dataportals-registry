# Harvesting datasets from open data portals

CKAN, OpenDataSoft, Socrata, and similar portals already treat **datasets** (packages, views) as the primary object. You rarely need the publication-type filters used for [scientific repositories](harvest-scientific.md). You still must avoid harvesting the wrong object (resources, showcases, harvest sources, individual files).

Overview: [harvest.md](harvest.md). Finding portals: [discovery-opendata.md](discovery-opendata.md).

Replace `https://host` with the catalog origin. GET only. Stop on `401`/`403`. Prefer URLs already in `endpoints[]`.

## What to keep

| Keep | Drop |
|------|------|
| Dataset / package / view / explore dataset | A file **resource** (CSV) as if it were a separate catalog record |
| The dataset landing API object | Harvest **source** metadata (the remote catalog CKAN is pulling from) |
| | Showcase, article, blog, app gallery, idea box |
| | Organization or group objects without a dataset list |

One dataset with five CSV resources is **one** dataset.

## CKAN (`ckan`) and DKAN (`dkan`)

**Search (preferred):**

```text
GET https://host/api/3/action/package_search?q=&rows=100&start=0
```

Use `start` += `rows` until `count` is reached. Each `results[]` element is a dataset.

**Optional filter** when the site mixes types:

```text
GET https://host/api/3/action/package_search?fq=dataset_type:dataset&rows=100
```

Drop `dataset_type:showcase` (ckanext-showcase), harvest objects, and `type:harvest`.

`package_list` returns names only and is painful on large sites — prefer `package_search`.

DKAN: same CKAN-style actions when enabled; also `/api/1/search`. Confirm JSON `"success": true`.

DataPress (`datapress`) is CKAN plus CMS — harvest `package_search`, not CMS pages.

## OpenDataSoft (`opendatasoft`)

Already a dataset catalog:

```text
GET https://host/api/explore/v2.1/catalog/datasets?limit=100&offset=0
```

Follow `links` / offset until `total_count`. Do not harvest the vendor academy or `www.opendatasoft.com`.

## Socrata (`socrata`)

Views include charts, maps, files, and stories.

```text
GET https://host/api/catalog/v1?only=datasets&limit=100&offset=0
```

Legacy: `/api/views.json` mixes types — filter `viewType` / `displayType` to tabular datasets, or use the catalog API `only=datasets`. Drop `only=stories`, `only=filters`.

## uData (`udata`)

```text
GET https://host/api/1/datasets/?page_size=100&page=1
```

Do not page `/api/1/reuses/` or `/api/1/posts/` as datasets.

## Magda (`magda`), JKAN (`jkan`), Junar (`junar`)

Use the dataset search/list endpoint from `endpoints[]`. Magda: catalog search API. JKAN is often static JSON in the repo — harvest the published `datasets.json` / equivalent, not GitHub issues. Junar: dataset API on the tenant, not junar.com marketing.

## EntryScape (`entryscape`), Piveau (`piveau`), Idra (`idra`)

DCAT-AP catalogs. Harvest **Dataset** / `dcat:Dataset` only.

- EntryScape: public DCAT/search API on the tenant.
- Piveau: DCAT-AP search; skip the hub UI chrome.
- Idra: federation of other catalogs (`catalog_type` is often Data search engine). Harvesting Idra duplicates member catalogs — prefer harvesting the **source** portals from this registry unless you need the federation view.

## ArcGIS Hub (`arcgishub`) as open data

```text
GET https://host/api/search/v1
```

Keep dataset / feature layer **items** that are public data. Drop StoryMaps, sites, and applications unless you have a separate apps index.

## Portals without a dataset API

Liferay, Drupal, WordPress, Bitrix, Our Open Data, oPortal, OGD India, Seoul plaza: many have HTML catalogs and only a partial API. Harvest DCAT (`/catalog.xml`, `/data.json`) when present. If there is no machine list, do not scrape every CMS page from this repository’s workflows.

## Related

- [harvest.md](harvest.md)
- [harvest-scientific.md](harvest-scientific.md)
- [harvest-geoportals.md](harvest-geoportals.md)
- [discovery-opendata.md](discovery-opendata.md)
- [apidetect.md](apidetect.md)
