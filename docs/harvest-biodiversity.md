# Harvesting biodiversity and genomics catalogs

IPT, Symbiota, Living Atlases, and Ensembl publish **datasets, collections, or genome databases**. Occurrence rows, gene records, and map clicks are the wrong grain.

Overview: [harvest.md](harvest.md). Finding portals: [discovery-scientific.md](discovery-scientific.md). GET only. Stop on `401`/`403`. Prefer `endpoints[]`.

## What to keep

| Keep | Drop |
|------|------|
| IPT Darwin Core **archive** | Occurrence rows inside the archive |
| Symbiota published **dataset** (RSS) or collection (`collid`) if asked | Images, checklists, single occurrences |
| ALA **collection** / data resource | `/ws/occurrences/search` hits |
| GBIF **dataset** (`api.gbif.org`) | Occurrence search; publisher orgs as datasets |
| Ensembl **species / genome database** | Every gene, variation, or REST ping |

## GBIF IPT (`ipt`)

```text
GET https://host/inventory/dataset
GET https://host/rss.do
GET https://host/dcat
```

Each inventory/RSS item is one dataset. Prefer the IPT root on the catalog `link`. Skip harvesting gbif.org when you only needed publisher IPTs already in the registry.

## Symbiota (`symbiota`)

```text
GET https://host/collections/index.php
GET https://host/collections/datasets/rsshandler.php
```

**Keep** Darwin Core datasets (RSS). Collection-level harvest only if the user wants one record per `collid`. One portal = one harvest scope. Login-only: stop. Directory: [symbiota.org/symbiota-portals](https://symbiota.org/symbiota-portals/).

## Atlas of Living Australia (`ala`)

```text
GET https://host/ws/registry/collections
```

Harvest **collections** (data resources). Species autocomplete and occurrence search are not dataset lists. Same pattern on other Living Atlases.

## GBIF platform (`gbifplatform`)

```text
GET https://api.gbif.org/v1/dataset?limit=100&offset=0
```

Use this only when the registry record **is** GBIF (or a national GBIF portal whose API is GBIF). Filter with `publishingCountry` / `publishingOrg` when the catalog is a country node. Prefer harvesting member **IPTs** from this registry for publisher-level ids. Do not page `/v1/occurrence/search`.

## Ensembl (`ensembl`)

```text
GET https://host/info/ping
GET https://host/info/species
```

REST base is often `https://rest.ensembl.org` or `https://host/rest`. Harvest **species / assembly** databases on that taxon portal (Fungi, Protists, Metazoa, …). Do not harvest every gene. Do not clone `ensembl.org` if you only needed an existing registry row.

## SEANOE / IFREMER Catalog (`ifremercatalog`)

Marine-science dataset repository (seanoe.org). Prefer `endpoints[]` (OAI-PMH Identify is already recorded).

```text
GET https://www.seanoe.org/oai/OAIHandler?verb=Identify
GET https://www.seanoe.org/oai/OAIHandler?verb=ListRecords&metadataPrefix=oai_dc
```

Keep **datasets** (DataCite/OAI type Dataset). Drop publications mixed into the same OAI set without a type filter. Do not harvest every NetCDF file under a parent dataset. Skip cloning seanoe.org if you only needed the existing registry row.

## Related scientific IDs

| `software.id` | Harvest | Skip |
|---------------|---------|------|
| `ipt`, `symbiota`, `ala` | sections above | occurrences |
| `gbifplatform` | GBIF dataset API with a country/org filter | occurrence API |
| `ensembl` | species list on that portal | gene endpoints |
| `ifremercatalog` | SEANOE OAI/dataset list | publication mix; file-level NetCDF |

Institutional IRs that also hold Darwin Core: use [harvest-scientific.md](harvest-scientific.md) type filters, not occurrence APIs.

## Related

- [harvest.md](harvest.md)
- [harvest-scientific.md](harvest-scientific.md)
- [harvest-earthdata.md](harvest-earthdata.md)
- [harvest-protocols.md](harvest-protocols.md)
- [harvest-identifiers.md](harvest-identifiers.md)
- [harvest-output.md](harvest-output.md)
- [discovery-scientific.md](discovery-scientific.md)
- [agents/harvest.md](agents/harvest.md)
