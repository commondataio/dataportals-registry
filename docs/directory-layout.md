# Directory layout

Catalog files are organized by **country**, optional **subregion**, and **catalog type**.

```
data/entities/
├── US/
│   ├── Federal/opendata/catalogdatafaagov.yaml
│   └── US-CA/geo/...
├── FR/
│   └── Federal/opendata/...
└── ...
data/scheduled/          # unverified records pending promotion
data/software/           # platform definitions under opendata/, geo/, scientific/, indicators/, microdata/, metadata/
data/schemes/            # Cerberus + JSON Schema
data/reference/          # controlled vocabularies
data/datasets/           # generated exports (do not edit)
```

## Path rules

| Segment | Rule |
|---------|------|
| Country folder | ISO 3166-1 alpha-2 (`US`, `FR`) or special roots (`World`, `EU`, `Africa`) |
| `Federal/` | National / central government catalogs |
| Subregion folder | ISO 3166-2 style (`US-CA`, `GB-SCT`, `BR-SP`) |
| Type folder | See [catalog-types.md](catalog-types.md) |
| Filename | Must equal `id` + `.yaml` |

## Type folders

| Folder | `catalog_type` |
|--------|----------------|
| `opendata/` | Open data portal |
| `geo/` | Geoportal |
| `scientific/` | Scientific data repository |
| `indicators/` | Indicators catalog |
| `microdata/` | Microdata catalog |
| `ml/` | Machine learning catalog |
| `search/` | Data search engine |
| `api/` | API Catalog |
| `marketplace/` | Data marketplace |
| `metadata/` | Metadata catalog |
| `other/` | Other / Datasets list / General research repository |

## File naming

- Lowercase letters and digits only
- Strip dots, dashes, and underscores from the host
- Example: `https://catalog.data.faa.gov` → `id: catalogdatafaagov` → `catalogdatafaagov.yaml`

## UID

- Entities: `cdi########` assigned by `python scripts/builder.py assign`
- Scheduled: `temp########`
- Do **not** invent UIDs by hand

## Generated vs source

| Edit | Do not edit |
|------|-------------|
| `data/entities/**/*.yaml` | `data/datasets/**` |
| `data/scheduled/**/*.yaml` | `dataquality/**` (except when regenerating reports) |
| `data/software/**/*.yaml` | |
| `data/reference/**` | |

New catalog files: [discovery.md](discovery.md) then [agents/contribute.md](agents/contribute.md). Dataset crawl recipes (not YAML in this repo): [harvest.md](harvest.md). Levels, identifiers, endpoints: [vocabularies.md](vocabularies.md). Promote scheduled: [scheduled.md](scheduled.md).
