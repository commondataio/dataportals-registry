# Agent guide: contributing catalog records

Platform-neutral workflow for adding or editing catalog YAML. Full human guide: [CONTRIBUTING.md](https://github.com/datenoio/dataportals-registry/blob/main/CONTRIBUTING.md).

## Before editing

1. Search exports (and `data/scheduled/`) so you do not duplicate `link` / `id`. Finding candidates: [discover.md](discover.md).
2. Read [directory-layout.md](../directory-layout.md) and [data-model.md](../data-model.md).
3. Consumers querying data should use [ai-consumers.md](../ai-consumers.md) — do not parse YAML unless authoring.

## Source layout

| Path | Rule |
|------|------|
| `data/entities/{CC}/{Federal\|SUB}/{type}/{id}.yaml` | Verified records; filename = `id` |
| `data/scheduled/` | Unverified; promote later ([scheduled.md](../scheduled.md)) |
| `data/software/` | Platform definitions ([software-taxonomy.md](../software-taxonomy.md)) |
| `data/datasets/` | **Generated only** — never hand-edit |

## New catalog checklist

- Prefer CLI:

  ```bash
  python scripts/builder.py add-single \
    --url "https://example.com/data" \
    --software ckan \
    --catalog-type "Open data portal" \
    --name "Example Data Portal" \
    --country US \
    --scheduled
  ```

- Filename / `id`: lowercase letters and digits only
- Required fields: `id`, `uid`, `name`, `link`, `catalog_type`, `access_mode`, `status`, `software`, `owner`, `coverage`
- **Do not** invent `uid`. Run `python scripts/builder.py assign`
- `owner.type` from `data/reference/owner_types.yaml`
- `software.id` from `data/software/` (or `custom`)
- Path country must match `owner.location.country.id` / coverage country
- Regional/local owners: `owner.location.level` 30 and a subregion folder (`US-CA/`, …)

## After editing

```bash
python scripts/builder.py validate-yaml --id {id}
python scripts/builder.py assign
pytest tests/test_yaml.py -q
```

Validate a single file with `--file path/to/file.yaml`. Run full `validate-yaml` before a large PR.

## Do not

- Commit generated `data/datasets/` dumps unless the change is an intentional rebuild
- Put secrets in YAML
- Implement schema or pipeline changes without OpenSpec — see [openspec-quickstart.md](openspec-quickstart.md)

## Quality

If `analyze-quality` flags the record, follow [metadata-quality.md](../metadata-quality.md) and [quality-rules.md](../quality-rules.md). Integrity-track issues (invalid enums, duplicates, path mismatches) must be fixed; enrichment-track gaps are optional in the same PR.

New shared platforms: [software-taxonomy.md](../software-taxonomy.md#adding-a-software-definition).
