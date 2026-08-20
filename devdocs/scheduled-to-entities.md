# Promoting scheduled entries to entities

Canonical published guide: [docs/scheduled.md](../docs/scheduled.md).

Catalogs in **data/scheduled/** are unverified. After review, promote them to **data/entities/** so they appear in the main catalogs export.

## Script

```bash
python scripts/promote_scheduled.py --dry-run
python scripts/promote_scheduled.py
python scripts/builder.py assign
python scripts/builder.py validate-yaml
```

`promote_scheduled.py` moves each scheduled YAML to `data/entities/{country}/Federal/{type}/`, sets `status` to `active` (or `inactive` for staging/demo hosts), and skips IDs that already exist as entities.

## Manual promotion

Move the file to the correct `data/entities/{COUNTRY}/{Federal|SUBREGION}/{type}/` path, ensure the filename matches `id`, then run `assign` and `validate-yaml`.
