# CKAN ecosystem sync

`scripts/sync_ckan_ecosystem.py` imports CKAN sites from the [CKAN ecosystem dataset](https://ecosystem.ckan.org/dataset/ckan-sites-metadata). Duplicate `link` values are skipped. New records default to `data/scheduled/`.

## Commands

```bash
python scripts/sync_ckan_ecosystem.py --dry-run
python scripts/sync_ckan_ecosystem.py
python scripts/sync_ckan_ecosystem.py --entities
python scripts/sync_ckan_ecosystem.py --delay 2.0 --no-enrich
```

| Flag | Effect |
|------|--------|
| `--dry-run` | Log candidates; write nothing |
| `--scheduled` / `--entities` | Target directory (default scheduled) |
| `--enrich` / `--no-enrich` | Scrape title/description from the live site |
| `--delay` | Seconds between HTTP requests (default `1.0`) |

## What it does

1. Fetches CKAN site records from ecosystem.ckan.org.
2. Loads existing registry ids/URLs from exports.
3. Normalizes URLs (scheme, `www`, trailing slash) and skips duplicates.
4. Optionally scrapes the public homepage for name/description.
5. Calls the same `add-single` path as the CLI.

After a real sync, run `python scripts/builder.py assign` and `validate-yaml`. Promote reviewed files with [scheduled.md](scheduled.md). Finding catalogs in general: [discovery.md](discovery.md).

Maintainer notes: [devdocs/ckan_ecosystem_sync.md](https://github.com/datenoio/dataportals-registry/blob/main/devdocs/ckan_ecosystem_sync.md).
