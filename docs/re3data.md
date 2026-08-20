# Re3Data enrichment

Catalogs with a re3data identifier can be enriched from [re3data.org](https://www.re3data.org). The script writes an optional `_re3data` object and does not overwrite core fields (`name`, `link`, `software`, …).

## Commands

```bash
python scripts/re3data_enrichment.py fetch --id r3d100010078
python scripts/re3data_enrichment.py fetch --all --limit 20
python scripts/re3data_enrichment.py enrich --dry-run
python scripts/re3data_enrichment.py enrich
python scripts/re3data_enrichment.py enrich --force --delay 1.5
```

Cache: `data/cache/re3data_repositories.json`.

## `_re3data` payload

Typical keys: `re3data_id`, `keywords`, `content_type`, `contact_email`, `description`, `persistent_identifiers`, `software`, `versioning`, `institutions`, `repository_type`, `subjects`, `database_access`, `data_access`, `open_access`, `database_licenses`, `data_policy`, `privacy_policy`, `standards`, `certifications`, `apis`, `protocols`, `last_updated`.

JSON-LD maps `_re3data` to `cdi:re3dataEnrichment`. Underscore-prefixed keys can be dropped by some JSON tools; use the export field as stored.

## When to run

After adding scientific repositories that already have `identifiers` with `id: re3data`. Not required for open-data portals without a re3data record.

Working notes: [dev/docs/re3data_enrichment.md](https://github.com/datenoio/dataportals-registry/blob/main/dev/docs/re3data_enrichment.md).
