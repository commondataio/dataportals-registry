# Quick Start: CKAN Ecosystem Sync

## Quick Reference

### Basic Commands

```bash
# See what would be added (safe, no changes)
python scripts/sync_ckan_ecosystem.py --dry-run

# Sync and add to scheduled directory (for review)
python scripts/sync_ckan_ecosystem.py

# Sync and add directly to entities (production)
python scripts/sync_ckan_ecosystem.py --entities
```

### Common Options

- `--dry-run` - Preview mode, no files created
- `--entities` - Add directly to entities (default: scheduled)
- `--delay 2.0` - Custom delay between requests
- `--no-enrich` - Skip web scraping, use dataset only

### What It Does

1. Fetches CKAN sites from ecosystem.ckan.org
2. Checks for duplicates in existing registry
3. Enriches metadata from dataset + web scraping
4. Adds missing sites using standard registry format

### Expected Output

```
Starting CKAN ecosystem synchronization...
Loaded 12756 existing IDs and 18895 existing URLs/domains
Fetching CKAN sites from ecosystem.ckan.org...
Found dataset: ckan-sites-metadata
Total unique records found: X
Added: Y
Skipped (duplicates): Z
Errors: 0
```

### Troubleshooting

**No records found?**
- Check internet connection
- Verify ecosystem.ckan.org is accessible
- Try increasing delay: `--delay 2.0`

**Too many duplicates?**
- This is normal - means registry is up to date!
- Check logs to see which sites were skipped

**Timeout errors?**
- Increase delay: `--delay 2.0` or `--delay 3.0`
- Disable enrichment: `--no-enrich`

### Files Created

New entries are created in:
- `data/scheduled/` (default) - for review
- `data/entities/` (with `--entities`) - production

### Next Steps After Sync

1. Review added entries in `data/scheduled/`
2. Validate: `python scripts/builder.py validate-yaml`
3. Promote reviewed entries from scheduled to entities
4. Run data quality checks

### Documentation

- Full docs: `devdocs/ckan_ecosystem_sync.md`
- OpenSpec: `openspec/changes/add-ckan-ecosystem-sync/`
