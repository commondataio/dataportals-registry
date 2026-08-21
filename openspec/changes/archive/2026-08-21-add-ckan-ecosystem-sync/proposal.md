# Change: Add CKAN Ecosystem Synchronization

## Why

The CKAN ecosystem maintains a comprehensive dataset of all known CKAN installations at ecosystem.ckan.org. This dataset contains valuable metadata about CKAN sites that may not yet be in our registry. By synchronizing with this dataset, we can automatically discover new CKAN installations, enrich existing entries with additional metadata, ensure comprehensive coverage of CKAN-based data portals, and reduce manual curation effort.

## What Changes

- Add new capability to synchronize CKAN websites from ecosystem.ckan.org dataset
- Create script `scripts/sync_ckan_ecosystem.py` to fetch and process CKAN site metadata
- Implement duplicate detection to avoid adding existing entries
- Enrich metadata from both the dataset and web scraping
- Integrate with existing builder.py infrastructure for adding entries
- Add CLI command with dry-run support and progress reporting

## Impact

- Affected specs: New capability `ckan-sync` (specs/ckan-sync/spec.md)
- Affected code: 
  - New script: `scripts/sync_ckan_ecosystem.py`
  - May extend `builder.py` if additional helper functions needed
- Data: New YAML files in `data/scheduled/` or `data/entities/` directories
- No breaking changes: Pure addition, no modifications to existing functionality
