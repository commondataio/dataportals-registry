# CKAN Ecosystem Synchronization

## Overview

The CKAN ecosystem synchronization script (`scripts/sync_ckan_ecosystem.py`) automatically discovers and synchronizes CKAN websites from the official [CKAN ecosystem dataset](https://ecosystem.ckan.org/dataset/ckan-sites-metadata) hosted at ecosystem.ckan.org.

## Features

- **Automatic Discovery**: Fetches CKAN site metadata from ecosystem.ckan.org via CKAN API
- **Duplicate Detection**: Intelligently detects existing entries by URL/domain matching
- **Metadata Enrichment**: Enriches metadata from both the dataset and web scraping
- **Integration**: Uses existing `builder.py` infrastructure for consistent entry creation
- **Dry Run Mode**: Safe testing without making changes
- **Error Handling**: Graceful handling of API failures, timeouts, and invalid URLs
- **Rate Limiting**: Configurable delays between requests to respect server limits

## Usage

### Basic Usage

```bash
# Preview what would be added (dry run)
python scripts/sync_ckan_ecosystem.py --dry-run

# Sync and add missing CKAN sites to scheduled directory
python scripts/sync_ckan_ecosystem.py

# Sync and add directly to entities directory
python scripts/sync_ckan_ecosystem.py --entities
```

### Advanced Options

```bash
# Customize delay between requests (default: 1.0 seconds)
python scripts/sync_ckan_ecosystem.py --delay 2.0

# Disable web scraping enrichment (use only dataset metadata)
python scripts/sync_ckan_ecosystem.py --no-enrich

# Combine options
python scripts/sync_ckan_ecosystem.py --dry-run --delay 1.5 --no-enrich
```

## How It Works

1. **Fetch Dataset**: Connects to ecosystem.ckan.org CKAN API and fetches CKAN site records
2. **Load Existing Entries**: Loads all existing registry entries to check for duplicates
3. **Parse Records**: Extracts URL, name, description, location, and owner information
4. **Check Duplicates**: Normalizes URLs and checks against existing entries
5. **Enrich Metadata**: Optionally scrapes websites for additional metadata
6. **Add Entries**: Uses `builder.py` `_add_single_entry()` to create new registry entries

## Duplicate Detection

The script uses multiple strategies to detect duplicates:

- **URL Normalization**: Removes protocol (http/https), www prefix, trailing slashes
- **Domain Matching**: Matches by normalized domain name
- **ID Generation**: Checks if generated ID would conflict with existing entries

## Metadata Sources

### From Dataset
- URL
- Name/Title
- Description
- Location/Country (from tags)
- Owner/Organization information

### From Web Scraping (optional)
- Meta description
- Open Graph description
- Page title
- Organization name from page content

## Output

The script provides detailed logging:

- Number of existing entries loaded
- Number of CKAN sites found
- Duplicate detection results
- Summary statistics (added, skipped, errors)

Example output:
```
2026-02-06 14:42:37,283 - __main__ - INFO - Starting CKAN ecosystem synchronization...
2026-02-06 14:42:37,709 - __main__ - INFO - Loaded 12756 existing IDs and 18895 existing URLs/domains
2026-02-06 14:42:37,726 - __main__ - INFO - Fetching CKAN sites from ecosystem.ckan.org...
2026-02-06 14:42:38,521 - __main__ - INFO - Found dataset: ckan-sites-metadata
2026-02-06 14:46:55,339 - __main__ - INFO - Total unique records found: 4
2026-02-06 14:46:55,342 - __main__ - INFO - Skipping duplicate: https://ckan.americaview.org (existing: ckanamericavieworg)
...
2026-02-06 14:46:55,342 - __main__ - INFO - ============================================================
2026-02-06 14:46:55,342 - __main__ - INFO - Synchronization complete!
2026-02-06 14:46:55,342 - __main__ - INFO - Added: 0
2026-02-06 14:46:55,342 - __main__ - INFO - Skipped (duplicates): 4
2026-02-06 14:46:55,342 - __main__ - INFO - Errors: 0
```

## Integration with Registry

New entries are created using the same infrastructure as manual additions:

- Uses `ENTRY_TEMPLATE` from `constants.py`
- Follows same file organization (country/type subdirectories)
- Sets `status: "scheduled"` by default (or `"active"` if using `--entities`)
- Automatically detects API endpoints using `apidetect.py`
- Validates against `catalog.json` schema

## Error Handling

The script handles various error conditions gracefully:

- **API Failures**: Logs error and continues with available data
- **Invalid URLs**: Skips entries without valid URLs
- **Network Timeouts**: Uses configurable timeouts and retries
- **Parsing Errors**: Logs errors and skips problematic records

## Rate Limiting

To avoid overwhelming servers, the script includes:

- Configurable delay between requests (`--delay` option)
- Default 1.0 second delay
- Separate delays for API calls and web scraping

## Testing

Unit tests are available in `tests/test_sync_ckan_ecosystem.py`:

```bash
python -m pytest tests/test_sync_ckan_ecosystem.py -v
```

Tests cover:
- URL normalization
- Duplicate detection
- Metadata extraction
- Web scraping enrichment
- Integration scenarios

## Related Documentation

- [OpenSpec Proposal](../openspec/changes/add-ckan-ecosystem-sync/proposal.md)
- [Design Document](../openspec/changes/add-ckan-ecosystem-sync/design.md)
- [Specification](../openspec/changes/add-ckan-ecosystem-sync/specs/ckan-sync/spec.md)
