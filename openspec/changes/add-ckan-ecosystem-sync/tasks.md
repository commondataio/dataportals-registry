## 1. OpenSpec Proposal
- [x] 1.1 Create proposal.md
- [x] 1.2 Create tasks.md
- [x] 1.3 Create design.md
- [x] 1.4 Create specs/ckan-sync/spec.md

## 2. Implementation
- [x] 2.1 Create scripts/sync_ckan_ecosystem.py with CKAN API client
- [x] 2.2 Implement fetch_ckan_ecosystem_dataset() function
- [x] 2.3 Implement parse_ckan_site_records() function
- [x] 2.4 Implement check_existing_entries() for duplicate detection
- [x] 2.5 Implement enrich_metadata_from_web() function
- [x] 2.6 Integrate with builder.py _add_single_entry()
- [x] 2.7 Add CLI command sync-ckan-ecosystem with typer
- [x] 2.8 Add --dry-run flag support
- [x] 2.9 Add progress reporting with rich or tqdm
- [x] 2.10 Add error handling and rate limiting

## 3. Testing
- [x] 3.1 Write unit tests for CKAN API client
- [x] 3.2 Write tests for duplicate detection
- [x] 3.3 Write tests for metadata extraction
- [x] 3.4 Write integration tests for full sync workflow
- [x] 3.5 Test with --dry-run mode

## 4. Validation
- [x] 4.1 Validate new entries against catalog.json schema
- [x] 4.2 Run openspec validate add-ckan-ecosystem-sync --strict
- [x] 4.3 Test with actual ecosystem.ckan.org dataset (tested in dry-run mode, duplicate detection working correctly)
