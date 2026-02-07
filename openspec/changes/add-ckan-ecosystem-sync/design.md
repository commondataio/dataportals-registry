# Design: CKAN Ecosystem Synchronization

## Context

The CKAN ecosystem dataset at https://ecosystem.ckan.org/dataset/ckan-sites-metadata is itself a CKAN instance. We need to fetch CKAN site metadata from this dataset, check against existing registry entries, and add missing sites with enriched metadata.

## Goals

- Automatically discover CKAN sites from ecosystem.ckan.org
- Avoid duplicate entries in the registry
- Enrich metadata from multiple sources (dataset + web scraping)
- Integrate seamlessly with existing registry infrastructure
- Provide dry-run capability for safe testing

## Non-Goals

- Modifying existing registry entries (only adding new ones)
- Real-time synchronization (this is a batch operation)
- Handling non-CKAN sites from the dataset

## Decisions

### Decision: Use CKAN API to fetch dataset

**What**: Access ecosystem.ckan.org via CKAN API (`/api/3/action/package_search` or `package_list`)

**Why**: 
- Standard CKAN API is reliable and well-documented
- Can fetch all records programmatically
- Supports pagination for large datasets

**Alternatives considered**:
- Web scraping: More fragile, harder to maintain
- Direct database access: Not available, would require special permissions

### Decision: Match duplicates by normalized URL

**What**: Normalize URLs by domain (remove protocol, www, trailing slashes) and match against existing registry entries

**Why**:
- URLs are the primary identifier for CKAN sites
- Domain-based matching catches variations (http/https, www/non-www)
- Existing registry uses domain-based `id` generation

**Alternatives considered**:
- Exact URL matching: Too strict, misses variations
- Name-based matching: Too fuzzy, prone to false positives

### Decision: Add entries with status "scheduled" initially

**What**: New entries added with `status: "scheduled"` following existing pattern

**Why**:
- Consistent with existing workflow
- Allows for review before promoting to "active"
- Matches behavior of other import scripts

**Alternatives considered**:
- Adding directly as "active": Bypasses review process
- Custom status: Inconsistent with existing status values

### Decision: Dataset metadata takes precedence, web scraping fills gaps

**What**: Use metadata from ecosystem.ckan.org dataset first, then enrich with web scraping for missing fields

**Why**:
- Dataset metadata is curated and reliable
- Web scraping is fallback for missing information
- Reduces unnecessary web requests

**Alternatives considered**:
- Web scraping only: More requests, slower, less reliable
- Dataset only: May miss valuable metadata available on websites

### Decision: Use existing builder.py infrastructure

**What**: Leverage `_add_single_entry()` function from builder.py

**Why**:
- Reuses proven code
- Maintains consistency with other import methods
- Handles file organization, ID generation, validation

**Alternatives considered**:
- New implementation: Duplicates logic, harder to maintain
- Direct YAML writing: Bypasses validation and organization logic

## Risks / Trade-offs

### Risk: API rate limiting
**Mitigation**: Add delays between requests, respect rate limits, implement retry logic

### Risk: Invalid or inaccessible URLs
**Mitigation**: Graceful error handling, skip invalid entries, log errors for review

### Risk: Duplicate detection false negatives
**Mitigation**: Use multiple matching strategies (URL, domain, name), log potential duplicates for manual review

### Risk: Large dataset processing time
**Mitigation**: Progress reporting, ability to resume, batch processing

## Migration Plan

1. Create script with dry-run mode
2. Test with small subset of data
3. Run full sync in dry-run mode to review
4. Execute actual sync
5. Review added entries
6. Promote reviewed entries from "scheduled" to "active" if needed

## Open Questions

- What fields are available in the ecosystem.ckan.org dataset?
- How frequently should this sync run?
- Should we update existing entries if dataset has newer metadata?
