# CKAN Ecosystem Synchronization

## ADDED Requirements

### Requirement: CKAN Ecosystem Dataset Fetching

The system SHALL fetch CKAN site metadata from the ecosystem.ckan.org dataset via CKAN API.

#### Scenario: Successfully fetch dataset
- **WHEN** the sync script is executed
- **THEN** it connects to https://ecosystem.ckan.org/api/3/action/package_search
- **AND** retrieves all CKAN site records from the dataset
- **AND** handles pagination if the dataset is large

#### Scenario: Handle API errors gracefully
- **WHEN** the CKAN API is unavailable or returns an error
- **THEN** the script logs the error
- **AND** exits gracefully without adding any entries

### Requirement: Duplicate Detection

The system SHALL detect existing CKAN sites in the registry to avoid duplicates.

#### Scenario: Detect duplicate by URL
- **WHEN** a CKAN site URL from the dataset matches an existing registry entry
- **THEN** the system identifies it as a duplicate
- **AND** skips adding it to the registry
- **AND** logs the duplicate detection

#### Scenario: Detect duplicate by normalized domain
- **WHEN** a CKAN site URL normalizes to the same domain as an existing entry
- **THEN** the system identifies it as a duplicate
- **AND** handles URL variations (http/https, www/non-www, trailing slashes)

### Requirement: Metadata Extraction

The system SHALL extract relevant metadata from CKAN dataset records.

#### Scenario: Extract basic metadata
- **WHEN** processing a CKAN site record from the dataset
- **THEN** the system extracts URL, name, and description if available
- **AND** extracts location/geographic information if present
- **AND** extracts owner/organization information if available

### Requirement: Web Scraping Enrichment

The system SHALL enrich metadata by scraping CKAN websites when dataset metadata is incomplete.

#### Scenario: Enrich missing description
- **WHEN** a CKAN site record lacks a description
- **THEN** the system attempts to scrape the website
- **AND** extracts description from page metadata or content
- **AND** adds it to the registry entry

#### Scenario: Enrich missing owner information
- **WHEN** a CKAN site record lacks owner information
- **THEN** the system attempts to infer owner from website content
- **AND** extracts organization name and type if available

### Requirement: Registry Entry Creation

The system SHALL create registry entries for new CKAN sites using existing infrastructure.

#### Scenario: Add new CKAN site
- **WHEN** a CKAN site is not found in the registry
- **THEN** the system creates a new YAML entry using builder.py _add_single_entry()
- **AND** sets software to "ckan"
- **AND** sets catalog_type to "Open data portal" (or appropriate type)
- **AND** sets status to "scheduled"
- **AND** validates the entry against catalog.json schema

#### Scenario: Detect API endpoints
- **WHEN** creating a new CKAN site entry
- **THEN** the system uses apidetect.py to detect CKAN API endpoints
- **AND** adds detected endpoints to the entry

### Requirement: CLI Interface

The system SHALL provide a CLI command for synchronizing CKAN ecosystem sites.

#### Scenario: Execute sync with dry-run
- **WHEN** the sync command is executed with --dry-run flag
- **THEN** it processes all sites and checks for duplicates
- **AND** reports what would be added
- **AND** does not create any files

#### Scenario: Execute sync normally
- **WHEN** the sync command is executed without --dry-run
- **THEN** it processes sites and adds missing ones to the registry
- **AND** creates YAML files in appropriate directories
- **AND** reports progress and summary statistics

#### Scenario: Handle rate limiting
- **WHEN** making API requests or web scraping
- **THEN** the system adds delays between requests
- **AND** respects rate limits to avoid overwhelming servers
