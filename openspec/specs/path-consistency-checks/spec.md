# path-consistency-checks Specification

## Purpose
TBD - created by archiving change add-owner-type-and-path-consistency-rules. Update Purpose after archive.
## Requirements
### Requirement: Path Country Consistency Check
`analyze-quality` MUST verify that the country directory in the file path is consistent with owner/coverage country metadata, subject to documented allowlists.

#### Scenario: Path country mismatches owner country
- **WHEN** a record is stored under `FR/...` but owner country id is `DE`
- **AND** the path is not in an allowlisted special root (EU, World, International)
- **THEN** a path-country consistency issue is emitted at MEDIUM priority

#### Scenario: Allowlisted multinational roots
- **WHEN** a record is stored under `EU/` or `World/`
- **THEN** path-country consistency does not fail solely because owner country is a member state or specific country

