# data-quality-reporting Specification

## Purpose
TBD - created by archiving change recalibrate-endpoint-quality-priorities. Update Purpose after archive.
## Requirements
### Requirement: Integrity Versus Enrichment Priority Tracks
The quality pipeline MUST distinguish integrity issues from enrichment-debt issues when assigning priority.

#### Scenario: API-capable software without endpoints and api not true
- **WHEN** a record has API-capable `software.id`, empty `endpoints`, and `api` is not `true`
- **AND** the catalog `link` is not already a recognized service root
- **THEN** the issue is classified as enrichment debt at MEDIUM priority
- **AND** the issue type remains `SOFTWARE_EXPECTED_ENDPOINTS_MISSING_*`

#### Scenario: Explicit api true without endpoints
- **WHEN** a record has `api: true` and empty `endpoints`
- **THEN** `MISSING_ENDPOINTS` is flagged at IMPORTANT priority as an integrity/coherence issue

#### Scenario: Service-root link exemption retained
- **WHEN** `software.id` is GeoServer or ArcGIS Server and `link` already points at a service root
- **THEN** no software-expected-endpoints issue is emitted

