## ADDED Requirements

### Requirement: Integrity-Focused Regression Guard
CI MUST fail on growth in integrity issue counts and MUST NOT treat enrichment backlog growth as a hard failure by default.

#### Scenario: Integrity CRITICAL or IMPORTANT increases
- **WHEN** a PR increases integrity-track CRITICAL or IMPORTANT counts above baseline
- **THEN** the quality regression test fails
- **AND** the failure message lists the top growing integrity issue types

#### Scenario: Enrichment-only increase
- **WHEN** only enrichment-track issue counts increase (e.g. software-expected endpoints)
- **THEN** the regression test does not fail by default
- **AND** the report may still surface the enrichment delta for visibility

### Requirement: Baseline Captures Track Metadata
The quality baseline MUST identify which issue types belong to the integrity track used by CI.

#### Scenario: Baseline lists integrity issue types or track counts
- **WHEN** a maintainer inspects `dataquality/baseline_counts.json` after this change
- **THEN** the baseline includes enough metadata to evaluate integrity growth separately from enrichment growth
