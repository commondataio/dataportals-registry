# quality-regression-guard Specification

## Purpose
TBD - created by archiving change recalibrate-endpoint-quality-priorities. Update Purpose after archive.
## Requirements
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

### Requirement: Quality Count Baseline
The project MUST maintain a machine-readable baseline of quality issue counts.

#### Scenario: Baseline file present
- **WHEN** a developer inspects `dataquality/baseline_counts.json`
- **THEN** it contains counts for CRITICAL and IMPORTANT priority tiers
- **AND** includes a `generated_at` timestamp and registry record count

### Requirement: CI Regression Guard
Pull requests MUST NOT merge if they increase high-priority quality issue counts beyond the baseline.

#### Scenario: PR introduces new CRITICAL issues
- **WHEN** a PR changes catalog YAML and `primary_priority.jsonl` CRITICAL count exceeds baseline
- **THEN** the quality regression test fails
- **AND** CI reports the delta by issue type

#### Scenario: PR fixes issues without regression
- **WHEN** a PR reduces or maintains CRITICAL and IMPORTANT counts
- **THEN** the quality regression test passes

### Requirement: Baseline Update Procedure
Maintainers MUST be able to update the baseline after intentional bulk remediation.

#### Scenario: Maintainer updates baseline after fix campaign
- **WHEN** a maintainer runs the documented baseline update command after merging bulk fixes
- **THEN** `dataquality/baseline_counts.json` is regenerated
- **AND** the PR that updates the baseline includes the new counts in the commit message or PR description

