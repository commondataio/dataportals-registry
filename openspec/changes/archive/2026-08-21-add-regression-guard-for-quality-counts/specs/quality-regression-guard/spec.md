## ADDED Requirements

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
