## ADDED Requirements

### Requirement: Complete Quality Report Aggregation
The `analyze-quality` command MUST include issues from every registered check in all primary report outputs.

#### Scenario: Multiple rule families produce issues
- **WHEN** `python scripts/builder.py analyze-quality` completes
- **THEN** `dataquality/full_report.txt` lists issues grouped by every issue type that has at least one finding
- **AND** `dataquality/full_report.jsonl` contains one JSON object per issue across all rule families

#### Scenario: Single rule family has zero issues
- **WHEN** a registered check finds no issues
- **THEN** that rule type is omitted from the issues-by-type section
- **AND** no stale rule file with non-zero count remains in `dataquality/rules/`

### Requirement: Report Output Consistency
Quality report outputs MUST remain internally consistent after each analysis run.

#### Scenario: Post-run consistency validation
- **WHEN** quality analysis completes successfully
- **THEN** the total issue count in `full_report.jsonl` equals the sum of per-rule issue counts written to `dataquality/rules/*.txt`
- **AND** `primary_priority.jsonl` contains a deduplicated subset of issues at CRITICAL and IMPORTANT priority

### Requirement: No Deprecated Stub Checks in Pipeline
The quality pipeline MUST NOT execute checks that always return `None`.

#### Scenario: Deprecated stubs removed
- **WHEN** the checks list in `analyze_quality` is inspected
- **THEN** `check_path_country_consistency`, `check_id_host_correlation`, and `check_owner_coverage_coherence` are not registered
- **AND** the documented active check count matches the registered list
