## ADDED Requirements

### Requirement: Deterministic Endpoint Discovery
The API endpoint detection workflow MUST return deterministic endpoint candidates without duplicate entries introduced by control-flow artifacts.

#### Scenario: Probe loop completes without duplicate tail entry
- **WHEN** a software URL map is iterated for one base URL
- **THEN** each successful probe result is appended at most once
- **AND** no additional endpoint is appended after loop completion unless it was produced by a successful probe

### Requirement: Robust JSON Verification
JSON endpoint verification MUST handle malformed JSON responses without crashing the detection workflow.

#### Scenario: Invalid JSON response for JSON-marked probe
- **WHEN** an endpoint probe marked as JSON returns non-JSON or malformed payload
- **THEN** detection records the probe as failed JSON verification
- **AND** processing continues for remaining probes

### Requirement: Consistent MIME Validation Contract
Endpoint MIME validation MUST use a normalized list-based accepted MIME contract for every probe definition.

#### Scenario: Single expected MIME value is configured
- **WHEN** a probe definition declares one accepted MIME type
- **THEN** validation treats it as a single-item list
- **AND** content-type comparison uses exact MIME membership semantics

#### Scenario: Multiple expected MIME values are configured
- **WHEN** a probe definition declares multiple accepted MIME types
- **THEN** validation accepts responses matching any declared MIME
- **AND** rejects responses outside the declared set

### Requirement: Deep Discovery Must Be Crash-Safe
Deep discovery helpers MUST tolerate missing, malformed, or partial root metadata without raising unhandled exceptions.

#### Scenario: JSON-LD script contains empty list or missing `mainEntity` type
- **WHEN** root page parsing reaches JSON-LD blocks with incomplete structures
- **THEN** parser skips invalid structures safely
- **AND** continues scanning remaining root metadata blocks

#### Scenario: robots endpoint is unavailable
- **WHEN** `robots.txt` returns non-success status or request errors
- **THEN** robots analysis returns no extracted endpoints
- **AND** main detection continues without failure

### Requirement: CLI Mutation Contract
CLI detection commands MUST provide explicit and enforceable mutation behavior.

#### Scenario: Dry run is enabled
- **WHEN** a detection command is executed with `--dryrun`
- **THEN** no catalog YAML file is modified
- **AND** command output reports proposed endpoint changes

#### Scenario: Dry run is disabled
- **WHEN** detection produces endpoint changes and `--dryrun` is not enabled
- **THEN** updates are written to target records according to selected action mode

### Requirement: Repository-Relative Data Path Resolution
Detection commands MUST resolve entity and scheduled data roots independently of current working directory.

#### Scenario: Command launched outside script directory
- **WHEN** a command is executed from repository root or another working directory
- **THEN** the same `data/entities` and `data/scheduled` directories are resolved consistently
- **AND** detection scans the intended catalog files

### Requirement: Regression Test Coverage for Detection Core
The project MUST include automated tests for the highest-risk endpoint detection behavior.

#### Scenario: Core detection regressions are introduced
- **WHEN** changes alter probe loop behavior, MIME checks, deep parsing, or CKAN/report edge cases
- **THEN** automated tests fail before merge
- **AND** identify the impacted behavior class
