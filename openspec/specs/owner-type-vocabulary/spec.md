# owner-type-vocabulary Specification

## Purpose
TBD - created by archiving change add-owner-type-and-path-consistency-rules. Update Purpose after archive.
## Requirements
### Requirement: Canonical Owner Type Vocabulary
The registry MUST maintain a reference vocabulary of canonical `owner.type` values and recognized synonyms.

#### Scenario: Reference file present
- **WHEN** a maintainer inspects `data/reference/owner_types.yaml`
- **THEN** it lists canonical owner types
- **AND** maps common synonyms to those canonical values

### Requirement: Owner Type Quality Checks
`analyze-quality` MUST flag unknown or non-canonical owner types.

#### Scenario: Unknown owner type
- **WHEN** `owner.type` is present and not in the canonical set or synonym map
- **THEN** `INVALID_OWNER_TYPE` is emitted

#### Scenario: Synonym owner type
- **WHEN** `owner.type` matches a known synonym (e.g. `University` → `Academy`)
- **THEN** `OWNER_TYPE_NONCANONICAL` is emitted
- **AND** the suggested action names the canonical value

