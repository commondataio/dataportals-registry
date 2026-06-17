## ADDED Requirements

### Requirement: LLM Agent Index File
The repository MUST provide an `llms.txt` file at the root for agent discovery.

#### Scenario: Agent loads llms.txt
- **WHEN** an LLM agent reads `/llms.txt`
- **THEN** it finds concise descriptions and links to AGENTS.md, schema files, data exports, and quality outputs
- **AND** the file is under 500 lines

### Requirement: Dataset Datasheet
The registry MUST publish a `DATASHEET.md` describing dataset characteristics and limitations.

#### Scenario: Downstream consumer assesses fitness for use
- **WHEN** a researcher or agent reads `DATASHEET.md`
- **THEN** it describes geographic coverage bias, record count, update cadence, and known limitations
- **AND** it references the CC-BY 4.0 data license

### Requirement: Citation Metadata
The repository MUST include a `CITATION.cff` file for academic attribution.

#### Scenario: Researcher cites the registry
- **WHEN** a researcher uses citation tooling on the repository
- **THEN** `CITATION.cff` provides title, authors, repository URL, and license
- **AND** includes a preferred citation string

### Requirement: Security Disclosure Policy
The repository MUST publish a `SECURITY.md` vulnerability reporting policy.

#### Scenario: Reporter finds a security issue
- **WHEN** a user reads `SECURITY.md`
- **THEN** they find instructions for responsible disclosure
- **AND** a contact method or issue label is specified

### Requirement: Valid Documentation Links
README and agent docs MUST not reference missing files.

#### Scenario: README quality report link
- **WHEN** a user follows the quality findings link in `README.md`
- **THEN** the target file exists in the repository
- **AND** is not a 404 path
