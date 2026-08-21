## ADDED Requirements

### Requirement: JSON Schema Publication
The project MUST publish a JSON Schema Draft 2020-12 description of the catalog data model.

#### Scenario: External validator consumes schema
- **WHEN** a tool loads `data/schemes/catalog.schema.json`
- **THEN** it can validate catalog records without Cerberus
- **AND** every property includes a `description` string

#### Scenario: Schema parity with Cerberus
- **WHEN** CI runs schema parity tests
- **THEN** required fields and allowed value sets match between `catalog.json` and `catalog.schema.json`
- **AND** any drift fails the test

### Requirement: JSON-LD Semantic Mapping
The project MUST publish a JSON-LD context mapping registry fields to standard vocabularies.

#### Scenario: Record exported as JSON-LD
- **WHEN** `builder.py build --jsonld` completes
- **THEN** `data/datasets/catalogs.jsonld` contains framed catalog objects
- **AND** each object uses `@context` from `catalog.context.jsonld`

#### Scenario: DCAT field mapping
- **WHEN** a catalog record has `link`, `name`, and `description`
- **THEN** the JSON-LD export maps them to `dcat:landingPage`, `dct:title`, and `dct:description` respectively

### Requirement: Additive Schema Migration
JSON Schema addition MUST NOT break existing Cerberus validation.

#### Scenario: Existing validate-yaml workflow
- **WHEN** a contributor runs `python scripts/builder.py validate-yaml`
- **THEN** validation uses Cerberus `catalog.json` as today
- **AND** passes for all currently valid entities
