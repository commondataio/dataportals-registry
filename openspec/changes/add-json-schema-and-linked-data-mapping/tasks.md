## 1. JSON Schema

- [x] 1.1 Create `data/schemes/catalog.schema.json` (JSON Schema Draft 2020-12)
- [x] 1.2 Add `description` and `examples` for all top-level and nested properties
- [x] 1.3 Align `required`, `enum`, and type constraints with Cerberus `catalog.json`
- [x] 1.4 Add `tests/test_schema_parity.py` comparing Cerberus and JSON Schema constraints

## 2. JSON-LD context

- [x] 2.1 Create `data/schemes/catalog.context.jsonld` with DCAT-AP and schema.org mappings
- [x] 2.2 Map: name, description, link, catalog_type, owner, coverage, identifiers, endpoints, rights, status
- [x] 2.3 Document unmapped fields under `commondataio:` namespace

## 3. Build integration

- [x] 3.1 Add optional `builder.py build --jsonld` flag to emit `data/datasets/catalogs.jsonld`
- [x] 3.2 Frame each record with `@context` and `@type: dcat:DataCatalog`

## 4. Documentation

- [x] 4.1 Update `CONTRIBUTING.md` with schema dual-publish policy
- [x] 4.2 Link JSON Schema and context from `llms.txt` (after `add-agent-facing-data-contract`)

## 5. Verification

- [x] 5.1 Validate sample records against JSON Schema (ajv or jsonschema library)
- [x] 5.2 Run `openspec validate add-json-schema-and-linked-data-mapping --strict`
- [x] 5.3 Run pytest
