# Change: Add JSON Schema and linked-data mapping

## Why

The catalog schema uses Cerberus DSL without field-level `description` keys, limiting LLM grounding and non-Python tooling consumption. No DCAT-AP or schema.org JSON-LD mapping exists, hindering federation with data.europa.eu and Google Dataset Search.

## What Changes

- Dual-publish `data/schemes/catalog.schema.json` as JSON Schema Draft 2020-12 with `description` and `examples` for every property.
- Keep existing Cerberus `catalog.json` as the validation source of truth until migration is complete.
- Add `data/schemes/catalog.context.jsonld` mapping registry fields to DCAT-AP and schema.org terms.
- Add build step to emit optional `catalogs.jsonld` export alongside JSONL.
- Document schema relationship in `CONTRIBUTING.md`.

## Impact

- Affected specs: `catalog-interoperability` (new capability)
- Affected code:
  - `data/schemes/catalog.schema.json` (new)
  - `data/schemes/catalog.context.jsonld` (new)
  - `scripts/builder.py` (optional JSON-LD export)
- **BREAKING** (future): Cerberus removal is out of scope; this change is additive only
