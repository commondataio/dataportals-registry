## Context

Agents and external tools (TypeScript validators, OpenAI structured outputs, MCP servers) cannot consume Cerberus schemas directly. JSON Schema with descriptions is the emerging cross-ecosystem standard. JSON-LD enables semantic federation without replacing the YAML-first authoring model.

## Goals

- Publish a canonical JSON Schema with human- and machine-readable field descriptions
- Map registry fields to DCAT-AP and schema.org for semantic interoperability
- Keep Cerberus validation working during transition

## Non-Goals

- Removing Cerberus validation in this change
- Full DCAT-AP compliance certification
- SPARQL endpoint

## Decisions

### Decision: Dual-publish, Cerberus remains validation source

**What**: Add `catalog.schema.json` alongside `catalog.json`; CI validates YAML against Cerberus; add test that both schemas agree on required fields and allowed values.

**Why**: Lowest-risk migration path; avoids breaking existing `validate-yaml` workflow.

### Decision: JSON-LD context as separate file

**What**: `catalog.context.jsonld` maps `link` → `dcat:landingPage`, `uid` → `schema:identifier`, etc.

**Why**: Context file is stable; per-record JSON-LD can be generated at build time without editing 14k YAML files.

### Decision: Key DCAT mappings first

**What**: Phase 1 maps: name, description, link, catalog_type, owner, coverage, identifiers, endpoints, rights, status.

**Why**: Covers 90% of agent and federation use cases per Genspark audit.

## Risks / Trade-offs

- **Risk**: Schema drift between Cerberus and JSON Schema → Mitigation: CI sync test
- **Risk**: JSON-LD mapping ambiguity for custom fields → Mitigation: use `commondataio:` namespace for unmapped fields

## Migration Plan

1. Generate initial JSON Schema from Cerberus + manual descriptions
2. Add context file and build-time JSON-LD export (opt-in flag)
3. Future change: switch `validate-yaml` to JSON Schema via adapter

## Open Questions

- Whether to publish combined `catalogs.jsonld` or per-record `.jsonld` sidecars
