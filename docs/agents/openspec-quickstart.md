# OpenSpec quickstart for agents

Short guide for schema, pipeline, and documentation-architecture changes. Full reference: [openspec/AGENTS.md](https://github.com/datenoio/dataportals-registry/blob/main/openspec/AGENTS.md).

## When to create a proposal

**Do propose** for: new capabilities, breaking schema/export changes, architecture shifts.

**Skip proposal** for: bug fixes restoring intended behavior, typos, adding/editing catalog YAML, non-breaking dependency updates, tests for existing behavior.

## Checklist

1. Run `openspec list` and `openspec list --specs` — check for conflicts.
2. Pick a unique verb-led `change-id` (e.g. `add-field-endpoints-status`).
3. Scaffold under `openspec/changes/<change-id>/`:
   - `proposal.md` — why, what, impact
   - `tasks.md` — implementation checklist
   - `design.md` — only if cross-cutting or ambiguous
   - `specs/<capability>/spec.md` — deltas with `## ADDED|MODIFIED|REMOVED Requirements`
4. Every requirement needs at least one `#### Scenario:` block.
5. Run `openspec validate <change-id> --strict` and fix all issues.
6. Do **not** implement until the proposal is approved (unless a maintainer already requested the work in the same change).

## Scenario format (required)

```markdown
#### Scenario: Descriptive name
- **WHEN** condition
- **THEN** expected outcome
```

Use `#### Scenario:` (four hashes). Bullets or `### Scenario:` fail validation.

## Dataset scope

This repository is a **catalog registry**. Do not propose production query APIs or MCP servers here. Do not add dataset-level records into catalog YAML.

## CLI essentials

```bash
openspec list
openspec list --specs
openspec show <id> --json --deltas-only
openspec validate <change-id> --strict
openspec archive <change-id> --yes
```

Update `CHANGELOG.md` under `[Unreleased]` for consumer-visible changes.
