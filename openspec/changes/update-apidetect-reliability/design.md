## Context
`scripts/apidetect.py` combines endpoint catalog configuration, HTTP probing behavior, deep HTML/robots discovery, record mutation, and CLI command orchestration in one module. This creates high regression risk when making changes and has already allowed multiple correctness defects to persist.

## Goals / Non-Goals
- Goals:
  - Restore deterministic, crash-safe endpoint detection behavior.
  - Make endpoint probe semantics explicit and testable.
  - Reduce duplication in command execution paths to lower maintenance cost.
  - Preserve compatibility with existing catalog data shape (`endpoints` list entries).
- Non-Goals:
  - Full rewrite into a separate package.
  - Changing endpoint output schema beyond bug fixes and clarified semantics.
  - Introducing external service dependencies.

## Decisions
- Decision: Apply a phased reliability-first refactor.
  - Phase 1: correctness and crash fixes only.
  - Phase 2: shared helpers and path/I/O normalization.
  - Phase 3: optional modular split if behavior remains stable.
- Decision: Define endpoint probe entries with a strict normalized contract.
  - `expected_mime` is treated as `list[str]` at runtime.
  - JSON verification must use decode-exception handling, not key-based exceptions.
- Decision: Make command mutation behavior explicit.
  - `--dryrun` must guarantee no writes while still reporting would-be changes.

### Alternatives considered
- Big-bang rewrite into multiple modules immediately.
  - Rejected: high regression risk without baseline tests.
- Only patch critical bugs without structural cleanup.
  - Rejected: quickly reintroduces divergence and duplicated bug surfaces.

## Risks / Trade-offs
- Risk: Refactoring shared command flows may alter update behavior.
  - Mitigation: add regression tests around endpoint replacement/insert/update modes before refactor steps.
- Risk: Tightening MIME checks can reduce false-positive endpoint detections.
  - Mitigation: document expected behavior and include targeted fixtures for known software families.
- Risk: Path resolution changes can affect ad hoc script invocation.
  - Mitigation: preserve CLI flags and keep repository-relative defaults deterministic.

## Migration Plan
1. Add regression tests that reproduce current known defects.
2. Implement phase-1 fixes and confirm tests pass.
3. Introduce shared helpers and switch command handlers incrementally.
4. Validate with `builder.py` integration path and representative country slices.
5. Roll out with clear release note of behavior clarifications (`dryrun`, MIME strictness).

## Open Questions
- Should endpoint probe definitions stay in Python constants or move to a structured YAML reference file in a later change?
- Should `verify_json` default be enabled for all JSON endpoint probes after false-positive baseline is measured?
