# Change: Improve `apidetect.py` reliability and maintainability

## Why
`scripts/apidetect.py` is a critical enrichment script that currently contains several correctness defects and structural issues that can silently produce wrong endpoint data or crash on edge-case catalogs. The script is also hard to evolve safely because configuration, HTTP behavior, and CLI concerns are tightly coupled in one large file with duplicated logic.

## What Changes
- Fix correctness and crash bugs in endpoint detection and deep discovery flows.
- Standardize endpoint probe configuration semantics, especially MIME expectations and JSON validation behavior.
- Define explicit CLI behavior for write operations, including a working `--dryrun` contract.
- Introduce safer path resolution and file I/O conventions to remove current working directory fragility.
- Refactor duplicated command flows into shared helpers while preserving output behavior.
- Add targeted automated tests for detection core, deep analyzers, and regression-prone edge cases.

## Impact
- Affected specs: `api-endpoint-detection`
- Affected code:
  - `scripts/apidetect.py`
  - `scripts/builder.py` (integration path that invokes `apidetect`)
  - `tests/` (new unit and integration coverage for API detection)
