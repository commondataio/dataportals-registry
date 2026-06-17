# Change: Add agent-facing data contract

## Why

The Genspark audit identified documentation fragmentation across 7+ files and missing FAIR/governance artifacts (no `llms.txt`, `DATASHEET.md`, `CITATION.cff`, `SECURITY.md`). A broken README link to `devdocs/duplicates_and_errors_report.md` (404) undermines trust. Downstream LLM agents cannot reason about dataset limitations without a datasheet.

## What Changes

- Add `llms.txt` at repository root summarizing agent-relevant docs and data exports.
- Add `DATASHEET.md` following Gebru et al. datasheets convention (coverage bias, update cadence, known limitations).
- Add `CITATION.cff` for academic citation.
- Add `SECURITY.md` and `CODE_OF_CONDUCT.md`.
- Add `.github/ISSUE_TEMPLATE/` and `PULL_REQUEST_TEMPLATE.md`.
- Fix broken README link (remove or replace with valid quality report path).

## Impact

- Affected specs: `agent-documentation` (new capability)
- Affected code: Repository root docs, `.github/`, `README.md`
- No breaking changes to catalog data or build pipeline
