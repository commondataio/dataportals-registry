## 1. Agent documentation

- [x] 1.1 Create `llms.txt` with links to AGENTS.md, openspec/, data exports, quality outputs, and schema paths
- [x] 1.2 Create `DATASHEET.md` covering: purpose, composition, collection methodology, coverage bias (US over-representation), update cadence, known limitations, recommended uses
- [x] 1.3 Create `CITATION.cff` with title, authors, repository URL, license (CC-BY 4.0 for data)

## 2. Governance files

- [x] 2.1 Add `SECURITY.md` with vulnerability reporting instructions
- [x] 2.2 Add `CODE_OF_CONDUCT.md` (Contributor Covenant or equivalent)
- [x] 2.3 Add `.github/ISSUE_TEMPLATE/bug_report.md` and `catalog_entry.md`
- [x] 2.4 Add `.github/PULL_REQUEST_TEMPLATE.md`

## 3. README fix

- [x] 3.1 Replace broken `devdocs/duplicates_and_errors_report.md` link with `dataquality/full_report.txt` or remove reference
- [x] 3.2 Add links to new `llms.txt`, `DATASHEET.md`, `CITATION.cff` in README

## 4. Verification

- [x] 4.1 Verify all new doc links resolve (no 404s)
- [x] 4.2 Run `openspec validate add-agent-facing-data-contract --strict`
