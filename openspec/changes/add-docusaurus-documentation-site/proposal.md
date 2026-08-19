# Change: Add Docusaurus documentation site

## Why

Registry internals live across README, AGENTS.md, CONTRIBUTING, DATASHEET, llms.txt, and ad-hoc analysis notes. Humans and coding agents need a single GitHub Pages site that explains the YAML model, exports, CLI, quality pipeline, and contribution workflows.

## What Changes

- Move existing analysis notes out of `docs/` into `devdocs/` so `docs/` can hold published site content.
- Add a Docusaurus site in `website/` (same layout as internacia-db) that reads markdown from `docs/`.
- Write internals documentation for human readers and agent workflows (query, contribute, OpenSpec).
- Deploy the site to GitHub Pages via Actions (same workflow pattern as internacia-db and iterabledata).
- Serve `llms.txt` from the site origin for agent discovery.

## Impact

- Affected specs: `documentation-site` (new capability)
- Affected code: `docs/`, `website/`, `.github/workflows/deploy-docs.yml`, README, CONTRIBUTING, `llms.txt`, CHANGELOG
- No breaking changes to catalog data or the build pipeline
