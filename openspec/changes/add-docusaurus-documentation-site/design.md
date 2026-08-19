## Context

internacia-db and iterabledata already ship Docusaurus sites on GitHub Pages. internacia-db is the closer analog: a reference-data registry with markdown at repo-root `docs/` and the generator in `website/`. iterabledata instead puts the Docusaurus project inside `docs/`. This repository already had a `docs/` folder with three analysis/methodology notes, so those notes must move before the internacia-db layout can be used.

## Goals / Non-Goals

- Goals:
  - Publish registry internals (layout, schema, exports, CLI, quality, agents) on GitHub Pages
  - Keep markdown in `docs/` so agents can read it without Node
  - Mirror internacia-db site structure and iterabledata agent discovery (`llms.txt` on the site)
- Non-Goals:
  - Auto-generated per-software pages (`builder.py build_docs` still targets cdi-docs)
  - Production query API / MCP runtime docs (out of this repository)
  - Translating the full site

## Decisions

- Decision: `website/` holds Docusaurus; `docs/` is the content source (`path: '../docs'`).
  Alternatives considered: iterabledata's `docs/`-as-site layout — rejected because it nests content in `docs/docs/` and would collide with existing contributor mental model of `docs/` as markdown.
- Decision: move the previous `docs/*.md` files to `devdocs/` (analysis and working notes). Republish metadata-quality and trust-score methodology as first-class site pages.
- Decision: GitHub Pages via `actions/deploy-pages` from `website/build`, org `datenoio`, baseUrl `/dataportals-registry/`.
- Decision: copy `llms.txt` into `website/static/` during CI so `/dataportals-registry/llms.txt` and `/.well-known/llms.txt` are served.

## Risks / Trade-offs

- GitHub Pages must be switched to GitHub Actions in repo settings → document in website README.
- Broken links to repo-root files from markdown → `onBrokenLinks: 'warn'` like internacia-db.
- `docs/` path change for old analysis files → update README/CONTRIBUTING/llms.txt links.

## Migration Plan

1. `git mv` existing `docs/` files to `devdocs/`.
2. Add new published markdown and the `website/` project.
3. Deploy on merge to `main` after Pages environment is enabled.
4. Rollback: remove `website/` and workflow; restore files from `devdocs/` if needed.

## Open Questions

- None for the initial site; per-software KB pages remain in the separate cdi-docs repo.
