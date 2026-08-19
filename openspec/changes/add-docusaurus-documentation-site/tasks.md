## 1. Relocate existing docs

- [x] 1.1 Move `docs/geoseer-analysis.md`, `docs/metadata-quality.md`, and `docs/trust_score_methodology.md` to `devdocs/`
- [x] 1.2 Update repository links that pointed at the old paths

## 2. Docusaurus site

- [x] 2.1 Add `website/` Docusaurus project (config, sidebars, homepage, theme) following internacia-db
- [x] 2.2 Add GitHub Pages workflow `.github/workflows/deploy-docs.yml`
- [x] 2.3 Serve `llms.txt` from the site static origin

## 3. Published documentation

- [x] 3.1 Overview: getting started, when to use, architecture, directory layout
- [x] 3.2 Data contracts: data model, catalog types, software taxonomy, exports, quality, trust score
- [x] 3.3 Query examples (DuckDB / Parquet)
- [x] 3.4 Agent workflows: query, contribute, OpenSpec quickstart
- [x] 3.5 CLI reference

## 4. Validation

- [x] 4.1 `openspec validate add-docusaurus-documentation-site --strict`
- [x] 4.2 `npm run build` in `website/`
