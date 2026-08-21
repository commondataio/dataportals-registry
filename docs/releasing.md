# Publishing a release

Maintainer checklist for GitHub releases. Update snapshot counts in [README.md](https://github.com/datenoio/dataportals-registry/blob/main/README.md) and [getting-started.md](getting-started.md) when the entity count changes.

## 1. Changelog and counts

- In `CHANGELOG.md`, add `## [X.Y.Z] - YYYY-MM-DD` under `[Unreleased]` with Added/Changed/Fixed/Removed.
- Add a GitHub Release line: `**GitHub Release**: [vX.Y.Z](https://github.com/datenoio/dataportals-registry/releases/tag/vX.Y.Z) - Published Month DD, YYYY`
- Update “Latest snapshot” dates and record counts in `README.md`, `docs/getting-started.md`, `docs/exports.md`, `DATASHEET.md`, and `AGENTS.md`.

## 2. Release notes (optional)

Copy or adapt `.github/RELEASE_NOTES_vX.Y.Z.md`, or use the CHANGELOG section as the GitHub release body.

## 3. Commit, tag, and push

```bash
git add CHANGELOG.md README.md docs/getting-started.md docs/exports.md DATASHEET.md AGENTS.md .github/RELEASE_NOTES_v*.md
git commit -m "Release vX.Y.Z"
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin main
git push origin vX.Y.Z
```

## 4. GitHub release

1. Open [Releases](https://github.com/datenoio/dataportals-registry/releases).
2. Draft a release on tag `vX.Y.Z`.
3. Title `vX.Y.Z`.
4. Paste notes from `.github/RELEASE_NOTES_vX.Y.Z.md` or CHANGELOG.
5. Publish. Attach `data/datasets/full.jsonl.zst`, `full.parquet`, and `datasets.duckdb` when you want consumers to download dumps without cloning.

## Documentation site (GitHub Pages)

The published site is https://datenoio.github.io/dataportals-registry/. It builds from `docs/` plus the Docusaurus app in `website/`.

`.github/workflows/deploy-docs.yml` runs on pushes to `main` that touch `docs/**`, `website/**`, `llms.txt`, or the workflow file itself, and on `workflow_dispatch`.

Preview locally:

```bash
cd website
npm start
```

Production build check:

```bash
cd website
npm run build
```

Docs-only commits should not include `data/entities/**`, `data/software/**`, or generated `data/datasets/**` unless the release is meant to ship those changes.

## Citation

`CITATION.cff` is the citation source. There is no Zenodo DOI yet; add `doi:` to `CITATION.cff` when a deposit exists. Preferred string:

```
dataportals-registry: A global registry of open data portals and catalogs
(Common Data Index, 2026). CC-BY-4.0.
https://github.com/datenoio/dataportals-registry
```
