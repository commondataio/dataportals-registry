# Website

This website is built using [Docusaurus](https://docusaurus.io/). Markdown content lives in the repository `docs/` folder (same layout as internacia-db).

## Installation

```bash
cd website
npm install
```

## Local development

```bash
npm run start
```

This starts a local server. Most changes are reflected live.

## Build

```bash
npm run build
```

Static output is written to `website/build`.

## GitHub Pages

Deployment is handled by `.github/workflows/deploy-docs.yml` on pushes to `main` that touch `docs/` or `website/`. Enable GitHub Pages in repository settings with **Source: GitHub Actions**.

The production URL is `https://datenoio.github.io/dataportals-registry/`.
