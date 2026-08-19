## ADDED Requirements

### Requirement: Published documentation source
The repository MUST keep human- and agent-facing internals documentation as Markdown under `docs/`, separate from working notes in `devdocs/`.

#### Scenario: Agent reads internals docs without Node
- **WHEN** an agent opens `docs/getting-started.md` or `docs/agents/query.md` from the repository
- **THEN** the files exist as Markdown at those paths
- **AND** they describe exports, schema, and contribution rules without requiring the Docusaurus build

#### Scenario: Analysis notes are not the site source
- **WHEN** a contributor looks for GeoSeer analysis or similar working notes
- **THEN** those files live under `devdocs/`
- **AND** they are not required to build the documentation site

### Requirement: Docusaurus GitHub Pages site
The repository MUST provide a Docusaurus site that publishes `docs/` to GitHub Pages.

#### Scenario: Local site build
- **WHEN** a maintainer runs `npm ci` and `npm run build` in `website/`
- **THEN** a static site is written to `website/build`
- **AND** the site uses base URL `/dataportals-registry/`

#### Scenario: GitHub Pages deploy workflow
- **WHEN** documentation files under `docs/` or `website/` change on `main`
- **THEN** `.github/workflows/deploy-docs.yml` builds the site and deploys it with GitHub Pages actions

### Requirement: Agent discovery from the site
The documentation site MUST serve the repository `llms.txt` index at a stable URL.

#### Scenario: Agent fetches hosted llms.txt
- **WHEN** an agent requests `llms.txt` from the GitHub Pages origin
- **THEN** the file is available at `/dataportals-registry/llms.txt`
- **AND** a copy is available at `/dataportals-registry/.well-known/llms.txt`
