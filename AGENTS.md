<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

---

## Project (dataportals-registry)

**When to use OpenSpec vs direct work**
- Open `@/openspec/AGENTS.md` for: proposals, specs, planning, breaking changes, architecture (per block above).
- Work directly for: adding/editing single catalog entries (YAML), bug fixes, typos, validation fixes, CONTRIBUTING-style contributions.

**Registry layout**
- Entities: `data/entities/COUNTRY_CODE/` then by type (`opendata/`, `geo/`, `scientific/`, etc.).
- Federal vs subregion: `Federal/` or subregion code (e.g. `US-CA/`) under country.
- One YAML file per catalog; filename = `id` (lowercase, no special characters), e.g. `catalogdatagov.yaml` → `id: catalogdatagov`.

**Schema and validation**
- Schema: [data/schemes/catalog.json](data/schemes/catalog.json). Entries must validate.
- Required fields: `id`, `uid`, `name`, `link`, `catalog_type`, `access_mode`, `status`, `software`, `owner`, `coverage`.

**Adding/editing entities**
- Prefer `python scripts/builder.py add-single ...` for new entries when appropriate; otherwise create/edit YAML under the correct `data/entities/...` path.
- Generate or assign `uid` using `python scripts/builder.py assign`—do not set or edit `uid` directly in YAML.
- Follow YAML conventions: 2 spaces, filename = `id`; see [CONTRIBUTING.md](CONTRIBUTING.md) and [openspec/project.md](openspec/project.md) for full conventions.

**Agent tasks**
- **Adding a catalog**: Prefer `python scripts/builder.py add-single ...`; or create the YAML under the correct `data/entities/COUNTRY/type/` (or `data/scheduled/`) path, then run `python scripts/builder.py assign` and `python scripts/builder.py validate-yaml`.
- **Fixing quality**: Run `python scripts/builder.py analyze-quality`, then use `dataquality/primary_priority.jsonl` and the `scripts/fix_*_issues.py` scripts or Cursor with `scripts/generate_cursor_commands.py` / `fix_all_issues`; re-run `validate-yaml` and `analyze-quality` to verify. See [devdocs/quality-fix-workflow.md](devdocs/quality-fix-workflow.md).
- **Schema or process changes**: Use OpenSpec; see [openspec/AGENTS.md](openspec/AGENTS.md).

**References**
- Full project conventions and domain context: [openspec/project.md](openspec/project.md).
- Adding entries, validation, PR process: [CONTRIBUTING.md](CONTRIBUTING.md).