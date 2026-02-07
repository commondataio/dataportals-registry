# Promoting scheduled entries to entities

Catalogs in **data/scheduled/** are unverified or not yet fully reviewed. Once reviewed and validated, they can be promoted to **data/entities/** so they are included in the main catalogs export and discovery.

## Intended workflow

1. **Review** – Check the YAML files in `data/scheduled/` (or a subset). Ensure required fields are present, links are correct, and metadata is accurate.
2. **Validate** – Run schema validation:
   ```bash
   python scripts/builder.py validate-yaml
   ```
   The validator checks `data/entities/` only. After you move files from scheduled to entities, run it again to ensure the promoted entries pass.
3. **Promote** – Move the YAML file(s) from `data/scheduled/` to the correct path under `data/entities/COUNTRY_CODE/type/`. Use the same directory layout as entities (e.g. `US/Federal/opendata/`, `FR/geo/`).
4. **Assign UID** – If the entry has a temporary `uid` (e.g. `temp########`) or no `uid`, run:
   ```bash
   python scripts/builder.py assign
   ```
   This assigns a permanent `cdi########` UID to entries that do not already have one.
5. **Re-validate** – Run `validate-yaml` again and run the build to regenerate exports:
   ```bash
   python scripts/builder.py validate-yaml
   python scripts/builder.py build
   ```

## Manual promotion

- Copy or move the file from e.g. `data/scheduled/examplecatalog.yaml` to `data/entities/US/Federal/opendata/examplecatalog.yaml` (adjust country and type to match the catalog’s `coverage` and `catalog_type`).
- Ensure the filename matches the `id` field and the path matches the entity layout (see [CONTRIBUTING.md](../CONTRIBUTING.md) and [AGENTS.md](../AGENTS.md)).

## Bulk promotion

To promote multiple scheduled entries by ID, you can script the move: list the IDs, determine each file’s target path from its `coverage` and `catalog_type` (and optionally `owner.location`), then move the files and run `assign` and `validate-yaml`. A dedicated “promote” script could be added in a future change (e.g. `scripts/promote_scheduled.py --ids id1,id2`).
