## 1. Schema update

- [x] 1.1 Add `allowed` to `catalog_type` in `data/schemes/catalog.json`
- [x] 1.2 Add `allowed` to `status` in `data/schemes/catalog.json`
- [x] 1.3 Add `allowed` to `access_mode` list item schema in `data/schemes/catalog.json`

## 2. Verification

- [ ] 2.1 Run `python scripts/builder.py validate-yaml` and fix any failing entities (if needed)
- [ ] 2.2 Run pytest to ensure no regressions (CI runs both validate-yaml and pytest)
