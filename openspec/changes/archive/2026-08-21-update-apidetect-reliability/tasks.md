## 1. Correctness and Safety
- [x] 1.1 Fix duplicate endpoint insertion caused by loop `else` behavior in endpoint probing.
- [x] 1.2 Fix JSON verification exception handling to catch decode failures correctly.
- [x] 1.3 Fix `mainEntity` DataCatalog parsing in deep root analysis (loop scope and empty-list handling).
- [x] 1.4 Fix CKAN endpoint base URL extraction path to avoid `KeyError` when `endpoints` is missing.
- [x] 1.5 Remove or guard debug file writes during JSON-LD parsing to prevent filesystem-related failures.
- [x] 1.6 Fix report CSV header generation so exported columns remain aligned.

## 2. Detection Semantics
- [x] 2.1 Enforce consistent `expected_mime` handling as a list-based contract.
- [x] 2.2 Ensure content-type validation behaves identically for single and multiple accepted MIME types.
- [x] 2.3 Remove redundant prefetch request behavior or reuse prefetched responses deterministically.
- [x] 2.4 Replace hard-coded GeoServer suffix slicing with explicit suffix handling.

## 3. CLI and I/O Contracts
- [x] 3.1 Implement and document `--dryrun` so commands can execute without mutating YAML files.
- [x] 3.2 Resolve data directory paths relative to repository/script location instead of current working directory.
- [x] 3.3 Replace repeated open/close patterns with context managers and shared loaders/savers.

## 4. Structure and Refactor
- [x] 4.1 Extract shared detection execution helper(s) used by `detect_software`, `detect_single`, `detect_country`, and `detect_cattype`.
- [x] 4.2 Reduce divergence in `detect_ckan`, `detect_all`, and `update_broken_arcgis` by routing through shared detection paths.
- [x] 4.3 Normalize logging semantics (`found` vs failures) and message consistency.

## 5. Test Coverage
- [x] 5.1 Add unit tests for `api_identifier` (status handling, MIME checks, JSON verification, duplicate prevention).
- [x] 5.2 Add unit tests for `analyze_root` and `analyze_robots` edge cases.
- [x] 5.3 Add regression tests for CKAN path handling and report output format.
- [x] 5.4 Add integration test coverage for builder-to-apidetect invocation path.
