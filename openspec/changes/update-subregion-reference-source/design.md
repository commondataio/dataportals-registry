## Context

ISO 3166-2 subdivision codes are used to validate `owner.location.subregion` and `coverage[].location.subregion` fields. The current IP2Location CSV is a commercial geolocation subset, not an authoritative ISO registry.

## Goals

- Reduce false-positive `SUBREGION_INVALID_ISO3166_2` issues to near zero for codes valid per ISO 3166-2
- Maintain a reproducible, version-pinned reference dataset in the repository
- Provide a documented refresh procedure for maintainers

## Non-Goals

- Validating that a subregion code is geographically correct for a given catalog (only format/registry membership)
- Replacing free-text subregion names with codes automatically

## Decisions

### Decision: Use debian iso-codes as primary source

**What**: Generate `data/reference/subregions/iso3166-2.csv` from the `iso-codes` package (or equivalent Debian export).

**Why**: Comprehensive, maintained, machine-readable, no licensing ambiguity for redistribution.

**Alternatives considered**:
- Wikidata SPARQL export: More complete for edge cases but harder to version-pin and refresh reproducibly
- Keep IP2Location and patch manually: Does not scale; root cause of 82% false positives

### Decision: Deprecate IP2LOCATION filename, keep migration shim

**What**: Load new file by default; log a deprecation warning if old path is referenced.

**Why**: Avoids breaking external scripts that hard-code the old path during one release cycle.

## Risks / Trade-offs

- **Risk**: New source may include codes not yet used in registry → Mitigation: test with known false-positive set from analysis doc
- **Risk**: Refresh procedure drifts from CI → Mitigation: add `tests/test_subregion_reference.py` with pinned known codes

## Migration Plan

1. Add new reference file alongside old CSV
2. Switch loader in `builder.py` and dependent scripts
3. Run `analyze-quality` and compare `SUBREGION_INVALID_ISO3166_2` count
4. Remove IP2Location CSV after one release cycle (document in CHANGELOG)

## Open Questions

- Whether to merge Wikidata-only codes as a secondary supplement for territories missing from iso-codes
