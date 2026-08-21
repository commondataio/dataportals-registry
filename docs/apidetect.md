# API endpoint detection (`apidetect.py`)

Fill `endpoints[]` on catalog YAML after the record exists. The script GETs known URL templates for a `software.id` and writes types/URLs that respond.

This is **enrichment**, not discovery. Find catalogs with [discovery.md](discovery.md); add YAML with [cli.md](cli.md); then optionally run apidetect.

Do not treat `scripts/apidetect_urlmaps_draft.py` as a CLI. Draft maps are merged into `CATALOGS_URLMAP` inside `apidetect.py` at import time.

## When to run

- After adding or retagging a catalog whose `software.id` has a URL map
- When quality reports `MISSING_ENDPOINTS` and `api: true`
- In `--dryrun` first; write YAML only when probes match the live site

Skip software IDs with no map (including most `custom` records). Guessing endpoint paths by hand is worse than leaving `endpoints` empty.

## Commands

From the repository root:

```bash
python scripts/apidetect.py detect-single catalogdatagov --dryrun
python scripts/apidetect.py detect-single cdi00001616 --dryrun
python scripts/apidetect.py detect-software ckan --dryrun
python scripts/apidetect.py detect-software ckan --max-endpoints 1 --dryrun
python scripts/apidetect.py detect-country US --dryrun
python scripts/apidetect.py detect-cattype "Open data portal" --dryrun
```

`--dryrun` prints planned endpoints and does not write YAML. Omit it to insert. `--action insert` is the default; use the script `--help` for replace behaviour.

`--mode entries` (default) walks `data/entities/`. Use `--mode scheduled` for unverified files.

`detect-all` walks every mapped `software.id` — too heavy for a normal contribution; prefer `detect-single` or `detect-software`.

## Software IDs with URL maps

Maps exist for the IDs in `CATALOGS_URLMAP` (built-in plus draft merge). High-traffic examples:

| Area | `software.id` |
|------|----------------|
| Open data | `ckan`, `dkan`, `opendatasoft`, `socrata`, `udata`, `magda`, `jkan`, `junar`, `entryscape`, `drupal`, `wordpress`, `triplydb` |
| Geo | `geonetwork`, `geonode`, `geoserver`, `arcgishub`, `arcgisserver`, `pycsw`, `pygeoapi`, `mapproxy`, `qwc2`, `mapstore`, `lizmap`, `mapbender`, `geomapfish`, `getsdiportal`, `terria`, `gvsigonline`, `erdasapollo`, `wis20box`, `koordinates`, `nextgisweb` |
| Scientific | `dataverse`, `dspace`, `invenio`, `inveniordm`, `eprints`, `hyrax`, `opus`, `esploro`, `pure`, `weko3`, `elsevierdigitalcommons`, `opendap`, `thredds`, `erddap`, `ipt`, `galaxy`, `ala`, `figshare` |
| Indicators / microdata | `pxweb`, `opensdg`, `statsuite`, `sdmxri`, `nada`, `nesstar`, `redatam`, `colectica`, `obibamica`, `knoema` |
| Metadata | `fusionregistry`, `aristotlemdr`, `mwmb` |

If `detect-single` reports no map for the ID, stop. Do not copy URLs from a different platform.

## After a successful run

1. `python scripts/builder.py validate-yaml --id` for that catalog `id`
2. Set `api` / `api_status` together when an API is confirmed ([data-model.md](data-model.md))
3. Prefer endpoint `type` values already used for that `software.id` ([vocabularies.md](vocabularies.md#endpoint-types))

## Related

- [cli.md](cli.md)
- [liveness.md](liveness.md) (URL reachability of `link`, not API maps)
- [architecture.md](architecture.md)
- [quality-rules.md](quality-rules.md)
