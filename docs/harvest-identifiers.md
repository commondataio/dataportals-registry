# Dataset identifiers from a harvest

This registry’s `uid` (`cdi########`) identifies **catalogs**. Harvested datasets need their **own** ids, always paired with the catalog `uid`. Incremental crawls: [harvest-incremental.md](harvest-incremental.md). Protocol grain: [harvest-protocols.md](harvest-protocols.md).

Do not mint `cdi########` or `temp########` for datasets. Do not write dataset YAML into `data/entities/`.

## Store this tuple

| Field | Rule |
|-------|------|
| Catalog `uid` | From this registry (DuckDB/Parquet) |
| Catalog `id` / `link` | Replay and debug |
| `software.id` | Which recipe you used |
| Native id | The catalog’s primary key (required) |
| Persistent id | DOI, handle, or ARK when present (optional but preferred for dedup) |
| Landing URL | Canonical dataset page, not a session or tile URL |
| Type filter | The query/set you applied |

## Prefer persistent ids when present

Order when several exist:

1. DOI (`10.…`) — Dataverse `persistentId`, DataCite, Figshare, many IRs
2. Handle (`hdl:…` or `https://hdl.handle.net/…`) — DSpace, some EPrints
3. ARK / PURL documented by the catalog
4. Software-native id (CKAN `id` UUID, Invenio record id, ERDDAP `datasetID`, STAC collection id, CSW `fileIdentifier`, IPT dataset UUID, PxWeb table path)

Use the **package/dataset** id, not a file/resource/distribution id. Rows for IPUMS, DHIS2, OpenAIRE, Yoda, and RADAR apply by hostname — those catalogs may still be `custom` in current exports.

## Native ids by platform (typical)

| `software.id` | Native id | Not an id |
|---------------|-----------|-----------|
| `ckan` / `dkan` / `datapress` | `id` (UUID) and/or `name` | Resource `id` |
| `opendatasoft` | `dataset_id` | Attachment filename |
| `socrata` | `id` (four-four) | Column id |
| `dataverse` | `persistentId` (DOI) | File id |
| `dspace` / `dspacecris` | Handle or UUID | Bitstream id |
| `inveniordm` | Record id / DOI | File key |
| `geonetwork` | ISO `fileIdentifier` | Thumbnail URL |
| `stacserver` | Collection `id` | Item `id` (unless item grain) |
| `arcgisserver` | Service URL + name | Extent query |
| `erddap` | `datasetID` | Table row |
| `ipt` | Dataset UUID / key | Occurrence id |
| `pxweb` | Table path (`type: t`) | Folder path |
| `fairdatapoint` | Dataset IRI | Distribution IRI |
| `ipums` | Series / sample id | Extract job id |
| `dhis2` | dataSet / indicator id | orgUnit, analytics cell |
| `openaire` | Graph dataset product id / DOI | Publication id |
| `yoda` | Vault dataset DOI | iRODS path in `/research/` |
| `radar` | RADAR dataset id / DOI | Landing-page URL only |

Normalize DOI to `10.prefix/suffix` (lowercase). Strip `https://doi.org/` and `doi:`. Handles: keep the handle string, not only the UI URL.

## Deduplication

- **Inside one catalog:** native id (plus version if the API versions datasets).
- **Across catalogs:** DOI/handle first. The same Dataverse study harvested from DataONE and from the MN should collapse if you asked for a global index.
- **Aggregators:** prefer the **source** catalog `uid` when Idra/DCAT names a publisher URL that is already in this registry ([harvest-other.md](harvest-other.md)).

A landing URL with query tokens, `/latest/`, or map bbox is a locator, not a durable id.

## Versions and replacements

Keep `version` / `metadata_modified` for incremental updates. A new DOI is a new dataset; a new CKAN `revision_id` on the same `id` is an update. Withdrawn IR items: keep the id, set status — do not delete unless the user asked for tombstones ([harvest-incremental.md](harvest-incremental.md)).

## Related

- [harvest.md](harvest.md)
- [harvest-incremental.md](harvest-incremental.md)
- [harvest-protocols.md](harvest-protocols.md)
- [harvest-output.md](harvest-output.md)
- [agents/harvest.md](agents/harvest.md)
- [ai-consumers.md](ai-consumers.md) (catalog `uid` join)
