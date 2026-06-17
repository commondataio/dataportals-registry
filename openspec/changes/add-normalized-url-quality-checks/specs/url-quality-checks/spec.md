## ADDED Requirements

### Requirement: URL Canonicalization
The quality pipeline MUST normalize URLs before cross-record comparison.

#### Scenario: HTTP and HTTPS variants of same portal
- **WHEN** two records have links `http://example.gov/` and `https://example.gov`
- **THEN** canonicalization produces the same normalized form for both
- **AND** duplicate detection flags `DUPLICATE_LINK_NORMALIZED`

#### Scenario: Trailing slash difference only
- **WHEN** two records have links `https://data.gov` and `https://data.gov/`
- **THEN** canonicalization treats them as equivalent
- **AND** duplicate detection flags `DUPLICATE_LINK_NORMALIZED`

### Requirement: Normalized Duplicate Link Detection
The `analyze-quality` command MUST detect duplicate catalogs by canonical URL in addition to exact URL match.

#### Scenario: Exact duplicates still detected
- **WHEN** two records share the identical `link` string
- **THEN** `DUPLICATE_LINK` is flagged as today
- **AND** `DUPLICATE_LINK_NORMALIZED` is also flagged when canonical forms match

#### Scenario: Normalized-only duplicates
- **WHEN** two records have different exact `link` strings but identical canonical forms
- **THEN** `DUPLICATE_LINK_NORMALIZED` is flagged
- **AND** `DUPLICATE_LINK` is not flagged unless exact strings also match

### Requirement: Consistent URL Validation Across Fields
URL format and duplicate checks MUST apply consistently to portal link and auxiliary URL fields.

#### Scenario: Identifier URL validation
- **WHEN** `identifiers[].url` contains a malformed URL
- **THEN** `check_identifier_urls` flags the issue using the same URL validation rules as `check_urls`

#### Scenario: Rights URL validation
- **WHEN** `rights.tos_url` or `rights.privacy_policy_url` contains a malformed URL
- **THEN** `check_rights_urls` flags the issue
- **AND** canonicalization rules match those used for `link` duplicate detection
