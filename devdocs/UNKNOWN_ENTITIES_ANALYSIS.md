# Analysis of YAML Files in entities/Unknown Directory

## Executive Summary

Analyzed **149 YAML files** in `data/entities/Unknown/geo/` directory to identify missing or fixable metadata. The analysis reveals several categories of issues that could be improved to enhance data quality.

## Key Findings

### 1. Country Information Issues (65 files - 43.6%)

**Problem:** Files have `country.id: Unknown` in both `coverage` and `owner.location` sections.

**Impact:** High - Country information is critical for geographic organization and filtering.

**Examples:**
- `opendataaeroterrahubarcgiscom.yaml` - Description mentions "Mendoza, Argentina" but country is Unknown
- `kpisburhubarcgiscom.yaml` - Description mentions "Burleson" (likely Texas, US) but country is Unknown
- `j2nvngafdcgshubarcgiscom.yaml` - Country is Unknown

**Potential Fixes:**
- Extract country from descriptions (e.g., "Mendoza, Argentina" → AR)
- Extract from URLs/domains where possible
- Use owner information to infer country
- Check if files can be moved to proper country directories

### 2. Missing Identifiers (148 files - 99.3%)

**Problem:** Files are missing the `identifiers` field which could include Wikidata, DOI, or other external identifiers.

**Impact:** Medium - Identifiers help link records to external knowledge bases and improve discoverability.

**Potential Fixes:**
- Add Wikidata identifiers where available
- Add DOI if applicable
- Add other relevant identifiers from external sources

### 3. Missing Export Standard (149 files - 100%)

**Problem:** All files are missing the `export_standard` field which indicates the API standard used (e.g., "CKAN API", "DCAT-AP", "OGC Records API").

**Impact:** Medium - This information is useful for API consumers and integration.

**Potential Fixes:**
- Most files have `endpoints` with `type` field (e.g., `dcatap201`, `ogcrecordsapi`)
- Can derive `export_standard` from endpoint types:
  - `dcatap201` → "DCAT-AP 2.0.1"
  - `dcatus11` → "DCAT-US 1.1"
  - `ogcrecordsapi` → "OGC Records API"
  - `ckanapi` → "CKAN API"

### 4. Missing Dates (149 files - 100%)

**Problem:** All files are missing `add_date` and `update_date` fields.

**Impact:** Low - Dates are optional but useful for tracking when records were added/updated.

**Potential Fixes:**
- Add `add_date` based on file creation or first commit date
- Add `update_date` based on last modification date
- Can be extracted from git history if needed

### 5. Empty Topics (68 files - 45.6%)

**Problem:** Files have empty `topics: []` arrays, missing thematic categorization.

**Impact:** Medium - Topics help with discovery and filtering by subject area.

**Potential Fixes:**
- Extract topics from descriptions using NLP/AI
- Use existing enrichment scripts (`enrich_topics` command)
- Infer from catalog_type and content_types
- Use tags to suggest topics

**Example:** A file with tags like "GIS", "Open Data", "City Data" could have topics like:
- `SOCI` (Population and society)
- `GOVE` (Government and public sector)

### 6. Null Owner Link (33 files - 22.1%)

**Problem:** Files have `owner.link: null` instead of a valid URL.

**Impact:** Low-Medium - Owner links help identify the organization behind the portal.

**Potential Fixes:**
- Use the catalog's main `link` if owner link is missing
- Extract from descriptions or metadata
- Set to the portal's homepage URL

### 7. Generic Owner Name (36 files - 24.2%)

**Problem:** Files have generic owner names like "Not provided in available content" or "Unknown".

**Impact:** Medium - Owner information is important for understanding data provenance.

**Potential Fixes:**
- Extract from descriptions
- Use catalog name as fallback
- Infer from URL patterns
- Use AI enrichment if available

## Recommendations by Priority

### High Priority (Should Fix)

1. **Country Information** (65 files)
   - Extract country from descriptions, URLs, or owner information
   - Move files to appropriate country directories once identified
   - Update both `coverage` and `owner.location` sections

2. **Export Standard** (149 files)
   - Automatically derive from `endpoints[].type` field
   - This is a straightforward fix that can be automated

### Medium Priority (Should Consider Fixing)

3. **Empty Topics** (68 files)
   - Run existing `enrich_topics` command
   - Use AI enrichment if available
   - Infer from existing tags and descriptions

4. **Missing Identifiers** (148 files)
   - Add Wikidata identifiers where available
   - Can be done incrementally as resources allow

5. **Generic Owner Names** (36 files)
   - Improve owner names using descriptions or AI enrichment
   - Use catalog name as fallback

6. **Null Owner Links** (33 files)
   - Set to catalog's main link if owner link is missing

### Low Priority (Nice to Have)

7. **Missing Dates** (149 files)
   - Add dates from git history or file metadata
   - Can be automated but lower impact

## Implementation Suggestions

### Automated Fixes

1. **Export Standard**: Create a script to derive `export_standard` from `endpoints`:
   ```python
   endpoint_type_map = {
       "dcatap201": "DCAT-AP 2.0.1",
       "dcatus11": "DCAT-US 1.1",
       "ogcrecordsapi": "OGC Records API",
       "ckanapi": "CKAN API",
       # ... etc
   }
   ```

2. **Owner Link**: Set `owner.link` to catalog's `link` if null

3. **Dates**: Extract from git history or file system metadata

### Manual/Semi-Automated Fixes

1. **Country Information**: 
   - Use NLP to extract country mentions from descriptions
   - Check URL patterns and domain information
   - Use existing country detection scripts if available

2. **Topics**: 
   - Run `python scripts/enrich.py enrich-topics` command
   - Use AI enrichment for better results

3. **Identifiers**: 
   - Manual research for Wikidata links
   - Can be done incrementally

## Files with Multiple Issues

Many files have multiple issues. Here are some examples:

- `opendataaeroterrahubarcgiscom.yaml`: Unknown country, empty topics, missing identifiers, missing export_standard, missing dates
- `kpisburhubarcgiscom.yaml`: Unknown country, generic owner name, empty topics, missing identifiers, missing export_standard, missing dates
- `datadownloadpageliberhubarcgiscom.yaml`: Unknown country, null owner link, generic owner name, empty topics, missing identifiers, missing export_standard, missing dates

## Next Steps

1. **Immediate Actions:**
   - Create script to auto-fill `export_standard` from endpoints
   - Create script to set `owner.link` to catalog link if null
   - Run `enrich_topics` command on files with empty topics

2. **Short-term Actions:**
   - Develop country extraction logic from descriptions
   - Improve owner names using descriptions
   - Add dates from git history

3. **Long-term Actions:**
   - Add Wikidata identifiers incrementally
   - Consider AI-powered enrichment for complex fields
   - Move files to proper country directories once countries are identified

## Detailed Report

A complete detailed report with all file-specific issues is available in:
`unknown_entities_analysis_report.txt`

