# Naming Issues Analysis Summary

## Overview

Analysis of **10,100 YAML files** in `data/entities` and `data/scheduled` directories revealed several categories of naming issues that need attention.

## Key Findings

### 1. Duplicate IDs (7 cases)
**Critical Issue** - IDs should be unique identifiers

Found 7 duplicate IDs across entities and scheduled directories:

1. **`wwwgeoboundariesorg`** - Appears in both entities and scheduled
2. **`erddapbcdcno`** - Different files with same ID
3. **`wwwimagegeomaticsgovntca`** - Appears in both entities and scheduled
4. **`wwwstatistikat`** - Same ID used for indicators and geo catalogs
5. **`datoscdmxgobmx`** - Same ID used for opendata and scientific catalogs
6. **`servicesbgrde`** - Appears in two different geo files
7. **`webgateeceuropaeu`** - Appears in both entities and scheduled

**Impact**: These duplicates can cause conflicts in systems that rely on unique IDs.

### 2. Duplicate Names (120 cases)
**Medium Priority** - Names can legitimately be the same, but many indicate duplicates or missing context

Most common duplicates:
- "ArcGIS REST Services Directory" (16 occurrences)
- "GeoServer Data Catalog" (6 occurrences)
- "Città Metropolitana di Firenze metadata catalogue" (4 occurrences)
- "Open SDG Data Hub" (3 occurrences)
- "Maryland's GIS Data Catalog" (3 occurrences)

**Impact**: Many of these are likely the same catalog listed multiple times, or need distinguishing information added.

### 3. Duplicate UIDs (0 cases)
✅ **No issues found** - All UIDs are unique.

### 4. Anomalous Names (469 cases)
**Medium Priority** - Names with formatting issues or unusual patterns

Common issues found:
- **Multiple consecutive spaces** (e.g., "Linkoping  Statistikdatabas")
- **Unusual characters** (e.g., "–", "!", "|", HTML tags like `<br>`)
- **Very long names** (>200 chars) - One example: 235 character name that appears to be an error message
- **All uppercase** (e.g., "MID-OHIO OPEN DATA (MOOD)", "NASA CMR STAC")
- **Leading/trailing whitespace** (e.g., "Open Data City of Johnson City, TN ")
- **Contains file extensions** (e.g., "pgc-opendata-dems.s3.us-west-2.amazonaws.com/pgc-data-stac.json")
- **Looks like URLs/paths** instead of catalog names

**Impact**: These issues affect data quality and display, but don't break functionality.

### 5. Anomalous IDs (0 cases)
✅ **No issues found** - All IDs follow proper conventions.

## Files Generated

1. **`naming_issues_report.txt`** - Detailed report with all findings
2. **`naming_issues_fix_guide.txt`** - Comprehensive guide on how to fix each type of issue
3. **`scripts/analyze_naming_issues.py`** - Analysis script (can be re-run anytime)

## How to Use

### View the Reports
```bash
# View the detailed report
cat naming_issues_report.txt

# View the fix guide
cat naming_issues_fix_guide.txt
```

### Re-run the Analysis
```bash
python3 scripts/analyze_naming_issues.py
```

## Priority Recommendations

### High Priority (Fix First)
1. **Fix duplicate IDs** - These can cause system conflicts
   - Review each case to determine if catalogs are the same (merge) or different (make IDs unique)
   - See `naming_issues_fix_guide.txt` section 1 for detailed instructions

### Medium Priority
2. **Review duplicate names** - Many indicate duplicate entries
   - Check if same catalog is listed multiple times
   - Add distinguishing information if catalogs are different
   - See `naming_issues_fix_guide.txt` section 2 for detailed instructions

3. **Fix anomalous names** - Improve data quality
   - Start with most common issues: multiple spaces, trailing whitespace
   - Fix unusual characters and very long names
   - See `naming_issues_fix_guide.txt` section 4 for detailed instructions

### Low Priority
4. **General cleanup** - Ongoing maintenance
   - Regular re-runs of the analysis script
   - Fix issues as they're discovered

## Quick Fix Examples

### Fix Duplicate ID
```yaml
# Before (duplicate)
id: wwwgeoboundariesorg

# After (make unique)
id: wwwgeoboundariesorg-us  # or add location/type suffix
```

### Fix Multiple Spaces
```yaml
# Before
name: "Linkoping  Statistikdatabas"

# After
name: "Linkoping Statistikdatabas"
```

### Fix Trailing Whitespace
```yaml
# Before
name: "Open Data City of Johnson City, TN "

# After
name: "Open Data City of Johnson City, TN"
```

### Fix Unusual Characters
```yaml
# Before
name: "Khyber Pakhtunkhwa Open Data Portal!"

# After
name: "Khyber Pakhtunkhwa Open Data Portal"
```

## Next Steps

1. Review the detailed report: `naming_issues_report.txt`
2. Read the fix guide: `naming_issues_fix_guide.txt`
3. Start with high-priority duplicate IDs
4. Work through duplicate names systematically
5. Fix anomalous names (can be automated for many cases)
6. Re-run analysis periodically to track progress

