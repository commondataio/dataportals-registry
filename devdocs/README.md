# Development Documentation

This directory contains development tools, analysis scripts, and quality reports for the dataportals-registry project.

## Analysis Tools

### `analyze_duplicates_and_errors.py`

Comprehensive data quality analysis tool that scans all YAML records in the repository.

**Usage:**
```bash
python devdocs/analyze_duplicates_and_errors.py
```

**What it checks:**
- **Duplicate UID's**: Identifies records with duplicate unique identifiers
- **Duplicate ID's**: Finds records with duplicate `id` fields
- **Missing Required Fields**: Detects records missing required schema fields
- **Empty Required Fields**: Finds records with empty required fields
- **Filename Mismatches**: Identifies files where the `id` field doesn't match the filename
- **Empty Files**: Finds empty or invalid YAML files
- **Parsing Errors**: Detects YAML syntax errors

**Output:**
- `duplicates_and_errors_report.json` - Full JSON report with all findings
- `duplicates_and_errors_report.md` - Human-readable Markdown report
- `duplicates_and_errors_summary.txt` - Quick text summary

**Last Run Results:**
- Total files scanned: 10,299
- Duplicate UID's: 0
- Duplicate ID's: 7
- Missing required fields: 204 (mostly missing `uid`)
- Filename mismatches: 63
- Empty files: 1

### Other Analysis Scripts

- `analyze_unknown_entities.py` - Analyzes entities with unknown country information
- `summarize_software_metadata.py` - Summarizes software metadata
- `update_software_metadata.py` - Updates software metadata

## Reports

### `duplicates_and_errors_report.md`
Comprehensive report on duplicate identifiers and critical errors found in the registry.

### `UNKNOWN_ENTITIES_ANALYSIS.md`
Analysis of entities with unknown country information and recommendations for improvement.

### `NAMING_ISSUES_SUMMARY.md`
Summary of naming inconsistencies and filename mismatches.

### `MISSING_SOFTWARE_REPORT.md`
Report on missing software information in entity records.

### `ANALYSIS_AND_IMPROVEMENTS.md`
General analysis and improvement recommendations.

## Running Analysis

To run all analysis tools and generate fresh reports:

```bash
# From project root
cd devdocs
python analyze_duplicates_and_errors.py
```

Reports will be generated in the `devdocs/` directory.

## Contributing

When adding new analysis tools:
1. Place scripts in the `devdocs/` directory
2. Follow the naming convention: `analyze_*.py` for analysis scripts
3. Generate reports in multiple formats (JSON, Markdown, text) when possible
4. Update this README with tool descriptions

