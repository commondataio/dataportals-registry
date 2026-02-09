<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

---

# dataportals-registry - Agent Guide

## Project Overview

The **dataportals-registry** is a comprehensive registry of data portals, catalogs, data repositories, and related data infrastructure. It serves as the first pillar of the open search engine project, aiming to create a unified discovery system for open data across the globe.

The registry collects and maintains structured metadata about:
- Open data portals
- Geoportals  
- Scientific data repositories
- Indicators catalogs
- Microdata catalogs
- Machine learning catalogs
- Data search engines
- API Catalogs
- Data marketplaces
- Other data infrastructure

As of February 2026, the registry contains **12,489 catalog entries** from countries worldwide, stored as individual YAML files and exported as JSONL, Parquet, and DuckDB formats.

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| **Language** | Python 3.9-3.12 |
| **Data Storage** | YAML files (individual catalog entries) |
| **Export Formats** | JSONL, Parquet, DuckDB |
| **Compression** | zstandard (zstd) |
| **CLI Framework** | typer |
| **Schema Validation** | Cerberus, pydantic |
| **Testing** | pytest with coverage (pytest-cov) |
| **Data Analysis** | pandas, DuckDB |
| **HTTP Client** | requests |
| **YAML Processing** | PyYAML |
| **Terminal UI** | rich (progress bars, tables) |
| **Web Scraping** | beautifulsoup4 |

---

## Project Structure

```
dataportals-registry/
├── data/
│   ├── entities/           # Verified catalog entries (YAML)
│   │   ├── US/             # Country code folders
│   │   │   ├── Federal/    # Federal-level catalogs
│   │   │   ├── US-CA/      # Subregion (state) catalogs
│   │   │   └── ...
│   │   └── ...             # 195+ countries/territories
│   ├── scheduled/          # Unverified/scheduled entries
│   ├── software/           # Software/platform definitions (YAML)
│   ├── schemes/            # JSON schemas for validation
│   │   ├── catalog.json    # Main catalog schema
│   │   └── software.json   # Software schema
│   ├── datasets/           # Generated exports
│   │   ├── catalogs.jsonl  # Main catalog export
│   │   ├── software.jsonl  # Software export
│   │   ├── full.jsonl      # Combined entities + scheduled
│   │   ├── *.zst           # Compressed versions
│   │   ├── datasets.duckdb # DuckDB database
│   │   └── full.parquet    # Parquet format
│   └── reference/          # Reference data and vocabularies
├── scripts/                # Python automation scripts
│   ├── builder.py          # Main build/validation CLI
│   ├── constants.py        # Constants and mappings
│   ├── re3data_enrichment.py  # Re3Data integration
│   ├── sync_ckan_ecosystem.py # CKAN ecosystem sync
│   ├── fix_*_issues.py     # Data quality fix scripts
│   └── ...
├── tests/                  # pytest test suite
│   ├── test_builder.py
│   ├── test_yaml.py
│   └── ...
├── dataquality/            # Quality analysis outputs
│   ├── full_report.txt     # Human-readable quality report
│   ├── primary_priority.jsonl  # Machine-readable issues
│   ├── countries/          # Per-country breakdowns
│   └── priorities/         # By priority level
├── openspec/               # OpenSpec for spec-driven dev
│   ├── AGENTS.md           # OpenSpec instructions
│   ├── project.md          # Project conventions
│   ├── specs/              # Current capability specs
│   └── changes/            # Proposed changes
├── devdocs/                # Development documentation
│   ├── quality-fix-workflow.md
│   ├── ckan_ecosystem_sync.md
│   └── scheduled-to-entities.md
├── requirements.txt        # Python dependencies
├── pytest.ini             # pytest configuration
├── CONTRIBUTING.md         # Contribution guidelines
└── README.md               # Project overview
```

---

## Catalog Entry Organization

### Directory Structure

Entities are organized hierarchically:

```
data/entities/
├── {COUNTRY_CODE}/              # ISO country code (US, GB, FR, etc.)
│   ├── Federal/                 # Federal/national level
│   ├── {SUBREGION_CODE}/        # State/province codes (US-CA, GB-SCT)
│   │   └── {CATALOG_TYPE}/      # Type subdirectory
│   │       └── {id}.yaml        # Individual catalog entry
│   └── {CATALOG_TYPE}/
│       └── {id}.yaml
```

### Catalog Type Subdirectories

| Type | Subdirectory | Description |
|------|--------------|-------------|
| Open data portal | `opendata/` | Default, government open data |
| Geoportal | `geo/` | Geographic/spatial data portals |
| Scientific data repository | `scientific/` | Research data repositories |
| Indicators catalog | `indicators/` | Statistical indicators |
| Microdata catalog | `microdata/` | Survey/microdata catalogs |
| Machine learning catalog | `ml/` | ML datasets and models |
| Data search engine | `search/` | Dataset search engines |
| API Catalog | `api/` | API directories |
| Data marketplace | `marketplace/` | Commercial data markets |
| Metadata catalog | `metadata/` | Metadata registries |
| Other | `other/` | Uncategorized |

---

## YAML Schema and Required Fields

### Required Fields

Every catalog entry MUST include these fields:

```yaml
id: catalogdatagov                    # Unique ID (matches filename)
uid: cdi00001616                      # Unique identifier (cdi######## format)
name: The Home of the U.S. Government Open Data  # Display name
link: https://catalog.data.gov        # URL to catalog
catalog_type: Open data portal        # One of the allowed types
access_mode:                          # List of access modes
  - open
status: active                        # active, inactive, or scheduled
software:                             # Software platform info
  id: ckan
  name: CKAN
owner:                                # Owner organization
  name: GSA Technology Transformation Services
  type: Central government            # Owner type
  location:                           # Geographic location
    country:
      id: US
      name: United States
coverage:                             # Geographic coverage
  - location:
      country:
        id: US
        name: United States
      level: 20                       # Geographic level
```

### File Naming Convention

- **Filename must match the `id` field exactly**
- Use only lowercase letters and numbers
- Remove special characters (dots, dashes, underscores)
- Example: `https://catalog.data.gov` → `id: catalogdatagov` → `catalogdatagov.yaml`

### Schema Validation

Schema is defined in `data/schemes/catalog.json` using Cerberus format. Key validation rules:

- `access_mode`: Must be list of "open" or "restricted"
- `catalog_type`: Must be one of the allowed types (see table above)
- `status`: Must be "active", "inactive", or "scheduled"
- `software`: Must have `id` and `name` subfields
- `owner`: Must have `name`, `type`, and `location` with country info
- `uid`: Format `cdi########` for entities, `temp########` for scheduled

---

## Build and Test Commands

### Essential Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Build all datasets (from YAML to JSONL/DuckDB)
python scripts/builder.py build

# Validate all YAML files against schema
python scripts/builder.py validate-yaml

# Run full test suite with coverage
pytest

# Run specific test file
pytest tests/test_builder.py -v

# Assign UIDs to new entries (run after adding entries)
python scripts/builder.py assign

# Analyze data quality
python scripts/builder.py analyze-quality

# Generate quality control metrics report
python scripts/builder.py quality-control
```

### Adding New Catalogs

**Method 1: Using CLI (recommended)**

```bash
python scripts/builder.py add-single \
  --url "https://example.com/data" \
  --software "ckan" \
  --catalog-type "Open data portal" \
  --name "Example Data Portal" \
  --country "US" \
  --scheduled
```

**Method 2: Manual YAML creation**

1. Create file in correct location: `data/entities/{COUNTRY}/{TYPE}/{id}.yaml`
2. Ensure `id` field matches filename
3. Run `python scripts/builder.py assign` to generate UID
4. Run `python scripts/builder.py validate-yaml` to verify

### Data Quality Workflow

```bash
# 1. Generate quality report
python scripts/builder.py analyze-quality

# 2. Review reports
cat dataquality/full_report.txt

# 3. Apply fixes (choose one method)
# Method A: Priority-based scripts
python scripts/fix_critical_issues.py
python scripts/fix_important_issues.py

# Method B: Generate Cursor commands
python scripts/generate_cursor_commands.py
# Then use scripts/update_all_issues.sh

# 4. Validate fixes
python scripts/builder.py validate-yaml

# 5. Re-run quality check
python scripts/builder.py analyze-quality
```

---

## Code Style Guidelines

### Python Code

- Follow **PEP 8** style guidelines
- Use meaningful variable and function names
- Add docstrings to functions and classes
- Keep functions focused and small
- Use type hints where appropriate
- Import ordering: standard library, third-party, local

### YAML Files

- Use **2 spaces for indentation** (no tabs)
- Use consistent formatting
- Keep lines under 100 characters when possible
- Use quotes for strings with special characters
- Use lists for multiple values
- **Filename must match the `id` field**

### Git Commits

- Write clear, descriptive commit messages
- Start with a verb: "Add", "Fix", "Update", "Remove"
- Make atomic commits (one logical change per commit)
- Reference issue numbers when applicable: `"Add example catalog (fixes #123)"`

---

## Testing Strategy

### Test Configuration

Configured in `pytest.ini`:
- Test files: `test_*.py` pattern
- Coverage for `scripts/` directory
- Markers: `unit`, `integration`, `slow`
- Reports: terminal, HTML, XML

### Running Tests

```bash
# Run all tests with coverage
pytest

# Run with verbose output
pytest -v

# Run specific test class
pytest tests/test_builder.py::TestLoadJsonl -v

# Run only unit tests
pytest -m unit

# Run without coverage (faster)
pytest --no-cov
```

### Writing Tests

- Place tests in `tests/` directory
- Use pytest fixtures from `conftest.py`
- Test both valid and invalid cases
- Mock external API calls
- Test file I/O with temporary directories

---

## Key Scripts Reference

### Main Build Script (`scripts/builder.py`)

CLI commands available:

| Command | Description |
|---------|-------------|
| `build` | Build JSONL datasets from YAML files |
| `validate-yaml` | Validate all YAML files against schema |
| `validate` | Validate JSONL against schema |
| `assign` | Assign UIDs to entries missing them |
| `add-single` | Add a single catalog via CLI |
| `add-list` | Add catalogs from a list file |
| `analyze-quality` | Run data quality analysis |
| `quality-control` | Generate quality metrics report |
| `export` | Export to CSV format |
| `stats` | Generate statistics tables |
| `report` | Report incomplete data |

### Enrichment Scripts

| Script | Purpose |
|--------|---------|
| `re3data_enrichment.py` | Enrich entries with Re3Data metadata |
| `sync_ckan_ecosystem.py` | Sync with CKAN ecosystem dataset |
| `enrich.py` | General enrichment utilities |
| `enrich_soft.py` | Software detection enrichment |

### Fix Scripts (Data Quality)

| Script | Priority |
|--------|----------|
| `fix_critical_issues.py` | CRITICAL |
| `fix_important_issues.py` | IMPORTANT |
| `fix_medium_issues.py` | MEDIUM |
| `fix_low_issues.py` | LOW |
| `fix_all_issues.py` | All priorities |
| `fix_duplicate_tags.py` | Tag duplicates |
| `fix_tag_hygiene.py` | Tag quality |
| `fix_software_id.py` | Software ID fixes |
| `fix_api_status_mismatch.py` | API status fixes |

---

## External Integrations

### Re3Data Enrichment

Enriches scientific repositories with metadata from re3data.org:

```bash
# Preview enrichment
python scripts/re3data_enrichment.py enrich --dry-run

# Apply enrichment
python scripts/re3data_enrichment.py enrich
```

### CKAN Ecosystem Sync

Discovers and adds CKAN sites from ecosystem.ckan.org:

```bash
# Preview sync
python scripts/sync_ckan_ecosystem.py --dry-run

# Sync and add to scheduled
python scripts/sync_ckan_ecosystem.py

# Sync and add to entities (verified)
python scripts/sync_ckan_ecosystem.py --entities
```

---

## OpenSpec Workflow

This project uses **OpenSpec** for spec-driven development of new features and breaking changes.

### When to Use OpenSpec

**Create a proposal for:**
- New features or capabilities
- Breaking changes (API, schema)
- Architecture or pattern changes
- Performance optimizations that change behavior
- Security pattern updates

**Work directly for:**
- Bug fixes (restore intended behavior)
- Typos, formatting, comments
- Adding/editing single catalog entries
- Dependency updates (non-breaking)
- Tests for existing behavior

### Quick OpenSpec Commands

```bash
# List active changes
openspec list

# List specifications
openspec list --specs

# Show change details
openspec show <change-id>

# Validate change
openspec validate <change-id> --strict

# Archive completed change
openspec archive <change-id> --yes
```

### OpenSpec Directory Structure

```
openspec/
├── AGENTS.md           # This file - OpenSpec instructions
├── project.md          # Project conventions and context
├── specs/              # Current capability specs
│   └── [capability]/
│       ├── spec.md     # Requirements and scenarios
│       └── design.md   # Technical patterns
└── changes/            # Proposed changes
    ├── [change-id]/
    │   ├── proposal.md # Why and what
    │   ├── tasks.md    # Implementation checklist
    │   ├── design.md   # Technical decisions (optional)
    │   └── specs/      # Delta specs
    │       └── [capability]/
    │           └── spec.md
    └── archive/        # Completed changes
```

See `openspec/AGENTS.md` for full OpenSpec instructions.

---

## Common Tasks for AI Agents

### Task: Add a New Catalog Entry

1. Check if catalog already exists in `data/entities/` or `data/scheduled/`
2. Use CLI to add: `python scripts/builder.py add-single --url ... --scheduled`
3. Or create YAML manually in correct location
4. Run `python scripts/builder.py assign` to generate UID
5. Run `python scripts/builder.py validate-yaml` to verify
6. Run `pytest` to ensure tests pass

### Task: Fix Data Quality Issues

1. Run `python scripts/builder.py analyze-quality`
2. Review `dataquality/full_report.txt` and `dataquality/primary_priority.jsonl`
3. Apply fixes using appropriate script or manual editing
4. Validate: `python scripts/builder.py validate-yaml`
5. Re-run quality analysis to confirm fixes

### Task: Update Schema or Validation

1. Read `data/schemes/catalog.json` for current schema
2. **If breaking change**: Create OpenSpec proposal first
3. Modify schema in `data/schemes/catalog.json`
4. Update validation logic in `scripts/builder.py` if needed
5. Run `python scripts/builder.py validate-yaml` to test
6. Run `pytest` to ensure tests pass

### Task: Add New Software Definition

1. Check existing software in `data/software/`
2. Create new YAML file with software metadata
3. Update `scripts/constants.py` if needed for mappings
4. Run `python scripts/builder.py build` to regenerate
5. Validate and test

---

## Security Considerations

- **No secrets in code**: Do not commit API keys, passwords, or tokens
- **URL validation**: All URLs are validated for proper format (scheme + netloc)
- **Input sanitization**: YAML parsing uses safe loading
- **HTTP requests**: Use requests library with proper timeout and SSL verification
- **File permissions**: YAML files should be world-readable (0644)

---

## License

- **Code**: MIT License
- **Data**: CC-BY 4.0 License

---

## References

- [README.md](README.md) - Project overview and data sources
- [CONTRIBUTING.md](CONTRIBUTING.md) - Full contribution guidelines
- [openspec/project.md](openspec/project.md) - Project conventions
- [openspec/AGENTS.md](openspec/AGENTS.md) - OpenSpec instructions
- [devdocs/quality-fix-workflow.md](devdocs/quality-fix-workflow.md) - Quality fix procedures
