# Quality fix workflow

This document describes how to fix data quality issues reported by the registry's quality analysis, including using Cursor or other agentic IDEs.

## 1. Generate the quality report

From the repository root:

```bash
python scripts/builder.py analyze-quality
```

Output is written to `dataquality/`:

- **full_report.txt** – Human-readable summary and issues by type
- **primary_priority.jsonl** – One JSON object per record with issues (used by fix automation)
- **countries/** – Per-country breakdowns
- **priorities/** – CRITICAL, IMPORTANT, MEDIUM, LOW
- **rules/** – Per-rule type details

## 2. Inspect issues

- Open `dataquality/full_report.txt` for an overview.
- For scripted or agent-driven fixes, use `dataquality/primary_priority.jsonl`: each line is a JSON object with `record_id`, `file_path`, and `issues` (list of issue objects with `issue_type`, `field`, `current_value`, `suggested_action`, etc.).

## 3. Apply fixes

**Option A – Priority-based fix scripts**

Run the existing fix scripts by priority (see `scripts/`):

- `fix_critical_issues.py`
- `fix_important_issues.py`
- `fix_medium_issues.py`
- `fix_low_issues.py`

Or run all: `fix_all_issues.py` (may use Cursor automation).

**Option B – Cursor / agent-driven fixes**

1. Generate prompts or commands from the primary priority file:
   ```bash
   python scripts/generate_cursor_commands.py
   ```
   This produces `scripts/update_all_issues.sh` and `scripts/all_issues_prompts.txt`.

2. Use the prompts or run the generated script with Cursor (or another agent) to apply edits to the YAML files referenced in `primary_priority.jsonl`.

**Option C – Manual edits**

Edit the YAML files listed in the report according to the `suggested_action` for each issue.

## 4. Verify

After making changes:

1. Validate schema:
   ```bash
   python scripts/builder.py validate-yaml
   ```

2. Re-run quality analysis to confirm issues are resolved or reduced:
   ```bash
   python scripts/builder.py analyze-quality
   ```

3. Run the test suite:
   ```bash
   pytest tests -v
   ```

## Summary

| Step | Command / action |
|------|------------------|
| Generate report | `python scripts/builder.py analyze-quality` |
| Inspect | `dataquality/full_report.txt`, `dataquality/primary_priority.jsonl` |
| Fix | `scripts/fix_*_issues.py` or `generate_cursor_commands.py` + Cursor |
| Validate | `python scripts/builder.py validate-yaml` |
| Re-check quality | `python scripts/builder.py analyze-quality` |
