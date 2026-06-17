"""Regression guard for quality issue counts."""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from quality_regression import (  # noqa: E402
    DEFAULT_BASELINE,
    DEFAULT_REPORT,
    compare_to_baseline,
    load_issue_counts,
)


def test_baseline_file_present_and_well_formed():
    assert DEFAULT_BASELINE.exists(), "dataquality/baseline_counts.json is required"
    with DEFAULT_BASELINE.open("r", encoding="utf-8") as f:
        baseline = json.load(f)

    assert "generated_at" in baseline
    assert "total_records_analyzed" in baseline
    assert "by_priority" in baseline
    for priority in ("CRITICAL", "IMPORTANT", "MEDIUM", "LOW"):
        assert priority in baseline["by_priority"]


def test_quality_counts_do_not_regress():
    if not DEFAULT_REPORT.exists():
        pytest.skip("full_report.jsonl not present; run analyze-quality first")

    errors = compare_to_baseline()
    assert not errors, "Quality regression detected:\n" + "\n".join(errors)


def test_current_report_matches_baseline_when_unchanged():
    """Sanity check: baseline should reflect committed full_report.jsonl."""
    if not DEFAULT_REPORT.exists() or not DEFAULT_BASELINE.exists():
        pytest.skip("quality artifacts missing")

    with DEFAULT_BASELINE.open("r", encoding="utf-8") as f:
        baseline = json.load(f)

    by_priority, _ = load_issue_counts(DEFAULT_REPORT)
    for priority in ("CRITICAL", "IMPORTANT", "MEDIUM", "LOW"):
        current = by_priority.get(priority, 0)
        expected = baseline["by_priority"].get(priority, 0)
        assert current == expected, (
            f"{priority} count mismatch between full_report.jsonl ({current}) "
            f"and baseline ({expected}); run scripts/update_quality_baseline.py"
        )
