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
    compare_to_baseline_with_warnings,
    is_enrichment_issue_type,
    load_issue_counts,
    load_track_counts,
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


def test_enrichment_issue_classification():
    assert is_enrichment_issue_type("SOFTWARE_EXPECTED_ENDPOINTS_MISSING_CKAN")
    assert is_enrichment_issue_type("MISSING_TOPICS")
    assert not is_enrichment_issue_type("DUPLICATE_LINK_NORMALIZED")
    assert not is_enrichment_issue_type("INVALID_URL")


def test_quality_counts_do_not_regress():
    if not DEFAULT_REPORT.exists():
        pytest.skip("full_report.jsonl not present; run analyze-quality first")

    errors = compare_to_baseline()
    assert not errors, "Quality regression detected:\n" + "\n".join(errors)


def test_enrichment_growth_is_warning_by_default(tmp_path):
    report = tmp_path / "report.jsonl"
    baseline = tmp_path / "baseline.json"
    report.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "issue_type": "SOFTWARE_EXPECTED_ENDPOINTS_MISSING_CKAN",
                        "priority": "MEDIUM",
                    }
                ),
                json.dumps(
                    {
                        "issue_type": "SOFTWARE_EXPECTED_ENDPOINTS_MISSING_CKAN",
                        "priority": "MEDIUM",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    baseline.write_text(
        json.dumps(
            {
                "by_priority": {
                    "CRITICAL": 0,
                    "IMPORTANT": 0,
                    "MEDIUM": 1,
                    "LOW": 0,
                },
                "by_track": {
                    "integrity": {
                        "CRITICAL": 0,
                        "IMPORTANT": 0,
                        "MEDIUM": 0,
                        "LOW": 0,
                    },
                    "enrichment": {
                        "CRITICAL": 0,
                        "IMPORTANT": 0,
                        "MEDIUM": 1,
                        "LOW": 0,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    errors, warnings = compare_to_baseline_with_warnings(
        baseline_path=baseline, report_path=report
    )
    assert errors == []
    assert any("Enrichment MEDIUM" in w for w in warnings)


def test_integrity_growth_fails(tmp_path):
    report = tmp_path / "report.jsonl"
    baseline = tmp_path / "baseline.json"
    report.write_text(
        json.dumps({"issue_type": "DUPLICATE_RECORD_ID", "priority": "CRITICAL"}) + "\n",
        encoding="utf-8",
    )
    baseline.write_text(
        json.dumps(
            {
                "by_track": {
                    "integrity": {
                        "CRITICAL": 0,
                        "IMPORTANT": 0,
                        "MEDIUM": 0,
                        "LOW": 0,
                    },
                    "enrichment": {
                        "CRITICAL": 0,
                        "IMPORTANT": 0,
                        "MEDIUM": 0,
                        "LOW": 0,
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    errors = compare_to_baseline(baseline_path=baseline, report_path=report)
    assert errors
    assert "Integrity CRITICAL" in errors[0]


def test_current_report_matches_baseline_when_unchanged():
    """Sanity check: integrity CRITICAL/IMPORTANT in the report match the baseline.

    Enrichment MEDIUM/LOW counts are not exact-matched here: they vary with
    software-endpoint coverage and are warnings in the regression guard.
    """
    if not DEFAULT_REPORT.exists() or not DEFAULT_BASELINE.exists():
        pytest.skip("quality artifacts missing")

    with DEFAULT_BASELINE.open("r", encoding="utf-8") as f:
        baseline = json.load(f)

    by_priority, _ = load_issue_counts(DEFAULT_REPORT)
    for priority in ("CRITICAL", "IMPORTANT"):
        current = by_priority.get(priority, 0)
        expected = baseline["by_priority"].get(priority, 0)
        assert current == expected, (
            f"{priority} count mismatch between full_report.jsonl ({current}) "
            f"and baseline ({expected}); run scripts/update_quality_baseline.py"
        )

    if "by_track" in baseline:
        current_tracks = load_track_counts(DEFAULT_REPORT)
        for priority in ("CRITICAL", "IMPORTANT"):
            assert current_tracks["integrity"][priority] == baseline["by_track"]["integrity"][priority], (
                f"Integrity {priority} count mismatch between full_report.jsonl "
                f"({current_tracks['integrity'][priority]}) and baseline "
                f"({baseline['by_track']['integrity'][priority]}); "
                "run scripts/update_quality_baseline.py"
            )
