"""Helpers for quality-count regression checks."""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPORT = REPO_ROOT / "dataquality" / "full_report.jsonl"
DEFAULT_BASELINE = REPO_ROOT / "dataquality" / "baseline_counts.json"
DEFAULT_FULL_REPORT_TXT = REPO_ROOT / "dataquality" / "full_report.txt"


def load_issue_counts(report_path: Path = DEFAULT_REPORT) -> Tuple[Counter, Counter]:
    """Return (by_priority, by_issue_type) counters from full_report.jsonl."""
    by_priority: Counter = Counter()
    by_issue_type: Counter = Counter()
    if not report_path.exists():
        raise FileNotFoundError(f"Quality report not found: {report_path}")

    with report_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            issue = json.loads(line)
            priority = issue.get("priority", "MEDIUM")
            by_priority[priority] += 1
            by_issue_type[issue.get("issue_type", "UNKNOWN")] += 1
    return by_priority, by_issue_type


def parse_total_records_analyzed(full_report_txt: Path = DEFAULT_FULL_REPORT_TXT) -> int | None:
    """Parse total records analyzed from full_report.txt header."""
    if not full_report_txt.exists():
        return None
    match = re.search(r"Total Records Analyzed:\s*(\d+)", full_report_txt.read_text(encoding="utf-8"))
    return int(match.group(1)) if match else None


def build_baseline_payload(
    report_path: Path = DEFAULT_REPORT,
    full_report_txt: Path = DEFAULT_FULL_REPORT_TXT,
) -> dict:
    by_priority, by_issue_type = load_issue_counts(report_path)
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": str(report_path.relative_to(REPO_ROOT)),
        "total_records_analyzed": parse_total_records_analyzed(full_report_txt),
        "by_priority": {
            "CRITICAL": by_priority.get("CRITICAL", 0),
            "IMPORTANT": by_priority.get("IMPORTANT", 0),
            "MEDIUM": by_priority.get("MEDIUM", 0),
            "LOW": by_priority.get("LOW", 0),
        },
        "by_issue_type": dict(sorted(by_issue_type.items())),
    }
    return payload


def write_baseline(
    baseline_path: Path = DEFAULT_BASELINE,
    report_path: Path = DEFAULT_REPORT,
    full_report_txt: Path = DEFAULT_FULL_REPORT_TXT,
) -> dict:
    payload = build_baseline_payload(report_path, full_report_txt)
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    with baseline_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return payload


def compare_to_baseline(
    baseline_path: Path = DEFAULT_BASELINE,
    report_path: Path = DEFAULT_REPORT,
) -> list[str]:
    """Return human-readable regression messages (empty if OK)."""
    if not baseline_path.exists():
        return [f"Missing baseline file: {baseline_path}"]

    with baseline_path.open("r", encoding="utf-8") as f:
        baseline = json.load(f)

    current_priority, _ = load_issue_counts(report_path)
    baseline_priority = baseline.get("by_priority", {})
    errors: list[str] = []

    for priority in ("CRITICAL", "IMPORTANT"):
        current = current_priority.get(priority, 0)
        allowed = baseline_priority.get(priority, 0)
        if current > allowed:
            delta = current - allowed
            top_types = _top_issue_types_for_priority(report_path, priority)
            detail = f" ({', '.join(top_types)})" if top_types else ""
            errors.append(
                f"{priority} issues increased: {allowed} -> {current} (+{delta}){detail}"
            )

    return errors


def _top_issue_types_for_priority(report_path: Path, priority: str, limit: int = 5) -> list[str]:
    """Return top issue types for a priority tier from the current report."""
    type_counts: Counter = Counter()
    with report_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            issue = json.loads(line)
            if issue.get("priority") == priority:
                type_counts[issue.get("issue_type", "UNKNOWN")] += 1
    return [f"{issue_type}:{count}" for issue_type, count in type_counts.most_common(limit)]
