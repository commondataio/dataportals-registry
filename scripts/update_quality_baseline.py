#!/usr/bin/env python3
"""Regenerate dataquality/baseline_counts.json from current quality reports."""

from __future__ import annotations

import argparse
from pathlib import Path

from quality_regression import DEFAULT_BASELINE, DEFAULT_REPORT, write_baseline


def main() -> None:
    parser = argparse.ArgumentParser(description="Update quality regression baseline counts")
    parser.add_argument("--report", default=str(DEFAULT_REPORT), help="Path to full_report.jsonl")
    parser.add_argument("--baseline", default=str(DEFAULT_BASELINE), help="Path to baseline JSON")
    args = parser.parse_args()

    payload = write_baseline(
        baseline_path=Path(args.baseline),
        report_path=Path(args.report),
    )
    print(f"Updated baseline: {args.baseline}")
    print(f"  CRITICAL: {payload['by_priority']['CRITICAL']}")
    print(f"  IMPORTANT: {payload['by_priority']['IMPORTANT']}")
    print(f"  MEDIUM: {payload['by_priority']['MEDIUM']}")
    print(f"  LOW: {payload['by_priority']['LOW']}")


if __name__ == "__main__":
    main()
