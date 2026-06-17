#!/usr/bin/env python3
"""
Generate ISO 3166-2 subregions reference CSV from pycountry.

Output schema is intentionally compatible with existing consumers:
  code,subdivision_name
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import pycountry


def build_rows() -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for subdivision in pycountry.subdivisions:
        code = (getattr(subdivision, "code", "") or "").strip()
        name = (getattr(subdivision, "name", "") or "").strip()
        if code and name:
            rows.append((code, name))
    # Stable order for deterministic diffs
    rows.sort(key=lambda item: item[0])
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default="data/reference/subregions/ISO3166-2.CSV",
        help="Output CSV path relative to repository root.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    output_path = (repo_root / args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = build_rows()
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["code", "subdivision_name"])
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {output_path}")


if __name__ == "__main__":
    main()
