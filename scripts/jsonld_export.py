#!/usr/bin/env python3
"""Export catalog JSONL records as JSON-LD using catalog.context.jsonld."""

from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CONTEXT_PATH = REPO_ROOT / "data" / "schemes" / "catalog.context.jsonld"
DEFAULT_INPUT = REPO_ROOT / "data" / "datasets" / "catalogs.jsonl"
DEFAULT_OUTPUT = REPO_ROOT / "data" / "datasets" / "catalogs.jsonld"


def load_context() -> dict:
    with CONTEXT_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def record_to_jsonld(record: dict, context_document: dict) -> dict:
    """Frame a catalog record with JSON-LD @context and @type."""
    framed = dict(record)
    framed["@context"] = context_document["@context"]
    framed["@type"] = "dcat:DataCatalog"
    uid = record.get("uid") or record.get("id")
    if uid:
        framed["@id"] = f"urn:cdi:catalog:{uid}"
    return framed


def export_catalogs_jsonld(
    input_path: Path = DEFAULT_INPUT,
    output_path: Path = DEFAULT_OUTPUT,
    context_path: Path = CONTEXT_PATH,
) -> int:
    """Write JSON-LD export; returns number of records exported."""
    with context_path.open("r", encoding="utf-8") as handle:
        context_document = json.load(handle)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with input_path.open("r", encoding="utf-8") as src, output_path.open(
        "w", encoding="utf-8"
    ) as dst:
        for line in src:
            if not line.strip():
                continue
            record = json.loads(line)
            dst.write(
                json.dumps(
                    record_to_jsonld(record, context_document),
                    ensure_ascii=False,
                )
                + "\n"
            )
            count += 1
    return count
