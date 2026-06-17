#!/usr/bin/env python
"""One-time migration: add dublin_core, iso19115, rdf_sparql to software profiles."""

from __future__ import annotations

import os
import sys

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

import yaml

_SOFTWARE_ROOT = os.path.join(os.path.dirname(__file__), "..", "data", "software")

METADATA_SUPPORT_ORDER = [
    "ckan_api",
    "csw",
    "custom_api",
    "dcat",
    "dublin_core",
    "iso19115",
    "oai-pmh",
    "ogcrecords",
    "openaire",
    "opensearch",
    "rdf_sparql",
    "schema-org",
    "sdmx",
    "stac",
    "swordapi",
    "wcs",
    "wfs",
    "wms",
]

RDF_SPARQL_STRONG = frozenset(
    {"triplydb", "fusionregistry", "aristotlemdr", "ontoportal", "publishmydata"}
)


def infer_dublin_core(ms: dict) -> str:
    oai = ms.get("oai-pmh", "")
    if oai == "Yes":
        return "Yes"
    if oai == "No":
        return "No"
    if oai in ("Plugin only", "Limited", "Compatible"):
        return "Limited"
    return "Uncertain"


def infer_iso19115(ms: dict) -> str:
    csw = ms.get("csw", "")
    if csw in ("Yes", "Compatible"):
        return "Yes"
    if csw in ("Plugin only", "Limited"):
        return "Limited"
    if csw == "No":
        return "No"
    return "Uncertain"


def infer_rdf_sparql(ms: dict, software_id: str) -> str:
    if software_id == "custom":
        return "No"
    if software_id in RDF_SPARQL_STRONG:
        return "Yes"
    dcat = ms.get("dcat", "")
    if dcat in ("Yes", "Plugin only"):
        return "Yes"
    if dcat == "Limited":
        return "Limited"
    sch = ms.get("schema-org", "")
    if sch in ("Yes", "Limited", "Plugin only"):
        return "Limited"
    if ms.get("openaire") == "Yes":
        return "Limited"
    if dcat == "No" and sch == "No":
        return "No"
    return "Uncertain"


def reorder_metadata_support(ms: dict) -> dict:
    out = {}
    for k in METADATA_SUPPORT_ORDER:
        if k in ms:
            out[k] = ms[k]
    for k, v in ms.items():
        if k not in out:
            out[k] = v
    return out


def migrate_file(path: str, dry_run: bool) -> bool:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.load(f, Loader=Loader)
    if not data or data.get("type") != "Software":
        return False
    ms = data.get("metadata_support")
    if not isinstance(ms, dict):
        return False

    sid = data.get("id", "")
    orig = dict(ms)
    changed = False
    if "dublin_core" not in ms:
        ms["dublin_core"] = infer_dublin_core(orig)
        changed = True
    if "iso19115" not in ms:
        ms["iso19115"] = infer_iso19115(orig)
        changed = True
    if "rdf_sparql" not in ms:
        ms["rdf_sparql"] = infer_rdf_sparql(orig, sid)
        changed = True

    data["metadata_support"] = reorder_metadata_support(ms)

    if not changed:
        return False

    if dry_run:
        print(f"would update: {path}")
        return True

    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(
            data,
            f,
            Dumper=Dumper,
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
        )
    return True


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    n = 0
    for root, _dirs, files in os.walk(_SOFTWARE_ROOT):
        for name in files:
            if not name.endswith(".yaml") or name == "types.yaml":
                continue
            path = os.path.join(root, name)
            if migrate_file(path, dry_run):
                n += 1
    print(f"Updated {n} files (dry_run={dry_run})")


if __name__ == "__main__":
    main()
