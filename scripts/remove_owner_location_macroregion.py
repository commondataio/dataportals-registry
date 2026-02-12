#!/usr/bin/env python
"""Remove owner.location.macroregion from entity and scheduled YAML files.

The catalog schema allows macroregion only under coverage[].location, not under
owner.location. This script strips the invalid field from existing YAMLs so
validation passes.
"""
import os
import sys

import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)
ROOT_DIR = os.path.join(_REPO_ROOT, "data", "entities")
SCHEDULED_DIR = os.path.join(_REPO_ROOT, "data", "scheduled")


def process_file(filepath: str) -> bool:
    """Load YAML, remove owner.location.macroregion if present, save. Returns True if changed."""
    with open(filepath, "r", encoding="utf8") as f:
        record = yaml.load(f, Loader=Loader)
    if not record or not isinstance(record, dict):
        return False
    owner = record.get("owner")
    if not owner or not isinstance(owner, dict):
        return False
    location = owner.get("location")
    if not location or not isinstance(location, dict):
        return False
    if location.pop("macroregion", None) is None:
        return False
    with open(filepath, "w", encoding="utf8") as f:
        f.write(yaml.safe_dump(record, allow_unicode=True))
    return True


def main():
    changed = 0
    for label, root in [("entities", ROOT_DIR), ("scheduled", SCHEDULED_DIR)]:
        if not os.path.isdir(root):
            continue
        for dirpath, _dirnames, filenames in os.walk(root):
            for name in filenames:
                if not name.endswith(".yaml"):
                    continue
                filepath = os.path.join(dirpath, name)
                try:
                    if process_file(filepath):
                        changed += 1
                        print(os.path.relpath(filepath, _REPO_ROOT))
                except Exception as e:
                    print("Error %s: %s" % (filepath, e), file=sys.stderr)
    print("Removed owner.location.macroregion from %d file(s)." % changed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
