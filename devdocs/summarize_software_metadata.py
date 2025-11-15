#!/usr/bin/env python3
"""Summarize software metadata completeness."""

import yaml
import os
from pathlib import Path

software_dir = Path('data/software')
total = 0
with_website = 0
with_api_info = 0
with_metadata_support = 0

for root, dirs, files in os.walk(software_dir):
    for file in files:
        if file.endswith('.yaml') and not file.startswith('_'):
            total += 1
            filepath = Path(root) / file
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    record = yaml.safe_load(f)
                    if record and 'id' in record:
                        if record.get('website') and record.get('website').strip():
                            with_website += 1
                        if record.get('has_api') and record.get('has_api') != 'Uncertain':
                            with_api_info += 1
                        # Check if any metadata_support field is not Uncertain
                        metadata_support = record.get('metadata_support', {})
                        if any(v != 'Uncertain' for v in metadata_support.values() if isinstance(v, str)):
                            with_metadata_support += 1
            except Exception as e:
                print(f"Error reading {filepath}: {e}")

print("=" * 80)
print("SOFTWARE METADATA SUMMARY")
print("=" * 80)
print(f"Total software records: {total}")
print(f"Records with website URLs: {with_website} ({with_website*100//total if total > 0 else 0}%)")
print(f"Records with API info (has_api != Uncertain): {with_api_info} ({with_api_info*100//total if total > 0 else 0}%)")
print(f"Records with metadata support info: {with_metadata_support} ({with_metadata_support*100//total if total > 0 else 0}%)")
print("=" * 80)

