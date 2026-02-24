#!/usr/bin/env python
"""
Verify ArcGIS Hub domains via DCAT US 1.1 feed.
Reads domains from dev/data/arcgishub_working.txt, fetches each domain's DCAT feed,
validates that datasets exist and at least one has a non-Web-Page distribution,
and writes verified domains to dev/data/arcgishub_verified.txt.
"""
import json
import os
from datetime import datetime

import requests
from requests.exceptions import RequestException

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)
INPUT_PATH = os.path.join(_REPO_ROOT, "dev", "data", "arcgishub_working.txt")
OUTPUT_PATH = os.path.join(_REPO_ROOT, "dev", "data", "arcgishub_verified.txt")

DCAT_URL_TEMPLATE = "https://{domain}/api/feed/dcat-us/1.1.json"
USER_AGENT = "Mozilla/5.0 (compatible; DataPortalsRegistry/1.0; +https://github.com/commondataio/dataportals-registry)"
TIMEOUT = 15


def load_domains(filepath: str) -> list[str]:
    """Load domains from file, skipping comments and empty lines."""
    domains = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            domains.append(line)
    return domains


def is_verified(data: dict) -> bool:
    """
    Check if DCAT feed has datasets and at least one has a non-Web-Page distribution.
    """
    datasets = data.get("dataset") or data.get("datasets") or []
    if not datasets:
        return False
    for ds in datasets:
        for dist in ds.get("distribution") or []:
            fmt = (dist.get("format") or "").strip()
            if fmt and fmt != "Web Page":
                return True
    return False


def fetch_dcat(domain: str) -> dict | None:
    """Fetch DCAT US 1.1 JSON for a domain. Returns None on failure."""
    url = DCAT_URL_TEMPLATE.format(domain=domain)
    try:
        response = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=TIMEOUT,
            verify=True,
        )
        response.raise_for_status()
        return response.json()
    except (RequestException, json.JSONDecodeError):
        return None


def main():
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    domains = load_domains(INPUT_PATH)
    verified = []

    try:
        import tqdm
        iterator = tqdm.tqdm(domains, desc="Verifying")
    except ImportError:
        iterator = domains

    for domain in iterator:
        data = fetch_dcat(domain)
        if data and is_verified(data):
            print(f"Verified: {domain}")
            verified.append(domain)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write("# ArcGIS Hub domains verified via DCAT US 1.1 (datasets with non-Web-Page distribution)\n")
        f.write(f"# Total: {len(verified)} domains\n")
        f.write("# Source: arcgishub_working.txt\n")
        f.write(f"# Verified: {datetime.now().strftime('%Y-%m-%d')}\n")
        f.write("\n")
        for domain in verified:
            f.write(f"{domain}\n")

    print(f"Domains checked: {len(domains)}")
    print(f"Verified: {len(verified)}")
    print(f"Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
