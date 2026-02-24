#!/usr/bin/env python
"""
Fetch host data from Censys API for a fixed list of IPs, save as JSON under
dev/data/opendatasoft/, then extract all domains from dns.names and write
a deduplicated list to dev/data/opendatasoft_extracted.txt.

Authentication (one of the following when fetching):
- CENSYS_API_TOKEN: Personal Access Token for Censys Platform API v3 (Bearer).
- CENSYS_API_ID + CENSYS_API_SECRET: Legacy Search API v2 (Basic auth).

Optional for v3: CENSYS_ORGANIZATION_ID to associate requests with a Censys organization.

Not needed if all JSON files already exist.
"""
import json
import os
import sys
import time

import requests

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)
OPENDATASOFT_DIR = os.path.join(_REPO_ROOT, "dev", "data", "opendatasoft")
OUTPUT_PATH = os.path.join(_REPO_ROOT, "dev", "data", "opendatasoft_extracted.txt")

# Legacy Search API v2 (Basic auth: API ID + secret)
CENSYS_V2_BASE = "https://search.censys.io/api/v2/hosts"
# Platform API v3 (Bearer token / PAT)
CENSYS_V3_BASE = "https://api.platform.censys.io/v3/global/asset/host"
CENSYS_V3_ACCEPT = "application/vnd.censys.api.v3.host.v1+json"
# Rate limit: small delay between API calls (seconds)
API_DELAY_SECONDS = 1.0

# 12 IPs for OpenDataSoft host lookup (fixed list per plan)
IPS = [
    "109.232.232.161",
    "148.253.127.165",
    "18.199.232.122",
    "18.200.140.238",
    "3.122.30.148",
    "5.104.97.33",
    "52.211.64.165",
    "54.234.220.90",
    "52.63.171.240",
    "54.79.171.70",
    "80.247.7.214",
    "98.85.149.186",
]


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _log_fetch_error(ip: str, e: requests.RequestException) -> None:
    print(f"Fetch failed for {ip}: {e}", file=sys.stderr)
    if hasattr(e, "response") and e.response is not None:
        try:
            body = e.response.text
            if len(body) > 200:
                body = body[:200] + "..."
            print(f"  Response: {body}", file=sys.stderr)
        except Exception:
            pass


def fetch_host_v2(ip: str, api_id: str, api_secret: str) -> dict | None:
    """Legacy Search API v2 (Basic auth). Returns host object with top-level dns.names."""
    url = f"{CENSYS_V2_BASE}/{ip}"
    try:
        r = requests.get(url, auth=(api_id, api_secret), timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        _log_fetch_error(ip, e)
        return None


def fetch_host_v3(ip: str, token: str, organization_id: str | None = None) -> dict | None:
    """Platform API v3 (Bearer token). Returns host resource (same shape as v2 for dns.names)."""
    url = f"{CENSYS_V3_BASE}/{ip}"
    params = {}
    if organization_id and organization_id.strip():
        params["organization_id"] = organization_id.strip()
    try:
        r = requests.get(
            url,
            params=params or None,
            headers={
                "Authorization": f"Bearer {token.strip()}",
                "Accept": CENSYS_V3_ACCEPT,
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        # v3 wraps in result.resource; return resource so saved JSON matches v2 shape
        if isinstance(data.get("result"), dict) and "resource" in data["result"]:
            return data["result"]["resource"]
        return data
    except requests.RequestException as e:
        _log_fetch_error(ip, e)
        return None


def load_or_fetch(
    ip: str,
    json_path: str,
    *,
    api_token: str | None = None,
    api_id: str | None = None,
    api_secret: str | None = None,
    organization_id: str | None = None,
) -> dict | None:
    if os.path.isfile(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"Failed to load {json_path}: {e}", file=sys.stderr)
            return None
    if api_token:
        data = fetch_host_v3(ip, api_token, organization_id=organization_id)
    elif api_id and api_secret:
        data = fetch_host_v2(ip, api_id, api_secret)
    else:
        print(
            "Set CENSYS_API_TOKEN (v3) or both CENSYS_API_ID and CENSYS_API_SECRET (v2); cannot fetch " + ip,
            file=sys.stderr,
        )
        return None
    if data is None:
        return None
    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except OSError as e:
        print(f"Failed to write {json_path}: {e}", file=sys.stderr)
    return data


def extract_dns_names(data: dict) -> list[str]:
    # Support v3 envelope: result.resource.dns.names
    if isinstance(data.get("result"), dict) and "resource" in data["result"]:
        data = data["result"]["resource"]
    names = data.get("dns", {}).get("names") or []
    return [n for n in names if isinstance(n, str) and n.strip()]


def main() -> int:
    ensure_dir(OPENDATASOFT_DIR)
    api_token = os.environ.get("CENSYS_API_TOKEN")
    api_id = os.environ.get("CENSYS_API_ID")
    api_secret = os.environ.get("CENSYS_API_SECRET")
    organization_id = os.environ.get("CENSYS_ORGANIZATION_ID")
    can_fetch = bool(api_token or (api_id and api_secret))

    all_domains: set[str] = set()
    failed_ips: list[str] = []

    for i, ip in enumerate(IPS):
        json_path = os.path.join(OPENDATASOFT_DIR, f"{ip}.json")
        if not os.path.isfile(json_path) and can_fetch and i > 0:
            time.sleep(API_DELAY_SECONDS)
        data = load_or_fetch(
            ip,
            json_path,
            api_token=api_token,
            api_id=api_id,
            api_secret=api_secret,
            organization_id=organization_id,
        )
        if data is None:
            failed_ips.append(ip)
            continue
        for name in extract_dns_names(data):
            all_domains.add(name.strip())

    if failed_ips:
        print(f"Skipped IPs ({len(failed_ips)}): {', '.join(failed_ips)}", file=sys.stderr)

    sorted_domains = sorted(all_domains)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write("# Domains from dns.names in dev/data/opendatasoft/*.json\n")
        for d in sorted_domains:
            f.write(d + "\n")

    print(f"Wrote {len(sorted_domains)} unique domains to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
