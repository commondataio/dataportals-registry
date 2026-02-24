#!/usr/bin/env python
"""
Compare Censys .Stat Suite domains against the registry and identify missing ones.
Output: dev/data/statsuite_for_review.txt
"""
import json
import os
import re
from urllib.parse import urlparse

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)
# Note: file is censis_statsuite_list.json (typo in original)
CENSYS_PATH = os.path.join(_REPO_ROOT, "dev", "data", "censis_statsuite_list.json")
FULL_JSONL_PATH = os.path.join(_REPO_ROOT, "data", "datasets", "full.jsonl")
OUTPUT_PATH = os.path.join(_REPO_ROOT, "dev", "data", "statsuite_for_review.txt")

# IP address pattern - exclude from domain comparison (registry uses hostnames)
IP_PATTERN = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")


def normalize_domain(hostname: str) -> str:
    """Normalize domain for comparison: lowercase, strip www."""
    if not hostname:
        return ""
    hostname = hostname.lower().strip()
    if hostname.startswith("www."):
        hostname = hostname[4:]
    return hostname


def extract_domain_from_url(url: str) -> str | None:
    """Extract hostname from URL."""
    if not url or not isinstance(url, str):
        return None
    try:
        parsed = urlparse(url if url.startswith("http") else f"https://{url}")
        return parsed.netloc.lower() if parsed.netloc else None
    except Exception:
        return None


def is_ip_address(key: str) -> bool:
    """Check if key is an IP address."""
    return bool(IP_PATTERN.match(key))


def load_registry_domains() -> set[str]:
    """Load all domains from full.jsonl (link + endpoints)."""
    domains = set()
    with open(FULL_JSONL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            # Main link
            link = record.get("link")
            if link:
                domain = extract_domain_from_url(link)
                if domain:
                    domains.add(normalize_domain(domain))
            # Endpoints
            for ep in record.get("endpoints", []) or []:
                url = ep.get("url") if isinstance(ep, dict) else None
                if url:
                    domain = extract_domain_from_url(url)
                    if domain:
                        domains.add(normalize_domain(domain))
    return domains


def main():
    # Load Censys domains
    with open(CENSYS_PATH, "r", encoding="utf-8") as f:
        content = f.read()
    # Fix truncated JSON (missing closing brace) if needed
    content = content.rstrip()
    if not content.endswith("}"):
        content = content + "\n}"
    censys_data = json.loads(content)
    censys_hosts = [b["key"] for b in censys_data.get("buckets", [])]

    # Load registry domains
    registry_domains = load_registry_domains()

    # Find missing domains (Censys has it, registry doesn't)
    # Exclude IP addresses - registry uses hostnames
    missing = []
    ips_excluded = []
    for host in censys_hosts:
        if is_ip_address(host):
            ips_excluded.append(host)
            continue
        norm = normalize_domain(host)
        if not norm:
            continue
        if norm not in registry_domains:
            missing.append(host)

    # Sort for consistent output
    missing.sort()

    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    # Write output
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write("# .Stat Suite domains from Censys missing in the registry\n")
        f.write(f"# Total: {len(missing)} domains\n")
        f.write("# Source: dev/data/censis_statsuite_list.json\n")
        f.write(f"# Excluded (IPs, cannot match): {len(ips_excluded)}\n")
        f.write("\n")
        for domain in missing:
            f.write(f"{domain}\n")

    print(f"Censys .Stat Suite entries: {len(censys_hosts)}")
    print(f"  - Hostnames: {len(censys_hosts) - len(ips_excluded)}")
    print(f"  - IPs excluded: {len(ips_excluded)}")
    print(f"Registry domains (from full.jsonl): {len(registry_domains)}")
    print(f"Missing domains (for review): {len(missing)}")
    print(f"Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
