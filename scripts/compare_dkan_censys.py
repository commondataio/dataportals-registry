#!/usr/bin/env python
"""
Compare Censys DKAN domains against the registry and identify missing ones.
Output: dev/data/dkan_for_review.txt
"""
import json
import os
import re
from urllib.parse import urlparse

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)
# Try both spellings: censys vs censis (typo in existing file)
CENSYS_PATH = os.path.join(_REPO_ROOT, "dev", "data", "censys_dkan_list.json")
CENSIS_PATH = os.path.join(_REPO_ROOT, "dev", "data", "censis_dkan_list.json")
FULL_JSONL_PATH = os.path.join(_REPO_ROOT, "data", "datasets", "full.jsonl")
OUTPUT_PATH = os.path.join(_REPO_ROOT, "dev", "data", "dkan_for_review.txt")

# IP address pattern - exclude these from domain comparison
IPV4_PATTERN = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")

# Infrastructure/non-portal domains to exclude from review (if any)
EXCLUDE_DOMAINS = set()


def is_ip_address(hostname: str) -> bool:
    """Check if hostname is an IP address (IPv4 or IPv6)."""
    if not hostname:
        return False
    if IPV4_PATTERN.match(hostname):
        return True
    # IPv6: contains colons, typically no dots (or IPv4-mapped)
    if ":" in hostname and "." not in hostname:
        return True
    return False


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
    # Load Censys domains - try censys_dkan_list.json first, then censis_dkan_list.json
    censys_path = CENSYS_PATH if os.path.exists(CENSYS_PATH) else CENSIS_PATH
    source_name = os.path.basename(censys_path)

    with open(censys_path, "r", encoding="utf-8") as f:
        content = f.read()
    content = content.rstrip()
    if not content.endswith("}"):
        content = content + "\n}"
    censys_data = json.loads(content)
    censys_hosts = [b["key"] for b in censys_data.get("buckets", [])]

    # Load registry domains
    registry_domains = load_registry_domains()

    # Find missing domains (Censys has it, registry doesn't)
    # Only consider hostnames (skip IP addresses)
    missing = []
    for host in censys_hosts:
        if is_ip_address(host):
            continue
        norm = normalize_domain(host)
        if not norm:
            continue
        if norm in EXCLUDE_DOMAINS:
            continue
        if norm not in registry_domains:
            missing.append(host)

    # Deduplicate by normalized domain (www.x and x are same)
    seen = set()
    unique_missing = []
    for host in sorted(missing):
        norm = normalize_domain(host)
        if norm not in seen:
            seen.add(norm)
            unique_missing.append(host)

    unique_missing.sort()

    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    # Write output
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write("# DKAN domains from Censys missing in the registry\n")
        f.write(f"# Total: {len(unique_missing)} domains\n")
        f.write(f"# Source: dev/data/{source_name}\n")
        f.write("# Excluded: IP addresses\n")
        f.write("\n")
        for domain in unique_missing:
            f.write(f"{domain}\n")

    print(f"Censys DKAN hosts (total): {len(censys_hosts)}")
    print(f"Censys DKAN hostnames (excl. IPs): {len([h for h in censys_hosts if not is_ip_address(h)])}")
    print(f"Registry domains (from full.jsonl): {len(registry_domains)}")
    print(f"Missing domains (for review): {len(unique_missing)}")
    print(f"Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
