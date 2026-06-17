#!/usr/bin/env python3
"""
Probe catalog URL reachability and write a machine-readable liveness report.

Phase 1 writes report-only output; schema fields (liveness_status, last_verified_at)
may be added to catalog YAML in a later phase.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

import requests
import yaml

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENTITIES = REPO_ROOT / "data" / "entities"
DEFAULT_OUTPUT = REPO_ROOT / "dataquality" / "liveness_report.jsonl"

RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


@dataclass
class ProbeResult:
    uid: str
    link: str
    liveness_status: str
    http_code: Optional[int]
    checked_at: str
    error: Optional[str] = None
    final_url: Optional[str] = None

    def to_dict(self) -> dict:
        payload = {
            "uid": self.uid,
            "link": self.link,
            "liveness_status": self.liveness_status,
            "http_code": self.http_code,
            "checked_at": self.checked_at,
        }
        if self.error:
            payload["error"] = self.error
        if self.final_url and self.final_url != self.link:
            payload["final_url"] = self.final_url
        return payload


def classify_liveness(http_code: Optional[int], error: Optional[str] = None) -> str:
    """Map HTTP response or transport error to a liveness status."""
    if error:
        lowered = error.lower()
        if any(token in lowered for token in ("timeout", "timed out", "connection refused", "name or service not known", "nodename nor servname")):
            return "dead"
        return "error"

    if http_code is None:
        return "dead"

    if 200 <= http_code < 300:
        return "live"
    if 300 <= http_code < 400:
        return "redirect"
    if http_code == 403:
        return "inconclusive"
    if http_code == 404:
        return "dead"
    if http_code >= 500:
        return "dead"
    if http_code >= 400:
        return "inconclusive"
    return "error"


def iter_catalog_records(
    entities_dir: Path,
    country: Optional[str] = None,
) -> Iterator[dict]:
    """Yield catalog records with uid and link from entity YAML files."""
    country_code = country.upper() if country else None
    for yaml_path in sorted(entities_dir.rglob("*.yaml")):
        rel_parts = yaml_path.relative_to(entities_dir).parts
        if not rel_parts:
            continue
        file_country = rel_parts[0]
        if country_code and file_country != country_code:
            continue

        with yaml_path.open("r", encoding="utf-8") as handle:
            record = yaml.load(handle, Loader=Loader)
        if not record:
            continue

        link = (record.get("link") or "").strip()
        uid = (record.get("uid") or "").strip()
        if not link or not uid:
            continue

        yield {
            "uid": uid,
            "link": link,
            "id": record.get("id"),
            "country": file_country,
        }


def probe_url(
    url: str,
    session: requests.Session,
    timeout: float = 10.0,
    retries: int = 2,
) -> tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Probe a URL with HEAD, GET fallback, retries on timeout/5xx.

    Returns (http_code, error_message, final_url).
    """
    last_error: Optional[str] = None
    last_code: Optional[int] = None
    final_url: Optional[str] = None
    attempts = retries + 1

    for attempt in range(attempts):
        for method in ("HEAD", "GET"):
            try:
                response = session.request(
                    method,
                    url,
                    timeout=timeout,
                    allow_redirects=True,
                )
                last_code = response.status_code
                final_url = response.url
                if last_code in RETRYABLE_STATUS_CODES and attempt < attempts - 1:
                    break
                return last_code, None, final_url
            except requests.exceptions.Timeout as exc:
                last_error = f"timeout: {exc}"
            except requests.exceptions.ConnectionError as exc:
                last_error = f"connection error: {exc}"
            except requests.exceptions.RequestException as exc:
                last_error = str(exc)

        if last_code in RETRYABLE_STATUS_CODES and attempt < attempts - 1:
            time.sleep(0.5 * (attempt + 1))
            continue
        if last_error and attempt < attempts - 1:
            time.sleep(0.5 * (attempt + 1))
            continue
        break

    return last_code, last_error, final_url


def checked_at_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def probe_record(
    record: dict,
    session: requests.Session,
    timeout: float,
    retries: int,
) -> ProbeResult:
    http_code, error, final_url = probe_url(record["link"], session, timeout=timeout, retries=retries)
    status = classify_liveness(http_code, error)
    return ProbeResult(
        uid=record["uid"],
        link=record["link"],
        liveness_status=status,
        http_code=http_code,
        checked_at=checked_at_now(),
        error=error,
        final_url=final_url,
    )


def write_report(results: list[ProbeResult], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for result in results:
            handle.write(json.dumps(result.to_dict(), ensure_ascii=False) + "\n")


def summarize(results: list[ProbeResult]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for result in results:
        counts[result.liveness_status] = counts.get(result.liveness_status, 0) + 1
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe catalog URL liveness.")
    parser.add_argument("--entities", default=str(DEFAULT_ENTITIES), help="Entities directory")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output JSONL path")
    parser.add_argument("--country", help="Limit to ISO country code (e.g. US)")
    parser.add_argument("--sample", type=int, help="Probe only N random records")
    parser.add_argument("--timeout", type=float, default=10.0, help="Per-request timeout seconds")
    parser.add_argument("--retries", type=int, default=2, help="Retries on timeout/5xx")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay between probes in seconds")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for --sample")
    args = parser.parse_args()

    entities_dir = Path(args.entities)
    output_path = Path(args.output)
    records = list(iter_catalog_records(entities_dir, country=args.country))

    if args.sample:
        random.seed(args.seed)
        if args.sample < len(records):
            records = random.sample(records, args.sample)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "dataportals-registry-liveness/1.0 (+https://github.com/commondataio/dataportals-registry)",
            "Accept": "*/*",
        }
    )

    results: list[ProbeResult] = []
    for index, record in enumerate(records):
        results.append(
            probe_record(record, session, timeout=args.timeout, retries=args.retries)
        )
        if args.delay and index < len(records) - 1:
            time.sleep(args.delay)

    write_report(results, output_path)
    summary = summarize(results)
    print(f"Wrote {len(results)} results to {output_path}")
    for status, count in sorted(summary.items()):
        print(f"  {status}: {count}")


if __name__ == "__main__":
    main()
