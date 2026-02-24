#!/usr/bin/env python3
"""
Add ArcGIS Hub domains to entities using cursor-agent.

Reads domain names from dev/data/arcgishub_verified.txt and invokes cursor-agent
for each domain with the prompt: "Add https://[domain] to the entities. It uses ArcGIS hub software"

Usage:
    python scripts/add_arcgishub_via_cursor.py
    python scripts/add_arcgishub_via_cursor.py --dry-run
    python scripts/add_arcgishub_via_cursor.py --limit 10
    python scripts/add_arcgishub_via_cursor.py -v

Requires:
    - cursor-agent CLI installed and in PATH
    - cursor-agent login (authentication)
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

# Base directory (repository root)
BASE_DIR = Path(__file__).parent.parent
DOMAINS_FILE = BASE_DIR / "dev" / "data" / "arcgishub_verified.txt"


def load_domains(file_path: Path) -> list[str]:
    """Load domain names from file, skipping comments and empty lines."""
    domains = []
    if not file_path.exists():
        print(f"Error: File not found: {file_path}", file=sys.stderr)
        return domains

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            domains.append(line)

    return domains


def call_cursor_agent(workspace: Path, prompt: str, timeout: int = 300) -> tuple[bool, str]:
    """Call cursor-agent and return success status and output."""
    try:
        workspace_abs = str(workspace.resolve())

        result = subprocess.run(
            [
                "cursor-agent",
                "--print",
                "--workspace", workspace_abs,
                "--output-format", "text",
                prompt,
            ],
            text=True,
            capture_output=True,
            timeout=timeout,
        )

        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        combined = stdout
        if stderr:
            combined = f"{stdout}\n--- stderr ---\n{stderr}" if stdout else stderr

        if result.returncode != 0:
            return False, combined or f"Exit code: {result.returncode}"

        return True, combined

    except subprocess.TimeoutExpired:
        return False, f"Timeout after {timeout} seconds"
    except FileNotFoundError:
        return False, "cursor-agent not found in PATH"
    except Exception as e:
        return False, str(e)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Add ArcGIS Hub domains to entities via cursor-agent"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print domains and prompts, do not call cursor-agent",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process only first N domains (0 = all)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Seconds to wait between cursor-agent calls (default: 2)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Timeout per domain in seconds (default: 300)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show full prompts, cursor-agent output, and timing",
    )
    args = parser.parse_args()

    domains = load_domains(DOMAINS_FILE)
    if not domains:
        return 1

    if args.limit > 0:
        domains = domains[: args.limit]

    total = len(domains)
    print(f"Loaded {total} domains from {DOMAINS_FILE}")
    if args.verbose and not args.dry_run:
        print(f"Workspace: {BASE_DIR.resolve()}")
    print()

    if args.dry_run:
        print("DRY RUN - would process the following:")
        for i, domain in enumerate(domains, 1):
            url = f"https://{domain}"
            prompt = f"Add {url} to the entities. It uses ArcGIS hub software"
            print(f"  [{i}/{total}] {domain}")
            print(f"       Prompt: {prompt}")
        return 0

    if args.verbose:
        print()

    # Check cursor-agent availability
    try:
        version_result = subprocess.run(
            ["cursor-agent", "--version"],
            capture_output=True,
            text=True,
            check=True,
        )
        if args.verbose and (version_result.stdout or version_result.stderr):
            print(f"cursor-agent: {(version_result.stdout or version_result.stderr).strip()}")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: cursor-agent not found or not working.", file=sys.stderr)
        print("Install from: https://docs.cursor.com/tools/cli", file=sys.stderr)
        print("Run 'cursor-agent login' to authenticate.", file=sys.stderr)
        return 1

    success_count = 0
    fail_count = 0

    for i, domain in enumerate(domains, 1):
        url = f"https://{domain}"
        prompt = f"Add {url} to the entities. It uses ArcGIS hub software"

        print(f"[{i}/{total}] Processing: {domain}")
        if args.verbose:
            print(f"  Prompt: {prompt}")

        start = time.perf_counter()
        ok, output = call_cursor_agent(BASE_DIR, prompt, timeout=args.timeout)
        elapsed = time.perf_counter() - start

        if ok:
            success_count += 1
            print(f"  ✓ Done" + (f" ({elapsed:.1f}s)" if args.verbose else ""))
            if output:
                if args.verbose:
                    print(f"  Output:\n{output}")
                elif len(output) < 500:
                    print(f"  Output: {output[:200]}...")
        else:
            fail_count += 1
            print(f"  ✗ Failed" + (f" ({elapsed:.1f}s)" if args.verbose else ""))
            if args.verbose:
                print(f"  Error output:\n{output}")
            else:
                print(f"  Error: {output[:200]}...")

        # Delay between calls (except after last)
        if args.delay > 0 and i < total:
            time.sleep(args.delay)

    print()
    print(f"Summary: {success_count} succeeded, {fail_count} failed, {total} total")

    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
