"""Scan discovery/harvest docs for software.id mentions.

Used to generate docs/software-index.md and by tests to require a fingerprint
when a software YAML exists.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS = REPO_ROOT / "docs"
SOFTWARE_DIR = REPO_ROOT / "data" / "software"
SOFTWARE_IDS = REPO_ROOT / "data" / "reference" / "software_ids.yaml"

HEADING_ID_RE = re.compile(
    r"^## .+?\(`([a-z][a-z0-9]*)`\)(?:\s*\{#([a-z0-9]+)\})?",
    re.MULTILINE,
)
HEADING_MULTI_RE = re.compile(
    r"^## .+?\(`([a-z][a-z0-9]*)`(,\s*`([a-z][a-z0-9]*)`)+\)",
    re.MULTILINE,
)
BACKTICK_ID_RE = re.compile(r"`([a-z][a-z0-9]*)`")
EXPLICIT_ANCHOR_RE = re.compile(r"\{#([a-z][a-z0-9]*)\}")

DISCOVERY_GLOBS = (
    "discovery.md",
    "discovery-*.md",
    "agents/discover.md",
)
HARVEST_GLOBS = (
    "harvest.md",
    "harvest-*.md",
    "agents/harvest.md",
)

SKIP_IDS = frozenset({"custom"})
URLMAP_KEY_RE = re.compile(r'"([a-z][a-z0-9]*)":\s*[A-Z][A-Z0-9_]*URLMAP')
COMBINED_H2_RE = re.compile(
    r"^## .+\(`[a-z][a-z0-9]*`\).+\(`[a-z][a-z0-9]*`\)"
)


def _glob_docs(patterns: tuple[str, ...]) -> list[Path]:
    files: list[Path] = []
    seen = set()
    for pat in patterns:
        for path in sorted(DOCS.glob(pat)):
            if path.resolve() in seen:
                continue
            seen.add(path.resolve())
            files.append(path)
    return files


def load_software_ids() -> list[str]:
    ids = yaml.safe_load(SOFTWARE_IDS.read_text(encoding="utf-8"))
    return [i for i in ids if i not in SKIP_IDS]


def load_apidetect_ids() -> set[str]:
    keys: set[str] = set()
    for name in ("apidetect.py", "apidetect_urlmaps_draft.py"):
        text = (REPO_ROOT / "scripts" / name).read_text(encoding="utf-8")
        keys.update(URLMAP_KEY_RE.findall(text))
    keys.discard("custom")
    return keys


def combined_software_headings() -> list[str]:
    """H2s that name two or more software.ids — unique {#id} headings required."""
    hits: list[str] = []
    for path in _glob_docs(DISCOVERY_GLOBS + HARVEST_GLOBS):
        rel = path.relative_to(DOCS).as_posix()
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if COMBINED_H2_RE.match(line):
                hits.append(f"{rel}:{i}:{line}")
    return hits


def software_category(software_id: str) -> str:
    for path in SOFTWARE_DIR.rglob(f"{software_id}.yaml"):
        if path.name == "custom.yaml":
            continue
        return path.parent.name
    return "unknown"


def _heading_ids_in_text(text: str) -> dict[str, str]:
    """Map software.id -> markdown heading slug fragment (explicit {#id} or first capture)."""
    found: dict[str, str] = {}
    for match in HEADING_ID_RE.finditer(text):
        sid = match.group(1)
        explicit = match.group(2)
        found[sid] = explicit or sid
    for match in HEADING_MULTI_RE.finditer(text):
        for sid in re.findall(r"`([a-z][a-z0-9]*)`", match.group(0)):
            found.setdefault(sid, sid)
    for match in EXPLICIT_ANCHOR_RE.finditer(text):
        found.setdefault(match.group(1), match.group(1))
    return found


def scan_area(patterns: tuple[str, ...], known_ids: set[str]) -> dict[str, dict]:
    """Return {software_id: {files, heading_file, heading_anchor}}."""
    out: dict[str, dict] = {
        sid: {"files": [], "heading_file": None, "heading_anchor": None}
        for sid in known_ids
    }
    for path in _glob_docs(patterns):
        text = path.read_text(encoding="utf-8")
        rel = path.relative_to(DOCS).as_posix()
        headings = _heading_ids_in_text(text)
        mentioned = set(BACKTICK_ID_RE.findall(text)) & known_ids
        for sid in mentioned:
            if rel not in out[sid]["files"]:
                out[sid]["files"].append(rel)
        for sid, anchor in headings.items():
            if sid in known_ids and out[sid]["heading_file"] is None:
                out[sid]["heading_file"] = rel
                out[sid]["heading_anchor"] = anchor
    return out


def coverage_rows() -> list[dict]:
    known = load_software_ids()
    known_set = set(known)
    disc = scan_area(DISCOVERY_GLOBS, known_set)
    harv = scan_area(HARVEST_GLOBS, known_set)
    apidetect = load_apidetect_ids()
    rows = []
    for sid in known:
        d = disc[sid]
        h = harv[sid]
        rows.append(
            {
                "id": sid,
                "category": software_category(sid),
                "discovery_files": d["files"],
                "harvest_files": h["files"],
                "discovery_heading": d["heading_file"],
                "discovery_anchor": d["heading_anchor"],
                "harvest_heading": h["heading_file"],
                "harvest_anchor": h["heading_anchor"],
                "in_discovery": bool(d["files"]),
                "in_harvest": bool(h["files"]),
                "apidetect": sid in apidetect,
            }
        )
    return rows


def missing_mentions() -> tuple[list[str], list[str]]:
    rows = coverage_rows()
    missing_d = [r["id"] for r in rows if not r["in_discovery"]]
    missing_h = [r["id"] for r in rows if not r["in_harvest"]]
    return missing_d, missing_h


def _doc_link(rel: str | None, anchor: str | None) -> str:
    if not rel:
        return "—"
    label = rel.replace(".md", "")
    if anchor:
        return f"[{label}]({rel}#{anchor})"
    return f"[{label}]({rel})"


def render_index_markdown(rows: list[dict]) -> str:
    lines = [
        "# Software index",
        "",
        "Map each published `software.id` (except `custom`) to the discovery and "
        "harvest guide that mentions it, plus whether `apidetect` has a URL map. "
        "Generated from YAML + docs headings. "
        "Regenerate with `python scripts/docs_software_coverage.py`.",
        "",
        "Adding a software definition: [software-taxonomy.md](software-taxonomy.md). "
        "Agent checklists: [agents/discover.md](agents/discover.md), "
        "[agents/harvest.md](agents/harvest.md).",
        "",
        "| `software.id` | Category | Discovery | Harvest | apidetect |",
        "|---------------|----------|-----------|---------|-----------|",
    ]
    for row in rows:
        disc = _doc_link(
            row["discovery_heading"] or (row["discovery_files"][0] if row["discovery_files"] else None),
            row["discovery_anchor"] if row["discovery_heading"] else None,
        )
        harv = _doc_link(
            row["harvest_heading"] or (row["harvest_files"][0] if row["harvest_files"] else None),
            row["harvest_anchor"] if row["harvest_heading"] else None,
        )
        api = "yes" if row["apidetect"] else "—"
        lines.append(
            f"| `{row['id']}` | {row['category']} | {disc} | {harv} | {api} |"
        )
    lines.append("")
    return "\n".join(lines)


MD_LINK_RE = re.compile(r"\[[^\]]*\]\(([^)]+)\)")


def stale_software_anchor_links() -> list[str]:
    """Links whose fragment is an old Docusaurus slug of a heading that now has {#id}."""
    known = set(load_software_ids())
    hits: list[str] = []
    for path in sorted(DOCS.rglob("*.md")):
        if path.name == "software-index.md":
            continue
        rel = path.relative_to(DOCS).as_posix()
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            for match in MD_LINK_RE.finditer(line):
                target = match.group(1).split()[0]
                if "#" not in target or target.startswith("http"):
                    continue
                filepart, frag = target.split("#", 1)
                frag = frag.strip()
                if not frag or "/" in frag:
                    continue
                if filepart:
                    dest = (path.parent / filepart).resolve()
                else:
                    dest = path
                if not dest.is_file():
                    continue
                anchors = set(EXPLICIT_ANCHOR_RE.findall(dest.read_text(encoding="utf-8")))
                for sid in known:
                    if frag != sid and frag.endswith(sid) and sid in anchors:
                        hits.append(f"{rel}:{i}: #{frag} → #{sid}")
                        break
    return hits


def write_index(path: Path | None = None) -> Path:
    dest = path or (DOCS / "software-index.md")
    dest.write_text(render_index_markdown(coverage_rows()), encoding="utf-8")
    return dest


def main() -> None:
    missing_d, missing_h = missing_mentions()
    combined = combined_software_headings()
    stale = stale_software_anchor_links()
    dest = write_index()
    print(f"Wrote {dest}")
    if missing_d:
        print("Missing discovery mentions:", ", ".join(missing_d))
    if missing_h:
        print("Missing harvest mentions:", ", ".join(missing_h))
    if combined:
        print("Combined software H2s (split these):")
        for hit in combined:
            print(" ", hit)
    if stale:
        print("Stale software heading links:")
        for hit in stale:
            print(" ", hit)
    if not missing_d and not missing_h and not combined and not stale:
        print("All software IDs mentioned; unique H2s; anchors current.")


if __name__ == "__main__":
    main()
