"""Discovery and harvest docs must mention every published software.id."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from docs_software_coverage import (
    REPO_ROOT,
    combined_software_headings,
    missing_mentions,
    render_index_markdown,
    coverage_rows,
    stale_software_anchor_links,
    DOCS,
)


class TestDocsSoftwareCoverage:
    def test_all_software_ids_mentioned(self):
        missing_d, missing_h = missing_mentions()
        assert missing_d == [], (
            "software IDs missing from discovery docs: " + ", ".join(missing_d)
        )
        assert missing_h == [], (
            "software IDs missing from harvest docs: " + ", ".join(missing_h)
        )

    def test_no_combined_software_headings(self):
        hits = combined_software_headings()
        assert hits == [], "split combined software H2s:\n" + "\n".join(hits)

    def test_stale_software_anchor_links(self):
        hits = stale_software_anchor_links()
        assert hits == [], "update links to {#id} anchors:\n" + "\n".join(hits)

    def test_software_index_is_current(self):
        expected = render_index_markdown(coverage_rows())
        actual = (DOCS / "software-index.md").read_text(encoding="utf-8")
        assert actual == expected, (
            "docs/software-index.md is stale; run "
            "python scripts/docs_software_coverage.py"
        )

    def test_llms_txt_static_copies_match_root(self):
        root = (REPO_ROOT / "llms.txt").read_text(encoding="utf-8")
        static = (REPO_ROOT / "website" / "static" / "llms.txt").read_text(
            encoding="utf-8"
        )
        wellknown = (
            REPO_ROOT / "website" / "static" / ".well-known" / "llms.txt"
        ).read_text(encoding="utf-8")
        assert static == root
        assert wellknown == root
