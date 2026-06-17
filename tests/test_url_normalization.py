import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from builder import canonicalize_url


def test_canonicalize_url_trims_trailing_slash():
    assert canonicalize_url("https://data.example.gov/") == "https://data.example.gov"


def test_canonicalize_url_normalizes_case_and_default_port():
    assert (
        canonicalize_url("HTTPS://WWW.Example.Gov:443/path/")
        == "https://example.gov/path"
    )


def test_canonicalize_url_keeps_query_string():
    assert (
        canonicalize_url("https://example.gov/search/?q=test")
        == "https://example.gov/search?q=test"
    )
