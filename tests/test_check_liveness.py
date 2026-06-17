import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from check_liveness import classify_liveness, probe_url


class _FakeResponse:
    def __init__(self, status_code: int, url: str):
        self.status_code = status_code
        self.url = url


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def request(self, method, url, timeout=None, allow_redirects=None):
        self.calls.append((method, url))
        if not self._responses:
            raise RuntimeError("no fake responses left")
        action = self._responses.pop(0)
        if isinstance(action, Exception):
            raise action
        return action


def test_classify_live():
    assert classify_liveness(200) == "live"


def test_classify_redirect():
    assert classify_liveness(301) == "redirect"


def test_classify_inconclusive_for_403():
    assert classify_liveness(403) == "inconclusive"


def test_classify_dead_for_timeout():
    assert classify_liveness(None, "timeout: read timed out") == "dead"


def test_classify_dead_for_404():
    assert classify_liveness(404) == "dead"


def test_probe_url_head_success():
    session = _FakeSession([_FakeResponse(200, "https://example.gov")])
    code, error, final_url = probe_url("https://example.gov", session)
    assert code == 200
    assert error is None
    assert final_url == "https://example.gov"
    assert session.calls[0][0] == "HEAD"


def test_probe_url_get_fallback():
    import requests

    session = _FakeSession(
        [
            requests.exceptions.RequestException("405 Method Not Allowed"),
            _FakeResponse(200, "https://example.gov"),
        ]
    )
    code, error, final_url = probe_url("https://example.gov", session)
    assert code == 200
    assert error is None
    assert [call[0] for call in session.calls] == ["HEAD", "GET"]


def test_probe_url_retries_on_503():
    session = _FakeSession(
        [
            _FakeResponse(503, "https://example.gov"),
            _FakeResponse(503, "https://example.gov"),
            _FakeResponse(200, "https://example.gov"),
        ]
    )
    code, error, final_url = probe_url("https://example.gov", session, retries=2)
    assert code == 200
    assert error is None
