"""Regression tests for apidetect endpoint probing."""

import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import apidetect


class _DummyResponse:
    def __init__(self, status_code=200, content=b"{}", headers=None, text=None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {"Content-Type": "application/json"}
        self.text = text if text is not None else content.decode("utf8", errors="ignore")


class _DummySession:
    def __init__(self, responses):
        self._responses = list(responses)

    def _next(self):
        if not self._responses:
            raise AssertionError("No more fake responses configured")
        return self._responses.pop(0)

    def get(self, *args, **kwargs):
        return self._next()

    def post(self, *args, **kwargs):
        return self._next()


def _patch_session(monkeypatch, responses):
    monkeypatch.setattr(apidetect.requests, "Session", lambda: _DummySession(responses))


def _patch_requests_get(monkeypatch, response):
    monkeypatch.setattr(apidetect.requests, "get", lambda *args, **kwargs: response)


def test_api_identifier_non_200_does_not_add_endpoint(monkeypatch):
    monkeypatch.setitem(
        apidetect.CATALOGS_URLMAP,
        "testsw",
        [{"id": "probe", "url": "/probe", "expected_mime": ["application/json"], "version": None}],
    )
    _patch_session(monkeypatch, [_DummyResponse(status_code=404)])

    found = apidetect.api_identifier("https://example.org", "testsw")

    assert found == []


def test_api_identifier_verify_json_decode_error_is_handled(monkeypatch):
    monkeypatch.setitem(
        apidetect.CATALOGS_URLMAP,
        "testsw",
        [
            {
                "id": "probe",
                "url": "/probe",
                "expected_mime": ["application/json"],
                "is_json": True,
                "version": None,
            }
        ],
    )
    _patch_session(monkeypatch, [_DummyResponse(content=b"{invalid-json}")])

    found = apidetect.api_identifier(
        "https://example.org", "testsw", verify_json=True
    )

    assert found == []


def test_api_identifier_accepts_string_expected_mime(monkeypatch):
    monkeypatch.setitem(
        apidetect.CATALOGS_URLMAP,
        "testsw",
        [
            {
                "id": "probe",
                "url": "/probe",
                "expected_mime": "text/turtle",
                "version": "1.0",
            }
        ],
    )
    _patch_session(
        monkeypatch,
        [
            _DummyResponse(
                headers={"Content-Type": "text/turtle; charset=utf-8"},
                content=b"@prefix ex: <https://example.org/> .",
            )
        ],
    )

    found = apidetect.api_identifier("https://example.org", "testsw")

    assert len(found) == 1
    assert found[0]["type"] == "probe"
    assert found[0]["url"] == "https://example.org/probe"


def test_api_identifier_rejects_wrong_mime_for_string_expected_mime(monkeypatch):
    monkeypatch.setitem(
        apidetect.CATALOGS_URLMAP,
        "testsw",
        [
            {
                "id": "probe",
                "url": "/probe",
                "expected_mime": "text/turtle",
                "version": None,
            }
        ],
    )
    _patch_session(
        monkeypatch,
        [_DummyResponse(headers={"Content-Type": "application/json"})],
    )

    found = apidetect.api_identifier("https://example.org", "testsw")

    assert found == []


def test_api_identifier_geoserver_stac_uses_display_url_for_endpoint(monkeypatch):
    """STAC probe hits collections JSON; stored URL is the API root (see GEOSERVER_URLMAP)."""
    monkeypatch.setitem(
        apidetect.CATALOGS_URLMAP,
        "testsw",
        [
            {
                "id": "stacserverapi",
                "display_url": "/ogc/stac/v1",
                "url": "/ogc/stac/v1/collections?f=json",
                "accept": "application/json",
                "expected_mime": apidetect.JSON_MIMETYPES,
                "is_json": True,
                "version": None,
            }
        ],
    )
    _patch_session(
        monkeypatch,
        [_DummyResponse(content=b'{"collections":[],"links":[]}')],
    )

    found = apidetect.api_identifier("https://example.org/geoserver", "testsw")

    assert len(found) == 1
    assert found[0]["type"] == "stacserverapi"
    assert found[0]["url"] == "https://example.org/geoserver/ogc/stac/v1"


def test_analyze_robots_returns_empty_for_non_200(monkeypatch):
    _patch_requests_get(
        monkeypatch,
        _DummyResponse(
            status_code=404,
            content=b"Not found",
            headers={"Content-Type": "text/plain"},
        ),
    )

    found = apidetect.analyze_robots("https://example.org")

    assert found == []


def test_analyze_root_continues_after_empty_jsonld_list(monkeypatch):
    html = """
    <html>
      <head>
        <script type="application/ld+json">[]</script>
        <script type="application/ld+json">{"@graph":[{"@type":"DataCatalog"}]}</script>
      </head>
      <body></body>
    </html>
    """
    _patch_session(
        monkeypatch,
        [
            _DummyResponse(
                status_code=200,
                content=html.encode("utf8"),
                headers={"Content-Type": "text/html"},
            )
        ],
    )

    found = apidetect.analyze_root("https://example.org")

    assert {"type": "schemaorg:datacatalog", "url": "https://example.org"} in found


def test_analyze_root_detects_datacatalog_in_mainentity_list(monkeypatch):
    html = """
    <html>
      <head>
        <script type="application/ld+json">
          {"@graph":[{"@type":"WebPage","mainEntity":[{"name":"no-type"},{"@type":"DataCatalog"}]}]}
        </script>
      </head>
      <body></body>
    </html>
    """
    _patch_session(
        monkeypatch,
        [
            _DummyResponse(
                status_code=200,
                content=html.encode("utf8"),
                headers={"Content-Type": "text/html"},
            )
        ],
    )

    found = apidetect.analyze_root("https://example.org")

    assert {"type": "schemaorg:datacatalog", "url": "https://example.org"} in found


def test_detect_ckan_uses_ckanapi_endpoint_base_url(monkeypatch):
    test_record = {
        "id": "testckan",
        "link": "https://catalog.example.org",
        "software": {"id": "ckan"},
        "endpoints": [{"type": "ckanapi", "url": "https://catalog.example.org/api/3"}],
    }
    calls = []

    monkeypatch.setattr(apidetect, "_resolve_root_dir", lambda mode: "/unused")
    monkeypatch.setattr(apidetect, "_iter_yaml_files", lambda root: ["fake.yaml"])
    monkeypatch.setattr(apidetect, "_load_record", lambda filepath: test_record)

    def _fake_api_identifier(base_url, software_id, **kwargs):
        calls.append((base_url, software_id))
        return []

    monkeypatch.setattr(apidetect, "api_identifier", _fake_api_identifier)

    apidetect.detect_ckan(dryrun=True, mode="entries")

    assert calls == [("https://catalog.example.org", "ckan")]


def test_catalogs_urlmap_includes_draft_software():
  expected = {
      "stacserver",
      "galaxy",
      "udata",
      "lizmap",
      "nextgisweb",
      "fusionregistry",
      "aristotlemdr",
  }
  assert expected.issubset(apidetect.CATALOGS_URLMAP.keys())


def test_opendap_urlmap_is_not_empty():
  assert len(apidetect.OPENDAP_URLMAP) > 0


def test_api_identifier_stacserver_collections(monkeypatch):
  collections = b'{"collections":[],"links":[]}'

  class _StacSession:
      def get(self, url, **kwargs):
          if url.endswith("/collections"):
              return _DummyResponse(content=collections)
          return _DummyResponse(status_code=404)

      def post(self, *args, **kwargs):
          return _DummyResponse(status_code=404)

  monkeypatch.setattr(apidetect.requests, "Session", lambda: _StacSession())

  found = apidetect.api_identifier("https://example.org/stac/v1", "stacserver")

  assert any(item["type"] == "stacserverapi:collections" for item in found)
  assert any(
      item["url"] == "https://example.org/stac/v1/collections" for item in found
  )


def test_api_identifier_galaxy_version(monkeypatch):
  class _GalaxySession:
      def get(self, url, **kwargs):
          if url.endswith("/api/version"):
              return _DummyResponse(
                  content=b'{"version_major":"24.1","version_minor":"0"}',
              )
          return _DummyResponse(status_code=404)

      def post(self, *args, **kwargs):
          return _DummyResponse(status_code=404)

  monkeypatch.setattr(apidetect.requests, "Session", lambda: _GalaxySession())

  found = apidetect.api_identifier("https://usegalaxy.org", "galaxy")

  assert any(item["type"] == "galaxy:api" for item in found)
  assert any(item["url"] == "https://usegalaxy.org/api/version" for item in found)


def test_api_identifier_udata_datasets(monkeypatch):
  class _UdataSession:
      def get(self, url, **kwargs):
          if url.endswith("/api/1/datasets/"):
              return _DummyResponse(
                  content=b'{"data":[],"page":1,"page_size":20,"total":0}',
              )
          return _DummyResponse(status_code=404)

      def post(self, *args, **kwargs):
          return _DummyResponse(status_code=404)

  monkeypatch.setattr(apidetect.requests, "Session", lambda: _UdataSession())

  found = apidetect.api_identifier("https://www.data.gouv.fr", "udata")

  assert any(item["type"] == "udataapi" for item in found)
  assert any(
      item["url"] == "https://www.data.gouv.fr/api/1/datasets/" for item in found
  )


def test_report_writes_expected_header(tmp_path, monkeypatch):
    test_record = {
        "id": "id1",
        "uid": "cdi00000001",
        "link": "https://catalog.example.org",
        "software": {"id": "ckan"},
    }

    monkeypatch.setitem(apidetect.CATALOGS_URLMAP, "ckan", [{}])
    monkeypatch.setattr(apidetect, "_resolve_root_dir", lambda mode: "/unused")
    monkeypatch.setattr(apidetect, "_iter_yaml_files", lambda root: ["fake.yaml"])
    monkeypatch.setattr(apidetect, "_load_record", lambda filepath: test_record)

    out_file = tmp_path / "report.csv"
    apidetect.report(status="undetected", filename=str(out_file), mode="entries")

    lines = out_file.read_text(encoding="utf8").splitlines()
    assert lines[0] == "id,uid,link,software_id,status"
