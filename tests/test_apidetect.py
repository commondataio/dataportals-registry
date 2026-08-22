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


def test_geoserver_root_url_strips_workspace_and_web():
    assert (
        apidetect.geoserver_root_url("https://maps.example.org/geoserver/web/")
        == "https://maps.example.org/geoserver"
    )
    assert (
        apidetect.geoserver_root_url("https://geo.example.org/geoserver/geo")
        == "https://geo.example.org/geoserver"
    )


def test_api_identifier_geoserver_fast_path_ows(monkeypatch):
    class _GeoSession:
        def get(self, url, **kwargs):
            if url.startswith("https://maps.example.org/geoserver/ows") and "WMS" in url and "1.3.0" in url:
                return _DummyResponse(
                    content=b"<WMS_Capabilities/>",
                    headers={"Content-Type": "text/xml"},
                )
            if "/ogc/" in url or "/rest/" in url or "/gwc/" in url:
                raise AssertionError(f"fast path must not probe {url}")
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _GeoSession())

    found = apidetect.api_identifier(
        "https://maps.example.org/geoserver/web/", "geoserver"
    )

    assert any(item["type"] == "wms130" for item in found)
    assert any(
        item["url"]
        == "https://maps.example.org/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities"
        for item in found
    )


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
      "geomapfish",
      "getsdiportal",
      "redatam",
      "scicat",
      "mapstore",
      "opensdg",
      "terria",
      "seek",
      "supermapiserver",
      "mapgisigserver",
      "gvsigonline",
      "ingrid",
      "erdasapollo",
      "drupal",
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


def test_api_identifier_geomapfish_themes(monkeypatch):
    class _GmfSession:
        def get(self, url, **kwargs):
            if url.endswith("/themes"):
                return _DummyResponse(content=b'{"themes":[]}')
            if "mapserv_proxy" in url and "WMS" in url and "1.3.0" in url:
                return _DummyResponse(
                    content=b"<WMS_Capabilities/>",
                    headers={"Content-Type": "text/xml"},
                )
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _GmfSession())

    found = apidetect.api_identifier("https://map.example.ch/", "geomapfish")

    assert any(item["type"] == "geomapfish:themes" for item in found)
    assert any(
        item["url"] == "https://map.example.ch/themes" for item in found
    )
    assert any(item["type"] == "wms130" for item in found)


def test_api_identifier_getsdiportal_geoserver_ows(monkeypatch):
    class _GetSdiSession:
        def get(self, url, **kwargs):
            if "/geoserver/ows" in url and "WMS" in url and "1.3.0" in url:
                return _DummyResponse(
                    content=b"<WMS_Capabilities/>",
                    headers={"Content-Type": "text/xml"},
                )
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _GetSdiSession())

    found = apidetect.api_identifier("https://gis.example.gr/", "getsdiportal")

    assert any(item["type"] == "wms130" for item in found)
    assert any(
        item["url"]
        == "https://gis.example.gr/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities"
        for item in found
    )


def test_api_identifier_redatam_uses_engine_link(monkeypatch):
    engine = "https://prod.redatam.org/binpry/RpWebEngine.exe/Portal?BASE=CPV2022"

    class _RedatamSession:
        def get(self, url, **kwargs):
            if url == engine:
                return _DummyResponse(
                    content=b"<html>REDATAM</html>",
                    headers={"Content-Type": "text/html"},
                )
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _RedatamSession())

    found = apidetect.api_identifier(engine, "redatam")

    assert any(item["type"] == "redatam" and item["url"] == engine for item in found)


def test_api_identifier_scicat_datasets(monkeypatch):
    class _SciCatSession:
        def get(self, url, **kwargs):
            if url.endswith("/api/v3/datasets"):
                return _DummyResponse(content=b"[]")
            return _DummyResponse(status_code=404)

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _SciCatSession())

    found = apidetect.api_identifier("https://scicat.example.org/", "scicat")

    assert any(item["type"] == "customapi" for item in found)
    assert any(
        item["url"] == "https://scicat.example.org/api/v3/datasets" for item in found
    )


def test_api_identifier_scicat_skips_doi_host(monkeypatch):
    class _FailSession:
        def get(self, *args, **kwargs):
            raise AssertionError("DOI landing hosts must not be probed")

        def post(self, *args, **kwargs):
            raise AssertionError("DOI landing hosts must not be probed")

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _FailSession())

    found = apidetect.api_identifier("https://doi.ess.eu/", "scicat")

    assert found == []


def test_api_identifier_mapstore_uses_origin_geoserver(monkeypatch):
    class _MapStoreSession:
        def get(self, url, **kwargs):
            if url.startswith("https://webgis.example.it/geoserver/ows") and "WMS" in url and "1.3.0" in url:
                return _DummyResponse(
                    content=b"<WMS_Capabilities/>",
                    headers={"Content-Type": "text/xml"},
                )
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _MapStoreSession())

    found = apidetect.api_identifier(
        "https://webgis.example.it/mapstore", "mapstore"
    )

    assert any(item["type"] == "wms130" for item in found)
    assert any(
        item["url"]
        == "https://webgis.example.it/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities"
        for item in found
    )


def test_api_identifier_opensdg_indicator_json(monkeypatch):
    class _OpenSdgSession:
        def get(self, url, **kwargs):
            if url.endswith("/en/data/1-1-1.json"):
                return _DummyResponse(content=b'{"data":[]}')
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _OpenSdgSession())

    found = apidetect.api_identifier("https://sdg.example.gov/", "opensdg")

    assert any(item["type"] == "opensdg:data" for item in found)
    assert any(
        item["url"] == "https://sdg.example.gov/en/data/1-1-1.json" for item in found
    )


def test_opensdg_parse_remote_data_base_url():
    html = """
    var opensdg = {
      remoteDataBaseUrl: 'https://bristolsdgs.github.io/sdg-data-bristol/en',
      language: 'en',
    };
    """
    assert (
        apidetect.opensdg_parse_remote_data_base_url(
            html, "https://bristolsdgs.github.io/"
        )
        == "https://bristolsdgs.github.io/sdg-data-bristol/en"
    )
    assert (
        apidetect.opensdg_parse_remote_data_base_url(
            "var opensdg = { remoteDataBaseUrl: '/', };",
            "https://example.org/",
        )
        is None
    )


def test_api_identifier_opensdg_uses_remote_data_base_url(monkeypatch):
    html = b"""
    <script>
    var opensdg = {
      remoteDataBaseUrl: 'https://example.org/sdg-data/en',
    };
    </script>
    """

    class _OpenSdgRemoteSession:
        def get(self, url, **kwargs):
            if url.rstrip("/") == "https://sdg.example.org":
                return _DummyResponse(
                    content=html,
                    headers={"Content-Type": "text/html"},
                )
            if url == "https://example.org/sdg-data/en/data/1-1-1.json":
                return _DummyResponse(content=b'{"data":[]}')
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _OpenSdgRemoteSession())

    found = apidetect.api_identifier("https://sdg.example.org/", "opensdg")

    assert any(
        item["url"] == "https://example.org/sdg-data/en/data/1-1-1.json"
        for item in found
    )


def test_api_identifier_terria_config(monkeypatch):
    class _TerriaSession:
        def get(self, url, **kwargs):
            if url.endswith("/config.json"):
                return _DummyResponse(content=b'{"catalog":[]}')
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _TerriaSession())

    found = apidetect.api_identifier("https://maps.example.org/", "terria")

    assert any(item["type"] == "terria:config" for item in found)
    assert any(
        item["url"] == "https://maps.example.org/config.json" for item in found
    )


def test_nextgisweb_url_cleanup_strips_resource_path():
    cleaned = apidetect.nextgisweb_url_cleanup_func(
        "https://ngw.example.ru/resource/0"
    )
    assert cleaned == "https://ngw.example.ru"


def test_api_identifier_giswebse_uses_origin_service(monkeypatch):
    class _GiswebSession:
        def get(self, url, **kwargs):
            if url.startswith("https://maps.example.ru/GISWebServiceSE/service.php") and "WMS" in url:
                return _DummyResponse(
                    content=b"<WMS_Capabilities/>",
                    headers={"Content-Type": "text/xml"},
                )
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _GiswebSession())

    found = apidetect.api_identifier(
        "https://maps.example.ru/GISWebServerSE/", "giswebse"
    )

    assert any(item["type"] == "wms130" for item in found)
    assert any(
        item["url"].startswith("https://maps.example.ru/GISWebServiceSE/service.php")
        for item in found
    )


def test_api_identifier_oskari_tries_oskari_prefix(monkeypatch):
    class _OskariSession:
        def get(self, url, **kwargs):
            if "GetHierarchicalMapLayerGroups" in url and "/oskari/" in url:
                return _DummyResponse(content=b'{"layers":[]}')
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _OskariSession())

    found = apidetect.api_identifier("https://kortagluggi.is", "oskari")

    assert any(item["type"] == "oskari:gethiermaplayers" for item in found)
    assert any(
        item["url"]
        == "https://kortagluggi.is/oskari/action?action_route=GetHierarchicalMapLayerGroups"
        for item in found
    )


def test_api_identifier_supermap_services_json(monkeypatch):
    class _SuperMapSession:
        def get(self, url, **kwargs):
            if url.endswith("/iserver/services.json"):
                return _DummyResponse(content=b'[{"name":"map-world"}]')
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _SuperMapSession())

    found = apidetect.api_identifier(
        "https://gis.example.gov/iserver/", "supermapiserver"
    )

    assert any(item["type"] == "supermap:services" for item in found)
    assert any(
        item["url"] == "https://gis.example.gov/iserver/services.json" for item in found
    )


def test_mapgisigserver_url_cleanup_keeps_igs_root():
    cleaned = apidetect.mapgisigserver_url_cleanup_func(
        "https://gis.example.gov:6163/igs/rest/mrcs/docs"
    )
    assert cleaned == "https://gis.example.gov:6163/igs"


def test_api_identifier_mapgis_docs_json(monkeypatch):
    class _MapGisSession:
        def get(self, url, **kwargs):
            if url.endswith("/igs/rest/mrcs/docs?f=json") or url.endswith(
                "/rest/mrcs/docs?f=json"
            ):
                return _DummyResponse(content=b'["WorldMap","CityMap"]')
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _MapGisSession())

    found = apidetect.api_identifier(
        "https://gis.example.gov:6163/igs/", "mapgisigserver"
    )

    assert any(item["type"] == "mapgis:docs" for item in found)
    assert any(
        item["url"] == "https://gis.example.gov:6163/igs/rest/mrcs/docs?f=json"
        for item in found
    )


def test_api_identifier_gvsigonline_uses_origin_geoserver(monkeypatch):
    class _GvSigSession:
        def get(self, url, **kwargs):
            if url.startswith("https://geoportal.example.es/geoserver/ows") and "WMS" in url and "1.3.0" in url:
                return _DummyResponse(
                    content=b"<WMS_Capabilities/>",
                    headers={"Content-Type": "text/xml"},
                )
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _GvSigSession())

    found = apidetect.api_identifier(
        "https://geoportal.example.es/gvsigonline/", "gvsigonline"
    )

    assert any(item["type"] == "wms130" for item in found)
    assert any(
        item["url"]
        == "https://geoportal.example.es/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities"
        for item in found
    )


def test_api_identifier_ingrid_csw(monkeypatch):
    class _IngridSession:
        def get(self, url, **kwargs):
            if "/csw?" in url and "CSW" in url:
                return _DummyResponse(
                    content=b"<Capabilities/>",
                    headers={"Content-Type": "application/xml"},
                )
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _IngridSession())

    found = apidetect.api_identifier("https://metaver.example.de/", "ingrid")

    assert any(item["type"] == "csw202" for item in found)
    assert any(
        item["url"]
        == "https://metaver.example.de/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities"
        for item in found
    )


def test_api_identifier_erdasapollo_wms(monkeypatch):
    class _ErdasSession:
        def get(self, url, **kwargs):
            if "/erdas-iws/ogc/wms/" in url and "1.3.0" in url:
                return _DummyResponse(
                    content=b"<WMS_Capabilities/>",
                    headers={"Content-Type": "text/xml"},
                )
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _ErdasSession())

    found = apidetect.api_identifier(
        "https://maps.example.gov/erdas-apollo", "erdasapollo"
    )

    assert any(item["type"] == "wms130" for item in found)
    assert any("/erdas-iws/ogc/wms/" in item["url"] for item in found)


def test_save_record_preserves_key_order(tmp_path):
    path = tmp_path / "catalog.yaml"
    record = {
        "access_mode": ["open"],
        "id": "example",
        "link": "https://example.org",
        "name": "Example",
    }
    apidetect._save_record(str(path), record)
    text = path.read_text(encoding="utf8")
    assert text.index("access_mode") < text.index("id")
    assert text.index("id") < text.index("name")


def test_infer_endpoints_verified_skips_unknown_software():
    found = apidetect.infer_endpoints_verified(
        {"software": {"id": "wagmap"}, "link": "https://example.org"}
    )
    assert found == []


def test_infer_endpoints_verified_skips_empty_link():
    found = apidetect.infer_endpoints_verified(
        {"software": {"id": "ckan"}, "link": ""}
    )
    assert found == []


def test_infer_endpoints_verified_uses_urlmap(monkeypatch):
    monkeypatch.setattr(
        apidetect,
        "api_identifier",
        lambda *args, **kwargs: [
            {"type": "ckan", "url": "https://example.org/api/3"}
        ],
    )
    found = apidetect.infer_endpoints_verified(
        {"software": {"id": "ckan"}, "link": "https://example.org"}
    )
    assert found == [{"type": "ckan", "url": "https://example.org/api/3"}]


def test_icat_urlmap_is_registered():
    assert "icat" in apidetect.CATALOGS_URLMAP
    urls = [item["url"] for item in apidetect.CATALOGS_URLMAP["icat"]]
    assert "/oaipmh/request?verb=Identify" in urls


def test_quality_fixer_infer_endpoints_delegates():
    from endpoints_infer import infer_endpoints

    found = infer_endpoints(
        {"software": {"id": "wagmap"}, "link": "https://example.org"}
    )
    assert found == []


def test_infer_endpoints_verified_skips_custom():
    found = apidetect.infer_endpoints_verified(
        {"software": {"id": "custom"}, "link": "https://example.org/data"}
    )
    assert found == []


def test_api_identifier_cogis_elitegis_rest(monkeypatch):
    class _CogisSession:
        def get(self, url, **kwargs):
            if url.endswith("/elitegis/rest/services?f=pjson"):
                return _DummyResponse(
                    content=b'{"currentVersion":10.8}',
                    headers={"Content-Type": "text/plain"},
                )
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _CogisSession())

    found = apidetect.api_identifier(
        "https://citycloud.example.com/portal/catalog", "cogis"
    )

    assert any(item["type"] == "arcgis:rest:services" for item in found)
    assert any(
        item["url"]
        == "https://citycloud.example.com/elitegis/rest/services?f=pjson"
        for item in found
    )


def test_api_identifier_gin_skips_doi_host(monkeypatch):
    def _fail(*args, **kwargs):
        raise AssertionError("DOI GIN hosts should not be probed")

    monkeypatch.setattr(
        apidetect.requests,
        "Session",
        lambda: type("S", (), {"get": staticmethod(_fail), "post": staticmethod(_fail)})(),
    )

    found = apidetect.api_identifier("https://doi.gin.example.org/", "gin")
    assert found == []


def test_api_identifier_gin_version(monkeypatch):
    class _GinSession:
        def get(self, url, **kwargs):
            if url.endswith("/api/v1/version"):
                return _DummyResponse(
                    content=b'{"version":"1.22.0"}',
                    headers={"Content-Type": "application/json"},
                )
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _GinSession())

    found = apidetect.api_identifier("https://gin.g-node.org/", "gin")

    assert any(item["type"] == "gogs:api" for item in found)
    assert any(item["url"] == "https://gin.g-node.org/api/v1/version" for item in found)


def test_api_identifier_osf_uses_jsonapi_host(monkeypatch):
    class _OsfSession:
        def get(self, url, **kwargs):
            if url == "https://api.osf.io/v2/":
                return _DummyResponse(
                    content=b'{"data":[]}',
                    headers={"Content-Type": "application/vnd.api+json"},
                )
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _OsfSession())

    found = apidetect.api_identifier("https://osf.io", "osf")

    assert any(item["type"] == "osf:api" for item in found)
    assert any(item["url"] == "https://api.osf.io/v2/" for item in found)


def test_api_identifier_samvera_catalog_json(monkeypatch):
    class _SamveraSession:
        def get(self, url, **kwargs):
            if url.endswith("/catalog.json"):
                return _DummyResponse(
                    content=b'{"response":{"docs":[]}}',
                    headers={"Content-Type": "application/json"},
                )
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _SamveraSession())

    found = apidetect.api_identifier("https://curate.example.edu", "samvera")

    assert any(item["type"] == "hyrax:catalog" for item in found)
    assert any(
        item["url"] == "https://curate.example.edu/catalog.json" for item in found
    )


def test_api_identifier_ensembl_rest_ping(monkeypatch):
    class _EnsemblSession:
        def get(self, url, **kwargs):
            if url.endswith("/rest/info/ping"):
                return _DummyResponse(
                    content=b'{"ping":1}',
                    headers={"Content-Type": "application/json"},
                )
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _EnsemblSession())

    found = apidetect.api_identifier("https://parasite.example.org/", "ensembl")

    assert any(item["type"] == "rest" for item in found)
    assert any(
        item["url"] == "https://parasite.example.org/rest/info/ping" for item in found
    )


def test_api_identifier_phaidra_search_select(monkeypatch):
    class _PhaidraSession:
        def get(self, url, **kwargs):
            if url.endswith("/api/search/select"):
                return _DummyResponse(
                    content=b"<response/>",
                    headers={"Content-Type": "application/xml"},
                )
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _PhaidraSession())

    found = apidetect.api_identifier("https://phaidra.example.ac.at", "phaidra")

    assert any(item["type"] == "rest" for item in found)
    assert any(
        item["url"] == "https://phaidra.example.ac.at/api/search/select" for item in found
    )


def test_api_identifier_maptilerserver_api_index(monkeypatch):
    class _MapTilerSession:
        def get(self, url, **kwargs):
            if url.endswith("/api"):
                return _DummyResponse(
                    content=b'{"openapi":"3.0.0"}',
                    headers={"Content-Type": "application/json"},
                )
            return _DummyResponse(status_code=404, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _MapTilerSession())

    found = apidetect.api_identifier("https://tile.example.gov", "maptilerserver")

    assert any(item["type"] == "openapi" for item in found)
    assert any(item["url"] == "https://tile.example.gov/api" for item in found)


def test_api_identifier_nyudatacatalog_jsonld(monkeypatch):
    html = (
        b"<html><head><script type=\"application/ld+json\">"
        b'{"@type":"DataCatalog","name":"Example"}'
        b"</script></head><body></body></html>"
    )

    class _NyuSession:
        def get(self, url, **kwargs):
            return _DummyResponse(content=html, headers={"Content-Type": "text/html"})

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404)

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _NyuSession())

    found = apidetect.api_identifier(
        "https://datacatalog.example.edu", "nyudatacatalog"
    )

    assert any(item["type"] == "schemaorg:datacatalog" for item in found)
    assert any(
        item["url"] == "https://datacatalog.example.edu" for item in found
    )


def test_no_standard_probe_skip_list_is_not_in_urlmap():
    from apidetect_urlmaps_draft import NO_STANDARD_PROBE

    skip = (
        "oportal",
        "ogdindia",
        "masterportal",
        "wagmap",
        "ewmapa",
        "seoulopendataplaza",
        "mapbiomas",
        "cardo",
        "genesisonline",
    )
    for sid in skip:
        assert sid in NO_STANDARD_PROBE
        assert sid not in apidetect.CATALOGS_URLMAP


def test_tianditu_urlmap_is_registered():
    assert "tianditu" in apidetect.CATALOGS_URLMAP
    urls = [item["url"] for item in apidetect.CATALOGS_URLMAP["tianditu"]]
    assert "/iserver/services.json" in urls
    assert "/iportal/web/services.json" in urls
    assert "/arcgis/rest/services?f=pjson" in urls


def test_tianditu_url_cleanup_strips_html():
    assert (
        apidetect.tianditu_url_cleanup_func(
            "https://hunan.example.gov.cn/TDTHN/portal/homePage.html"
        )
        == "https://hunan.example.gov.cn/TDTHN/portal"
    )


def test_api_identifier_tianditu_uses_origin(monkeypatch):
    class _TdtSession:
        def get(self, url, **kwargs):
            if url == "https://henan.example.gov.cn/iserver/services.json":
                return _DummyResponse(
                    content=b'[{"componentType":"com.supermap.services.components.impl.MapImpl"}]',
                    headers={"Content-Type": "application/json"},
                )
            return _DummyResponse(
                status_code=404, headers={"Content-Type": "text/html"}, content=b""
            )

        def post(self, *args, **kwargs):
            return _DummyResponse(status_code=404, content=b"")

    monkeypatch.setattr(apidetect.requests, "Session", lambda: _TdtSession())

    found = apidetect.api_identifier(
        "https://henan.example.gov.cn/jiaozuo/", "tianditu"
    )

    assert any(item["type"] == "supermap:services" for item in found)
    assert any(
        item["url"] == "https://henan.example.gov.cn/iserver/services.json"
        for item in found
    )
