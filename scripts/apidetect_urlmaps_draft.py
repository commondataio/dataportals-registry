"""
URLMAP entries for API-capable software merged into apidetect.py.

Imported by scripts/apidetect.py (DRAFT_CATALOGS_URLMAP, OPENDAP_URLMAP_DRAFT).

Research date: 2026-06-17
Sources: official docs, existing catalog endpoint patterns, registry records.

Confidence tiers:
  A – stable, widely deployed path (recommended for apidetect)
  B – common but deployment-specific (try multiple paths)
  C – auth-required, POST-only, or host-specific (document only / deep mode)
  D – no standard relative API on catalog link (skip or sitemap-only)
"""

# Re-use MIME lists from apidetect.py when merging:
# from apidetect import JSON_MIMETYPES, XML_MIMETYPES, HTML_MIMETYPES

JSON_MIMETYPES = ["application/json", "text/json"]
XML_MIMETYPES = ["application/xml", "text/xml"]
HTML_MIMETYPES = ["text/html"]
PLAIN_MIMETYPES = ["text/plain"]

# ---------------------------------------------------------------------------
# Tier A – high-confidence probes
# ---------------------------------------------------------------------------

STACSERVER_URLMAP = [
    # STAC API Core (OGC 25-005 / stac-api-spec)
    {
        "id": "stacserverapi",
        "url": "/",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.0",
    },
    {
        "id": "stacserverapi:collections",
        "url": "/collections",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.0",
    },
    {
        "id": "stacserverapi:conformance",
        "url": "/conformance",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.0",
    },
    # Common when STAC is mounted under /stac
    {
        "id": "stacserverapi:stac-root",
        "url": "/stac",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.0",
    },
    {
        "id": "stacserverapi:stac-collections",
        "url": "/stac/collections",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.0",
    },
]

GALAXY_URLMAP = [
    # https://docs.galaxyproject.org/ – GET /api/version (anonymous)
    {
        "id": "galaxy:api",
        "url": "/api/version",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "galaxy:api:configuration",
        "url": "/api/configuration",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

UDATA_URLMAP = [
    # Etalab uData – https://udata.readthedocs.io/
    {
        "id": "udataapi",
        "url": "/api/1/datasets/",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1",
    },
    {
        "id": "udataapi:organizations",
        "url": "/api/1/organizations/",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1",
    },
    {
        "id": "dcatap21",
        "url": "/api/1/site/catalog.rdf",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

RASDAMAN_URLMAP = [
    # https://doc.rasdaman.com/stable/05_geo-services-guide.html
    {
        "id": "wcs201",
        "url": "/rasdaman/ows?service=WCS&version=2.0.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.1",
    },
    {
        "id": "wms130",
        "url": "/rasdaman/ows?service=WMS&version=1.3.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.3.0",
    },
]

FUSIONREGISTRY_URLMAP = [
    # https://fmrwiki.sdmx.io/ – public SDMX REST
    {
        "id": "sdmx:dataflows",
        "url": "/ws/public/sdmxapi/rest/dataflow/all/all/latest?detail=allstubs",
        "accept": "application/vnd.sdmx.structure+json",
        "expected_mime": JSON_MIMETYPES + ["application/vnd.sdmx.structure+json"],
        "is_json": True,
        "version": "2.1",
    },
    {
        "id": "fusionregistry:rest",
        "url": "/ws/rest",
        "expected_mime": JSON_MIMETYPES + XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "fusionregistry:sdmxapi",
        "url": "/ws/public/sdmxapi/rest",
        "expected_mime": JSON_MIMETYPES + XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

ARISTOTLEMDR_URLMAP = [
    # https://docs.aristotlemetadata.com/api/rest
    {
        "id": "aristotlemdr:api",
        "url": "/api/v4/",
        "expected_mime": JSON_MIMETYPES,
        "is_json": False,
        "version": "4",
    },
    {
        "id": "aristotlemdr:metadata",
        "url": "/api/v4/metadata/",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "4",
    },
]

EVERGIS_URLMAP = [
    # https://everpoint.github.io/api/resources/layer_list.html
    {
        "id": "evergis:layers",
        "url": "/sp/layers?group=public",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "evergis:projects",
        "url": "/sp/projects?group=public",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "evergis:tables",
        "url": "/sp/tables?group=public",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

NEXTGISWEB_URLMAP = [
    # https://docs.nextgis.com/docs_ngweb_dev/doc/developer/
    {
        "id": "nextgisweb:api",
        "url": "/api/resource/",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "nextgisweb:pkg-version",
        "url": "/api/component/pyramid/pkg_version",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "nextgisweb:routes",
        "url": "/api/component/pyramid/route",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

VUFIND_URLMAP = [
    # VuFind 9+ REST API; legacy installs use /Search/API
    {
        "id": "vufind:api",
        "url": "/api/v1/search",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1",
    },
    {
        "id": "vufind:api:legacy",
        "url": "/Search/API?method=search&lookfor=test&type=AllFields",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

WORDPRESS_URLMAP = [
    {
        "id": "rest",
        "url": "/wp-json/",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "rest:posts",
        "url": "/wp-json/wp/v2/posts",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "2",
    },
]

ONTOPORTAL_URLMAP = [
    # BioPortal / OntoPortal REST – https://data.bioontology.org/documentation
    {
        "id": "rest",
        "url": "/ontologies",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "rest:search",
        "url": "/search?q=test",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

GBIFPLATFORM_URLMAP = [
    # Registry entries often point at gbif.org data portal; API is on api.gbif.org
    # Probe only works when link host is api.gbif.org
    {
        "id": "gbif:dataset",
        "url": "/v1/dataset",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1",
    },
    {
        "id": "gbif:organization",
        "url": "/v1/organization",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1",
    },
]

OPENMLORG_URLMAP = [
    {
        "id": "openmlorgapi",
        "url": "/api/v1/json/data/list/data_name/iris/limit/1",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1",
    },
]

DSPACECRIS_URLMAP = [
    # DSpace 7+ REST (CRIS builds on DSpace)
    {
        "id": "dspace",
        "url": "/server/api",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "7",
    },
    {
        "id": "dspace:discover",
        "url": "/server/api/discover/search/objects",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "7",
    },
]

# Fill existing empty map in apidetect.py
OPENDAP_URLMAP_DRAFT = [
    {
        "id": "opendap:catalog",
        "url": "/",
        "expected_mime": HTML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "opendap:dds",
        "url": "/dds/",
        "expected_mime": HTML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "opendap:opendap",
        "url": "/opendap/",
        "expected_mime": HTML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

# ---------------------------------------------------------------------------
# Tier B – common patterns with deployment variance
# ---------------------------------------------------------------------------

LIZMAP_URLMAP = [
    # Lizmap proxies QGIS Server – paths vary by install prefix
    {
        "id": "lizmap:service:wms",
        "url": "/index.php/lizmap/service/?SERVICE=WMS&REQUEST=GetCapabilities&VERSION=1.3.0",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.3.0",
    },
    {
        "id": "lizmap:service:wms:alt",
        "url": "/lizmap/www/index.php/lizmap/service/?SERVICE=WMS&REQUEST=GetCapabilities&VERSION=1.3.0",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.3.0",
    },
    {
        "id": "wms111",
        "url": "/index.php/lizmap/service/?SERVICE=WMS&REQUEST=GetCapabilities&VERSION=1.1.1",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
]

MAPBENDER_URLMAP = [
    # https://docs.mapbender.org/current/en/customization/api.html
    {
        "id": "mapbender:api-doc",
        "url": "/api/doc/",
        "expected_mime": HTML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "mapbender:api-doc:mb3",
        "url": "/mapbender3/api/doc/",
        "expected_mime": HTML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

ALA_URLMAP = [
    # Living Atlases / ALA stack – https://docs.ala.org.au/
    {
        "id": "ala:api",
        "url": "/ws/species/search/auto?q=test&idxType=TAXON&limit=1",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "ala:collections",
        "url": "/ws/registry/collections",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "ala:occurrences",
        "url": "/ws/occurrences/search?q=test&pageSize=1",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

OBIBAMICA_URLMAP = [
    # Mica REST – https://micadoc.obiba.org/en/latest/rest/
    {
        "id": "obibamica:api",
        "url": "/studies",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "mica:api",
        "url": "/api/studies",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

COLECTICA_URLMAP = [
    # Swagger UI is the reliable unauthenticated probe; search is POST+auth
    {
        "id": "colectica:api",
        "url": "/swagger/ui",
        "expected_mime": HTML_MIMETYPES,
        "is_json": False,
        "version": "1",
    },
    {
        "id": "colectica:api:swagger",
        "url": "/swagger/v1/swagger.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1",
    },
]

GISWEBSE_URLMAP = [
    {
        "id": "wms130",
        "url": "/GISWebServiceSE/service.php?SERVICE=WMS&REQUEST=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.3.0",
    },
    {
        "id": "wfs200",
        "url": "/GISWebServiceSE/service.php?SERVICE=WFS&REQUEST=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.0",
    },
    {
        "id": "wmts100",
        "url": "/GISWebServiceSE/service.php?SERVICE=WMTS&REQUEST=GetCapabilities&VERSION=1.0.0",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
]

POPGIS_URLMAP = [
    # SPC PopGIS deployments expose /api on same host
    {
        "id": "customapi",
        "url": "/api",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "customapi:layers",
        "url": "/api/layers",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

ISIGEO_URLMAP = [
    {
        "id": "openapi",
        "url": "/api",
        "expected_mime": JSON_MIMETYPES + HTML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

ENTRYSCAPE_URLMAP = [
    {
        "id": "entrystore:search",
        "url": "/store/search",
        "expected_mime": JSON_MIMETYPES + XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "sparql",
        "url": "/sparql",
        "expected_mime": JSON_MIMETYPES + XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "dcatap21",
        "url": "/all.rdf",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

PUBLISHMYDATA_URLMAP = [
    {
        "id": "dcatus11",
        "url": "/data.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.1",
    },
]

DATAPRESS_URLMAP = [
    {
        "id": "dcatus11",
        "url": "/data.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.1",
    },
]

ISLANDORA_URLMAP = [
    # Drupal JSON:API + REST – https://islandora.github.io/documentation/
    {
        "id": "drupal:jsonapi",
        "url": "/jsonapi",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "islandora:rest",
        "url": "/node?_format=json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

NESSTAR_URLMAP = [
    {
        "id": "nesstar:webview",
        "url": "/webview/",
        "expected_mime": HTML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "nesstar:api",
        "url": "/api",
        "expected_mime": JSON_MIMETYPES + XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

STATTECH_URLMAP = [
    # .Stat Technology (SDMX/OData varies by agency)
    {
        "id": "stattech:api",
        "url": "/api",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "sdmx-json",
        "url": "/sdmx-json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

EUROSTAT_URLMAP = [
    {
        "id": "eurostat:json",
        "url": "/api/dissemination/statistics/1.0/data",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.0",
    },
]

ECB_URLMAP = [
    {
        "id": "sdmx:data",
        "url": "/service/data",
        "expected_mime": XML_MIMETYPES + JSON_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

DATABISORG_URLMAP = [
    {
        "id": "databisorgapi",
        "url": "/api/v0/search",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "0",
    },
]

RAMADDA_URLMAP = [
    # https://ramadda.geoscience.xyz/ – repository API
    {
        "id": "ramadda:api",
        "url": "/repository/api",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "ramadda:entries",
        "url": "/repository/entries?output=json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

HAPLO_URLMAP = [
    {
        "id": "haplo:api",
        "url": "/api",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

TABLION_URLMAP = [
    {
        "id": "tablion:api",
        "url": "/api",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

MWMB_URLMAP = [
    # Metadata Browser (MWMB) – typical OAI/REST installs
    {
        "id": "oaipmh20",
        "url": "/oai?verb=Identify",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
    {
        "id": "mwmb:api",
        "url": "/api",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

# ---------------------------------------------------------------------------
# Tier C – limited / auth / host-specific (optional deep probes)
# ---------------------------------------------------------------------------

CARTO_URLMAP = [
    # Legacy Carto Builder – only when link is {user}.carto.com
    {
        "id": "carto:sql",
        "url": "/api/v2/sql?q=SELECT%201",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "2",
    },
    {
        "id": "carto:v1",
        "url": "/api/v1/map",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1",
    },
]

STRAPI_URLMAP = [
    # Content-type slug unknown – probe common bootstrap endpoints
    {
        "id": "strapi:api",
        "url": "/api",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "strapi:users-permissions",
        "url": "/api/users-permissions/roles",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

BITRIX_URLMAP = [
    {
        "id": "bitrix:rest",
        "url": "/rest/",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

CONVERIS_URLMAP = [
    {
        "id": "converis:api",
        "url": "/ws/public/v1/projects",
        "expected_mime": JSON_MIMETYPES + XML_MIMETYPES,
        "is_json": False,
        "version": "1",
    },
]

DATALAD_URLMAP = []  # git/annex only – see NO_STANDARD_PROBE

SURVEYSOLUTIONS_URLMAP = [
    {
        "id": "surveysolutions:api",
        "url": "/api/v1/questionnaires",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1",
    },
]

OGC_XML_MIMETYPES = XML_MIMETYPES + [
    "application/vnd.ogc.wms_xml",
    "application/vnd.ogc.wfs_xml",
    "application/vnd.ogc.se_xml",
]

GEOMAPFISH_URLMAP = [
    # https://camptocamp.github.io/c2cgeoportal/master/
    {
        "id": "geomapfish:themes",
        "url": "/themes",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "wms111",
        "url": "/mapserv_proxy?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetCapabilities",
        "expected_mime": OGC_XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "wms130",
        "url": "/mapserv_proxy?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetCapabilities",
        "expected_mime": OGC_XML_MIMETYPES,
        "is_json": False,
        "version": "1.3.0",
    },
    {
        "id": "wfs200",
        "url": "/mapserv_proxy?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetCapabilities",
        "expected_mime": OGC_XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.0",
    },
    {
        "id": "wmts100",
        "url": "/tiles/1.0.0/WMTSCapabilities.xml",
        "expected_mime": OGC_XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
]

GETSDIPORTAL_URLMAP = [
    {
        "id": "wms111",
        "url": "/geoserver/ows?service=WMS&version=1.1.1&request=GetCapabilities",
        "expected_mime": OGC_XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "wms130",
        "url": "/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities",
        "expected_mime": OGC_XML_MIMETYPES,
        "is_json": False,
        "version": "1.3.0",
    },
    {
        "id": "wfs100",
        "url": "/geoserver/ows?service=WFS&version=1.0.0&request=GetCapabilities",
        "expected_mime": OGC_XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
    {
        "id": "wfs110",
        "url": "/geoserver/ows?service=WFS&version=1.1.0&request=GetCapabilities",
        "expected_mime": OGC_XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.0",
    },
    {
        "id": "wfs200",
        "url": "/geoserver/ows?service=WFS&version=2.0.0&request=GetCapabilities",
        "expected_mime": OGC_XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.0",
    },
    {
        "id": "wcs111",
        "url": "/geoserver/ows?service=WCS&version=1.1.1&request=GetCapabilities",
        "expected_mime": OGC_XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "wcs201",
        "url": "/geoserver/ows?service=WCS&version=2.0.1&request=GetCapabilities",
        "expected_mime": OGC_XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.1",
    },
]

REDATAM_URLMAP = [
    {
        "id": "redatam",
        "url": "/redbin/RpWebEngine.exe/Portal",
        "expected_mime": HTML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "redatam",
        "url": "/RpWebEngine.exe/Portal",
        "expected_mime": HTML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

SCICAT_URLMAP = [
    {
        "id": "customapi",
        "url": "/api/v3/datasets",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "3",
    },
    {
        "id": "customapi",
        "url": "/api/v3/Datasets",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "3",
    },
]

# MapStore2 often sits next to GeoServer; GeoStore REST is on the viewer path.
MAPSTORE_JSON_MIMETYPES = JSON_MIMETYPES + PLAIN_MIMETYPES + ["application/octet-stream"]
MAPSTORE_URLMAP = GETSDIPORTAL_URLMAP + [
    {
        "id": "mapstore:geostore",
        "url": "/rest/geostore/misc/categories/",
        "expected_mime": XML_MIMETYPES + JSON_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "mapstore:config",
        "url": "/configs/localConfig.json",
        "expected_mime": MAPSTORE_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

# Open SDG publishes indicator JSON at documented, language-prefixed paths:
# https://open-sdg.readthedocs.io/en/latest/faq/
OPENSDG_JSON_MIMETYPES = JSON_MIMETYPES + PLAIN_MIMETYPES + ["application/octet-stream"]
OPENSDG_URLMAP = [
    {
        "id": "opensdg:data",
        "url": "/data/1-1-1.json",
        "expected_mime": OPENSDG_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "opensdg:data",
        "url": "/en/data/1-1-1.json",
        "expected_mime": OPENSDG_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "opensdg:data",
        "url": "/sdg-data/en/data/1-1-1.json",
        "expected_mime": OPENSDG_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "opensdg:data",
        "url": "/data/1-2-1.json",
        "expected_mime": OPENSDG_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "opensdg:data",
        "url": "/en/data/1-2-1.json",
        "expected_mime": OPENSDG_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "opensdg:reporting",
        "url": "/data/reporting.json",
        "expected_mime": OPENSDG_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

TERRIA_URLMAP = [
    {
        "id": "terria:config",
        "url": "/config.json",
        "expected_mime": JSON_MIMETYPES + PLAIN_MIMETYPES + ["application/octet-stream"],
        "is_json": True,
        "version": None,
    },
]

SEEK_URLMAP = [
    {
        "id": "customapi",
        "url": "/api",
        "expected_mime": JSON_MIMETYPES + HTML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

SUPERMAPISERVER_URLMAP = [
    {
        "id": "supermap:services",
        "url": "/services.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "supermap:services",
        "url": "/iserver/services.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

# MapGIS IGServer (.NET often :6163, Java often :8089). IGS 1.0 catalog is
# /igs/rest/mrcs/docs; IGS 2.0 lists services at /igs/rest/services.
MAPGIS_JSON_MIMETYPES = JSON_MIMETYPES + PLAIN_MIMETYPES
MAPGISIGSERVER_URLMAP = [
    {
        "id": "mapgis:docs",
        "url": "/rest/mrcs/docs?f=json",
        "expected_mime": MAPGIS_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "mapgis:docs",
        "url": "/igs/rest/mrcs/docs?f=json",
        "expected_mime": MAPGIS_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "mapgis:services",
        "url": "/rest/services?f=json",
        "expected_mime": MAPGIS_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "mapgis:services",
        "url": "/igs/rest/services?f=json",
        "expected_mime": MAPGIS_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

# Provincial 天地图 nodes sit on SuperMap iServer/iPortal or ArcGIS Server.
# Viewer paths (/map, /jiaozuo/, *.html) are stripped to origin in apidetect.
# Do not probe national t0.tianditu.gov.cn tiles or token-gated JS APIs here.
TIANDITU_JSON_MIMETYPES = JSON_MIMETYPES + PLAIN_MIMETYPES
TIANDITU_URLMAP = [
    {
        "id": "supermap:services",
        "url": "/iserver/services.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "supermapiportal:services",
        "url": "/iportal/web/services.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "arcgis:rest:services",
        "url": "/arcgis/rest/services?f=pjson",
        "expected_mime": TIANDITU_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "arcgis:rest:info",
        "url": "/arcgis/rest/info?f=pjson",
        "expected_mime": TIANDITU_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "arcgis:rest:services",
        "url": "/OneMapServer/rest/services?f=pjson",
        "expected_mime": TIANDITU_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

GVSIGONLINE_URLMAP = GETSDIPORTAL_URLMAP

INGRID_URLMAP = [
    {
        "id": "csw202",
        "url": "/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.2",
    },
    {
        "id": "csw202",
        "url": "/interface/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.2",
    },
]

ERDASAPOLLO_URLMAP = [
    {
        "id": "wms130",
        "url": "/erdas-iws/ogc/wms/?service=WMS&request=GetCapabilities&version=1.3.0",
        "expected_mime": OGC_XML_MIMETYPES,
        "is_json": False,
        "version": "1.3.0",
    },
    {
        "id": "wms111",
        "url": "/erdas-iws/ogc/wms/?service=WMS&request=GetCapabilities&version=1.1.1",
        "expected_mime": OGC_XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
]

DRUPAL_JSONAPI_MIMETYPES = JSON_MIMETYPES + ["application/vnd.api+json"]
DRUPAL_URLMAP = [
    {
        "id": "drupal:jsonapi",
        "url": "/jsonapi",
        "accept": "application/vnd.api+json, application/json",
        "expected_mime": DRUPAL_JSONAPI_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

ICAT_URLMAP = [
    {
        "id": "oaipmh20",
        "url": "/oaipmh/request?verb=Identify",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
    {
        "id": "icat:datagateway-api",
        "url": "/datagateway-api",
        "expected_mime": JSON_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

COGIS_JSON_MIMETYPES = JSON_MIMETYPES + PLAIN_MIMETYPES
COGIS_URLMAP = [
    {
        "id": "arcgis:rest:services",
        "url": "/elitegis/rest/services?f=pjson",
        "accept": "application/json",
        "expected_mime": COGIS_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "arcgis:rest:services",
        "url": "/arcgis3/rest/services?f=pjson",
        "accept": "application/json",
        "expected_mime": COGIS_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "arcgis:rest:services",
        "url": "/arcgisserver/rest/services?f=pjson",
        "accept": "application/json",
        "expected_mime": COGIS_JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

GIN_URLMAP = [
    {
        "id": "gogs:api",
        "url": "/api/v1/version",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

OSF_JSONAPI_MIMETYPES = JSON_MIMETYPES + ["application/vnd.api+json"]
OSF_URLMAP = [
    {
        "id": "osf:api",
        "url": "/v2/",
        "absolute_url": "https://api.osf.io/v2/",
        "expected_mime": OSF_JSONAPI_MIMETYPES,
        "is_json": True,
        "version": "2",
    },
]

SAMVERA_URLMAP = [
    {
        "id": "hyrax:catalog",
        "url": "/catalog.json",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "hyrax:catalog",
        "url": "/catalog",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

SMW_URLMAP = [
    {
        "id": "mediawiki:api",
        "url": "/w/api.php?action=query&meta=siteinfo&format=json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "mediawiki:api",
        "url": "/api.php?action=query&meta=siteinfo&format=json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "smw:ask",
        "url": "/api.php?action=askargs&format=json&conditions=[[Category:+]]&printouts=Category&parameters=limit=1",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

ENSEMBL_URLMAP = [
    {
        "id": "rest",
        "url": "/rest/info/ping",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

PHAIDRA_JSON_MIMETYPES = JSON_MIMETYPES + PLAIN_MIMETYPES
PHAIDRA_URLMAP = [
    {
        "id": "rest",
        "url": "/api/search/select",
        "expected_mime": PHAIDRA_JSON_MIMETYPES + XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "oaipmh20",
        "url": "/api/oai?verb=Identify",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
    {
        "id": "openapi",
        "url": "/api/openapi/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

MAPTILERSERVER_URLMAP = [
    {
        "id": "openapi",
        "url": "/api",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

MYTARDIS_URLMAP = [
    {
        "id": "rest",
        "url": "/api/v1/",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1",
    },
]

NYUDATACATALOG_URLMAP = []  # schema.org DataCatalog JSON-LD on the homepage

BREEDBASE_URLMAP = [
    {
        "id": "brapi:serverinfo",
        "url": "/brapi/v2/serverinfo",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "2",
    },
    {
        "id": "brapi:studies",
        "url": "/brapi/v2/studies?page=0&pageSize=1",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "2",
    },
]

TRIPAL_URLMAP = [
    {
        "id": "tripal:webservices",
        "url": "/web-services/",
        "expected_mime": HTML_MIMETYPES + JSON_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

VEUPATHDB_URLMAP = [
    {
        "id": "veupathdb:webservices",
        "url": "/webservices/",
        "expected_mime": HTML_MIMETYPES + JSON_MIMETYPES + XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

MASSBANK_URLMAP = [
    {
        "id": "massbank:api",
        "url": "/MassBank/api/records",
        "expected_mime": JSON_MIMETYPES + HTML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "mona:spectra",
        "url": "/rest/spectra",
        "expected_mime": JSON_MIMETYPES + HTML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

IOCHEMBD_URLMAP = [
    {
        "id": "dspace:items",
        "url": "/rest/items",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "6",
    },
    {
        "id": "oaipmh20",
        "url": "/oai/request?verb=Identify",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
]

ESGF_URLMAP = [
    {
        "id": "esgf:search",
        "url": "/esg-search/search?format=application%2Fsolr%2Bjson&limit=0",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

# ---------------------------------------------------------------------------
# Proposed CATALOGS_URLMAP additions (merge into apidetect.py)
# ---------------------------------------------------------------------------

DRAFT_CATALOGS_URLMAP = {
    # Tier A
    "stacserver": STACSERVER_URLMAP,
    "galaxy": GALAXY_URLMAP,
    "udata": UDATA_URLMAP,
    "rasdaman": RASDAMAN_URLMAP,
    "fusionregistry": FUSIONREGISTRY_URLMAP,
    "aristotlemdr": ARISTOTLEMDR_URLMAP,
    "nextgisweb": NEXTGISWEB_URLMAP,
    "evergis": EVERGIS_URLMAP,
    "vufind": VUFIND_URLMAP,
    "wordpress": WORDPRESS_URLMAP,
    "ontoportal": ONTOPORTAL_URLMAP,
    "gbifplatform": GBIFPLATFORM_URLMAP,
    "openmlorg": OPENMLORG_URLMAP,
    "dspacecris": DSPACECRIS_URLMAP,
    "geomapfish": GEOMAPFISH_URLMAP,
    "getsdiportal": GETSDIPORTAL_URLMAP,
    "redatam": REDATAM_URLMAP,
    "scicat": SCICAT_URLMAP,
    "mapstore": MAPSTORE_URLMAP,
    "opensdg": OPENSDG_URLMAP,
    "terria": TERRIA_URLMAP,
    "seek": SEEK_URLMAP,
    "supermapiserver": SUPERMAPISERVER_URLMAP,
    "mapgisigserver": MAPGISIGSERVER_URLMAP,
    "tianditu": TIANDITU_URLMAP,
    "gvsigonline": GVSIGONLINE_URLMAP,
    "ingrid": INGRID_URLMAP,
    "erdasapollo": ERDASAPOLLO_URLMAP,
    "drupal": DRUPAL_URLMAP,
    "icat": ICAT_URLMAP,
    "cogis": COGIS_URLMAP,
    "elitegis": COGIS_URLMAP,
    "gin": GIN_URLMAP,
    "osf": OSF_URLMAP,
    "samvera": SAMVERA_URLMAP,
    "smw": SMW_URLMAP,
    "ensembl": ENSEMBL_URLMAP,
    "phaidra": PHAIDRA_URLMAP,
    "maptilerserver": MAPTILERSERVER_URLMAP,
    "mytardis": MYTARDIS_URLMAP,
    "nyudatacatalog": NYUDATACATALOG_URLMAP,
    # Tier B
    "lizmap": LIZMAP_URLMAP,
    "mapbender": MAPBENDER_URLMAP,
    "ala": ALA_URLMAP,
    "obibamica": OBIBAMICA_URLMAP,
    "colectica": COLECTICA_URLMAP,
    "giswebse": GISWEBSE_URLMAP,
    "popgis": POPGIS_URLMAP,
    "isigeo": ISIGEO_URLMAP,
    "entryscape": ENTRYSCAPE_URLMAP,
    "publishmydata": PUBLISHMYDATA_URLMAP,
    "datapress": DATAPRESS_URLMAP,
    "islandora": ISLANDORA_URLMAP,
    "nesstar": NESSTAR_URLMAP,
    "stattech": STATTECH_URLMAP,
    "eurostat": EUROSTAT_URLMAP,
    "ecb": ECB_URLMAP,
    "databisorg": DATABISORG_URLMAP,
    "ramadda": RAMADDA_URLMAP,
    "haplo": HAPLO_URLMAP,
    "tablion": TABLION_URLMAP,
    "mwmb": MWMB_URLMAP,
    # Tier C
    "carto": CARTO_URLMAP,
    "strapi": STRAPI_URLMAP,
    "bitrix": BITRIX_URLMAP,
    "converis": CONVERIS_URLMAP,
    "surveysolutions": SURVEYSOLUTIONS_URLMAP,
    "breedbase": BREEDBASE_URLMAP,
    "tripal": TRIPAL_URLMAP,
    "veupathdb": VEUPATHDB_URLMAP,
    "massbank": MASSBANK_URLMAP,
    "iochembd": IOCHEMBD_URLMAP,
    "esgf": ESGF_URLMAP,
}

# Software reviewed for auto-fill: do not invent relative API paths.
# Entries that now have a URLMAP were removed from this list.
NO_STANDARD_PROBE = {
    "activemapgis": "Proprietary GIS; no documented public REST on portal URL.",
    "aodn": "AODN portal search API path varies (/portal/search/api).",
    "axiomportal": "Axiom Data Science portals; instance-specific ERDDAP/API hosts.",
    "cadenza": "disy Cadenza; JSF workbook paths, no stable anonymous catalog API.",
    "cardo": "cardo GIS viewers; no shared REST path on the portal URL.",
    "copernicuscds": "CDS retrieve API needs a personal access token, not on catalog link.",
    "d4science": "VRE platform; API behind auth, no stable relative path.",
    "datacubews": "Datacube OWS only; link often points at OWS not STAC root.",
    "datafair": "Data Fair/Koumoul; instance-specific API paths.",
    "datagovmy": "Static site generators; mostly sitemap-only in records.",
    "datalibrary": "Esri Data Library; no standard open API path.",
    "dataone": "Metacat OAI/sitemap; API on separate metacat paths.",
    "datauniceforg": "UNICEF data site; external API not on catalog link.",
    "datavavt": "Custom /analytic/api/v1 on Russian portals.",
    "datawheel": "DataWheel sites; frontend-only, no common /api.",
    "dataworldbankorg": "API on api.worldbank.org not catalog link.",
    "datalad": "DataLad/git annex – no HTTP API on portal link; git-only.",
    "dlibra": "dLibra OAI/REST varies by install; no single path.",
    "ewmapa": "geoportal2.pl HTML viewers; WMS often 403 and path is instance-specific.",
    "fedora": "Fedora LDP/OAI is behind a public UI; leftover links are not Fedora roots.",
    "gcnavi": "GC Navi municipal viewers; no documented catalog API on the viewer URL.",
    "genesisonline": "GENESIS-Online web services are POST-only (Destatis as of mid-2025).",
    "geonomics": "Kazakh municipal GIS; no shared REST path.",
    "geomediawebmap": "Hexagon GeoMedia WebMap; no standard relative catalog API.",
    "geoportalrlp": "Custom geoportal CMS; sitemap only in records.",
    "gisoftgis": "GISoft GIS viewers; no documented public REST on portal URL.",
    "ilostat": "ILOSTAT bulk download; no API on www host.",
    "instdb": "Institutional CRIS; generic /api per site.",
    "jdop": "Zhejiang JDOP portals; no documented anonymous default API path.",
    "mangomap": "MangoMap hosted maps; no shared catalog API on /maps URLs.",
    "mapapps": "con terra map.apps; OWS service names are instance-specific.",
    "mapbiomas": "MapBiomas country platforms; no shared catalog API on plataforma URLs.",
    "mapserver": "MapServer CGI/OWS path is instance-specific (not a generic /geomet).",
    "masterportal": "Masterportal config/service JSON names are instance-specific.",
    "modaopendata": "Taiwan MODA OpenAPI swagger path is not present on all city portals.",
    "netgisserver": "NetCAD KEOS/NetGIS; no documented public REST on /keos URLs.",
    "nolis": "NOL-IS municipal viewers; no documented catalog API on the viewer URL.",
    "ogdindia": "OGD Platform India dataset APIs require a registered API key.",
    "omegapsir": "Omega-PSIR Seam pages; no REST catalog.",
    "opendatacube": "STAC under explorer path – use stacserver rules.",
    "opengeoportal": "Legacy OGP; OAI and Solr paths vary.",
    "oportal": "Inspur oPortal; no verified anonymous default API on /oportal URLs.",
    "oracleapex": "APEX apps; no standard API on portal URL.",
    "pomosam": "Slovak eGov CMS; sitemap only, no public API documentation found.",
    "pydap": "PyDAP server root; overlap with opendap/thredds.",
    "reearth": "Re:Earth/PLATEAU VIEW; 3D viewer, no catalog harvest API on the viewer URL.",
    "seoulopendataplaza": "Open API developer space requires a key; sitemaps only on some tenants.",
    "seue": "Catalan seu-e.cat HTML transparency pages, not a machine catalog API.",
    "smartfindersdi": "Custom SDI portals; CSW path varies, sitemap only on most records.",
    "superstar": "Space-Time Research SuperSTAR; desktop/server product.",
    "wagmap": "わが街ガイド HTML geoportals; /opendata/ is HTML, not a harvest API.",
    "weboffice": "VertiGIS WebOffice; no standard relative catalog API on the viewer URL.",
    "geocortex": "Geocortex Essentials REST Sites Directory; path is instance-specific (often /Geocortex/Essentials/REST/sites).",
    "whoint": "WHO website; not a data API on link.",
}
