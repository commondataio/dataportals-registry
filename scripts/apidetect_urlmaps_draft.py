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
        "expected_mime": JSON_MIMETYPES + HTML_MIMETYPES,
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

SMW_URLMAP = [
    # Semantic MediaWiki – action API
    {
        "id": "smw:ask",
        "url": "/api.php?action=askargs&format=json&conditions=[[Category:+]]&printouts=Category&parameters=limit=1",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

SURVEYSOLUTIONS_URLMAP = [
    {
        "id": "surveysolutions:api",
        "url": "/api/v1/questionnaires",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1",
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
    "vufind": VUFIND_URLMAP,
    "wordpress": WORDPRESS_URLMAP,
    "ontoportal": ONTOPORTAL_URLMAP,
    "gbifplatform": GBIFPLATFORM_URLMAP,
    "openmlorg": OPENMLORG_URLMAP,
    "dspacecris": DSPACECRIS_URLMAP,
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
    "smw": SMW_URLMAP,
}

# Software with has_api=Yes but NO draft URLMAP (see devdocs note)
NO_STANDARD_PROBE = {
    "activemapgis": "Proprietary GIS; no documented public REST on portal URL.",
    "aodn": "AODN portal search API path varies (/portal/search/api).",
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
    "elitegis": "ArcGIS-compatible /elitegis/rest/services (see arcgisserver).",
    "erdasapollo": "ERDAS APOLLO proprietary services.",
    "geoportalrlp": "Custom geoportal CMS; sitemap only in records.",
    "icat": "ICAT REST at /icat/api/v1 but path prefix varies.",
    "ilostat": "ILOSTAT bulk download; no API on www host.",
    "instdb": "Institutional CRIS; generic /api per site.",
    "lizmap": "Listed in draft – high path variance.",
    "mapbender": "Listed in draft – admin API needs JWT.",
    "mytardis": "MyTardis REST at /api/v1/ but auth required.",
    "obibamica": "Listed in draft.",
    "omegapsir": "Omega-PSIR Seam pages; no REST catalog.",
    "opendatacube": "STAC under explorer path – use stacserver rules.",
    "opengeoportal": "Legacy OGP; OAI and Solr paths vary.",
    "oracleapex": "APEX apps; no standard API on portal URL.",
    "pomosam": "Slovak eGov CMS; sitemap only in 64 records.",
    "pydap": "PyDAP server root; overlap with opendap/thredds.",
    "samvera": "Samvera/Hyrax family – use hyrax rules.",
    "smartfindersdi": "Custom SDI portals; sitemap only.",
    "smw": "Semantic MediaWiki ask API – /api.php?action=askargs.",
    "stacserver": "Listed in draft.",
    "supermapiserver": "SuperMap iServer REST at /iserver/services but path varies.",
    "superstar": "Space-Time Research SuperSTAR; desktop/server product.",
    "whoint": "WHO website; not a data API on link.",
    "pomosam": "No public API documentation found.",
}
