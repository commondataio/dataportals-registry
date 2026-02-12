#!/usr/bin/env python
# This script intended to detect data catalogs API
import logging
import sys
from io import BytesIO
import typer
from typing_extensions import Annotated
import requests
from urllib.parse import urljoin
import datetime
import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
import csv
import json
import os
import shutil
import pprint
from urllib.parse import urlparse
import lxml.html
import lxml.etree
import urllib.robotparser
from requests.exceptions import ConnectionError, TooManyRedirects, ContentDecodingError
from urllib3.exceptions import InsecureRequestWarning  # , ConnectionError

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,
)
root = logging.getLogger()
root.setLevel(logging.INFO)


_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)
ENTRIES_DIR = os.path.join(_REPO_ROOT, "data", "entities")
SCHEDULED_DIR = os.path.join(_REPO_ROOT, "data", "scheduled")
app = typer.Typer()

DEFAULT_TIMEOUT = 5

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
)

KML_MIMETYPES = ["application/vnd.google-earth.kml+xml"]
HTML_MIMETYPES = ["text/html", "text/html; charset=UTF-8"]
XML_MIMETYPES = [
    "text/xml",
    "application/xml",
    "application/vnd.ogc.se_xml",
    "application/vnd.ogc.wms_xml",
    "application/rdf+xml",
    "application/rss+xml",
    "application/atom+xml",
    "application/xml;charset=UTF-8",
] + KML_MIMETYPES
JSON_MIMETYPES = [
    "text/json",
    "application/json",
    "application/hal+json",
    "application/vnd.oai.openapi+json;version=3.0; charset=utf-8",
    "application/vnd.oai.openapi+json",
]
N3_MIMETYPES = ["text/n3"]
ZIP_MIMETYPES = ["application/zip"]
EXCEL_MIMETYPES = [
    "application/vnd.ms-excel",
]

CSV_MIMETYPES = ["text/csv"]
PLAIN_MIMETYPES = ["text/plain"]
KMZ_MIMETYPES = ["application/vnd.google-earth.kmz"]


GEONETWORK_SEARCH_POST_PARAMS = """{"from":0,"size":20, "bucket" : "metadata", "sort":["_score"],"query":{"function_score":{"boost":"5","functions":[{"filter":{"match":{"resourceType":"series"}},"weight":1.5},{"filter":{"exists":{"field":"parentUuid"}},"weight":0.3},{"filter":{"match":{"cl_status.key":"obsolete"}},"weight":0.2},{"filter":{"match":{"cl_status.key":"superseded"}},"weight":0.3},{"gauss":{"dateStamp":{"scale":"365d","offset":"90d","decay":0.5}}}],"score_mode":"multiply","query":{"bool":{"must":[{"terms":{"isTemplate":["n"]}}]}}}},"aggregations":{"groupOwner":{"terms":{"field":"groupOwner"},"aggs":{"sourceCatalogue":{"terms":{"field":"sourceCatalogue"}}},"meta":{"field":"groupOwner"}},"resourceType":{"terms":{"field":"resourceType"},"meta":{"decorator":{"type":"icon","prefix":"fa fa-fw gn-icon-"},"field":"resourceType"}},"availableInServices":{"filters":{"filters":{"availableInViewService":{"query_string":{"query":"+linkProtocol:/OGC:WMS.*/"}},"availableInDownloadService":{"query_string":{"query":"+linkProtocol:/OGC:WFS.*/"}}}},"meta":{"decorator":{"type":"icon","prefix":"fa fa-fw ","map":{"availableInViewService":"fa-globe","availableInDownloadService":"fa-download"}}}},"cl_topic.key":{"terms":{"field":"cl_topic.key","size":5},"meta":{"decorator":{"type":"icon","prefix":"fa fa-fw gn-icon-"},"field":"cl_topic.key"}},"th_httpinspireeceuropaeutheme-theme_tree.key":{"terms":{"field":"th_httpinspireeceuropaeutheme-theme_tree.key","size":5},"meta":{"decorator":{"type":"icon","prefix":"fa fa-fw gn-icon iti-","expression":"http://inspire.ec.europa.eu/theme/(.*)"},"field":"th_httpinspireeceuropaeutheme-theme_tree.key"}},"tag":{"terms":{"field":"tag.default","include":".*","size":5},"meta":{"caseInsensitiveInclude":true,"field":"tag.default"}},"sourceCatalogue":{"terms":{"field":"sourceCatalogue","size":5,"include":".*"},"meta":{"orderByTranslation":true,"filterByTranslation":true,"displayFilter":true,"field":"sourceCatalogue"}},"OrgForResource":{"terms":{"field":"OrgForResourceObject.default","include":".*","size":5},"meta":{"caseInsensitiveInclude":true,"field":"OrgForResourceObject.default"}},"creationYearForResource":{"terms":{"field":"creationYearForResource","size":5,"order":{"_key":"desc"}},"meta":{"field":"creationYearForResource"}},"format":{"terms":{"field":"format","size":5,"order":{"_key":"asc"}},"meta":{"field":"format"}},"cl_spatialRepresentationType.key":{"terms":{"field":"cl_spatialRepresentationType.key","size":5},"meta":{"field":"cl_spatialRepresentationType.key"}},"cl_maintenanceAndUpdateFrequency.key":{"terms":{"field":"cl_maintenanceAndUpdateFrequency.key","size":5},"meta":{"field":"cl_maintenanceAndUpdateFrequency.key"}},"cl_status.key":{"terms":{"field":"cl_status.key","size":5},"meta":{"field":"cl_status.key"}},"resolutionScaleDenominator":{"terms":{"field":"resolutionScaleDenominator","size":5,"order":{"_key":"asc"}},"meta":{"field":"resolutionScaleDenominator"}},"resolutionDistance":{"terms":{"field":"resolutionDistance","size":5,"order":{"_key":"asc"}},"meta":{"field":"resolutionDistance"}},"dateStamp":{"auto_date_histogram":{"field":"dateStamp","buckets":50}}},"_source":{"includes":["uuid","id","groupOwner","logo","cat","inspireThemeUri","inspireTheme_syn","cl_topic","resourceType","resourceTitle*","resourceAbstract*","draft","owner","link","status*","rating","geom","contact*","Org*","isTemplate","valid","isHarvested","dateStamp","documentStandard","standardNameObject.default","cl_status*","mdStatus*"]},"script_fields":{"overview":{"script":{"source":"return params['_source'].overview == null ? [] : params['_source'].overview.stream().findFirst().orElse([]);"}}},"track_total_hits":true}"""

GEONODE_URLMAP = [
    {
        "id": "geonode:layers",
        "url": "/api/layers/",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "",
    },
    {
        "id": "geonode:datasets",
        "url": "/api/datasets/",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "",
    },
    {
        "id": "geonode:documents",
        "url": "/api/documents/",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "",
    },
    {
        "id": "dcatus11",
        "url": "/data.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "",
    },
    {
        "id": "csw202",
        "url": "/catalogue/csw?service=CSW&version=2.0.2&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.2",
    },
    {
        "id": "oaipmh20",
        "url": "/catalogue/csw?mode=oaipmh&verb=Identify",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
    {
        "id": "opensearch",
        "url": "/catalogue/csw?mode=opensearch&service=CSW&version=2.0.2&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0",
    },
    {
        "id": "wms111",
        "url": "/geoserver/ows?service=WMS&version=1.1.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "wfs110",
        "url": "/geoserver/ows?service=WFS&version=1.1.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.0",
    },
    {
        "id": "wcs111",
        "url": "/geoserver/ows?service=WCS&version=1.1.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "wmts100",
        "url": "/geoserver/gwc/service/wmts?service=WMTS&version=1.0.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
    {
        "id": "wms130",
        "url": "/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.3.0",
    },
    {
        "id": "wfs100",
        "url": "/geoserver/ows?service=WFS&version=1.0.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
    {
        "id": "wfs200",
        "url": "/geoserver/ows?service=WFS&version=2.0.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.0",
    },
    {
        "id": "wcs100",
        "url": "/geoserver/ows?service=WCS&version=1.0.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
    {
        "id": "wcs110",
        "url": "/geoserver/ows?service=WCS&version=1.1.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.0",
    },
    {
        "id": "wcs11",
        "url": "/geoserver/ows?service=WCS&version=1.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1",
    },
    {
        "id": "wcs201",
        "url": "/geoserver/ows?service=WCS&version=2.0.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.1",
    },
    {
        "id": "wps100",
        "url": "/geoserver/ows?service=WPS&version=1.0.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
    {
        "id": "tms100",
        "url": "/geoserver/gwc/service/tms/1.0.0",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
    {
        "id": "wms-c111",
        "url": "/geoserver/gwc/service/wms?request=GetCapabilities&version=1.1.1&tiled=true",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "ogc:tiles",
        "url": "/geoserver/ogc/tiles/collections",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "ogc:images",
        "url": "/geoserver/ogc/images/collections",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "ogc:maps",
        "url": "/geoserver/ogc/maps/collections",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "ogc:features",
        "url": "/geoserver/ogc/features/collections",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "geoserver:version",
        "url": "/geoserver/rest/about/version",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "geoserver:server-status",
        "url": "/geoserver/rest/about/server-status",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "geoserver:settings",
        "url": "/geoserver/rest/settings",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "geoserver:layers",
        "url": "/geoserver/rest/layers",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

DKAN_URLMAP = [
    {
        "id": "ckan:package-search",
        "url": "/api/3/action/package_search",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "3",
    },
    {
        "id": "ckan:package-list",
        "url": "/api/3/action/package_list",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "3",
    },
    {
        "id": "dcatus11",
        "url": "/data.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "dkan:search",
        "url": "/api/1/search",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1",
    },
    {
        "id": "dkan:metastore",
        "url": "/api/1/metastore",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1",
    },
    {
        "id": "dkan:datastore",
        "url": "/api/1/metastore",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1",
    },
    {
        "id": "dcat:n3",
        "url": "/catalog.n3",
        "expected_mime": "text/n3",
        "is_json": False,
        "version": None,
    },
    {
        "id": "dcat:ttl",
        "url": "/catalog.ttl",
        "expected_mime": "text/turtle",
        "is_json": False,
        "version": None,
    },
    {
        "id": "dcat:xml",
        "url": "/catalog.xml",
        "expected_mime": "application/rdf+xml",
        "is_json": False,
        "version": None,
    },
    {
        "id": "dcat:jsonld",
        "url": "/catalog.jsonld",
        "expected_mime": "application/ld+json",
        "is_json": True,
        "version": None,
    },
]

CKAN_URLMAP = [
    {
        "id": "ckan",
        "url": "/api/3",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "3",
    },
    {
        "id": "ckan:package-search",
        "url": "/api/3/action/package_search",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "3",
    },
    {
        "id": "ckan:package-list",
        "url": "/api/3/action/package_list",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "3",
    },
    {
        "id": "dcatus11",
        "url": "/data.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "dcat:n3",
        "url": "/catalog.n3",
        "expected_mime": "text/n3",
        "is_json": False,
        "version": None,
    },
    {
        "id": "dcat:ttl",
        "url": "/catalog.ttl",
        "expected_mime": "text/turtle",
        "is_json": False,
        "version": None,
    },
    {
        "id": "dcat:xml",
        "url": "/catalog.xml",
        "expected_mime": "application/rdf+xml",
        "is_json": False,
        "version": None,
    },
    {
        "id": "dcat:jsonld",
        "url": "/catalog.jsonld",
        "expected_mime": "application/ld+json",
        "is_json": True,
        "version": None,
    },
]

IPT_URLMAP = [
    {
        "id": "dcat:ttl",
        "url": "/dcat",
        "expected_mime": "text/turtle",
        "is_json": False,
        "version": None,
    },
    {
        "id": "ipt:dataset",
        "url": "/inventory/dataset",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "rss",
        "url": "/rss.do",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
]


JUNAR_URLMAP = [
    {
        "id": "dcatus11",
        "url": "/data.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

TRIPLYDB_URLMAP = [
    {
        "id": "triplydb:datasets",
        "url": "/_api/facets/datasets",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]


GEONETWORK_URLMAP = [
    {
        "id": "geonetwork:api",
        "url": "/srv/api",
        "expected_mime": XML_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "geonetwork:query",
        "display_url": "/srv/eng/q",
        "url": "/srv/eng/q?_content_type=json&bucket=s101&facet.q=&fast=index&resultType=details&sortBy=relevance&sortOrder=&title_OR_altTitle_OR_any=",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "geonetwork:records",
        "url": "/srv/api/records",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "dcatus11",
        "url": "/data.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "openapi",
        "url": "/srv/v2/api-docs",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "2",
    },
    {
        "id": "csw202",
        "url": "/srv/eng/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.2",
    },
    {
        "id": "opensearch",
        "url": "/srv/eng/portal.opensearch",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0",
    },
    {
        "id": "oaipmh20",
        "url": "/srv/eng/oaipmh?verb=Identify",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
    #    {'id' : 'geonetwork:search', 'url' : '/srv/api/search/records/_search?bucket=metadata', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : False, 'version': None, 'post_params': GEONETWORK_SEARCH_POST_PARAMS},
    {
        "id": "geonetwork:settings",
        "url": "/srv/api/settings",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "geonetwork:selections",
        "url": "/srv/api/selections",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "geonetwork:site",
        "url": "/srv/api/site",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

FIGSHARE_URLMAP = [
    {
        "id": "sitemap",
        "url": "/sitemap/siteindex.xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
        "urlpat": "/articles/dataset/",
    },
    {
        "id": "figshare:graphql",
        "url": "/api/graphql?thirdPartyCookies=true&type=current&operation=advancedSearch",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.0",
    },
]

SOCRATA_URLMAP = [
    {
        "id": "dcatus11",
        "url": "/data.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "socrata:views",
        "url": "/api/views",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

PXWEB_URLMAP = [
    {
        "id": "pxwebapi",
        "url": "/api/v1/en/",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1",
    }
]

STATSUITE_URLMAP = [
    {
        "id": "statsuite:search",
        "url": "/api/search",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    }
]


DATAVERSE_URLMAP = [
    {
        "id": "dataverseapi",
        "display_url": "/api/search",
        "url": "/api/search?q=*&type=dataset&sort=name&order=asc",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "oaipmh20",
        "url": "/oai?verb=Identify",
        "accept": "application/xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
]


INVENIORDM_URLMAP = [
    {
        "id": "inveniordmapi",
        "url": "/api",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "inveniordmapi:records",
        "url": "/api/records",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "oaipmh20",
        "url": "/oai2d",
        "accept": "application/xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
]

INVENIO_URLMAP = [
    {
        "id": "inveniordmapi",
        "url": "/api",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "inveniordmapi:records",
        "url": "/api/records",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "oaipmh20",
        "url": "/oai2d",
        "accept": "application/xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
]


WORKTRIBE_URLMAP = [
    {
        "id": "oaipmh20",
        "url": "/oaiprovider?verb=Identify",
        "accept": "application/xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
    {
        "id": "sitemap",
        "url": "/sitemap_index.xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]


HYRAX_URLMAP = [
    {
        "id": "hyrax:catalog",
        "url": "/catalog",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    }
]

DSPACE_URLMAP = [
    {
        "id": "dspace",
        "url": "/server/api",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "7",
    },
    {
        "id": "dspace:objects",
        "url": "/server/api/discover/search/objects",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "7",
    },
    {
        "id": "dspace:items",
        "url": "/rest/items",
        "accept": "application/json",
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
    {
        "id": "opensearch",
        "url": "/open-search/description.xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0",
    },
    {
        "id": "rss",
        "url": "/feed/rss_2.0/site",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
    {
        "id": "atom",
        "url": "/feed/atom_1.0/site",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0",
    },
    {
        "id": "sitemap",
        "url": "/sitemap_index.xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

ESPLORO_URLMAP = [
    {
        "id": "esploro:search",
        "display_url": "/esplorows/rest/research/simpleSearch",
        "url": "/esplorows/rest/research/simpleSearch?_wadl",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "sitemap",
        "url": "/view/google/siteindex.xml",
        "is_json": False,
        "version": None,
        "urlpat": "/dataset/",
    },
]

ELSEVIERPURE_URLMAP = [
    {
        "id": "oaipmh20",
        "url": "/ws/oai?verb=Identify",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
    {
        "id": "rss",
        "url": "/en/datasets/?search=&isCopyPasteSearch=false&format=rss",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
    {
        "id": "sitemap",
        "url": "/sitemap/datasets.xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "pure:export_excel",
        "url": "/en/datasets/?export=xls",
        "expected_mime": EXCEL_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

ELSEVIERDC_URLMAP = [
    {
        "id": "sitemap",
        "url": "/sitemap/index",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]


NADA_URLMAP = [
    {
        "id": "nada:catalog-search",
        "url": "/index.php/api/catalog/search",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "nada:csvexport",
        "url": "/index.php/catalog/export/csv?ps=5000&collection[]",
        "expected_mime": CSV_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

GEOSERVER_URLMAP = [
    {
        "id": "wms111",
        "url": "/ows?service=WMS&version=1.1.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "wms130",
        "url": "/ows?service=WMS&version=1.3.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.3.0",
    },
    {
        "id": "wfs100",
        "url": "/ows?service=WFS&version=1.0.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
    {
        "id": "wfs110",
        "url": "/ows?service=WFS&version=1.1.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.0",
    },
    {
        "id": "wfs200",
        "url": "/ows?service=WFS&version=2.0.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.0",
    },
    {
        "id": "wcs100",
        "url": "/ows?service=WCS&version=1.0.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
    {
        "id": "wcs110",
        "url": "/ows?service=WCS&version=1.1.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.0",
    },
    {
        "id": "wcs111",
        "url": "/ows?service=WCS&version=1.1.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "wcs11",
        "url": "/ows?service=WCS&version=1.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1",
    },
    {
        "id": "wcs201",
        "url": "/ows?service=WCS&version=2.0.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.1",
    },
    {
        "id": "wps100",
        "url": "/ows?service=WPS&version=1.0.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
    {
        "id": "tms100",
        "url": "/gwc/service/tms/1.0.0",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
    {
        "id": "wms-c111",
        "url": "/gwc/service/wms?request=GetCapabilities&version=1.1.1&tiled=true",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "wmts100",
        "url": "/gwc/service/wmts?REQUEST=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
    {
        "id": "csw202",
        "url": "/csw?service=csw&version=2.0.2&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.2",
    },
    {
        "id": "ogc:tiles",
        "url": "/ogc/tiles/collections",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "ogc:images",
        "url": "/ogc/images/collections",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "ogc:maps",
        "url": "/ogc/maps/collections",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "ogc:features",
        "url": "/ogc/features/collections",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "geoserver:version",
        "url": "/rest/about/version",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "geoserver:server-status",
        "url": "/rest/about/server-status",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "geoserver:settings",
        "url": "/rest/settings",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "geoserver:layers",
        "url": "/rest/layers",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    # Non-standard GeoServer paths (e.g., /geo/wms instead of /ows)
    {
        "id": "wms111",
        "url": "/geo/wms?service=WMS&version=1.1.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "wms130",
        "url": "/geo/wms?service=WMS&version=1.3.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.3.0",
    },
    {
        "id": "wfs100",
        "url": "/geo/wfs?service=WFS&version=1.0.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
    {
        "id": "wfs110",
        "url": "/geo/wfs?service=WFS&version=1.1.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.0",
    },
    {
        "id": "wfs200",
        "url": "/geo/wfs?service=WFS&version=2.0.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.0",
    },
    {
        "id": "wcs100",
        "url": "/geo/wms?service=WCS&version=1.0.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
    {
        "id": "wcs110",
        "url": "/geo/wms?service=WCS&version=1.1.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.0",
    },
    {
        "id": "wcs111",
        "url": "/geo/wms?service=WCS&version=1.1.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "wcs11",
        "url": "/geo/wms?service=WCS&version=1.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1",
    },
    {
        "id": "wcs201",
        "url": "/geo/wms?service=WCS&version=2.0.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.1",
    },
]

MAPPROXY_URLMAP = [
    {
        "id": "wms111",
        "url": "/service?REQUEST=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "wmts100",
        "url": "/service?REQUEST=GetCapabilities&SERVICE=WMTS",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
    {
        "id": "tms100",
        "url": "/tms/1.0.0/",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
    {
        "id": "wmts100",
        "url": "/wmts/1.0.0/WMTSCapabilities.xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0.0",
    },
]

NCWMS_URLMAP = [
    {
        "id": "wms111",
        "url": "/wms?SERVICE=WMS&REQUEST=GetCapabilities&VERSION=1.1.1",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "wms130",
        "url": "/wms?SERVICE=WMS&REQUEST=GetCapabilities&VERSION=1.3.0",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.3.0",
    },
]


EPRINTS_URLMAP = [
    {
        "id": "oaipmh20",
        "url": "/cgi/oai2?verb=Identify",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
    {
        "id": "rss",
        "url": "/cgi/latest_tool?output=RSS2",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
    {
        "id": "atom",
        "url": "/cgi/latest_tool?output=Atom",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "eprints:rest",
        "url": "/rest/eprint",
        "expected_mime": HTML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "eprints:rdf",
        "url": "/cgi/export/repository/RDFXML/devel.rdf",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "eprints:n3",
        "url": "/cgi/export/repository/RDFN3/devel.n3",
        "expected_mime": N3_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

KOORDINATES_URLMAP = [
    {
        "id": "koordinates:data-catalog",
        "url": "/services/api/v1.x/data/",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.0",
    },
    {
        "id": "csw202",
        "url": "/services/csw/?service=CSW&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.2",
    },
]

BLACKLIGHT_URLMAP = [
    {
        "id": "blacklight:catalog",
        "url": "/catalog.json",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.0",
    }
]


ALEPH_URLMAP = [
    {
        "id": "aleph:collections",
        "url": "/api/2/collections",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "2.0",
    },
    {
        "id": "aleph:query",
        "url": "/api/1/query",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.0",
    },
]

QWC2_URLMAP = [
    {
        "id": "qwc2:layers",
        "url": "/themes.json",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    }
]

PYGEOAPI_URLMAP = [
    {
        "id": "pygeoapi:openapi",
        "url": "/openapi",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.0",
    },
    {
        "id": "pygeoapi:collections",
        "url": "/collections/?f=json",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.0",
    },
]

PYCSW30_URLMAP = [
    {
        "id": "ogcrecords",
        "url": "/collections?f=json",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.0",
    },
    {
        "id": "oaipmh20",
        "url": "/oaipmh",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
    {
        "id": "csw202",
        "url": "/csw?service=CSW&version=2.0.2&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.2",
    },
    {
        "id": "csw300",
        "url": "/csw",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "3.0.0",
    },
    {
        "id": "opensearch",
        "url": "/opensearch",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0",
    },
    {
        "id": "sru",
        "url": "/sru",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.0",
    },
    {
        "id": "openapi",
        "url": "/openapi?f=json",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "stac:collection",
        "url": "/search?f=json",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]


WIS20BOX_URLMAP = [
    {
        "id": "pygeoapi:openapi",
        "url": "/oapi/openapi",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.0",
    },
    {
        "id": "pygeoapi:collections",
        "url": "/oapi/collections/?f=json",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": "1.0",
    },
]


OPENDATASOFT_URLMAP = [
    {
        "id": "opendatasoft",
        "display_url": "/api",
        "url": "/api/v2/catalog/datasets/",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    }
]

MAGDA_URLMAP = [
    {
        "id": "magda:datasets",
        "url": "/search/api/v0/search/datasets",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "magda:organizations",
        "url": "/search/api/v0/search/organisations",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "magda:datasets",
        "url": "/api/v0/search/datasets",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "magda:organizations",
        "url": "/api/v0/search/organisations",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

ARCGISHUB_URLMAP = [
    {
        "id": "dcatap201",
        "url": "/api/feed/dcat-ap/2.0.1.json",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "dcatus11",
        "url": "/api/feed/dcat-us/1.1.json",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "rss",
        "url": "/api/feed/rss/2.0",
        "accept": "application/json",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "ogcrecordsapi",
        "url": "/api/search/v1",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

ARCGISSERVER_URLMAP = [
    {
        "id": "arcgis:portals:self",
        "url": "/portal/sharing/rest/portals/self?f=pjson",
        "accept": "application/json",
        "expected_mime": PLAIN_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "arcgis:rest:info",
        "url": "/rest/info?f=pjson",
        "accept": "application/json",
        "expected_mime": PLAIN_MIMETYPES + JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "arcgis:rest:services",
        "url": "/rest/services?f=pjson",
        "accept": "application/json",
        "expected_mime": PLAIN_MIMETYPES + JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "arcgis:soap",
        "url": "/services?wsdl",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "arcgis:sitemap",
        "url": "/rest/services?f=sitemap",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "arcgis:geositemap",
        "url": "/rest/services?f=geositemap",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "arcgis:kmz",
        "url": "/rest/services?f=kmz",
        "expected_mime": KMZ_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

OSKARI_URLMAP = [
    {
        "id": "oskari:getmaplayers",
        "url": "/action?action_route=GetMapLayers&lang=en&epsg=EPSG%3A3067",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "oskari:gethiermaplayers",
        "url": "/action?action_route=GetHierarchicalMapLayerGroups",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

METAGIS_URLMAP = [
    {
        "id": "metagis:layers",
        "url": "/ResultJSONGNServlet",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
        "prefetch": True,
    },
]

THREDDS_URLMAP = [
    {
        "id": "thredds:catalog",
        "url": "/catalog.xml",
        "accept": "application/xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
        "prefetch": False,
    },
    {
        "id": "thredds:info",
        "url": "/serverInfo.xml",
        "accept": "application/xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
        "prefetch": False,
    },
    {
        "id": "thredds:info",
        "url": "/info/serverInfo.xml",
        "accept": "application/xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
        "prefetch": False,
    },
]

ERDDAP_URLMAP = [
    {
        "id": "erddap:index",
        "url": "/index.json",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
        "prefetch": False,
    },
    {
        "id": "erddap:datasets",
        "url": "/info/index.json",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
        "prefetch": False,
    },
    {
        "id": "opensearch",
        "url": "/opensearch1.1/description.xml",
        "expected_mime": "application/opensearchdescription+xml",
        "is_json": False,
        "version": None,
        "prefetch": False,
    },
    {
        "id": "sitemap",
        "url": "/sitemap.xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
        "prefetch": False,
    },
]

MYCORE_URLMAP = [
    {
        "id": "mycore:objects",
        "url": "/api/v2/objects",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "oaipmh20",
        "url": "/servlets/OAIDataProvider?verb=Identify",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
    {
        "id": "sitemap",
        "url": "/sitemap_google.xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]


IFREMER_URLMAP = [
    {
        "id": "ifremer:search",
        "url": "/api/full-search-response",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "oaipmh20",
        "url": "/oai/OAIHandler?verb=Identify",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0",
    },
]


ESRIGEO_URLMAP = [
    {
        "id": "esrigeo:geoportal",
        "url": "/rest/geoportal",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "esrigeo:metadata:search",
        "url": "/rest/metadata/search",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "csw202",
        "url": "/csw",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "3.0.0",
    },
    {
        "id": "atom",
        "url": "/opensearch?f=atom&from=1&size=10&sort=title.sort%3Aasc&esdsl=%7B%7D",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "esrigeo:json",
        "url": "/opensearch?f=json&from=1&size=10&sort=title.sort%3Aasc&esdsl=%7B%7D",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "rss",
        "url": "/opensearch?f=rss&from=1&size=10&sort=title.sort%3Aasc&esdsl=%7B%7D",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "dcatus11",
        "url": "/opensearch?f=dcat&from=1&size=10&sort=title.sort%3Aasc&esdsl=%7B%7D",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "esrigeo:csv",
        "url": "/opensearch?f=csv&from=1&size=10&sort=title.sort%3Aasc&esdsl=%7B%7D",
        "expected_mime": CSV_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "esrigeo:kml",
        "url": "/opensearch?f=kml&from=1&size=10&sort=title.sort%3Aasc&esdsl=%7B%7D",
        "expected_mime": KML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
]

JKAN_URLMAP = [
    {
        "id": "dcatus11",
        "url": "/data.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    }
]

WEKO3_URLMAP = [
    {
        "id": "weko3:records",
        "url": "/api/records/?page=1&size=20&sort=-createdate&search_type=0&q=&title=&creator=&filedate_from=&filedate_to=&fd_attr=&id=&id_attr=&srctitle=&type=17&dissno=&lang=english",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    }
]

SDMXRI_URLMAP = [
    {
        "id": "sdmx:dataflows",
        "url": "/rest/dataflow",
        "accept": "application/json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "sdmx:datastructure",
        "url": "/rest/datastructure",
        "expected_mime": "application/vnd.sdmx.structure+xml",
        "is_json": False,
        "version": "1.0",
    },
    {
        "id": "sdmx:codelist",
        "url": "/rest/codelist",
        "expected_mime": "application/vnd.sdmx.structure+xml",
        "is_json": False,
        "version": "1.0",
    },
    {
        "id": "sdmx:conceptscheme",
        "url": "/rest/conceptscheme",
        "expected_mime": "application/vnd.sdmx.structure+xml",
        "is_json": False,
        "version": "1.0",
    },
]


OPENDAP_URLMAP = []

OPENDATAREG_URLMAP = [
    {
        "id": "opendatareg:catalog",
        "url": "/catalog.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "opendatareg:collections",
        "url": "/collections",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "opendatareg:stac",
        "url": "/stac",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
]

CUSTOM_URLMAP = [
    {
        "id": "dcatus11",
        "url": "/data.json",
        "expected_mime": JSON_MIMETYPES,
        "is_json": True,
        "version": None,
    },
    {
        "id": "sitemap",
        "url": "/sitemap.xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
        "prefetch": False,
    },
    {
        "id": "sitemap",
        "url": "/sitemap.xml.gz",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
        "prefetch": False,
    },
    {
        "id": "sitemap",
        "url": "/sitemap_google.xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "sitemap",
        "url": "/sitemap/index",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    {
        "id": "sitemap",
        "url": "/sitemap_index.xml",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": None,
    },
    # Generic OGC service detection for custom APIs (non-standard geoportals)
    {
        "id": "wms111",
        "url": "/wms?service=WMS&version=1.1.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "wms130",
        "url": "/wms?service=WMS&version=1.3.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.3.0",
    },
    {
        "id": "wfs200",
        "url": "/wfs?service=WFS&version=2.0.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.0",
    },
    {
        "id": "wcs111",
        "url": "/wcs?service=WCS&version=1.1.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "wms111",
        "url": "/ows?service=WMS&version=1.1.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "wms130",
        "url": "/ows?service=WMS&version=1.3.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.3.0",
    },
    {
        "id": "wfs200",
        "url": "/ows?service=WFS&version=2.0.0&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.0",
    },
    {
        "id": "wcs111",
        "url": "/ows?service=WCS&version=1.1.1&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "1.1.1",
    },
    {
        "id": "csw202",
        "url": "/csw?service=CSW&version=2.0.2&request=GetCapabilities",
        "expected_mime": XML_MIMETYPES,
        "is_json": False,
        "version": "2.0.2",
    },
]


def analyze_robots(root_url):
    p = urlparse(root_url)
    robots_url = p.scheme + "://" + p.netloc + "/robots.txt"
    logger = logging.getLogger(__name__)
    logger.info("Analyzing robots.txt %s", robots_url)
    try:
        r = requests.get(
            robots_url,
            timeout=DEFAULT_TIMEOUT,
            verify=False,
        )
    except Exception as e:
        logger.error("Error analyzing robots.txt: %s", e)
        return []
    if r.status_code != 200:
        logger.info("robots.txt unavailable, status=%d", r.status_code)
        return []
    parser = urllib.robotparser.RobotFileParser()
    parser.parse(r.text)
    sitemaps = parser.site_maps()
    output = []
    logger = logging.getLogger(__name__)
    if sitemaps is not None and len(sitemaps) > 0:
        logger.info("Found sitemaps: %s", ", ".join(sitemaps))
        for s in sitemaps:
            output.append({"type": "sitemap", "url": s})
    return output


FILTER_RELS = [
    "stylesheet",
    "icon",
    "shortcut icon",
    "mask-icon",
    "apple-touch-icon",
    "manifest",
    "apple-touch-icon-precomposed",
    "preconnect",
    "shortlink",
    "canonical",
    "dns-prefetch",
    "prefetch",
    "preload",
    "terms-of-service",
]
FILTER_TYPES = ["text/css", "image/x-icon", "image/png"]


def analyze_root(root_url):
    logger = logging.getLogger(__name__)
    logger.info("Analyzing root page %s", root_url)
    output = []
    s = requests.Session()
    try:
        response = s.get(
            root_url,
            verify=False,
            headers={"User-Agent": USER_AGENT},
            timeout=(DEFAULT_TIMEOUT, DEFAULT_TIMEOUT),
        )
    except requests.exceptions.Timeout:
        logging.info("Timeout error processing root page")
        #        results.append({'url' : request_url,'error' : 'Timeout'})
        return output
    except requests.exceptions.SSLError:
        logging.info("SSL error processing root page")
        #        results.append({'url' : request_url,'error' : 'SSL Error'})
        return output
    except ConnectionError:
        logging.info("Connection error processing root page")
        #        results.append({'url' : request_url,'error' : 'no connection'})
        return output
    except TooManyRedirects:
        logging.info("Redirects error processing root page")
        #        results.append({'url' : request_url,'error' : 'no connection'})
        return output
    if response.status_code != 200:
        #        results.append({'url' : request_url, 'status' : response.status_code, 'mime' : response.headers['Content-Type'].split(';', 1)[0].lower() if 'content-type' in response.headers.keys() else '', 'error' : 'Wrong status'})
        logging.info(
            f"Status code is {response.status_code}. Error processing root page"
        )
        return output
    #    print(response.text)
    try:
        hp = lxml.etree.HTMLParser()  # encoding='utf8')
        document = lxml.html.fromstring(response.content, parser=hp)
    except ValueError:
        logging.info("Error processing root page")
        return output
    except lxml.etree.ParserError:
        logging.info("Error parsing root page")
        return output
    links = document.xpath("//head/link")
    logger = logging.getLogger(__name__)
    logger.debug("Found header links %d", len(links))
    for link in links:
        lr = dict(link.attrib)
        if "rel" in lr.keys() and lr["rel"].lower() in FILTER_RELS:
            continue
        if "type" in lr.keys() and lr["type"].lower() in FILTER_TYPES:
            continue
        logger.debug("Found link: %s", lr)
        if "rel" in lr.keys() and "href" in lr.keys():
            if lr["rel"] == "resourcesync":
                output.append(
                    {
                        "type": "resourcesync",
                        "url": (
                            lr["href"]
                            if lr["href"][0:4] == "http"
                            else urljoin(root_url, lr["href"])
                        ),
                    }
                )
            elif lr["rel"] == "Sword":
                output.append(
                    {
                        "type": "sword",
                        "url": (
                            lr["href"]
                            if lr["href"][0:4] == "http"
                            else urljoin(root_url, lr["href"])
                        ),
                    }
                )
            elif lr["rel"] == "SwordDeposit":
                output.append(
                    {
                        "type": "sword:deposit",
                        "url": (
                            lr["href"]
                            if lr["href"][0:4] == "http"
                            else urljoin(root_url, lr["href"])
                        ),
                    }
                )
            elif lr["rel"] == "SwordDeposit":
                output.append(
                    {
                        "type": "sword:deposit",
                        "url": (
                            lr["href"]
                            if lr["href"][0:4] == "http"
                            else urljoin(root_url, lr["href"])
                        ),
                    }
                )
            elif lr["rel"] == "unapi-server":
                output.append(
                    {
                        "type": "unapi",
                        "url": (
                            lr["href"]
                            if lr["href"][0:4] == "http"
                            else urljoin(root_url, lr["href"])
                        ),
                    }
                )
        if "type" in lr.keys() and "href" in lr.keys():
            if lr["type"] == "application/opensearchdescription+xml":
                output.append(
                    {
                        "type": "opensearch",
                        "url": (
                            lr["href"]
                            if lr["href"][0:4] == "http"
                            else urljoin(root_url, lr["href"])
                        ),
                    }
                )
    scripts = document.xpath("//script[@type='application/ld+json']")
    for s in scripts:
        logger = logging.getLogger(__name__)
        logger.debug("ld+json script found")
        try:
            data = json.loads(s.text)
            #            logger.debug(data)
            if isinstance(data, list):
                if len(data) > 0:
                    data = data[0]
                else:
                    continue
            logger.debug("JSON-LD keys: %s", list(data.keys()))
            if "@graph" in data.keys() and data["@graph"] is not None:
                logger.debug("graph found")
                root_item = data["@graph"]
            else:
                root_item = data
            if root_item is not None:
                slist = []
                if isinstance(root_item, dict):
                    slist.append(root_item)
                elif isinstance(root_item, list):
                    slist = root_item
                logger.debug("slist created with %d items", len(slist))
                found = False
                for stype in slist:
                    if "@type" in stype.keys():
                        if (
                            isinstance(stype["@type"], list)
                            and "DataCatalog" in stype["@type"]
                        ):
                            output.append(
                                {"type": "schemaorg:datacatalog", "url": root_url}
                            )
                            found = True
                            break
                        elif (
                            isinstance(stype["@type"], str)
                            and stype["@type"] == "DataCatalog"
                        ):
                            output.append(
                                {"type": "schemaorg:datacatalog", "url": root_url}
                            )
                            found = True
                            break
                    if "mainEntity" in stype.keys() and stype["mainEntity"] is not None:
                        mainlist = []
                        if isinstance(stype["mainEntity"], dict):
                            mainlist.append(stype["mainEntity"])
                        elif isinstance(stype["mainEntity"], list):
                            mainlist = stype["mainEntity"]
                        for entity in mainlist:
                            if "@type" not in entity.keys():
                                continue
                            if (
                                isinstance(entity["@type"], list)
                                and "DataCatalog" in entity["@type"]
                            ):
                                output.append(
                                    {"type": "schemaorg:datacatalog", "url": root_url}
                                )
                                found = True
                                break
                            elif (
                                isinstance(entity["@type"], str)
                                and entity["@type"] == "DataCatalog"
                            ):
                                found = True
                                output.append(
                                    {"type": "schemaorg:datacatalog", "url": root_url}
                                )
                                break

        except ValueError:
            continue

    return output


DEEP_SEARCH_FUNCTIONS = [analyze_robots, analyze_root]


CATALOGS_URLMAP = {
    "geonode": GEONODE_URLMAP,
    "dkan": DKAN_URLMAP,
    "ckan": CKAN_URLMAP,
    "geonetwork": GEONETWORK_URLMAP,
    "openwis": GEONETWORK_URLMAP,
    "pxweb": PXWEB_URLMAP,
    "socrata": SOCRATA_URLMAP,
    "dataverse": DATAVERSE_URLMAP,
    "dspace": DSPACE_URLMAP,
    "elsevierpure": ELSEVIERPURE_URLMAP,
    "nada": NADA_URLMAP,
    "geoserver": GEOSERVER_URLMAP,
    "eprints": EPRINTS_URLMAP,
    "koordinates": KOORDINATES_URLMAP,
    "aleph": ALEPH_URLMAP,
    "mycore": MYCORE_URLMAP,
    "magda": MAGDA_URLMAP,
    "opendatasoft": OPENDATASOFT_URLMAP,
    "arcgishub": ARCGISHUB_URLMAP,
    "arcgisserver": ARCGISSERVER_URLMAP,
    "oskari": OSKARI_URLMAP,
    "metagis": METAGIS_URLMAP,
    "esrigeo": ESRIGEO_URLMAP,
    "geoblacklight": BLACKLIGHT_URLMAP,
    "pygeoapi": PYGEOAPI_URLMAP,
    "thredds": THREDDS_URLMAP,
    "erddap": ERDDAP_URLMAP,
    "mapproxy": MAPPROXY_URLMAP,
    "statsuite": STATSUITE_URLMAP,
    "worktribe": WORKTRIBE_URLMAP,
    "inveniordm": INVENIORDM_URLMAP,
    "invenio": INVENIO_URLMAP,
    "esploro": ESPLORO_URLMAP,
    "hyrax": HYRAX_URLMAP,
    "ifremercatalog": IFREMER_URLMAP,
    "jkan": JKAN_URLMAP,
    "qwc2": QWC2_URLMAP,
    "weko3": WEKO3_URLMAP,
    "wis20box": WIS20BOX_URLMAP,
    "ncwms": NCWMS_URLMAP,
    "figshare": FIGSHARE_URLMAP,
    "elsevierdigitalcommons": ELSEVIERDC_URLMAP,
    "junar": JUNAR_URLMAP,
    "custom": CUSTOM_URLMAP,
    "pycsw": PYCSW30_URLMAP,
    "opendap": OPENDAP_URLMAP,
    "triplydb": TRIPLYDB_URLMAP,
    "ipt": IPT_URLMAP,
    "sdmxri": SDMXRI_URLMAP,
    "opendatareg": OPENDATAREG_URLMAP,
}


def geoserver_url_cleanup_func(url):
    url = url.rstrip("/")
    if len(url) >= 4 and url.endswith("/web"):
        url = url[:-4]
    return url


def arcgisserver_url_cleanup_func(url):
    domain = urlparse(url).netloc
    if domain.find("443") > -1 or url[0:5] == "https":
        url = url.replace("http://", "https://")
    if url.find("/rest/services") > -1:
        url = url.rsplit("/rest/services", 1)[0]
    elif url.find("/services") > -1:
        url = url.rsplit("/services", 1)[0]
    return url


def geonetwork_url_cleanup_func(url):
    return url.split("/srv")[0]


def thredds_url_cleanup_func(url):
    return url.split("/catalog.html")[0]


def erddap_url_cleanup_func(url):
    return url.split("/index.html")[0]


URL_CLEANUP_MAP = {
    "geoserver": geoserver_url_cleanup_func,
    "arcgisserver": arcgisserver_url_cleanup_func,
    "geonetwork": geonetwork_url_cleanup_func,
    "thredds": thredds_url_cleanup_func,
    "erddap": erddap_url_cleanup_func,
}


def api_identifier(
    website_url, software_id, verify_json=False, deep=False, timeout=DEFAULT_TIMEOUT
):
    logger = logging.getLogger(__name__)
    url_map = CATALOGS_URLMAP[software_id]
    results = []
    found = []
    s = requests.Session()
    #
    if software_id in URL_CLEANUP_MAP:
        website_url = URL_CLEANUP_MAP[software_id](website_url)
    else:
        website_url = website_url.rstrip("/")
    
    # For GeoServer, try multiple base URL variations to handle non-standard paths
    base_urls = [website_url]
    if software_id == "geoserver":
        parsed = urlparse(website_url)
        # If URL doesn't end with /geoserver, try adding it
        if not website_url.endswith("/geoserver") and "/geoserver" not in parsed.path:
            base_urls.append(website_url + "/geoserver")
        # If URL ends with /geoserver, also try without it
        if website_url.endswith("/geoserver"):
            base_urls.append(website_url.removesuffix("/geoserver"))
    
    umap = url_map.copy()
    if software_id != "custom" and deep:
        umap.extend(CUSTOM_URLMAP)
    
    # Track URLs we've already tried to avoid duplicates
    tried_urls = set()
    
    for base_url in base_urls:
        for item in umap:
            try:
                request_url = base_url + item["url"]
                # Skip if we've already tried this URL
                if request_url in tried_urls:
                    continue
                tried_urls.add(request_url)
                logger.info("Requesting %s", request_url)
                if "post_params" in item.keys():
                    if "accept" in item.keys():
                        response = s.post(
                            request_url,
                            verify=False,
                            headers={"User-Agent": USER_AGENT, "Accept": item["accept"]},
                            json=json.loads(item["post_params"]),
                            timeout=(timeout, timeout),
                        )
                    else:
                        response = s.post(
                            request_url,
                            verify=False,
                            headers={"User-Agent": USER_AGENT},
                            json=json.loads(item["post_params"]),
                            timeout=(timeout, timeout),
                        )
                else:
                    response = None
                    if "prefetch" in item and item["prefetch"]:
                        # Reuse prefetched response instead of issuing a duplicate request.
                        response = s.get(
                            request_url,
                            headers={"User-Agent": USER_AGENT},
                            timeout=(timeout, timeout),
                        )
                    # request_url already set above with base_url
                    if response is None and "accept" in item.keys():
                        response = s.get(
                            request_url,
                            verify=False,
                            headers={"User-Agent": USER_AGENT, "Accept": item["accept"]},
                            timeout=(timeout, timeout),
                        )
                    elif response is None:
                        response = s.get(
                            request_url,
                            verify=False,
                            headers={"User-Agent": USER_AGENT},
                            timeout=(timeout, timeout),
                        )
                if response.status_code != 200:
                    results.append(
                        {
                            "url": request_url,
                            "status": response.status_code,
                            "mime": (
                                response.headers["Content-Type"].split(";", 1)[0].lower()
                                if "content-type" in response.headers.keys()
                                else ""
                            ),
                            "error": "Wrong status",
                        }
                    )
                    continue
            except requests.exceptions.Timeout:
                results.append({"url": request_url, "error": "Timeout"})
                continue
            except requests.exceptions.SSLError:
                results.append({"url": request_url, "error": "SSL Error"})
                continue
            except ConnectionError:
                results.append({"url": request_url, "error": "no connection"})
                continue
            except TooManyRedirects:
                results.append({"url": request_url, "error": "no connection"})
                continue
            except ContentDecodingError:
                results.append({"url": request_url, "error": "content error"})
                continue
            logger.info("Finished request to %s", request_url)
            if (
                "expected_mime" in item.keys()
                and item["expected_mime"] is not None
                and "Content-Type" in response.headers.keys()
            ):
                if verify_json:
                    if "is_json" in item.keys() and item["is_json"]:
                        try:
                            data = json.loads(response.content)
                        except (json.JSONDecodeError, ValueError, TypeError):
                            results.append(
                                {
                                    "url": request_url,
                                    "status": response.status_code,
                                    "mime": response.headers["Content-Type"]
                                    .split(";", 1)[0]
                                    .lower(),
                                    "error": "Error loading JSON",
                                }
                            )
                            continue
                expected_mime = item["expected_mime"]
                if isinstance(expected_mime, str):
                    expected_mime = [expected_mime]
                if (
                    response.headers["Content-Type"].split(";", 1)[0].lower()
                    not in expected_mime
                ):
                    results.append(
                        {
                            "url": request_url,
                            "status": response.status_code,
                            "mime": response.headers["Content-Type"]
                            .split(";", 1)[0]
                            .lower(),
                            "error": "Wrong content type",
                        }
                    )
                    continue
            api = {
                "type": item["id"],
                "url": (
                    base_url + item["display_url"]
                    if "display_url" in item.keys()
                    else request_url
                ),
            }
            if item["version"]:
                api["version"] = item["version"]
            if "urlpat" in item.keys():
                api["url_pattern"] = item["urlpat"]
            found.append(api)
    if deep:
        logger.info("Going deep")
        for func in DEEP_SEARCH_FUNCTIONS:
            extracted = func(website_url)
            if len(extracted) > 0:
                found.extend(extracted)
    logger.info("Failures: %s", results)
    return found


def __detect_one(
    filename,
    record,
    software,
    action,
    deep,
    filepath,
    timeout=DEFAULT_TIMEOUT,
    dryrun=False,
):
    logger = logging.getLogger(__name__)
    logger.info("Processing %s", os.path.basename(filename).split(".", 1)[0])
    if (
        "endpoints" in record.keys()
        and len(record["endpoints"]) > 0
        and action == "insert"
    ):
        logger.info(
            " - skip, we have endpoints already and not in replace or update mode"
        )
        return
    found = api_identifier(
        record["link"].rstrip("/"), software, deep=deep, timeout=timeout
    )
    keys = []
    if action == "update":
        if "endpoints" in record.keys() and len(record["endpoints"]) > 0:
            for e in record["endpoints"]:
                if "url" in e.keys():
                    keys.append(e["url"])
        else:
            record["endpoints"] = []
    else:
        record["endpoints"] = []
    added = 0
    for api in found:
        if api["url"] not in keys:
            logger.info("- %s %s", api["type"], api["url"])
            record["endpoints"].append(api)
            keys.append(api["url"])
            added += 1
    logger.info("Found %d, added %d", len(found), added)
    if added > 0:
        record["api"] = True
        record["api_status"] = "active"
        if dryrun:
            logger.info("- dryrun enabled, profile not updated")
        else:
            _save_record(filepath, record)
            logger.info("- updated profile")
    else:
        logger.info("- no endpoints or no new endpoints, not updated")


def _resolve_root_dir(mode):
    return ENTRIES_DIR if mode == "entries" else SCHEDULED_DIR


def _iter_yaml_files(root_dir):
    for root, _, files in os.walk(root_dir):
        for fi in files:
            if fi.endswith(".yaml"):
                yield os.path.join(root, fi)


def _load_record(filepath):
    with open(filepath, "r", encoding="utf8") as f:
        return yaml.load(f, Loader=Loader)


def _save_record(filepath, record):
    with open(filepath, "w", encoding="utf8") as f:
        f.write(yaml.safe_dump(record, allow_unicode=True))


def _detect_record(
    filename,
    filepath,
    record,
    action,
    deep,
    timeout=DEFAULT_TIMEOUT,
    dryrun=False,
):
    software = record["software"]["id"] if record["software"]["id"] in CATALOGS_URLMAP else "custom"
    __detect_one(
        filename,
        record,
        software,
        action,
        deep,
        filepath,
        timeout=timeout,
        dryrun=dryrun,
    )


def _replace_detected_endpoints(
    filepath, record, software_id, base_url=None, dryrun=False
):
    logger = logging.getLogger(__name__)
    detection_base = base_url if base_url else record["link"].rstrip("/")
    found = api_identifier(detection_base, software_id)
    record["endpoints"] = []
    for api in found:
        logger.info("- %s %s", api["type"], api["url"])
        record["endpoints"].append(api)
    if len(record["endpoints"]) > 0:
        if dryrun:
            logger.info("- dryrun enabled, profile not updated")
        else:
            _save_record(filepath, record)
            logger.info("- updated profile")
    else:
        logger.info("- no endpoints, not updated")


@app.command()
def detect_software(
    software,
    dryrun: Annotated[bool, typer.Option("--dryrun")] = False,
    action: Annotated[str, typer.Option("--action")] = "insert",
    mode: str = "entries",
    deep: bool = False,
):
    """Enrich data catalogs with API endpoints by software"""
    root_dir = _resolve_root_dir(mode)
    for filepath in _iter_yaml_files(root_dir):
        record = _load_record(filepath)
        if record["software"]["id"] == software:
            _detect_record(
                filepath,
                filepath,
                record,
                action,
                deep,
                dryrun=dryrun,
            )


@app.command()
def detect_single(
    uniqid,
    dryrun: Annotated[bool, typer.Option("--dryrun")] = False,
    action: Annotated[str, typer.Option("--action")] = "insert",
    mode: str = "entries",
    deep: bool = False,
    timeout: int = DEFAULT_TIMEOUT,
):
    """Enrich single data catalog with API endpoints"""
    root_dir = _resolve_root_dir(mode)
    found = False
    for filepath in _iter_yaml_files(root_dir):
        record = _load_record(filepath)
        idkeys = []
        for k in ["uid", "id", "link"]:
            if k in record.keys():
                idkeys.append(record[k])
        if uniqid not in idkeys:
            continue
        found = True
        _detect_record(
            filepath,
            filepath,
            record,
            action,
            deep,
            timeout=timeout,
            dryrun=dryrun,
        )


@app.command()
def detect_country(
    country,
    dryrun: Annotated[bool, typer.Option("--dryrun")] = False,
    action: Annotated[str, typer.Option("--action")] = "insert",
    mode: str = "entries",
    deep: bool = False,
):
    """Enrich data catalogs with API endpoints by country"""
    root_dir = _resolve_root_dir(mode)
    for filepath in _iter_yaml_files(root_dir):
        record = _load_record(filepath)
        if record["owner"]["location"]["country"]["id"] != country:
            continue
        _detect_record(
            filepath,
            filepath,
            record,
            action,
            deep,
            dryrun=dryrun,
        )


@app.command()
def detect_cattype(
    catalogtype,
    dryrun: Annotated[bool, typer.Option("--dryrun")] = False,
    action: Annotated[str, typer.Option("--action")] = "insert",
    mode: str = "entries",
    deep: bool = False,
):
    """Enrich data catalogs with API endpoints by catalog type"""
    root_dir = _resolve_root_dir(mode)
    for filepath in _iter_yaml_files(root_dir):
        record = _load_record(filepath)
        if record["catalog_type"] != catalogtype:
            continue
        _detect_record(
            filepath,
            filepath,
            record,
            action,
            deep,
            dryrun=dryrun,
        )


@app.command()
def detect_ckan(dryrun=False, replace_endpoints=True, mode="entries"):
    """Enrich data catalogs with API endpoints by CKAN instance (special function to update all endpoints"""
    root_dir = _resolve_root_dir(mode)
    for filepath in _iter_yaml_files(root_dir):
        record = _load_record(filepath)
        if record["software"]["id"] == "ckan":
            logger = logging.getLogger(__name__)
            logger.info("Processing %s", os.path.basename(filepath).split(".", 1)[0])
            if "endpoints" in record.keys() and len(record["endpoints"]) > 1:
                logger.info(" - skip, we have more than 2 endpoints so we skip")
                continue
            if (
                "endpoints" in record.keys()
                and len(record["endpoints"]) == 1
                and record["endpoints"][0]["type"] == "ckanapi"
            ):
                base_url = record["endpoints"][0]["url"][0:-6]
            else:
                base_url = record["link"].rstrip("/")
            _replace_detected_endpoints(
                filepath,
                record,
                record["software"]["id"],
                base_url=base_url,
                dryrun=dryrun,
            )


@app.command()
def detect_all(
    status="undetected",
    replace_endpoints: Annotated[bool, typer.Option("--replace")] = False,
    mode="entries",
):
    """Detect all known API endpoints"""
    root_dir = _resolve_root_dir(mode)
    for filepath in _iter_yaml_files(root_dir):
        record = _load_record(filepath)
        if record["software"]["id"] in CATALOGS_URLMAP.keys():
            if "endpoints" not in record.keys() or len(record["endpoints"]) == 0:
                if status == "undetected":
                    logger = logging.getLogger(__name__)
                    logger.info(
                        "Processing catalog %s, software %s",
                        os.path.basename(filepath).split(".", 1)[0],
                        record["software"]["id"],
                    )
                    if (
                        "endpoints" in record.keys()
                        and len(record["endpoints"]) > 0
                        and replace_endpoints is False
                    ):
                        logger.info(
                            " - skip, we have endpoints already and no replace mode"
                        )
                        continue
                    _replace_detected_endpoints(
                        filepath,
                        record,
                        record["software"]["id"],
                    )


@app.command()
def report(status="undetected", filename=None, mode="entries"):
    """Report data catalogs with undetected API endpoints"""
    out = sys.stdout if filename is None else open(filename, "w", encoding="utf8")

    root_dir = _resolve_root_dir(mode)

    if status == "undetected":
        out.write(",".join(["id", "uid", "link", "software_id", "status"]) + "\n")
    for filepath in _iter_yaml_files(root_dir):
        record = _load_record(filepath)
        if record["software"]["id"] in CATALOGS_URLMAP.keys():
            if "endpoints" not in record.keys() or len(record["endpoints"]) == 0:
                if status == "undetected":
                    out.write(
                        ",".join(
                            [
                                record["id"],
                                record["uid"],
                                record["link"],
                                record["software"]["id"],
                                "undetected",
                            ]
                        )
                        + "\n"
                    )
    if filename is not None:
        out.close()


@app.command()
def update_broken_arcgis(
    status="undetected",
    replace_endpoints: Annotated[bool, typer.Option("--replace")] = True,
    mode="entries",
):
    """Detect all broken ArcGIS portals and update endpoints"""
    root_dir = _resolve_root_dir(mode)
    for filepath in _iter_yaml_files(root_dir):
        record = _load_record(filepath)
        if record["software"]["id"] in ["arcgishub", "arcgisserver"]:
            if "endpoints" not in record.keys() or len(record["endpoints"]) < 2:
                if status == "undetected":
                    logger = logging.getLogger(__name__)
                    logger.info(
                        "Processing catalog %s, software %s",
                        os.path.basename(filepath).split(".", 1)[0],
                        record["software"]["id"],
                    )
                    if (
                        "endpoints" in record.keys()
                        and len(record["endpoints"]) > 0
                        and replace_endpoints is False
                    ):
                        logger.info(
                            " - skip, we have endpoints already and no replace mode"
                        )
                        continue
                    _replace_detected_endpoints(
                        filepath,
                        record,
                        record["software"]["id"],
                    )


if __name__ == "__main__":
    app()
