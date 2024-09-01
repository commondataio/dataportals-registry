#!/usr/bin/env python
# This script intended to detect data catalogs API

import logging
import sys
import typer
from typing_extensions import Annotated
import requests
import datetime
import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
import csv
import json
import os
from  urllib.parse import urlparse
import shutil
import pprint
from urllib.parse import urlparse
from requests.exceptions import ConnectionError, TooManyRedirects
from urllib3.exceptions import InsecureRequestWarning#, ConnectionError
# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

logging.basicConfig(
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            level=logging.DEBUG,
)
root = logging.getLogger()
root.setLevel(logging.INFO)


ENTRIES_DIR = '../data/entities'
SCHEDULED_DIR = '../data/scheduled'
app = typer.Typer()

DEFAULT_TIMEOUT = 300

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0'

KML_MIMETYPES = ['application/vnd.google-earth.kml+xml']
HTML_MIMETYPES = ['text/html','text/html; charset=UTF-8']
XML_MIMETYPES = ['text/xml', 'application/xml', 'application/vnd.ogc.se_xml', 'application/vnd.ogc.wms_xml', 'application/rdf+xml', 'application/rss+xml', 'application/atom+xml', 'application/xml;charset=UTF-8'] + KML_MIMETYPES
JSON_MIMETYPES = ['text/json', 'application/json', 'application/hal+json', 'application/vnd.oai.openapi+json;version=3.0; charset=utf-8', 'application/vnd.oai.openapi+json']
N3_MIMETYPES = ['text/n3']
ZIP_MIMETYPES = ['application/zip']


CSV_MIMETYPES = ['text/csv']
PLAIN_MIMETYPES = ['text/plain']
KMZ_MIMETYPES = ['application/vnd.google-earth.kmz .kmz']


GEONETWORK_SEARCH_POST_PARAMS = """{"from":0,"size":20, "bucket" : "metadata", "sort":["_score"],"query":{"function_score":{"boost":"5","functions":[{"filter":{"match":{"resourceType":"series"}},"weight":1.5},{"filter":{"exists":{"field":"parentUuid"}},"weight":0.3},{"filter":{"match":{"cl_status.key":"obsolete"}},"weight":0.2},{"filter":{"match":{"cl_status.key":"superseded"}},"weight":0.3},{"gauss":{"dateStamp":{"scale":"365d","offset":"90d","decay":0.5}}}],"score_mode":"multiply","query":{"bool":{"must":[{"terms":{"isTemplate":["n"]}}]}}}},"aggregations":{"groupOwner":{"terms":{"field":"groupOwner"},"aggs":{"sourceCatalogue":{"terms":{"field":"sourceCatalogue"}}},"meta":{"field":"groupOwner"}},"resourceType":{"terms":{"field":"resourceType"},"meta":{"decorator":{"type":"icon","prefix":"fa fa-fw gn-icon-"},"field":"resourceType"}},"availableInServices":{"filters":{"filters":{"availableInViewService":{"query_string":{"query":"+linkProtocol:/OGC:WMS.*/"}},"availableInDownloadService":{"query_string":{"query":"+linkProtocol:/OGC:WFS.*/"}}}},"meta":{"decorator":{"type":"icon","prefix":"fa fa-fw ","map":{"availableInViewService":"fa-globe","availableInDownloadService":"fa-download"}}}},"cl_topic.key":{"terms":{"field":"cl_topic.key","size":5},"meta":{"decorator":{"type":"icon","prefix":"fa fa-fw gn-icon-"},"field":"cl_topic.key"}},"th_httpinspireeceuropaeutheme-theme_tree.key":{"terms":{"field":"th_httpinspireeceuropaeutheme-theme_tree.key","size":5},"meta":{"decorator":{"type":"icon","prefix":"fa fa-fw gn-icon iti-","expression":"http://inspire.ec.europa.eu/theme/(.*)"},"field":"th_httpinspireeceuropaeutheme-theme_tree.key"}},"tag":{"terms":{"field":"tag.default","include":".*","size":5},"meta":{"caseInsensitiveInclude":true,"field":"tag.default"}},"sourceCatalogue":{"terms":{"field":"sourceCatalogue","size":5,"include":".*"},"meta":{"orderByTranslation":true,"filterByTranslation":true,"displayFilter":true,"field":"sourceCatalogue"}},"OrgForResource":{"terms":{"field":"OrgForResourceObject.default","include":".*","size":5},"meta":{"caseInsensitiveInclude":true,"field":"OrgForResourceObject.default"}},"creationYearForResource":{"terms":{"field":"creationYearForResource","size":5,"order":{"_key":"desc"}},"meta":{"field":"creationYearForResource"}},"format":{"terms":{"field":"format","size":5,"order":{"_key":"asc"}},"meta":{"field":"format"}},"cl_spatialRepresentationType.key":{"terms":{"field":"cl_spatialRepresentationType.key","size":5},"meta":{"field":"cl_spatialRepresentationType.key"}},"cl_maintenanceAndUpdateFrequency.key":{"terms":{"field":"cl_maintenanceAndUpdateFrequency.key","size":5},"meta":{"field":"cl_maintenanceAndUpdateFrequency.key"}},"cl_status.key":{"terms":{"field":"cl_status.key","size":5},"meta":{"field":"cl_status.key"}},"resolutionScaleDenominator":{"terms":{"field":"resolutionScaleDenominator","size":5,"order":{"_key":"asc"}},"meta":{"field":"resolutionScaleDenominator"}},"resolutionDistance":{"terms":{"field":"resolutionDistance","size":5,"order":{"_key":"asc"}},"meta":{"field":"resolutionDistance"}},"dateStamp":{"auto_date_histogram":{"field":"dateStamp","buckets":50}}},"_source":{"includes":["uuid","id","groupOwner","logo","cat","inspireThemeUri","inspireTheme_syn","cl_topic","resourceType","resourceTitle*","resourceAbstract*","draft","owner","link","status*","rating","geom","contact*","Org*","isTemplate","valid","isHarvested","dateStamp","documentStandard","standardNameObject.default","cl_status*","mdStatus*"]},"script_fields":{"overview":{"script":{"source":"return params['_source'].overview == null ? [] : params['_source'].overview.stream().findFirst().orElse([]);"}}},"track_total_hits":true}"""

GEONODE_URLMAP = [
    {'id' : 'geonode:layers', 'url' : '/api/layers/', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'geonode:datasets', 'url' : '/api/datasets/', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'geonode:documents', 'url' : '/api/documents/', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'dcatus11', 'url' : '/data.json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'wms111', 'url' : '/geoserver/ows?service=WMS&version=1.1.1&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.1.1'},
    {'id' : 'wfs110', 'url' : '/geoserver/ows?service=WFS&version=1.1.0&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.1.0'},
    {'id' : 'wcs111', 'url' : '/geoserver/ows?service=WCS&version=1.1.1&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.1.1'},
    {'id' : 'csw202', 'url' : '/catalogue/csw?service=CSW&version=2.0.2&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0.2'},
    {'id' : 'oaipmh20', 'url' : '/catalogue/csw?mode=oaipmh&verb=Identify', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'},
    {'id' : 'wmts100', 'url' : '/geoserver/gwc/service/wmts?service=WMTS&version=1.0.0&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.0.0'},
    {'id' : 'opensearch', 'url' : '/catalogue/csw?mode=opensearch&service=CSW&version=2.0.2&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.0'},
]

DKAN_URLMAP = [
    {'id' : 'ckan:package-search', 'url' : '/api/3/action/package_search', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '3'},
    {'id' : 'ckan:package-list', 'url' : '/api/3/action/package_list', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '3'},
    {'id' : 'dcatus11', 'url' : '/data.json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'dkan:search', 'url' : '/api/1/search', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '1'},
    {'id' : 'dkan:metastore', 'url' : '/api/1/metastore', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '1'},
    {'id' : 'dkan:datastore', 'url' : '/api/1/metastore', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '1'},
]

CKAN_URLMAP = [
    {'id' : 'ckan', 'url' : '/api/3', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '3'},
    {'id' : 'ckan:package-search', 'url' : '/api/3/action/package_search', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '3'},
    {'id' : 'ckan:package-list', 'url' : '/api/3/action/package_list', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '3'},
    {'id' : 'dcatus11', 'url' : '/data.json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
]

GEONETWORK_URLMAP = [
    {'id' : 'geonetwork:api', 'url' : '/srv/api', 'expected_mime' : XML_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'geonetwork:query', 'display_url' : '/srv/eng/q', 'url' : '/srv/eng/q?_content_type=json&bucket=s101&facet.q=&fast=index&resultType=details&sortBy=relevance&sortOrder=&title_OR_altTitle_OR_any=', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'geonetwork:records', 'url' : '/srv/api/records', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': None},
    {'id' : 'dcatus11', 'url' : '/data.json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'openapi', 'url' : '/srv/v2/api-docs', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '2'},
    {'id' : 'csw202', 'url' : '/srv/eng/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0.2'},
    {'id' : 'opensearch', 'url' : '/srv/eng/portal.opensearch', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.0'},
    {'id' : 'oaipmh20', 'url' : '/srv/eng/oaipmh?verb=Identify', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'},    
#    {'id' : 'geonetwork:search', 'url' : '/srv/api/search/records/_search?bucket=metadata', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : False, 'version': None, 'post_params': GEONETWORK_SEARCH_POST_PARAMS},
    {'id' : 'geonetwork:settings', 'url' : '/srv/api/settings', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : False, 'version': None},    
    {'id' : 'geonetwork:selections', 'url' : '/srv/api/selections', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : False, 'version': None},
    {'id' : 'geonetwork:site', 'url' : '/srv/api/site', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : False, 'version': None},
]

SOCRATA_URLMAP = [
    {'id' : 'dcatus11', 'url' : '/data.json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'socrata:views', 'url' : '/api/views', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None}
]

PXWEB_URLMAP = [
    {'id' : 'pxwebapi', 'url' : '/api/v1/en/', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '1'}
]

STATSUITE_URLMAP = [
    {'id' : 'statsuite:search', 'url' : '/api/search', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None}
]


DATAVERSE_URLMAP = [
    {'id' : 'dataverseapi', 'display_url' : '/api/search','url' : '/api/search?q=*&type=dataset&sort=name&order=asc', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'oaipmh20', 'url' : '/oai?verb=Identify', 'accept' : 'application/xml', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'} 
]


INVENIORDM_URLMAP = [
    {'id' : 'inveniordmapi', 'url' : '/api', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'inveniordmapi:records', 'url' : '/api/records', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'oaipmh20', 'url' : '/oai2d', 'accept' : 'application/xml', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'}
]

INVENIO_URLMAP = [
    {'id' : 'inveniordmapi', 'url' : '/api', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'inveniordmapi:records', 'url' : '/api/records', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'oaipmh20', 'url' : '/oai2d', 'accept' : 'application/xml', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'}
]


WORKTRIBE_URLMAP = [
    {'id' : 'oaipmh20', 'url' : '/oaiprovider?verb=Identify', 'accept' : 'application/xml', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'} 
]


HYRAX_URLMAP = [
    {'id' : 'hyrax:catalog', 'url' : '/catalog', 'accept': 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None}
]

DSPACE_URLMAP = [                               
    {'id' : 'dspace', 'url' : '/server/api', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '7'},
    {'id' : 'dspace:objects', 'url' : '/server/api/discover/search/objects', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '7'},
    {'id' : 'dspace:items', 'url' : '/rest/items', 'accept': 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '6'},
    {'id' : 'oaipmh20', 'url' : '/oai/request?verb=Identify', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'},
    {'id' : 'opensearch', 'url' : '/open-search/description.xml', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.0'},
    {'id' : 'rss', 'url' : '/feed/rss_2.0/site', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'},
    {'id' : 'atom', 'url' : '/feed/atom_1.0/site', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.0'}      
]
ESPLORO_URLMAP = [
    {'id' : 'esploro:search', 'display_url' : '/esplorows/rest/research/simpleSearch', 'url' : '/esplorows/rest/research/simpleSearch?_wadl', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': None} 
]

ELSVIERPURE_URLMAP = [
    {'id' : 'oaipmh20', 'url' : '/ws/oai?verb=Identify', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'},
    {'id' : 'rss', 'url' : '/en/datasets/?search=&isCopyPasteSearch=false&format=rss', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'} 
]

NADA_URLMAP = [
    {'id' : 'nada:catalog-search', 'url' : '/index.php/api/catalog/search', 'accept': 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'nada:csvexport', 'url' : '/index.php/catalog/export/csv?ps=5000&collection[]', 'expected_mime' : CSV_MIMETYPES, 'is_json' : False, 'version': None}
]

GEOSERVER_URLMAP = [
    {'id' : 'wms111', 'url' : '/ows?service=WMS&version=1.1.1&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.1.1'},
    {'id' : 'wms130', 'url' : '/ows?service=WMS&version=1.3.0&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.3.0'},
    {'id' : 'wfs100', 'url' : '/ows?service=WFS&version=1.0.0&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.0.0'},
    {'id' : 'wfs110', 'url' : '/ows?service=WFS&version=1.1.0&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.1.0'},
    {'id' : 'wfs200', 'url' : '/ows?service=WFS&version=2.0.0&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0.0'},
    {'id' : 'wcs100', 'url' : '/ows?service=WCS&version=1.0.0&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.0.0'},
    {'id' : 'wcs110', 'url' : '/ows?service=WCS&version=1.1.0&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.1.0'},
    {'id' : 'wcs111', 'url' : '/ows?service=WCS&version=1.1.1&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.1.1'},
    {'id' : 'wcs11', 'url' : '/ows?service=WCS&version=1.1&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.1'},
    {'id' : 'wcs201', 'url' : '/ows?service=WCS&version=2.0.1&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0.1'},    
    {'id' : 'wps100', 'url' : '/ows?service=WPS&version=1.0.0&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.0.0'},
    {'id' : 'tms100', 'url' : '/gwc/service/tms/1.0.0', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.0.0'},
    {'id' : 'wms-c111', 'url' : '/gwc/service/wms?request=GetCapabilities&version=1.1.1&tiled=true', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.1.1'},
    {'id' : 'wmts100', 'url' : '/gwc/service/wmts?REQUEST=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.0.0'},
    {'id' : 'csw202', 'url' : '/csw?service=csw&version=2.0.2&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0.2'},    
    {'id' : 'ogc:tiles', 'url' : '/ogc/tiles/collections', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'ogc:images', 'url' : '/ogc/images/collections', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'ogc:maps', 'url' : '/ogc/maps/collections', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'ogc:features', 'url' : '/ogc/features/collections', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'geoserver:version', 'url' : '/rest/about/version','accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'geoserver:server-status', 'url' : '/rest/about/server-status','accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'geoserver:settings', 'url' : '/rest/settings','accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'geoserver:layers', 'url' : '/rest/layers','accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
]

MAPPROXY_URLMAP = [
    {'id' : 'wms111', 'url' : '/service?REQUEST=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.1.1'},
    {'id' : 'wmts100', 'url' : '/service?REQUEST=GetCapabilities&SERVICE=WMTS', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.0.0'},
    {'id' : 'tms100', 'url' : '/tms/1.0.0/', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.0.0'},
    {'id' : 'wmts100', 'url' : '/wmts/1.0.0/WMTSCapabilities.xml', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.0.0'},
]

NCWMS_URLMAP = [
    {'id' : 'wms111', 'url' : '/wms?SERVICE=WMS&REQUEST=GetCapabilities&VERSION=1.1.1', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.1.1'},
    {'id' : 'wms130', 'url' : '/wms?SERVICE=WMS&REQUEST=GetCapabilities&VERSION=1.3.0', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.3.0'},
]


EPRINTS_URLMAP = [
    {'id' : 'oaipmh20', 'url' : '/cgi/oai2?verb=Identify', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'},
    {'id' : 'rss', 'url' : '/cgi/latest_tool?output=RSS2', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'},
    {'id' : 'atom', 'url' : '/cgi/latest_tool?output=Atom', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': None},
    {'id' : 'eprints:rest', 'url' : '/rest/eprint', 'expected_mime' : HTML_MIMETYPES, 'is_json' : False, 'version': None},
    {'id' : 'eprints:rdf', 'url' : '/cgi/export/repository/RDFXML/devel.rdf', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': None},
    {'id' : 'eprints:n3', 'url' : '/cgi/export/repository/RDFN3/devel.n3', 'expected_mime' : N3_MIMETYPES, 'is_json' : False, 'version': None}
]

KOORDINATES_URLMAP = [
    {'id' : 'koordinates:data-catalog', 'url' : '/services/api/v1.x/data/', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '1.0'},
    {'id' : 'csw202', 'url' : '/services/csw/?service=CSW&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0.2'}    
]

BLACKLIGHT_URLMAP = [
    {'id' : 'blacklight:catalog', 'url' : '/catalog.json', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '1.0'}
]


ALEPH_URLMAP = [
    {'id' : 'aleph:collections', 'url' : '/api/2/collections', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '2.0'}
]

QWC2_URLMAP = [
    {'id' : 'qwc2:layers', 'url' : '/themes.json', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None}
]

PYGEOAPI_URLMAP = [
    {'id' : 'pygeoapi:openapi', 'url' : '/openapi', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '1.0'},
    {'id' : 'pygeoapi:collections', 'url' : '/collections/?f=json', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '1.0'}
]

WIS20BOX_URLMAP = [
    {'id' : 'pygeoapi:openapi', 'url' : '/oapi/openapi', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '1.0'},
    {'id' : 'pygeoapi:collections', 'url' : '/oapi/collections/?f=json', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '1.0'}
]


OPENDATASOFT_URLMAP = [
    {'id' : 'opendatasoft', 'display_url' : '/api', 'url' : '/api/v2/catalog/datasets/', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None}
]

MAGDA_URLMAP = [
    {'id' : 'magda:datasets', 'url' : '/search/api/v0/search/datasets', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'magda:organizations', 'url' : '/search/api/v0/search/organisations', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'magda:datasets', 'url' : '/api/v0/search/datasets', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'magda:organizations', 'url' : '/api/v0/search/organisations', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None}
]

ARCGISHUB_URLMAP = [
    {'id' : 'dcatap201',  'url' : '/api/feed/dcat-ap/2.0.1.json', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'dcatus11',  'url' : '/api/feed/dcat-us/1.1.json', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'rss',  'url' : '/api/feed/rss/2.0', 'accept' : 'application/json', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': None},
    {'id' : 'ogcrecordsapi',  'url' : '/api/search/v1', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
]

ARCGISSERVER_URLMAP = [
    {'id' : 'arcgis:portals:self',  'url' : '/portal/sharing/rest/portals/self?f=pjson', 'accept' : 'application/json', 'expected_mime' : PLAIN_MIMETYPES, 'is_json' : True, 'version': None},

    {'id' : 'arcgis:rest:info',  'url' : '/rest/info?f=pjson', 'accept' : 'application/json', 'expected_mime' : PLAIN_MIMETYPES + JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'arcgis:rest:services',  'url' : '/rest/services?f=pjson', 'accept' : 'application/json', 'expected_mime' : PLAIN_MIMETYPES + JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'arcgis:soap',  'url' : '/services?wsdl', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': None},
    {'id' : 'arcgis:sitemap',  'url' : '/rest/services?f=sitemap', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': None},
    {'id' : 'arcgis:geositemap',  'url' : '/rest/services?f=geositemap', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': None},
    {'id' : 'arcgis:kmz',  'url' : '/rest/services?f=kmz', 'expected_mime' : KMZ_MIMETYPES, 'is_json' : False, 'version': None},

]

OSKARI_URLMAP = [
    {'id' : 'oskari:getmaplayers',  'url' : '/action?action_route=GetMapLayers&lang=en&epsg=EPSG%3A3067', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'oskari:gethiermaplayers',  'url' : '/action?action_route=GetHierarchicalMapLayerGroups', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
]

METAGIS_URLMAP = [
    {'id' : 'metagis:layers',  'url' : '/ResultJSONGNServlet', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None, 'prefetch' : True},    
]

THREDDS_URLMAP = [
    {'id' : 'thredds:catalog',  'url' : '/catalog.xml', 'accept' : 'application/xml', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': None, 'prefetch' : False},    
]

ERDDAP_URLMAP = [
    {'id' : 'erddap:index',  'url' : '/index.json', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None, 'prefetch' : False},    
]

MYCORE_URLMAP = [
    {'id' : 'mycore:objects', 'url' : '/api/v2/objects', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'oaipmh20', 'url' : '/servlets/OAIDataProvider?verb=Identify', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'}
]


IFREMER_URLMAP = [
    {'id' : 'ifremer:search',  'url' : '/api/full-search-response', 'accept' : 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},    
    {'id' : 'oaipmh20', 'url' : '/oai/OAIHandler?verb=Identify', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'}
]


ESRIGEO_URLMAP = [
    {'id' : 'esrigeo:geoportal', 'url' : '/rest/geoportal', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'esrigeo:metadata:search', 'url' : '/rest/metadata/search', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'csw202', 'url' : '/csw', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': "3.0.0"},      
    {'id' : 'atom', 'url' : '/opensearch?f=atom&from=1&size=10&sort=title.sort%3Aasc&esdsl=%7B%7D', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': None},
    {'id' : 'esrigeo:json', 'url' : '/opensearch?f=json&from=1&size=10&sort=title.sort%3Aasc&esdsl=%7B%7D', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'rss', 'url' : '/opensearch?f=rss&from=1&size=10&sort=title.sort%3Aasc&esdsl=%7B%7D', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': None},      
    {'id' : 'dcatus11', 'url' : '/opensearch?f=dcat&from=1&size=10&sort=title.sort%3Aasc&esdsl=%7B%7D', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},      
    {'id' : 'esrigeo:csv', 'url' : '/opensearch?f=csv&from=1&size=10&sort=title.sort%3Aasc&esdsl=%7B%7D', 'expected_mime' : CSV_MIMETYPES, 'is_json' : False, 'version': None},      
    {'id' : 'esrigeo:kml', 'url' : '/opensearch?f=kml&from=1&size=10&sort=title.sort%3Aasc&esdsl=%7B%7D', 'expected_mime' : KML_MIMETYPES, 'is_json' : False, 'version': None},      
]

JKAN_URLMAP = [
    {'id' : 'dcatus11', 'url' : '/data.json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None}
 ]

WEKO3_URLMAP = [
    {'id' : 'weko3:records', 'url' : '/api/records/?page=1&size=20&sort=-createdate&search_type=0&q=&title=&creator=&filedate_from=&filedate_to=&fd_attr=&id=&id_attr=&srctitle=&type=17&dissno=&lang=english', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None}
 ]




CATALOGS_URLMAP = {'geonode' : GEONODE_URLMAP, 'dkan' : DKAN_URLMAP, 
'ckan' : CKAN_URLMAP, 'geonetwork' : GEONETWORK_URLMAP, 'pxweb' : PXWEB_URLMAP,
'socrata' : SOCRATA_URLMAP, 'dataverse' : DATAVERSE_URLMAP,
'dspace' : DSPACE_URLMAP, 'elsevierpure' : ELSVIERPURE_URLMAP, 'nada' : NADA_URLMAP, 'geoserver' : GEOSERVER_URLMAP, 
'eprints' :EPRINTS_URLMAP, 'koordinates' : KOORDINATES_URLMAP, 'aleph' : ALEPH_URLMAP, 'mycore' : MYCORE_URLMAP,
'magda' : MAGDA_URLMAP, 'opendatasoft' : OPENDATASOFT_URLMAP, 'arcgishub' : ARCGISHUB_URLMAP, 
'arcgisserver' : ARCGISSERVER_URLMAP, 'oskari' : OSKARI_URLMAP, 'metagis' : METAGIS_URLMAP,
'esrigeo' : ESRIGEO_URLMAP, 'geoblacklight': BLACKLIGHT_URLMAP,
'pygeoapi': PYGEOAPI_URLMAP, 'thredds' : THREDDS_URLMAP, 'erddap' : ERDDAP_URLMAP,
'mapproxy': MAPPROXY_URLMAP, 'statsuite' : STATSUITE_URLMAP, 'worktribe' : WORKTRIBE_URLMAP,
'inveniordm' : INVENIORDM_URLMAP, 'invenio' : INVENIO_URLMAP, 'esploro' : ESPLORO_URLMAP, 'hyrax' : HYRAX_URLMAP,
'ifremercatalog' : IFREMER_URLMAP, 'jkan' : JKAN_URLMAP,
'qwc2': QWC2_URLMAP, 'weko3' : WEKO3_URLMAP, 'wis20box': WIS20BOX_URLMAP, 'ncwms': NCWMS_URLMAP
}

def geoserver_url_cleanup_func(url):
    url = url.rstrip('/')
    if url[-3:] == 'web':        
        url = url[:-4]
    return url

def arcgisserver_url_cleanup_func(url):
    domain = urlparse(url).netloc
    if domain.find('443') > -1 or url[0:5] == 'https':
        url = url.replace('http://', 'https://')
    if url.find('/rest/services') > -1:
        url = url.rsplit('/rest/services', 1)[0]
    elif url.find('/services') > -1:
        url = url.rsplit('/services', 1)[0]
    return url

def geonetwork_url_cleanup_func(url):
    return url.split('/srv')[0]

def thredds_url_cleanup_func(url):
    return url.split('/catalog.html')[0]

def erddap_url_cleanup_func(url):
    return url.split('/index.html')[0]


URL_CLEANUP_MAP = {'geoserver' : geoserver_url_cleanup_func, 'arcgisserver' : arcgisserver_url_cleanup_func, 'geonetwork' : geonetwork_url_cleanup_func, 
                  'thredds' : thredds_url_cleanup_func, 'erddap' : erddap_url_cleanup_func}

def api_identifier(website_url, software_id, verify_json=False):
    url_map = CATALOGS_URLMAP[software_id]
    results = []
    found = []
    s = requests.Session()
    # 
    if software_id in URL_CLEANUP_MAP:
        website_url = URL_CLEANUP_MAP[software_id](website_url)
    else:
        website_url = website_url.rstrip('/')
    for item in url_map:
        try:
            if 'post_params' in item.keys():
                if 'accept' in item.keys():
                    response = s.post(request_url, verify=False, headers={'User-Agent' : USER_AGENT, 'Accept' : item['accept']}, json=json.loads(item['post_params']), timeout=(DEFAULT_TIMEOUT, DEFAULT_TIMEOUT))
                else:
                    response = s.post(request_url, verify=False, headers={'User-Agent' : USER_AGENT}, json=json.loads(item['post_params']), timeout=(DEFAULT_TIMEOUT, DEFAULT_TIMEOUT))
                
            else:
                if 'prefetch' in item and item['prefetch']:
                    request_url = website_url
                    prefeteched_data = s.get(request_url, headers={'User-Agent' : USER_AGENT}, timeout=(DEFAULT_TIMEOUT, DEFAULT_TIMEOUT))           
                request_url = website_url + item['url']
                if 'accept' in item.keys():
                    response = s.get(request_url, verify=False, headers={'User-Agent' : USER_AGENT, 'Accept' : item['accept']}, timeout=(DEFAULT_TIMEOUT, DEFAULT_TIMEOUT))
                else:
                    response = s.get(request_url, verify=False, headers={'User-Agent' : USER_AGENT}, timeout=(DEFAULT_TIMEOUT, DEFAULT_TIMEOUT))
        except requests.exceptions.Timeout:
            results.append({'url' : request_url,'error' : 'Timeout'})
            continue       
        except requests.exceptions.SSLError:
            results.append({'url' : request_url,'error' : 'SSL Error'})
            continue       
        except ConnectionError:
            results.append({'url' : request_url,'error' : 'no connection'})
            continue       
        except TooManyRedirects:
            results.append({'url' : request_url,'error' : 'no connection'})
            continue       
        if response.status_code != 200: 
            results.append({'url' : request_url, 'status' : response.status_code, 'mime' : response.headers['Content-Type'].split(';', 1)[0].lower() if 'content-type' in response.headers.keys() else '', 'error' : 'Wrong status'})
            continue
        if item['expected_mime'] is not None and 'Content-Type' in response.headers.keys():
            if response.headers['Content-Type'].split(';', 1)[0].lower() not in item['expected_mime']:
                results.append({'url' : request_url, 'status' : response.status_code, 'mime' : response.headers['Content-Type'].split(';', 1)[0].lower(), 'error' : 'Wrong content type'})
                continue
            if verify_json:
                if 'is_json' in item.keys() and item['is_json']:
                    try:
                        data = json.loads(response.content)
                    except KeyError:
                        results.append({'url' : request_url, 'status' : response.status_code, 'mime' : response.headers['Content-Type'].split(';', 1)[0].lower(), 'error' : 'Error loading JSON'})
                        continue
            api = {'type': item['id'], 'url' : website_url + item['display_url'] if 'display_url' in item.keys() else request_url}
            if item['version']: 
                api['version'] = item['version']
            found.append(api)
    logging.info('Found: ' + str(results))
    return found



@app.command()
def detect_software(software, dryrun: Annotated[bool, typer.Option("--dryrun")]=False, replace_endpoints: Annotated[bool, typer.Option("--replace")]=False, mode='entries'):
    """Enrich data catalogs with API endpoints by software"""
    if mode == 'entries':
        root_dir = ENTRIES_DIR
    else:
        root_dir = SCHEDULED_DIR
    dirs = os.listdir(root_dir)    
    for root, dirs, files in os.walk(root_dir):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            if record['software']['id']  == software:
                print('Processing %s' % (os.path.basename(filename).split('.', 1)[0]))
                if 'endpoints' in record.keys() and len(record['endpoints']) > 0 and replace_endpoints is False:
                    print(' - skip, we have endpoints already and no replace mode')
                    continue
                found = api_identifier(record['link'].rstrip('/'), software)
                record['endpoints'] = []
                for api in found:
                    print('- %s %s' % (api['type'], api['url']))
                    record['endpoints'].append(api)                
                if len(record['endpoints']) > 0:
                    record['api'] = True
                    record['api_status'] = 'active'
                    f = open(filepath, 'w', encoding='utf8')
                    f.write(yaml.safe_dump(record, allow_unicode=True))
                    f.close()
                    print('- updated profile')
                else:
                    print('- no endpoints, not updated')

@app.command()
def detect_single(uniqid, dryrun: Annotated[bool, typer.Option("--dryrun")]=False, replace_endpoints: Annotated[bool, typer.Option("--replace")]=False, mode='entries'):
    """Enrich single data catalog with API endpoints"""
    if mode == 'entries':
        root_dir = ENTRIES_DIR
    else:
        root_dir = SCHEDULED_DIR
    found = False        
    for root, dirs, files in os.walk(root_dir):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            filepath = filename
#            print(filepath)
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            idkeys = []
            for k in ['uid', 'id', 'link']:
                if k in record.keys():
                    idkeys.append(record[k])
            if uniqid not in idkeys:
       
                continue
            found = True
            if record['software']['id']  in CATALOGS_URLMAP.keys():
                print('Processing %s' % (os.path.basename(filename).split('.', 1)[0]))
                if 'endpoints' in record.keys() and len(record['endpoints']) > 0 and replace_endpoints is False:
                    print(' - skip, we have endpoints already and no replace mode')
                    continue
                found = api_identifier(record['link'].rstrip('/'), record['software']['id'])
                record['endpoints'] = []
                for api in found:
                    print('- %s %s' % (api['type'], api['url']))
                    record['endpoints'].append(api)
                if len(record['endpoints']) > 0:
                    f = open(filepath, 'w', encoding='utf8')
                    f.write(yaml.safe_dump(record, allow_unicode=True))
                    f.close()
                    print('- updated profile')
                else:
                    print('- no endpoints, not updated')
            else:
                print('There is no rules for software: %s' % (record['software']['id']))
    if not found:
        print('Data catalog with id %s not found' %(uniqid))                

@app.command()
def detect_country(country, dryrun=False, replace_endpoints=True, mode='entries'):
    """Enrich data catalogs with API endpoints by country"""
    if mode == 'entries':
        root_dir = ENTRIES_DIR
    else:
        root_dir = SCHEDULED_DIR    
    for root, dirs, files in os.walk(root_dir):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            if record['owner']['location']['country']['id'] != country:
                continue
            if record['software']['id']  in CATALOGS_URLMAP.keys():
                print('Processing %s' % (os.path.basename(filename).split('.', 1)[0]))
                if 'endpoints' in record.keys() and len(record['endpoints']) > 0 and replace_endpoints is False:
                    print(' - skip, we have endpoints already and no replace mode')
                    continue
                found = api_identifier(record['link'].rstrip('/'), record['software']['id'])
                record['endpoints'] = []
                for api in found:
                    print('- %s %s' % (api['type'], api['url']))
                    record['endpoints'].append(api)
                if len(record['endpoints']) > 0:
                    f = open(filepath, 'w', encoding='utf8')
                    f.write(yaml.safe_dump(record, allow_unicode=True))
                    f.close()
                    print('- updated profile')
                else:
                    print('- no endpoints, not updated')

@app.command()
def detect_ckan(dryrun=False, replace_endpoints=True, mode='entries'):
    """Enrich data catalogs with API endpoints by CKAN instance (special function to update all endpoints"""
    """Enrich data catalogs with API endpoints by country"""
    if mode == 'entries':
        root_dir = ENTRIES_DIR
    else:
        root_dir = SCHEDULED_DIR   
    for root, dirs, files in os.walk(root_dir):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            if record['software']['id'] == 'ckan':
                print('Processing %s' % (os.path.basename(filename).split('.', 1)[0]))
                if 'endpoints' in record.keys() and len(record['endpoints']) > 1:
                    print(' - skip, we have more than 2 endpoints so we skip')
                    continue
                if 'endpoints' and len(record['endpoints']) == 1 and record['endpoints'][0]['type'] == 'ckanapi':
                    base_url = record['endpoints'][0]['url'][0:-6]
                else:
                    base_url = record['link'].rstrip('/')
                found = api_identifier(base_url, record['software']['id'])
                record['endpoints'] = []
                for api in found:
                    print('- %s %s' % (api['type'], api['url']))
                    record['endpoints'].append(api)
                if len(record['endpoints']) > 0:
                    f = open(filepath, 'w', encoding='utf8')
                    f.write(yaml.safe_dump(record, allow_unicode=True))
                    f.close()
                    print('- updated profile')
                else:
                    print('- no endpoints, not updated')


@app.command()
def detect_all(status='undetected', replace_endpoints: Annotated[bool, typer.Option("--replace")]=False, mode='entries'):
    """Detect all known API endpoints"""
    if mode == 'entries':
        root_dir = ENTRIES_DIR
    else:
        root_dir = SCHEDULED_DIR   
    for root, dirs, files in os.walk(root_dir):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            if record['software']['id'] in CATALOGS_URLMAP.keys():                
                if 'endpoints' not in record.keys() or len(record['endpoints']) == 0:
                    if status == 'undetected':
                        print('Processing catalog %s, software %s' % (os.path.basename(filename).split('.', 1)[0], record['software']['id']))
                        if 'endpoints' in record.keys() and len(record['endpoints']) > 0 and replace_endpoints is False:
                            print(' - skip, we have endpoints already and no replace mode')
                            continue
                        found = api_identifier(record['link'].rstrip('/'), record['software']['id'])
                        record['endpoints'] = []
                        for api in found:
                            print('- %s %s' % (api['type'], api['url']))
                            record['endpoints'].append(api)
                        if len(record['endpoints']) > 0:
                            f = open(filepath, 'w', encoding='utf8')
                            f.write(yaml.safe_dump(record, allow_unicode=True))
                            f.close()
                            print('- updated profile')
                        else:
                            print('- no endpoints, not updated')


@app.command()
def report(status='undetected', filename=None, mode='entries'):
    """Report data catalogs with undetected API endpoints"""
    out = sys.stdout if filename is None else open(filename, 'w', encoding='utf8')

    if mode == 'entries':
        root_dir = ENTRIES_DIR
    else:
        root_dir = SCHEDULED_DIR   

    if status == 'undetected':
        out.write(','.join(['id','uid', 'link', 'software_id' 'status']) + '\n')
    for root, dirs, files in os.walk(root_dir):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            if record['software']['id'] in CATALOGS_URLMAP.keys():                
                if 'endpoints' not in record.keys() or len(record['endpoints']) == 0:
                    if status == 'undetected':
                        out.write(','.join([record['id'], record['uid'], record['link'], record['software']['id'], 'undetected']) + '\n')
    if filename is not None:
        out.close()
                    

@app.command()
def update_broken_arcgis(status='undetected', replace_endpoints: Annotated[bool, typer.Option("--replace")]=True, mode='entries'):
    """Detect all broken ArcGIS portals and update endpoints"""
    if mode == 'entries':
        root_dir = ENTRIES_DIR
    else:
        root_dir = SCHEDULED_DIR   
    for root, dirs, files in os.walk(root_dir):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            if record['software']['id'] in ['arcgishub', 'arcgisserver']:                
                if 'endpoints' not in record.keys() or len(record['endpoints']) < 2:
                    if status == 'undetected':
                        print('Processing catalog %s, software %s' % (os.path.basename(filename).split('.', 1)[0], record['software']['id']))
                        if 'endpoints' in record.keys() and len(record['endpoints']) > 0 and replace_endpoints is False:
                            print(' - skip, we have endpoints already and no replace mode')
                            continue
                        found = api_identifier(record['link'].rstrip('/'), record['software']['id'])
                        record['endpoints'] = []
                        for api in found:
                            print('- %s %s' % (api['type'], api['url']))
                            record['endpoints'].append(api)
                        if len(record['endpoints']) > 0:
                            f = open(filepath, 'w', encoding='utf8')
                            f.write(yaml.safe_dump(record, allow_unicode=True))
                            f.close()
                            print('- updated profile')
                        else:
                            print('- no endpoints, not updated')



if __name__ == "__main__":
    app()