#!/usr/bin/env python
# This script intended to detect data catalogs API

import sys
import typer
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
from requests.exceptions import ConnectionError, TooManyRedirects
from urllib3.exceptions import InsecureRequestWarning#, ConnectionError
# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


ENTRIES_DIR = '../data/entities'
app = typer.Typer()

DEFAULT_TIMEOUT = 15

XML_MIMETYPES = ['text/xml', 'application/xml', 'application/vnd.ogc.se_xml', 'application/vnd.ogc.wms_xml', 'application/rdf+xml', 'application/rss+xml']
JSON_MIMETYPES = ['text/json', 'application/json']
CSV_MIMETYPES = ['text/csv']

GEONODE_URLMAP = [
    {'id' : 'geonode:layers', 'url' : '/api/layers/', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'geonode:datasets', 'url' : '/api/datasets/', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'geonode:documents', 'url' : '/api/documents/', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'dcatus11', 'url' : '/data.json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'wms', 'url' : '/geoserver/ows?service=WMS&version=1.1.1&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.1.1'},
    {'id' : 'wfs', 'url' : '/geoserver/ows?service=WFS&version=1.1.0&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.1.0'},
    {'id' : 'wcs', 'url' : '/geoserver/ows?service=WCS&version=1.1.1&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.1.1'},
    {'id' : 'csw', 'url' : '/catalogue/csw?service=CSW&version=2.0.2&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0.2'},
    {'id' : 'oaipmh', 'url' : '/catalogue/csw?mode=oaipmh&verb=Identify', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'},
    {'id' : 'wmts', 'url' : '/geoserver/gwc/service/wmts?service=WMTS&version=1.0.0&request=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.0.0'},
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
    {'id' : 'csw', 'url' : '/srv/eng/csw?SERVICE=CSW&VERSION=2.0.2&REQUEST=GetCapabilities', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0.2'},
    {'id' : 'opensearch', 'url' : '/srv/eng/portal.opensearch', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '1.0'},
    {'id' : 'oaipmh', 'url' : '/srv/eng/oaipmh?verb=Identify', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'},    
]

SOCRATA_URLMAP = [
    {'id' : 'dcatus11', 'url' : '/data.json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'socrata:views', 'url' : '/api/views', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None}
]

PXWEB_URLMAP = [
    {'id' : 'pxwebapi', 'url' : '/api/v1/en/', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '1'}
]

DATAVERSE_URLMAP = [
    {'id' : 'dataverseapi', 'display_url' : '/api/search','url' : '/api/search?q=*&type=dataset&sort=name&order=asc', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'oaipmh', 'url' : '/oai?verb=Identify', 'accept' : 'application/xml', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'} 
]

DSPACE_URLMAP = [
    {'id' : 'dspace:objects', 'url' : '/server/api/discover/search/objects', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '7'},
    {'id' : 'dspace:items', 'url' : '/rest/items', 'accept': 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': '6'},
    {'id' : 'oaipmh', 'url' : '/oai/request?verb=Identify', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'} 
]

ELSVIERPURE_URLMAP = [
    {'id' : 'oaipmh', 'url' : '/ws/oai?verb=Identify', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'},
    {'id' : 'rss', 'url' : '/en/datasets/?search=&isCopyPasteSearch=false&format=rss', 'expected_mime' : XML_MIMETYPES, 'is_json' : False, 'version': '2.0'} 
]

NADA_URLMAP = [
    {'id' : 'nada:catalog-search', 'url' : '/index.php/api/catalog/search', 'accept': 'application/json', 'expected_mime' : JSON_MIMETYPES, 'is_json' : True, 'version': None},
    {'id' : 'nada:csvexport', 'url' : '/index.php/catalog/export/csv?ps=5000&collection[]', 'expected_mime' : CSV_MIMETYPES, 'is_json' : False, 'version': None}
]


CATALOGS_URLMAP = {'geonode' : GEONODE_URLMAP, 'dkan' : DKAN_URLMAP, 
'ckan' : CKAN_URLMAP, 'geonetwork' : GEONETWORK_URLMAP, 'pxweb' : PXWEB_URLMAP,
'socrata' : SOCRATA_URLMAP, 'dataverse' : DATAVERSE_URLMAP,
'dspace' : DSPACE_URLMAP, 'elsevierpure' : ELSVIERPURE_URLMAP, 'nada' : NADA_URLMAP}



def api_identifier(website_url, url_map, verify_json=False):
    results = []
    found = []
    print('-', end="")
    for item in url_map:
        print(" %s" % (item['id']), end="")
        request_url = website_url + item['url']        
        try:
            if 'accept' in item.keys():
                response = requests.get(request_url, verify=False, headers={'Accept' : item['accept']}, timeout=(DEFAULT_TIMEOUT, DEFAULT_TIMEOUT))
            else:
                response = requests.get(request_url, verify=False, timeout=(DEFAULT_TIMEOUT, DEFAULT_TIMEOUT))
        except requests.exceptions.Timeout:
            results.append({'url' : request_url,'error' : 'Timeout'})
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
    print()
    print(results)
    return found



@app.command()
def detect(software, dryrun=False, replace_endpoints=True):
    """Enrich data catalogs with API endpoints"""
    dirs = os.listdir(ENTRIES_DIR)

    dirs = os.listdir(ENTRIES_DIR)    
    for root, dirs, files in os.walk(ENTRIES_DIR):
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
                found = api_identifier(record['link'].rstrip('/'), CATALOGS_URLMAP[software])
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
def detect_all(mode='undetected', replace_endpoints=True):
    """Detect all known API endpoints"""
    dirs = os.listdir(ENTRIES_DIR)    
    for root, dirs, files in os.walk(ENTRIES_DIR):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            if record['software']['id'] in CATALOGS_URLMAP.keys():                
                if 'endpoints' not in record.keys() or len(record['endpoints']) == 0:
                    if mode == 'undetected':
                        print('Processing catalog %s, software %s' % (os.path.basename(filename).split('.', 1)[0], record['software']['id']))
                        if 'endpoints' in record.keys() and len(record['endpoints']) > 0 and replace_endpoints is False:
                            print(' - skip, we have endpoints already and no replace mode')
                            continue
                        found = api_identifier(record['link'].rstrip('/'), CATALOGS_URLMAP[record['software']['id']])
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
def report(mode='undetected', filename=None):
    """Report data catalogs with undetected API endpoints"""
    out = sys.stdout if filename is None else open(filename, 'w', encoding='utf8')
    dirs = os.listdir(ENTRIES_DIR)    
    if mode == 'undetected':
        out.write(','.join(['uid', 'link', 'software_id' 'status']) + '\n')
    for root, dirs, files in os.walk(ENTRIES_DIR):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            if record['software']['id'] in CATALOGS_URLMAP.keys():                
                if 'endpoints' not in record.keys() or len(record['endpoints']) == 0:
                    if mode == 'undetected':
                        out.write(','.join([record['uid'], record['link'], record['software']['id'], 'undetected']) + '\n')
    if filename is not None:
        out.close()
                    


if __name__ == "__main__":
    app()