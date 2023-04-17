#!/usr/bin/env python
# This script intended to enrich data of catalogs entries 

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
from urllib3.exceptions import InsecureRequestWarning
# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


ROOT_DIR = '../data/entities'

app = typer.Typer()

DATA_NAMES = ['data', 'dati', 'datos', 'dados', 'podatki', 'datosabiertos', 'opendata', 'data', 'dados abertos', 'daten', 'offendaten']
GOV_NAMES = ['gov', 'gob', 'gouv', 'egov', 'e-gov', 'go', 'govt']


@app.command()
def enrich_ot(dryrun=False):
    """Update owner type according to name"""
    dirs = os.listdir(ROOT_DIR)
    for adir in dirs:
        subdirs = os.listdir(os.path.join(ROOT_DIR, adir))
        for subdir in subdirs:
            files = os.listdir(os.path.join(ROOT_DIR, adir, subdir))
            for filename in files:                
                filepath = os.path.join(ROOT_DIR, adir, subdir, filename)
                if os.path.isdir(filepath): continue
                changed = False
                f = open(filepath, 'r', encoding='utf8')
                data = yaml.load(f, Loader=Loader)            
                f.close()
                if 'owner_type' in data.keys() and data['owner_type'] == 'Government':
                    parts = urlparse(data['link'])
                    netloc_parts = parts.netloc.lower().split('.')
                    if len(netloc_parts) > 2:
                        if netloc_parts[-2] in GOV_NAMES and netloc_parts[-3] in DATA_NAMES:
                            data['owner_type'] = 'Central government'
                            changed = True
                if changed:
                    if dryrun is True:
                        print('Dryrun: should be updated %s' % (filename))
                    else:
                        f = open(filepath, 'w', encoding='utf8')
                        f.write(yaml.safe_dump(data, allow_unicode=True))
                        f.close()
                        print('updated %s' % (filename))


@app.command()
def enrich_topics(dryrun=False):
    """Set topics tags from catalog metadata to be improved later"""
    dirs = os.listdir(ROOT_DIR)
    for adir in dirs:
        subdirs = os.listdir(os.path.join(ROOT_DIR, adir))
        for subdir in subdirs:
            files = os.listdir(os.path.join(ROOT_DIR, adir, subdir))
            for filename in files:                
                filepath = os.path.join(ROOT_DIR, adir, subdir, filename)
                if os.path.isdir(filepath): continue
                changed = False
                f = open(filepath, 'r', encoding='utf8')
                data = yaml.load(f, Loader=Loader)            
                f.close()

                del data['tags']
                
                if 'tags' in data.keys():
                    tags = set(data['tags'])
                else:
                    tags = set([])

                if data['catalog_type'] == 'Indicators catalog':
                    tags.add('statistics')
                elif data['catalog_type'] == 'Microdata catalog':
                    tags.add('microdata')
                elif data['catalog_type'] == 'Geoportal':
                    tags.add('geospatial')
                elif data['catalog_type'] == 'Scientific data repository':
                    tags.add('scientific')
                if 'owner_type' in data.keys() and data['owner_type'] in ['Government', 'Local government', 'Central government']:
                    tags.add('government')
                if 'api' in data.keys() and data['api'] is True:
                    tags.add('has_api')
                data['tags'] = list(tags)
                changed = True
                if changed:
                    if dryrun is True:
                        print('Dryrun: should be updated %s' % (filename))
                    else:
                        f = open(filepath, 'w', encoding='utf8')
                        f.write(yaml.safe_dump(data, allow_unicode=True))
                        f.close()
                        print('updated %s' % (filename))


@app.command()
def enrich_countries(dryrun=False):
    """Update countries with codes"""
    dirs = os.listdir(ROOT_DIR)
    for adir in dirs:
        subdirs = os.listdir(os.path.join(ROOT_DIR, adir))
        for subdir in subdirs:
            files = os.listdir(os.path.join(ROOT_DIR, adir, subdir))
            for filename in files:                
                filepath = os.path.join(ROOT_DIR, adir, subdir, filename)
                if os.path.isdir(filepath): continue
                changed = False
                f = open(filepath, 'r', encoding='utf8')
                data = yaml.load(f, Loader=Loader)            
                f.close()
                if 'countries' in data.keys():
                    if isinstance(data['countries'][0], str):
                        name = data['countries'][0]
                    elif isinstance(data['countries'][0]['name'], str):
                        name = data['countries'][0]['name']
                    else:
                        name = data['countries'][0]['name']['name']
                    print(name)
                    data['countries'] = [{'id' : adir, 'name' : name}]                          
                     
                    changed = True
                if changed:
                    if dryrun is True:
                        print('Dryrun: should be updated %s' % (filename))
                    else:
                        f = open(filepath, 'w', encoding='utf8')
                        f.write(yaml.safe_dump(data, allow_unicode=True))
                        f.close()
                        print('updated %s' % (filename))


headers = {'Accept': 'application/json'}

@app.command()
def setstatus(updatedata=False):
    """Checks every site existence and endpoints availability except Geonetwork yet"""    
    session = requests.Session()
    session.max_redirects = 100    
    results = []
    out = open('statusreport.jsonl', 'w', encoding='utf8')    
    dirs = os.listdir(ROOT_DIR)
    for adir in dirs:
        subdirs = os.listdir(os.path.join(ROOT_DIR, adir))
        for subdir in subdirs:
            files = os.listdir(os.path.join(ROOT_DIR, adir, subdir))
            for filename in files:                
                filepath = os.path.join(ROOT_DIR, adir, subdir, filename)
                if os.path.isdir(filepath): continue
                changed = False
                f = open(filepath, 'r', encoding='utf8')
                item = yaml.load(f, Loader=Loader)            
                f.close()
                if 'status' in item.keys(): continue
                print(item['link'])
                report = {'id' : item['id'], 'name' : item['name'] if 'name' in item.keys() else '', 'link' : item['link'], 'date_verified' : datetime.datetime.now().isoformat(), 'software' : item['software'] if 'software' in item.keys() else ''}
                report['api_status'] = 'uncertain'
                report['api_http_code'] = ''
                report['api_content_type'] = ''
                try:
                    response = session.head(item['link'], verify=False, allow_redirects=True)
                except ConnectionError as e:
                    report['status'] = 'deprecated'
                    report['api_status'] = 'deprecated'
                    report['link_http_code'] = ''
                    item['status'] = report['status']
                    item['api_status'] = report['api_status']
                    f = open(filepath, 'w', encoding='utf8')
                    f.write(yaml.safe_dump(item, allow_unicode=True))
                    f.close()
                    print('updated %s' % (filename))
                    continue            
                except ReadTimeout as e:
                    report['status'] = 'deprecated'
                    report['api_status'] = 'deprecated'
                    report['link_http_code'] = ''
                    item['status'] = report['status']
                    item['api_status'] = report['api_status']
                    f = open(filepath, 'w', encoding='utf8')
                    f.write(yaml.safe_dump(item, allow_unicode=True))
                    f.close()
                    print('updated %s' % (filename))
                    continue            
                except TooManyRedirects as e:
                    report['status'] = 'deprecated'
                    report['api_status'] = 'deprecated'
                    report['link_http_code'] = ''
                    item['status'] = report['status']
                    item['api_status'] = report['api_status']
                    f = open(filepath, 'w', encoding='utf8')
                    f.write(yaml.safe_dump(item, allow_unicode=True))
                    f.close()
                    print('updated %s' % (filename))
                    continue            


                report['link_http_code'] = response.status_code                
                if not response.ok:
                    report['status'] = 'deprecated'
                    report['api_status'] = 'deprecated'
                else:
                    report['status'] = 'active'
                if 'software' in item.keys():
                    if item['software'] == 'CKAN':
                        if 'endpoints' in item.keys() and len(item['endpoints']) > 0:
                            if item['endpoints'][0]['type'] == 'ckanapi':
                                search_endpoint = item['endpoints'][0]['url'] + '/action/package_search'
                                noerror = True
                                try:  
                                    response = session.get(search_endpoint, headers=headers, verify=False)                                    
                                except:
                                    report['api_status'] = 'deprecated'
                                    noerror = False
                                if noerror:
                                    report['api_http_code'] = response.status_code
                                    report['api_content_type'] = response.headers['content-type'] if 'content-type' in response.headers.keys() else ''
                                    if not response.ok:
                                        report['api_status'] = 'deprecated'
                                    else:
                                        if 'content-type' in response.headers.keys() and response.headers['content-type'].split(';',1)[0].lower() == 'application/json':
                                            report['api_status'] = 'active'
                    elif item['software'] == 'NADA':
                        if 'endpoints' in item.keys() and len(item['endpoints']) > 0:
                                if item['endpoints'][0]['type'] == 'nada:catalog-search':                
                                    search_endpoint = item['endpoints'][0]['url']
                                    response = session.get(search_endpoint, headers=headers, verify=False)
                                    report['api_http_code'] = response.status_code
                                    report['api_content_type'] = response.headers['content-type'] if 'content-type' in response.headers.keys() else ''
                                    if not response.ok:
                                        report['api_status'] = 'deprecated'
                                    else:
                                        if response.headers['content-type'].split(';',1)[0].lower() == 'application/json':
                                            report['api_status'] = 'active'
                    elif item['software'] == 'Dataverse':
                        if 'endpoints' in item.keys() and len(item['endpoints']) > 0:
                                if item['endpoints'][0]['type'] == 'dataverseapi':                
                                    search_endpoint = item['endpoints'][0]['url']
                                    response = session.get(search_endpoint, headers=headers, verify=False)
                                    report['api_http_code'] = response.status_code
                                    report['api_content_type'] = response.headers['content-type'] if 'content-type' in response.headers.keys() else ''
                                    if not response.ok:
                                        report['api_status'] = 'deprecated'
                                    else:
                                        if response.headers['content-type'].split(';',1)[0].lower() == 'application/json':
                                            report['api_status'] = 'active'
                    elif item['software'] == 'ArcGIS Hub':
                        if 'endpoints' in item.keys() and len(item['endpoints']) > 0:
                                endpoints = {item['type']:item for item in item['endpoints']}
                                if 'dcatap201' in endpoints.keys():
                                    search_endpoint = endpoints['dcatap201']['url']
                                    response = session.get(search_endpoint, headers=headers, verify=False)
                                    report['api_http_code'] = response.status_code
                                    report['api_content_type'] = response.headers['content-type'] if 'content-type' in response.headers.keys() else ''
                                    if not response.ok:
                                        report['api_status'] = 'deprecated'
                                    else:
                                        if 'content-type' in response.headers.keys() and response.headers['content-type'].split(';',1)[0].lower() == 'application/json':
                                            test_resp = json.loads(response.text)
                                            if 'error' not in test_resp.keys():
                                                report['api_status'] = 'active'
                                    report['status'] = report['api_status']
                    elif item['software'] == 'InvenioRDM':
                        if 'endpoints' in item.keys() and len(item['endpoints']) > 0:
                                endpoints = {item['type']:item for item in item['endpoints']}
                                if 'inveniordmapi:records' in endpoints.keys():
                                    search_endpoint = endpoints['inveniordmapi:records']['url']
                                    response = session.get(search_endpoint, headers=headers, verify=False)
                                    report['api_http_code'] = response.status_code
                                    report['api_content_type'] = response.headers['content-type'] if 'content-type' in response.headers.keys() else ''
                                    if not response.ok:
                                        report['api_status'] = 'deprecated'
                                    else:
                                        if 'content-type' in response.headers.keys() and response.headers['content-type'].split(';',1)[0].lower() == 'application/json':
                                            report['api_status'] = 'active'
                    elif item['software'] == 'uData':
                        search_endpoint = item['link'] + '/api/1/datasets/'
                        response = session.get(search_endpoint, headers=headers, verify=False)
                        report['api_http_code'] = response.status_code
                        report['api_content_type'] = response.headers['content-type']
                        if not response.ok:
                            report['api_status'] = 'deprecated'
                        else:
                            if 'content-type' in response.headers.keys() and response.headers['content-type'].split(';',1)[0].lower() == 'application/json':
                                report['api_status'] = 'active'        
                item['status'] = report['status']
                item['api_status'] = report['api_status']
                f = open(filepath, 'w', encoding='utf8')
                f.write(yaml.safe_dump(item, allow_unicode=True))
                f.close()
                print('updated %s' % (filename))
#        print(report)
#        out.write(json.dumps(report) + '\n')


if __name__ == "__main__":
    app()