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
DATASETS_DIR = '../data/datasets'
SCHEDULED_DIR = '../data/scheduled'

app = typer.Typer()

DATA_NAMES = ['data', 'dati', 'datos', 'dados', 'podatki', 'datosabiertos', 'opendata', 'data', 'dados abertos', 'daten', 'offendaten']
GOV_NAMES = ['gov', 'gob', 'gouv', 'egov', 'e-gov', 'go', 'govt']


def load_csv_dict(filepath, key, delimiter='\t'):
    data = {}
    f = open(filepath, 'r', encoding='utf8')
    reader = csv.DictReader(f, delimiter=delimiter)
    for r in reader:
        data[r[key]] = r
    f.close()
    return data


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


def __topic_find(topics, id, topic_type='eudatatheme'):
    for t in topics:
        if topic_type == topic_type and id == t['id']:
            return True
    return False


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
                if data is None: 
                    print('error on %s' %(filename))
                    break
                if 'tags' in data.keys() and data['tags'] is not None:
                    tags = set(data['tags'])
                else:
                    tags = set([])
                if 'topics' in data.keys():
                    topics = data['topics']
                else:
                    data['topics'] = []
                    topics = []

                if data['catalog_type'] == 'Indicators catalog':
                    tags.add('statistics')
                elif data['catalog_type'] == 'Microdata catalog':
                    if 'properties' not in data:
                        data['properties'] = {}
                    data['properties']['transferable_topics'] = True
                    found = __topic_find(topics, 'SOCI', topic_type='eudatatheme')
                    if not found:
                        topics.append({'id' : 'SOCI', 'name' : 'Population and society', 'type' : 'eudatatheme'})
                    found = __topic_find(topics, 'Society', topic_type='iso19115')
                    if not found:
                        topics.append({'id' : 'Society', 'name' : 'Society', 'type' : 'iso19115'})
                    tags.add('microdata')
                elif data['catalog_type'] == 'Geoportal':
                    tags.add('geospatial')
                elif data['catalog_type'] == 'Scientific data repository':
                    if 'properties' not in data:
                        data['properties'] = {}
                    data['properties']['tranferable_topics'] = True
                    found = __topic_find(topics, 'TECH', topic_type='eudatatheme')                      
                    if not found:
                        print('Added', {'id' : 'TECH', 'name' : 'Science and technology', 'type' : 'eudatatheme'})
                        topics.append({'id' : 'TECH', 'name' : 'Science and technology', 'type' : 'eudatatheme'})
                    tags.add('scientific')
                if 'owner' in data.keys():
                    if data['owner']['type'] in ['Regional government', 'Local government', 'Central government']:
                        tags.add('government')
                    if data['owner']['type']  in ['Regional government', 'Local government']:
                        print('transferable4')
                        if 'properties' not in data:
                            data['properties'] = {}
                        data['properties']['transferable_location'] = True
                if 'api' in data.keys() and data['api'] is True:
                    tags.add('has_api')     
                tags = list(tags)
                if 'tags' in data.keys() and tags != data['tags']:
                    changed = True
                data['topics'] = topics
                changed = True
                print(data)
                if changed:
                    if dryrun is True:
                        print('Dryrun: should be updated %s' % (filename))
                    else:
                        f = open(filepath, 'w', encoding='utf8')
                        f.write(yaml.safe_dump(data, allow_unicode=True))
                        f.close()
                        print('updated %s' % (filename))
#                        print(data)


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


@app.command()
def enrich_from_csv(filename="catalogs_enrich.csv "):
    """Build datasets as JSONL from entities as YAML"""
    profiles = {}
    reader = csv.DictReader(open(filename, 'r', encoding='utf8'), delimiter='\t') 
    for row in reader:
        profiles[row['id']] = row

    dirs = os.listdir(ROOT_DIR)
    for root, dirs, files in os.walk(ROOT_DIR):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            print('Processing %s' % (os.path.basename(filename).split('.', 1)[0]))
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            if record['id'] in profiles.keys():
                for key in ['status', 'name', 'owner_type', 'owner_name', 'api_status', 'catalog_type', 'software']:
                    record[key] = profiles[record['id']][key]   
                f = open(filepath, 'w', encoding='utf8')
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                print('- saved')


@app.command()
def enrich_location(dryrun=False):
    """Enrich location codes"""
    dirs = os.listdir(ROOT_DIR)

    dirs = os.listdir(ROOT_DIR)
    for root, dirs, files in os.walk(ROOT_DIR):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            print('Processing %s' % (os.path.basename(filename).split('.', 1)[0]))
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            # Create owner record
            owner = {'name' : record['owner_name'], 'type' : record['owner_type'], 'location' : {'level' : 1, 'country' : record['countries'][0].copy()}}
            if owner['location']['country']['id'] == 'UK':
                owner['location']['country']['id'] = 'GB'
            if 'owner_link' in record.keys(): 
                owner['link'] = record['owner_link']
            coverage = []
            for country in record['countries']:
                country_id = country['id']
                if country_id == 'UK': 
                    country_id = 'GB'
                coverage.append({'location' : {'level' : 1, 'country' : {'id' : country_id, 'name': country['name']}}})
            for key in ['countries', 'owner_name', 'owner_type', 'owner_link']:
                if key in record.keys():
                    del record[key]
            parent_path = root.rsplit('\\', 2)[-2]
            if parent_path.find('-') > -1:
                owner['location']['level']  = 2
                owner['location']['subregion']  = {'id' : parent_path}
                coverage[0]['location']['level']  = 2
                coverage[0]['location']['subregion']  = {'id' : parent_path}
            record['coverage'] = coverage
            record['owner'] = owner
#            if owner['location']['level'] == 2:
#              print(yaml.safe_dump(record, allow_unicode=True))
            f = open(filepath, 'w', encoding='utf8')
            f.write(yaml.safe_dump(record, allow_unicode=True))
            f.close()

@app.command()
def enrich_identifiers(filepath, idtype, dryrun=False):
    """Enrich identifiers"""    
    f = open(filepath, 'r', encoding='utf8')
    reader = csv.DictReader(f, delimiter='\t')
    reg_map = {}
    for row in reader:
        if row['registry_uid'] not in reg_map.keys():
            reg_map[row['registry_uid']] = [{'id' : idtype, 'url' : row['dataportals_url'], 'value' : row['dataportals_name']}]
        else:
            reg_map[row['registry_uid']].append({'id' : idtype, 'url' : row['dataportals_url'], 'value' : row['dataportals_name']})

    dirs = os.listdir(ROOT_DIR)
    for root, dirs, files in os.walk(ROOT_DIR):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
#            print('Processing %s' % (os.path.basename(filename).split('.', 1)[0]))
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            changed = False
            if 'identifiers' not in record.keys():
                if record['uid'] in reg_map.keys():
                    record['identifiers'] = reg_map[record['uid']]
                    changed = True
            else:
                if record['uid'] in reg_map.keys():
                    ids = []
                    for item in record['identifiers']:
                        ids.append(item['url'])
                    for item in reg_map[record['uid']]:
                        if item['url'] not in ids:
                            record['identifiers'].append(item)
                            changed = True
                
            if changed: 
                f = open(filepath, 'w', encoding='utf8')
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
            print('Updated %s' % (os.path.basename(filename).split('.', 1)[0]))
            

@app.command()
def fix_api(dryrun=False, mode='entities'):
    """Fix API"""    
    root_dir = ROOT_DIR if mode == 'entities' else SCHEDULED_DIR
    dirs = os.listdir(root_dir)
    for root, dirs, files in os.walk(root_dir):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
#            print('Processing %s' % (os.path.basename(filename).split('.', 1)[0]))
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            changed = False
            if record['software']['id'] == 'geonode':                   
                if 'endpoints' in record.keys():
                    endpoints = []
                    for endp in record['endpoints']:
                        print(endp)
                        if endp['type'] == 'dcatus11':
                            endp['type'] = 'geonode:dcatus11'   
                            print('Fixed endpoint')
                            changed = True         
                        endpoints.append(endp)
            if 'endpoints' in record.keys():
                endpoints = []
                for endp in record['endpoints']:
                    if endp['type'] in ['wfs', 'wcs', 'tms', 'wms-c', 'wms', 'csw', 'wmts', 'wps', 'oaipmh']:
                        if 'version' in endp.keys():
                            endp['type'] = endp['type'] + endp['version'].replace('.', '')
                            print('Fixed endpoint')
                            changed = True         
                    if endp['type'] == 'ckanapi':
                        endp['type'] = 'ckan'
                        print('Fixed endpoint')
                        changed = True         
                    if endp['type'] == 'geonetworkapi':
                        endp['type'] = 'geonetwork'
                        print('Fixed endpoint')
                        changed = True         
                    if endp['type'] == 'geonetworkapi:query':
                        endp['type'] = 'geonetwork:query'
                        print('Fixed endpoint')
                        changed = True         
                    if endp['type'] == 'opendatasoft':
                        endp['type'] = 'opendatasoftapi'
                        print('Fixed endpoint')
                        changed = True         
                    if endp['type'] == 'arcgisrest':
                        endp['type'] = 'arcgis:rest:services'
                        endp['url'] = endp['url'] + '?f=pjson'
                        print('Fixed endpoint')
                        changed = True         
                    endpoints.append(endp)                        
            else:
                if record['software']['id'] == 'arcgisserver':
                    endpoints = []
                    endp = {}
                    endp['type'] = 'arcgis:rest:services'
                    endp['url'] = record['link'] + '?f=pjson'
                    endp['version'] = None
                    print('Fixed endpoint')
                    changed = True         
                    endpoints.append(endp)                       
            if changed is True:
                record['endpoints'] = endpoints
                f = open(filepath, 'w', encoding='utf8')
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                print('Updated %s' % (os.path.basename(filename).split('.', 1)[0]))
            

@app.command()
def fix_catalog_type(dryrun=False, mode='entities'):
    """Fix catalog_type"""    
    root_dir = ROOT_DIR if mode == 'entities' else SCHEDULED_DIR
    dirs = os.listdir(root_dir)
    for root, dirs, files in os.walk(root_dir):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
#            print('Processing %s' % (os.path.basename(filename).split('.', 1)[0]))
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            changed = False
            if record['catalog_type'] == 'Unknown':
                record['catalog_type'] = 'Open data portal'
                changed = True
            if changed is True:
                f = open(filepath, 'w', encoding='utf8')
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                print('Updated %s' % (os.path.basename(filename).split('.', 1)[0]))
            

@app.command()
def update_macroregions(dryrun=False, mode='entities'):
    """Update macro regions"""    
    macro_dict = load_csv_dict('../data/reference/macroregion_countries.tsv', delimiter='\t', key='alpha2')
    root_dir = ROOT_DIR if mode == 'entities' else SCHEDULED_DIR
    dirs = os.listdir(root_dir)
    for root, dirs, files in os.walk(root_dir):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
#            print('Processing %s' % (os.path.basename(filename).split('.', 1)[0]))
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            changed = False            
            country_ids = []
#            country_ids.append(record['owner']['location']['country']['id'])
            n = 0
            for location in record['coverage']:                 
                cid = location['location']['country']['id']
                if cid not in macro_dict.keys():
                    print(f'Not found country {cid}')
                else:
                    location['location']['macroregion'] = {'id' : macro_dict[cid]['macroregion_code'], 'name' : macro_dict[cid]['macroregion_name']}
                    record['coverage'][n] = location
                    changed = True
                n += 1                             
            if changed is True:
                f = open(filepath, 'w', encoding='utf8')
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                print('Updated %s' % (os.path.basename(filename).split('.', 1)[0]))


@app.command()
def update_languages(dryrun=False, mode='entities'):
    """Update languages schema and codes"""    
    lang_dict = load_csv_dict('../data/reference/langs.tsv', delimiter='\t', key='code')
    country_lang_dict = load_csv_dict('../data/reference/country_langs.tsv', delimiter='\t', key='alpha2')
    root_dir = ROOT_DIR if mode == 'entities' else SCHEDULED_DIR
    dirs = os.listdir(root_dir)
    for root, dirs, files in os.walk(root_dir):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            changed = False            
            cid = record['owner']['location']['country']['id']
            langs = []
            new_langs = []
            if 'langs' not in record.keys():
                if cid in country_lang_dict.keys():
                    langs.append(country_lang_dict[cid]['langcode'])
            else:
                if isinstance(record['langs'], dict):
                    continue
                if len(record['langs']) == 0:
                    if cid in country_lang_dict.keys():
                        langs.append(country_lang_dict[cid]['langcode'])
                else:
                    langs = record['langs']
            for code in langs:
                if code not in lang_dict.keys():
                    print(f'Not found language with code: {code}')
                    print(record['id'])
                else:
                    new_langs.append({'id' : code, 'name' : lang_dict[code]['name']})
                    record['langs'] = new_langs
                    changed = True
            n = 0
            if changed is True:
                f = open(filepath, 'w', encoding='utf8')
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                print('Updated %s' % (os.path.basename(filename).split('.', 1)[0]))

            
@app.command()
def update_subregions(dryrun=False, mode='entities'):
    """Update sub regions names"""    
    data_dict = load_csv_dict('../data/reference/subregions/IP2LOCATION-ISO3166-2.CSV ', delimiter=',', key='code')
    root_dir = ROOT_DIR if mode == 'entities' else SCHEDULED_DIR
    dirs = os.listdir(root_dir)
    for root, dirs, files in os.walk(root_dir):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
#            print('Processing %s' % (os.path.basename(filename).split('.', 1)[0]))
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            changed = False            
            country_ids = []
#            country_ids.append(record['owner']['location']['country']['id'])
            n = 0
            for location in record['coverage']:                 
                if not 'level' in location['location'].keys():                   
                    continue
                if location['location']['level'] != 2:
                    continue
                if 'subregion' in location['location'].keys():                
                    if 'name' in location['location']['subregion'].keys(): continue
                    sid = location['location']['subregion']['id']
                    if sid not in data_dict.keys():
                        print(f'Not found coverage subregion {sid}')
                    else:
                        location['location']['subregion']['name'] = data_dict[sid]['subdivision_name']
                        record['coverage'][n] = location
                        changed = True
                n += 1                
            if 'owner' in record.keys():
                if 'location' in record['owner'].keys():
                    if 'subregion' in record['owner']['location'].keys() and 'name' not in record['owner']['location']['subregion'].keys():
                        sid = record['owner']['location']['subregion']['id']
                        if sid not in data_dict.keys():
                             print(f'Not found owner subregion {sid}')
                        else:
                            record['owner']['location']['subregion']['name'] = data_dict[sid]['subdivision_name']
                            changed = True
            if changed is True:
                f = open(filepath, 'w', encoding='utf8')
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                print('Updated %s' % (os.path.basename(filename).split('.', 1)[0]))
            

@app.command()
def update_terms(dryrun=False, mode='entities'):
    """Update terms"""    
    software_dict = {}
    f = open('../data/datasets/software.jsonl', 'r', encoding='utf8')
    for l in f:
        record = json.loads(l)
        software_dict[record['id']] = record
    f.close()
    root_dir = ROOT_DIR if mode == 'entities' else SCHEDULED_DIR
    dirs = os.listdir(root_dir)
    for root, dirs, files in os.walk(root_dir):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
#            print('Processing %s' % (os.path.basename(filename).split('.', 1)[0]))
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close()
            changed = False          
            if record is None: continue
#            print(record['id'], record['software']['id'])  
            if record['software']['id'] not in software_dict.keys(): continue
            software = software_dict[record['software']['id']]            
            lic_type = software['rights_management']['licensing_type']            
            rights_type = None
            if lic_type == 'Global':
                rights_type = 'global'
            elif lic_type == 'Per dataset':
                rights_type = 'granular'
            elif lic_type == 'Not applicable':
                rights_type = 'inapplicable'
            else:
                rights_type = 'unknown'
            rights = {'tos_url' : None, 'privacy_policy_url' :None, 'rights_type' : rights_type, 'license_id' : None, 'license_name' : None, 'license_url' : None}
            if 'tos_url' in software['rights_management'].keys():
                rights['tos_url'] = software['rights_management']['tos_url']
            if 'privacy_policy_url' in software['rights_management'].keys():
                rights['privacy_policy_url'] = software['rights_management']['privacy_policy_url']
            if software['id'] == 'opendatasoft':
                rights['tos_url'] = record['link'] + '/terms/terms-and-conditions/'
                rights['privacy_policy_url'] = record['link'] + '/terms/privacy-policy/'
            properties = {} 
            if software['pid_support']['has_doi'] == 'No':
                properties['has_doi'] = False
                changed = True
            elif software['pid_support']['has_doi'] == 'Yes':
                properties['has_doi'] = True
                changed = True
            if len(properties.values()) > 0:
                if 'properties' in record.keys():
                    record['properties'].update(properties)                    
                else:
                    record['properties'] = properties
                changed = True
            record['rights'] = rights
            changed = True
            if changed is True:
                f = open(filepath, 'w', encoding='utf8')
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                print('Updated %s' % (os.path.basename(filename).split('.', 1)[0]))


if __name__ == "__main__":
    app()


