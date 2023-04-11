#!/usr/bin/env python
# This script intended to enrich data of catalogs entries 

import typer
import datetime
import requests
from requests.exceptions import ConnectionError
from urllib3.exceptions import InsecureRequestWarning
# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

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

ROOT_DIR = '../data/entities'
DATASETS_DIR = '../data/datasets'

app = typer.Typer()

def load_jsonl(filepath):
    data = []
    f = open(filepath, 'r', encoding='utf8')
    for l in f:
        data.append(json.loads(l))
    f.close()
    return data


@app.command()
def build():
    """Build datasets as JSONL from entities as YAML"""
    dirs = os.listdir(ROOT_DIR)
    out = open(os.path.join(DATASETS_DIR, 'catalogs.jsonl'), 'w', encoding='utf8')
    for adir in dirs:
        subdirs = os.listdir(os.path.join(ROOT_DIR, adir))
        for subdir in subdirs:
            print('Processing %s' % (subdir))
            files = os.listdir(os.path.join(ROOT_DIR, adir, subdir))
            for filename in files:                
                print('- adding %s' % (filename.split('.', 1)[0]))
                filepath = os.path.join(ROOT_DIR, adir, subdir, filename)
                if os.path.isdir(filepath): continue
                f = open(filepath, 'r', encoding='utf8')
                data = yaml.load(f, Loader=Loader)            
                f.close()
                
                out.write(json.dumps(data, ensure_ascii=False) + '\n')
    out.close()    
    print('Finished building catalogs dataset. File saved as %s' % (os.path.join(DATASETS_DIR, 'catalogs.jsonl')))


@app.command()
def report():
    """Report incomplete data per set"""
    data = load_jsonl(os.path.join(DATASETS_DIR, 'catalogs.jsonl'))
    typer.echo('')
    for d in data:     
        irep = []
        if 'name' not in d.keys():
            irep.append('no name')
        if 'countries' not in d.keys():
            irep.append('no countries')
        if 'tags' not in d.keys():
            irep.append('no tags')
        if 'software' not in d.keys():
            irep.append('no software')
        if 'langs' not in d.keys():
            irep.append('no langs')
        if 'owner_name' not in d.keys():
            irep.append('no owner name')
        if len(irep) > 0:
            print('%s / %s' % (d['id'], d['name'] if 'name' in d.keys() else ''))
            for r in irep:
                print('- %s' % (r))


@app.command()
def validate():
    """Validates YAML entities files against simple Cerberus schema"""
    from cerberus import Validator
    schema_file = os.path.join(DATASETS_DIR, '../schemes/catalog.json')
    f = open(schema_file, 'r', encoding='utf8')
    schema = json.load(f)
    f.close()
    records = load_jsonl(os.path.join(DATASETS_DIR, 'catalogs.jsonl'))
    typer.echo('Loaded %d data catalog records' % (len(records)))

    v = Validator(schema)
    for d in records:
        if d:
            try:
                r = v.validate(d, schema)
                if not r:
                    print('%s is not valid %s' % (d['id'], str(v.errors)))
            except Exception as e:
                print('%s error %s' % (d['id'], str(e)))


headers = {'Accept': 'application/json'}

@app.command()
def statusreport(updatedata=False):
    """Checks every site existence and endpoints availability except Geonetwork yet"""    
    session = requests.Session()
    session.max_redirects = 100    
    results = []
    records = load_jsonl(os.path.join(DATASETS_DIR, 'catalogs.jsonl'))
    typer.echo('Loaded %d data catalog records' % (len(records)))
    out = open('statusreport.jsonl', 'w', encoding='utf8')    
    for item in records:

        report = {'id' : item['id'], 'name' : item['name'] if 'name' in item.keys() else '', 'link' : item['link'], 'date_verified' : datetime.datetime.now().isoformat(), 'software' : item['software'] if 'software' in item.keys() else ''}
        report['api_status'] = 'uncertain'
        report['api_http_code'] = ''
        report['api_content_type'] = ''
        try:
            response = session.head(item['link'], verify=False, allow_redirects=True)
        except ConnectionError as e:
            report['status'] = 'deprecated'
            report['link_http_code'] = ''
            print(report)
            out.write(json.dumps(report) + '\n')
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
                        response = session.get(search_endpoint, headers=headers, verify=False)
                        report['api_http_code'] = response.status_code
                        report['api_content_type'] = response.headers['content-type']
                        if not response.ok:
                            report['api_status'] = 'deprecated'
                        else:
                            if response.headers['content-type'].split(';',1)[0].lower() == 'application/json':
                                report['api_status'] = 'active'
            elif item['software'] == 'NADA':
               if 'endpoints' in item.keys() and len(item['endpoints']) > 0:
                    if item['endpoints'][0]['type'] == 'nada:catalog-search':                
                        search_endpoint = item['endpoints'][0]['url']
                        response = session.get(search_endpoint, headers=headers, verify=False)
                        report['api_http_code'] = response.status_code
                        report['api_content_type'] = response.headers['content-type']
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
                        report['api_content_type'] = response.headers['content-type']
                        if not response.ok:
                            report['api_status'] = 'deprecated'
                        else:
                            if response.headers['content-type'].split(';',1)[0].lower() == 'application/json':
                                report['api_status'] = 'active'
            elif item['software'] == 'InvenioRDM':
               if 'endpoints' in item.keys() and len(item['endpoints']) > 0:
                    endpoints = {item['type']:item for item in item['endpoints']}
                    if 'inveniordmapi:records' in endpoints.keys():
                        search_endpoint = endpoints['inveniordmapi:records']['url']
                        response = session.get(search_endpoint, headers=headers, verify=False)
                        report['api_http_code'] = response.status_code
                        report['api_content_type'] = response.headers['content-type']
                        if not response.ok:
                            report['api_status'] = 'deprecated'
                        else:
                            if response.headers['content-type'].split(';',1)[0].lower() == 'application/json':
                                report['api_status'] = 'active'
            elif item['software'] == 'uData':
                search_endpoint = item['link'] + '/api/1/datasets/'
                response = session.get(search_endpoint, headers=headers, verify=False)
                report['api_http_code'] = response.status_code
                report['api_content_type'] = response.headers['content-type']
                if not response.ok:
                    report['api_status'] = 'deprecated'
                else:
                    if response.headers['content-type'].split(';',1)[0].lower() == 'application/json':
                        report['api_status'] = 'active'

        print(report)
        out.write(json.dumps(report) + '\n')

            

if __name__ == "__main__":    
    app()