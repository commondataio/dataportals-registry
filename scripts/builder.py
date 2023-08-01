#!/usr/bin/env python
# This script intended to enrich data of catalogs entries 

import copy
import typer
import datetime
from urllib.parse import urlparse
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

from constants import ENTRY_TEMPLATE, CUSTOM_SOFTWARE_KEYS, MAP_SOFTWARE_OWNER_CATALOG_TYPE, DOMAIN_LOCATIONS, DEFAULT_LOCATION, COUNTRIES_LANGS, MAP_CATALOG_TYPE_SUBDIR


ROOT_DIR = '../data/entities'
SCHEDULED_DIR = '../data/scheduled'
SOFTWARE_DIR = '../data/software'
DATASETS_DIR = '../data/datasets'
UNPROCESSED_DIR = '../data/_unprocessed'

app = typer.Typer()

def load_jsonl(filepath):
    data = []
    f = open(filepath, 'r', encoding='utf8')
    for l in f:
        data.append(json.loads(l))
    f.close()
    return data


def build_dataset(datapath, dataset_filename):
    out = open(os.path.join(DATASETS_DIR, dataset_filename), 'w', encoding='utf8')
    for root, dirs, files in os.walk(datapath):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            print('- adding %s' % (os.path.basename(filename).split('.', 1)[0]))
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            data = yaml.load(f, Loader=Loader)            
            f.close()
                
            out.write(json.dumps(data, ensure_ascii=False) + '\n')
    out.close()    

def merge_datasets(list_datasets, result_file):
    out = open(os.path.join(DATASETS_DIR, result_file), 'w', encoding='utf8')
    for filename in list_datasets:                
        print('- adding %s' % (os.path.basename(filename).split('.', 1)[0]))
        filepath = filename
        f = open(os.path.join(DATASETS_DIR, filepath), 'r', encoding='utf8')
        for line in f:
            out.write(line.rstrip() + '\n')
        f.close()
    out.close()    


@app.command()
def build():
    """Build datasets as JSONL from entities as YAML"""
    print('Started building software dataset')
    build_dataset(SOFTWARE_DIR, 'software.jsonl')
    print('Finished building software dataset. File saved as %s' % (os.path.join(DATASETS_DIR, 'software.jsonl')))
    print('Started building catalogs dataset')
    build_dataset(ROOT_DIR, 'catalogs.jsonl')
    print('Finished building catalogs dataset. File saved as %s' % (os.path.join(DATASETS_DIR, 'catalogs.jsonl')))
    print('Started building scheduled dataset')
    build_dataset(SCHEDULED_DIR, 'scheduled.jsonl')
    print('Finished building scheduled dataset. File saved as %s' % (os.path.join(DATASETS_DIR, 'scheduled.jsonl')))
    merge_datasets(['catalogs.jsonl', 'scheduled.jsonl'], 'full.jsonl')
    print('Merged datasets %s as %s' % (','.join(['catalogs.jsonl', 'scheduled.jsonl']), 'full.jsonl'))



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
def export(output='export.csv'):
    """Export to CSV"""
    data = load_jsonl(os.path.join(DATASETS_DIR, 'catalogs.jsonl'))
    typer.echo('')
    items = []
    for record in data:     
        item = {}
        for k in ['api_status','catalog_type', 'id', 'link', 'name', 'status', 'api', 'catalog_export']:
            item[k] = record[k] if k in record.keys() else ''
        
        for k in ['type', 'link', 'name']:
            item['owner_' + k] = record['owner'][k] if k in record['owner'].keys() else ''
        item['owner_country_id'] = record['owner']['location']['country']['id']
        item['software_id'] = record['software']['id']
    
        for k in ['access_mode', 'content_types', 'langs', 'tags']:
            item[k] = ','.join(record[k]) if k in record.keys() else ''
        
        countries_ids = []
        for location in record['coverage']:            
            cid = str(location['location']['country']['id'])
            if cid not in countries_ids: 
                countries_ids.append(cid)
        item['coverage_countries'] = ','.join(countries_ids)
        items.append(item)
    outfile = open(output, 'w', encoding='utf8')
    writer = csv.DictWriter(outfile, fieldnames=['id', 'link', 'name', 'owner_name', 'catalog_type', 'owner_type', 'software_id', 'langs', 'content_types', 'access_mode', 'owner_country_id', 'coverage_countries', 'tags', 'status', 'api', 'owner_link', 'catalog_export', 'api_status'], delimiter='\t')
    writer.writeheader()
    writer.writerows(items)
    outfile.close()
    typer.echo('Wrote %s' % (output))    


@app.command()
def stats(output='country_software.csv'):
    """Generates statistics tables"""
    data = load_jsonl(os.path.join(DATASETS_DIR, 'catalogs.jsonl'))
    typer.echo('')
    items = []
    countries = []
    software = []
    for record in data:     
        if 'coverage' in record.keys():
            for loc_rec in record['coverage']:
                country = loc_rec['location']['country']
                if country['name'] not in countries:
                    countries.append(country['name'])
        if 'software' in record.keys():
            if record['software']['name'] not in software:
                software.append(record['software']['name'])
    countries.sort()
    software.sort()
    matrix = {}
    for country in countries:
        matrix[country] = {'country' : country}
        for soft in software:
            matrix[country][soft] = 0
    for record in data:
        if 'coverage' in record.keys():
            for loc_rec in record['coverage']:
                country = loc_rec['location']['country']
                if 'software' in record.keys():
                    matrix[country['name']][record['software']['name']] +=1
    results = matrix.values()
    outfile = open(output, 'w', encoding='utf8')
    fieldnames = ['country',]
    fieldnames.extend(software)
    print(fieldnames)
    writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter='\t')
    writer.writeheader()
    writer.writerows(results)
    outfile.close()
    typer.echo('Wrote %s' % (output))    


def assign_by_dir(prefix='cdi', dirpath=ROOT_DIR):
    max_num = 0

    for root, dirs, files in os.walk(dirpath):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            print('Processing %s' % (os.path.basename(filename).split('.', 1)[0]))
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            if 'uid' in record.keys():
                num = int(record['uid'].split(prefix, 1)[-1])
                if num > max_num:
                    max_num = num
            f.close() 


    for root, dirs, files in os.walk(dirpath):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close() 
            if 'uid' not in record.keys():
                max_num += 1
                record['uid'] = f'{prefix}{max_num:08}'
                print('Wrote %s uid for %s' % (record['uid'], os.path.basename(filename).split('.', 1)[0]))
                f = open(filepath, 'w', encoding='utf8')
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()

@app.command()
def assign(dryrun=False, mode='entries'):
    """Assign unique identifier to each data catalog entry"""
    if mode == 'entries':
        assign_by_dir('cdi', ROOT_DIR)
    else:
        assign_by_dir('temp', SCHEDULED_DIR)

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
            r = v.validate(d, schema)
            try:
                r = v.validate(d, schema)
                if not r:
                    print('%s is not valid %s' % (d['id'], str(v.errors)))
            except Exception as e:
                print('%s error %s' % (d['id'], str(e)))

@app.command()
def add_legacy():
    """Adds all legacy catalogs"""

    scheduled_data = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl'))
    scheduled_list = []
    for row in scheduled_data:
        scheduled_list.append(row['id'])

    files = os.listdir(UNPROCESSED_DIR)
    for filename in files:
        if filename[-4:] != '.txt': continue
        software = filename[0:-4]
        f = open(os.path.join(UNPROCESSED_DIR, filename), 'r', encoding='utf8')
        for l in f:
            url = l.rstrip()
            _add_single_entry(url, software, preloaded=scheduled_list)
        f.close()

def _add_single_entry(url, software, preloaded=None):
    from apidetect import detect_single
    domain = urlparse(url).netloc.lower()
    record_id = domain.split(':', 1)[0].replace('_', '').replace('-', '').replace('.', '')

    if record_id in preloaded:
        print('URL %s already scheduled to be added' % (record_id))
        return

    software_data = load_jsonl(os.path.join(DATASETS_DIR, 'software.jsonl'))
    software_map = {}
    for row in software_data:
        software_map[row['id']] = row['name']

    record = copy.deepcopy(ENTRY_TEMPLATE)
    record['id']  = record_id

    postfix = domain.rsplit('.', 1)[-1].split(':', 1)[0]
    if postfix in DOMAIN_LOCATIONS.keys():
        location = DOMAIN_LOCATIONS[postfix]
    else:
        location = DEFAULT_LOCATION
    
    record['langs'] = []
    if postfix in COUNTRIES_LANGS.keys():
        record['langs'].append(COUNTRIES_LANGS[postfix])

    record['link'] = url
    record['name'] = domain
    record['coverage'].append(location)
    record['owner'].update(location)

    if software in MAP_SOFTWARE_OWNER_CATALOG_TYPE.keys():
        record['catalog_type'] = MAP_SOFTWARE_OWNER_CATALOG_TYPE[software]
    if record['catalog_type'] == 'Geoportal':
        record['content_types'].append('map_layer')
    if software in CUSTOM_SOFTWARE_KEYS:
        record['software'] = {'id' : 'custom', 'name' : 'Custom software'}
    elif software in software_map.keys():
        record['software'] = {'id' : software, 'name' : software_map[software]}
    else:
        record['software'] = {'id' : software, 'name' : software.title()}
    country_dir = os.path.join(SCHEDULED_DIR, location['location']['country']['id'])
    if not os.path.exists(country_dir):
        os.mkdir(country_dir)
    subdir_name = MAP_CATALOG_TYPE_SUBDIR[record['catalog_type']] if record['catalog_type'] in MAP_CATALOG_TYPE_SUBDIR.keys() else 'opendata'
    subdir_dir = os.path.join(country_dir, subdir_name)
    if not os.path.exists(subdir_dir):
        os.mkdir(subdir_dir)    
    filename = os.path.join(subdir_dir, record_id + '.yaml')
    f = open(filename, 'w', encoding='utf8')
    f.write(json.dumps(record, indent=4))
    f.close()
    print('%s saved' % (record_id))
    detect_single(record_id, dryrun=False, replace_endpoints=True, mode='scheduled')

@app.command()
def add_single(url, software='custom'):
    """Adds data catalog to the scheduled list"""

    full_data = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl'))
    full_list = []
    for row in full_data:
        full_list.append(row['id'])
    _add_single_entry(url, software, preloaded=full_list)


@app.command()
def add_list(filename, software='custom'):
    """Adds data catalog one by one from list to the scheduled list"""

    full_data = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl'))
    full_list = []
    for row in full_data:
        full_list.append(row['id'])
    f = open(filename, 'r', encoding='utf8')
    for line in f:
        line = line.strip()
        _add_single_entry(line, software, preloaded=full_list)
    f.close()


SOFTWARE_MD_TEMPLATE = """---
sidebar_position: 1
position: 1
---
# %s


Type: %s
Website: %s

## Schema types

## API Endpoints

## Notes
"""

SOFTWARE_DOCS_PATH = '../../cdi-docs/docs/kb/software'

SOFTWARE_PATH_MAP = {
'Open data portal' : 'opendata',
'Geoportal' : 'geo',
'Indicators catalog' : 'indicators',
'Metadata catalog' : 'metadata',
'Microdata catalog' : 'microdata',
'Scientific data repository' : 'scientific'
}

@app.command()
def build_docs(rewrite=True):
    """Generates docs stubs"""
    software_data = load_jsonl(os.path.join(DATASETS_DIR, 'software.jsonl'))
    for row in software_data:
        category = SOFTWARE_PATH_MAP[row['category']]
        filename = os.path.join(SOFTWARE_DOCS_PATH, category, row['id'] + '.md')        
        if os.path.exists(filename) and rewrite is False:
            print('Already exists %s' % row['id'])
        else:
            text = SOFTWARE_MD_TEMPLATE % (row['name'], row['category'], row['website'])
            f = open(filename, 'w', encoding='utf8')
            f.write(text)
            f.close()
            print('Wrote %s' % (row['id']))
        

if __name__ == "__main__":    
    app()