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
SOFTWARE_DIR = '../data/software'
DATASETS_DIR = '../data/datasets'

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


@app.command()
def build():
    """Build datasets as JSONL from entities as YAML"""
    print('Started building catalogs dataset')
    build_dataset(ROOT_DIR, 'catalogs.jsonl')
    print('Finished building catalogs dataset. File saved as %s' % (os.path.join(DATASETS_DIR, 'catalogs.jsonl')))
    print('Started building software dataset')
    build_dataset(SOFTWARE_DIR, 'software.jsonl')
    print('Finished building software dataset. File saved as %s' % (os.path.join(DATASETS_DIR, 'software.jsonl')))



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
        for k in ['api_status','catalog_type', 'id', 'link', 'name', 'software', 'status', 'api', 'catalog_export']:
            item[k] = record[k] if k in record.keys() else ''
        
        for k in ['type', 'link', 'name']:
            item['owner_' + k] = record['owner'][k] if k in record['owner'].keys() else ''
        item['owner_country_id'] = record['owner']['location']['country']['id']
    
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
    writer = csv.DictWriter(outfile, fieldnames=['id', 'link', 'name', 'owner_name', 'catalog_type', 'owner_type', 'software', 'langs', 'content_types', 'access_mode', 'owner_country_id', 'coverage_countries', 'tags', 'status', 'api', 'owner_link', 'catalog_export', 'api_status'], delimiter='\t')
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
            for country in record['coverage']:
                if country['name'] not in countries:
                    countries.append(country['name'])
        if 'software' in record.keys():
            if record['software'] not in software:
                software.append(record['software'])
    countries.sort()
    software.sort()
    matrix = {}
    for country in countries:
        matrix[country] = {'country' : country}
        for soft in software:
            matrix[country][soft] = 0
    for record in data:     
        if 'countries' in record.keys():
            for country in record['countries']:
                if 'software' in record.keys():
                        matrix[country['name']][record['software']] +=1
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



@app.command()
def assign(dryrun=False):
    """Assign unique identifier to each data catalog entry"""
    max_num = 0

    for root, dirs, files in os.walk(ROOT_DIR):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            print('Processing %s' % (os.path.basename(filename).split('.', 1)[0]))
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            if 'uid' in record.keys():
                num = int(record['uid'].split('cdi', 1)[-1])
                if num > max_num:
                    max_num = num
            f.close() 


    for root, dirs, files in os.walk(ROOT_DIR):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close() 
            if 'uid' not in record.keys():
                max_num += 1
                record['uid'] = f'cdi{max_num:08}'
                print('Wrote %s uid for %s' % (record['uid'], os.path.basename(filename).split('.', 1)[0]))
                f = open(filepath, 'w', encoding='utf8')
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()


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


            

if __name__ == "__main__":    
    app()