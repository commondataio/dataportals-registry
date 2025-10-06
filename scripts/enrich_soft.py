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


ROOT_DIR = '../data/software'
ENTRIES_DIR = '../data/entities'
app = typer.Typer()

DATA_NAMES = ['data', 'dati', 'datos', 'dados', 'podatki', 'datosabiertos', 'opendata', 'data', 'dados abertos', 'daten', 'offendaten']
GOV_NAMES = ['gov', 'gob', 'gouv', 'egov', 'e-gov', 'go', 'govt']


@app.command()
def create_from_csv(filename="../data/_original/software.csv"):
    """Create YAML files from original csv"""
    reader = csv.DictReader(open(filename, 'r', encoding='utf8'), delimiter='\t') 
    for row in reader:
        profile = {}
        for key in ['id', 'name','type','category','storage_type','has_api', 'has_bulk']:
            profile[key] = row[key]
        profile['datatypes'] = {}
        for key in ['datasets', 'organizations', 'topics', 'resources', 'fields']:
            profile['datatypes'][key] = row[key]

        profile['pid_support'] = {}
        for key in ['persistent_dataset_url', 'has_doi']:
            profile['pid_support'][key] = row[key]

        profile['rights_management'] = {}
        for key in ['supports_licenses', "licensing_type"]:
            profile['rights_management'][key] = row[key]

        profile['metadata_support'] = {}
        for key in ['custom_api', 'ckan_api', 'dcat', 'ogcrecords', 'oai-pmh', 'swordapi', 'sdmx', 'wfs', 'wms', 'wcs', 'csw', 'stac', 'opensearch', 'schema-org', 'openaire']:
            profile['metadata_support'][key] = row[key]

        filepath = os.path.join(ROOT_DIR, '%s.yaml' % (profile['id']))
        f = open(filepath, 'w', encoding='utf8')
        f.write(yaml.safe_dump(profile, allow_unicode=True))
        f.close()
        print(profile['id'] + ' written')




@app.command()
def update_software(dryrun=False):
    """Update software. No need to run twice, changes schema"""
    software = {}
    f = open('../data/datasets/software.jsonl', 'r', encoding='utf8')
    for l in f:
        record = json.loads(l)
        software[record['name']] = record
    f.close()

    dirs = os.listdir(ENTRIES_DIR)
    for root, dirs, files in os.walk(ENTRIES_DIR):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            print('Processing %s' % (os.path.basename(filename).split('.', 1)[0]))
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)            
            f.close() 
#            print(record)
#            record['software'] = {'id' : '', 'name' : record['software']}
            if record['software']['name'] in software.keys():
                record['software']['id'] = software[record['software']['name']]['id']                
#            print(record)
            f = open(filepath, 'w', encoding='utf8')
            f.write(yaml.safe_dump(record, allow_unicode=True))
            f.close()



if __name__ == "__main__":
    app()