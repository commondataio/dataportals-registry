#!/usr/bin/env python
# This script intended to enrich data of catalogs entries 

import typer
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

ROOT_DIR = '../data/entities'

app = typer.Typer()

DATA_NAMES = ['data', 'dati', 'datos', 'dados', 'podatki', 'datosabiertos', 'opendata', 'data', 'dados abertos', 'daten', 'offendaten']
GOV_NAMES = ['gov', 'gob', 'gouv', 'egov', 'e-gov', 'go']


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

if __name__ == "__main__":
    app()