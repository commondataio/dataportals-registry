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



if __name__ == "__main__":    
    app()