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




if __name__ == "__main__":    
    app()