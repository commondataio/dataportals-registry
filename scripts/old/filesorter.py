#!/usr/bin/env python
import typer
import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
import csv
import os
import shutil
import pprint

ROOT_DIR = '../data/datasets'

MAP_TYPES = {'Scientific data repository' : 'scientific', 'Open data portal' : 'opendata', 
'Microdata catalog' : 'microdata', 'Indicators catalog' : 'indicators', 
'Machine learning catalog' : 'ml', 'Geoportal' : 'geo',
'Data search engine' : 'search', 'API Catalog' : 'api', 'Data marketplace' : 'marketplace', 'Other' : 'other'
}


@app.command()
def bio;d():
    dirs = os.listdir(ROOT_DIR)
    for adir in dirs:
        files = os.listdir(os.path.join(ROOT_DIR, adir))
        for filename in files:
            filepath = os.path.join(ROOT_DIR, adir, filename)
            if os.path.isdir(filepath): continue
            f = open(filepath, 'r', encoding='utf8')
            data = yaml.load(f, Loader=Loader)            
            f.close()
            subdir = MAP_TYPES[data['catalog_type']]
            sub_path = os.path.join(os.path.join(ROOT_DIR, adir, subdir))
#            if not os.path.exists(sub_path):
#                os.makedirs(sub_path, exist_ok=True)
            new_path = os.path.join(sub_path, filename)
            if os.path.exists(new_path):
                os.remove(filepath)
            print(filename, "cleared")


if __name__ == "__main__":
    app()
    typer.run(copyfiles)