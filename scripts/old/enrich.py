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


def enrichdata_old():
    """Already applied"""
    dirs = os.listdir(ROOT_DIR)
    for adir in dirs:
        subdirs = os.listdir(os.path.join(ROOT_DIR, adir))
        for subdir in subdirs:
            files = os.listdir(os.path.join(ROOT_DIR, adir, subdir))
            for filename in files:                
                filepath = os.path.join(ROOT_DIR, adir, subdir, filename)
                if os.path.isdir(filepath): continue
                f = open(filepath, 'r', encoding='utf8')
                data = yaml.load(f, Loader=Loader)            
                f.close()

                # Convert languages, export formats and countries to YAML arrays
                for key in ['langs', 'countries', 'export_formats']:
                    if key in data.keys():
                        data[key] = data[key].split(',')

                # Convert access modes and content types to array and lower values
                for key in ['access_mode', 'content_types']:
                    if key in data.keys():
                        data[key] = [word.lower() for word in data[key].split(',')] 
                
                # If catalog is geoportal, add content type map layer
                if data['catalog_type'] == 'Geoportal':
                    if 'map_layer' not in data['content_types']:
                        data['content_types'].append('map_layer')

                # Replace 'checked' for 'api' key to True, since it's boolean
                if 'api' in data.keys():
                    data['api'] = True

                # Add https if needed for links
                if data['link'][0:4] != 'http':
                    data['link'] = 'https://' + data['link']

                if 'software' in data.keys():
                    if data['software'] == 'ArcGIS Hub':
                        root_url = data['link'].rstrip('/')
                        endpoints = [{'type' : 'dcatap201', 'url' : root_url + '/api/feed/dcat-ap/2.0.1.json'}, 
                        {'type' : 'dcatus11', 'url' : root_url + '/api/feed/dcat-us/1.1.json'},
                        {'type' : 'rss',  'url' : root_url + '/api/feed/rss/2.0'}, 
                        {'type' : 'ogcrecordsapi', 'url' : root_url + '/api/search/v1'}]
                        data['endpoints'] = endpoints
                    elif data['software'] == 'Stac-server':
                        data['endpoints'] = [{'type' : 'stacserverapi', 'url': data['link']},]
                    elif data['software'] == 'CKAN':
                        if 'api_link' in data.keys():
                            api_url = data['api_link']
                        else:
                            root_url = data['link'].rstrip('/')
                            api_url = root_url + '/api/3'
                        data['endpoints'] = [{'type' : 'ckanapi', 'url': api_url},]                        
                    elif data['software'] == 'Dataverse':
                        if 'api_link' in data.keys():
                            api_url = data['api_link']
                        else:
                            root_url = data['link'].rstrip('/')
                            api_url = root_url + '/api'
                        data['endpoints'] = [{'type' : 'dataverseapi', 'url': api_url},]                        
                    elif data['software'] == 'uData':
                        if 'api_link' in data.keys():
                            api_url = data['api_link']
                        else:
                            root_url = data['link'].rstrip('/')
                            api_url = root_url + '/api/1'
                        data['endpoints'] = [{'type' : 'udataapi', 'url': api_url},]                        
                    elif data['software'] == 'OpenDataSoft':
                        if 'api_link' in data.keys():
                            api_url = data['api_link']
                        else:
                            root_url = data['link'].rstrip('/')
                            api_url = root_url + '/api'
                        data['endpoints'] = [{'type' : 'opendatasoftapi', 'url': api_url},]                        
                    elif data['software'] == 'Geonetwork':
                        data['endpoints'] = [{'type' : 'geonetworkapi:query', 'url' : data['link'] + '/srv/eng/q'}, {'type' : 'geonetworkapi', 'url' : data['link'] + '/srv/api'}]
                    elif data['software'] == 'InvenioRDM':
                        data['endpoints'] = [{'type' : 'inveniordmapi', 'url' : data['link'] + '/api'}, {'type' : 'inveniordmapi:records', 'url' : data['link'] + '/api/records'}]
                    elif data['software'] == 'NADA':
                        if data['link'].find('index.php') > -1:
                            api_root = data['link'].split('index.php', 1)[0] + 'index.php/api' 
                            data['endpoints'] = [{'type' : 'nada:catalog-search', 'url': api_root + '/catalog/search'},]
                    else:
                        if 'api_link' in data.keys():
                            data['endpoints'] = [{'type' : 'api', 'url': data['api_link']},]
#                            print(data)                            
                if 'endpoints' in data.keys():
                    data['api'] = True
                    if 'api_link' in data.keys():
                        del data['api_link']
                
                f = open(filepath, 'w', encoding='utf8')
                f.write(yaml.safe_dump(data, allow_unicode=True))
                f.close()
                print(json.dumps(data, indent=4))


def enrichdata_new():
    """Updates existing"""
    dirs = os.listdir(ROOT_DIR)
    for adir in dirs:
        subdirs = os.listdir(os.path.join(ROOT_DIR, adir))
        for subdir in subdirs:
            files = os.listdir(os.path.join(ROOT_DIR, adir, subdir))
            for filename in files:                
                filepath = os.path.join(ROOT_DIR, adir, subdir, filename)
                if os.path.isdir(filepath): continue
                f = open(filepath, 'r', encoding='utf8')
                data = yaml.load(f, Loader=Loader)            
                f.close()
                if 'software' in data.keys():
                    if data['software'] == 'NADA':
                        if data['link'].find('index.php') > -1:
                            api_root = data['link'].split('index.php', 1)[0] + 'index.php/api' 
                            data['endpoints'] = [{'type' : 'nada:catalog-search', 'url': api_root + '/catalog/search'},]
                        else:
                            api_root = data['link'].rstrip('/')
                            data['endpoints'] = [{'type' : 'nada:catalog-search', 'url': api_root + '/index.php/api/catalog/search'},]                            
                    else:
                        if 'api_link' in data.keys():
                            data['endpoints'] = [{'type' : 'api', 'url': data['api_link']},]
#                            print(data)                            
                if 'endpoints' in data.keys():
                    data['api'] = True
                    if 'api_link' in data.keys():
                        del data['api_link']
                
                f = open(filepath, 'w', encoding='utf8')
                f.write(yaml.safe_dump(data, allow_unicode=True))
                f.close()
                print(json.dumps(data, indent=4))



if __name__ == "__main__":
    typer.run(enrichdata_new)