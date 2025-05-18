#!/usr/bin/env python
# This script intended to enrich data of catalogs entries 

import typer
import requests
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:                                                                                                     
    from yaml import Loader, Dumper
import csv
import tqdm
import json
import os
import yaml

from glom import glom



ROOT_DIR = '../data/entities'
DATASETS_DIR = '../data/datasets'
SCHEDULED_DIR = '../data/scheduled'
ENRICHED_DIR = '../data/enriched'
app = typer.Typer()

from prompts import get_profile

def load_csv_dict(filepath, key, delimiter='\t'):
    data = {}
    f = open(filepath, 'r', encoding='utf8')
    reader = csv.DictReader(f, delimiter=delimiter)
    for r in reader:
        data[r[key]] = r
    f.close()
    return data

def load_jsonl_dict(filepath, key):
    data = {}
    f = open(filepath, 'r', encoding='utf8')
    for line in f:
        r = json.loads(line)
        data[r[key]] = r
    f.close()
    return data


def load_yaml_dict(filepath, key):
    data = {}
    f = open(filepath, 'r', encoding='utf8')
    item = yaml.load(f, Loader=Loader)
    for r in item:
        data[r[key]] = r
    f.close()
    return data


@app.command()
def add_structured(dryrun=False):
    """Update profiles using AI agent"""    
    from iterable.helpers.detect import open_iterable
    it = open_iterable(DATASETS_DIR + '/full.jsonl')
    for row in tqdm.tqdm(it):
        filename = ENRICHED_DIR + '/records/' + row['uid'] + '.json'
        if os.path.exists(filename): continue
        raw = get_profile(row['link'])
        data = json.loads(raw)
        data['uid'] = row['uid']
        data['id'] = row['id']

        f = open(filename, 'w', encoding='utf8')
        f.write(json.dumps(data))
        f.close()

@app.command()
def merge(dryrun=False):
    """Prepare dataset for future OpenRefine usage"""    
    from iterable.helpers.detect import open_iterable
    it = open_iterable(DATASETS_DIR + '/full.jsonl')
    writer = None
    outj = open_iterable('../data/enriched/_merged.jsonl', mode='w')
    for row in tqdm.tqdm(it):
        filename = ENRICHED_DIR + '/records/' + row['uid'] + '.json'
        if os.path.exists(filename): 
            f = open(filename, 'r', encoding='utf8')
            data = json.load(f)
            f.close()
        merged = {'uid' : row['uid'], 'id' : row['id'], 'name' : row['name'], 'name_ai' : data['name'], 
            'link' : row['link'], 'description' : glom(row, 'description', default=''), 'description_ai' : data['description'], 
            'owner_name' : row['owner']['name'], 'owner_name_ai' : data['owner_name'], 'owner_link' : glom(row, 'owner.link',default=''), 'owner_link_ai' : data['owner_website'],
            'owner_type' : row['owner']['type'], 'owner_type_ai' : data['owner_type'],
            'owner_country_iso' : glom(row, 'owner.location.country.id', default=''), 'owner_country_name' : glom(row, 'owner.location.country.name', default=''),
            'owner_country_iso_ai' : data['owner_country_iso2'], 'owner_country_name_ai' : data['owner_country'], 
            'owner_subregion_id' : glom(row, 'owner.location.subregion.id', default=''), 'owner_subregion_name' : glom(row, 'owner.location.subregion.name', default=''), 
            'owner_subregion_id_ai' : data['owner_subregion_iso3166_2'], 'owner_subregion_name_ai' : data['owner_subregion_name'], 
            }
        outj.write(merged)
        if writer is None:
            f = open('../data/enriched/_merged.csv', 'w', encoding='utf8')
            writer = csv.DictWriter(f, fieldnames = merged.keys())
            writer.writeheader()
            writer.writerow(merged)
        else:
            writer.writerow(merged)        


@app.command()
def writetopics(key:str='data_themes'):
    """Write topics"""    
    from collections import Counter
    it = os.listdir('../data/enriched/records')
    output = open(f'../data/enriched/_{key}.csv', 'w', encoding='utf8')
    items = []
    for name in tqdm.tqdm(it):
        filename = ENRICHED_DIR + '/records/' + name
        f = open(filename, 'r', encoding='utf8')
        data = json.load(f)
        f.close()
        if data[key] is None: continue        
        for item in data[key]:
            if isinstance(item, str):
                items.append(item)
    
    c = Counter(items)
    my_list = sorted(set(items))
    output.write('name\tcounter\n')
    for item in c.items():
        output.write(f'{item[0]}\t{item[1]}\n')
    output.close()

@app.command()
def update_ai_enriched(dryrun:bool=True, mode:str='entities'):
    """Update sub regions names"""    
    from pycountry import countries 
    ip2_data_dict = load_csv_dict('../data/reference/subregions/IP2LOCATION-ISO3166-2.CSV ', delimiter=',', key='code')
#    merged_data_dict = load_jsonl_dict('../data/enriched/_merged.jsonl ', key='uid')
    data_themes = load_yaml_dict('../data/reference/data_themes.yaml', key='id')
    data_themes_data_dict = load_csv_dict('../data/enriched/_data_themes_fixed.csv ', key='name', delimiter=',')
    geotopics_data_dict = load_csv_dict('../data/enriched/_geotopics_fixed.csv ', key='name', delimiter=',')
    countries_data_dict = load_csv_dict('../data/enriched/_countries_fixed.csv ', key='name', delimiter=',')
    subregions_data_dict = load_csv_dict('../data/enriched/_subregions_fixed.csv ', key='name', delimiter=',')
    macroregion_dict = load_csv_dict('../data/reference/macroregion_countries.tsv', delimiter='\t', key='alpha2')
    
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

            ai_filename = ENRICHED_DIR + '/records/' + record['uid'] + '.json'
            if not os.path.exists(ai_filename): continue
            f = open(ai_filename, 'r', encoding='utf8')
            ai_data = json.load(f)
            f.close()            
            changed = False            
            
            # Add tags section
            has_tags = 'tags' in record.keys() and len(record['tags']) > 0
            tags = record['tags'] if 'tags' in record.keys() else []
            ai_tags = ai_data['tags']
            if ai_tags is not None and len(ai_tags) > 0: 
                for tag in ai_tags:
                    if tag not in tags:
                        tags.append(tag)
                record['tags'] = tags
                changed = True

            # Update name if it's longer and not an URL
            if ai_data['name'] is not None:
                if len(record['name']) < len(ai_data['name']) and ai_data['name'][0:4] != 'http':
                    record['name'] = ai_data['name']
                    changed = True

            # Update description if it's longer or if original is default            
            if ai_data['description'] is not None:
                if 'description' not in record.keys() or record['description'] is None:
                    record['description']  = ai_data['description']
                    changed = True
                elif record['description'][0:19] == 'This is a temporary':
                    record['description']  = ai_data['description']
                    changed = True
                elif len(record['description']) < len(ai_data['description']):
                    record['description']  = ai_data['description']
                    changed = True

            # Update catalog owner name
            if len(record['owner']['name']) == 0 and ai_data['owner_name'] is not None and len(ai_data['owner_name']) > 0:
                record['owner']['name'] = ai_data['owner_name']
                changed = True

            # Update catalog owner website
            if 'link' not in record['owner'].keys():
                if ai_data['owner_website'] is not None and len(ai_data['owner_website']) > 0:
                    record['owner']['link'] = ai_data['owner_website']
                    changed = True            
            elif (record['owner']['link'] is None or len(record['owner']['link']) == 0) and ai_data['owner_website'] is not None and len(ai_data['owner_website']) > 0:
                record['owner']['link'] = ai_data['owner_website']
                changed = True

                
            # Update catalog type
            if record['owner']['type'] == 'Unknown' and ai_data['owner_type'] is not None and len(ai_data['owner_type']) > 0:
                record['owner']['type'] = ai_data['owner_type']
                changed = True


            # Update owner country and subregion
            if 'owner' in record.keys():
                if 'location' in record['owner'].keys():
                    if 'country' in record['owner']['location'].keys():
                        sid = record['owner']['location']['country']['id']
                        if sid == 'Unknown':
                            if ai_data['owner_country_iso2'] is not None and len(ai_data['owner_country_iso2']) > 0:
                                if len(ai_data['owner_country_iso2'].strip()) > 0:
                                    sid = countries_data_dict[ai_data['owner_country_iso2']]['fixed']
                                    country = countries.get(alpha_2=sid)
                                    if country is not None:
                                        record['owner']['location']['country'] = {'id' : countries_data_dict[ai_data['owner_country_iso2']]['fixed'], 'name': country.name}
                                        changed = True
#                                else:
#                                    print(ai_data['owner_country_iso2'])

                    if 'subregion' not in record['owner']['location'].keys() and ai_data['owner_subregion_iso3166_2'] is not None and len(ai_data['owner_subregion_iso3166_2']) > 0:
                        if len(ai_data['owner_subregion_iso3166_2'].strip()) > 0:
                            sid = subregions_data_dict[ai_data['owner_subregion_iso3166_2']]['fixed']  
                            if sid in ip2_data_dict.keys():
                                record['owner']['location']['subregion'] = {'id' : sid, 'name': ip2_data_dict[sid]['subdivision_name']}
                                record['owner']['location']['level'] = 30
                                changed = True
#                            print(record['owner']['location']['subregion'])


            # Update coverage country and subregion
            if 'coverage' in record.keys():
                if record['coverage'] is not None and len(record['coverage']) > 0:
                    location = record['coverage'][0]
                    if 'country' in location['location'].keys():
                        sid = location['location']['country']['id']
                        if sid == 'Unknown':
                            if ai_data['owner_country_iso2'] is not None and len(ai_data['owner_country_iso2'].strip()) > 0:
                                sid = countries_data_dict[ai_data['owner_country_iso2']]['fixed']
                                if len(sid.strip()) > 0:
                                    country = countries.get(alpha_2=sid)
                                    if country is not None:
                                        location['location']['country'] = {'id' : sid, 'name': country.name}
                                        if sid in macroregion_dict.keys():
                                            location['location']['macroregion'] = {'id' : macroregion_dict[sid]['macroregion_code'], 'name' : macroregion_dict[sid]['macroregion_name']}
                                        record['coverage'][0] = location
                                        changed = True
                    if 'subregion' not in location['location'].keys() and ai_data['owner_subregion_iso3166_2'] is not None and len(ai_data['owner_subregion_iso3166_2'].strip()) > 0:
                        sid = subregions_data_dict[ai_data['owner_subregion_iso3166_2']]['fixed']
                        if sid in ip2_data_dict.keys():
                            location['location']['subregion'] = {'id' : sid, 'name': ip2_data_dict[sid]['subdivision_name']}
                            location['location']['level'] = 30
                            record['coverage'][0] = location
                            changed = True
                            print(record['coverage'])


            else:
                if ai_data['owner_country_iso2'] is not None and len(ai_data['owner_country_iso2']) > 0:
                    country = countries.get(alpha_2=ai_data['owner_country_iso2'])
                    if country is not None:
                        record['coverage'] = [{'location' : {'level' : 20, 'country' :{'id' : ai_data['owner_country_iso2'], 'name': country.name}}}]
                        changed = True


            
            # Update topics and geotopics
            topics = record['topics'] if 'topics' in record.keys() else []
            topics_keys = []
            for k in topics:
                topics_keys.append(k['id'])
#            print(ai_data['data_themes'])
            if ai_data['data_themes'] is not None and len(ai_data['data_themes']) > 0 and isinstance(ai_data['data_themes'][0], str):
                for topic in ai_data['data_themes']:
                    if topic in data_themes_data_dict.keys():
                        fixed = data_themes_data_dict[topic]['fixed']
                        for k in fixed.split(','):
                            k = k.strip()
                            if k in topics_keys: continue
                            if len(k) == 0 : continue
                            topics.append({'id' : k, 'name' : data_themes[k]['name'], 'type' : 'eudatatheme'})
                            changed = True

            if ai_data['geotopics'] is not None and len(ai_data['geotopics']) > 0 and isinstance(ai_data['geotopics'][0], str):
                for topic in ai_data['geotopics']:
                    if topic in data_themes_data_dict.keys():
                        fixed = geotopics_data_dict[topic]['fixed']
                        for k in fixed.split(','):
                            k = k.strip()
                            if k in topics_keys: continue
                            if len(k) == 0 : continue
                            topics.append({'id' : k, 'name' : k, 'type' : 'iso19115'})
                            changed = True
                          
#            print(yaml.dump(record))

            if changed is True and not dryrun:
                f = open(filepath, 'w', encoding='utf8')
                f.write(yaml.safe_dump(record, allow_unicode=True))
                f.close()
                print('Updated %s' % (os.path.basename(filename).split('.', 1)[0]))




if __name__ == "__main__":
    app()


