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

from constants import ENTRY_TEMPLATE, CUSTOM_SOFTWARE_KEYS, MAP_SOFTWARE_OWNER_CATALOG_TYPE, DOMAIN_LOCATIONS, DEFAULT_LOCATION, COUNTRIES_LANGS, MAP_CATALOG_TYPE_SUBDIR, COUNTRIES


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


def split_by_software(filepath):
    os.makedirs('../data/datasets/bysoftware', exist_ok=True)
    os.system('undatum split -f software.id -d %s %s' % ('../data/datasets/bysoftware', os.path.join(DATASETS_DIR, filepath)))

def split_by_type(filepath):
    os.makedirs('../data/datasets/bytype', exist_ok=True)
    os.system('undatum split -f catalog_type -d %s %s' % ('../data/datasets/bytype', os.path.join(DATASETS_DIR, filepath)))



def build_dataset(datapath, dataset_filename):
    out = open(os.path.join(DATASETS_DIR, dataset_filename), 'w', encoding='utf8')
    n = 0
    for root, dirs, files in os.walk(datapath):       
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            n += 1
            if n % 1000 == 0: print('- processed %d' % (n))
#            print('- adding %s' % (os.path.basename(filename).split('.', 1)[0]))
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            data = yaml.load(f, Loader=Loader)            
            f.close()
                
            out.write(json.dumps(data, ensure_ascii=False) + '\n')
    print('- processed %d' % (n))
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
    print('Split by software')
    split_by_software('full.jsonl')
    print('Split by catalog type')
    split_by_type('full.jsonl')
    print('Building final parquet file %s' % (os.path.join(DATASETS_DIR, 'full.parquet')))
    os.system("duckdb -c \"copy '%s' to '%s'  (FORMAT 'parquet', COMPRESSION 'zstd');\"" % (os.path.join(DATASETS_DIR, 'full.jsonl'), os.path.join(DATASETS_DIR, 'full.parquet')))


@app.command()
def report():
    """Report incomplete data per set"""
    data = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl'))
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
    n = 0
    for root, dirs, files in os.walk(dirpath):
        files = [ os.path.join(root, fi) for fi in files if fi.endswith(".yaml") ]
        for filename in files:                
            print(filename)
            n += 1
            if n % 1000 == 0: print('Processed %d' % (n))
            filepath = filename
            f = open(filepath, 'r', encoding='utf8')
            record = yaml.load(f, Loader=Loader)                         
            if 'uid' in record.keys():
                num = int(record['uid'].split(prefix, 1)[-1])
                if num > max_num:
                    max_num = num
            f.close() 
    print('Processed %d' % (n))
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
    records = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl'))
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
def validate_typing():
    """Validates YAML entities files against pydantic model"""
    from cerberus import Validator
    records = load_jsonl(os.path.join(DATASETS_DIR, 'catalogs.jsonl'))
    typer.echo('Loaded %d data catalog records' % (len(records)))
    from cdiapi.data.datacatalog import DataCatalog
 

    for d in records:
        print('Validating %s' % (d['id']))
        try:
            entry = DataCatalog.parse_obj(d)
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

def _add_single_entry(url, software, catalog_type="Open data portal", name=None, description=None, lang=None, country=None, owner_name=None, owner_link=None, owner_type=None, scheduled=True, force=False, preloaded=None):
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

    postfix = None
    has_location = False
    if country is not None:
        if country in COUNTRIES.keys():
            location = {'location' : {'country' : {'id' : country, 'name' : COUNTRIES[country]}}}
            has_location = True
        
    if not has_location:
        postfix = domain.rsplit('.', 1)[-1].split(':', 1)[0]
        if postfix in DOMAIN_LOCATIONS.keys():
            location = DOMAIN_LOCATIONS[postfix]
        else:
            location = DEFAULT_LOCATION
    
    record['langs'] = []
    if lang:
        record['langs'].append(lang)
    if has_location and postfix in COUNTRIES_LANGS.keys():
        record['langs'].append(COUNTRIES_LANGS[postfix])

    record['link'] = url
    record['name'] = domain if name is None else name
    if description is not None:
        record['description'] = description

    record['coverage'].append(copy.deepcopy(location))
    record['owner'].update(copy.deepcopy(location))
    if owner_name is not None:
        record['owner']['name'] = owner_name
    if owner_link is not None:
        record['owner']['link'] = owner_link
    if owner_type is not None:
        record['owner']['type'] = owner_type
    

    if software in MAP_SOFTWARE_OWNER_CATALOG_TYPE.keys():
        record['catalog_type'] = MAP_SOFTWARE_OWNER_CATALOG_TYPE[software]
    else:
        record['catalog_type'] = catalog_type
    if record['catalog_type'] == 'Geoportal':
        record['content_types'].append('map_layer')
    if software in CUSTOM_SOFTWARE_KEYS:
        record['software'] = {'id' : 'custom', 'name' : 'Custom software'}
    elif software in software_map.keys():
        record['software'] = {'id' : software, 'name' : software_map[software]}
    else:
        record['software'] = {'id' : software, 'name' : software.title()}
    root_dir = SCHEDULED_DIR if scheduled else ROOT_DIR
    country_dir = os.path.join(root_dir, location['location']['country']['id'])
    if not os.path.exists(country_dir):
        os.mkdir(country_dir)
    subdir_name = MAP_CATALOG_TYPE_SUBDIR[record['catalog_type']] if record['catalog_type'] in MAP_CATALOG_TYPE_SUBDIR.keys() else 'opendata'
    subdir_dir = os.path.join(country_dir, subdir_name)
    if not os.path.exists(subdir_dir):
        os.mkdir(subdir_dir)    
    filename = os.path.join(subdir_dir, record_id + '.yaml')
    if os.path.exists(filename) and force:
        print('Already processed and force not set')
    else:
        f = open(filename, 'w', encoding='utf8')
#        print(record)
        f.write(yaml.safe_dump(record, allow_unicode=True))
        f.close()
        print('%s saved' % (record_id))
        detect_single(record_id, dryrun=False, replace_endpoints=True, mode='scheduled' if scheduled else 'entries')

@app.command()
def add_single(url, software='custom', catalog_type="Open data portal", name=None, description=None, lang=None, country=None, owner_name=None, owner_link=None, owner_type=None, force=False, scheduled=True):
    """Adds data catalog to the scheduled list"""

    full_data = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl'))
    full_list = []
    for row in full_data:
        full_list.append(row['id'])
    _add_single_entry(url, software, name=name, description=description, lang=lang, country=country, owner_name=owner_name, owner_link=owner_link, owner_type=owner_type, scheduled=scheduled, force=force, preloaded=full_list)


@app.command()
def add_list(filename, software='custom', catalog_type="Open data portal", name=None, description=None, lang=None, country=None, owner_name=None, owner_link=None, owner_type=None):
    """Adds data catalog one by one from list to the scheduled list"""
    if not os.path.exists(filename):
        print('File %s not exists' % (filename))
        return
    full_data = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl'))
    full_list = []
    for row in full_data:
        full_list.append(row['id'])
    f = open(filename, 'r', encoding='utf8')
    for line in f:
        line = line.strip()
        _add_single_entry(line, software, catalog_type=catalog_type, name=name, description=description, lang=lang, country=country, owner_name=owner_name, owner_link=owner_link, owner_type=owner_type, preloaded=full_list)
    f.close()

@app.command()
def add_opendatasoft_catalog(filename):
    """Adds OpenDataSoft prepared data catalogs list"""
    full_data = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl'))
    full_list = []
    for row in full_data:
        full_list.append(row['id'])
    ods_data = load_jsonl(filename)
    for item in ods_data:
        lang = item['lang'].rsplit('/', 1)[-1].upper()
        _add_single_entry(item['website'], software="opendatasoft", name=item['title'], description=item['description'], lang=lang, preloaded=full_list)

@app.command()
def add_socrata_catalog(filename):
    """Adds Socrata prepared data catalogs list"""
    full_data = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl'))
    full_list = []
    for row in full_data:
        full_list.append(row['id'])
    ods_data = load_jsonl(filename)
    for item in ods_data:
        lang = item['locale'].rsplit('/', 1)[-1].upper()
        _add_single_entry(item['website'], software="socrata", name=item['title'], lang=lang, preloaded=full_list)


@app.command()
def add_arcgishub_catalog(filename, force=False):
    """Adds ArcGIS Hub prepared data catalogs list"""
    full_data = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl'))
    full_list = []
    for row in full_data:
        full_list.append(row['id'])
    ods_data = load_jsonl(filename)
    for item in ods_data:
        if item['culture'] is not None:
            lang = item['culture'].rsplit('-', 1)[0].upper() if item['culture'] != 'zh-TW' else item['culture']
        else:
            lang = 'EN'
        country = item['region']
        if country == 'WO': country = 'US'       
        _add_single_entry(item['website'], software="arcgishub", name=item['title'], description=item['description'], lang=lang, owner_name = item['owner_name'], country=country, force=force, scheduled=False, preloaded=full_list)




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
        


@app.command()
def get_countries():
    """Generate countries code list"""
    ids = []
    full_data = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl'))
    text = "COUNTRIES = { "
    for row in full_data:
        for loc in row['coverage']:
            id = loc['location']['country']['id']
            name= loc['location']['country']['name']
            if id not in ids: 
                ids.append(id)
                text += '"%s" : "%s",\n' % (id, name)
            else:
                continue
    text += "}"
    print(text)
   


METRICS = {'has_owner_name' : 'Has owner organization name', 
'has_owner_type' : "Has owner organization type",
'has_owner_link' : "Has owner organization link",
'has_catalog_type' : "Has catalog type",
'has_country' : 'Owner country known',
'has_coverage' : 'Coverage known',
'has_macroregion' : 'Macroregion information known',
'has_subregion' : 'Subregion information known',
"has_description" : "Has description",
"has_langs" : 'Has languages',
'has_tags' : 'Has tags',
'has_topics' : "Has topics",
'has_endpoints' : "Has endpoints",
'valid_title' : 'Title is not empty or temporary',
'perm_records' : "Permanent records",
}

@app.command()
def quality_control(mode='full'):
    """Quality control metrics"""
    from rich.console import Console
    from rich.table import Table
    data = load_jsonl(os.path.join(DATASETS_DIR, f'{mode}.jsonl'))
    metrics = {}
    for key in METRICS.keys():
        metrics[key] = [key, METRICS[key], 0, 0, 0]
    total = 0
    for d in data:     
        total += 1
        if 'coverage' in d.keys() and len(d['coverage']) > 0:
            metrics['has_coverage'][3] += 1
            if 'location' in d['coverage'][0].keys():
                location = d['coverage'][0]['location']
                if 'macroregion' in location.keys():
                    metrics['has_macroregion'][3] += 1
        if 'langs' in d.keys() and len(d['langs']) > 0:
            metrics['has_langs'][3] += 1
        if 'tags' in d.keys() and len(d['tags']) > 0:
            metrics['has_tags'][3] += 1
        if 'topics' in d.keys() and len(d['topics']) > 0:
            metrics['has_topics'][3] += 1
        if 'endpoints' in d.keys() and len(d['endpoints']) > 0:
            metrics['has_endpoints'][3] += 1
        if 'status' in d.keys() and d['status'] == 'active':
            metrics['perm_records'][3] += 1
        if 'catalog_type' in d.keys() and d['catalog_type'] not in [None, 'Unknown']:
            metrics['has_catalog_type'][3] += 1
        if 'description' in d.keys() and d['description'] != 'This is a temporary record with some data collected but it should be updated befor adding to the index':
            metrics['has_description'][3] += 1
        if 'name' in d.keys():
            if not d['name'].lower() == urlparse(d['link']).netloc.lower():
                metrics['valid_title'][3] += 1 
        if 'owner' in d.keys():
            if 'type' in d['owner'].keys() and d['owner']['type'] != 'Unknown':
                metrics['has_owner_type'][3] += 1
            if 'link' in d['owner'].keys() and d['owner']['link'] is not None and len(d['owner']['link']) > 0:
                metrics['has_owner_link'][3] += 1
            if 'name' in d['owner'].keys() and d['owner']['name'] is not None and len(d['owner']['name']) > 0 and d['owner']['name'] != 'Unknown':
                metrics['has_owner_name'][3] += 1
            if 'location' in d['owner'].keys() and d['owner']['location'] is not None and 'country' in d['owner']['location'].keys() and d['owner']['location']['country']['id'] != 'Unknown': 
                metrics['has_country'][3] += 1
            if d['owner']['type'] in ['Regional government', 'Local government', 'Unknown']:
                metrics['has_subregion'][2] += 1
                if 'location' in d['owner'].keys() and d['owner']['location'] is not None and 'subregion' in d['owner']['location'].keys(): 
                    metrics['has_subregion'][3] += 1

        for key in ['has_tags', 'has_langs', 'has_topics', 'has_endpoints', 'has_description', 'perm_records', 'has_owner_link', 'has_owner_type', 'has_owner_name', 'valid_title', 'has_country', 'has_catalog_type', 'has_coverage', 'has_macroregion']:
            metrics[key][2] += 1
#    for metric in metrics.values(): 
#        print('%s, total %d, found %d, share %0.2f' % (metric[1], metric[2], metric[3], metric[3]*100.0 / metric[2] if metric[2] > 0 else 0))
    table = Table(title='Common Data Index registry. Metadata quality metrics') 
    table.add_column("Metric name", justify="right", style="cyan", no_wrap=True)
    table.add_column("Total", style="magenta")
    table.add_column("Count", style="magenta")
    table.add_column("Share", justify="right", style="green", no_wrap=True)
    for metric in metrics.values(): 
        item = []
        for o in metric[1:-1]: item.append(str(o))
        item.append('%0.2f' % (metric[3]*100.0 / metric[2] if metric[2] > 0 else 0))
        table.add_row(*item)
    table.add_section()
    table.add_row('Total', str(total))
    console = Console()
    console.print(table)  

  
@app.command()
def country_report():
    """Country report"""
    import duckdb
    from rich.console import Console
    from rich.table import Table
#    data = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl')) 
#    ids = duckdb.sql("select distinct(unnest(coverage).location.country.id) as id from '%s';" % (os.path.join(DATASETS_DIR, 'full.parquet'))).df().id.tolist()
    ids = duckdb.sql("select distinct(unnest(source.countries).id) as id from '%s';" % (os.path.join("../../cdi-data/search", 'dateno.parquet'))).df().id.tolist()
#    print(ids)
    reg_countries = set(ids)
    countries_data = {}
    tlds_data = {}
#    for row in data:
#        if 'owner' in row.keys() and 'location' in row['owner'].keys():
#            if row['owner']['location']['country']['id'] not in reg_countries: 
#                reg_countries.add(row['owner']['location']['country']['id'])
#        if 'coverage' in row.keys():
#            for loc in row['coverage']:
#                if loc['location']['country']['id'] not in reg_countries: 
#                    reg_countries.add(loc['location']['country']['id'])

    f = open('../data/reference/countries.csv', 'r', encoding='utf8')
    reader = csv.DictReader(f)
    for row in reader:
        if row['status'] == 'UN member state':
            countries_data[row['alpha2']] = row
    wb_countries = set(countries_data.keys())
    all_set = wb_countries.difference(reg_countries)
#    all_set = wb_countries.intersection(reg_countries)
    table = Table(title='Missing countries report') 
    table.add_column("Alpha-2", justify="right", style="cyan", no_wrap=True)
    table.add_column("Name", style="magenta")
    table.add_column("Internet TLD", style="magenta")
    n = 0
    for row_id in all_set:
        item = [row_id, countries_data[row_id]['name'], countries_data[row_id]['cctld']]
#        item = [row_id, row_id]
        table.add_row(*item)
        n += 1
    table.add_section()
    table.add_row('Total', str(n))
    console = Console()
    console.print(table)  
    f = open('countries_report', 'w', encoding='utf8')
    writer = csv.writer(f)
    writer.writerow(['code', 'name', 'tld'])
    for row_id in all_set:
        item = [row_id, countries_data[row_id]['name'], countries_data[row_id]['cctld']]
        writer.writerow(item)
    f.close()


    


if __name__ == "__main__":    
    app()