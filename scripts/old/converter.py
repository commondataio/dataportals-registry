#!/usr/bin/env python
import typer
import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
import csv
import os

import pprint
DATA_FILE = '../data/entities.yaml'
headers = ['id', 'name', 'doc', 'is_pii', 'langs', 'contexts',
           'translations.ru.name', 'translations.ru.doc', 'links.url', 'links.type']


def tocsv():
    fout = open('entities.csv', 'w', encoding='utf8')
    f = open(DATA_FILE, 'r', encoding='utf8')
    data = yaml.load(f, Loader=Loader)
    keys = list(data.keys())
    keys.sort()
    print('\t'.join(headers))
    writer = csv.writer(fout, delimiter='\t')
    writer.writerow(headers)
    for key in keys:
        record = [key, data[key]['name'], data[key]['doc'], str(data[key]['is_pii']),','.join(data[key]['langs']), ','.join(data[key]['contexts']),
                  data[key]['translations']['ru']['name'], data[key]['translations']['ru']['doc'],
                  data[key]['links'][0]['url'] if len(data[key]['links']) > 0 else '',
                  data[key]['links'][0]['type'] if len(data[key]['links']) > 0 else '',
                  ]
        writer.writerow(record)
        print('\t'.join(record))
    pass


def dict_csv_to_yaml(infile, outfile):
    out = open(outfile, 'w', encoding='utf8')
    f = open(infile, 'r', encoding='utf8')
    reader = csv.reader(f, delimiter='\t')
    data = []
    for row in reader:
        data.append({'id' : row[0], 'name' : row[1]})
    out.write(yaml.dump(data))
    out.close()

def csv_to_yaml(infile, outpath, schema=[], splittable=[], removable=[]):
    f = open(infile, 'r', encoding='utf8')
    reader = csv.reader(f, delimiter=',')
    data = {}
    for row in reader:
        record = {}
        for n in range(0, len(row)):
            if schema[n] in removable: continue
            if len(row[n]) > 0:
                if schema[n].find('.') > -1:
                    parts = schema[n].split('.')
                    if len(parts) == 2:
                        if parts[0] not in record.keys():
                            record[parts[0]] = {}
                        record[parts[0]][parts[1]] = row[n]
                    elif len(parts) == 3:
                        if parts[0] not in record.keys():
                            record[parts[0]] = {}
                        if parts[1] not in record[parts[0]].keys():
                            record[parts[0]][parts[1]] = {}
                        record[parts[0]][parts[1]][parts[2]] = row[n]
                else:
                    record[schema[n]] = row[n] if schema[n] not in splittable else [x.strip() for x in row[n].split(',')]
#        data[record['id']] = record
        outfile = os.path.join(outpath, '%s.yaml' % (record['id']))
        out = open(outfile, 'w', encoding='utf8')
        out.write(yaml.safe_dump(record, allow_unicode=True))
        out.close()


def from_csv():
#     dict_csv_to_yaml('languages.csv', '../data/langs.yaml')
#    dict_csv_to_yaml('categories.csv', '../data/categories.yaml')
#    dict_csv_to_yaml('countries.csv', '../data/countries.yaml')
    csv_to_yaml('datasets.csv', '../data/datasets', schema=['id','name','link','langs','catalog_type','access_mode','content_types','countries','owner_type','software','export_standard','add_date','update_date','catalog_export','description','owner_name','owner_link','search_index','storage_volume','dataset_count','api','api_link'], splittable=['Languages', 'Countries', 'Content types', 'Export standard'], removable=[])


if __name__ == "__main__":
    typer.run(from_csv)