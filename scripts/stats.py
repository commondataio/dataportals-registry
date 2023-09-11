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


  
@app.command()
def software():
    """Software report"""
    import pandas as pd
    from rich.console import Console
    from rich.table import Table
    data = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl'))

    series = []
    for row in data:
        series.append(row['software']['name']) 

    data = pd.Series(series)

    df_count = data.value_counts()

    df_count = df_count.reset_index()  # make sure indexes pair with number of rows
    df_count['pct'] = 100*df_count['count'] / df_count['count'].sum()
    
    total = df_count['count'].sum()
    table = Table(title='Software statistics') 
    table.add_column("Software", justify="right", style="cyan", no_wrap=True)
    table.add_column("Count", style="magenta")
    table.add_column("Percent", justify="right", style="green", no_wrap=True)
    other = 0
    for index, row in df_count.iterrows():
        if row['count'] > 9:
            item = [row['index'], str(row['count']), '%0.3f' % (row['pct'])]
            table.add_row(*item)
        else:
            other += row['count']
    item = ['Other', str(other), '%0.3f' % (100.0 * other / total)]
    table.add_row(*item)    
    table.add_section()
    table.add_row('Total', str(total))
    console = Console()
    console.print(table)  

@app.command()
def cattype():
    """Catalog type report"""
    import pandas as pd
    from rich.console import Console
    from rich.table import Table
    data = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl'))

    series = []
    for row in data:
        series.append(row['catalog_type']) 

    data = pd.Series(series)

    df_count = data.value_counts()

    df_count = df_count.reset_index()  # make sure indexes pair with number of rows
    df_count['pct'] = 100*df_count['count'] / df_count['count'].sum()
    
    total = df_count['count'].sum()
    table = Table(title='Catalog type statistics') 
    table.add_column("Catalog type", justify="right", style="cyan", no_wrap=True)
    table.add_column("Count", style="magenta")
    table.add_column("Percent", justify="right", style="green", no_wrap=True)
    other = 0
    for index, row in df_count.iterrows():
        if row['count'] > 9:
            item = [row['index'], str(row['count']), '%0.3f' % (row['pct'])]
            table.add_row(*item)
        else:
            other += row['count']
    item = ['Other', str(other), '%0.3f' % (100.0 * other / total)]
    table.add_row(*item)    
    table.add_section()
    table.add_row('Total', str(total))
    console = Console()
    console.print(table)  


@app.command()
def country():
    """Countries report"""
    import pandas as pd
    from rich.console import Console
    from rich.table import Table
    data = load_jsonl(os.path.join(DATASETS_DIR, 'full.jsonl'))

    series = []
    for row in data:
        series.append(row['owner']['location']['country']['name']) 

    data = pd.Series(series)

    df_count = data.value_counts()

    df_count = df_count.reset_index()  # make sure indexes pair with number of rows
    df_count['pct'] = 100*df_count['count'] / df_count['count'].sum()
    
    total = df_count['count'].sum()
    table = Table(title='Country statistics') 
    table.add_column("Catalog type", justify="right", style="cyan", no_wrap=True)
    table.add_column("Count", style="magenta")
    table.add_column("Percent", justify="right", style="green", no_wrap=True)
    other = 0
    for index, row in df_count.iterrows():
        if row['count'] > 50:
            item = [row['index'], str(row['count']), '%0.3f' % (row['pct'])]
            table.add_row(*item)
        else:
            other += row['count']
    item = ['Other', str(other), '%0.3f' % (100.0 * other / total)]
    table.add_row(*item)    
    table.add_section()
    table.add_row('Total', str(total))
    console = Console()
    console.print(table)  
    


if __name__ == "__main__":    
    app()