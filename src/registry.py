from flask import Flask, json, jsonify, redirect, render_template, send_file, send_from_directory, request, url_for, flash, Response
import collections

DEBUG = False
SECRET_KEY = "azt3eycglbkj30i6tdfg,xfkxflgkdrfogkotg,/vxlf"
REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 8089
CATALOGS_DATA_PATH = '../data/datasets/catalogs.jsonl'

def load_data(filename):
    data = {}
    f = open(filename, 'r', encoding='utf8')
    for row in f:
        record = json.loads(row)
        data[record['id']] = record
    f.close()
    return data

CATALOGS_GLOBAL = load_data(CATALOGS_DATA_PATH)    

def root_view():
    global CATALOGS_GLOBAL
    return render_template('catalogs_list.tmpl', objects=CATALOGS_GLOBAL.values())

def catalog_view(slug):
    global CATALOGS_GLOBAL
    obj = CATALOGS_GLOBAL[slug]
    return render_template('catalog.tmpl', object=obj)

def catalog_view_json(slug):
    global CATALOGS_GLOBAL
    obj = CATALOGS_GLOBAL[slug]
    return jsonify(obj)


def registry_view_json():
    global CATALOGS_GLOBAL
    return jsonify(CATALOGS_GLOBAL)


def add_views_rules(app):
    app.add_url_rule('/', 'root', root_view)
    app.add_url_rule('/catalogs.json', '/catalogs.json', registry_view_json)
    app.add_url_rule('/catalog/<slug>', 'catalogs/<slug>', catalog_view)
    app.add_url_rule('/catalog/<slug>.json', 'catalogs/<slug>.json', catalog_view_json)

def run_server():

    app = Flask("Metacrafter registry", static_url_path='/assets')
    app.config['SECRET_KEY'] = SECRET_KEY
    app.config['PROPAGATE_EXCEPTIONS'] = True

    add_views_rules(app)

    app.run(host=REGISTRY_HOST, port=REGISTRY_PORT, debug=DEBUG)


if __name__ == "__main__":
    run_server()
