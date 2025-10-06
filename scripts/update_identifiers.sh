#!/bin/sh
python enrich.py  enrich-identifiers ../../cdi-data/datasources/re3data/re3data.tsv re3data
python enrich.py  enrich-identifiers ../../cdi-data/datasources/roar/eprints.tsv roar
python enrich.py  enrich-identifiers ../../cdi-data/datasources/dataportals/mapped.tsv dataportals.org
python enrich.py  enrich-identifiers ../../cdi-data/datasources/wikidata/wikidata.tsv wikidata
python enrich.py  enrich-identifiers ../../cdi-data/datasources/sherpa/sherpa.tsv opendoar
python enrich.py  enrich-identifiers ../../cdi-data/datasources/datacite/datacite.tsv datacite