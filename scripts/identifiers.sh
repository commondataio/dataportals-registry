#!/bin/sh

python enrich.py  enrich-identifiers ../../commondata-crawl/datasources/re3data/re3data.tsv re3data
python enrich.py  enrich-identifiers ../../commondata-crawl/datasources/roar/eprints.tsv roar
python enrich.py  enrich-identifiers ../../commondata-crawl/datasources/dataportals/mapped.tsv dataportals.org