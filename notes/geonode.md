# GeoNode

GeoNode is a geodata catalogue

Project website: https://www.geonode.org
API documentation: https://docs.geonode.org/en/master/devel/api/V2/index.html


## Schema types

- Layer - raster or vector map layer, could be mapped to dataset
- Document - could be any type of document, most often PDF or image types of files

More info https://docs.geonode.org/en/3.2.x/usage/data/data_types.html

## API Endpoints

GeoNode supports OpenSearch, WFS, CSW, WMS, OAI-PMH, WCS, WMTS and some installations support data export as DCAT.
GeoNode provides it's own API endpoints at /api

- /api/layers - Layers
- /api/documents - Documents

## Notes

1. Geonode switching from layers to datasets since version 3.3 https://docs.geonode.org/en/master/usage/data/data_types.html so it's important to extract GeoNode version
2. There are diffences between 2.0 and 3.0 API of the Geonode, need to ping endpoints to detect 