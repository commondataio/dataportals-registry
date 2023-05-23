# NADA

NADA is a popular open source microdata catalog publishing tool

Project website: https://nada.ihsn.org
Code repository: https://github.com/ihsn/nada

API documentation: microdata.worldbank.org/api-documentation/

## Schema types
- Collection - group of datasets/studies
- Dataset - collection of data files (resources)
- File - single file/url that is part of the dataset

## API Endpoints

NADA API endpoints: 
- /api/catalog/collections - list of collections
- /api/catalog/search - API endpont to search datasets
- /api/catalog/[dataset public id] - single dataset metadata by public dataset id
- /api/catalog/[dataset public id]/data_files - dataset data files information
- /api/catalog/[dataset public id]/variables - list of all dataset variables
- /metadata/export/[dataset id]/json - metadata for selected dataset by internal id [dataset id] as JSON data
- /metadata/export/[dataset id]/ddi - metadata for selected dataset by internal id [dataset id] as DDI/XML data

## Notes
