# Duplicate UID's, ID's and Critical Errors Analysis

Generated: 1763220956.1681092

## Summary

- **Total files scanned**: 10299
- **Successfully processed**: 10298
- **Empty files**: 1
- **Duplicate UID's**: 0
- **Duplicate ID's**: 7
- **YAML parsing errors**: 0
- **Other errors**: 0
- **Missing required fields**: 204
- **Empty required fields**: 0
- **Filename mismatches**: 63

## Duplicate UID's

✓ No duplicate UID's found.

## Duplicate ID's

Found **7** duplicate ID's:

### ID: `databisorg`

Found in 2 file(s):

- `data/entities/World/indicators/databisorg.yaml`
- `data/software/indicators/databisorg.yaml`

### ID: `datagovmy`

Found in 2 file(s):

- `data/entities/MY/Federal/opendata/datagovmy.yaml`
- `data/software/opendata/datagovmy.yaml`

### ID: `datauniceforg`

Found in 2 file(s):

- `data/entities/World/indicators/datauniceforg.yaml`
- `data/software/indicators/datauniceforg.yaml`

### ID: `dataworldbankorg`

Found in 2 file(s):

- `data/entities/World/indicators/dataworldbankorg.yaml`
- `data/software/indicators/dataworldbankorg.yaml`

### ID: `openmlorg`

Found in 2 file(s):

- `data/entities/World/ml/openmlorg.yaml`
- `data/software/scientific/openmlorg.yaml`

### ID: `tdsmarinerutgersedu`

Found in 2 file(s):

- `data/entities/US/Other/scientific/tdsmarinerutgersedu.yaml`
- `data/scheduled/Unknown/opendata/tdsmarinerutgersedu.yaml`

### ID: `threddsaoswiscedu`

Found in 2 file(s):

- `data/entities/US/Other/scientific/threddsaoswiscedu.yaml`
- `data/scheduled/Unknown/opendata/threddsaoswiscedu.yaml`

## Empty Files

Found **1** empty files:

- `data/scheduled/EE/Federal/geo/eeliskeskkonnaportaalee.yaml`

## Missing Required Fields

Found **204** records with missing required fields:

### Missing `uid` (204 records)

- `data/entities/PL/Federal/scientific/pkrodbukpl.yaml` (type: entities)
- `data/entities/PL/Federal/scientific/sanorodbukpl.yaml` (type: entities)
- `data/entities/PL/Federal/scientific/ifjrodbukpl.yaml` (type: entities)
- `data/entities/PL/Federal/scientific/aghrodbukpl.yaml` (type: entities)
- `data/entities/PL/Federal/scientific/akfrodbukpl.yaml` (type: entities)
- `data/entities/PL/Federal/scientific/danebadawczeuwedupl.yaml` (type: entities)
- `data/entities/PL/Federal/scientific/ukenrodbukpl.yaml` (type: entities)
- `data/entities/PL/Federal/scientific/uekrodbukpl.yaml` (type: entities)
- `data/entities/PL/Federal/scientific/uwrrodbukpl.yaml` (type: entities)
- `data/entities/PL/Federal/scientific/dataportalingpanpl.yaml` (type: entities)
- `data/entities/PL/Federal/scientific/ujrodbukpl.yaml` (type: entities)
- `data/entities/PL/Federal/scientific/rodbukpl.yaml` (type: entities)
- `data/entities/PE/Federal/scientific/datasetsupedupe.yaml` (type: entities)
- `data/entities/US/Other/scientific/oceanwatchaomlnoaagov.yaml` (type: entities)
- `data/entities/US/Other/scientific/erddapaomlgdpnoaagov.yaml` (type: entities)
- `data/entities/US/Other/scientific/erddapaomlhdbaomlnoaagov.yaml` (type: entities)
- `data/entities/US/Other/scientific/dataneracoosorgthredds.yaml` (type: entities)
- `data/entities/US/Other/scientific/erddapnanoosorg.yaml` (type: entities)
- `data/entities/US/Other/scientific/tdsmarinerutgersedu.yaml` (type: entities)
- `data/entities/US/Other/scientific/osmcnoaagoverddap.yaml` (type: entities)
- ... and 184 more

## Filename Mismatches

Found **63** files where the `id` field doesn't match the filename:

- `data/entities/SK/SK-BL/opendata/opendatabratislavask.yaml`
  - Filename: `opendatabratislavask`
  - ID field: `opendatabratislavash`
- `data/entities/PL/Federal/indicators/strategstatgovpl.yaml`
  - Filename: `strategstatgovpl`
  - ID field: `statregstatgovpl`
- `data/entities/PL/Federal/geo/espdppl.yaml`
  - Filename: `espdppl`
  - ID field: `espdpplgeonetwork`
- `data/entities/ZW/geo/wis2boxmeteonacom.yaml`
  - Filename: `wis2boxmeteonacom`
  - ID field: `136156130101`
- `data/entities/US/US-ID/geo/datagisidahogov.yaml`
  - Filename: `datagisidahogov`
  - ID field: `gisidahohubarcgiscom`
- `data/entities/US/US-PA/opendata/wwwopendataphillyorg.yaml`
  - Filename: `wwwopendataphillyorg`
  - ID field: `opendataphillyorg`
- `data/entities/US/Other/opendata/andrewfriedmangithubio.yaml`
  - Filename: `andrewfriedmangithubio`
  - ID field: `andrew-friedman.github.io`
- `data/entities/US/Other/other/datausaio.yaml`
  - Filename: `datausaio`
  - ID field: `datausaiio`
- `data/entities/US/Other/scientific/ncbinlmnihgov.yaml`
  - Filename: `ncbinlmnihgov`
  - ID field: `geoncbinlmnihgov`
- `data/entities/US/Other/scientific/appglerlnoaagov.yaml`
  - Filename: `appglerlnoaagov`
  - ID field: `appsglerlnoaagov`
- `data/entities/US/US-NC/opendata/datawakegovcom.yaml`
  - Filename: `datawakegovcom`
  - ID field: `datawakegov`
- `data/entities/US/US-IN/opendata/datasouthbendingov.yaml`
  - Filename: `datasouthbendingov`
  - ID field: `datasouthbendopendataarcgiscom`
- `data/entities/US/US-FL/geo/fgdlorgprod.yaml`
  - Filename: `fgdlorgprod`
  - ID field: `fgdlorgcurrent`
- `data/entities/US/US-FL/geo/geodatafloridaiogov.yaml`
  - Filename: `geodatafloridaiogov`
  - ID field: `geodatafloridagiogov`
- `data/entities/US/US-CA/opendata/dataoaklandnetcom.yaml`
  - Filename: `dataoaklandnetcom`
  - ID field: `dataoaklandcagov`
- `data/entities/US/US-MA/geo/wwwcptsorg.yaml`
  - Filename: `wwwcptsorg`
  - ID field: `wwwctpsorg`
- `data/entities/MT/opendata/ckanopendatamaltacom.yaml`
  - Filename: `ckanopendatamaltacom`
  - ID field: `opendatagovmt`
- `data/entities/NZ/Federal/geo/nationalmapconz.yaml`
  - Filename: `nationalmapconz`
  - ID field: `datanationalmapconz`
- `data/entities/GR/Federal/scientific/opendatathessalonikigr.yaml`
  - Filename: `opendatathessalonikigr`
  - ID field: `ckansmokefreebraineu`
- `data/entities/IN/IN-DL/geo/bhubaneswaronein.yaml`
  - Filename: `bhubaneswaronein`
  - ID field: `gsdlorgin`
- `data/entities/CR/CR-SJ/geo/atlassantaanagocr.yaml`
  - Filename: `atlassantaanagocr`
  - ID field: `atlassantaanago.cr`
- `data/entities/CH/Federal/microdata/menuchunisantech.yaml`
  - Filename: `menuchunisantech`
  - ID field: `wwwstudydatablvadminch`
- `data/entities/CH/Federal/microdata/wwwstudydatablvadminch.yaml`
  - Filename: `wwwstudydatablvadminch`
  - ID field: `devisuforscenterch`
- `data/entities/CH/Federal/opendata/datasbbch.yaml`
  - Filename: `datasbbch`
  - ID field: `datadbbch`
- `data/entities/CO/Federal/opendata/wwwdatosgovco.yaml`
  - Filename: `wwwdatosgovco`
  - ID field: `datagovco`
- `data/entities/IT/IT-45/geo/mapperegioneemilia-romagnait.yaml`
  - Filename: `mapperegioneemilia-romagnait`
  - ID field: `mapperegioneemiliaromagnait`
- `data/entities/CA/CA-ON/opendata/yorkca.yaml`
  - Filename: `yorkca`
  - ID field: `insightsyorkopendataarcgiscom`
- `data/entities/CA/CA-AB/opendata/datacountygpabca.yaml`
  - Filename: `datacountygpabca`
  - ID field: `opendatacountygpabca`
- `data/entities/CA/Federal/geo/spotorthoimagescanada2005.yaml`
  - Filename: `spotorthoimagescanada2005`
  - ID field: `spot-orthoimages-canada-2005`
- `data/entities/CZ/Federal/opendata/datactucz.yaml`
  - Filename: `datactucz`
  - ID field: `dataarmycz`
- `data/entities/CZ/Federal/opendata/dataarmycz.yaml`
  - Filename: `dataarmycz`
  - ID field: `datactucz`
- `data/entities/CZ/Federal/geo/csugovcz.yaml`
  - Filename: `csugovcz`
  - ID field: `geodatacsugovcz`
- `data/entities/RU/RU-MOW/geo/apieatlasmosru.yaml`
  - Filename: `apieatlasmosru`
  - ID field: `apietlasmosru`
- `data/entities/RU/RU-KYA/geo/csw24bpd.yaml`
  - Filename: `csw24bpd`
  - ID field: `24bpdru`
- `data/entities/RU/RU-PRI/geo/geofehriru.yaml`
  - Filename: `geofehriru`
  - ID field: `geoferhriru`
- `data/entities/RU/Federal/indicators/sberindex.yaml`
  - Filename: `sberindex`
  - ID field: `sberindexru`
- `data/entities/RU/RU-KHM/geo/geouriitru_mapproxy.yaml`
  - Filename: `geouriitru_mapproxy`
  - ID field: `geouriitrumapproxy`
- `data/entities/RU/RU-KDA/opendata/opendatakrasnodarru.yaml`
  - Filename: `opendatakrasnodarru`
  - ID field: `datakrasnodarru`
- `data/entities/World/indicators/statsunidoorg.yaml`
  - Filename: `statsunidoorg`
  - ID field: `statunidoorg`
- `data/entities/TW/Federal/indicators/statdbdgbasgovtw.yaml`
  - Filename: `statdbdgbasgovtw`
  - ID field: `nstatdbdgbasgovtw`
- `data/entities/SA/Federal/geo/geoportalsa.yaml`
  - Filename: `geoportalsa`
  - ID field: `wwwgeoportalsa`
- `data/entities/BO/Federal/microdata/andainegobbo.yaml`
  - Filename: `andainegobbo`
  - ID field: `andaineinegobbo`
- `data/entities/KR/Federal/indicators/houstathfgokr.yaml`
  - Filename: `houstathfgokr`
  - ID field: `houstathgokr`
- `data/entities/GB/Federal/scientific/biostudies.yaml`
  - Filename: `biostudies`
  - ID field: `wwwebiacuk`
- `data/entities/GB/Federal/scientific/basacuk.yaml`
  - Filename: `basacuk`
  - ID field: `bassacuk`
- `data/entities/GB/GB-ENG/opendata/databirminghamgovuk.yaml`
  - Filename: `databirminghamgovuk`
  - ID field: `cityobservatorybirminghamgovuk`
- `data/entities/AR/AR-L/geo/geonodeidesantarosagobar.yaml`
  - Filename: `geonodeidesantarosagobar`
  - ID field: `geonodeidesantarosagobargeonetwork`
- `data/entities/AU/Federal/geo/wwwsaxgisorg.yaml`
  - Filename: `wwwsaxgisorg`
  - ID field: `wwwsaxigisorg`
- `data/entities/KZ/KZ-55/geo/mapgeopavlodarkz.yaml`
  - Filename: `mapgeopavlodarkz`
  - ID field: `geopavlodarkz`
- `data/entities/NP/Federal/indicators/opendatanepalcom.yaml`
  - Filename: `opendatanepalcom`
  - ID field: `nationaldatagovnp`

... and 13 more

