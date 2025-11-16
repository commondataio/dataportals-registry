# ArcGIS Server Discovery for Uzbekistan

This document describes the script and process for discovering ArcGIS Server instances in Uzbekistan that are not yet listed in the registry.

## Script: `find_arcgis_uzbekistan.py`

### Purpose
The script searches for ArcGIS Server instances in Uzbekistan using multiple discovery methods and verifies which ones are not already recorded in the registry.

### Discovery Methods

1. **Existing Records Analysis**
   - Loads all existing ArcGIS Server instances from `data/entities/UZ/`
   - Currently finds 4 existing instances:
     - Toshkentboshplan LITI ArcGIS (gis.boshplan.uz)
     - Общественный геопортал Кадастрового агентства (open.ngis.uz)
     - Minagro Uzbekistan ArcGIS server (gis.agro.uz)
     - Kadastr Agentligi ArcGIS (db.ngis.uz)

2. **Shodan Search** (requires API key)
   - Searches for ArcGIS Server instances in Uzbekistan
   - Query: `country:UZ "ArcGIS Server" OR "arcgis/rest" OR "rest/services"`
   - Set `SHODAN_API_KEY` environment variable to use

3. **Censys Search** (requires API credentials)
   - Searches for HTTP services in Uzbekistan with ArcGIS indicators
   - Set `CENSYS_API_ID` and `CENSYS_API_SECRET` environment variables

4. **Domain Testing**
   - Tests potential Uzbekistan government domains for ArcGIS Server
   - Includes common patterns like:
     - `gis.gov.uz`, `geoportal.gov.uz`
     - Ministry patterns: `gis.minagro.uz`, `gis.mintrans.uz`, etc.
     - Agency patterns: `gis.uzstat.uz`, `gis.uzgeodez.uz`, etc.
     - Regional patterns: `gis.tashkent.uz`, `gis.samarkand.uz`, etc.

5. **Web Search** (manual research recommended)
   - Provides suggestions for manual web searches
   - Search queries:
     - `ArcGIS Server site:.uz`
     - `geoportal Uzbekistan`
     - `GIS services Uzbekistan`

6. **Known Patterns**
   - Manual list of suspected URLs discovered through research
   - Can be populated with findings from other sources

### Usage

```bash
cd devdocs
python3 find_arcgis_uzbekistan.py
```

### With API Keys (for automated search)

```bash
export SHODAN_API_KEY="your-shodan-api-key"
export CENSYS_API_ID="your-censys-api-id"
export CENSYS_API_SECRET="your-censys-api-secret"
python3 find_arcgis_uzbekistan.py
```

### Output

The script generates:
- Console output showing discovery progress and results
- `uzbekistan_arcgis_discovery.json` - JSON report with:
  - Existing server count and list
  - Discovered potential servers
  - Verified new ArcGIS Server instances
  - Timestamp

### Verification Process

For each discovered URL, the script:
1. Checks if it's already in the registry
2. Tests common ArcGIS Server endpoints:
   - `/rest/info?f=pjson`
   - `/rest/services?f=pjson`
   - `/server/rest/info?f=pjson`
   - `/server/rest/services?f=pjson`
   - `/arcgis/rest/info?f=pjson`
   - `/services?wsdl`
3. Verifies the response contains ArcGIS Server indicators
4. Reports verified instances not in the registry

### Additional Discovery Resources

Based on research:

1. **Esri Distributor in Uzbekistan**
   - Data+ International (esri-cis.uz)
   - May have information about organizations using ArcGIS Server

2. **Government Agencies Known to Use GIS**
   - Ministry of Agriculture (already recorded: gis.agro.uz)
   - State Cadastre Agency (already recorded: db.ngis.uz, open.ngis.uz)
   - Toshkentboshplan LITI (already recorded: gis.boshplan.uz)

3. **Potential Sources**
   - Other ministries and agencies
   - Regional governments
   - Academic institutions
   - Private organizations

### Manual Research Steps

1. **Search Engines**
   - Google: `ArcGIS Server site:.uz`
   - Google: `geoportal Uzbekistan`
   - Google: `GIS services Uzbekistan`
   - Bing: Similar queries

2. **Government Websites**
   - Visit ministry websites and look for GIS/geoportal sections
   - Check agency websites for data/GIS links
   - Review regional government portals

3. **Open Data Sources**
   - Review Open Data Inception list for Uzbekistan (reports 99 sources)
   - Check DataPortals.org for additional entries
   - Review ArcGIS Hub for Uzbekistan instances

4. **Technical Forums and Communities**
   - GIS forums and user groups
   - Professional networks
   - Technical documentation and case studies

### Legal and Ethical Considerations

⚠️ **Important**: 
- Only test publicly accessible servers
- Do not attempt to access servers without permission
- Respect robots.txt and terms of service
- Comply with all applicable laws and regulations
- Use rate limiting when testing multiple URLs

### Next Steps

1. Run the script regularly to discover new instances
2. Manually verify discovered servers
3. Research organization/agency information for each server
4. Create YAML records for verified servers not in the registry
5. Update the script with newly discovered patterns

### Example Output

```
================================================================================
ArcGIS Server Discovery for Uzbekistan
================================================================================

1. Loading existing ArcGIS Server records...
   Found 4 existing ArcGIS Server instances:
     - Toshkentboshplan LITI ArcGIS (https://gis.boshplan.uz/server/rest/services)
     - Общественный геопортал Кадастрового агентства (https://open.ngis.uz)
     - Minagro Uzbekistan ArcGIS server (https://gis.agro.uz/server/rest/services)
     - Kadastr Agentligi ArcGIS (https://db.ngis.uz/db/rest/services)

...

================================================================================
RESULTS
================================================================================

Existing ArcGIS Server instances: 4
Discovered potential servers: X
Verified new ArcGIS Server instances: Y

NEW ARCGIS SERVER INSTANCES NOT IN REGISTRY:
...
```

