# Missing Data Portals in Uzbekistan - Analysis Report

## Executive Summary

This report identifies data portals in Uzbekistan that are potentially missing from the dataportals-registry. According to Open Data Inception, Uzbekistan has **99 open data sources**, but the registry currently contains only **7 portals** for Uzbekistan.

## Currently Recorded Portals (7)

### Open Data Portals
1. **Uzbekistan open data portal** (`dataegovuz`)
   - URL: https://data.egov.uz
   - Type: Open data portal
   - Owner: Government of Uzbekistan

### Indicators Catalogs
2. **Портал статистики Республики Узбекистан** (`siatstatuz`)
   - URL: https://siat.stat.uz
   - Type: Indicators catalog
   - Owner: Statistics of Uzbekistan

### Geoportals (5)
3. **Общественный геопортал Кадастрового агентства Республики Узбекистан** (`openngisuz`)
   - URL: https://open.ngis.uz
   - Type: Geoportal
   - Owner: Кадастровое агентство Республики Узбекистан

4. **Kadastr Agentligi ArcGIS** (`dbngisuz`)
   - URL: https://db.ngis.uz/db/rest/services
   - Type: Geoportal
   - Owner: Kadastr Agentligi

5. **Minagro Uzbekistan ArcGIS server** (`gisagrouz`)
   - URL: https://gis.agro.uz/server/rest/services
   - Type: Geoportal
   - Owner: Ministry of agriculture of Uzbekistan

6. **Toshkentboshplan LITI ArcGIS** (`gisboshplanuz`)
   - URL: https://gis.boshplan.uz/server/rest/services
   - Type: Geoportal
   - Owner: Toshkentboshplan LITI

7. **Геопортал ГИС ГГК Республики Узбекистан** (`dshkuz`)
   - URL: https://dshk.uz/ru/main/
   - Type: Geoportal
   - Owner: ГГК Республики Узбекистан

## Potentially Missing Portals

### High Priority - Verified URLs

1. **Old Data Portal**
   - URL: https://olddata.gov.uz
   - Type: Open data portal
   - Description: Old version of data portal mentioned in search results
   - Status: **NOT IN REGISTRY**
   - Action: Verify if this is an archived/legacy portal or still active

2. **Agency for Strategic Reforms Open Data**
   - URL: https://asr.gov.uz/en/open-data
   - Type: Open data portal
   - Description: Agency for Strategic Reforms under the President - open data section
   - Status: **NOT IN REGISTRY**
   - Action: Check if this is a separate data portal or a section of the main portal

3. **e-Government Portal**
   - URL: https://egov.uz
   - Type: Open data portal
   - Description: e-Government portal - might have data catalog
   - Status: **NOT IN REGISTRY**
   - Action: Verify if this portal contains a data catalog or links to data.egov.uz

### Needs Verification

4. **data.gov.uz**
   - URL: https://data.gov.uz
   - Status: May be the same as data.egov.uz (alternative URL)
   - Action: Verify if data.gov.uz redirects to data.egov.uz or is a separate portal

## Gap Analysis

- **Recorded**: 7 portals
- **Reported by Open Data Inception**: 99 sources
- **Potential Missing**: ~92 portals

This significant gap suggests there are many additional portals that need to be discovered and added to the registry.

## Recommendations for Finding Missing Portals

### 1. Ministry and Agency Portals
Check government ministries and agencies for data portals:
- Ministry of Health
- Ministry of Education
- Ministry of Finance
- Ministry of Economy
- Ministry of Agriculture (already has GIS portal)
- Ministry of Transport
- Ministry of Energy
- Ministry of Water Resources
- Ministry of Environment

### 2. Regional and Local Government Portals
- Tashkent (capital city)
- Samarkand
- Bukhara
- Andijan
- Fergana
- Other regional administrations

### 3. Sector-Specific Portals
- Health data portals
- Education statistics
- Economic indicators
- Environmental data
- Transportation data
- Energy statistics

### 4. Data Sources to Check
- **Open Data Inception**: https://data.opendatasoft.com/explore/dataset/open-data-sources%40public/
  - Filter by Uzbekistan to get the complete list of 99 sources
- **ArcGIS Hub**: Search for Uzbekistan instances
- **DataPortals.org**: Check for additional entries
- **Government websites**: Review ministry and agency websites for data sections

### 5. Technical Discovery Methods
- Search for ArcGIS Server instances in Uzbekistan (.uz domains)
- Search for CKAN installations
- Search for other open data platform installations
- Review government website sitemaps for data sections
- Check for API endpoints on government websites

## Next Steps

1. **Immediate Actions**:
   - Verify the 3 identified missing portals (olddata.gov.uz, asr.gov.uz, egov.uz)
   - Check if data.gov.uz is different from data.egov.uz
   - Access Open Data Inception to get the complete list of 99 sources

2. **Short-term Actions**:
   - Review ministry websites for data portals
   - Check regional government portals
   - Search for ArcGIS Hub instances in Uzbekistan

3. **Long-term Actions**:
   - Systematic review of all 99 sources from Open Data Inception
   - Automated discovery of data portals using technical methods
   - Regular updates to keep the registry current

## Notes

- The registry structure supports various catalog types: Open data portal, Geoportal, Indicators catalog, Scientific data repositories, Microdata catalogs, etc.
- Uzbekistan has made significant progress in open data, ranking 5th globally in number of open data sources according to Open Data Inception (2021).
- The country was the first in Central Asia to have portals included in DataPortals.org.

## Report Generated

This report was generated by analyzing the existing registry and comparing it with publicly available information about Uzbekistan's open data ecosystem.

**Date**: 2025-01-27
**Script**: `find_missing_uzbekistan_portals.py`
**Registry Version**: Current as of analysis date

