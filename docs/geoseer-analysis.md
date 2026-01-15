# GeoSeer Geospatial Search Engine - Comprehensive Analysis

**Analysis Date:** December 2025  
**Website:** https://www.geoseer.net  
**Statistics Page:** https://www.geoseer.net/stats/

## Executive Summary

GeoSeer is a specialized geospatial search engine designed to address the challenge of discovering geospatial data by indexing a vast array of public-facing geospatial web services that adhere to Open Geospatial Consortium (OGC) standards. The platform provides centralized discovery of spatial datasets from around the world, making it easier for users to find WMS, WFS, WCS, and WMTS datasets.

## Statistics

### Current Statistics (as of December 2025)

GeoSeer's statistics page provides comprehensive insights into its extensive database:

- **Current Datasets**: 3,781,462 distinct datasets (after cleanup and deduplication)
- **Distinct Datasets**: 3,283,770 unique datasets
- **Hosts**: 4,374 currently online (10,151 historical total)
- **Endpoints**: 314,758 active (761,622 ever discovered)
- **Services**: 358,661 operational (1,391,137 cumulative)
- **Countries**: 95 countries represented
- **Historical Datasets**: 17,646,949 identified historically

**Note:** The actual number of public datasets GeoSeer's index represents is estimated to be 50-100% higher than the reported numbers, as these figures represent datasets after clean-up and post-processing (such as duplicate removal).

### Service Statistics Breakdown

The platform indexes various OGC service types:
- **WMS (Web Map Service)**: Georeferenced map images
- **WFS (Web Feature Service)**: Vector feature data access
- **WCS (Web Coverage Service)**: Raster data delivery
- **WMTS (Web Map Tile Service)**: Pre-rendered map tiles

### Geographic Coverage

- **Global Coverage**: Datasets span diverse domains including hydrology, geology, demographics, and political boundaries
- **Dataset Extents**: Provides visualizations showing global distribution of dataset extents
- **Top-Level Domains**: Tracks datasets by top-level domain, hosts, and service types

## Crawl Strategy

GeoSeer employs a systematic web crawling process to discover and index geospatial web services:

### Discovery Phase
- The GeoSeer spider identifies geospatial web services through various APIs and sources
- Systematically searches for public-facing services adhering to OGC standards

### Metadata Retrieval
- Downloads **GetCapabilities documents** for each service
- GetCapabilities documents are standard XML files that map servers use to describe their layers and features
- These documents contain comprehensive metadata about available datasets

### Processing Phase
The system processes GetCapabilities documents to:
1. **Extract dataset information** from the XML structure
2. **Clean data** to ensure quality and consistency
3. **Remove duplicates** to create distinct dataset counts
4. **Determine spatial extents** for geographic search capabilities
5. **Index metadata** to make datasets searchable

### Indexing
- The refined data is indexed, making it searchable through GeoSeer's platform
- Maintains historical tracking of services and datasets
- Continuously updates the index as new services are discovered

### Methodology Notes
- Focuses exclusively on public-facing services
- Processes OGC-compliant services (WMS, WFS, WCS, WMTS)
- Blog posts document the methodology and counting approach
- The platform acknowledges that "distinct" layers can be counted in various ways, with the largest number being the total from all unique capabilities documents

## Geodata Software and Standards

### Supported OGC Standards

GeoSeer focuses on indexing services that adhere to OGC (Open Geospatial Consortium) standards:

1. **WMS (Web Map Service)**
   - Provides georeferenced map images
   - Enables visualization of spatial data as maps

2. **WFS (Web Feature Service)**
   - Allows access to vector feature data
   - Supports querying and retrieval of geographic features

3. **WCS (Web Coverage Service)**
   - Delivers raster data
   - Provides access to coverage data (gridded data)

4. **WMTS (Web Map Tile Service)**
   - Serves pre-rendered map tiles
   - Optimized for high-performance map rendering

### Software Compatibility

By supporting these OGC standards, GeoSeer ensures compatibility with a wide range of geospatial software and applications, including:
- GIS software (QGIS, ArcGIS, etc.)
- Web mapping applications
- Geospatial data portals
- Scientific research tools

## Business Model

GeoSeer operates on a multi-tier business model:

### 1. Free Web Service
- **Ad-free search engine** available to the public at no cost
- Enables discovery of geospatial datasets through a user-friendly web interface
- No registration required for basic search functionality
- Enhances discoverability of open geospatial data

### 2. Public API
- **API access** for integration into WebGIS and other applications
- Enables organizations to integrate GeoSeer's database into their own products
- Features include:
  - Service searches
  - Spatial location searches
  - List all datasets on a service
  - Access to detailed dataset metadata

### 3. Licensed Database
- **Commercial licensing** available for organizations
- Allows businesses to integrate GeoSeer's comprehensive database directly into their applications or services
- Improves end-user workflows by facilitating discovery of third-party data
- Pricing tiers available (e.g., £65/month, £245/month, custom enterprise pricing)

### Business Value Proposition

The European Commission forecasts that Open Data will provide over €75 billion in economic benefit across the EU in 2020 alone. GeoSeer's platform supports this by:
- Making geospatial data more discoverable
- Reducing the time and effort required to find relevant datasets
- Enabling integration of authoritative, official data into applications
- Supporting various domains from hydrology to geology, demographics to politics

## API Capabilities

### API Endpoint
- **URL**: https://www.geoseer.net/api.php
- **Documentation**: Available on the API page

### Key Features
- **Service Searches**: Search for geospatial web services by various criteria
- **Spatial Location Searches**: Find datasets based on geographic location
- **Dataset Listing**: List all datasets available on a specific service
- **Metadata Access**: Retrieve detailed metadata about datasets and services
- **Integration Support**: Designed for easy integration into WebGIS platforms

### Use Cases
- Embedding geospatial search into custom applications
- Building WebGIS interfaces with pre-indexed data
- Creating data discovery tools for organizations
- Research and academic applications

## Additional Insights

### Data Quality and Processing
- The platform performs significant data cleaning and deduplication
- Maintains both current and historical statistics
- Tracks service availability (online vs. historical)
- Provides transparency about counting methodology through blog posts

### Dataset Extent Visualizations
- Offers visualizations showing global distribution of dataset extents
- Provides maps showing where datasets are located geographically
- Helps users understand geographic coverage

### Historical Tracking
- Maintains historical records of:
  - Total hosts ever discovered (10,151 vs. 4,374 current)
  - Total endpoints ever discovered (761,622 vs. 314,758 current)
  - Total services ever indexed (1,391,137 vs. 358,661 current)
  - Total datasets ever identified (17,646,949 vs. 3,781,462 current)

This historical tracking demonstrates the dynamic nature of geospatial data availability and the challenges in maintaining an up-to-date index.

### Blog and Documentation
- Maintains an active blog documenting:
  - Methodology for counting layers/datasets
  - Statistics updates
  - Technical insights
  - API announcements
- Provides RSS feed for blog updates

### Top Statistics
The platform tracks and publishes:
- Top 10 top-level domains with most datasets
- Top 10 hosts with the most datasets
- Top 10 WMS services with the most datasets
- Top 10 WFS services with the most datasets
- Top 10 WCS services with the most datasets
- Top 10 WMTS services with the most datasets

## Technical Architecture

### Data Collection
- Web crawlers for service discovery
- GetCapabilities document retrieval
- XML parsing and processing
- Spatial extent calculation

### Data Storage
- Indexed database of services and datasets
- Metadata storage
- Historical tracking system
- Statistics aggregation

### Search Capabilities
- Text-based search
- Spatial search (by location/extent)
- Service type filtering
- Country/region filtering

## References and Sources

1. **Primary Website**: https://www.geoseer.net
2. **Statistics Page**: https://www.geoseer.net/stats/
3. **API Documentation**: https://www.geoseer.net/api.php
4. **Blog**: https://www.geoseer.net/blog/
5. **Web Research**: Geospatial World, GeoConnexion articles on GeoSeer's licensed database

## Conclusion

GeoSeer serves as a valuable resource for discovering and accessing a vast array of geospatial datasets, supporting various OGC standards, and offering integration options through its API and licensed database. The platform addresses a critical need in the geospatial community by making it easier to discover and access distributed geospatial resources, ultimately supporting the broader goals of open data and data accessibility.

The platform's comprehensive indexing, systematic crawling approach, and multi-tier business model make it a significant player in the geospatial data discovery ecosystem, supporting both individual users and organizations seeking to integrate geospatial data into their applications and workflows.

