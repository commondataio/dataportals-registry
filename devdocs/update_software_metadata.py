#!/usr/bin/env python3
"""
Update software records with metadata found online.
This script enriches software YAML files with website URLs and other metadata.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional

BASE_DIR = Path(__file__).parent
SOFTWARE_DIR = BASE_DIR / "data" / "software"

# Knowledge base of software metadata
SOFTWARE_METADATA = {
    # Recently added software
    "strapi": {
        "website": "https://strapi.io",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "terria": {
        "website": "https://terria.io",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "haplo": {
        "website": "https://www.haplo.com",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "wis20box": {
        "website": "https://github.com/wmo-im/wis2box",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "rasdaman": {
        "website": "https://rasdaman.com",
        "has_api": "Yes",
        "has_bulk": "No",
        "wcs": "Yes",
        "wms": "Yes",
    },
    "opendap": {
        "website": "https://www.opendap.org",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "pure": {
        "website": "https://www.elsevier.com/solutions/pure",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "ala": {
        "website": "https://www.ala.org.au",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "gbifplatform": {
        "website": "https://www.gbif.org",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "aodn": {
        "website": "https://portal.aodn.org.au",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "sdmxri": {
        "website": "https://sdmx.org",
        "has_api": "Yes",
        "has_bulk": "No",
        "sdmx": "Yes",
    },
    "stattech": {
        "website": "https://siscc.org/stat-suite/",
        "has_api": "Yes",
        "has_bulk": "No",
        "sdmx": "Yes",
    },
    "superstar": {
        "website": "https://www.str.com/products/superstar",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "supermapiserver": {
        "website": "https://www.supermap.com/en",
        "has_api": "Yes",
        "has_bulk": "No",
        "wms": "Yes",
        "wfs": "Yes",
        "wcs": "Yes",
    },
    "openwis": {
        "website": "https://github.com/OpenWIS/openwis",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "aristotlemdr": {
        "website": "https://www.aristotlemetadata.com",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "datawheel": {
        "website": "https://datawheel.us",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "openmlorg": {
        "website": "https://www.openml.org",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "datacubews": {
        "website": "https://www.opendatacube.org",
        "has_api": "Yes",
        "has_bulk": "No",
        "wcs": "Yes",
        "wms": "Yes",
    },
    "popgis": {
        "website": "https://www.spc.int/our-work/geospatial/popgis",
        "has_api": "No",
        "has_bulk": "No",
    },
    "giswebse": {
        "website": "https://www.gisweb.ru",
        "has_api": "Yes",
        "has_bulk": "No",
        "wms": "Yes",
        "wfs": "Yes",
    },
    "activemapgis": {
        "website": "https://www.activemap.ru",
        "has_api": "Yes",
        "has_bulk": "No",
        "wms": "Yes",
    },
    "othergeo": {
        "website": "",
        "has_api": "Uncertain",
        "has_bulk": "Uncertain",
    },
    "datalibrary": {
        "website": "",
        "has_api": "Uncertain",
        "has_bulk": "Uncertain",
    },
    "ifremercatalog": {
        "website": "https://www.seanoe.org",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "mwmb": {
        "website": "",
        "has_api": "Uncertain",
        "has_bulk": "Uncertain",
    },
    "datagovmy": {
        "website": "https://www.data.gov.my",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "searchengine": {
        "website": "",
        "has_api": "Uncertain",
        "has_bulk": "Uncertain",
    },
    "databisorg": {
        "website": "https://data.bis.org",
        "has_api": "Yes",
        "has_bulk": "No",
        "sdmx": "Yes",
    },
    "datauniceforg": {
        "website": "https://data.unicef.org",
        "has_api": "Yes",
        "has_bulk": "No",
        "sdmx": "Yes",
    },
    "dataworldbankorg": {
        "website": "https://data.worldbank.org",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "ecb": {
        "website": "https://data.ecb.europa.eu",
        "has_api": "Yes",
        "has_bulk": "No",
        "sdmx": "Yes",
    },
    "eurostat": {
        "website": "https://ec.europa.eu/eurostat",
        "has_api": "Yes",
        "has_bulk": "No",
        "sdmx": "Yes",
    },
    "ilostat": {
        "website": "https://ilostat.ilo.org",
        "has_api": "Yes",
        "has_bulk": "No",
        "sdmx": "Yes",
    },
    "oracleapex": {
        "website": "https://apex.oracle.com",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "whoint": {
        "website": "https://www.who.int",
        "has_api": "Yes",
        "has_bulk": "No",
        "custom_api": "Yes",
    },
    "datavavt": {
        "website": "",
        "has_api": "Uncertain",
        "has_bulk": "Uncertain",
    },
    # Additional well-known software that might need updates
    "ckan": {
        "website": "https://ckan.org",
    },
    "dspace": {
        "website": "https://dspace.lyrasis.org",
    },
    "dataverse": {
        "website": "https://dataverse.org",
    },
    "geonetwork": {
        "website": "https://geonetwork-opensource.org",
    },
    "geoserver": {
        "website": "https://geoserver.org",
    },
    "figshare": {
        "website": "https://figshare.com",
    },
    "erddap": {
        "website": "https://upwell.pfeg.noaa.gov/erddap/information.html",
    },
    "galaxy": {
        "website": "https://galaxyproject.org",
    },
    "invenio": {
        "website": "https://inveniosoftware.org",
    },
    "inveniordm": {
        "website": "https://inveniosoftware.org/products/rdm/",
    },
    "eprints": {
        "website": "https://www.eprints.org",
    },
    "opendatasoft": {
        "website": "https://opendatasoft.com",
    },
    "socrata": {
        "website": "https://socrata.com",
    },
    "arcgishub": {
        "website": "https://hub.arcgis.com",
    },
    "arcgisserver": {
        "website": "https://enterprise.arcgis.com/en/server/",
    },
    "pycsw": {
        "website": "https://pycsw.org",
    },
    "pygeoapi": {
        "website": "https://www.pygeoapi.io",
    },
    "geonode": {
        "website": "https://geonode.org",
    },
    "opendatacube": {
        "website": "https://www.opendatacube.org",
    },
    "stacserver": {
        "website": "https://github.com/stac-utils/stac-server",
    },
    "opensdg": {
        "website": "https://open-sdg.org",
    },
    "pxweb": {
        "website": "https://www.scb.se/en/services/statistical-programs-for-px-files/px-web/",
    },
    "fusionregistry": {
        "website": "https://www.metadatatechnology.com/fusion-registry",
    },
    "hubzero": {
        "website": "https://hubzero.org",
    },
    "ipt": {
        "website": "https://www.gbif.org/ipt",
    },
    "obibamica": {
        "website": "https://www.obiba.org/pages/products/mica/",
    },
    "omegapsir": {
        "website": "https://www.omegaportal.org",
    },
    "qwc2": {
        "website": "https://github.com/qgis/qwc2",
    },
    "ibmcognos": {
        "website": "https://www.ibm.com/products/cognos-analytics",
    },
    "tablion": {
        "website": "",
    },
    "nextgisweb": {
        "website": "https://www.nextgis.com",
    },
    "custom": {
        "website": "",
    },
    # More software that might need website URLs
    "dkan": {
        "website": "https://www.getdkan.org",
    },
    "udata": {
        "website": "https://github.com/opendatateam/udata",
    },
    "jkan": {
        "website": "https://jkan.io",
    },
    "junar": {
        "website": "https://junar.com",
    },
    "triplydb": {
        "website": "https://triplydb.com",
    },
    "wordpress": {
        "website": "https://www.wordpress.org",
    },
    "drupal": {
        "website": "https://www.drupal.org",
    },
    "bitrix": {
        "website": "https://www.bitrix24.com",
    },
    "aleph": {
        "website": "https://github.com/alephdata/aleph",
    },
    "magda": {
        "website": "https://github.com/magda-io/magda",
    },
    "publishmydata": {
        "website": "https://www.publishmydata.com",
    },
    "entryscape": {
        "website": "https://entryscape.com",
    },
    "datapress": {
        "website": "https://datapress.com",
    },
    "d4science": {
        "website": "https://www.d4science.org",
    },
    "smw": {
        "website": "https://www.semantic-mediawiki.org",
    },
    "pomosam": {
        "website": "http://www.pomosam.sk",
    },
    "carto": {
        "website": "https://carto.com",
    },
    "koordinates": {
        "website": "https://koordinates.com",
    },
    "lizmap": {
        "website": "https://www.lizmap.com",
    },
    "mapproxy": {
        "website": "https://www.mapproxy.org",
    },
    "mapbender": {
        "website": "https://www.mapbender.org",
    },
    "oskari": {
        "website": "https://oskari.org",
    },
    "opengeoportal": {
        "website": "https://github.com/opengeoportal",
    },
    "geoblacklight": {
        "website": "https://geoblacklight.org",
    },
    "erdasapollo": {
        "website": "https://hexagon.com/products/erdas-apollo",
    },
    "elitegis": {
        "website": "https://atemiko.com",
    },
    "ewmapa": {
        "website": "",
    },
    "orbismap": {
        "website": "",
    },
    "metagis": {
        "website": "",
    },
    "esrigeo": {
        "website": "https://www.esri.com",
    },
    "geoportalrlp": {
        "website": "https://github.com/mrmap-community/GeoPortal.rlp",
    },
    "smartfindersdi": {
        "website": "https://www.conterra.de/portfolio/smartfinder-sdi",
    },
    "ncwms": {
        "website": "https://reading-escience-centre.github.io/ncwms/",
    },
    "mycore": {
        "website": "https://www.mycore.de",
    },
    "librecat": {
        "website": "https://librecat.org",
    },
    "vufind": {
        "website": "https://vufind.org",
    },
    "worktribe": {
        "website": "https://www.worktribe.com",
    },
    "weko3": {
        "website": "https://github.com/RCOSDP/weko",
    },
    "samvera": {
        "website": "https://samvera.org",
    },
    "mytardis": {
        "website": "https://www.mytardis.org",
    },
    "islandora": {
        "website": "https://islandora.ca",
    },
    "esploro": {
        "website": "https://www.exlibrisgroup.com/products/esploro-research-services-platform/",
    },
    "dlibra": {
        "website": "https://dlibra.psnc.pl",
    },
    "datalad": {
        "website": "https://www.datalad.org",
    },
    "converis": {
        "website": "https://www.clarivate.com/products/converis/",
    },
    "djehuty": {
        "website": "https://github.com/4TUResearchData/djehuty",
    },
    "dspacecris": {
        "website": "https://www.4science.com/dspace-cris/",
    },
    "elsevierpure": {
        "website": "https://www.elsevier.com/solutions/pure",
    },
    "elsevierdigitalcommons": {
        "website": "https://www.bepress.com/products/digital-commons/",
    },
    "dataone": {
        "website": "http://dataone.org",
    },
    "hyrax": {
        "website": "https://hyrax.samvera.org",
    },
    "ramadda": {
        "website": "https://www.ramadda.org",
    },
    "instdb": {
        "website": "https://market.csdb.cn/InstDB",
    },
    "nada": {
        "website": "https://nada.ihsn.org",
    },
    "colectica": {
        "website": "https://colectica.com",
    },
    "nesstar": {
        "website": "https://en.wikipedia.org/wiki/Nesstar",
    },
    "bicontour": {
        "website": "",
    },
    "datainsight": {
        "website": "https://www.veritas.com/insights/data-insight",
    },
}

def update_software_record(filepath: Path, metadata: Dict[str, Any], dry_run: bool = False) -> bool:
    """Update a software record with metadata."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            record = yaml.safe_load(f)
        
        if not record or 'id' not in record:
            return False
        
        changed = False
        software_id = record.get('id', '')
        
        # Update website (if empty or missing)
        if metadata.get('website') and metadata['website']:
            current_website = record.get('website', '').strip()
            if not current_website or current_website == '':
                record['website'] = metadata['website']
                changed = True
        
        # Update has_api
        if metadata.get('has_api') and record.get('has_api') == 'Uncertain':
            record['has_api'] = metadata['has_api']
            changed = True
        
        # Update has_bulk
        if metadata.get('has_bulk') and record.get('has_bulk') == 'Uncertain':
            record['has_bulk'] = metadata['has_bulk']
            changed = True
        
        # Update metadata_support
        if 'metadata_support' not in record:
            record['metadata_support'] = {}
        
        for key in ['custom_api', 'sdmx', 'wms', 'wfs', 'wcs', 'dcat', 'ckan_api', 'csw', 'oai-pmh']:
            if key in metadata and record['metadata_support'].get(key) == 'Uncertain':
                record['metadata_support'][key] = metadata[key]
                changed = True
        
        if changed and not dry_run:
            with open(filepath, 'w', encoding='utf-8') as f:
                yaml.safe_dump(record, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
            return True
        
        return changed
    
    except Exception as e:
        print(f"Error updating {filepath}: {e}")
        return False

def main(dry_run: bool = False):
    """Main function to update all software records."""
    updated_count = 0
    total_count = 0
    
    print(f"{'DRY RUN - ' if dry_run else ''}Updating software records...")
    print("=" * 80)
    
    for root, dirs, files in os.walk(SOFTWARE_DIR):
        for file in files:
            if file.endswith('.yaml') and not file.startswith('_'):
                filepath = Path(root) / file
                total_count += 1
                
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        record = yaml.safe_load(f)
                    
                    software_id = record.get('id', '')
                    if software_id in SOFTWARE_METADATA:
                        metadata = SOFTWARE_METADATA[software_id]
                        if update_software_record(filepath, metadata, dry_run):
                            updated_count += 1
                            status = "[DRY RUN] Would update" if dry_run else "Updated"
                            print(f"{status}: {software_id} - {record.get('name', 'Unknown')}")
                
                except Exception as e:
                    print(f"Error processing {filepath}: {e}")
    
    print("=" * 80)
    print(f"Total software records: {total_count}")
    print(f"{'Would update' if dry_run else 'Updated'}: {updated_count}")
    
    if dry_run:
        print("\nRun without --dry-run to apply changes")

if __name__ == "__main__":
    import sys
    dry_run = '--dry-run' in sys.argv or '-n' in sys.argv
    main(dry_run=dry_run)

