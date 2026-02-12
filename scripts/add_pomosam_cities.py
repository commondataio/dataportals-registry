#!/usr/bin/env python
# Script to add all Slovak cities from POMOSAM platform to the registry

import os
import re
import yaml
import logging
import requests
from urllib.parse import urlparse
from typing import Dict, List, Tuple, Optional
import copy

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Get script directory and repository root for path resolution
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)

ROOT_DIR = os.path.join(_REPO_ROOT, "data", "entities")
DATASETS_DIR = os.path.join(_REPO_ROOT, "data", "datasets")

# Slovak region codes mapping
# ISO 3166-2:SK codes
SLOVAK_REGIONS = {
    "SK-BC": "Banská Bystrica",
    "SK-BL": "Bratislava",
    "SK-KI": "Košice",
    "SK-PV": "Prešov",
    "SK-TA": "Trnava",
    "SK-TC": "Trenčín",
    "SK-ZI": "Žilina",
}

# Mapping of Slovak cities to region codes
# This is a comprehensive mapping of major Slovak cities to their regions
# Based on ISO 3166-2:SK region codes
CITY_TO_REGION = {
    # Banská Bystrica Region (SK-BC)
    "banska-bystrica": "SK-BC",
    "zvolen": "SK-BC",
    "lucenec": "SK-BC",
    "rimavska-sobota": "SK-BC",
    "brezno": "SK-BC",
    "revuca": "SK-BC",
    "poltar": "SK-BC",
    "detva": "SK-BC",
    "krupina": "SK-BC",
    "hrinova": "SK-BC",
    "modry-kamen": "SK-BC",
    "tisovec": "SK-BC",
    "velky-krtis": "SK-BC",
    "zeliezovce": "SK-BC",
    "filakovo": "SK-BC",
    "krupina": "SK-BC",
    
    # Bratislava Region (SK-BL)
    "bratislava": "SK-BL",
    "malacky": "SK-BL",
    "pezinok": "SK-BL",
    "senec": "SK-BL",
    "stupava": "SK-BL",
    "svaty-jur": "SK-BL",
    "modra": "SK-BL",
    "ivanka-pri-dunaji": "SK-BL",
    "samorin": "SK-BL",
    "dunajska-streda": "SK-BL",
    
    # Košice Region (SK-KI)
    "kosice": "SK-KI",
    "spisska-nova-ves": "SK-KI",
    "roznava": "SK-KI",
    "michalovce": "SK-KI",
    "trebisov": "SK-KI",
    "moldava-nad-bodvou": "SK-KI",
    "gelnica": "SK-KI",
    "krompachy": "SK-KI",
    "medzev": "SK-KI",
    "slavec": "SK-KI",
    "strazske": "SK-KI",
    "vranov-nad-toplou": "SK-KI",
    "kralovsky-chlmec": "SK-KI",
    
    # Prešov Region (SK-PV)
    "presov": "SK-PV",
    "poprad": "SK-PV",
    "bardejov": "SK-PV",
    "humenne": "SK-PV",
    "levoca": "SK-PV",
    "kezmarok": "SK-PV",
    "stara-lubovna": "SK-PV",
    "snina": "SK-PV",
    "sabinov": "SK-PV",
    "stropkov": "SK-PV",
    "medzilaborce": "SK-PV",
    "svidnik": "SK-PV",
    "spisske-podhradie": "SK-PV",
    
    # Trnava Region (SK-TA)
    "trnava": "SK-TA",
    "piestany": "SK-TA",
    "hlohovec": "SK-TA",
    "galanta": "SK-TA",
    "senica": "SK-TA",
    "skalica": "SK-TA",
    "nove-zamky": "SK-TA",
    "komarno": "SK-TA",
    "levice": "SK-TA",
    "nitra": "SK-TA",
    "sala": "SK-TA",
    "sturovo": "SK-TA",
    "topolcany": "SK-TA",
    
    # Trenčín Region (SK-TC)
    "trencin": "SK-TC",
    "puchov": "SK-TC",
    "prievidza": "SK-TC",
    "partizanske": "SK-TC",
    "povazska-bystrica": "SK-TC",
    "ilava": "SK-TC",
    "myjava": "SK-TC",
    "banovce-nad-bebravou": "SK-TC",
    "novaky": "SK-TC",
    "handlova": "SK-TC",
    "bojnice": "SK-TC",
    "nove-mesto-nad-vahom": "SK-TC",
    
    # Žilina Region (SK-ZI)
    "zilina": "SK-ZI",
    "martin": "SK-ZI",
    "liptovsky-mikulas": "SK-ZI",
    "ruzomberok": "SK-ZI",
    "cadca": "SK-ZI",
    "dolny-kubin": "SK-ZI",
    "namestovo": "SK-ZI",
    "tvrdosin": "SK-ZI",
    "kysucke-nove-mesto": "SK-ZI",
    "stara-tura": "SK-ZI",
    "bytca": "SK-ZI",
    "krasno-nad-kysucou": "SK-ZI",
}

# Entry template based on existing POMOSAM entries
ENTRY_TEMPLATE = {
    "access_mode": ["open"],
    "api": True,
    "api_status": "active",
    "catalog_type": "Open data portal",
    "content_types": ["dataset"],
    "coverage": [],
    "langs": [{"id": "SK", "name": "Slovak"}],
    "owner": {
        "type": "Local government",
    },
    "properties": {
        "has_doi": False,
        "transferable_location": True,
    },
    "rights": {
        "license_id": None,
        "license_name": None,
        "license_url": None,
        "privacy_policy_url": None,
        "rights_type": "granular",
        "tos_url": None,
    },
    "software": {
        "id": "pomosam",
        "name": "POMOSAM",
    },
    "status": "active",
    "tags": [
        "government",
        "has_api",
        "eGovernment",
        "local data",
        "Slovakia",
        "municipality",
    ],
    "topics": [
        {"id": "REGI", "name": "Regions and cities", "type": "eudatatheme"},
        {"id": "GOVE", "name": "Government and public sector", "type": "eudatatheme"},
        {"id": "Society", "name": "Society", "type": "iso19115"},
        {"id": "Location", "name": "Location", "type": "iso19115"},
        {"id": "Boundaries", "name": "Boundaries", "type": "iso19115"},
    ],
}


def remove_diacritics(text: str) -> str:
    """Remove Slovak diacritics from text"""
    # Slovak diacritics mapping
    diacritics_map = {
        'á': 'a', 'ä': 'a', 'č': 'c', 'ď': 'd', 'é': 'e', 'í': 'i', 'ľ': 'l',
        'ĺ': 'l', 'ň': 'n', 'ó': 'o', 'ô': 'o', 'ŕ': 'r', 'š': 's', 'ť': 't',
        'ú': 'u', 'ý': 'y', 'ž': 'z',
        'Á': 'A', 'Ä': 'A', 'Č': 'C', 'Ď': 'D', 'É': 'E', 'Í': 'I', 'Ľ': 'L',
        'Ĺ': 'L', 'Ň': 'N', 'Ó': 'O', 'Ô': 'O', 'Ŕ': 'R', 'Š': 'S', 'Ť': 'T',
        'Ú': 'U', 'Ý': 'Y', 'Ž': 'Z',
    }
    result = text
    for diacritic, replacement in diacritics_map.items():
        result = result.replace(diacritic, replacement)
    return result


def normalize_city_name(city_name: str) -> str:
    """Normalize city name for URL and ID generation"""
    # Remove diacritics first
    city_name = remove_diacritics(city_name)
    # Convert to lowercase
    city_name = city_name.lower()
    # Replace spaces and special characters with hyphens
    city_name = re.sub(r'[^a-z0-9]+', '-', city_name)
    # Remove leading/trailing hyphens
    city_name = city_name.strip('-')
    return city_name


def get_city_region(city_name: str) -> Optional[str]:
    """Get region code for a city name"""
    # Try multiple normalization approaches
    normalized = normalize_city_name(city_name)
    
    # First try exact match
    if normalized in CITY_TO_REGION:
        return CITY_TO_REGION[normalized]
    
    # Try without hyphens (for cases like "banskabystrica")
    normalized_no_hyphen = normalized.replace('-', '')
    for key, value in CITY_TO_REGION.items():
        if key.replace('-', '') == normalized_no_hyphen:
            return value
    
    # Try matching by extracting city domain from URL if available
    # This is a fallback - we'll handle it in create_city_record if URL is available
    return None


def scrape_pomosam_website() -> List[Tuple[str, str]]:
    """
    Scrape POMOSAM website to get list of cities and their URLs.
    Returns list of tuples: (city_name, url)
    """
    cities = []
    if not HAS_BS4:
        logger.warning("BeautifulSoup4 not available, skipping web scraping")
        return cities
    
    try:
        response = requests.get("http://www.pomosam.sk/", timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for links to egov.*.sk domains
        links = soup.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            # Check if it's an egov.*.sk link
            if 'egov.' in href and '.sk' in href:
                # Extract city name from URL
                match = re.search(r'egov\.([^.]+)\.sk', href)
                if match:
                    city_domain = match.group(1)
                    city_name = city_domain.replace('-', ' ')
                    url = f"https://egov.{city_domain}.sk/default.aspx?NavigationState=1100:0:"
                    cities.append((city_name, url))
        
        # Also check for text content that might list cities
        text_content = soup.get_text()
        # Look for patterns like "egov.cityname.sk"
        matches = re.findall(r'egov\.([a-z0-9-]+)\.sk', text_content, re.IGNORECASE)
        for match in matches:
            city_domain = match.lower()
            city_name = city_domain.replace('-', ' ')
            url = f"https://egov.{city_domain}.sk/default.aspx?NavigationState=1100:0:"
            if (city_name, url) not in cities:
                cities.append((city_name, url))
        
        logger.info(f"Found {len(cities)} cities from POMOSAM website")
    except Exception as e:
        logger.warning(f"Failed to scrape POMOSAM website: {e}")
        logger.info("Will use fallback list of known cities")
    
    return cities


def get_known_slovak_cities() -> List[Tuple[str, str]]:
    """
    Fallback list of known Slovak cities with POMOSAM portals.
    Returns list of tuples: (city_name, url)
    """
    # Common Slovak cities known to use POMOSAM
    cities = [
        ("Žilina", "https://egov.zilina.sk/default.aspx?NavigationState=1100:0:"),
        ("Trenčín", "https://egov.trencin.sk/default.aspx?NavigationState=1100:0:"),
        ("Bratislava", "https://egov.bratislava.sk/default.aspx?NavigationState=1100:0:"),
        ("Košice", "https://egov.kosice.sk/default.aspx?NavigationState=1100:0:"),
        ("Prešov", "https://egov.presov.sk/default.aspx?NavigationState=1100:0:"),
        ("Trnava", "https://egov.trnava.sk/default.aspx?NavigationState=1100:0:"),
        ("Banská Bystrica", "https://egov.banska-bystrica.sk/default.aspx?NavigationState=1100:0:"),
        ("Nitra", "https://egov.nitra.sk/default.aspx?NavigationState=1100:0:"),
        ("Martin", "https://egov.martin.sk/default.aspx?NavigationState=1100:0:"),
        ("Poprad", "https://egov.poprad.sk/default.aspx?NavigationState=1100:0:"),
        ("Prievidza", "https://egov.prievidza.sk/default.aspx?NavigationState=1100:0:"),
        ("Zvolen", "https://egov.zvolen.sk/default.aspx?NavigationState=1100:0:"),
        ("Považská Bystrica", "https://egov.povazska-bystrica.sk/default.aspx?NavigationState=1100:0:"),
        ("Nové Zámky", "https://egov.nove-zamky.sk/default.aspx?NavigationState=1100:0:"),
        ("Michalovce", "https://egov.michalovce.sk/default.aspx?NavigationState=1100:0:"),
        ("Spišská Nová Ves", "https://egov.spisska-nova-ves.sk/default.aspx?NavigationState=1100:0:"),
        ("Komárno", "https://egov.komarno.sk/default.aspx?NavigationState=1100:0:"),
        ("Humenne", "https://egov.humenne.sk/default.aspx?NavigationState=1100:0:"),
        ("Levice", "https://egov.levice.sk/default.aspx?NavigationState=1100:0:"),
        ("Bardejov", "https://egov.bardejov.sk/default.aspx?NavigationState=1100:0:"),
        ("Liptovský Mikuláš", "https://egov.liptovsky-mikulas.sk/default.aspx?NavigationState=1100:0:"),
        ("Ružomberok", "https://egov.ruzomberok.sk/default.aspx?NavigationState=1100:0:"),
        ("Piešťany", "https://egov.piestany.sk/default.aspx?NavigationState=1100:0:"),
        ("Topoľčany", "https://egov.topolcany.sk/default.aspx?NavigationState=1100:0:"),
        ("Čadca", "https://egov.cadca.sk/default.aspx?NavigationState=1100:0:"),
        ("Rimavská Sobota", "https://egov.rimavska-sobota.sk/default.aspx?NavigationState=1100:0:"),
        ("Dunajská Streda", "https://egov.dunajska-streda.sk/default.aspx?NavigationState=1100:0:"),
        ("Pezinok", "https://egov.pezinok.sk/default.aspx?NavigationState=1100:0:"),
        ("Partizánske", "https://egov.partizanske.sk/default.aspx?NavigationState=1100:0:"),
        ("Hlohovec", "https://egov.hlohovec.sk/default.aspx?NavigationState=1100:0:"),
        ("Vranov nad Topľou", "https://egov.vranov-nad-toplou.sk/default.aspx?NavigationState=1100:0:"),
        ("Senica", "https://egov.senica.sk/default.aspx?NavigationState=1100:0:"),
        ("Nové Mesto nad Váhom", "https://egov.nove-mesto-nad-vahom.sk/default.aspx?NavigationState=1100:0:"),
        ("Kežmarok", "https://egov.kezmarok.sk/default.aspx?NavigationState=1100:0:"),
        ("Rožňava", "https://egov.roznava.sk/default.aspx?NavigationState=1100:0:"),
        ("Dolný Kubín", "https://egov.dolny-kubin.sk/default.aspx?NavigationState=1100:0:"),
        ("Stará Ľubovňa", "https://egov.stara-lubovna.sk/default.aspx?NavigationState=1100:0:"),
        ("Banská Štiavnica", "https://egov.banska-stiavnica.sk/default.aspx?NavigationState=1100:0:"),
        ("Skalica", "https://egov.skalica.sk/default.aspx?NavigationState=1100:0:"),
        ("Lučenec", "https://egov.lucenec.sk/default.aspx?NavigationState=1100:0:"),
        ("Snina", "https://egov.snina.sk/default.aspx?NavigationState=1100:0:"),
        ("Trebisov", "https://egov.trebisov.sk/default.aspx?NavigationState=1100:0:"),
        ("Revúca", "https://egov.revuca.sk/default.aspx?NavigationState=1100:0:"),
        ("Myjava", "https://egov.myjava.sk/default.aspx?NavigationState=1100:0:"),
        ("Veľký Krtíš", "https://egov.velky-krtis.sk/default.aspx?NavigationState=1100:0:"),
        ("Detva", "https://egov.detva.sk/default.aspx?NavigationState=1100:0:"),
        ("Krupina", "https://egov.krupina.sk/default.aspx?NavigationState=1100:0:"),
        ("Šaľa", "https://egov.sala.sk/default.aspx?NavigationState=1100:0:"),
        ("Stropkov", "https://egov.stropkov.sk/default.aspx?NavigationState=1100:0:"),
        ("Brezno", "https://egov.brezno.sk/default.aspx?NavigationState=1100:0:"),
        ("Sabinov", "https://egov.sabinov.sk/default.aspx?NavigationState=1100:0:"),
        ("Stará Turá", "https://egov.stara-tura.sk/default.aspx?NavigationState=1100:0:"),
        ("Fiľakovo", "https://egov.filakovo.sk/default.aspx?NavigationState=1100:0:"),
        ("Svidník", "https://egov.svidnik.sk/default.aspx?NavigationState=1100:0:"),
        ("Krásno nad Kysucou", "https://egov.krasno-nad-kysucou.sk/default.aspx?NavigationState=1100:0:"),
        ("Gelnica", "https://egov.gelnica.sk/default.aspx?NavigationState=1100:0:"),
        ("Kysucké Nové Mesto", "https://egov.kysucke-nove-mesto.sk/default.aspx?NavigationState=1100:0:"),
        ("Medzilaborce", "https://egov.medzilaborce.sk/default.aspx?NavigationState=1100:0:"),
        ("Štúrovo", "https://egov.sturovo.sk/default.aspx?NavigationState=1100:0:"),
        ("Tvrdošín", "https://egov.tvrdosin.sk/default.aspx?NavigationState=1100:0:"),
        ("Námestovo", "https://egov.namestovo.sk/default.aspx?NavigationState=1100:0:"),
        ("Bytča", "https://egov.bytca.sk/default.aspx?NavigationState=1100:0:"),
        ("Spišské Podhradie", "https://egov.spisske-podhradie.sk/default.aspx?NavigationState=1100:0:"),
        ("Levoča", "https://egov.levoca.sk/default.aspx?NavigationState=1100:0:"),
    ]
    
    return cities


def get_existing_entries() -> set:
    """Get set of existing entry IDs to avoid duplicates"""
    existing = set()
    
    # Check existing YAML files
    if os.path.exists(ROOT_DIR):
        for root, dirs, files in os.walk(ROOT_DIR):
            for filename in files:
                if filename.endswith('.yaml'):
                    filepath = os.path.join(root, filename)
                    try:
                        with open(filepath, 'r', encoding='utf8') as f:
                            record = yaml.safe_load(f)
                            if record and 'id' in record:
                                existing.add(record['id'])
                    except Exception as e:
                        logger.warning(f"Error reading {filepath}: {e}")
    
    # Also check from JSONL files if they exist
    jsonl_file = os.path.join(DATASETS_DIR, "full.jsonl")
    if os.path.exists(jsonl_file):
        try:
            import json
            with open(jsonl_file, 'r', encoding='utf8') as f:
                for line in f:
                    if line.strip():
                        try:
                            record = json.loads(line)
                            if 'id' in record:
                                existing.add(record['id'])
                        except:
                            pass
        except Exception as e:
            logger.warning(f"Error reading JSONL file: {e}")
    
    return existing


def generate_record_id(url: str) -> str:
    """Generate record ID from URL"""
    domain = urlparse(url).netloc.lower()
    record_id = domain.split(":", 1)[0].replace("_", "").replace("-", "").replace(".", "")
    return record_id


def create_city_record(city_name: str, url: str, region_code: Optional[str] = None) -> dict:
    """Create a YAML record for a city"""
    record = copy.deepcopy(ENTRY_TEMPLATE)
    
    # Generate record ID
    record_id = generate_record_id(url)
    record["id"] = record_id
    
    # Set link
    record["link"] = url
    
    # Set name - use city name
    record["name"] = f"Open Data {city_name}"
    
    # Set description
    record["description"] = f"Data catalog for the city of {city_name}, Slovakia"
    
    # Determine region code if not provided
    if not region_code:
        region_code = get_city_region(city_name)
        
        # If still not found, try to extract from URL domain
        if not region_code:
            domain = urlparse(url).netloc.lower()
            # Extract city domain part (e.g., "kosice" from "egov.kosice.sk")
            match = re.search(r'egov\.([^.]+)\.sk', domain)
            if match:
                city_domain = match.group(1)
                region_code = CITY_TO_REGION.get(city_domain)
    
    # Set location
    location = {
        "location": {
            "country": {"id": "SK", "name": "Slovakia"},
            "level": 30,
            "macroregion": {"id": "151", "name": "Eastern Europe"},
        }
    }
    
    if region_code:
        region_name = SLOVAK_REGIONS.get(region_code, region_code)
        location["location"]["subregion"] = {
            "id": region_code,
            "name": region_name,
        }
    
    record["coverage"].append(location)
    
    # Set owner (schema allows only country, level, subregion — not macroregion)
    owner_location = {k: v for k, v in location["location"].items() if k != "macroregion"}
    record["owner"]["location"] = owner_location
    record["owner"]["name"] = f"Mesto {city_name}"
    # Extract city domain for owner link
    domain = urlparse(url).netloc
    city_domain = domain.replace('egov.', 'www.')
    record["owner"]["link"] = f"https://{city_domain}"
    
    # Set endpoints
    domain_base = urlparse(url).netloc
    record["endpoints"] = [
        {
            "type": "sitemap",
            "url": f"https://{domain_base}/sitemap.xml",
        }
    ]
    
    return record, region_code


def save_record(record: dict, region_code: Optional[str]) -> bool:
    """Save record to appropriate directory"""
    # Determine directory structure
    country_dir = os.path.join(ROOT_DIR, "SK")
    if not os.path.exists(country_dir):
        os.makedirs(country_dir, exist_ok=True)
    
    # Use region code if available, otherwise use Federal
    if region_code:
        region_dir = os.path.join(country_dir, region_code)
    else:
        region_dir = os.path.join(country_dir, "Federal")
    
    if not os.path.exists(region_dir):
        os.makedirs(region_dir, exist_ok=True)
    
    # Create opendata subdirectory
    opendata_dir = os.path.join(region_dir, "opendata")
    if not os.path.exists(opendata_dir):
        os.makedirs(opendata_dir, exist_ok=True)
    
    # Save file
    filename = os.path.join(opendata_dir, f"{record['id']}.yaml")
    
    try:
        with open(filename, 'w', encoding='utf8') as f:
            yaml.safe_dump(record, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
        logger.info(f"Saved: {filename}")
        return True
    except Exception as e:
        logger.error(f"Failed to save {filename}: {e}")
        return False


def main(dryrun: bool = False):
    """Main function to add all POMOSAM cities"""
    logger.info("Starting POMOSAM cities addition")
    
    # Get existing entries to avoid duplicates
    existing = get_existing_entries()
    logger.info(f"Found {len(existing)} existing entries")
    
    # Try to scrape website first, fallback to known list
    cities = scrape_pomosam_website()
    if not cities:
        logger.info("Using fallback list of known cities")
        cities = get_known_slovak_cities()
    
    logger.info(f"Processing {len(cities)} cities")
    
    added = 0
    skipped = 0
    errors = 0
    
    for city_name, url in cities:
        try:
            # Generate record ID
            record_id = generate_record_id(url)
            
            # Check if already exists
            if record_id in existing:
                logger.info(f"Skipping {city_name} (already exists: {record_id})")
                skipped += 1
                continue
            
            # Get region code
            region_code = get_city_region(city_name)
            if not region_code:
                logger.warning(f"Could not determine region for {city_name}, will use Federal")
            
            # Create record
            record, final_region_code = create_city_record(city_name, url, region_code)
            
            if dryrun:
                logger.info(f"[DRYRUN] Would add: {city_name} -> {record_id} (region: {final_region_code})")
                added += 1
            else:
                # Save record
                if save_record(record, final_region_code):
                    added += 1
                    existing.add(record_id)  # Add to existing set to avoid duplicates in same run
                else:
                    errors += 1
                    
        except Exception as e:
            logger.error(f"Error processing {city_name}: {e}")
            errors += 1
    
    logger.info(f"Completed: {added} added, {skipped} skipped, {errors} errors")
    return added, skipped, errors


if __name__ == "__main__":
    import sys
    
    dryrun = "--dryrun" in sys.argv or "-d" in sys.argv
    
    if dryrun:
        logger.info("Running in DRYRUN mode - no files will be created")
    
    added, skipped, errors = main(dryrun=dryrun)
    
    if dryrun:
        print(f"\nDRYRUN Results: {added} would be added, {skipped} would be skipped")
    else:
        print(f"\nResults: {added} added, {skipped} skipped, {errors} errors")
    
    sys.exit(0 if errors == 0 else 1)

