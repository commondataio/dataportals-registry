#!/usr/bin/env python3
"""
Script to fix IMPORTANT priority issues:
- MISSING_OWNER_LOCATION
- MISSING_OWNER_NAME
- MISSING_OWNER_TYPE
- COVERAGE_NORMALIZATION (add macroregion)
- INCONSISTENT_LICENSE (add license_url)
- MISSING_API_STATUS (add api_status)
- SOFTWARE_ID_UNKNOWN (map to custom or verify)
- SOFTWARE_NAME_MISMATCH (fix name)
"""
import csv
import re
import yaml
from pathlib import Path
from urllib.parse import urlparse
from typing import Dict, Optional, Tuple

# Base directories
BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
SOFTWARE_DIR = BASE_DIR / "data" / "software"
MACROREGION_FILE = BASE_DIR / "data" / "reference" / "macroregion_countries.tsv"

# License URL mappings
LICENSE_URL_MAP = {
    "Open Government Licence v3.0": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
    "Open Government Licence": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
    "OGL": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
    "Creative Commons Atribuição-SemDerivações 3.0": "https://creativecommons.org/licenses/by-nd/3.0/",
    "CC BY-ND 3.0": "https://creativecommons.org/licenses/by-nd/3.0/",
    "Creative Commons Attribution-NoDerivatives 3.0": "https://creativecommons.org/licenses/by-nd/3.0/",
}

# Software ID mappings for unknown IDs (only for IDs with no software definition)
SOFTWARE_ID_MAPPING = {}

def load_macroregion_dict() -> Dict[str, Dict[str, str]]:
    """Load macroregion mapping from TSV file"""
    macroregion_dict = {}
    if MACROREGION_FILE.exists():
        with open(MACROREGION_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                alpha2 = row.get('alpha2', '').strip()
                if alpha2:
                    macroregion_dict[alpha2] = {
                        'macroregion_code': row.get('macroregion_code', '').strip(),
                        'macroregion_name': row.get('macroregion_name', '').strip(),
                    }
    return macroregion_dict

def load_software_definitions() -> Dict[str, Dict]:
    """Load all software definitions from data/software directory."""
    software = {}
    
    for software_file in SOFTWARE_DIR.rglob("*.yaml"):
        if software_file.name == "_template.tmpl":
            continue
            
        try:
            with open(software_file, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                if data and "id" in data:
                    software[data["id"]] = {
                        "id": data["id"],
                        "name": data.get("name", ""),
                    }
        except Exception as e:
            print(f"Error loading {software_file}: {e}")
    
    return software

def infer_country_from_coverage(record):
    """Try to infer country from record's coverage"""
    coverage = record.get("coverage", [])
    if coverage and isinstance(coverage, list) and len(coverage) > 0:
        location = coverage[0].get("location", {})
        country = location.get("country", {})
        country_id = country.get("id")
        country_name = country.get("name")
        if country_id and country_id != "Unknown":
            return {
                "id": country_id,
                "name": country_name or country_id
            }
    return None

def infer_country_from_link(link):
    """Try to infer country from domain TLD"""
    if not link:
        return None
    
    try:
        parsed = urlparse(link)
        domain = parsed.netloc or link
        domain = domain.lower()
        
        # Common country TLDs
        country_tlds = {
            '.gov': 'US', '.gov.uk': 'GB', '.gov.au': 'AU', '.gov.ca': 'CA',
            '.gov.nz': 'NZ', '.gov.sg': 'SG', '.gov.in': 'IN', '.gov.br': 'BR',
            '.gov.mx': 'MX', '.gov.ar': 'AR', '.gov.co': 'CO', '.gov.cl': 'CL',
            '.gov.pe': 'PE', '.gov.ec': 'EC', '.gov.uy': 'UY', '.gov.py': 'PY',
            '.gov.ve': 'VE', '.gov.bo': 'BO', '.gov.ec': 'EC', '.gov.gt': 'GT',
            '.gov.hn': 'HN', '.gov.ni': 'NI', '.gov.pa': 'PA', '.gov.cr': 'CR',
            '.gov.do': 'DO', '.gov.cu': 'CU', '.gov.jm': 'JM', '.gov.tt': 'TT',
            '.gov.za': 'ZA', '.gov.ng': 'NG', '.gov.ke': 'KE', '.gov.gh': 'GH',
            '.gov.tz': 'TZ', '.gov.ug': 'UG', '.gov.rw': 'RW', '.gov.et': 'ET',
            '.gov.eg': 'EG', '.gov.ma': 'MA', '.gov.tn': 'TN', '.gov.dz': 'DZ',
            '.gov.ly': 'LY', '.gov.sd': 'SD', '.gov.sn': 'SN', '.gov.ci': 'CI',
            '.gov.bf': 'BF', '.gov.ml': 'ML', '.gov.ne': 'NE', '.gov.td': 'TD',
            '.gov.cm': 'CM', '.gov.cg': 'CG', '.gov.cd': 'CD', '.gov.cf': 'CF',
            '.gov.ga': 'GA', '.gov.gq': 'GQ', '.gov.st': 'ST', '.gov.ao': 'AO',
            '.gov.mz': 'MZ', '.gov.mw': 'MW', '.gov.zm': 'ZM', '.gov.bw': 'BW',
            '.gov.na': 'NA', '.gov.ls': 'LS', '.gov.sz': 'SZ', '.gov.zw': 'ZW',
            '.gov.bi': 'BI', '.gov.dj': 'DJ', '.gov.er': 'ER', '.gov.so': 'SO',
            '.gov.ss': 'SS', '.gov.mu': 'MU', '.gov.sc': 'SC', '.gov.km': 'KM',
            '.gov.mg': 'MG', '.gov.mv': 'MV', '.gov.lk': 'LK', '.gov.bd': 'BD',
            '.gov.mm': 'MM', '.gov.th': 'TH', '.gov.la': 'LA', '.gov.kh': 'KH',
            '.gov.vn': 'VN', '.gov.ph': 'PH', '.gov.my': 'MY', '.gov.bn': 'BN',
            '.gov.id': 'ID', '.gov.sg': 'SG', '.gov.tw': 'TW', '.gov.hk': 'HK',
            '.gov.mo': 'MO', '.gov.jp': 'JP', '.gov.kr': 'KR', '.gov.cn': 'CN',
            '.gov.mn': 'MN', '.gov.kz': 'KZ', '.gov.kg': 'KG', '.gov.tj': 'TJ',
            '.gov.uz': 'UZ', '.gov.tm': 'TM', '.gov.af': 'AF', '.gov.pk': 'PK',
            '.gov.in': 'IN', '.gov.np': 'NP', '.gov.bt': 'BT', '.gov.lk': 'LK',
            '.gov.mv': 'MV', '.gov.ir': 'IR', '.gov.iq': 'IQ', '.gov.sy': 'SY',
            '.gov.lb': 'LB', '.gov.jo': 'JO', '.gov.il': 'IL', '.gov.ps': 'PS',
            '.gov.sa': 'SA', '.gov.ae': 'AE', '.gov.om': 'OM', '.gov.ye': 'YE',
            '.gov.bh': 'BH', '.gov.qa': 'QA', '.gov.kw': 'KW', '.gov.tr': 'TR',
            '.gov.cy': 'CY', '.gov.gr': 'GR', '.gov.bg': 'BG', '.gov.ro': 'RO',
            '.gov.md': 'MD', '.gov.ua': 'UA', '.gov.by': 'BY', '.gov.lt': 'LT',
            '.gov.lv': 'LV', '.gov.ee': 'EE', '.gov.pl': 'PL', '.gov.cz': 'CZ',
            '.gov.sk': 'SK', '.gov.hu': 'HU', '.gov.si': 'SI', '.gov.hr': 'HR',
            '.gov.ba': 'BA', '.gov.rs': 'RS', '.gov.me': 'ME', '.gov.mk': 'MK',
            '.gov.al': 'AL', '.gov.xk': 'XK', '.gov.it': 'IT', '.gov.es': 'ES',
            '.gov.pt': 'PT', '.gov.fr': 'FR', '.gov.be': 'BE', '.gov.nl': 'NL',
            '.gov.lu': 'LU', '.gov.de': 'DE', '.gov.at': 'AT', '.gov.ch': 'CH',
            '.gov.li': 'LI', '.gov.ad': 'AD', '.gov.mc': 'MC', '.gov.sm': 'SM',
            '.gov.va': 'VA', '.gov.fi': 'FI', '.gov.se': 'SE', '.gov.no': 'NO',
            '.gov.dk': 'DK', '.gov.is': 'IS', '.gov.ie': 'IE', '.gov.gb': 'GB',
            '.gov.uk': 'GB', '.gov.nz': 'NZ', '.gov.au': 'AU', '.gov.fj': 'FJ',
            '.gov.pg': 'PG', '.gov.sb': 'SB', '.gov.vu': 'VU', '.gov.nc': 'NC',
            '.gov.pf': 'PF', '.gov.ws': 'WS', '.gov.to': 'TO', '.gov.ck': 'CK',
            '.gov.nu': 'NU', '.gov.tk': 'TK', '.gov.fm': 'FM', '.gov.mh': 'MH',
            '.gov.pw': 'PW', '.gov.pg': 'PG', '.gov.nr': 'NR', '.gov.ki': 'KI',
            '.gov.tv': 'TV', '.gov.as': 'AS', '.gov.gu': 'GU', '.gov.mp': 'MP',
            '.gov.vi': 'VI', '.gov.pr': 'PR', '.gov.um': 'UM', '.gov.ax': 'AX',
            '.gov.fo': 'FO', '.gov.gl': 'GL', '.gov.sj': 'SJ', '.gov.bv': 'BV',
            '.gov.gs': 'GS', '.gov.tf': 'TF', '.gov.hm': 'HM', '.gov.cc': 'CC',
            '.gov.cx': 'CX', '.gov.nf': 'NF', '.gov.pn': 'PN', '.gov.sh': 'SH',
            '.gov.ac': 'AC', '.gov.ta': 'TA', '.gov.io': 'IO', '.gov.dg': 'DG',
            '.edu': 'US', '.edu.au': 'AU', '.edu.uk': 'GB', '.edu.ca': 'CA',
            '.edu.nz': 'NZ', '.edu.sg': 'SG', '.edu.in': 'IN', '.edu.br': 'BR',
            '.edu.mx': 'MX', '.edu.ar': 'AR', '.edu.co': 'CO', '.edu.cl': 'CL',
            '.edu.pe': 'PE', '.edu.ec': 'EC', '.edu.uy': 'UY', '.edu.py': 'PY',
            '.edu.ve': 'VE', '.edu.bo': 'BO', '.edu.gt': 'GT', '.edu.hn': 'HN',
            '.edu.ni': 'NI', '.edu.pa': 'PA', '.edu.cr': 'CR', '.edu.do': 'DO',
            '.edu.cu': 'CU', '.edu.jm': 'JM', '.edu.tt': 'TT', '.edu.za': 'ZA',
            '.edu.ng': 'NG', '.edu.ke': 'KE', '.edu.gh': 'GH', '.edu.tz': 'TZ',
            '.edu.ug': 'UG', '.edu.rw': 'RW', '.edu.et': 'ET', '.edu.eg': 'EG',
            '.edu.ma': 'MA', '.edu.tn': 'TN', '.edu.dz': 'DZ', '.edu.ly': 'LY',
            '.edu.sd': 'SD', '.edu.sn': 'SN', '.edu.ci': 'CI', '.edu.bf': 'BF',
            '.edu.ml': 'ML', '.edu.ne': 'NE', '.edu.td': 'TD', '.edu.cm': 'CM',
            '.edu.cg': 'CG', '.edu.cd': 'CD', '.edu.cf': 'CF', '.edu.ga': 'GA',
            '.edu.gq': 'GQ', '.edu.st': 'ST', '.edu.ao': 'AO', '.edu.mz': 'MZ',
            '.edu.mw': 'MW', '.edu.zm': 'ZM', '.edu.bw': 'BW', '.edu.na': 'NA',
            '.edu.ls': 'LS', '.edu.sz': 'SZ', '.edu.zw': 'ZW', '.edu.bi': 'BI',
            '.edu.dj': 'DJ', '.edu.er': 'ER', '.edu.so': 'SO', '.edu.ss': 'SS',
            '.edu.mu': 'MU', '.edu.sc': 'SC', '.edu.km': 'KM', '.edu.mg': 'MG',
            '.edu.mv': 'MV', '.edu.lk': 'LK', '.edu.bd': 'BD', '.edu.mm': 'MM',
            '.edu.th': 'TH', '.edu.la': 'LA', '.edu.kh': 'KH', '.edu.vn': 'VN',
            '.edu.ph': 'PH', '.edu.my': 'MY', '.edu.bn': 'BN', '.edu.id': 'ID',
            '.edu.sg': 'SG', '.edu.tw': 'TW', '.edu.hk': 'HK', '.edu.mo': 'MO',
            '.edu.jp': 'JP', '.edu.kr': 'KR', '.edu.cn': 'CN', '.edu.mn': 'MN',
            '.edu.kz': 'KZ', '.edu.kg': 'KG', '.edu.tj': 'TJ', '.edu.uz': 'UZ',
            '.edu.tm': 'TM', '.edu.af': 'AF', '.edu.pk': 'PK', '.edu.in': 'IN',
            '.edu.np': 'NP', '.edu.bt': 'BT', '.edu.lk': 'LK', '.edu.mv': 'MV',
            '.edu.ir': 'IR', '.edu.iq': 'IQ', '.edu.sy': 'SY', '.edu.lb': 'LB',
            '.edu.jo': 'JO', '.edu.il': 'IL', '.edu.ps': 'PS', '.edu.sa': 'SA',
            '.edu.ae': 'AE', '.edu.om': 'OM', '.edu.ye': 'YE', '.edu.bh': 'BH',
            '.edu.qa': 'QA', '.edu.kw': 'KW', '.edu.tr': 'TR', '.edu.cy': 'CY',
            '.edu.gr': 'GR', '.edu.bg': 'BG', '.edu.ro': 'RO', '.edu.md': 'MD',
            '.edu.ua': 'UA', '.edu.by': 'BY', '.edu.lt': 'LT', '.edu.lv': 'LV',
            '.edu.ee': 'EE', '.edu.pl': 'PL', '.edu.cz': 'CZ', '.edu.sk': 'SK',
            '.edu.hu': 'HU', '.edu.si': 'SI', '.edu.hr': 'HR', '.edu.ba': 'BA',
            '.edu.rs': 'RS', '.edu.me': 'ME', '.edu.mk': 'MK', '.edu.al': 'AL',
            '.edu.xk': 'XK', '.edu.it': 'IT', '.edu.es': 'ES', '.edu.pt': 'PT',
            '.edu.fr': 'FR', '.edu.be': 'BE', '.edu.nl': 'NL', '.edu.lu': 'LU',
            '.edu.de': 'DE', '.edu.at': 'AT', '.edu.ch': 'CH', '.edu.li': 'LI',
            '.edu.ad': 'AD', '.edu.mc': 'MC', '.edu.sm': 'SM', '.edu.va': 'VA',
            '.edu.fi': 'FI', '.edu.se': 'SE', '.edu.no': 'NO', '.edu.dk': 'DK',
            '.edu.is': 'IS', '.edu.ie': 'IE', '.edu.gb': 'GB', '.edu.uk': 'GB',
            '.edu.nz': 'NZ', '.edu.au': 'AU', '.edu.fj': 'FJ', '.edu.pg': 'PG',
            '.edu.sb': 'SB', '.edu.vu': 'VU', '.edu.nc': 'NC', '.edu.pf': 'PF',
            '.edu.ws': 'WS', '.edu.to': 'TO', '.edu.ck': 'CK', '.edu.nu': 'NU',
            '.edu.tk': 'TK', '.edu.fm': 'FM', '.edu.mh': 'MH', '.edu.pw': 'PW',
            '.edu.pg': 'PG', '.edu.nr': 'NR', '.edu.ki': 'KI', '.edu.tv': 'TV',
            '.edu.as': 'AS', '.edu.gu': 'GU', '.edu.mp': 'MP', '.edu.vi': 'VI',
            '.edu.pr': 'PR', '.edu.um': 'UM', '.edu.ax': 'AX', '.edu.fo': 'FO',
            '.edu.gl': 'GL', '.edu.sj': 'SJ', '.edu.bv': 'BV', '.edu.gs': 'GS',
            '.edu.tf': 'TF', '.edu.hm': 'HM', '.edu.cc': 'CC', '.edu.cx': 'CX',
            '.edu.nf': 'NF', '.edu.pn': 'PN', '.edu.sh': 'SH', '.edu.ac': 'AC',
            '.edu.ta': 'TA', '.edu.io': 'IO', '.edu.dg': 'DG'
        }
        
        # Check for country-specific TLDs
        for tld, country_code in country_tlds.items():
            if domain.endswith(tld):
                # Map country codes to names (simplified)
                country_names = {
                    'US': 'United States', 'GB': 'United Kingdom', 'AU': 'Australia',
                    'CA': 'Canada', 'NZ': 'New Zealand', 'SG': 'Singapore', 'IN': 'India',
                    'BR': 'Brazil', 'MX': 'Mexico', 'AR': 'Argentina', 'CO': 'Colombia',
                    'CL': 'Chile', 'PE': 'Peru', 'EC': 'Ecuador', 'UY': 'Uruguay',
                    'PY': 'Paraguay', 'VE': 'Venezuela', 'BO': 'Bolivia', 'GT': 'Guatemala',
                    'HN': 'Honduras', 'NI': 'Nicaragua', 'PA': 'Panama', 'CR': 'Costa Rica',
                    'DO': 'Dominican Republic', 'CU': 'Cuba', 'JM': 'Jamaica', 'TT': 'Trinidad and Tobago',
                    'ZA': 'South Africa', 'NG': 'Nigeria', 'KE': 'Kenya', 'GH': 'Ghana',
                    'TZ': 'Tanzania', 'UG': 'Uganda', 'RW': 'Rwanda', 'ET': 'Ethiopia',
                    'EG': 'Egypt', 'MA': 'Morocco', 'TN': 'Tunisia', 'DZ': 'Algeria',
                    'LY': 'Libya', 'SD': 'Sudan', 'SN': 'Senegal', 'CI': 'Ivory Coast',
                    'BF': 'Burkina Faso', 'ML': 'Mali', 'NE': 'Niger', 'TD': 'Chad',
                    'CM': 'Cameroon', 'CG': 'Republic of the Congo', 'CD': 'Democratic Republic of the Congo',
                    'CF': 'Central African Republic', 'GA': 'Gabon', 'GQ': 'Equatorial Guinea',
                    'ST': 'São Tomé and Príncipe', 'AO': 'Angola', 'MZ': 'Mozambique',
                    'MW': 'Malawi', 'ZM': 'Zambia', 'BW': 'Botswana', 'NA': 'Namibia',
                    'LS': 'Lesotho', 'SZ': 'Eswatini', 'ZW': 'Zimbabwe', 'BI': 'Burundi',
                    'DJ': 'Djibouti', 'ER': 'Eritrea', 'SO': 'Somalia', 'SS': 'South Sudan',
                    'MU': 'Mauritius', 'SC': 'Seychelles', 'KM': 'Comoros', 'MG': 'Madagascar',
                    'MV': 'Maldives', 'LK': 'Sri Lanka', 'BD': 'Bangladesh', 'MM': 'Myanmar',
                    'TH': 'Thailand', 'LA': 'Laos', 'KH': 'Cambodia', 'VN': 'Vietnam',
                    'PH': 'Philippines', 'MY': 'Malaysia', 'BN': 'Brunei', 'ID': 'Indonesia',
                    'TW': 'Taiwan', 'HK': 'Hong Kong', 'MO': 'Macau', 'JP': 'Japan',
                    'KR': 'South Korea', 'CN': 'China', 'MN': 'Mongolia', 'KZ': 'Kazakhstan',
                    'KG': 'Kyrgyzstan', 'TJ': 'Tajikistan', 'UZ': 'Uzbekistan', 'TM': 'Turkmenistan',
                    'AF': 'Afghanistan', 'PK': 'Pakistan', 'NP': 'Nepal', 'BT': 'Bhutan',
                    'IR': 'Iran', 'IQ': 'Iraq', 'SY': 'Syria', 'LB': 'Lebanon',
                    'JO': 'Jordan', 'IL': 'Israel', 'PS': 'Palestine', 'SA': 'Saudi Arabia',
                    'AE': 'United Arab Emirates', 'OM': 'Oman', 'YE': 'Yemen', 'BH': 'Bahrain',
                    'QA': 'Qatar', 'KW': 'Kuwait', 'TR': 'Turkey', 'CY': 'Cyprus',
                    'GR': 'Greece', 'BG': 'Bulgaria', 'RO': 'Romania', 'MD': 'Moldova',
                    'UA': 'Ukraine', 'BY': 'Belarus', 'LT': 'Lithuania', 'LV': 'Latvia',
                    'EE': 'Estonia', 'PL': 'Poland', 'CZ': 'Czech Republic', 'SK': 'Slovakia',
                    'HU': 'Hungary', 'SI': 'Slovenia', 'HR': 'Croatia', 'BA': 'Bosnia and Herzegovina',
                    'RS': 'Serbia', 'ME': 'Montenegro', 'MK': 'North Macedonia', 'AL': 'Albania',
                    'XK': 'Kosovo', 'IT': 'Italy', 'ES': 'Spain', 'PT': 'Portugal',
                    'FR': 'France', 'BE': 'Belgium', 'NL': 'Netherlands', 'LU': 'Luxembourg',
                    'DE': 'Germany', 'AT': 'Austria', 'CH': 'Switzerland', 'LI': 'Liechtenstein',
                    'AD': 'Andorra', 'MC': 'Monaco', 'SM': 'San Marino', 'VA': 'Vatican City',
                    'FI': 'Finland', 'SE': 'Sweden', 'NO': 'Norway', 'DK': 'Denmark',
                    'IS': 'Iceland', 'IE': 'Ireland', 'FJ': 'Fiji', 'PG': 'Papua New Guinea',
                    'SB': 'Solomon Islands', 'VU': 'Vanuatu', 'NC': 'New Caledonia', 'PF': 'French Polynesia',
                    'WS': 'Samoa', 'TO': 'Tonga', 'CK': 'Cook Islands', 'NU': 'Niue',
                    'TK': 'Tokelau', 'FM': 'Micronesia', 'MH': 'Marshall Islands', 'PW': 'Palau',
                    'NR': 'Nauru', 'KI': 'Kiribati', 'TV': 'Tuvalu', 'AS': 'American Samoa',
                    'GU': 'Guam', 'MP': 'Northern Mariana Islands', 'VI': 'U.S. Virgin Islands',
                    'PR': 'Puerto Rico', 'UM': 'United States Minor Outlying Islands', 'AX': 'Åland',
                    'FO': 'Faroe Islands', 'GL': 'Greenland', 'SJ': 'Svalbard and Jan Mayen',
                    'BV': 'Bouvet Island', 'GS': 'South Georgia and the South Sandwich Islands',
                    'TF': 'French Southern Territories', 'HM': 'Heard Island and McDonald Islands',
                    'CC': 'Cocos Islands', 'CX': 'Christmas Island', 'NF': 'Norfolk Island',
                    'PN': 'Pitcairn Islands', 'SH': 'Saint Helena', 'AC': 'Ascension Island',
                    'TA': 'Tristan da Cunha', 'IO': 'British Indian Ocean Territory', 'DG': 'Diego Garcia'
                }
                return {
                    "id": country_code,
                    "name": country_names.get(country_code, country_code)
                }
    except Exception:
        pass
    return None

def infer_owner_name(record):
    """Try to infer owner name from record"""
    # Try from owner link domain
    owner_link = record.get("owner", {}).get("link", "")
    if owner_link:
        try:
            parsed = urlparse(owner_link)
            domain = parsed.netloc or owner_link
            # Remove www. prefix
            domain = re.sub(r'^www\.', '', domain.lower())
            # Remove common prefixes
            domain = re.sub(r'^(data|gis|geo|map|portal|opendata)\.', '', domain)
            # Take first part of domain
            parts = domain.split('.')
            if len(parts) > 0:
                name = parts[0].replace('-', ' ').title()
                if name and name != 'Unknown':
                    return name
        except Exception:
            pass
    
    # Try from portal name
    portal_name = record.get("name", "")
    if portal_name and portal_name != "Unknown":
        # Clean up the name
        name = portal_name.split('/')[0].split('.')[0].strip()
        if name and len(name) > 2:
            return name
    
    return None

def infer_owner_type(record, owner_link=None):
    """Try to infer owner type from context"""
    if not owner_link:
        owner_link = record.get("owner", {}).get("link", "")
    
    if owner_link:
        domain = urlparse(owner_link).netloc or owner_link
        domain = domain.lower()
        
        # Government domains (including .gov.net, .gov.com variations)
        if '.gov' in domain or '.gob' in domain or '.govt' in domain or domain.endswith('.gov.net') or domain.endswith('.gov.com'):
            # Check description for hints
            description = record.get("description", "").lower()
            if "county" in description or "municipal" in description or "local" in description:
                return "Local government"
            elif "state" in description or "province" in description or "regional" in description:
                return "Regional government"
            # Check for local/state indicators in domain
            elif any(x in domain for x in ['.city.', '.county.', '.local.', 'cityof', 'countyof', 'chippewa', 'municipal']):
                return "Local government"
            elif any(x in domain for x in ['.state.', '.province.', 'provincial', 'scgov']):
                return "Regional government"
            else:
                return "Central government"
        
        # Educational domains
        if '.edu' in domain or '.ac.' in domain or '.university' in domain or '.college' in domain:
            return "Academy"
        
        # International organizations
        if any(x in domain for x in ['.org', '.int', 'un.org', 'worldbank', 'iucn', 'iczn']):
            org_name = record.get("owner", {}).get("name", "").lower()
            if any(x in org_name for x in ['international', 'world', 'global', 'commission']):
                return "International organization"
            return "Civil society"
        
        # Business/Commercial
        if '.com' in domain or '.co.' in domain or '.biz' in domain:
            return "Business"
    
    # Check owner name for hints
    owner_name = record.get("owner", {}).get("name", "").lower()
    if any(x in owner_name for x in ['university', 'college', 'institute', 'academy']):
        return "Academy"
    if any(x in owner_name for x in ['government', 'ministry', 'department', 'agency']):
        return "Central government"
    if any(x in owner_name for x in ['city', 'municipal', 'county', 'local']):
        return "Local government"
    if any(x in owner_name for x in ['international', 'world', 'global']):
        return "International organization"
    
    # Default based on catalog type
    catalog_type = record.get("catalog_type", "").lower()
    if 'scientific' in catalog_type or 'research' in catalog_type:
        return "Academy"
    if 'government' in catalog_type or 'public' in catalog_type:
        return "Central government"
    
    return None

def fix_coverage_normalization(record, macroregion_dict: Dict[str, Dict]) -> Tuple[bool, str]:
    """Fix COVERAGE_NORMALIZATION by adding macroregion"""
    coverage = record.get("coverage", [])
    if not coverage:
        return False, None
    
    fixed = False
    messages = []
    
    for idx, cov_entry in enumerate(coverage):
        location = cov_entry.get("location", {})
        country = location.get("country", {})
        country_id = country.get("id")
        
        if country_id and ("macroregion" not in location or not location.get("macroregion")):
            # Skip Unknown, EU, World
            if country_id in ["Unknown", "EU", "World"]:
                continue
            
            if country_id in macroregion_dict:
                macroregion_info = macroregion_dict[country_id]
                location["macroregion"] = {
                    "id": macroregion_info["macroregion_code"],
                    "name": macroregion_info["macroregion_name"],
                }
                coverage[idx]["location"] = location
                fixed = True
                messages.append(f"Added macroregion {macroregion_info['macroregion_name']} for {country_id}")
    
    if fixed:
        record["coverage"] = coverage
        return True, "; ".join(messages)
    
    return False, None

def fix_inconsistent_license(record) -> Tuple[bool, str]:
    """Fix INCONSISTENT_LICENSE by adding license_url"""
    rights = record.get("rights", {})
    license_name = rights.get("license_name")
    license_url = rights.get("license_url")
    
    if license_name and not license_url:
        # Try to find URL in mapping
        for key, url in LICENSE_URL_MAP.items():
            if key.lower() in license_name.lower():
                rights["license_url"] = url
                record["rights"] = rights
                return True, f"Added license_url: {url}"
    
    return False, None

def fix_missing_api_status(record) -> Tuple[bool, str]:
    """Fix MISSING_API_STATUS by setting api_status based on api and endpoints"""
    api_status = record.get("api_status")
    
    if api_status is None or api_status == "":
        api = record.get("api", False)
        endpoints = record.get("endpoints", [])
        
        # If api=True or endpoints exist, set to active
        if api is True or len(endpoints) > 0:
            record["api_status"] = "active"
            return True, "Set api_status to 'active' (api=True or endpoints present)"
        else:
            # Otherwise set to uncertain
            record["api_status"] = "uncertain"
            return True, "Set api_status to 'uncertain' (no api or endpoints)"
    
    return False, None

def fix_owner_location(record, file_path):
    """Fix MISSING_OWNER_LOCATION"""
    owner = record.get("owner", {})
    location = owner.get("location", {})
    country = location.get("country", {})
    
    if country.get("id") == "Unknown":
        # Try to infer from coverage
        inferred_country = infer_country_from_coverage(record)
        if not inferred_country:
            # Try from owner link
            owner_link = owner.get("link", "")
            inferred_country = infer_country_from_link(owner_link) if owner_link else None
        
        if inferred_country:
            location["country"] = inferred_country
            # Preserve level if reasonable, otherwise set to 20 (country level)
            if location.get("level", 0) == 0:
                location["level"] = 20
            owner["location"] = location
            return True, f"Set owner location to {inferred_country['name']}"
        else:
            # For truly unknown, set to World
            location["country"] = {"id": "World", "name": "World"}
            location["level"] = 0
            owner["location"] = location
            return True, "Set owner location to World (truly international)"
    
    return False, None

def fix_owner_name(record, file_path):
    """Fix MISSING_OWNER_NAME"""
    owner = record.get("owner", {})
    owner_name = owner.get("name", "")
    
    if not owner_name or owner_name == "Unknown":
        inferred_name = infer_owner_name(record)
        if inferred_name:
            owner["name"] = inferred_name
            return True, f"Set owner name to '{inferred_name}'"
        else:
            # Use portal name as fallback
            portal_name = record.get("name", "")
            if portal_name and portal_name != "Unknown":
                owner["name"] = portal_name
                return True, f"Set owner name to portal name '{portal_name}'"
    
    return False, None

def fix_owner_type(record, file_path):
    """Fix MISSING_OWNER_TYPE"""
    owner = record.get("owner", {})
    owner_type = owner.get("type", "")
    
    if not owner_type or owner_type == "Unknown":
        inferred_type = infer_owner_type(record, owner.get("link"))
        if inferred_type:
            owner["type"] = inferred_type
            return True, f"Set owner type to '{inferred_type}'"
        else:
            # Default fallback
            owner["type"] = "Business"
            return True, "Set owner type to 'Business' (default)"
    
    return False, None

def fix_software_id_unknown(record, software_defs: Dict[str, Dict]) -> Tuple[bool, str]:
    """Fix SOFTWARE_ID_UNKNOWN by mapping to custom or verifying"""
    software = record.get("software", {})
    software_id = software.get("id")
    
    if not software_id:
        return False, None
    
    # Check if ID is in mapping
    if software_id in SOFTWARE_ID_MAPPING:
        new_id = SOFTWARE_ID_MAPPING[software_id]
        software["id"] = new_id
        if new_id == "custom":
            software["name"] = "Custom software"
        record["software"] = software
        return True, f"Mapped software.id from '{software_id}' to '{new_id}'"
    
    # Check if ID exists in software definitions
    if software_id not in software_defs:
        # Map to custom
        software["id"] = "custom"
        software["name"] = "Custom software"
        record["software"] = software
        return True, f"Mapped unknown software.id '{software_id}' to 'custom'"
    
    return False, None

def fix_software_name_mismatch(record, software_defs: Dict[str, Dict]) -> Tuple[bool, str]:
    """Fix SOFTWARE_NAME_MISMATCH by updating name to match ID"""
    software = record.get("software", {})
    software_id = software.get("id")
    software_name = software.get("name", "")
    
    if software_id and software_id in software_defs:
        expected_name = software_defs[software_id].get("name", "")
        if expected_name and software_name != expected_name:
            software["name"] = expected_name
            record["software"] = software
            return True, f"Updated software.name from '{software_name}' to '{expected_name}'"
    
    return False, None

def parse_important_file(important_file_path):
    """Parse IMPORTANT.txt and extract file paths and issues"""
    issues = []
    with open(important_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Pattern to match file entries
    pattern = r'File: ([^\n]+)\nRecord ID: [^\n]+\nCountry: [^\n]+\nIssue: ([^\n]+)\nField: ([^\n]+)'
    
    matches = re.finditer(pattern, content)
    for match in matches:
        file_path = match.group(1)
        issue_type = match.group(2)
        field = match.group(3)
        issues.append((file_path, issue_type, field))
    
    return issues

def fix_yaml_file(file_path, issue_type, macroregion_dict: Dict[str, Dict], software_defs: Dict[str, Dict]):
    """Fix issues in a YAML file"""
    full_path = ENTITIES_DIR / file_path
    
    if not full_path.exists():
        # Try scheduled directory
        scheduled_path = BASE_DIR / "data" / "scheduled" / file_path
        if scheduled_path.exists():
            full_path = scheduled_path
        else:
            print(f"Warning: File not found: {full_path}")
            return False
    
    try:
        with open(full_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        if not data:
            print(f"Warning: Empty file: {file_path}")
            return False
        
        fixed = False
        message = None
        
        if issue_type == "MISSING_OWNER_LOCATION":
            fixed, message = fix_owner_location(data, file_path)
        elif issue_type == "MISSING_OWNER_NAME":
            fixed, message = fix_owner_name(data, file_path)
        elif issue_type == "MISSING_OWNER_TYPE":
            fixed, message = fix_owner_type(data, file_path)
        elif issue_type == "COVERAGE_NORMALIZATION":
            fixed, message = fix_coverage_normalization(data, macroregion_dict)
        elif issue_type == "INCONSISTENT_LICENSE":
            fixed, message = fix_inconsistent_license(data)
        elif issue_type == "MISSING_API_STATUS":
            fixed, message = fix_missing_api_status(data)
        elif issue_type == "SOFTWARE_ID_UNKNOWN":
            fixed, message = fix_software_id_unknown(data, software_defs)
        elif issue_type == "SOFTWARE_NAME_MISMATCH":
            fixed, message = fix_software_name_mismatch(data, software_defs)
        
        if fixed and message:
            print(f"{issue_type} in {file_path}: {message}")
            # Write back
            with open(full_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            return True
        
        return False
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    important_file = BASE_DIR / "dataquality" / "priorities" / "IMPORTANT.txt"
    
    if not important_file.exists():
        print(f"Error: {important_file} not found")
        return
    
    print("Loading macroregion dictionary...")
    macroregion_dict = load_macroregion_dict()
    print(f"Loaded {len(macroregion_dict)} macroregion mappings")
    
    print("Loading software definitions...")
    software_defs = load_software_definitions()
    print(f"Loaded {len(software_defs)} software definitions")
    
    print("\nParsing IMPORTANT.txt...")
    issues = parse_important_file(important_file)
    print(f"Found {len(issues)} issues to fix")
    
    fixed_count = 0
    skipped_count = 0
    for file_path, issue_type, field in issues:
        result = fix_yaml_file(file_path, issue_type, macroregion_dict, software_defs)
        if result:
            fixed_count += 1
        else:
            skipped_count += 1
    
    print(f"\nFixed {fixed_count} out of {len(issues)} files")
    if skipped_count > 0:
        print(f"Skipped {skipped_count} files (may have been already fixed or couldn't be inferred)")

if __name__ == "__main__":
    main()
