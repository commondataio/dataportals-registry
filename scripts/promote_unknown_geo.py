#!/usr/bin/env python3
"""
Review records in data/scheduled/Unknown/geo, infer country from domain/link,
update status, and move to proper subdir in data/entities/.

For each record:
- Infer country from TLD (.gov, .edu, country TLDs), domain hints, owner name
- Update coverage and owner with inferred country
- Ensure status is active
- Move to data/entities/{COUNTRY}/{Federal|subregion}/geo/
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import urlparse

import yaml

BASE_DIR = Path(__file__).parent.parent
SCHEDULED_UNKNOWN_GEO = BASE_DIR / "data" / "scheduled" / "Unknown" / "geo"
ENTITIES_DIR = BASE_DIR / "data" / "entities"

# Country names for display
COUNTRY_NAMES = {
    "US": "United States",
    "GB": "United Kingdom",
    "World": "World",
    "AU": "Australia",
    "CA": "Canada",
    "FR": "France",
    "DE": "Germany",
    "NL": "Netherlands",
    "BE": "Belgium",
    "ES": "Spain",
    "IT": "Italy",
    "BR": "Brazil",
    "MX": "Mexico",
    "AR": "Argentina",
    "IN": "India",
    "CN": "China",
    "JP": "Japan",
    "KR": "South Korea",
    "ZA": "South Africa",
    "KE": "Kenya",
    "NG": "Nigeria",
    "EG": "Egypt",
    "MA": "Morocco",
    "TN": "Tunisia",
    "PL": "Poland",
    "CZ": "Czech Republic",
    "AT": "Austria",
    "CH": "Switzerland",
    "SE": "Sweden",
    "NO": "Norway",
    "DK": "Denmark",
    "FI": "Finland",
    "IE": "Ireland",
    "NZ": "New Zealand",
    "SG": "Singapore",
    "BD": "Bangladesh",
    "PK": "Pakistan",
    "TH": "Thailand",
    "VN": "Vietnam",
    "ID": "Indonesia",
    "PH": "Philippines",
    "MY": "Malaysia",
    "CO": "Colombia",
    "CL": "Chile",
    "PE": "Peru",
    "EC": "Ecuador",
    "UY": "Uruguay",
    "PY": "Paraguay",
    "BO": "Bolivia",
    "CR": "Costa Rica",
    "PA": "Panama",
    "GT": "Guatemala",
    "HN": "Honduras",
    "NI": "Nicaragua",
    "CU": "Cuba",
    "JM": "Jamaica",
    "PR": "Puerto Rico",
    "RU": "Russian Federation",
    "UA": "Ukraine",
    "BY": "Belarus",
    "KZ": "Kazakhstan",
    "TR": "Turkey",
    "IL": "Israel",
    "SA": "Saudi Arabia",
    "AE": "United Arab Emirates",
    "QA": "Qatar",
    "KW": "Kuwait",
    "BH": "Bahrain",
    "OM": "Oman",
    "JO": "Jordan",
    "LB": "Lebanon",
    "GR": "Greece",
    "PT": "Portugal",
    "RO": "Romania",
    "HU": "Hungary",
    "BG": "Bulgaria",
    "HR": "Croatia",
    "SI": "Slovenia",
    "SK": "Slovakia",
    "RS": "Serbia",
    "BA": "Bosnia and Herzegovina",
    "ME": "Montenegro",
    "MK": "North Macedonia",
    "AL": "Albania",
    "EE": "Estonia",
    "LV": "Latvia",
    "LT": "Lithuania",
    "GE": "Georgia",
    "AM": "Armenia",
    "AZ": "Azerbaijan",
    "IR": "Iran",
    "IQ": "Iraq",
    "SY": "Syria",
    "PS": "Palestine",
    "LY": "Libya",
    "DZ": "Algeria",
    "SD": "Sudan",
    "ET": "Ethiopia",
    "TZ": "Tanzania",
    "UG": "Uganda",
    "RW": "Rwanda",
    "GH": "Ghana",
    "SN": "Senegal",
    "CI": "Ivory Coast",
    "CM": "Cameroon",
    "CD": "Democratic Republic of the Congo",
    "AO": "Angola",
    "MZ": "Mozambique",
    "ZW": "Zimbabwe",
    "BW": "Botswana",
    "NA": "Namibia",
    "LS": "Lesotho",
    "MU": "Mauritius",
    "SC": "Seychelles",
    "MG": "Madagascar",
    "LK": "Sri Lanka",
    "NP": "Nepal",
    "BT": "Bhutan",
    "MM": "Myanmar",
    "KH": "Cambodia",
    "LA": "Laos",
    "TW": "Taiwan",
    "HK": "Hong Kong",
    "MO": "Macau",
    "MN": "Mongolia",
    "AF": "Afghanistan",
    "UZ": "Uzbekistan",
    "TM": "Turkmenistan",
    "TJ": "Tajikistan",
    "KG": "Kyrgyzstan",
}

# US subregion display names
US_SUBREGION_NAMES = {
    "US-AL": "Alabama", "US-AK": "Alaska", "US-AZ": "Arizona", "US-AR": "Arkansas",
    "US-CA": "California", "US-CO": "Colorado", "US-CT": "Connecticut", "US-DE": "Delaware",
    "US-FL": "Florida", "US-GA": "Georgia", "US-HI": "Hawaii", "US-ID": "Idaho",
    "US-IL": "Illinois", "US-IN": "Indiana", "US-IA": "Iowa", "US-KS": "Kansas",
    "US-KY": "Kentucky", "US-LA": "Louisiana", "US-ME": "Maine", "US-MD": "Maryland",
    "US-MA": "Massachusetts", "US-MI": "Michigan", "US-MN": "Minnesota", "US-MS": "Mississippi",
    "US-MO": "Missouri", "US-MT": "Montana", "US-NE": "Nebraska", "US-NV": "Nevada",
    "US-NH": "New Hampshire", "US-NJ": "New Jersey", "US-NM": "New Mexico", "US-NY": "New York",
    "US-NC": "North Carolina", "US-ND": "North Dakota", "US-OH": "Ohio", "US-OK": "Oklahoma",
    "US-OR": "Oregon", "US-PA": "Pennsylvania", "US-RI": "Rhode Island", "US-SC": "South Carolina",
    "US-SD": "South Dakota", "US-TN": "Tennessee", "US-TX": "Texas", "US-UT": "Utah",
    "US-VT": "Vermont", "US-VA": "Virginia", "US-WA": "Washington", "US-WV": "West Virginia",
    "US-WI": "Wisconsin", "US-WY": "Wyoming", "US-DC": "District of Columbia",
    "US-VI": "U.S. Virgin Islands",
}

# Domain/id tokens -> (country_id, subregion_id or None)
# Extends fix_us_ca_misclassified PLACE_TO_STATE with additional Unknown geo mappings
DOMAIN_TO_COUNTRY: dict[str, tuple[str, str | None]] = {
    # International / World
    "un": ("World", None),
    "geoservicesun": ("World", None),
    "ramsar": ("World", None),
    "servirglobal": ("World", None),
    "resilienceatlas": ("World", None),
    "caribbeanmarineatlas": ("World", None),
    "geossregistries": ("World", None),
    "airbusdsgeo": ("World", None),
    "geospatialorg": ("World", None),
    "openstreetmap": ("World", None),
    "yieldgap": ("World", None),
    "globaloceantrack": ("World", None),
    "atlasalpconv": ("World", None),
    "coastalresilience": ("World", None),
    "northeastoceandata": ("World", None),
    "geomatick": ("World", None),
    "d4science": ("World", None),
    # US - additional for Unknown geo
    "nashuarpc": ("US", "US-NH"),
    "nashua": ("US", "US-NH"),
    "nlauderdale": ("US", "US-FL"),
    "epcad": ("US", "US-TX"),  # El Paso County Appraisal District
    "wirapids": ("US", "US-WI"),
    "harnett": ("US", "US-NC"),
    "bozeman": ("US", "US-MT"),
    "charlottesville": ("US", "US-VA"),
    "lenexa": ("US", "US-KS"),
    "audubon": ("US", "US-NY"),  # National Audubon Society - US
    "lakewood": ("US", "US-CO"),
    "tnris": ("US", "US-TX"),
    "kissimmee": ("US", "US-FL"),
    "spokane": ("US", "US-WA"),
    "manitowoc": ("US", "US-WI"),
    "lsuagcenter": ("US", "US-LA"),
    "cedarfalls": ("US", "US-IA"),
    "sanda": ("US", "US-CA"),
    "appleton": ("US", "US-WI"),
    "broward": ("US", "US-FL"),
    "sarpy": ("US", "US-NE"),
    "integritygis": ("US", "US-FL"),
    "cmpdd": ("US", "US-NC"),
    "hgac": ("US", "US-TX"),
    "westplains": ("US", "US-MO"),
    "lcfpd": ("US", "US-IL"),
    "cherokee": ("US", "US-GA"),
    "drcog": ("US", "US-CO"),
    "owensboro": ("US", "US-KY"),
    "lansdale": ("US", "US-PA"),
    "baltometro": ("US", "US-MD"),
    "ksdot": ("US", "US-KS"),
    "wafwa": ("US", "US-WA"),
    "axisgis": ("US", None),
    "tpcg": ("US", "US-TX"),
    "cary": ("US", "US-NC"),
    "capecodcommission": ("US", "US-MA"),
    "menlopark": ("US", "US-CA"),
    "wyoroad": ("US", "US-WY"),
    "crcog": ("US", "US-CO"),
    "sccwrp": ("US", "US-CA"),
    "lojic": ("US", "US-KY"),
    "ncboc": ("US", "US-NC"),
    "hollandbpw": ("US", "US-MI"),
    "mvpc": ("US", "US-PA"),
    "stpete": ("US", "US-FL"),
    "edenprairie": ("US", "US-MN"),
    "derrynh": ("US", "US-NH"),
    "coloradodot": ("US", "US-CO"),
    "lpao": ("US", "US-FL"),
    "tigard": ("US", "US-OR"),
    "muskego": ("US", "US-WI"),
    "centralil": ("US", "US-IL"),
    "wycokck": ("US", "US-KS"),
    "mhgis": ("US", "US-MN"),
    "cupertino": ("US", "US-CA"),
    "tananachiefs": ("US", "US-AK"),
    "enfield": ("US", "US-CT"),
    "cedarrapids": ("US", "US-IA"),
    "myokaloosa": ("US", "US-FL"),
    "ecfrpc": ("US", "US-FL"),
    "mijackson": ("US", "US-MS"),
    "littleelm": ("US", "US-TX"),
    "sfmta": ("US", "US-CA"),
    "modesto": ("US", "US-CA"),
    "bostonplans": ("US", "US-MA"),
    "qac": ("US", "US-WA"),
    "garrigue": ("FR", None),
    "volcano": ("US", "US-HI"),
    "siedu": ("US", None),
    "evk2cnr": ("IT", None),
    "capnoip": ("US", "US-FL"),
    "airbusds": ("FR", None),
    "epures": ("FR", None),
    "uriadinformatique": ("FR", None),
    "washingtonedu": ("US", "US-WA"),
    "atkinsgeospatial": ("GB", None),
    "astuntechnology": ("GB", None),
    "rcmrd": ("KE", None),
    "oraotca": ("AU", None),
    "cattco": ("US", None),
    "klimaatatlas": ("NL", None),
    "observatoireenvironnement": ("FR", None),
    "agricloud": ("World", None),
    "landscapesportal": ("World", None),
    "korina": ("GR", None),
    "kartozacom": ("PL", None),
    "inspiremisoportal": ("EU", None),
    "inforac": ("US", None),
    "lensgis": ("US", None),
    "mallorca": ("ES", None),
    "icosurenio": ("IT", None),
    "geronimus": ("US", None),
    "gmrt": ("US", None),
    "forestatlas": ("US", None),
    "cbforg": ("US", "US-FL"),
    "artemisits": ("US", None),
    "tunaatlas": ("World", None),
}


def infer_country_from_link(link: str) -> tuple[str, str | None] | None:
    """Infer country from domain TLD. Returns (country_id, subregion_id or None)."""
    if not link:
        return None
    try:
        parsed = urlparse(link)
        domain = (parsed.netloc or link).lower()
        domain = domain.replace("www.", "").split("/")[0]

        # .gov -> US (default)
        if domain.endswith(".gov") and not any(
            domain.endswith(f".gov.{tld}") for tld in ["uk", "au", "ca", "nz", "sg", "in", "br", "mx", "ar"]
        ):
            return ("US", None)
        if domain.endswith(".gov.uk"):
            return ("GB", None)
        if domain.endswith(".gov.au"):
            return ("AU", None)
        if domain.endswith(".gov.ca"):
            return ("CA", None)
        if domain.endswith(".gov.nz"):
            return ("NZ", None)
        if domain.endswith(".gov.sg"):
            return ("SG", None)
        if domain.endswith(".gov.in"):
            return ("IN", None)
        if domain.endswith(".gov.br"):
            return ("BR", None)
        if domain.endswith(".gov.mx"):
            return ("MX", None)
        if domain.endswith(".gov.ar"):
            return ("AR", None)
        if domain.endswith(".gov.fr"):
            return ("FR", None)
        if domain.endswith(".gov.de"):
            return ("DE", None)
        if domain.endswith(".gov.nl"):
            return ("NL", None)
        if domain.endswith(".gov.be"):
            return ("BE", None)
        if domain.endswith(".gov.es"):
            return ("ES", None)
        if domain.endswith(".gov.it"):
            return ("IT", None)
        if domain.endswith(".gov.pl"):
            return ("PL", None)
        if domain.endswith(".gov.cz"):
            return ("CZ", None)
        if domain.endswith(".gov.at"):
            return ("AT", None)
        if domain.endswith(".gov.ch"):
            return ("CH", None)
        if domain.endswith(".gov.se"):
            return ("SE", None)
        if domain.endswith(".gov.no"):
            return ("NO", None)
        if domain.endswith(".gov.dk"):
            return ("DK", None)
        if domain.endswith(".gov.fi"):
            return ("FI", None)
        if domain.endswith(".gov.ie"):
            return ("IE", None)
        if domain.endswith(".gov.jp"):
            return ("JP", None)
        if domain.endswith(".gov.kr"):
            return ("KR", None)
        if domain.endswith(".gov.cn"):
            return ("CN", None)
        if domain.endswith(".gov.ru"):
            return ("RU", None)
        if domain.endswith(".gov.ua"):
            return ("UA", None)
        if domain.endswith(".gov.tr"):
            return ("TR", None)
        if domain.endswith(".gov.il"):
            return ("IL", None)
        if domain.endswith(".gov.za"):
            return ("ZA", None)
        if domain.endswith(".gov.ke"):
            return ("KE", None)
        if domain.endswith(".gov.ng"):
            return ("NG", None)
        if domain.endswith(".gov.eg"):
            return ("EG", None)
        if domain.endswith(".gov.bd"):
            return ("BD", None)
        if domain.endswith(".gov.pk"):
            return ("PK", None)
        if domain.endswith(".gov.th"):
            return ("TH", None)
        if domain.endswith(".gov.vn"):
            return ("VN", None)
        if domain.endswith(".gov.id"):
            return ("ID", None)
        if domain.endswith(".gov.ph"):
            return ("PH", None)
        if domain.endswith(".gov.my"):
            return ("MY", None)
        if domain.endswith(".gov.co"):
            return ("CO", None)
        if domain.endswith(".gov.cl"):
            return ("CL", None)
        if domain.endswith(".gov.pe"):
            return ("PE", None)
        if domain.endswith(".gov.ec"):
            return ("EC", None)
        if domain.endswith(".gov.uy"):
            return ("UY", None)
        if domain.endswith(".gov.py"):
            return ("PY", None)
        if domain.endswith(".gov.gr"):
            return ("GR", None)
        if domain.endswith(".gov.pt"):
            return ("PT", None)
        if domain.endswith(".gov.ro"):
            return ("RO", None)
        if domain.endswith(".gov.hu"):
            return ("HU", None)
        if domain.endswith(".gov.bg"):
            return ("BG", None)
        if domain.endswith(".gov.hr"):
            return ("HR", None)
        if domain.endswith(".gov.si"):
            return ("SI", None)
        if domain.endswith(".gov.sk"):
            return ("SK", None)
        if domain.endswith(".gov.rs"):
            return ("RS", None)
        if domain.endswith(".gov.ba"):
            return ("BA", None)
        if domain.endswith(".gov.me"):
            return ("ME", None)
        if domain.endswith(".gov.mk"):
            return ("MK", None)
        if domain.endswith(".gov.al"):
            return ("AL", None)
        if domain.endswith(".gov.ee"):
            return ("EE", None)
        if domain.endswith(".gov.lv"):
            return ("LV", None)
        if domain.endswith(".gov.lt"):
            return ("LT", None)
        if domain.endswith(".gov.ge"):
            return ("GE", None)
        if domain.endswith(".gov.am"):
            return ("AM", None)
        if domain.endswith(".gov.az"):
            return ("AZ", None)
        if domain.endswith(".gov.ir"):
            return ("IR", None)
        if domain.endswith(".gov.iq"):
            return ("IQ", None)
        if domain.endswith(".gov.sy"):
            return ("SY", None)
        if domain.endswith(".gov.lb"):
            return ("LB", None)
        if domain.endswith(".gov.jo"):
            return ("JO", None)
        if domain.endswith(".gov.ps"):
            return ("PS", None)
        if domain.endswith(".gov.sa"):
            return ("SA", None)
        if domain.endswith(".gov.ae"):
            return ("AE", None)
        if domain.endswith(".gov.om"):
            return ("OM", None)
        if domain.endswith(".gov.ye"):
            return ("YE", None)
        if domain.endswith(".gov.bh"):
            return ("BH", None)
        if domain.endswith(".gov.qa"):
            return ("QA", None)
        if domain.endswith(".gov.kw"):
            return ("KW", None)
        if domain.endswith(".gov.ly"):
            return ("LY", None)
        if domain.endswith(".gov.dz"):
            return ("DZ", None)
        if domain.endswith(".gov.sd"):
            return ("SD", None)
        if domain.endswith(".gov.et"):
            return ("ET", None)
        if domain.endswith(".gov.tz"):
            return ("TZ", None)
        if domain.endswith(".gov.ug"):
            return ("UG", None)
        if domain.endswith(".gov.rw"):
            return ("RW", None)
        if domain.endswith(".gov.gh"):
            return ("GH", None)
        if domain.endswith(".gov.sn"):
            return ("SN", None)
        if domain.endswith(".gov.ci"):
            return ("CI", None)
        if domain.endswith(".gov.cm"):
            return ("CM", None)
        if domain.endswith(".gov.cd"):
            return ("CD", None)
        if domain.endswith(".gov.ao"):
            return ("AO", None)
        if domain.endswith(".gov.mz"):
            return ("MZ", None)
        if domain.endswith(".gov.zw"):
            return ("ZW", None)
        if domain.endswith(".gov.bw"):
            return ("BW", None)
        if domain.endswith(".gov.na"):
            return ("NA", None)
        if domain.endswith(".gov.ls"):
            return ("LS", None)
        if domain.endswith(".gov.mu"):
            return ("MU", None)
        if domain.endswith(".gov.sc"):
            return ("SC", None)
        if domain.endswith(".gov.km"):
            return ("KM", None)
        if domain.endswith(".gov.mg"):
            return ("MG", None)
        if domain.endswith(".gov.mv"):
            return ("MV", None)
        if domain.endswith(".gov.lk"):
            return ("LK", None)
        if domain.endswith(".gov.mm"):
            return ("MM", None)
        if domain.endswith(".gov.la"):
            return ("LA", None)
        if domain.endswith(".gov.kh"):
            return ("KH", None)
        if domain.endswith(".gov.bn"):
            return ("BN", None)
        if domain.endswith(".gov.tw"):
            return ("TW", None)
        if domain.endswith(".gov.hk"):
            return ("HK", None)
        if domain.endswith(".gov.mo"):
            return ("MO", None)
        if domain.endswith(".gov.mn"):
            return ("MN", None)
        if domain.endswith(".gov.kz"):
            return ("KZ", None)
        if domain.endswith(".gov.kg"):
            return ("KG", None)
        if domain.endswith(".gov.tj"):
            return ("TJ", None)
        if domain.endswith(".gov.uz"):
            return ("UZ", None)
        if domain.endswith(".gov.tm"):
            return ("TM", None)
        if domain.endswith(".gov.af"):
            return ("AF", None)
        if domain.endswith(".gov.np"):
            return ("NP", None)
        if domain.endswith(".gov.bt"):
            return ("BT", None)

        # .edu -> US default (most .edu are US)
        if domain.endswith(".edu") and not any(
            domain.endswith(f".edu.{tld}") for tld in ["uk", "au", "ca", "nz", "sg", "in", "br", "mx", "ar", "jp", "kr", "cn", "tw", "hk", "ph", "my", "id", "th", "vn", "pk", "bd", "eg", "za", "ng", "ke", "gh", "tz", "ug", "et"]
        ):
            return ("US", None)
        if domain.endswith(".edu.au"):
            return ("AU", None)
        if domain.endswith(".edu.uk"):
            return ("GB", None)
        if domain.endswith(".edu.ca"):
            return ("CA", None)
        if domain.endswith(".edu.nz"):
            return ("NZ", None)
        if domain.endswith(".edu.sg"):
            return ("SG", None)
        if domain.endswith(".edu.in"):
            return ("IN", None)
        if domain.endswith(".edu.br"):
            return ("BR", None)
        if domain.endswith(".edu.mx"):
            return ("MX", None)
        if domain.endswith(".edu.ar"):
            return ("AR", None)
        if domain.endswith(".edu.jp"):
            return ("JP", None)
        if domain.endswith(".edu.kr"):
            return ("KR", None)
        if domain.endswith(".edu.cn"):
            return ("CN", None)
        if domain.endswith(".edu.tw"):
            return ("TW", None)
        if domain.endswith(".edu.hk"):
            return ("HK", None)
        if domain.endswith(".edu.ph"):
            return ("PH", None)
        if domain.endswith(".edu.my"):
            return ("MY", None)
        if domain.endswith(".edu.id"):
            return ("ID", None)
        if domain.endswith(".edu.th"):
            return ("TH", None)
        if domain.endswith(".edu.vn"):
            return ("VN", None)
        if domain.endswith(".edu.pk"):
            return ("PK", None)
        if domain.endswith(".edu.bd"):
            return ("BD", None)
        if domain.endswith(".edu.eg"):
            return ("EG", None)
        if domain.endswith(".edu.za"):
            return ("ZA", None)
        if domain.endswith(".edu.ng"):
            return ("NG", None)
        if domain.endswith(".edu.ke"):
            return ("KE", None)
        if domain.endswith(".edu.gh"):
            return ("GH", None)
        if domain.endswith(".edu.tz"):
            return ("TZ", None)
        if domain.endswith(".edu.ug"):
            return ("UG", None)
        if domain.endswith(".edu.et"):
            return ("ET", None)
        if domain.endswith(".ac.uk"):
            return ("GB", None)
        if domain.endswith(".ac.jp"):
            return ("JP", None)
        if domain.endswith(".ac.nz"):
            return ("NZ", None)
        if domain.endswith(".ac.in"):
            return ("IN", None)
        if domain.endswith(".ac.za"):
            return ("ZA", None)
        if domain.endswith(".ac.th"):
            return ("TH", None)
        if domain.endswith(".ac.id"):
            return ("ID", None)
        if domain.endswith(".ac.kr"):
            return ("KR", None)
        if domain.endswith(".ac.cn"):
            return ("CN", None)
        if domain.endswith(".ac.ru"):
            return ("RU", None)
        if domain.endswith(".ac.at"):
            return ("AT", None)
        if domain.endswith(".ac.be"):
            return ("BE", None)
        if domain.endswith(".ac.ir"):
            return ("IR", None)

        # Country code TLDs (2-letter)
        tld_2letter = domain.split(".")[-1] if "." in domain else ""
        tld_to_country = {
            "uk": "GB", "au": "AU", "ca": "CA", "nz": "NZ", "sg": "SG", "in": "IN",
            "br": "BR", "mx": "MX", "ar": "AR", "fr": "FR", "de": "DE", "nl": "NL",
            "be": "BE", "es": "ES", "it": "IT", "pl": "PL", "cz": "CZ", "at": "AT",
            "ch": "CH", "se": "SE", "no": "NO", "dk": "DK", "fi": "FI", "ie": "IE",
            "jp": "JP", "kr": "KR", "cn": "CN", "tw": "TW", "hk": "HK", "mo": "MO",
            "ru": "RU", "ua": "UA", "by": "BY", "kz": "KZ", "tr": "TR", "il": "IL",
            "sa": "SA", "ae": "AE", "qa": "QA", "kw": "KW", "bh": "BH", "om": "OM",
            "ye": "YE", "jo": "JO", "lb": "LB", "sy": "SY", "iq": "IQ", "ir": "IR",
            "ps": "PS", "eg": "EG", "ma": "MA", "tn": "TN", "dz": "DZ", "ly": "LY",
            "sd": "SD", "et": "ET", "tz": "TZ", "ug": "UG", "rw": "RW", "gh": "GH",
            "sn": "SN", "ci": "CI", "cm": "CM", "cd": "CD", "ao": "AO", "mz": "MZ",
            "zw": "ZW", "bw": "BW", "na": "NA", "ls": "LS", "mu": "MU", "sc": "SC",
            "km": "KM", "mg": "MG", "mv": "MV", "lk": "LK", "np": "NP", "bt": "BT",
            "bd": "BD", "mm": "MM", "th": "TH", "kh": "KH", "la": "LA", "vn": "VN",
            "ph": "PH", "my": "MY", "bn": "BN", "id": "ID", "pk": "PK", "af": "AF",
            "uz": "UZ", "tm": "TM", "tj": "TJ", "kg": "KG", "mn": "MN", "ge": "GE",
            "am": "AM", "az": "AZ", "gr": "GR", "pt": "PT", "ro": "RO", "hu": "HU",
            "bg": "BG", "hr": "HR", "si": "SI", "sk": "SK", "rs": "RS", "ba": "BA",
            "me": "ME", "mk": "MK", "al": "AL", "ee": "EE", "lv": "LV", "lt": "LT",
            "co": "CO", "cl": "CL", "pe": "PE", "ec": "EC", "uy": "UY", "py": "PY",
            "bo": "BO", "cr": "CR", "pa": "PA", "gt": "GT", "hn": "HN", "ni": "NI",
            "sv": "SV", "do": "DO", "cu": "CU", "jm": "JM", "tt": "TT", "pr": "PR",
            "gi": "GB", "eu": "EU",
        }
        if tld_2letter in tld_to_country:
            return (tld_to_country[tld_2letter], None)

    except Exception:
        pass
    return None


def infer_from_domain_hints(record: dict) -> tuple[str, str | None] | None:
    """Infer country from domain tokens in link, id, owner name."""
    link = (record.get("link") or "").strip()
    record_id = (record.get("id") or "").strip().lower()
    owner_name = ((record.get("owner", {}) or {}).get("name", "") or "").strip().lower()
    owner_link = ((record.get("owner", {}) or {}).get("link", "") or "").strip()

    combined = f" {record_id} {owner_name} "
    try:
        parsed = urlparse(link if "://" in link else f"https://{link}")
        host = (parsed.netloc or "").lower().replace("www.", "").split("/")[0]
        combined += f" {host} "
    except Exception:
        pass
    try:
        parsed = urlparse(owner_link if "://" in owner_link else f"https://{owner_link}")
        host = (parsed.netloc or "").lower().replace("www.", "").split("/")[0]
        combined += f" {host} "
    except Exception:
        pass

    combined_compact = re.sub(r"[^a-z0-9]", "", combined)

    for token, (country, subregion) in sorted(DOMAIN_TO_COUNTRY.items(), key=lambda x: -len(x[0])):
        if token in combined_compact or token in combined:
            if country == "EU":
                return ("World", None)  # EU -> World for international
            return (country, subregion)

    return None


def infer_country_and_subregion(record: dict) -> tuple[str, str | None]:
    """Infer (country_id, subregion_id or None). Default to World for unknown."""
    # 1. TLD from link
    result = infer_country_from_link(record.get("link") or "")
    if result:
        return result

    # 2. Domain hints
    result = infer_from_domain_hints(record)
    if result:
        return result

    # 3. Owner name hints for international
    owner_name = ((record.get("owner", {}) or {}).get("name", "") or "").lower()
    if any(x in owner_name for x in ["united nations", "international", "world", "global", "ramsar", "servir"]):
        return ("World", None)

    # 4. Description hints
    desc = (record.get("description") or "").lower()
    if any(x in desc for x in ["united nations", "international", "worldwide", "global", "caribbean"]):
        return ("World", None)

    # 5. Default: World for geoportals that couldn't be attributed
    return ("World", None)


def build_location(country_id: str, subregion_id: str | None) -> dict:
    """Build location dict for coverage/owner."""
    loc: dict = {
        "country": {
            "id": country_id,
            "name": COUNTRY_NAMES.get(country_id, country_id),
        },
        "level": 30 if subregion_id else 20,
    }
    if subregion_id and subregion_id != "US-Federal":
        loc["subregion"] = {
            "id": subregion_id,
            "name": US_SUBREGION_NAMES.get(subregion_id, subregion_id),
        }
    return loc


def get_target_path(country_id: str, subregion_id: str | None, catalog_type: str = "geo") -> Path:
    """Get target directory for entity file."""
    if subregion_id and subregion_id != "US-Federal":
        return ENTITIES_DIR / country_id / subregion_id / catalog_type
    return ENTITIES_DIR / country_id / "Federal" / catalog_type


def process_record(record: dict) -> bool:
    """Update record with inferred country and status. Returns True if changed."""
    changed = False
    country_id, subregion_id = infer_country_and_subregion(record)

    loc = build_location(country_id, subregion_id)

    # Update coverage
    coverage = record.get("coverage") or []
    if not coverage:
        record["coverage"] = [{"location": loc}]
        changed = True
    else:
        for cov in coverage:
            if isinstance(cov, dict) and "location" in cov:
                old_country = (cov.get("location") or {}).get("country") or {}
                if (old_country.get("id") or "").strip() != country_id:
                    cov["location"] = loc.copy()
                    changed = True
                elif subregion_id and (cov.get("location") or {}).get("subregion", {}).get("id") != subregion_id:
                    cov["location"] = loc.copy()
                    changed = True

    # Update owner location
    owner = record.get("owner") or {}
    owner_loc = owner.get("location") or {}
    old_country = (owner_loc.get("country") or {}).get("id") or ""
    if old_country != country_id or (subregion_id and (owner_loc.get("subregion") or {}).get("id") != subregion_id):
        owner["location"] = loc.copy()
        record["owner"] = owner
        changed = True

    # Ensure status is active
    if (record.get("status") or "").strip() != "active":
        record["status"] = "active"
        changed = True

    return changed


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("DRY RUN - no files will be moved\n")

    if not SCHEDULED_UNKNOWN_GEO.exists():
        print(f"Directory not found: {SCHEDULED_UNKNOWN_GEO}")
        return

    yaml_files = sorted(SCHEDULED_UNKNOWN_GEO.glob("*.yaml"))
    if not yaml_files:
        print("No YAML files in data/scheduled/Unknown/geo")
        return

    print(f"Processing {len(yaml_files)} records in data/scheduled/Unknown/geo\n")

    by_target: dict[tuple[str, str | None], list[tuple[Path, dict]]] = {}
    errors: list[tuple[Path, str]] = []

    for yaml_path in yaml_files:
        try:
            data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        except Exception as e:
            errors.append((yaml_path, str(e)))
            continue
        if not isinstance(data, dict):
            errors.append((yaml_path, "Invalid YAML structure"))
            continue

        process_record(data)
        country_id, subregion_id = infer_country_and_subregion(data)
        key = (country_id, subregion_id)
        by_target.setdefault(key, []).append((yaml_path, data))

    # Summary
    for (country_id, subregion_id), items in sorted(by_target.items(), key=lambda x: (x[0][0], x[0][1] or "")):
        target_dir = get_target_path(country_id, subregion_id)
        label = f"{country_id}/{subregion_id or 'Federal'}/geo"
        print(f"  {label}: {len(items)} records")
        for path, rec in items[:3]:
            print(f"    - {path.name} -> {rec.get('name', '?')[:50]}")
        if len(items) > 3:
            print(f"    ... and {len(items) - 3} more")

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for path, err in errors[:10]:
            print(f"  {path.name}: {err}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")

    if dry_run or not yaml_files:
        print("\nRun without --dry-run to apply changes.")
        return

    # Move files
    moved = 0
    for (country_id, subregion_id), items in by_target.items():
        target_dir = get_target_path(country_id, subregion_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        for src_path, data in items:
            target_path = target_dir / src_path.name
            if target_path.exists():
                # Remove duplicate from scheduled (entity already exists)
                src_path.unlink()
                continue
            target_path.write_text(
                yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
                encoding="utf-8",
            )
            src_path.unlink()
            moved += 1

    print(f"\nMoved {moved} files to entities.")
    print("Run: python scripts/builder.py assign")
    print("Run: python scripts/builder.py validate-yaml")
    print("Run: python scripts/builder.py build")


if __name__ == "__main__":
    main()
