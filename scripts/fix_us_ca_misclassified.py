#!/usr/bin/env python3
"""
Identify and move US-CA records that actually belong to other US subregions.

Records in data/entities/US/US-CA with owner subregion US-CA are reviewed.
Real subregion is inferred from link domain, record id, and owner name
(e.g. orlando.gov -> FL, wellingtonfl.gov -> FL, yakimawagov -> WA).
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse

import yaml

BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"
US_CA_DIR = ENTITIES_DIR / "US" / "US-CA"

# US state postal codes -> subregion id
US_STATE_ABBREVS = {
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id",
    "il", "in", "ia", "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms",
    "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok",
    "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv",
    "wi", "wy", "dc",
}

# Map state abbrev to subregion id
ABBREV_TO_SUBREGION = {abbrev: f"US-{abbrev.upper()}" for abbrev in US_STATE_ABBREVS}

# California places - these must stay in US-CA (checked after PLACE_TO_STATE)
# Also add to PLACE_TO_STATE with US-CA for overrides when substring matches wrong state
# (e.g. "costamesa" contains "mesa"->AZ, "morenovalley" contains "reno"->NV)
CA_PLACES = frozenset({
    "sonoma", "costamesa", "riverside", "eldorado", "edcgov", "stancounty",
    "stanislaus", "mariposa", "merced", "shasta", "ventura", "sanjo", "sanjose",
    "sfdpw", "newportbeach", "newportbeachca", "chulavista", "chulavistaca",
    "sjcounty", "sanjoaquin", "pasorobles", "prcity", "tracy", "cityoftracy",
    "alameda", "mobilealameda", "marin", "sanbernardino", "sbcounty", "lametro",
    "lacity", "culvercity", "morenovalley", "losangeles",
    "pasadena", "cityofpasadena", "sccgov", "santaclara",  # Santa Clara County CA
    "oakland", "cityofoakland", "clovis", "cityofclovis",  # Oakland CA, Clovis CA
    "redlands", "coredlands",  # Redlands CA
})

# Domain/id tokens that strongly indicate a state (token -> US-XX)
# Used when state abbrev is embedded in concatenated string like "wellingtonfl"
PLACE_TO_STATE: dict[str, str] = {
    # Florida
    "capecoral": "US-FL",
    "orlando": "US-FL",
    "wellington": "US-FL",  # Wellington FL (wellingtonfl.gov)
    "miami": "US-FL",
    "tampa": "US-FL",
    "jacksonville": "US-FL",
    "tallahassee": "US-FL",
    "broward": "US-FL",
    "miamidade": "US-FL",
    "miamidadegov": "US-FL",
    "sfwmd": "US-FL",  # South Florida Water Management
    "flhealth": "US-FL",
    "flhealthgov": "US-FL",
    "manatee": "US-FL",  # Manatee County FL
    "mymanatee": "US-FL",
    "pinellas": "US-FL",
    "brevard": "US-FL",
    "brevardfl": "US-FL",
    "highlandsfl": "US-FL",
    "fortlauderdale": "US-FL",
    "flagler": "US-FL",
    "leoncountyfl": "US-FL",
    "marcoisland": "US-FL",
    "cityofmarcoisland": "US-FL",
    "bcpao": "US-FL",  # Broward County Property Appraiser
    "vcgov": "US-FL",  # Volusia County FL
    "ircgov": "US-FL",  # Indian River County FL
    # Washington
    "tulalip": "US-WA",
    "tulaliptribes": "US-WA",
    "yakima": "US-WA",
    "yakimawa": "US-WA",
    "bellevue": "US-WA",
    "bellevuewa": "US-WA",
    "seattle": "US-WA",
    "seatac": "US-WA",
    "seatacwa": "US-WA",
    "redmond": "US-WA",  # Redmond WA (vs OR)
    "redmondwa": "US-WA",
    "everett": "US-WA",
    "everettwa": "US-WA",
    "renton": "US-WA",
    "rentonwa": "US-WA",
    "tacoma": "US-WA",
    "tacomawa": "US-WA",
    "wenatchee": "US-WA",
    "wenatcheewa": "US-WA",
    "clallam": "US-WA",
    "clallamcountywa": "US-WA",
    "cowlitz": "US-WA",
    "snoqualmie": "US-WA",
    "desmoineswa": "US-WA",
    "anacortes": "US-WA",
    "cityofanacortes": "US-WA",
    "kennewick": "US-WA",
    # Texas
    "brazoria": "US-TX",
    "brazoriacounty": "US-TX",
    "pflugerville": "US-TX",
    "pflugervilletx": "US-TX",
    "mesquite": "US-TX",
    "weatherfordtx": "US-TX",
    "thecolonytx": "US-TX",
    "kerrville": "US-TX",
    "kerrvilletx": "US-TX",
    "collincounty": "US-TX",
    "collincountytx": "US-TX",
    "traviscounty": "US-TX",
    "taxmapstraviscountytx": "US-TX",
    "coppelltx": "US-TX",
    "coppell": "US-TX",
    "newbraunfels": "US-TX",
    "keller": "US-TX",
    "cityofkeller": "US-TX",
    "sherman": "US-TX",
    "cityofsherman": "US-TX",
    "seguin": "US-TX",
    "plano": "US-TX",
    "planogis": "US-TX",
    # Wisconsin
    "racine": "US-WI",
    "weston": "US-WI",  # Weston WI
    "westonwi": "US-WI",
    "milwaukee": "US-WI",
    "milwaukeegov": "US-WI",
    "browncounty": "US-WI",
    "browncountywi": "US-WI",
    "waukesha": "US-WI",
    "waukeshacounty": "US-WI",
    "lacrosse": "US-WI",
    "cityoflacrosse": "US-WI",
    "madison": "US-WI",
    "cityofmadison": "US-WI",
    "datawidnr": "US-WI",
    "widnr": "US-WI",
    # New York
    "sullcony": "US-NY",
    "sullivancounty": "US-NY",
    "binghamton": "US-NY",
    "buffalo": "US-NY",
    "buffalony": "US-NY",
    "binghamton": "US-NY",
    "nassau": "US-NY",
    "nassaucounty": "US-NY",
    "nassaucountyny": "US-NY",
    "danc": "US-NY",  # Development Authority of North Country
    "dancgis": "US-NY",
    "mapsdancgis": "US-NY",
    "dutchess": "US-NY",
    "dutchessny": "US-NY",
    "niagara": "US-NY",
    "niagaracounty": "US-NY",
    "ulster": "US-NY",
    "ulstercounty": "US-NY",
    "ulstercountyny": "US-NY",
    "cayugacounty": "US-NY",
    "cayugacountyus": "US-NY",
    "westchester": "US-NY",
    "westchestergov": "US-NY",
    # Pennsylvania
    "alleghenycounty": "US-PA",
    "alleghenycountyus": "US-PA",
    "scranton": "US-PA",
    "scrantonplanning": "US-PA",
    "wcupa": "US-PA",
    "wcupagis": "US-PA",
    "westchesteruniversity": "US-PA",  # West Chester University PA (not Westchester NY)
    "bucks": "US-PA",
    "lancaster": "US-PA",
    "lancastercounty": "US-PA",
    "lancastercountypa": "US-PA",
    "luzerne": "US-PA",
    "luzernecounty": "US-PA",
    "centrecounty": "US-PA",
    "centrecountypa": "US-PA",
    "sites4centrecountypa": "US-PA",
    "yorkcounty": "US-PA",  # York PA (could be SC too, but PA more common for gov)
    # Michigan
    "grcity": "US-MI",  # Grand Rapids MI
    "oakland": "US-MI",
    "newaygo": "US-MI",
    "kentcountymi": "US-MI",
    "detroit": "US-MI",
    "detroitmi": "US-MI",
    "gvmc": "US-MI",  # Grand Valley Metropolitan Council - Grand Rapids MI
    "gvmcregis": "US-MI",
    "troy": "US-MI",
    "troymi": "US-MI",
    "calhouncounty": "US-MI",
    "calhouncountymi": "US-MI",
    "eastlansing": "US-MI",
    "cityofeastlansing": "US-MI",
    "noblecounty": "US-IN",  # Noble County IN (noblecous - OH also has Noble)
    "noblecous": "US-IN",
    # Virginia
    "roanoke": "US-VA",
    "charlottesville": "US-VA",
    "kinggeorge": "US-VA",
    "hanovercounty": "US-VA",
    "roanokeva": "US-VA",
    "lynchburg": "US-VA",
    "lynchburgva": "US-VA",
    "arlingtonva": "US-VA",
    "arlingtonvaus": "US-VA",
    "blacksburg": "US-VA",
    "blacksburggov": "US-VA",
    "henrico": "US-VA",
    "henricous": "US-VA",
    "hampton": "US-VA",
    "hamptongov": "US-VA",
    "accomack": "US-VA",
    "accomackcounty": "US-VA",
    # North Carolina
    "cognc": "US-NC",
    "giscognc": "US-NC",
    "greenvillenc": "US-NC",
    "gastonia": "US-NC",
    "greensboro": "US-NC",
    "greensboronc": "US-NC",
    "mecklenburg": "US-NC",
    "mecklenburgcounty": "US-NC",
    "mecklenburgcountync": "US-NC",
    "hendersonville": "US-NC",
    "hendersonvillenc": "US-NC",
    "edgecombe": "US-NC",
    "edgecombecountync": "US-NC",
    "daviecounty": "US-NC",
    "daviecountync": "US-NC",
    "randolphcounty": "US-NC",
    "randolphcountync": "US-NC",
    "buncombe": "US-NC",
    "buncombecounty": "US-NC",
    "onslowcounty": "US-NC",
    "brunswickcounty": "US-NC",
    "geobrunswickcounty": "US-NC",
    "pittcounty": "US-NC",
    "gispittcounty": "US-NC",
    "wakegov": "US-NC",
    "wakecounty": "US-NC",
    "wilsonco": "US-NC",
    "wilsoncounty": "US-NC",
    "milton": "US-GA",  # Milton GA (ondemandmiltongahub)
    # Ohio
    "hamilton": "US-OH",
    "hamiltonoh": "US-OH",
    "columbus": "US-OH",
    "columbusgov": "US-OH",
    "gahanna": "US-OH",
    "gahannagov": "US-OH",
    # California (override substring false positives: costamesa contains mesa, morenovalley contains reno)
    "costamesa": "US-CA",
    "appscostamesa": "US-CA",
    "morenovalley": "US-CA",
    "mobilealameda": "US-CA",
    "mobilefreshsbcounty": "US-CA",
    "sccgovorg": "US-CA",
    "datasccgov": "US-CA",
    # Arizona
    "azmag": "US-AZ",
    "mohave": "US-AZ",
    "mcgismohave": "US-AZ",
    "pima": "US-AZ",
    "mesa": "US-AZ",
    "mesaaz": "US-AZ",
    "yuma": "US-AZ",
    "yumacounty": "US-AZ",
    "yumacountyaz": "US-AZ",
    "gilacounty": "US-AZ",
    "gilacountyaz": "US-AZ",
    "goodyear": "US-AZ",
    "goodyearaz": "US-AZ",
    "tucson": "US-AZ",
    "tucsonaz": "US-AZ",
    # Oklahoma
    "norman": "US-OK",
    "normanok": "US-OK",
    "moore": "US-OK",
    "cityofmoore": "US-OK",
    # New Mexico
    "lascruces": "US-NM",
    "cabq": "US-NM",
    "cabqgov": "US-NM",
    "nmdot": "US-NM",
    "lasvegas": "US-NV",
    "coronaviruslasvegas": "US-NV",
    # Colorado
    "boulder": "US-CO",
    "bouldercounty": "US-CO",
    "centennial": "US-CO",
    "centennialco": "US-CO",
    "pitkin": "US-CO",
    "pitkincounty": "US-CO",
    "eaglecounty": "US-CO",
    "pueblo": "US-CO",
    "pueblous": "US-CO",
    "denver": "US-CO",
    "denvergov": "US-CO",
    "westminster": "US-CO",
    "cityofwestminster": "US-CO",
    "laramiecounty": "US-WY",  # Laramie is WY
    # Nevada
    "carsoncity": "US-NV",
    "reno": "US-NV",
    "washoecounty": "US-NV",
    "washoecountyus": "US-NV",
    # Utah
    "ogden": "US-UT",
    # Wyoming
    "casper": "US-WY",
    "casperwy": "US-WY",
    "powell": "US-WY",
    # Idaho
    "pocatello": "US-ID",
    "pocatellous": "US-ID",
    "bingham": "US-ID",  # Bingham County ID (not Binghamton NY)
    "binghamid": "US-ID",
    "cobinghamidus": "US-ID",
    "bonnercounty": "US-ID",
    "bonnercountyid": "US-ID",
    "cloudgisbonnercountyid": "US-ID",
    # Iowa
    "ames": "US-IA",
    "cityofames": "US-IA",
    # Montana
    "billings": "US-MT",
    "billingsgis": "US-MT",
    # South Dakota
    "siouxfalls": "US-SD",
    "siouxfallsgov": "US-SD",
    # Mississippi
    "harrisonms": "US-MS",
    "harrisoncounty": "US-MS",
    "geocoharrisonms": "US-MS",
    # Arkansas
    "littlerock": "US-AR",
    "littlerockgov": "US-AR",
    # Alabama
    "foley": "US-AL",
    "cityoffoley": "US-AL",
    # Louisiana
    "nola": "US-LA",
    "gulfport": "US-MS",
    "gulfportms": "US-MS",
    # Missouri
    "capecounty": "US-MO",
    "stlouis": "US-MO",
    "stlouisco": "US-MO",
    "stlouismogov": "US-MO",
    "maps6stlouismo": "US-MO",
    "libertymogov": "US-MO",
    # Minnesota
    "winonaco": "US-MN",
    "winonacounty": "US-MN",
    "wilkin": "US-MN",
    "wilkinco": "US-MN",
    "shareopendatawilkinco": "US-MN",
    "stlouiscounty": "US-MN",
    "stlouiscountymn": "US-MN",
    "claycounty": "US-MN",
    "claycountymn": "US-MN",
    "bigstonecounty": "US-MN",
    "eagan": "US-MN",
    "cityofeagan": "US-MN",
    # Illinois
    "gurnee": "US-IL",
    "villageofgurnee": "US-IL",
    "evanston": "US-IL",
    "cityofevanston": "US-IL",
    "lombard": "US-IL",
    "villageoflombard": "US-IL",
    "downers": "US-IL",
    "downersus": "US-IL",
    "lasalle": "US-IL",
    "lasallecounty": "US-IL",
    "peoria": "US-IL",
    "peoriacounty": "US-IL",
    "cookcounty": "US-IL",
    "cookcountyil": "US-IL",
    "imgcookcountyil": "US-IL",
    "decatur": "US-IL",
    "decaturil": "US-IL",
    "grundy": "US-IL",
    "grundyco": "US-IL",
    # Maryland
    "baltimore": "US-MD",
    "baltimorecounty": "US-MD",
    "baltimorecountymd": "US-MD",
    "bcgisbaltimorecountymd": "US-MD",
    "calvert": "US-MD",
    "calvertcounty": "US-MD",
    # Massachusetts
    "eastlongmeadow": "US-MA",
    "eastlongmeadowma": "US-MA",
    "boston": "US-MA",
    "cityofboston": "US-MA",
    "digitalmass": "US-MA",
    "massgov": "US-MA",
    "giseeamass": "US-MA",
    # Alabama
    "alexandercity": "US-AL",
    "alexandercityal": "US-AL",
    # Arkansas
    "springdale": "US-AR",
    "springdalear": "US-AR",
    "conway": "US-AR",
    # Connecticut
    "glastonbury": "US-CT",
    "glastonburyct": "US-CT",
    # South Carolina
    "charleston": "US-SC",
    "charlestonsc": "US-SC",
    "bluffton": "US-SC",
    "townofbluffton": "US-SC",
    "townofgreenville": "US-SC",
    "rockhill": "US-SC",
    "cityofrockhill": "US-SC",
    "clinton": "US-SC",
    "cityofclintonsc": "US-SC",
    "brookhaven": "US-GA",  # Brookhaven GA
    "brookhavenga": "US-GA",
    # Georgia
    "gwinnett": "US-GA",
    "sandysprings": "US-GA",
    "sandyspringsgagov": "US-GA",
    # Tennessee
    "cleveland": "US-TN",
    "clevelandtn": "US-TN",
    "nashville": "US-TN",
    "nashvillegov": "US-TN",
    "blount": "US-TN",
    "blountgis": "US-TN",
    # Indiana
    "porter": "US-IN",
    "portercounty": "US-IN",
    "proporterco": "US-IN",
    "southbend": "US-IN",
    "southbendingov": "US-IN",
    "evansville": "US-IN",
    "evansvillegis": "US-IN",
    "hobart": "US-IN",
    "cityofhobart": "US-IN",
    # Kentucky
    "shelbycountyauditors": "US-OH",  # Shelby County OH (cama.shelbycountyauditors.com)
    "camashelby": "US-OH",
    "lojic": "US-KY",
    "paducah": "US-KY",
    "paducahky": "US-KY",
    # Delaware
    "kentcounty": "US-DE",
    "kentcountyde": "US-DE",
    "giskentcountyde": "US-DE",
    # Ohio
    "putnamcounty": "US-OH",
    "putnamcountygis": "US-OH",
    # Illinois
    "cityofdanville": "US-IL",
    "danville": "US-IL",
    # Maryland
    "cityofbowie": "US-MD",
    "bowie": "US-MD",
    "annapolis": "US-MD",
    "cityofannapolis": "US-MD",
    "cecilcounty": "US-MD",
    "cecilmaps": "US-MD",
    "cecilmapsccgov": "US-MD",
    "ccgovorg": "US-MD",  # Cecil County ccgov.org - avoid matching sccgov
    "pgplanning": "US-MD",  # Prince George's County MD
    "frederick": "US-MD",
    "cityoffrederick": "US-MD",
    # Utah
    "cachecounty": "US-UT",
    "cachecountygov": "US-UT",
    # Georgia
    "accgov": "US-GA",  # Athens-Clarke County GA
    # Colorado
    "jeffco": "US-CO",
    "jeffcous": "US-CO",
    "elpasoco": "US-CO",
    "elpasocounty": "US-CO",
    "codbus": "US-CO",
    "cosbus": "US-CO",
    # Florida
    "leegov": "US-FL",
    "leecounty": "US-FL",
    "cityofdoral": "US-FL",
    "doral": "US-FL",
    # Minnesota
    "ottertail": "US-MN",
    "ottertailcounty": "US-MN",
    "coottertailmn": "US-MN",
    # Montana
    "mtdeq": "US-MT",
    "missoulacounty": "US-MT",
    "missoulacountyus": "US-MT",
    # Oregon
    "kfalls": "US-OR",
    "cityofkfalls": "US-OR",
    "klamathfalls": "US-OR",
    # Alabama
    "cityofmobile": "US-AL",
    "mobile": "US-AL",
    "cityofgadsden": "US-AL",
    "gadsden": "US-AL",
    # North Carolina
    "nconemap": "US-NC",
    "nconemapgov": "US-NC",
    # Washington
    "cityofpaus": "US-WA",  # Port Angeles WA (cityofpa.us)
    "jeffcowa": "US-WA",  # Jefferson County WA
    "cityofvancouver": "US-WA",
    "vancouverus": "US-WA",
    "kingcounty": "US-WA",
    "gisdatakingcounty": "US-WA",
    # North Dakota
    "cityoffargo": "US-ND",
    "fargo": "US-ND",
    # Idaho
    "postfalls": "US-ID",
    "postfallsgov": "US-ID",
    "boise": "US-ID",
    "cityofboise": "US-ID",
    "lewiston": "US-ID",
    "cityoflewiston": "US-ID",
    # Kentucky
    "boonecounty": "US-KY",
    "boonecountygis": "US-KY",
    # Rhode Island
    "provwater": "US-RI",
    "providencewater": "US-RI",
    # Texas
    "cityofdenton": "US-TX",
    "denton": "US-TX",
    "sanantonio": "US-TX",
    "qagissanantonio": "US-TX",
    "cityofwebster": "US-TX",
    "webster": "US-TX",
    # Hawaii
    "kauai": "US-HI",
    "kauaigov": "US-HI",
    # Connecticut
    "hartford": "US-CT",
    "hartfordgov": "US-CT",
    # Indiana
    "heartlandmpo": "US-IN",
    "heartlandmpoorg": "US-IN",
    # Ohio
    "cityofmiddletown": "US-OH",
    "middletown": "US-OH",
    # West Virginia
    "morgantownwv": "US-WV",
    "morgantown": "US-WV",
    # New Jersey
    "hoboken": "US-NJ",
    "cityofhoboken": "US-NJ",
    "somersetcounty": "US-NJ",
    "scogisopendatasomerset": "US-NJ",
    # Wyoming
    "wsgswyo": "US-WY",
    "wyogov": "US-WY",
    # US Virgin Islands
    "usvi": "US-VI",
    # Alaska
    "alaska": "US-AK",
    "kodiak": "US-AK",
    "kiborough": "US-AK",
    "adfg": "US-AK",  # Alaska Dept of Fish and Game
    # Montana
    "bozeman": "US-MT",
    # Texas (Alamo = San Antonio area)
    "alamoarea": "US-TX",
    "alamoareampo": "US-TX",
    # New Hampshire
    "nashua": "US-NH",
    "nashuanh": "US-NH",
    # North Carolina
    "salisburync": "US-NC",
    # Federal / multi-state (keep in CA or move to Federal)
    "geoplatform": "US-Federal",
    "disastersgeoplatform": "US-Federal",
    "usbr": "US-Federal",
    "geousbr": "US-Federal",
    "cogs": "US-Federal",
    "mapscogsus": "US-Federal",
    "widma": "US-Federal",
    "widmamaps": "US-Federal",
}

# Subregion display names
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


def extract_host(link: str) -> str:
    if not link:
        return ""
    parsed = urlparse(link if "://" in link else f"https://{link}")
    host = (parsed.netloc or "").lower().replace("www.", "").split("/")[0]
    return host


def infer_subregion_from_record(record: dict) -> str | None:
    """Infer actual US subregion from link, id, owner name."""
    link = (record.get("link") or "").strip()
    record_id = (record.get("id") or "").strip().lower()
    owner_name = ((record.get("owner", {}) or {}).get("name", "") or "").strip().lower()
    host = extract_host(link)

    combined = f" {record_id} {owner_name} {host} "
    combined_compact = re.sub(r"[^a-z0-9]", "", combined)

    # 1. Check place-to-state mapping first (before CA_PLACES to avoid false positives
    #    e.g. cecilmapsccgov contains "sccgov" but is Cecil County MD, not Santa Clara CA)
    for place, subregion in sorted(PLACE_TO_STATE.items(), key=lambda x: -len(x[0])):
        if place in combined_compact or place in combined:
            if subregion == "US-Federal":
                return None  # Federal - don't move
            return subregion

    # 2. California places - stay in US-CA (checked after PLACE_TO_STATE)
    for place in CA_PLACES:
        if place in combined_compact or place in combined:
            return None  # Stay in CA

    # 3. State abbrev suffix patterns - require clear place+state (e.g. wellingtonfl, citytxgov)
    # Avoid matching "coral"->al, "newaygo"->ar - use stricter pattern: place+abbrev+gov
    for abbrev in US_STATE_ABBREVS:
        if abbrev == "ca":
            continue
        subregion = ABBREV_TO_SUBREGION[abbrev]
        # Must have abbrev directly before gov/org/com (e.g. wellingtonflgov, brazoriacountytxgov)
        if f"{abbrev}gov" in combined_compact or f"{abbrev}org" in combined_compact:
            # Exclude false positives: "coral" has "al", "ral" - "al"+"gov" in "coralgov"
            # Cape Coral FL: capecoralgov - "fl" not in there. "al"+"gov" - "coralgov" has "al" before "gov"? "coral"+"gov" = "coralgov". "al" is before "gov" - "al"+"gov" - no, "l"+"gov" - "coral" ends with "l", so "coral"+"gov" = "coralgov" - the "al" is "a"+"l", and "l" is right before "g". So we have "al" but "l" connects to "gov". So "al"+"gov" - the substring "algov" - "al"+"gov" - in "coralgov" we have "ralgov" - "al" - "c-o-r-a-l-g-o-v" - "a-l" - "al" - then "g" - so "al" is followed by "g" from "gov". So "algov" is in "coralgov"! So we'd match. To avoid: require that the char before abbrev is not part of common false words. "coral" = c-o-r-a-l. Before "al" is "r". So we need "ral" - "r"+"al". We could require that abbrev is preceded by a lowercase letter that's not creating a word - hard. Simpler: add "capecoral" -> US-FL to override.
            return subregion
        if f"county{abbrev}" in combined_compact or f"city{abbrev}" in combined_compact:
            return subregion

    return None


def main() -> None:
    dry_run = "--dry-run" in __import__("sys").argv
    if dry_run:
        print("DRY RUN - no files will be moved\n")

    to_move: list[tuple[Path, str, dict]] = []  # (src_path, target_subregion, record)

    for yaml_path in sorted(US_CA_DIR.rglob("*.yaml")):
        try:
            data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  Skip {yaml_path.name}: {e}")
            continue
        if not isinstance(data, dict):
            continue

        owner_subregion = (
            (data.get("owner", {}) or {}).get("location", {}) or {}
        ).get("subregion")
        owner_sr_id = (owner_subregion.get("id") or "").strip() if isinstance(owner_subregion, dict) else None
        if owner_sr_id != "US-CA":
            continue

        inferred = infer_subregion_from_record(data)
        if inferred and inferred != "US-CA":
            rel = yaml_path.relative_to(US_CA_DIR)
            catalog_type = rel.parts[0] if len(rel.parts) > 1 else "geo"
            to_move.append((yaml_path, inferred, data))

    print(f"Found {len(to_move)} records in US-CA that belong to other subregions:\n")
    by_target: dict[str, list[tuple[Path, dict]]] = {}
    for path, subregion, record in to_move:
        by_target.setdefault(subregion, []).append((path, record))

    for subregion in sorted(by_target.keys()):
        items = by_target[subregion]
        print(f"  {subregion} ({len(items)}):")
        for path, record in items[:5]:
            print(f"    - {path.name} ({record.get('owner',{}).get('name','?')})")
        if len(items) > 5:
            print(f"    ... and {len(items)-5} more")
        print()

    if dry_run or not to_move:
        return

    # Move files and update YAML
    for src_path, target_subregion, data in to_move:
        rel = src_path.relative_to(US_CA_DIR)
        catalog_type = rel.parts[0] if len(rel.parts) > 1 else "geo"
        target_dir = ENTITIES_DIR / "US" / target_subregion / catalog_type
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / src_path.name

        # Update owner subregion in data
        owner = data.get("owner", {}) or {}
        loc = owner.get("location", {}) or {}
        loc["subregion"] = {
            "id": target_subregion,
            "name": US_SUBREGION_NAMES.get(target_subregion, target_subregion),
        }
        owner["location"] = loc
        data["owner"] = owner

        target_path.write_text(
            yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
        src_path.unlink()
        print(f"Moved {src_path.name} -> US/{target_subregion}/{catalog_type}/")

    print(f"\nMoved {len(to_move)} files. Run: python scripts/builder.py validate-yaml")


if __name__ == "__main__":
    main()
