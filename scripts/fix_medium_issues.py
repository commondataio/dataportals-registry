#!/usr/bin/env python3
"""
Script to fix MEDIUM priority issues:
- MISSING_DESCRIPTION
- MISSING_ENDPOINTS
- MISSING_LANGS
- SHORT_DESCRIPTION
- TAG_HYGIENE
"""
import re
import yaml
from pathlib import Path
from urllib.parse import urlparse

# Base directories
BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"

# Language mapping based on country codes (from constants.py)
LANGS_BY_COUNTRY = {
    "US": ["EN"], "GB": ["EN"], "AU": ["EN"], "CA": ["EN"], "NZ": ["EN"],
    "IE": ["EN"], "ZA": ["EN"], "ZW": ["EN"], "TT": ["EN"], "TZ": ["EN"],
    "RW": ["EN"], "UG": ["EN"], "NG": ["EN"], "ZM": ["EN"], "LK": ["EN"],
    "SG": ["EN"], "IN": ["EN"], "MN": ["MN"], "LT": ["LT"],
    "ES": ["ES"], "AR": ["ES"], "SV": ["ES"], "MX": ["ES"], "CL": ["ES"],
    "GT": ["ES"], "HN": ["ES"], "EC": ["ES"], "CR": ["ES"], "PE": ["ES"],
    "PY": ["ES"], "BO": ["ES"], "UY": ["ES"], "DO": ["ES"], "AD": ["ES"],
    "JP": ["JP"], "AT": ["DE"], "DE": ["DE"], "CH": ["DE"],
    "FR": ["FR"], "CI": ["FR"], "TG": ["FR"], "BE": ["FR"], "LU": ["FR"],
    "BF": ["FR"], "CD": ["FR"], "RU": ["RU"], "BY": ["RU"],
    "DK": ["DA"], "FO": ["DA"], "GL": ["DA"], "FI": ["FI"],
    "NL": ["NL"], "BG": ["BG"], "BH": ["AR"], "SA": ["AR"], "PS": ["AR"],
    "TN": ["AR"], "LB": ["AR"], "MA": ["AR"], "KW": ["AR"], "AE": ["AR"],
    "SD": ["AR"], "IT": ["IT"], "PT": ["PT"], "MZ": ["PT"], "BR": ["PT"],
    "TH": ["TH"], "TR": ["TR"], "GR": ["EL"], "CY": ["EL"],
    "VN": ["VN"], "CN": ["CN"], "MO": ["CN"], "ME": ["SR"], "RS": ["SR"],
    "BA": ["SR"], "AL": ["AL"], "LV": ["LV"], "PL": ["PL"],
    "LA": ["LA"], "AM": ["AM"], "SE": ["SV"], "TW": ["zh_TW"],
    "SI": ["SL"], "NO": ["NO"], "RO": ["RO"], "ID": ["ID"],
    "EE": ["ET"], "HU": ["HU"], "KR": ["KR"], "CZ": ["CZ"],
    "GE": ["KA"], "IL": ["HE"], "SK": ["SK"], "NP": ["NE"],
    "HR": ["HR"], "MK": ["MK"], "MD": ["MD"],
    # Multi-language countries
    "BE": ["FR", "NL"],  # Belgium has both French and Dutch
    "CH": ["DE", "FR", "IT"],  # Switzerland has multiple languages
    "CA": ["EN", "FR"],  # Canada has English and French
}

# Language code to name mapping (shared)
LANG_NAMES = {
    "EN": "English", "ES": "Spanish", "FR": "French", "DE": "German",
    "IT": "Italian", "PT": "Portuguese", "RU": "Russian", "JP": "Japanese",
    "CN": "Chinese", "KR": "Korean", "AR": "Arabic", "EL": "Greek",
    "TH": "Thai", "TR": "Turkish", "VN": "Vietnamese", "PL": "Polish",
    "NL": "Dutch", "SV": "Swedish", "NO": "Norwegian", "DA": "Danish",
    "FI": "Finnish", "CZ": "Czech", "SK": "Slovak", "HU": "Hungarian",
    "RO": "Romanian", "BG": "Bulgarian", "HR": "Croatian", "SR": "Serbian",
    "SL": "Slovenian", "MK": "Macedonian", "AL": "Albanian", "ET": "Estonian",
    "LV": "Latvian", "LT": "Lithuanian", "ID": "Indonesian", "HE": "Hebrew",
    "KA": "Georgian", "AM": "Armenian", "NE": "Nepali", "MD": "Moldovan",
    "zh_TW": "Chinese (Traditional)", "zh_CN": "Chinese (Simplified)",
    "MN": "Mongolian", "LA": "Lao"
}

def infer_languages_from_country(record):
    """Infer languages from country code (coverage or owner fallback)."""
    # Get country from coverage first
    country_id = None
    coverage = record.get("coverage", [])
    if coverage and isinstance(coverage, list) and len(coverage) > 0:
        location = coverage[0].get("location", {})
        country = location.get("country", {})
        country_id = country.get("id")

    # Fallback to owner location when coverage is Unknown/World/missing
    if not country_id or country_id in ("Unknown", "World"):
        owner = record.get("owner", {}) or {}
        owner_loc = owner.get("location", {}) or {}
        owner_country = owner_loc.get("country", {}) or {}
        country_id = owner_country.get("id") or country_id

    # Resolve language codes
    if country_id and country_id not in ("Unknown", "World"):
        country_langs = LANGS_BY_COUNTRY.get(country_id, [])
    else:
        country_langs = ["EN"]  # Default for Unknown/World/missing

    if not country_langs:
        country_langs = ["EN"]  # Fallback when country not in map (e.g. EU)

    langs = [
        {"id": code, "name": LANG_NAMES.get(code, code)}
        for code in country_langs
    ]
    return langs if langs else None

def generate_description(record):
    """Generate a basic description based on available information"""
    catalog_type = record.get("catalog_type", "")
    name = record.get("name", "")
    link = record.get("link", "")
    software = record.get("software", {})
    software_name = software.get("name", "") if isinstance(software, dict) else ""
    owner = record.get("owner", {})
    owner_name = owner.get("name", "") if isinstance(owner, dict) else ""
    
    # Build description from available info
    parts = []
    
    if catalog_type:
        parts.append(f"{catalog_type}")
    
    if owner_name and owner_name != "Unknown":
        parts.append(f"managed by {owner_name}")
    
    if software_name:
        parts.append(f"powered by {software_name}")
    
    if link:
        try:
            parsed = urlparse(link)
            domain = parsed.netloc or link
            # Extract meaningful domain info
            if domain:
                parts.append(f"accessible at {domain}")
        except:
            pass
    
    if parts:
        description = ". ".join(parts) + "."
        # Clean up common patterns
        description = re.sub(r'\.+', '.', description)  # Multiple dots
        description = re.sub(r'\s+', ' ', description)  # Multiple spaces
        return description.strip()
    
    # Fallback description
    if catalog_type:
        return f"{catalog_type} providing data and services."
    elif name:
        return f"Data portal: {name}."
    else:
        return "Data portal providing datasets and services."

def infer_endpoints(record):
    """Try to infer common endpoints based on software and link"""
    endpoints = []
    link = record.get("link", "")
    software = record.get("software", {})
    software_id = software.get("id", "") if isinstance(software, dict) else ""
    
    if not link:
        return endpoints
    
    try:
        parsed = urlparse(link)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        path = parsed.path.rstrip('/')
        
        # Common endpoint patterns by software
        if software_id == "ckan":
            endpoints.extend([
                {"type": "ckan:api", "url": f"{base_url}/api/3/action/package_list", "version": "3.0"},
                {"type": "sitemap", "url": f"{base_url}/sitemap.xml"},
            ])
        elif software_id == "arcgishub" or "arcgis" in link.lower():
            endpoints.extend([
                {"type": "dcatap201", "url": f"{base_url}/api/feed/dcat-ap/2.0.1.json"},
                {"type": "dcatus11", "url": f"{base_url}/api/feed/dcat-us/1.1.json"},
                {"type": "rss", "url": f"{base_url}/api/feed/rss/2.0"},
                {"type": "ogcrecordsapi", "url": f"{base_url}/api/search/v1"},
                {"type": "sitemap", "url": f"{base_url}/sitemap.xml"},
            ])
        elif software_id == "geoserver":
            endpoints.extend([
                # Standard GeoServer paths
                {"type": "wms130", "url": f"{base_url}/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities", "version": "1.3.0"},
                {"type": "wfs200", "url": f"{base_url}/geoserver/ows?service=WFS&version=2.0.0&request=GetCapabilities", "version": "2.0.0"},
                {"type": "wcs111", "url": f"{base_url}/geoserver/ows?service=WCS&version=1.1.1&request=GetCapabilities", "version": "1.1.1"},
                # Non-standard GeoServer paths (e.g., /geo/wms instead of /ows)
                {"type": "wms130", "url": f"{base_url}/geoserver/geo/wms?service=WMS&version=1.3.0&request=GetCapabilities", "version": "1.3.0"},
                {"type": "wfs200", "url": f"{base_url}/geoserver/geo/wfs?service=WFS&version=2.0.0&request=GetCapabilities", "version": "2.0.0"},
                {"type": "wcs111", "url": f"{base_url}/geoserver/geo/wms?service=WCS&version=1.1.1&request=GetCapabilities", "version": "1.1.1"},
            ])
        elif software_id == "arcgisserver":
            endpoints.extend([
                {"type": "arcgis:rest:info", "url": f"{base_url}/arcgis/rest/info?f=pjson"},
                {"type": "arcgis:rest:services", "url": f"{base_url}/arcgis/rest/services?f=pjson"},
            ])
        elif software_id == "dataverse":
            endpoints.extend([
                {"type": "dataverseapi", "url": f"{base_url}/api/search"},
                {"type": "oaipmh20", "url": f"{base_url}/oai?verb=Identify", "version": "2.0"},
            ])
        elif software_id == "opendatasoft":
            endpoints.extend([
                {"type": "opendatasoft:api", "url": f"{base_url}/api/v2/catalog/datasets"},
            ])
        elif software_id == "socrata":
            endpoints.extend([
                {"type": "socrata:api", "url": f"{base_url}/api/views"},
            ])
        
        # Always try sitemap
        if not any(e.get("type") == "sitemap" for e in endpoints):
            endpoints.append({"type": "sitemap", "url": f"{base_url}/sitemap.xml"})
    
    except Exception:
        pass
    
    return endpoints

def fix_description(record, file_path):
    """Fix MISSING_DESCRIPTION"""
    description = record.get("description", "")
    
    # Check if description is missing or is a placeholder
    if not description or description == "None" or "temporary record" in description.lower() or "should be updated" in description.lower():
        new_description = generate_description(record)
        record["description"] = new_description
        return True, f"Generated description: '{new_description[:80]}...'"
    
    return False, None

def fix_endpoints(record, file_path):
    """Fix MISSING_ENDPOINTS"""
    endpoints = record.get("endpoints", [])
    
    if not endpoints or (isinstance(endpoints, list) and len(endpoints) == 0):
        inferred_endpoints = infer_endpoints(record)
        if inferred_endpoints:
            record["endpoints"] = inferred_endpoints
            return True, f"Added {len(inferred_endpoints)} endpoints"
    
    return False, None

def fix_langs(record, file_path):
    """Fix MISSING_LANGS"""
    langs = record.get("langs", [])
    
    if not langs or (isinstance(langs, list) and len(langs) == 0):
        inferred_langs = infer_languages_from_country(record)
        if inferred_langs:
            record["langs"] = inferred_langs
            lang_names = [lang.get("name", lang.get("id")) for lang in inferred_langs]
            return True, f"Added languages: {', '.join(lang_names)}"
    
    return False, None

def expand_description(record, file_path):
    """Fix SHORT_DESCRIPTION by expanding descriptions that are too short"""
    description = record.get("description", "")
    
    if not description or not isinstance(description, str):
        return False, None
    
    description = description.strip()
    
    # Check if description is too short (less than 40 characters)
    if len(description) < 40:
        # Try to expand it using generate_description logic
        catalog_type = record.get("catalog_type", "")
        name = record.get("name", "")
        link = record.get("link", "")
        software = record.get("software", {})
        software_name = software.get("name", "") if isinstance(software, dict) else ""
        owner = record.get("owner", {})
        owner_name = owner.get("name", "") if isinstance(owner, dict) else ""
        
        # Build expanded description
        parts = []
        
        # Start with existing description if it's meaningful
        if description and description not in ["None", "Not specified", "No description provided"]:
            parts.append(description.rstrip('.'))
        
        # Add context
        if catalog_type and catalog_type not in description:
            parts.append(f"{catalog_type.lower()}")
        
        if owner_name and owner_name != "Unknown" and owner_name not in description:
            parts.append(f"managed by {owner_name}")
        
        if software_name and software_name not in description:
            parts.append(f"powered by {software_name}")
        
        if link:
            try:
                parsed = urlparse(link)
                domain = parsed.netloc or link
                if domain and domain not in description:
                    # Just add domain info if not already there
                    if not any(part.startswith("accessible at") for part in parts):
                        parts.append(f"accessible at {domain}")
            except:
                pass
        
        # If we have parts, combine them
        if parts:
            new_description = ". ".join(parts) + "."
            # Clean up
            new_description = re.sub(r'\.+', '.', new_description)
            new_description = re.sub(r'\s+', ' ', new_description)
            new_description = new_description.strip()
            
            # Only update if it's actually longer
            if len(new_description) >= 40:
                record["description"] = new_description
                return True, f"Expanded description from {len(description)} to {len(new_description)} characters"
        
        # Fallback: add generic context
        if not parts or len(description) < 20:
            if catalog_type:
                new_description = f"{description.rstrip('.')}. {catalog_type} providing datasets and services."
            else:
                new_description = f"{description.rstrip('.')}. Data portal providing datasets and services."
            
            new_description = re.sub(r'\.+', '.', new_description)
            new_description = re.sub(r'\s+', ' ', new_description)
            new_description = new_description.strip()
            
            if len(new_description) >= 40:
                record["description"] = new_description
                return True, f"Expanded description from {len(description)} to {len(new_description)} characters"
    
    return False, None

def fix_tag_hygiene(record, file_path, field):
    """Fix TAG_HYGIENE by removing or fixing problematic tags"""
    # Parse field like "tags[2]" to get index
    match = re.match(r'tags\[(\d+)\]', field)
    if not match:
        return False, None
    
    tag_index = int(match.group(1))
    tags = record.get("tags", [])
    
    if not isinstance(tags, list) or tag_index >= len(tags):
        return False, None
    
    tag = tags[tag_index]
    
    if not isinstance(tag, str):
        return False, None
    
    tag_stripped = tag.strip()
    
    # Remove tags that are too short (less than 3 characters) or empty
    if len(tag_stripped) < 3:
        tags.pop(tag_index)
        record["tags"] = tags
        return True, f"Removed short tag '{tag}' (less than 3 characters)"
    
    # Fix tags that are too long (more than 40 characters) - truncate or remove
    if len(tag_stripped) > 40:
        # Try to truncate intelligently
        truncated = tag_stripped[:37] + "..."
        tags[tag_index] = truncated
        record["tags"] = tags
        return True, f"Truncated long tag from {len(tag_stripped)} to {len(truncated)} characters"
    
    # Fix empty tags
    if not tag_stripped:
        tags.pop(tag_index)
        record["tags"] = tags
        return True, "Removed empty tag"
    
    return False, None

def parse_medium_file(medium_file_path):
    """Parse MEDIUM.txt and extract file paths and issues"""
    issues = []
    with open(medium_file_path, 'r', encoding='utf-8') as f:
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

def fix_yaml_file(file_path, issue_type, field=None):
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
        
        if issue_type == "MISSING_DESCRIPTION":
            fixed, message = fix_description(data, file_path)
        elif issue_type == "MISSING_ENDPOINTS":
            fixed, message = fix_endpoints(data, file_path)
        elif issue_type == "MISSING_LANGS":
            fixed, message = fix_langs(data, file_path)
        elif issue_type == "SHORT_DESCRIPTION":
            fixed, message = expand_description(data, file_path)
        elif issue_type == "TAG_HYGIENE":
            fixed, message = fix_tag_hygiene(data, file_path, field)
        
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
    medium_file = BASE_DIR / "dataquality" / "priorities" / "MEDIUM.txt"
    
    if not medium_file.exists():
        print(f"Error: {medium_file} not found")
        return
    
    print("Parsing MEDIUM.txt...")
    issues = parse_medium_file(medium_file)
    print(f"Found {len(issues)} issues to fix")
    
    fixed_count = 0
    skipped_count = 0
    for file_path, issue_type, field in issues:
        result = fix_yaml_file(file_path, issue_type, field)
        if result:
            fixed_count += 1
        else:
            skipped_count += 1
    
    print(f"\nFixed {fixed_count} out of {len(issues)} files")
    if skipped_count > 0:
        print(f"Skipped {skipped_count} files (may have been already fixed or couldn't be inferred)")

if __name__ == "__main__":
    main()
