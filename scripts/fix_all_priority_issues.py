#!/usr/bin/env python3
"""
Fix all data quality issues from primary_priority.jsonl.

Reads dataquality/primary_priority.jsonl and fixes all issues for each record.
"""

import json
import re
import yaml
from pathlib import Path
from urllib.parse import urlparse
from typing import Dict, Any, List, Optional, Tuple
import sys

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

# Import constants
sys.path.insert(0, str(Path(__file__).parent))
from constants import MAP_SOFTWARE_OWNER_CATALOG_TYPE, COUNTRIES

BASE_DIR = Path(__file__).parent.parent
ISSUES_FILE = BASE_DIR / "dataquality" / "primary_priority.jsonl"
ENTITIES_DIR = BASE_DIR / "data" / "entities"


# Language mapping (from fix_medium_issues.py)
LANGS_BY_COUNTRY_DETAILED = {
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
    "BE": ["FR", "NL"],  # Belgium
    "CH": ["DE", "FR", "IT"],  # Switzerland
    "CA": ["EN", "FR"],  # Canada
}

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


class IssueFixer:
    """Fixes data quality issues in YAML records."""
    
    def __init__(self, record: Dict[str, Any], file_path: Path):
        self.record = record
        self.file_path = file_path
        self.changes = []
    
    def fix_catalog_software_mismatch(self, issue: Dict[str, Any]) -> bool:
        """Fix CATALOG_SOFTWARE_MISMATCH by updating catalog_type to match software.id"""
        software = self.record.get("software", {})
        software_id = software.get("id")
        
        if software_id and software_id in MAP_SOFTWARE_OWNER_CATALOG_TYPE:
            expected_type = MAP_SOFTWARE_OWNER_CATALOG_TYPE[software_id]
            current_type = self.record.get("catalog_type", "")
            
            if current_type != expected_type:
                self.record["catalog_type"] = expected_type
                self.changes.append(f"Updated catalog_type from '{current_type}' to '{expected_type}'")
                return True
        return False
    
    def fix_missing_owner_name(self, issue: Dict[str, Any]) -> bool:
        """Fix MISSING_OWNER_NAME"""
        owner = self.record.get("owner", {})
        owner_name = owner.get("name", "")
        
        if not owner_name or owner_name == "Unknown" or owner_name == "":
            # Try to infer from portal link
            portal_link = self.record.get("link", "")
            if portal_link:
                try:
                    parsed = urlparse(portal_link)
                    domain = parsed.netloc or portal_link
                    domain = re.sub(r'^www\.', '', domain.lower())
                    domain = re.sub(r'^(data|gis|geo|map|portal|opendata)\.', '', domain)
                    parts = domain.split('.')
                    if len(parts) > 0:
                        name = parts[0].replace('-', ' ').title()
                        if name and name != 'Unknown':
                            owner["name"] = name
                            self.changes.append(f"Set owner.name to '{name}'")
                            return True
                except Exception:
                    pass
            
            # Fallback to portal name
            portal_name = self.record.get("name", "")
            if portal_name and portal_name != "Unknown":
                owner["name"] = portal_name
                self.changes.append(f"Set owner.name to portal name '{portal_name}'")
                return True
        
        return False
    
    def fix_missing_owner_type(self, issue: Dict[str, Any]) -> bool:
        """Fix MISSING_OWNER_TYPE"""
        owner = self.record.get("owner", {})
        owner_type = owner.get("type", "")
        
        if not owner_type or owner_type == "Unknown":
            owner_link = owner.get("link", "")
            if owner_link:
                domain = urlparse(owner_link).netloc or owner_link
                domain = domain.lower()
                
                if '.gov' in domain or '.gob' in domain:
                    if any(x in domain for x in ['.city.', '.county.', '.local.', 'cityof', 'countyof']):
                        owner_type = "Local government"
                    elif any(x in domain for x in ['.state.', '.province.', 'provincial']):
                        owner_type = "Regional government"
                    else:
                        owner_type = "Central government"
                elif '.edu' in domain or '.ac.' in domain:
                    owner_type = "Academy"
                elif any(x in domain for x in ['.org', '.int', 'un.org']):
                    owner_type = "International organization"
                elif '.com' in domain or '.co.' in domain:
                    owner_type = "Business"
            
            if not owner_type or owner_type == "Unknown":
                catalog_type = self.record.get("catalog_type", "").lower()
                if 'scientific' in catalog_type or 'research' in catalog_type:
                    owner_type = "Academy"
                elif 'government' in catalog_type or 'public' in catalog_type:
                    owner_type = "Central government"
                else:
                    owner_type = "Business"
            
            owner["type"] = owner_type
            self.changes.append(f"Set owner.type to '{owner_type}'")
            return True
        
        return False
    
    def fix_missing_owner_link(self, issue: Dict[str, Any]) -> bool:
        """Fix MISSING_OWNER_LINK"""
        owner = self.record.get("owner", {})
        if owner.get("link"):
            return False  # Already has link
        
        portal_link = self.record.get("link", "")
        if portal_link:
            try:
                parsed = urlparse(portal_link)
                domain = parsed.netloc or portal_link
                domain = re.sub(r'^(data|gis|geo|map|portal|opendata|www)\.', '', domain.lower())
                domain = re.sub(r'\.(hub|opendata|arcgis|geoserver|geonetwork|ckan|dataverse)', '', domain)
                
                parts = domain.split('.')
                if len(parts) >= 2:
                    base_domain = '.'.join(parts[-2:])
                    if re.match(r'^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*\.[a-zA-Z]{2,}$', base_domain):
                        owner["link"] = f"https://{base_domain}"
                        self.changes.append(f"Set owner.link to '{owner['link']}'")
                        return True
            except Exception:
                pass
        
        return False
    
    def fix_missing_langs(self, issue: Dict[str, Any]) -> bool:
        """Fix MISSING_LANGS"""
        if self.record.get("langs"):
            return False  # Already has langs
        
        coverage = self.record.get("coverage", [])
        if coverage and isinstance(coverage, list) and len(coverage) > 0:
            location = coverage[0].get("location", {})
            country = location.get("country", {})
            country_id = country.get("id")
            
            if country_id and country_id != "Unknown":
                country_langs = LANGS_BY_COUNTRY_DETAILED.get(country_id, ["EN"])
                langs = []
                for lang_code in country_langs:
                    langs.append({
                        "id": lang_code,
                        "name": LANG_NAMES.get(lang_code, lang_code)
                    })
                
                if langs:
                    self.record["langs"] = langs
                    self.changes.append(f"Set langs to {[l['id'] for l in langs]}")
                    return True
        
        return False
    
    def fix_missing_description(self, issue: Dict[str, Any]) -> bool:
        """Fix MISSING_DESCRIPTION"""
        if self.record.get("description"):
            return False  # Already has description
        
        catalog_type = self.record.get("catalog_type", "")
        name = self.record.get("name", "")
        owner = self.record.get("owner", {})
        owner_name = owner.get("name", "") if isinstance(owner, dict) else ""
        
        parts = []
        if catalog_type:
            parts.append(f"{catalog_type}")
        if owner_name:
            parts.append(f"managed by {owner_name}")
        if name:
            parts.append(f"providing access to {name.lower()}")
        
        if parts:
            description = ". ".join(parts) + "."
            self.record["description"] = description
            self.changes.append("Added generated description")
            return True
        
        return False
    
    def fix_short_description(self, issue: Dict[str, Any]) -> bool:
        """Fix SHORT_DESCRIPTION"""
        description = self.record.get("description", "")
        if len(description) >= 40:
            return False  # Already long enough
        
        # Expand description
        catalog_type = self.record.get("catalog_type", "")
        name = self.record.get("name", "")
        owner = self.record.get("owner", {})
        owner_name = owner.get("name", "") if isinstance(owner, dict) else ""
        
        parts = [description] if description else []
        if catalog_type and catalog_type not in description:
            parts.append(f"This {catalog_type.lower()}")
        if owner_name and owner_name not in description:
            parts.append(f"managed by {owner_name}")
        if name and name not in description:
            parts.append(f"provides access to datasets and resources")
        
        if len(parts) > 1 or (parts and len(parts[0]) < 40):
            new_description = ". ".join(parts) + "."
            if len(new_description) >= 40:
                self.record["description"] = new_description
                self.changes.append("Expanded description")
                return True
        
        return False
    
    def fix_missing_tags(self, issue: Dict[str, Any]) -> bool:
        """Fix MISSING_TAGS"""
        if self.record.get("tags"):
            return False  # Already has tags
        
        tags = set()
        catalog_type = self.record.get("catalog_type", "").lower()
        description = self.record.get("description", "").lower()
        software = self.record.get("software", {})
        software_name = software.get("name", "").lower() if isinstance(software, dict) else ""
        owner = self.record.get("owner", {})
        owner_type = owner.get("type", "").lower() if isinstance(owner, dict) else ""
        
        if "geoportal" in catalog_type or "geo" in catalog_type:
            tags.add("geospatial")
            tags.add("GIS")
        if "scientific" in catalog_type or "research" in catalog_type:
            tags.add("scientific")
            tags.add("research")
        if "open data" in catalog_type or "opendata" in catalog_type:
            tags.add("open data")
        if "indicators" in catalog_type:
            tags.add("statistics")
            tags.add("indicators")
        if "microdata" in catalog_type:
            tags.add("microdata")
            tags.add("statistics")
        
        if "government" in owner_type:
            tags.add("government")
        if "academy" in owner_type or "university" in owner_type:
            tags.add("academic")
        
        if "arcgis" in software_name:
            tags.add("ArcGIS")
        if "ckan" in software_name:
            tags.add("CKAN")
        if "dataverse" in software_name:
            tags.add("Dataverse")
        
        if tags:
            self.record["tags"] = sorted(list(tags))
            self.changes.append(f"Added tags: {sorted(list(tags))}")
            return True
        
        return False
    
    def fix_missing_topics(self, issue: Dict[str, Any]) -> bool:
        """Fix MISSING_TOPICS"""
        # Check if topics exists and is not empty
        existing_topics = self.record.get("topics")
        if existing_topics and isinstance(existing_topics, list) and len(existing_topics) > 0:
            return False  # Already has topics
        
        topics = []
        catalog_type = self.record.get("catalog_type", "").lower()
        description = self.record.get("description", "").lower()
        
        # Map catalog types to topics
        if "geoportal" in catalog_type or "geo" in catalog_type:
            topics.extend(["Geospatial", "Geography", "Maps"])
        if "scientific" in catalog_type or "research" in catalog_type:
            topics.extend(["Research", "Science", "Academic"])
        if "open data" in catalog_type or "opendata" in catalog_type:
            topics.extend(["Open Data", "Government Data"])
        if "indicators" in catalog_type:
            topics.extend(["Statistics", "Indicators", "Metrics"])
        if "microdata" in catalog_type:
            topics.extend(["Microdata", "Surveys", "Statistics"])
        if "marketplace" in catalog_type:
            topics.extend(["Data Marketplace", "Commercial Data", "Third-party Data"])
        if "ml" in catalog_type or "machine learning" in catalog_type:
            topics.extend(["Machine Learning", "AI", "Data Science"])
        if "search" in catalog_type:
            topics.extend(["Data Search", "Discovery"])
        if "api" in catalog_type:
            topics.extend(["API", "Web Services"])
        if "metadata" in catalog_type:
            topics.extend(["Metadata", "Data Catalog"])
        
        if topics:
            self.record["topics"] = topics
            self.changes.append(f"Added topics: {topics}")
            return True
        
        return False
    
    def fix_missing_endpoints(self, issue: Dict[str, Any]) -> bool:
        """Fix MISSING_ENDPOINTS"""
        if self.record.get("endpoints"):
            return False  # Already has endpoints
        
        endpoints = []
        portal_link = self.record.get("link", "")
        software = self.record.get("software", {})
        software_id = software.get("id") if isinstance(software, dict) else None
        
        if portal_link:
            try:
                parsed = urlparse(portal_link)
                base_url = f"{parsed.scheme}://{parsed.netloc}"
                
                # Add common API endpoints based on software
                if software_id == "ckan":
                    endpoints.append({"url": f"{base_url}/api/3/action/package_list", "type": "REST API"})
                elif software_id == "dataverse":
                    endpoints.append({"url": f"{base_url}/api/v1", "type": "REST API"})
                elif software_id == "geonetwork":
                    endpoints.append({"url": f"{base_url}/geonetwork/srv/eng/csw", "type": "CSW"})
                elif software_id == "arcgishub" or software_id == "arcgisserver":
                    endpoints.append({"url": f"{base_url}/arcgis/rest/services", "type": "REST API"})
                
                if endpoints:
                    self.record["endpoints"] = endpoints
                    self.changes.append(f"Added endpoints based on software type")
                    return True
            except Exception:
                pass
        
        return False
    
    def fix_missing_api_status(self, issue: Dict[str, Any]) -> bool:
        """Fix MISSING_API_STATUS"""
        if self.record.get("api_status"):
            return False  # Already has api_status
        
        # Set to 'uncertain' as default
        self.record["api_status"] = "uncertain"
        self.changes.append("Set api_status to 'uncertain'")
        return True
    
    def fix_inconsistent_license(self, issue: Dict[str, Any]) -> bool:
        """Fix INCONSISTENT_LICENSE"""
        rights = self.record.get("rights", {})
        license_name = rights.get("license_name", "")
        license_url = rights.get("license_url")
        
        if license_name and not license_url:
            # Try to map common license names to URLs
            license_urls = {
                "Open Government Licence v3.0": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
                "Open Government Licence": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
                "CC0": "https://creativecommons.org/publicdomain/zero/1.0/",
                "CC BY": "https://creativecommons.org/licenses/by/4.0/",
                "CC BY-SA": "https://creativecommons.org/licenses/by-sa/4.0/",
                "CC BY-NC": "https://creativecommons.org/licenses/by-nc/4.0/",
                "Public Domain": "https://creativecommons.org/publicdomain/zero/1.0/",
            }
            
            for name, url in license_urls.items():
                if name.lower() in license_name.lower():
                    rights["license_url"] = url
                    self.changes.append(f"Added license_url: {url}")
                    return True
        
        return False
    
    def fix_tag_hygiene(self, issue: Dict[str, Any]) -> bool:
        """Fix TAG_HYGIENE - remove tags that are too short"""
        tags = self.record.get("tags", [])
        if not tags:
            return False
        
        field = issue.get("field", "")
        # Extract tag index from field like "tags[3]"
        match = re.match(r'tags\[(\d+)\]', field)
        if match:
            tag_index = int(match.group(1))
            if 0 <= tag_index < len(tags):
                tag_value = tags[tag_index]
                if isinstance(tag_value, str) and len(tag_value) < 3:
                    tags.pop(tag_index)
                    self.record["tags"] = tags
                    self.changes.append(f"Removed short tag '{tag_value}'")
                    return True
        
        return False
    
    def fix_coverage_normalization(self, issue: Dict[str, Any]) -> bool:
        """Fix COVERAGE_NORMALIZATION - add macroregion if missing"""
        coverage = self.record.get("coverage", [])
        if not coverage:
            return False
        
        # This is a complex fix that would require geographic data
        # For now, we'll skip it as it requires external data
        return False
    
    def fix_missing_required_field(self, issue: Dict[str, Any]) -> bool:
        """Fix MISSING_REQUIRED_FIELD"""
        field = issue.get("field", "")
        
        if field == "owner.name":
            return self.fix_missing_owner_name(issue)
        elif field == "owner.type":
            return self.fix_missing_owner_type(issue)
        # Add other required fields as needed
        
        return False
    
    def fix_software_id_unknown(self, issue: Dict[str, Any]) -> bool:
        """Fix SOFTWARE_ID_UNKNOWN - skip for now as it requires verification"""
        # This requires manual verification, so we skip it
        return False
    
    def fix_api_status_mismatch(self, issue: Dict[str, Any]) -> bool:
        """Fix API_STATUS_MISMATCH - set api_status to 'active' if endpoints exist"""
        endpoints = self.record.get("endpoints", [])
        api = self.record.get("api", False)
        
        # If endpoints exist or api=True, set api_status to 'active'
        if endpoints or api:
            current_status = self.record.get("api_status")
            if current_status != "active":
                self.record["api_status"] = "active"
                self.changes.append(f"Set api_status to 'active' (endpoints or api present)")
                return True
        
        return False
    
    def fix_duplicate_tags(self, issue: Dict[str, Any]) -> bool:
        """Fix DUPLICATE_TAGS - remove duplicate tags"""
        tags = self.record.get("tags", [])
        if not tags:
            return False
        
        # Remove duplicates while preserving order
        seen = set()
        unique_tags = []
        for tag in tags:
            tag_lower = tag.lower() if isinstance(tag, str) else str(tag).lower()
            if tag_lower not in seen:
                seen.add(tag_lower)
                unique_tags.append(tag)
        
        if len(unique_tags) < len(tags):
            self.record["tags"] = unique_tags
            self.changes.append(f"Removed {len(tags) - len(unique_tags)} duplicate tags")
            return True
        
        return False
    
    def fix_issue(self, issue: Dict[str, Any]) -> bool:
        """Fix a single issue based on its type."""
        issue_type = issue.get("issue_type", "")
        
        fixers = {
            "CATALOG_SOFTWARE_MISMATCH": self.fix_catalog_software_mismatch,
            "MISSING_OWNER_NAME": self.fix_missing_owner_name,
            "MISSING_OWNER_TYPE": self.fix_missing_owner_type,
            "MISSING_OWNER_LINK": self.fix_missing_owner_link,
            "MISSING_LANGS": self.fix_missing_langs,
            "MISSING_DESCRIPTION": self.fix_missing_description,
            "SHORT_DESCRIPTION": self.fix_short_description,
            "MISSING_TAGS": self.fix_missing_tags,
            "MISSING_TOPICS": self.fix_missing_topics,
            "MISSING_ENDPOINTS": self.fix_missing_endpoints,
            "MISSING_API_STATUS": self.fix_missing_api_status,
            "API_STATUS_MISMATCH": self.fix_api_status_mismatch,
            "INCONSISTENT_LICENSE": self.fix_inconsistent_license,
            "TAG_HYGIENE": self.fix_tag_hygiene,
            "DUPLICATE_TAGS": self.fix_duplicate_tags,
            "COVERAGE_NORMALIZATION": self.fix_coverage_normalization,
            "MISSING_REQUIRED_FIELD": self.fix_missing_required_field,
            "SOFTWARE_ID_UNKNOWN": self.fix_software_id_unknown,
        }
        
        fixer = fixers.get(issue_type)
        if fixer:
            return fixer(issue)
        return False


def read_jsonl_issues(file_path: Path) -> List[Dict[str, Any]]:
    """Read and parse JSONL issues file."""
    records = []
    if not file_path.exists():
        print(f"Error: File not found: {file_path}")
        return records
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                record = json.loads(line)
                records.append(record)
            except json.JSONDecodeError as e:
                print(f"Warning: Failed to parse JSON at line {line_num}: {e}")
                continue
    
    return records


def fix_record(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Fix all issues for a single record."""
    file_path_str = record.get("file_path", "")
    full_path = ENTITIES_DIR / file_path_str
    
    if not full_path.exists():
        return False, [f"File not found: {full_path}"]
    
    try:
        # Load YAML file
        with open(full_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        if not data:
            return False, ["Empty YAML file"]
        
        # Create fixer and fix all issues
        fixer = IssueFixer(data, full_path)
        issues = record.get("issues", [])
        
        fixed_count = 0
        for issue in issues:
            if fixer.fix_issue(issue):
                fixed_count += 1
        
        # Save if changes were made
        if fixer.changes:
            with open(full_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            return True, fixer.changes
        
        return False, ["No changes made"]
    
    except Exception as e:
        return False, [f"Error: {str(e)}"]


def main():
    """Main entry point."""
    print("Reading issues from primary_priority.jsonl...")
    records = read_jsonl_issues(ISSUES_FILE)
    
    if not records:
        print("No records with issues found.")
        return
    
    print(f"Found {len(records)} records with issues to process.\n")
    
    fixed_count = 0
    skipped_count = 0
    error_count = 0
    
    for i, record in enumerate(records, 1):
        record_id = record.get("record_id", "unknown")
        file_path = record.get("file_path", "")
        issues_count = len(record.get("issues", []))
        
        print(f"[{i}/{len(records)}] Processing {record_id} ({file_path})")
        print(f"  Issues: {issues_count}")
        
        success, messages = fix_record(record)
        
        if success:
            fixed_count += 1
            print(f"  ✓ Fixed: {len(messages)} changes")
            for msg in messages:
                print(f"    - {msg}")
        elif messages and messages[0].startswith("Error"):
            error_count += 1
            print(f"  ✗ Error: {messages[0]}")
        else:
            skipped_count += 1
            print(f"  ○ Skipped: {messages[0] if messages else 'No changes needed'}")
        
        print()
    
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total records: {len(records)}")
    print(f"Fixed: {fixed_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Errors: {error_count}")


if __name__ == "__main__":
    main()
