#!/usr/bin/env python3
"""
Script to fix issues specifically for Russia (RU) based on dataquality/countries/RU.txt
"""
import re
import yaml
from pathlib import Path
from urllib.parse import urlparse

# Import fix functions from existing scripts
import sys
sys.path.insert(0, str(Path(__file__).parent))

# Base directories
BASE_DIR = Path(__file__).parent.parent
ENTITIES_DIR = BASE_DIR / "data" / "entities"

def infer_owner_link(record):
    """Try to infer owner link from portal link or owner name"""
    owner = record.get("owner", {})
    owner_name = owner.get("name", "")
    portal_link = record.get("link", "")
    
    if not portal_link:
        return None
    
    try:
        parsed = urlparse(portal_link)
        domain = parsed.netloc or portal_link
        
        # Remove common subdomains and paths
        domain = domain.lower()
        domain = re.sub(r'^(data|gis|geo|map|portal|opendata|www)\.', '', domain)
        domain = re.sub(r'\.(hub|opendata|arcgis|geoserver|geonetwork|ckan|dataverse)', '', domain)
        
        # Extract base domain
        parts = domain.split('.')
        if len(parts) >= 2:
            # Take last two parts (e.g., "example.com")
            base_domain = '.'.join(parts[-2:])
            # Try to construct owner website
            if base_domain and base_domain != domain:
                # Check if it's a valid-looking domain
                if re.match(r'^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*\.[a-zA-Z]{2,}$', base_domain):
                    return f"https://{base_domain}"
        
        # Fallback: use portal domain with https
        if domain and not domain.startswith('http'):
            return f"https://{domain}"
    except Exception:
        pass
    
    return None

def extract_tags(record):
    """Extract tags from description, catalog_type, and other metadata"""
    tags = set()
    
    catalog_type = record.get("catalog_type", "").lower()
    description = record.get("description", "").lower()
    name = record.get("name", "").lower()
    software = record.get("software", {})
    software_name = software.get("name", "").lower() if isinstance(software, dict) else ""
    owner = record.get("owner", {})
    owner_type = owner.get("type", "").lower() if isinstance(owner, dict) else ""
    
    # Tags based on catalog type
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
    
    # Tags based on owner type
    if "government" in owner_type:
        tags.add("government")
    if "academy" in owner_type or "university" in owner_type:
        tags.add("academic")
    if "business" in owner_type:
        tags.add("commercial")
    
    # Tags based on software
    if "arcgis" in software_name:
        tags.add("ArcGIS")
    if "geoserver" in software_name:
        tags.add("GeoServer")
    if "ckan" in software_name:
        tags.add("CKAN")
    if "dataverse" in software_name:
        tags.add("Dataverse")
    if "geonode" in software_name:
        tags.add("GeoNode")
    
    # Extract keywords from description
    keywords = [
        "environment", "climate", "weather", "transport", "traffic", "health",
        "education", "economy", "business", "agriculture", "fisheries",
        "forestry", "water", "energy", "infrastructure", "planning",
        "boundaries", "elevation", "imagery", "location", "society",
        "population", "demographics", "biodiversity", "ocean", "marine"
    ]
    
    text_to_search = f"{description} {name}"
    for keyword in keywords:
        if keyword in text_to_search:
            tags.add(keyword)
    
    # Extract country/region from coverage
    coverage = record.get("coverage", [])
    if coverage and isinstance(coverage, list) and len(coverage) > 0:
        location = coverage[0].get("location", {})
        country = location.get("country", {})
        country_name = country.get("name", "")
        if country_name and country_name != "Unknown":
            # Add country name as tag (lowercase, simplified)
            country_tag = country_name.lower().replace(" ", "-")
            tags.add(country_tag)
    
    return sorted(list(tags)) if tags else None

def infer_topics(record):
    """Infer topics based on catalog_type and description"""
    topics = []
    catalog_type = record.get("catalog_type", "")
    description = record.get("description", "").lower()
    
    # Common topic mappings
    # EU Data Theme topics
    eudata_topics = {
        "geoportal": [{"id": "GOVE", "name": "Government and public sector", "type": "eudatatheme"}],
        "scientific data repository": [{"id": "TECH", "name": "Science and technology", "type": "eudatatheme"}],
        "microdata catalog": [{"id": "SOCI", "name": "Population and society", "type": "eudatatheme"}],
        "indicators catalog": [{"id": "SOCI", "name": "Population and society", "type": "eudatatheme"}],
        "open data portal": [{"id": "GOVE", "name": "Government and public sector", "type": "eudatatheme"}],
    }
    
    # ISO19115 topics
    iso_topics = {
        "geoportal": [
            {"id": "Location", "name": "Location", "type": "iso19115"},
            {"id": "Boundaries", "name": "Boundaries", "type": "iso19115"},
        ],
        "scientific data repository": [
            {"id": "Science and technology", "name": "Science and technology", "type": "iso19115"},
        ],
        "microdata catalog": [
            {"id": "Society", "name": "Society", "type": "iso19115"},
        ],
    }
    
    # Add topics based on catalog type
    catalog_lower = catalog_type.lower()
    if "geoportal" in catalog_lower:
        topics.extend(eudata_topics.get("geoportal", []))
        topics.extend(iso_topics.get("geoportal", []))
    elif "scientific" in catalog_lower:
        topics.extend(eudata_topics.get("scientific data repository", []))
        topics.extend(iso_topics.get("scientific data repository", []))
    elif "microdata" in catalog_lower:
        topics.extend(eudata_topics.get("microdata catalog", []))
        topics.extend(iso_topics.get("microdata catalog", []))
    elif "indicators" in catalog_lower:
        topics.extend(eudata_topics.get("indicators catalog", []))
    elif "open data" in catalog_lower or "opendata" in catalog_lower:
        topics.extend(eudata_topics.get("open data portal", []))
    
    # Add topics based on description keywords
    if any(word in description for word in ["environment", "climate", "weather", "nature"]):
        topics.append({"id": "ENVI", "name": "Environment", "type": "eudatatheme"})
        topics.append({"id": "Environment", "name": "Environment", "type": "iso19115"})
    
    if any(word in description for word in ["transport", "traffic", "road", "transit"]):
        topics.append({"id": "TRAN", "name": "Transport", "type": "eudatatheme"})
        topics.append({"id": "Transportation", "name": "Transportation", "type": "iso19115"})
    
    if any(word in description for word in ["health", "medical", "hospital"]):
        topics.append({"id": "HEAL", "name": "Health", "type": "eudatatheme"})
    
    if any(word in description for word in ["education", "school", "university"]):
        topics.append({"id": "EDUC", "name": "Education, culture and sport", "type": "eudatatheme"})
    
    if any(word in description for word in ["agriculture", "farming", "crop"]):
        topics.append({"id": "AGRI", "name": "Agriculture, fisheries, forestry and food", "type": "eudatatheme"})
    
    if any(word in description for word in ["imagery", "satellite", "aerial", "remote sensing"]):
        topics.append({"id": "Imagery / Base Maps / Earth Cover", "name": "Imagery / Base Maps / Earth Cover", "type": "iso19115"})
    
    if any(word in description for word in ["elevation", "terrain", "dem"]):
        topics.append({"id": "Elevation", "name": "Elevation", "type": "iso19115"})
    
    if any(word in description for word in ["planning", "cadastre", "land use"]):
        topics.append({"id": "Planning / Cadastre", "name": "Planning / Cadastre", "type": "iso19115"})
    
    # Remove duplicates
    seen = set()
    unique_topics = []
    for topic in topics:
        topic_key = (topic.get("id"), topic.get("type"))
        if topic_key not in seen:
            seen.add(topic_key)
            unique_topics.append(topic)
    
    return unique_topics if unique_topics else None

def expand_description(record, file_path):
    """Fix SHORT_DESCRIPTION by expanding descriptions that are too short"""
    description = record.get("description", "")
    
    if not description or not isinstance(description, str):
        return False, None
    
    description = description.strip()
    
    # Check if description is too short (less than 40 characters)
    if len(description) < 40:
        # Try to expand it
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
        if description and description not in ["None", "Not specified", "No description available from source.", "No description provided"]:
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

def parse_ru_file(ru_file_path):
    """Parse RU.txt and extract file paths and issues"""
    issues = []
    with open(ru_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Pattern to match file entries in RU.txt format
    # Format: File: RU/.../file.yaml\nRecord ID: ...\nIssue: ...\nField: ...
    pattern = r'File: ([^\n]+)\nRecord ID: [^\n]+\nIssue: ([^\n]+)\nField: ([^\n]+)'
    
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
        
        if issue_type == "MISSING_OWNER_LINK":
            owner = data.get("owner", {})
            owner_link = owner.get("link")
            
            if not owner_link or owner_link == "None" or owner_link == "null":
                inferred_link = infer_owner_link(data)
                if inferred_link:
                    owner["link"] = inferred_link
                    fixed = True
                    message = f"Inferred owner link: '{inferred_link}'"
        
        elif issue_type == "MISSING_TAGS":
            tags = data.get("tags", [])
            
            if not tags or (isinstance(tags, list) and len(tags) == 0):
                inferred_tags = extract_tags(data)
                if inferred_tags:
                    data["tags"] = inferred_tags
                    fixed = True
                    message = f"Added {len(inferred_tags)} tags: {', '.join(inferred_tags[:5])}..."
        
        elif issue_type == "MISSING_TOPICS":
            topics = data.get("topics", [])
            
            if not topics or (isinstance(topics, list) and len(topics) == 0):
                inferred_topics = infer_topics(data)
                if inferred_topics:
                    data["topics"] = inferred_topics
                    topic_names = [f"{t.get('id')} ({t.get('type')})" for t in inferred_topics[:3]]
                    fixed = True
                    message = f"Added {len(inferred_topics)} topics: {', '.join(topic_names)}..."
        
        elif issue_type == "SHORT_DESCRIPTION":
            fixed, message = expand_description(data, file_path)
        
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
    ru_file = BASE_DIR / "dataquality" / "countries" / "RU.txt"
    
    if not ru_file.exists():
        print(f"Error: {ru_file} not found")
        return
    
    print("Parsing RU.txt...")
    issues = parse_ru_file(ru_file)
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
