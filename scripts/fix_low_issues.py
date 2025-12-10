#!/usr/bin/env python3
"""
Script to fix LOW priority issues:
- MISSING_OWNER_LINK
- MISSING_TAGS
- MISSING_TOPICS
"""
import re
import yaml
from pathlib import Path
from urllib.parse import urlparse

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
    content_types = record.get("content_types", [])
    
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

def fix_owner_link(record, file_path):
    """Fix MISSING_OWNER_LINK"""
    owner = record.get("owner", {})
    owner_link = owner.get("link")
    
    if not owner_link or owner_link == "None" or owner_link == "null":
        inferred_link = infer_owner_link(record)
        if inferred_link:
            owner["link"] = inferred_link
            return True, f"Inferred owner link: '{inferred_link}'"
    
    return False, None

def fix_tags(record, file_path):
    """Fix MISSING_TAGS"""
    tags = record.get("tags", [])
    
    if not tags or (isinstance(tags, list) and len(tags) == 0):
        inferred_tags = extract_tags(record)
        if inferred_tags:
            record["tags"] = inferred_tags
            return True, f"Added {len(inferred_tags)} tags: {', '.join(inferred_tags[:5])}..."
    
    return False, None

def fix_topics(record, file_path):
    """Fix MISSING_TOPICS"""
    topics = record.get("topics", [])
    
    if not topics or (isinstance(topics, list) and len(topics) == 0):
        inferred_topics = infer_topics(record)
        if inferred_topics:
            record["topics"] = inferred_topics
            topic_names = [f"{t.get('id')} ({t.get('type')})" for t in inferred_topics[:3]]
            return True, f"Added {len(inferred_topics)} topics: {', '.join(topic_names)}..."
    
    return False, None

def parse_low_file(low_file_path):
    """Parse LOW.txt and extract file paths and issues"""
    issues = []
    with open(low_file_path, 'r', encoding='utf-8') as f:
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

def fix_yaml_file(file_path, issue_type):
    """Fix issues in a YAML file"""
    full_path = Path("data/entities") / file_path
    
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
            fixed, message = fix_owner_link(data, file_path)
        elif issue_type == "MISSING_TAGS":
            fixed, message = fix_tags(data, file_path)
        elif issue_type == "MISSING_TOPICS":
            fixed, message = fix_topics(data, file_path)
        
        if fixed and message:
            print(f"{issue_type} in {file_path}: {message}")
            # Write back
            with open(full_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            return True
        
        return False
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    low_file = Path("dataquality/priorities/LOW.txt")
    
    if not low_file.exists():
        print(f"Error: {low_file} not found")
        return
    
    print("Parsing LOW.txt...")
    issues = parse_low_file(low_file)
    print(f"Found {len(issues)} issues to fix")
    
    fixed_count = 0
    skipped_count = 0
    for file_path, issue_type, field in issues:
        result = fix_yaml_file(file_path, issue_type)
        if result:
            fixed_count += 1
        else:
            skipped_count += 1
    
    print(f"\nFixed {fixed_count} out of {len(issues)} files")
    if skipped_count > 0:
        print(f"Skipped {skipped_count} files (may have been already fixed or couldn't be inferred)")

if __name__ == "__main__":
    main()
