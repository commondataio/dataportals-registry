#!/usr/bin/env python3
"""
Script to fix INVALID_OWNER_URL issues from CRITICAL.txt
"""
import re
import yaml
from pathlib import Path
from urllib.parse import urlparse

def is_valid_url(url):
    """Check if URL is valid (has scheme and netloc)"""
    if not url or not isinstance(url, str):
        return False
    try:
        parsed = urlparse(url)
        return bool(parsed.scheme and parsed.netloc)
    except Exception:
        return False

def fix_owner_link(value):
    """
    Fix invalid owner.link values
    Returns fixed URL or None if should be removed
    """
    if not value or not isinstance(value, str):
        return None
    
    value = value.strip()
    
    # List of invalid placeholder values that should be removed
    invalid_placeholders = [
        "not specified",
        "(not provided)",
        "not available",
        "not provided",
        "not found",
        "not detected",
        "not extracted",
        "unknown",
        "n/a",
        "-",
        "owner website",
        "[owner_website_here]",
        "none",
        "academy",  # This seems to be a type, not a URL
    ]
    
    # Check if it's an invalid placeholder
    if value.lower() in invalid_placeholders:
        return None
    
    # Check for placeholder-like text
    if any(phrase in value.lower() for phrase in [
        "not specified",
        "not provided",
        "not available",
        "not found",
        "not detected",
        "not extracted",
        "requires extraction",
        "no owner",
        "not available from",
        "not provided in",
        "not specified in",
        "not found in",
        "not extracted from",
        "not available in",
        "owner information not",
        "not explicitly stated",
    ]):
        return None
    
    # Fix malformed URLs with triple slashes
    if value.startswith("https:///"):
        value = value.replace("https:///", "https://")
    elif value.startswith("http:///"):
        value = value.replace("http:///", "http://")
    
    # Remove trailing brackets and numbers (like "[5]")
    value = re.sub(r'\[\d+\]$', '', value)
    
    # If it already has a scheme, validate it
    if value.startswith(("http://", "https://")):
        if is_valid_url(value):
            return value
        # Try to fix common issues
        # Remove triple slashes
        value = re.sub(r'https?:///+', lambda m: m.group(0)[:5] + '/', value)
        if is_valid_url(value):
            return value
    
    # If it's a domain name without protocol, add https://
    # Check if it looks like a domain name
    if re.match(r'^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*\.[a-zA-Z]{2,}(/.*)?$', value):
        # Remove any trailing path that might be invalid
        domain_part = value.split('/')[0]
        fixed = f"https://{domain_part}"
        if is_valid_url(fixed):
            return fixed
    
    # If it contains a domain-like pattern, try to extract and fix
    domain_match = re.search(r'([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}', value)
    if domain_match:
        domain = domain_match.group(0)
        fixed = f"https://{domain}"
        if is_valid_url(fixed):
            return fixed
    
    # If we can't fix it, return None to remove it
    return None

def parse_critical_file(critical_file_path):
    """Parse CRITICAL.txt and extract file paths and current values"""
    issues = []
    with open(critical_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Pattern to match file entries
    pattern = r'File: ([^\n]+)\nRecord ID: [^\n]+\nCountry: [^\n]+\nIssue: INVALID_OWNER_URL\nField: owner\.link\nCurrent Value: ([^\n]+)'
    
    matches = re.finditer(pattern, content)
    for match in matches:
        file_path = match.group(1)
        current_value = match.group(2)
        issues.append((file_path, current_value))
    
    return issues

def fix_yaml_file(file_path, current_value):
    """Fix owner.link in a YAML file"""
    full_path = Path("data/entities") / file_path
    
    if not full_path.exists():
        print(f"Warning: File not found: {full_path}")
        return False
    
    try:
        with open(full_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        if not data or 'owner' not in data:
            print(f"Warning: No owner field in {file_path}")
            return False
        
        owner = data.get('owner', {})
        old_value = owner.get('link')
        
        if old_value != current_value:
            print(f"Warning: Value mismatch in {file_path}: expected '{current_value}', got '{old_value}'")
        
        fixed_value = fix_owner_link(old_value)
        
        if fixed_value is None:
            # Remove the link field
            if 'link' in owner:
                del owner['link']
                print(f"Removed invalid owner.link from {file_path}")
        else:
            owner['link'] = fixed_value
            if fixed_value != old_value:
                print(f"Fixed owner.link in {file_path}: '{old_value}' -> '{fixed_value}'")
        
        # Write back
        with open(full_path, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        
        return True
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    critical_file = Path("dataquality/priorities/CRITICAL.txt")
    
    if not critical_file.exists():
        print(f"Error: {critical_file} not found")
        return
    
    print("Parsing CRITICAL.txt...")
    issues = parse_critical_file(critical_file)
    print(f"Found {len(issues)} issues to fix")
    
    fixed_count = 0
    for file_path, current_value in issues:
        if fix_yaml_file(file_path, current_value):
            fixed_count += 1
    
    print(f"\nFixed {fixed_count} out of {len(issues)} files")

if __name__ == "__main__":
    main()
