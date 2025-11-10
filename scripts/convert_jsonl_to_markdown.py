#!/usr/bin/env python3
"""
Convert compressed JSON lines file to individual Markdown files.

This script reads a zstandard-compressed JSON lines file containing data catalog
entries and converts each entry into a separate Markdown file. The files are
stored in a 'docs' directory with filenames based on the UID identifier from
each record.

Usage:
    python3 convert_jsonl_to_markdown.py [input_file] [output_dir]

Arguments:
    input_file: Path to the .jsonl.zst file (default: full.jsonl.zst)
    output_dir: Directory to store Markdown files (default: docs)
"""

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import zstandard as zstd
except ImportError:
    print("Error: zstandard package not found. Install it with: pip install zstandard")
    sys.exit(1)


def format_list(items: List[Any], prefix: str = "- ") -> str:
    """Format a list of items as Markdown list."""
    if not items:
        return "None"
    return "\n".join(f"{prefix}{item}" for item in items)


def format_dict_list(items: List[Dict], indent: int = 0) -> str:
    """Format a list of dictionaries as Markdown."""
    if not items:
        return "None"
    
    result = []
    indent_str = "  " * indent
    for item in items:
        for key, value in item.items():
            if isinstance(value, dict):
                result.append(f"{indent_str}- **{key}**:")
                for sub_key, sub_value in value.items():
                    result.append(f"{indent_str}  - {sub_key}: {sub_value}")
            elif isinstance(value, list):
                result.append(f"{indent_str}- **{key}**: {', '.join(str(v) for v in value)}")
            else:
                result.append(f"{indent_str}- **{key}**: {value}")
    return "\n".join(result)


def format_coverage(coverage: List[Dict]) -> str:
    """Format coverage information."""
    if not coverage:
        return "None"
    
    result = []
    for item in coverage:
        location = item.get("location", {})
        country = location.get("country", {})
        macroregion = location.get("macroregion", {})
        
        parts = []
        if country:
            parts.append(f"Country: {country.get('name', 'N/A')} ({country.get('id', 'N/A')})")
        if macroregion:
            parts.append(f"Macroregion: {macroregion.get('name', 'N/A')}")
        if location.get("level"):
            parts.append(f"Level: {location['level']}")
        
        if parts:
            result.append("- " + ", ".join(parts))
    
    return "\n".join(result) if result else "None"


def format_endpoints(endpoints: List[Dict]) -> str:
    """Format API endpoints."""
    if not endpoints:
        return "None"
    
    result = []
    for endpoint in endpoints:
        ep_type = endpoint.get("type", "N/A")
        url = endpoint.get("url", "N/A")
        version = endpoint.get("version", "")
        
        if version:
            result.append(f"- **{ep_type}** (v{version}): {url}")
        else:
            result.append(f"- **{ep_type}**: {url}")
    
    return "\n".join(result)


def format_owner(owner: Dict) -> str:
    """Format owner information."""
    if not owner:
        return "None"
    
    result = []
    if owner.get("name"):
        result.append(f"**Name**: {owner['name']}")
    if owner.get("type"):
        result.append(f"**Type**: {owner['type']}")
    if owner.get("link"):
        result.append(f"**Link**: [{owner['link']}]({owner['link']})")
    
    location = owner.get("location", {})
    country = location.get("country", {})
    if country:
        result.append(f"**Country**: {country.get('name', 'N/A')} ({country.get('id', 'N/A')})")
    
    return "\n\n".join(result) if result else "None"


def format_rights(rights: Dict) -> str:
    """Format rights and licensing information."""
    if not rights:
        return "None"
    
    result = []
    if rights.get("rights_type"):
        result.append(f"**Rights Type**: {rights['rights_type']}")
    if rights.get("license_name"):
        result.append(f"**License**: {rights['license_name']}")
    if rights.get("license_url"):
        result.append(f"**License URL**: [{rights['license_url']}]({rights['license_url']})")
    if rights.get("privacy_policy_url"):
        result.append(f"**Privacy Policy**: [{rights['privacy_policy_url']}]({rights['privacy_policy_url']})")
    if rights.get("tos_url"):
        result.append(f"**Terms of Service**: [{rights['tos_url']}]({rights['tos_url']})")
    
    return "\n\n".join(result) if result else "None"


def convert_record_to_markdown(record: Dict) -> str:
    """Convert a single JSON record to Markdown format."""
    
    # Header
    name = record.get("name", "Untitled Catalog")
    uid = record.get("uid", "unknown")
    
    md = f"# {name}\n\n"
    
    # Basic Information
    md += "## Basic Information\n\n"
    md += f"**UID**: {uid}\n\n"
    md += f"**ID**: {record.get('id', 'N/A')}\n\n"
    md += f"**Status**: {record.get('status', 'N/A')}\n\n"
    
    if record.get("link"):
        md += f"**Link**: [{record['link']}]({record['link']})\n\n"
    
    md += f"**Catalog Type**: {record.get('catalog_type', 'N/A')}\n\n"
    md += f"**Export Standard**: {record.get('export_standard', 'N/A')}\n\n"
    
    # Software
    software = record.get("software", {})
    if software:
        md += f"**Software**: {software.get('name', 'N/A')}\n\n"
    
    # API Information
    md += f"**Has API**: {'Yes' if record.get('api') else 'No'}\n\n"
    if record.get("api_status"):
        md += f"**API Status**: {record.get('api_status')}\n\n"
    
    # Access Mode
    access_mode = record.get("access_mode", [])
    if access_mode:
        md += f"**Access Mode**: {', '.join(access_mode)}\n\n"
    
    # Content Types
    content_types = record.get("content_types", [])
    if content_types:
        md += f"**Content Types**: {', '.join(content_types)}\n\n"
    
    # Languages
    langs = record.get("langs", [])
    if langs:
        lang_names = [lang.get("name", lang.get("id", "")) for lang in langs]
        md += f"**Languages**: {', '.join(lang_names)}\n\n"
    
    # Tags
    tags = record.get("tags", [])
    if tags:
        md += f"**Tags**: {', '.join(f'`{tag}`' for tag in tags)}\n\n"
    
    # Topics
    topics = record.get("topics", [])
    if topics:
        md += f"**Topics**: {', '.join([topic['name'] for topic in topics])}\n\n"
    
    # Owner
    md += "## Owner\n\n"
    md += format_owner(record.get("owner", {})) + "\n\n"
    
    # Coverage
    md += "## Coverage\n\n"
    md += format_coverage(record.get("coverage", [])) + "\n\n"
    
    # Endpoints
    endpoints = record.get("endpoints", [])
    if endpoints:
        md += "## API Endpoints\n\n"
        md += format_endpoints(endpoints) + "\n\n"
    
    # Rights and Licensing
    md += "## Rights and Licensing\n\n"
    md += format_rights(record.get("rights", {})) + "\n\n"
    
    # Properties
    properties = record.get("properties", {})
    if properties:
        md += "## Properties\n\n"
        for key, value in properties.items():
            md += f"**{key}**: {value}\n\n"
    
    return md


def convert_jsonl_to_markdown(input_file: str, output_dir: str) -> None:
    """
    Convert a compressed JSON lines file to individual Markdown files.
    
    Args:
        input_file: Path to the .jsonl.zst file
        output_dir: Directory to store the Markdown files
    """
    
    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    print(f"Reading from: {input_file}")
    print(f"Writing to: {output_dir}/")
    
    # Open and decompress the file
    with open(input_file, 'rb') as f:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(f) as reader:
            # Read and decode the entire content
            text_stream = reader.read().decode('utf-8')
            lines = text_stream.strip().split('\n')
            
            total = len(lines)
            print(f"\nProcessing {total} records...")
            
            success_count = 0
            error_count = 0
            
            for idx, line in enumerate(lines, 1):
                try:
                    # Parse JSON record
                    record = json.loads(line)
                    
                    # Get UID for filename
                    uid = record.get("uid", f"unknown_{idx}")
                    
                    # Convert to Markdown
                    markdown_content = convert_record_to_markdown(record)
                    
                    # Write to file
                    output_file = output_path / f"{uid}.md"
                    with open(output_file, 'w', encoding='utf-8') as md_file:
                        md_file.write(markdown_content)
                    
                    success_count += 1
                    
                    # Progress indicator
                    if idx % 100 == 0 or idx == total:
                        print(f"Progress: {idx}/{total} ({idx*100//total}%)")
                
                except KeyboardInterrupt as e:
                    error_count += 1
                    print(f"Error processing record {idx}: {e}")
                    continue
            
            print(f"\nConversion complete!")
            print(f"Successfully converted: {success_count} records")
            print(f"Errors: {error_count} records")
            print(f"Output directory: {output_path.absolute()}")


def main():
    """Main entry point."""
    # Parse command line arguments
    input_file = sys.argv[1] if len(sys.argv) > 1 else "full.jsonl.zst"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "docs"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)
    
    # Run conversion
    convert_jsonl_to_markdown(input_file, output_dir)


if __name__ == "__main__":
    main()

