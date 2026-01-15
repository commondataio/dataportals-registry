#!/usr/bin/env python
# Script to automatically update software profile metadata
# Updates version, repository_url, documentation_url, export_formats, and other fields

import logging
import os
import re
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse
import requests
from requests.exceptions import RequestException, ConnectionError
from urllib3.exceptions import InsecureRequestWarning

import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

import typer
from tqdm import tqdm

from constants import (
    SOFTWARE_REPOSITORY_URLS,
    SOFTWARE_DOCUMENTATION_URLS,
    SOFTWARE_EXPORT_FORMATS,
    SOFTWARE_SPECIFIC_EXPORT_FORMATS,
)

# Suppress urllib3 warnings
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Default timeout for HTTP requests
DEFAULT_TIMEOUT = 10

# GitHub API base URL
GITHUB_API_BASE = "https://api.github.com/repos"


def get_github_latest_release(repo_url: str) -> Optional[str]:
    """Get latest release version from GitHub API"""
    try:
        # Extract owner/repo from GitHub URL
        # e.g., https://github.com/ckan/ckan -> ckan/ckan
        match = re.search(r"github\.com/([^/]+/[^/]+)", repo_url)
        if not match:
            return None
        
        repo_path = match.group(1).rstrip('/')
        api_url = f"{GITHUB_API_BASE}/{repo_path}/releases/latest"
        
        response = requests.get(api_url, timeout=DEFAULT_TIMEOUT)
        if response.status_code == 200:
            data = response.json()
            tag_name = data.get("tag_name", "")
            # Remove 'v' prefix if present
            version = tag_name.lstrip("v")
            return version if version else None
    except (RequestException, ConnectionError, KeyError, ValueError) as e:
        logger.debug(f"Failed to get GitHub release for {repo_url}: {e}")
    return None


def infer_repository_url(software_id: str, website: Optional[str]) -> Optional[str]:
    """Infer repository URL from known mappings or website"""
    # Check known mappings first
    if software_id in SOFTWARE_REPOSITORY_URLS:
        return SOFTWARE_REPOSITORY_URLS[software_id]
    
    # Try to infer from website
    if website:
        try:
            parsed = urlparse(website)
            domain = parsed.netloc or parsed.path
            # Common patterns: github.com/org, gitlab.com/org, etc.
            if "github.com" in domain or "gitlab.com" in domain:
                return website.rstrip('/')
        except Exception:
            pass
    
    return None


def get_documentation_url(software_id: str, website: Optional[str]) -> Optional[str]:
    """Get documentation URL from known mappings or infer from website"""
    # Check known mappings first
    if software_id in SOFTWARE_DOCUMENTATION_URLS:
        return SOFTWARE_DOCUMENTATION_URLS[software_id]
    
    # Try common documentation patterns
    if website:
        common_docs_paths = [
            "/docs",
            "/documentation",
            "/guide",
            "/guides",
            "/wiki",
            "/manual",
        ]
        parsed = urlparse(website)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        for path in common_docs_paths:
            docs_url = f"{base_url}{path}"
            try:
                response = requests.head(docs_url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
                if response.status_code == 200:
                    return docs_url
            except Exception:
                continue
    
    return None


def infer_export_formats(software_id: str, category: str, metadata_support: Dict[str, Any]) -> List[str]:
    """Infer export formats from software-specific mappings, category, or metadata support"""
    formats = set()
    
    # Check software-specific mappings first
    if software_id in SOFTWARE_SPECIFIC_EXPORT_FORMATS:
        formats.update(SOFTWARE_SPECIFIC_EXPORT_FORMATS[software_id])
    
    # Check category-based mappings
    if category in SOFTWARE_EXPORT_FORMATS:
        formats.update(SOFTWARE_EXPORT_FORMATS[category])
    
    # Infer from metadata_support
    if metadata_support:
        if metadata_support.get("dcat") in ["Yes", "Plugin only"]:
            formats.add("RDF")
            formats.add("DCAT")
        if metadata_support.get("ckan_api") == "Yes":
            formats.add("JSON")
        if metadata_support.get("csw") == "Yes":
            formats.add("XML")
            formats.add("CSW")
        if metadata_support.get("oai-pmh") == "Yes":
            formats.add("XML")
        if metadata_support.get("schema-org") == "Yes":
            formats.add("JSON-LD")
        if metadata_support.get("sdmx") == "Yes":
            formats.add("SDMX")
            formats.add("XML")
        if metadata_support.get("wms") == "Yes" or metadata_support.get("wfs") == "Yes":
            formats.add("GeoJSON")
            formats.add("KML")
    
    return sorted(list(formats)) if formats else None


def infer_capabilities(metadata_support: Dict[str, Any], has_api: str, datatypes: Dict[str, str]) -> List[str]:
    """Infer capabilities from existing metadata"""
    capabilities = []
    
    if has_api == "Yes":
        capabilities.append("REST API")
    
    if metadata_support:
        if metadata_support.get("ckan_api") == "Yes":
            capabilities.append("CKAN API")
        if metadata_support.get("csw") == "Yes":
            capabilities.append("CSW catalog service")
        if metadata_support.get("dcat") in ["Yes", "Plugin only"]:
            capabilities.append("DCAT export")
        if metadata_support.get("oai-pmh") == "Yes":
            capabilities.append("OAI-PMH harvesting")
        if metadata_support.get("openaire") == "Yes":
            capabilities.append("OpenAIRE compatibility")
        if metadata_support.get("schema-org") == "Yes":
            capabilities.append("Schema.org markup")
        if metadata_support.get("sdmx") == "Yes":
            capabilities.append("SDMX support")
        if metadata_support.get("stac") == "Yes":
            capabilities.append("STAC catalog")
        if metadata_support.get("wms") == "Yes":
            capabilities.append("WMS service")
        if metadata_support.get("wfs") == "Yes":
            capabilities.append("WFS service")
        if metadata_support.get("wcs") == "Yes":
            capabilities.append("WCS service")
    
    if datatypes:
        if datatypes.get("organizations") == "Yes":
            capabilities.append("Organization management")
        if datatypes.get("topics") == "Yes":
            capabilities.append("Topic categorization")
    
    return capabilities if capabilities else None


def update_software_file(filepath: str, dry_run: bool = False) -> Dict[str, Any]:
    """Update a single software YAML file with new metadata"""
    changes = {}
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = yaml.load(f, Loader=Loader)
        
        if not data or data.get("type") != "Software":
            logger.warning(f"Skipping {filepath}: not a software file")
            return changes
        
        software_id = data.get("id")
        if not software_id:
            logger.warning(f"Skipping {filepath}: missing id")
            return changes
        
        logger.info(f"Processing {software_id}...")
        
        # Update repository_url
        if not data.get("repository_url"):
            repo_url = infer_repository_url(software_id, data.get("website"))
            if repo_url:
                data["repository_url"] = repo_url
                changes["repository_url"] = repo_url
                logger.info(f"  Added repository_url: {repo_url}")
        
        # Update documentation_url
        if not data.get("documentation_url"):
            docs_url = get_documentation_url(software_id, data.get("website"))
            if docs_url:
                data["documentation_url"] = docs_url
                changes["documentation_url"] = docs_url
                logger.info(f"  Added documentation_url: {docs_url}")
        
        # Update version from GitHub if repository_url is available
        if not data.get("version") and data.get("repository_url"):
            version = get_github_latest_release(data["repository_url"])
            if version:
                data["version"] = version
                changes["version"] = version
                logger.info(f"  Added version: {version}")
        
        # Update export_formats
        if not data.get("export_formats"):
            export_formats = infer_export_formats(
                software_id,
                data.get("category", ""),
                data.get("metadata_support", {})
            )
            if export_formats:
                data["export_formats"] = export_formats
                changes["export_formats"] = export_formats
                logger.info(f"  Added export_formats: {export_formats}")
        
        # Update capabilities
        if not data.get("capabilities"):
            capabilities = infer_capabilities(
                data.get("metadata_support", {}),
                data.get("has_api", "No"),
                data.get("datatypes", {})
            )
            if capabilities:
                data["capabilities"] = capabilities
                changes["capabilities"] = capabilities
                logger.info(f"  Added capabilities: {capabilities}")
        
        # Update changelog_url if repository_url exists
        if not data.get("changelog_url") and data.get("repository_url"):
            repo_url = data["repository_url"]
            if "github.com" in repo_url:
                changelog_url = f"{repo_url.rstrip('/')}/releases"
                data["changelog_url"] = changelog_url
                changes["changelog_url"] = changelog_url
                logger.info(f"  Added changelog_url: {changelog_url}")
        
        # Update last_updated timestamp
        if changes:
            data["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            changes["last_updated"] = data["last_updated"]
        
        # Write back to file if not dry run
        if changes and not dry_run:
            with open(filepath, "w", encoding="utf-8") as f:
                yaml.dump(data, f, Dumper=Dumper, default_flow_style=False, sort_keys=False, allow_unicode=True)
            logger.info(f"  Updated {filepath}")
        
    except Exception as e:
        logger.error(f"Error processing {filepath}: {e}", exc_info=True)
    
    return changes


def find_software_files(software_dir: str) -> List[str]:
    """Find all software YAML files"""
    files = []
    for root, dirs, filenames in os.walk(software_dir):
        for filename in filenames:
            if filename.endswith(".yaml") and filename != "_template.tmpl":
                files.append(os.path.join(root, filename))
    return sorted(files)


app = typer.Typer()


@app.command()
def update(
    software_id: Optional[str] = typer.Option(None, "--software", "-s", help="Update specific software by ID"),
    software_dir: str = typer.Option("data/software", "--dir", "-d", help="Software directory path"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be updated without making changes"),
    all: bool = typer.Option(False, "--all", help="Update all software files"),
):
    """
    Update software profile metadata automatically.
    
    Updates version, repository_url, documentation_url, export_formats, capabilities, and changelog_url.
    """
    software_dir_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), software_dir)
    
    if not os.path.exists(software_dir_path):
        logger.error(f"Software directory not found: {software_dir_path}")
        raise typer.Exit(1)
    
    if software_id:
        # Update specific software
        # Find the file
        software_files = find_software_files(software_dir_path)
        target_file = None
        for f in software_files:
            with open(f, "r", encoding="utf-8") as file:
                data = yaml.load(file, Loader=Loader)
                if data and data.get("id") == software_id:
                    target_file = f
                    break
        
        if not target_file:
            logger.error(f"Software '{software_id}' not found")
            raise typer.Exit(1)
        
        changes = update_software_file(target_file, dry_run=dry_run)
        if changes:
            logger.info(f"\nChanges for {software_id}:")
            for key, value in changes.items():
                logger.info(f"  {key}: {value}")
        else:
            logger.info(f"No changes for {software_id}")
    
    elif all:
        # Update all software files
        software_files = find_software_files(software_dir_path)
        logger.info(f"Found {len(software_files)} software files")
        
        total_changes = 0
        for filepath in tqdm(software_files, desc="Updating software"):
            changes = update_software_file(filepath, dry_run=dry_run)
            if changes:
                total_changes += 1
        
        logger.info(f"\nUpdated {total_changes} out of {len(software_files)} software files")
    
    else:
        logger.error("Please specify --software ID or --all")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()

