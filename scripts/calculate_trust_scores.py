#!/usr/bin/env python
# This script calculates trust scores for data catalogs

import logging
import typer
import yaml
import os
import json
from typing import Dict, Any, Optional, Tuple
from collections import defaultdict
import tqdm

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Get script directory and repository root for path resolution
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)

ROOT_DIR = os.path.join(_REPO_ROOT, "data", "entities")

app = typer.Typer()

# Owner type scores (0-40 points)
OWNER_TYPE_SCORES = {
    "Academy": 40,
    "Central government": 35,
    "Regional government": 30,
    "Local government": 25,
    "International": 30,
    "Civil society": 15,
    "NGO": 15,
    "Business": 10,
    "Community": 5,
}

# Catalog type scores (-10 to +10 points)
CATALOG_TYPE_SCORES = {
    "Scientific data repository": 10,
    "Open data portal": 5,
    "Geoportal": 5,
    "Indicators catalog": 5,
    "Microdata catalog": 5,
    "Data search engines": -10,  # Aggregators
    "Machine learning catalog": 0,
    "API Catalog": 0,
    "Data marketplaces": -5,
    "Metadata catalog": 0,
    "Other": 0,
}


def get_re3data_identifier(catalog: Dict[str, Any]) -> Optional[str]:
    """Extract re3data identifier from catalog identifiers."""
    identifiers = catalog.get("identifiers", [])
    for identifier in identifiers:
        if isinstance(identifier, dict) and identifier.get("id") == "re3data":
            return identifier.get("value")
    return None


def calculate_owner_type_score(catalog: Dict[str, Any]) -> int:
    """Calculate score based on owner type."""
    owner = catalog.get("owner", {})
    owner_type = owner.get("type", "")
    
    # Handle variations
    if owner_type in OWNER_TYPE_SCORES:
        return OWNER_TYPE_SCORES[owner_type]
    
    # Default for unknown types
    return 10


def calculate_catalog_type_score(catalog: Dict[str, Any]) -> int:
    """Calculate score based on catalog type."""
    catalog_type = catalog.get("catalog_type", "")
    return CATALOG_TYPE_SCORES.get(catalog_type, 0)


def calculate_license_score(catalog: Dict[str, Any]) -> int:
    """Calculate score based on license/rights information."""
    rights = catalog.get("rights", {})
    if not rights:
        return -15  # Missing all license information
    
    score = 0
    
    # Check for license information
    has_license = (
        rights.get("license_id") or
        rights.get("license_name") or
        rights.get("license_url")
    )
    
    if has_license:
        score += 15
    
    # Check rights_type
    rights_type = rights.get("rights_type")
    if rights_type:
        if rights_type == "unknown":
            score -= 5
        else:
            score += 5
    else:
        # No rights_type specified
        if not has_license:
            score = -15  # Missing all license information
    
    return score


def calculate_re3data_score(catalog: Dict[str, Any], re3data_trust_seals: Dict[str, bool] = None) -> int:
    """Calculate score based on re3data identifier and trust seals."""
    if re3data_trust_seals is None:
        re3data_trust_seals = {}
    
    re3data_id = get_re3data_identifier(catalog)
    if not re3data_id:
        return 0
    
    score = 10  # Has re3data identifier
    
    # Check for trust seal/certification
    if re3data_trust_seals.get(re3data_id, False):
        score += 10
    
    return score


def calculate_additional_factors_score(catalog: Dict[str, Any]) -> int:
    """Calculate score based on additional factors (API, status)."""
    score = 0
    
    # Active API
    if catalog.get("api") is True and catalog.get("api_status") == "active":
        score += 5
    
    # Status
    status = catalog.get("status", "").lower()
    if status == "active":
        score += 5
    elif status == "inactive":
        score -= 5
    
    return score


def calculate_trust_score(
    catalog: Dict[str, Any],
    re3data_trust_seals: Dict[str, bool] = None
) -> Tuple[int, Dict[str, int]]:
    """
    Calculate trust score for a catalog.
    
    Returns:
        tuple: (final_score, components_dict)
    """
    components = {}
    
    # Calculate each component
    components["owner_type_score"] = calculate_owner_type_score(catalog)
    components["catalog_type_score"] = calculate_catalog_type_score(catalog)
    components["license_score"] = calculate_license_score(catalog)
    components["re3data_score"] = calculate_re3data_score(catalog, re3data_trust_seals)
    components["additional_factors_score"] = calculate_additional_factors_score(catalog)
    
    # Calculate base score
    base_score = sum(components.values())
    components["base_score"] = base_score
    
    # Normalize to 0-100
    final_score = max(0, min(100, base_score))
    
    return final_score, components


def load_re3data_trust_seals(filepath: Optional[str] = None) -> Dict[str, bool]:
    """Load re3data trust seals mapping from file."""
    if filepath is None:
        filepath = os.path.join(_REPO_ROOT, "data", "re3data_trust_seals.json")
    
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load re3data trust seals: {e}")
    
    return {}


@app.command()
def calculate(
    dryrun: bool = typer.Option(False, "--dry-run", help="Dry run mode - don't update files"),
    re3data_file: Optional[str] = typer.Option(None, "--re3data-file", help="Path to re3data trust seals JSON file"),
    output_stats: Optional[str] = typer.Option(None, "--output-stats", help="Output statistics to JSON file"),
):
    """Calculate trust scores for all catalogs."""
    
    # Load re3data trust seals
    re3data_trust_seals = load_re3data_trust_seals(re3data_file)
    logger.info(f"Loaded {len(re3data_trust_seals)} re3data trust seal mappings")
    
    # Collect all YAML files
    all_files = []
    for root, dirs, files in os.walk(ROOT_DIR):
        all_files.extend(
            [os.path.join(root, fi) for fi in files if fi.endswith(".yaml")]
        )
    
    logger.info(f"Found {len(all_files)} catalog files")
    
    # Statistics
    stats = {
        "total": len(all_files),
        "processed": 0,
        "updated": 0,
        "skipped": 0,
        "errors": 0,
        "score_distribution": defaultdict(int),
        "by_owner_type": defaultdict(lambda: {"count": 0, "avg_score": 0.0, "scores": []}),
        "by_catalog_type": defaultdict(lambda: {"count": 0, "avg_score": 0.0, "scores": []}),
    }
    
    # Process each file
    with tqdm.tqdm(total=len(all_files), desc="Calculating trust scores") as pbar:
        for filepath in all_files:
            try:
                # Load YAML
                with open(filepath, "r", encoding="utf-8") as f:
                    catalog = yaml.load(f, Loader=Loader)
                
                if not catalog:
                    stats["skipped"] += 1
                    pbar.update(1)
                    continue
                
                # Calculate trust score
                trust_score, components = calculate_trust_score(catalog, re3data_trust_seals)
                
                # Update catalog
                catalog["trust_score"] = trust_score
                catalog["trust_score_components"] = components
                
                # Update statistics
                stats["processed"] += 1
                score_bucket = (trust_score // 10) * 10
                stats["score_distribution"][f"{score_bucket}-{score_bucket+9}"] += 1
                
                owner_type = catalog.get("owner", {}).get("type", "Unknown")
                stats["by_owner_type"][owner_type]["count"] += 1
                stats["by_owner_type"][owner_type]["scores"].append(trust_score)
                
                catalog_type = catalog.get("catalog_type", "Unknown")
                stats["by_catalog_type"][catalog_type]["count"] += 1
                stats["by_catalog_type"][catalog_type]["scores"].append(trust_score)
                
                # Save if not dry run
                if not dryrun:
                    with open(filepath, "w", encoding="utf-8") as f:
                        yaml.dump(catalog, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
                    stats["updated"] += 1
                
            except Exception as e:
                logger.error(f"Error processing {filepath}: {e}")
                stats["errors"] += 1
            
            pbar.update(1)
    
    # Calculate averages
    for owner_type in stats["by_owner_type"]:
        scores = stats["by_owner_type"][owner_type]["scores"]
        if scores:
            stats["by_owner_type"][owner_type]["avg_score"] = sum(scores) / len(scores)
        del stats["by_owner_type"][owner_type]["scores"]
    
    for catalog_type in stats["by_catalog_type"]:
        scores = stats["by_catalog_type"][catalog_type]["scores"]
        if scores:
            stats["by_catalog_type"][catalog_type]["avg_score"] = sum(scores) / len(scores)
        del stats["by_catalog_type"][catalog_type]["scores"]
    
    # Print statistics
    logger.info("\n=== Trust Score Calculation Statistics ===")
    logger.info(f"Total files: {stats['total']}")
    logger.info(f"Processed: {stats['processed']}")
    logger.info(f"Updated: {stats['updated']}")
    logger.info(f"Skipped: {stats['skipped']}")
    logger.info(f"Errors: {stats['errors']}")
    
    logger.info("\n=== Score Distribution ===")
    for bucket in sorted(stats["score_distribution"].keys(), key=lambda x: int(x.split("-")[0])):
        logger.info(f"{bucket}: {stats['score_distribution'][bucket]}")
    
    logger.info("\n=== Average Scores by Owner Type ===")
    for owner_type in sorted(stats["by_owner_type"].keys()):
        avg = stats["by_owner_type"][owner_type]["avg_score"]
        count = stats["by_owner_type"][owner_type]["count"]
        logger.info(f"{owner_type}: {avg:.1f} (n={count})")
    
    logger.info("\n=== Average Scores by Catalog Type ===")
    for catalog_type in sorted(stats["by_catalog_type"].keys()):
        avg = stats["by_catalog_type"][catalog_type]["avg_score"]
        count = stats["by_catalog_type"][catalog_type]["count"]
        logger.info(f"{catalog_type}: {avg:.1f} (n={count})")
    
    # Save statistics to file if requested
    if output_stats:
        with open(output_stats, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        logger.info(f"\nStatistics saved to {output_stats}")
    
    if dryrun:
        logger.info("\n=== DRY RUN MODE - No files were updated ===")


if __name__ == "__main__":
    app()

