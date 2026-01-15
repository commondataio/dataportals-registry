#!/usr/bin/env python3
"""
Fix all data quality issues using cursor-agent.

Reads dataquality/primary_priority.jsonl and processes each record
with issues by calling cursor-agent to fix them.
"""

import json
import logging
import subprocess
import hashlib
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Constants
BASE_DIR = Path(__file__).parent.parent
ISSUES_FILE = BASE_DIR / "dataquality" / "primary_priority.jsonl"
ENTITIES_DIR = BASE_DIR / "data" / "entities"


@dataclass
class RecordStats:
    """Statistics for a single record processing."""
    record_id: str
    file_path: str
    issues_count: int
    changed: bool = False
    error: Optional[str] = None


@dataclass
class SummaryStats:
    """Summary statistics for all processed records."""
    total: int = 0
    updated: int = 0
    no_change: int = 0
    failed: int = 0
    records: List[RecordStats] = field(default_factory=list)


def calculate_file_hash(file_path: Path) -> Optional[str]:
    """Calculate MD5 hash of a file."""
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logger.debug(f"Error calculating hash for {file_path}: {e}")
        return None


def read_jsonl(file_path: Path) -> List[Dict[str, Any]]:
    """Read and parse JSONL file."""
    records = []
    if not file_path.exists():
        logger.error(f"Error: File not found: {file_path}")
        return records
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    record = json.loads(line)
                    issues_list = record.get('issues', [])
                    if issues_list:
                        records.append(record)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON at line {line_num}: {e}")
                    continue
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
    
    return records


def build_prompt(record: Dict[str, Any]) -> str:
    """Build a simple, effective prompt for cursor-agent."""
    record_id = record.get('record_id', 'unknown')
    file_path = record.get('file_path', '')
    issues = record.get('issues', [])
    
    prompt_parts = [
        f"Fix all data quality issues for record {record_id} in file {file_path}.",
        "",
        f"Issues to fix ({len(issues)} total):"
    ]
    
    for issue in issues:
        issue_type = issue.get('issue_type', '')
        field = issue.get('field', '')
        action = issue.get('suggested_action', '')
        prompt_parts.append(f"- {issue_type}: {field} - {action}")
    
    prompt_parts.extend([
        "",
        "For each issue, follow the suggested action. Update the metadata comprehensively."
    ])
    
    return "\n".join(prompt_parts)


def call_cursor_agent(workspace: Path, prompt: str) -> tuple[bool, Optional[str]]:
    """Call cursor-agent and return success status and output."""
    try:
        result = subprocess.run(
            [
                "cursor-agent",
                "--print",
                "--workspace", str(workspace),
                "--output-format", "text"
            ],
            input=prompt,
            text=True,
            capture_output=True,
            timeout=300  # 5 minute timeout per record
        )
        
        if result.returncode != 0:
            error_msg = result.stderr.strip() if result.stderr else "Unknown error"
            return False, error_msg
        
        return True, result.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, "Timeout after 5 minutes"
    except FileNotFoundError:
        return False, "cursor-agent not found in PATH"
    except Exception as e:
        return False, str(e)


def process_record(record: Dict[str, Any], stats: SummaryStats, current_index: int) -> RecordStats:
    """Process a single record."""
    record_id = record.get('record_id', 'unknown')
    file_path_str = record.get('file_path', '')
    issues = record.get('issues', [])
    issues_count = len(issues)
    
    full_file_path = ENTITIES_DIR / file_path_str
    relative_file_path = f"data/entities/{file_path_str}"
    
    record_stats = RecordStats(
        record_id=record_id,
        file_path=file_path_str,
        issues_count=issues_count
    )
    
    logger.info(f"[{current_index + 1}/{stats.total}] Processing: {file_path_str}")
    logger.info(f"  Record ID: {record_id}")
    logger.info(f"  Issues to fix: {issues_count}")
    
    # Check if file exists
    if not full_file_path.exists():
        error_msg = f"File not found: {full_file_path}"
        logger.error(f"  ✗ {error_msg}")
        record_stats.error = error_msg
        stats.failed += 1
        return record_stats
    
    # Calculate hash before processing
    before_hash = calculate_file_hash(full_file_path)
    
    # Build prompt
    prompt = build_prompt(record)
    
    # Call cursor-agent
    success, output = call_cursor_agent(BASE_DIR, prompt)
    
    if not success:
        error_msg = output or "Unknown error"
        logger.error(f"  ✗ Failed to process {record_id}")
        logger.debug(f"  Error: {error_msg}")
        record_stats.error = error_msg
        stats.failed += 1
        return record_stats
    
    # Calculate hash after processing
    after_hash = calculate_file_hash(full_file_path)
    
    # Check if file was modified
    if before_hash and after_hash and before_hash != after_hash:
        logger.info(f"  ✓ Updated {record_id}")
        logger.info(f"  File modified: {file_path_str}")
        record_stats.changed = True
        stats.updated += 1
    else:
        logger.info(f"  ○ No changes detected for {record_id}")
        logger.info(f"  File unchanged: {file_path_str}")
        stats.no_change += 1
    
    return record_stats


def main() -> int:
    """Main entry point."""
    logger.info("Starting Cursor automation to fix all data quality issues...")
    logger.info("")
    
    # Check if cursor-agent is available
    try:
        subprocess.run(
            ["cursor-agent", "--version"],
            capture_output=True,
            timeout=5
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        logger.error("Error: cursor-agent is not installed or not in PATH")
        logger.error("Install it from: https://docs.cursor.com/tools/cli")
        return 1
    
    # Read records
    logger.info(f"Reading issues from {ISSUES_FILE}...")
    records = read_jsonl(ISSUES_FILE)
    
    if not records:
        logger.warning("No records with issues found.")
        return 0
    
    logger.info(f"Found {len(records)} records with issues to process.")
    logger.info("")
    
    # Initialize stats
    stats = SummaryStats()
    stats.total = len(records)
    
    # Process each record
    for index, record in enumerate(records):
        record_stats = process_record(record, stats, index)
        stats.records.append(record_stats)
        logger.info("")
    
    # Print summary
    logger.info("=" * 40)
    logger.info("Summary Report")
    logger.info("=" * 40)
    logger.info("")
    logger.info(f"Total records processed: {stats.total}")
    logger.info(f"Successfully updated: {stats.updated}")
    logger.info(f"No changes detected: {stats.no_change}")
    logger.info(f"Failed: {stats.failed}")
    logger.info("")
    
    # Return exit code based on results
    return 1 if stats.failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
