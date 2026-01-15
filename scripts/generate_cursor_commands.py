#!/usr/bin/env python3
"""
Generate Cursor automation script for fixing all data quality issues.

This script parses dataquality/primary_priority.jsonl and generates:
1. A shell script that uses cursor-agent in non-interactive mode to fix all issues
2. A text file with all prompts for manual use
"""

import json
import os
from pathlib import Path
from typing import List, Tuple, Dict, Any

# Base directory (repository root)
BASE_DIR = Path(__file__).parent.parent
ISSUES_FILE = BASE_DIR / "dataquality" / "primary_priority.jsonl"
OUTPUT_SCRIPT = BASE_DIR / "scripts" / "update_all_issues.sh"
OUTPUT_PROMPTS = BASE_DIR / "scripts" / "all_issues_prompts.txt"


def parse_issues_file(file_path: Path) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    Parse primary_priority.jsonl and extract records with all their issues.
    
    Returns:
        List of tuples: [(file_path, record_id, record_data), ...]
        where record_data contains the full record with issues
    """
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
                
                # Get all records that have any issues
                issues_list = record.get('issues', [])
                if issues_list:
                    record_id = record.get('record_id')
                    file_path_str = record.get('file_path')
                    
                    if record_id and file_path_str:
                        records.append((file_path_str, record_id, record))
                    else:
                        print(f"Warning: Skipping record at line {line_num}: missing record_id or file_path")
                        
            except json.JSONDecodeError as e:
                print(f"Warning: Failed to parse JSON at line {line_num}: {e}")
                continue
    
    return records


def generate_shell_script(records: List[Tuple[str, str, Dict[str, Any]]], output_path: Path):
    """
    Generate a bash script that uses cursor-agent in non-interactive mode to fix all issues.
    """
    script_content = """#!/bin/bash
# Generated script to fix all data quality issues using cursor-agent
# This script uses cursor-agent in non-interactive mode (--print flag)
# 
# Usage: ./scripts/update_all_issues.sh
# 
# Note: This script requires:
# - cursor-agent CLI installed and available in PATH
# - Cursor authentication (run 'cursor-agent login' if needed)

set -e  # Exit on error

# Colors for output
GREEN='\\033[0;32m'
BLUE='\\033[0;34m'
YELLOW='\\033[1;33m'
RED='\\033[0;31m'
NC='\\033[0m' # No Color

# Base directory (repository root)
BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"

# Check if cursor-agent is available
if ! command -v cursor-agent &> /dev/null; then
    echo -e "${{RED}}Error: cursor-agent is not installed or not in PATH${{NC}}"
    echo "Install it from: https://docs.cursor.com/tools/cli"
    exit 1
fi

# Check authentication status
if ! cursor-agent status &> /dev/null; then
    echo -e "${{YELLOW}}Warning: Not authenticated with cursor-agent${{NC}}"
    echo "Run 'cursor-agent login' to authenticate"
    echo ""
fi

echo -e "${{BLUE}}Starting Cursor automation to fix all data quality issues...${{NC}}"
echo ""

# Counter
TOTAL={total}
CURRENT=0
UPDATED=0
NO_CHANGE=0
FAILED=0

# Function to fix all issues for a record using cursor-agent
fix_record_issues() {{
    local file_path="$1"
    local record_id="$2"
    local issues_json="$3"
    local full_file_path="${{BASE_DIR}}/data/entities/${{file_path}}"
    local relative_file_path="data/entities/${{file_path}}"
    
    CURRENT=$((CURRENT + 1))
    
    echo -e "${{YELLOW}}[${{CURRENT}}/${{TOTAL}}] Processing: ${{file_path}}${{NC}}"
    echo "  Record ID: ${{record_id}}"
    
    # Parse issues JSON to show issue count and build prompt
    # The JSON is already in compact format, so we can use it directly
    # But we need to handle escaped quotes that bash might have interpreted
    local issue_count issues_summary
    if command -v python3 &> /dev/null; then
        # Pass the JSON directly to Python via command line argument to avoid stdin issues
        # Use python3 -c with the JSON as an argument, properly escaped
        issue_info=$(python3 -c "
import json
import sys
try:
    if len(sys.argv) < 2:
        print('0|')
        sys.exit(1)
    json_str = sys.argv[1]
    if not json_str:
        print('0|')
        sys.exit(1)
    issues = json.loads(json_str)
    if not isinstance(issues, list):
        print('0|')
        sys.exit(1)
    issue_list = []
    for issue in issues[:10]:
        issue_type = issue.get('issue_type', '')
        field = issue.get('field', '')
        action = issue.get('suggested_action', '')
        issue_list.append('- ' + issue_type + ': ' + field + ' - ' + action)
    newline_char = chr(10)
    print(str(len(issues)) + '|' + newline_char.join(issue_list))
except Exception as e:
    print('0|')
    sys.stderr.write('Error parsing issues: ' + str(e) + chr(10))
" "$issues_json" 2>&1 || echo "0|")
        issue_count=$(echo "$issue_info" | cut -d'|' -f1)
        issues_summary=$(echo "$issue_info" | cut -d'|' -f2-)
    else
        issue_count="?"
        issues_summary=""
    fi
    echo "  Issues to fix: $issue_count"
    
    # Build a comprehensive prompt with all issues
    local prompt="Fix all data quality issues for record ID ${{record_id}}. Open and edit the file ${{relative_file_path}}. Review the YAML file and fix the following $issue_count issues:\\n\\n"
    if [ -n "$issues_summary" ]; then
        prompt="$prompt$issues_summary\\n\\n"
    fi
    prompt="$prompt For each issue, follow the suggested action. Update the metadata comprehensively to improve data quality. Make sure to address all issues listed above."
    
    # Check if file exists
    if [ ! -f "${{full_file_path}}" ]; then
        echo -e "${{RED}}✗ File not found: ${{full_file_path}}${{NC}}"
        FAILED=$((FAILED + 1))
        return 1
    fi
    
    # Capture file state before processing
    local before_hash
    before_hash=$(md5sum "${{full_file_path}}" 2>/dev/null | cut -d' ' -f1 || md5 -q "${{full_file_path}}" 2>/dev/null || echo "")
    local before_mtime
    before_mtime=$(stat -f %m "${{full_file_path}}" 2>/dev/null || stat -c %Y "${{full_file_path}}" 2>/dev/null || echo "")
    
    # Extract key metadata fields before processing for comparison
    local before_metadata
    if command -v python3 &> /dev/null; then
        before_metadata=$(python3 -c "
import yaml
import json
import sys
try:
    with open('${{full_file_path}}', 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f) or {{}}
    metadata = {{
        'description': data.get('description') or '',
        'topics_count': len(data.get('topics', [])),
        'tags_count': len(data.get('tags', [])),
        'langs_count': len(data.get('langs', [])),
        'identifiers_count': len(data.get('identifiers', [])),
        'has_license': bool(data.get('rights', {{}}).get('license_id') or data.get('rights', {{}}).get('license_name') or data.get('rights', {{}}).get('license_url')),
        'api_status': data.get('api_status', ''),
        'has_owner_link': bool(data.get('owner', {{}}).get('link'))
    }}
    print(json.dumps(metadata))
except Exception as e:
    print('{{}}')
" 2>/dev/null || echo "{{}}")
    else
        before_metadata="{{}}"
    fi
    
    # Use cursor-agent in non-interactive mode
    # --print: Print responses to console (non-interactive mode, allows write operations)
    # --workspace: Set workspace to repository root (so file paths are relative)
    # --output-format text: Get text output
    # The prompt instructs cursor-agent to edit the specific file
    local output
    output=$(cursor-agent --print --workspace "${{BASE_DIR}}" --output-format text "${{prompt}}" 2>&1)
    local exit_code=$?
    
    # Check if file was modified
    local after_hash
    after_hash=$(md5sum "${{full_file_path}}" 2>/dev/null | cut -d' ' -f1 || md5 -q "${{full_file_path}}" 2>/dev/null || echo "")
    local after_mtime
    after_mtime=$(stat -f %m "${{full_file_path}}" 2>/dev/null || stat -c %Y "${{full_file_path}}" 2>/dev/null || echo "")
    
    # Extract key metadata fields after processing for comparison
    local after_metadata
    if command -v python3 &> /dev/null; then
        after_metadata=$(python3 -c "
import yaml
import json
import sys
try:
    with open('${{full_file_path}}', 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f) or {{}}
    metadata = {{
        'description': data.get('description') or '',
        'topics_count': len(data.get('topics', [])),
        'tags_count': len(data.get('tags', [])),
        'langs_count': len(data.get('langs', [])),
        'identifiers_count': len(data.get('identifiers', [])),
        'has_license': bool(data.get('rights', {{}}).get('license_id') or data.get('rights', {{}}).get('license_name') or data.get('rights', {{}}).get('license_url')),
        'api_status': data.get('api_status', ''),
        'has_owner_link': bool(data.get('owner', {{}}).get('link'))
    }}
    print(json.dumps(metadata))
except Exception as e:
    print('{{}}')
" 2>/dev/null || echo "{{}}")
    else
        after_metadata="{{}}"
    fi
    
    if [ $exit_code -ne 0 ] || echo "$output" | grep -qi "error\|failed"; then
        echo -e "${{RED}}✗ Failed to process ${{record_id}}${{NC}}"
        echo "  Error output: $output"
        FAILED=$((FAILED + 1))
        return 1
    elif [ -n "$before_hash" ] && [ -n "$after_hash" ] && [ "$before_hash" != "$after_hash" ]; then
        echo -e "${{GREEN}}✓ Updated ${{record_id}}${{NC}}"
        echo "  File modified: ${{file_path}}"
        
        # Show meaningful changes using metadata comparison
        if command -v python3 &> /dev/null && [ -n "$before_metadata" ] && [ -n "$after_metadata" ]; then
            python3 <<'PYEOF'
import json
import sys

try:
    before = json.loads(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else {{}}
    after = json.loads(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] else {{}}
    
    changes = []
    
    # Check description
    before_desc = before.get('description', '') or ''
    after_desc = after.get('description', '') or ''
    if not before_desc and after_desc:
        desc_preview = after_desc[:150] + ('...' if len(after_desc) > 150 else '')
        print("  \\033[0;32m+ Description added:\\033[0m")
        print("    " + desc_preview)
        changes.append("description")
    elif before_desc != after_desc and after_desc:
        before_preview = before_desc[:100] + ('...' if len(before_desc) > 100 else '')
        after_preview = after_desc[:150] + ('...' if len(after_desc) > 150 else '')
        print("  \\033[0;32m~ Description updated:\\033[0m")
        print("    Before: " + before_preview)
        print("    After:  " + after_preview)
        changes.append("description")
    
    # Check other fields
    if before.get('topics_count', 0) < after.get('topics_count', 0):
        print("  \\033[0;32m+ Topics added:\\033[0m " + str(before.get('topics_count', 0)) + " → " + str(after.get('topics_count', 0)))
        changes.append("topics")
    
    if before.get('tags_count', 0) < after.get('tags_count', 0):
        print("  \\033[0;32m+ Tags added:\\033[0m " + str(before.get('tags_count', 0)) + " → " + str(after.get('tags_count', 0)))
        changes.append("tags")
    
    if before.get('langs_count', 0) < after.get('langs_count', 0):
        print("  \\033[0;32m+ Languages added:\\033[0m " + str(before.get('langs_count', 0)) + " → " + str(after.get('langs_count', 0)))
        changes.append("langs")
    
    if before.get('identifiers_count', 0) < after.get('identifiers_count', 0):
        print("  \\033[0;32m+ Identifiers added:\\033[0m " + str(before.get('identifiers_count', 0)) + " → " + str(after.get('identifiers_count', 0)))
        changes.append("identifiers")
    
    if not before.get('has_license', False) and after.get('has_license', False):
        print("  \\033[0;32m+ License information added\\033[0m")
        changes.append("license")
    
    if before.get('api_status', '') != after.get('api_status', '') and after.get('api_status'):
        print("  \\033[0;32m~ API status updated:\\033[0m " + str(before.get('api_status', 'none')) + " → " + str(after.get('api_status', 'none')))
        changes.append("api_status")
    
    if not before.get('has_owner_link', False) and after.get('has_owner_link', False):
        print("  \\033[0;32m+ Owner link added\\033[0m")
        changes.append("owner_link")
    
    if not changes:
        print("  Note: File changed but no tracked metadata fields changed")
    else:
        changes_str = ', '.join(changes)
        summary_msg = "  Summary: Fixed " + str(len(changes)) + " field(s): " + changes_str
        print(summary_msg)
except Exception as e:
    print("  Note: Could not compare metadata changes")
PYEOF
            python3 - "$before_metadata" "$after_metadata"
        else
            echo "  Note: File was modified (metadata comparison unavailable)"
        fi
        
        UPDATED=$((UPDATED + 1))
    elif [ -n "$before_mtime" ] && [ -n "$after_mtime" ] && [ "$before_mtime" != "$after_mtime" ]; then
        echo -e "${{GREEN}}✓ Updated ${{record_id}}${{NC}}"
        echo "  File modified: ${{file_path}}"
        echo "  Note: File was modified (using modification time detection)"
        UPDATED=$((UPDATED + 1))
    else
        echo -e "${{YELLOW}}○ No changes detected for ${{record_id}}${{NC}}"
        echo "  File unchanged: ${{file_path}}"
        NO_CHANGE=$((NO_CHANGE + 1))
    fi
    echo ""
}}

"""
    
    # Generate the function calls
    function_calls = []
    for file_path, record_id, record_data in records:
        # Escape special characters in file_path and record_id for bash
        escaped_file = file_path.replace('"', '\\"').replace('$', '\\$').replace('`', '\\`')
        escaped_id = record_id.replace('"', '\\"').replace('$', '\\$').replace('`', '\\`')
        
        # Format issues as JSON - use compact format to avoid issues with long strings
        issues_json = json.dumps(record_data.get('issues', []), separators=(',', ':'))
        # Escape for bash - need to escape backslashes first, then quotes
        # Use single quotes where possible, but for the JSON string we need double quotes
        # So we escape: \ -> \\, " -> \", $ -> \$, ` -> \`, newlines stay as \n
        issues_json_escaped = issues_json.replace('\\', '\\\\').replace('"', '\\"').replace('$', '\\$').replace('`', '\\`')
        # For newlines, we want literal \n in the bash string, which printf %b will convert
        issues_json_escaped = issues_json_escaped.replace('\n', '\\n')
        
        function_calls.append(f'fix_record_issues "{escaped_file}" "{escaped_id}" "{issues_json_escaped}"')
    
    # Summary report section
    # Note: Use ${{variable}} which Python format() converts to ${variable} in bash
    summary_section_template = """
# Print summary report
echo ""
echo -e "${{BLUE}}========================================${{NC}}"
echo -e "${{BLUE}}Summary Report${{NC}}"
echo -e "${{BLUE}}========================================${{NC}}"
echo ""
echo -e "Total records processed: ${{TOTAL}}"
echo -e "${{GREEN}}Successfully updated: ${{UPDATED}}${{NC}}"
echo -e "${{YELLOW}}No changes detected: ${{NO_CHANGE}}${{NC}}"
echo -e "${{RED}}Failed: ${{FAILED}}${{NC}}"
echo ""
if [ $UPDATED -gt 0 ]; then
    echo -e "${{GREEN}}✓ ${{UPDATED}} record(s) were updated with fixes${{NC}}"
fi
if [ $NO_CHANGE -gt 0 ]; then
    echo -e "${{YELLOW}}○ ${{NO_CHANGE}} record(s) had no changes (issues may already be fixed or update failed silently)${{NC}}"
fi
if [ $FAILED -gt 0 ]; then
    echo -e "${{RED}}✗ ${{FAILED}} record(s) failed to process${{NC}}"
fi
echo ""
"""
    
    # Format the summary section (convert {{ to {)
    summary_section = summary_section_template.format()
    
    # Combine everything - only format the {total} placeholder in script_content
    full_script = script_content.format(total=len(records)) + "\n".join(function_calls) + summary_section
    
    # Write the script
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(full_script)
    
    # Make it executable
    os.chmod(output_path, 0o755)
    
    print(f"Generated shell script: {output_path}")
    print(f"  Total records: {len(records)}")


def generate_prompts_file(records: List[Tuple[str, str, Dict[str, Any]]], output_path: Path):
    """
    Generate a text file with all prompts for manual use.
    """
    prompts = []
    prompts.append("# Prompts for fixing all data quality issues")
    prompts.append("# Copy and paste these prompts into Cursor chat when working on each file")
    prompts.append("# Format: Record ID | File Path | Issue Count | Prompt")
    prompts.append("")
    
    for file_path, record_id, record_data in records:
        issues_list = record_data.get('issues', [])
        issue_count = len(issues_list)
        issues_summary = ", ".join([issue.get('issue_type', '') for issue in issues_list[:3]])
        if issue_count > 3:
            issues_summary += f", and {issue_count - 3} more"
        prompt = f"Fix all data quality issues for {record_id}. Issues: {issues_summary}"
        prompts.append(f"{record_id} | {file_path} | {issue_count} issues | {prompt}")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(prompts))
    
    print(f"Generated prompts file: {output_path}")
    print(f"  Total prompts: {len(records)}")


def main():
    """Main function."""
    print(f"Parsing {ISSUES_FILE.name}...")
    records = parse_issues_file(ISSUES_FILE)
    
    if not records:
        print("No records with issues found!")
        return
    
    # Calculate total issues
    total_issues = sum(len(record[2].get('issues', [])) for record in records)
    
    print(f"Found {len(records)} records with {total_issues} total issues")
    print("")
    
    # Generate shell script
    print("Generating shell script...")
    generate_shell_script(records, OUTPUT_SCRIPT)
    print("")
    
    # Generate prompts file
    print("Generating prompts file...")
    generate_prompts_file(records, OUTPUT_PROMPTS)
    print("")
    
    print("Done!")
    print("")
    print("Next steps:")
    print(f"  1. Review the generated script: {OUTPUT_SCRIPT}")
    print(f"  2. Review the prompts file: {OUTPUT_PROMPTS}")
    print(f"  3. Ensure cursor-agent is authenticated: cursor-agent login")
    print(f"  4. Run: ./scripts/update_all_issues.sh")
    print("")
    print("Note: This script uses cursor-agent in non-interactive mode (--print flag).")
    print("      No OS-level permissions are required.")


if __name__ == "__main__":
    main()
