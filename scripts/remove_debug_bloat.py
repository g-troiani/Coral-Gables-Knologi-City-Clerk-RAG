#!/usr/bin/env python3
"""
Remove or simplify debug code bloat from phase2_building.
This script removes verbose debug logging while preserving essential logging.
"""
import re
import os
from pathlib import Path

def remove_debug_blocks(content: str) -> str:
    """Remove debug conditional blocks but keep the essential logging."""
    # Pattern to match if DEBUG_* blocks with their content
    debug_block_pattern = r'if\s+DEBUG_[A-Z_]+:\s*\n(?:(?:    |\t).*\n)*'
    
    # Remove debug blocks but keep essential log statements
    lines = content.split('\n')
    new_lines = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        # Check if this is a debug block
        if re.match(r'\s*if\s+DEBUG_[A-Z_]+:', line):
            # Skip the if line
            i += 1
            # Skip indented content but extract important log messages
            while i < len(lines) and (lines[i].startswith('    ') or lines[i].startswith('\t')):
                # Keep essential log.info statements (not debug-specific ones)
                if 'log.info' in lines[i] and 'DEBUG' not in lines[i]:
                    # De-indent and keep
                    new_lines.append(lines[i].lstrip())
                i += 1
        else:
            new_lines.append(line)
            i += 1
    
    return '\n'.join(new_lines)

def simplify_debug_logs(content: str) -> str:
    """Convert verbose debug logs to simpler ones."""
    # Remove "🧹 DEBUG [DEDUPLICATION]" prefixes
    content = re.sub(r'🧹\s*DEBUG\s*\[[A-Z_]+\]\s*', '', content)
    
    # Remove "DEBUG" from log messages
    content = re.sub(r'(\blog\.(info|debug|warning)\s*\([^)]*)"[^"]*DEBUG[^"]*"', r'\1"', content)
    
    return content

def remove_merge_debug(content: str) -> str:
    """Remove MERGE_DEBUG_ON related code."""
    # Remove MERGE_DEBUG_ON class variable
    content = re.sub(r'MERGE_DEBUG_ON\s*=\s*os\.getenv\([^)]+\)[^\n]*\n', '', content)
    
    # Remove conditions with MERGE_DEBUG_ON
    content = re.sub(r'or\s+self\.MERGE_DEBUG_ON', '', content)
    content = re.sub(r'self\.MERGE_DEBUG_ON\s+or\s+', '', content)
    
    return content

def process_file(file_path: Path) -> bool:
    """Process a single file to remove debug bloat."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Remove debug blocks
        content = remove_debug_blocks(content)
        
        # Simplify debug logs
        content = simplify_debug_logs(content)
        
        # Remove MERGE_DEBUG
        content = remove_merge_debug(content)
        
        # Clean up multiple blank lines
        content = re.sub(r'\n\n\n+', '\n\n', content)
        
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        return False
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Main function to remove debug bloat."""
    phase2_dir = Path("scripts/graph_rag_stages/phase2_building")
    
    files_to_process = [
        phase2_dir / "entity_deduplicator_extended.py",
        phase2_dir / "taxonomy_synthesizer.py",
        phase2_dir / "custom_graph_builder.py",
    ]
    
    print("=== Removing Debug Bloat from Phase2 Building ===\n")
    
    modified_count = 0
    for file_path in files_to_process:
        if file_path.exists():
            print(f"Processing: {file_path}")
            if process_file(file_path):
                print(f"  ✓ Modified: Removed debug bloat")
                modified_count += 1
            else:
                print(f"  - No changes needed")
    
    print(f"\n✅ Complete! Modified {modified_count} files.")

if __name__ == "__main__":
    main()
