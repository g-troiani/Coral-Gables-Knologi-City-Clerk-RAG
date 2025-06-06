#!/usr/bin/env python3
"""
Script to update all occurrences of 'knologi' to 'knologi' in the codebase
"""

import os
import re
from pathlib import Path

def update_folder_references(root_dir='.', dry_run=True):
    """
    Find and replace all occurrences of 'knologi' with 'knologi'
    
    Args:
        root_dir: Root directory to search in
        dry_run: If True, only show what would be changed without modifying files
    """
    # Pattern to match knologi with combining ring above (U+030A)
    old_pattern = r'knologi\u030a'
    new_string = 'knologi'
    
    # File extensions to check
    extensions = ['.py', '.yaml', '.yml', '.json', '.txt', '.md', '.cfg', '.ini', '.env', '.sh']
    
    # Directories to skip
    skip_dirs = {'.git', '__pycache__', 'node_modules', '.venv', 'venv', 'env', '.idea', '.vscode'}
    
    files_to_update = []
    
    for root, dirs, files in os.walk(root_dir):
        # Remove directories to skip from the search
        dirs[:] = [d for d in dirs if d not in skip_dirs]
        
        for file in files:
            # Check if file has relevant extension
            if any(file.endswith(ext) for ext in extensions):
                file_path = Path(root) / file
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Check if the file contains the pattern
                    if re.search(old_pattern, content):
                        files_to_update.append(file_path)
                        
                        if dry_run:
                            print(f"\n📄 Would update: {file_path}")
                            # Show the lines that would be changed
                            lines = content.split('\n')
                            for i, line in enumerate(lines, 1):
                                if re.search(old_pattern, line):
                                    print(f"   Line {i}: {line.strip()}")
                                    print(f"   → Would change to: {re.sub(old_pattern, new_string, line).strip()}")
                        else:
                            # Actually update the file
                            new_content = re.sub(old_pattern, new_string, content)
                            with open(file_path, 'w', encoding='utf-8') as f:
                                f.write(new_content)
                            print(f"✅ Updated: {file_path}")
                            
                except Exception as e:
                    print(f"❌ Error processing {file_path}: {e}")
    
    print(f"\n{'=' * 60}")
    print(f"Summary: Found {len(files_to_update)} files to update")
    
    if dry_run and files_to_update:
        print("\n⚠️  This was a dry run. To actually update files, run with dry_run=False")
    elif not files_to_update:
        print("\n✅ No files found with 'knologi' references")
    
    return files_to_update

def update_graphrag_config():
    """
    Specifically update GraphRAG configuration files
    """
    config_files = [
        'graphrag_data/config.yml',
        'graphrag_data/settings.yaml',
        'settings.yaml',
        '.env',
        'config.py'
    ]
    
    for config_file in config_files:
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Replace the unicode character
                if '\u030a' in content:
                    new_content = content.replace('knologi\u030a', 'knologi')
                    with open(config_file, 'w', encoding='utf-8') as f:
                        f.write(new_content)
                    print(f"✅ Updated config file: {config_file}")
                    
            except Exception as e:
                print(f"❌ Error updating {config_file}: {e}")

def main():
    print("🔍 Searching for files with 'knologi' references...\n")
    
    # First do a dry run to show what will be changed
    print("DRY RUN - Showing what would be changed:")
    print("=" * 60)
    files = update_folder_references(dry_run=True)
    
    if files:
        response = input("\n\nProceed with updating these files? (y/n): ")
        if response.lower() == 'y':
            print("\n🔄 Updating files...")
            update_folder_references(dry_run=False)
            
            # Also update specific config files
            print("\n🔧 Updating GraphRAG configuration files...")
            update_graphrag_config()
            
            print("\n✅ All files updated successfully!")
            print("\n⚠️  Important: You may need to:")
            print("   1. Re-run the indexing process if paths in the index are affected")
            print("   2. Update any environment variables or system paths")
            print("   3. Clear any caches that might contain the old path")
    else:
        print("\n✅ No updates needed!")

if __name__ == "__main__":
    main() 