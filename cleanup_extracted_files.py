#!/usr/bin/env python3
"""
Cleanup script to keep only files from specific dates:
- 01.23.2024
- 08.26.2014  
- 01.09.2024
"""

import os
import re
from pathlib import Path

def should_keep_file(filename):
    """Check if file should be kept based on date patterns."""
    # Define the dates we want to keep in various formats
    keep_patterns = [
        # 01.23.2024 formats
        r'01[._-]23[._-]2024',
        # 08.26.2014 formats  
        r'08[._-]26[._-]2014',
        # 01.09.2024 formats
        r'01[._-]09[._-]2024'
    ]
    
    # Check if filename matches any of the patterns
    for pattern in keep_patterns:
        if re.search(pattern, filename, re.IGNORECASE):
            return True
    return False

def cleanup_directory(dir_path, dry_run=True):
    """Clean up directory, keeping only files from specified dates."""
    if not dir_path.exists():
        print(f"❌ Directory not found: {dir_path}")
        return 0, 0
    
    files = list(dir_path.glob("*"))
    kept_files = []
    deleted_files = []
    
    print(f"\n📁 Processing directory: {dir_path}")
    print(f"📄 Total files found: {len(files)}")
    
    for file_path in files:
        if file_path.is_file():
            if should_keep_file(file_path.name):
                kept_files.append(file_path.name)
                print(f"✅ KEEP: {file_path.name}")
            else:
                deleted_files.append(file_path.name)
                if not dry_run:
                    file_path.unlink()
                    print(f"🗑️  DELETED: {file_path.name}")
                else:
                    print(f"🗑️  WOULD DELETE: {file_path.name}")
    
    print(f"\n📊 Summary for {dir_path.name}:")
    print(f"  ✅ Files to keep: {len(kept_files)}")
    print(f"  🗑️  Files to delete: {len(deleted_files)}")
    
    if kept_files:
        print(f"\n📋 Files being kept:")
        for file in sorted(kept_files):
            print(f"  - {file}")
    
    return len(kept_files), len(deleted_files)

def main():
    """Main cleanup function."""
    print("🧹 Cleaning up extracted files to keep only specific dates")
    print("📅 Keeping files from: 01.23.2024, 08.26.2014, 01.09.2024")
    
    # Define directories to clean
    project_root = Path(__file__).parent
    directories = [
        project_root / "city_clerk_documents/extracted_markdown",
        project_root / "city_clerk_documents/extracted_json"
    ]
    
    # First do a dry run to show what would be deleted
    print("\n" + "="*60)
    print("🔍 DRY RUN - Showing what would be deleted")
    print("="*60)
    
    total_kept = 0
    total_deleted = 0
    
    for directory in directories:
        kept, deleted = cleanup_directory(directory, dry_run=True)
        total_kept += kept
        total_deleted += deleted
    
    print(f"\n📊 TOTAL SUMMARY (DRY RUN):")
    print(f"  ✅ Total files to keep: {total_kept}")
    print(f"  🗑️  Total files to delete: {total_deleted}")
    
    # Ask for confirmation
    print("\n" + "="*60)
    response = input("❓ Do you want to proceed with the deletion? (yes/no): ").lower().strip()
    
    if response in ['yes', 'y']:
        print("\n🗑️  PROCEEDING WITH ACTUAL DELETION...")
        print("="*60)
        
        total_kept = 0
        total_deleted = 0
        
        for directory in directories:
            kept, deleted = cleanup_directory(directory, dry_run=False)
            total_kept += kept
            total_deleted += deleted
        
        print(f"\n✅ CLEANUP COMPLETE!")
        print(f"📊 FINAL SUMMARY:")
        print(f"  ✅ Total files kept: {total_kept}")
        print(f"  🗑️  Total files deleted: {total_deleted}")
        print(f"\n💰 This should reduce your API costs significantly!")
        print(f"   Before: ~205 files = ~$10-50 in API costs")
        print(f"   After: ~{total_kept} files = ~${total_kept * 0.02:.2f}-{total_kept * 0.1:.2f} in API costs")
        
    else:
        print("❌ Deletion cancelled. No files were deleted.")

if __name__ == "__main__":
    main() 