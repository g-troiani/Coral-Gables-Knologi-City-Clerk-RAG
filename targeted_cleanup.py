#!/usr/bin/env python3
"""
Targeted cleanup script - more conservative approach
Only keeps files from exact dates: 01.23.2024, 08.26.2014, 01.09.2024
"""

import os
from pathlib import Path

def should_keep_file(filename):
    """Check if file should be kept - very conservative approach."""
    
    # Files explicitly protected by user
    protected_files = {
        'Agenda 01.23.2024_stage2_agenda.json',
        'Agenda 01.23.2024_stage3_ontology.json', 
        'Agenda 08.26.2014_stage1_ocr.json',
        'Agenda 08.26.2014_stage2_agenda.json',
        'Agenda 08.26.2014_stage3_ontology.json'
    }
    
    # If it's a specifically protected file, keep it
    if filename in protected_files:
        return True, "PROTECTED_FILE"
    
    # Target date strings to look for
    target_dates = [
        '01.23.2024', '01_23_2024', '01-23-2024',
        '08.26.2014', '08_26_2014', '08-26-2014', 
        '01.09.2024', '01_09_2024', '01-09-2024'
    ]
    
    # Check if filename contains any target date
    filename_lower = filename.lower()
    for date in target_dates:
        if date.replace('.', '_').replace('-', '_') in filename_lower.replace('.', '_').replace('-', '_'):
            return True, f"CONTAINS_DATE: {date}"
    
    return False, "NOT_TARGET_DATE"

def cleanup_directory(dir_path, dry_run=True):
    """Clean up directory with detailed reasoning."""
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
            keep, reason = should_keep_file(file_path.name)
            if keep:
                kept_files.append((file_path.name, reason))
                print(f"✅ KEEP: {file_path.name} ({reason})")
            else:
                deleted_files.append((file_path.name, reason))
                if not dry_run:
                    file_path.unlink()
                    print(f"🗑️  DELETED: {file_path.name}")
                else:
                    print(f"🗑️  WOULD DELETE: {file_path.name}")
    
    print(f"\n📊 Summary for {dir_path.name}:")
    print(f"  ✅ Files to keep: {len(kept_files)}")
    print(f"  🗑️  Files to delete: {len(deleted_files)}")
    
    return len(kept_files), len(deleted_files)

def main():
    """Main cleanup with confirmation."""
    print("🎯 TARGETED CLEANUP - Conservative Approach")
    print("📅 Keeping files from: 01.23.2024, 08.26.2014, 01.09.2024")
    print("🛡️  Plus specifically protected files")
    
    directories = [
        Path("city_clerk_documents/extracted_markdown"),
        Path("city_clerk_documents/extracted_json")
    ]
    
    # Show what would be kept/deleted
    print("\n" + "="*60)
    print("🔍 DRY RUN")
    print("="*60)
    
    total_kept = 0
    total_deleted = 0
    
    for directory in directories:
        kept, deleted = cleanup_directory(directory, dry_run=True)
        total_kept += kept
        total_deleted += deleted
    
    print(f"\n📊 TOTAL SUMMARY:")
    print(f"  ✅ Files to keep: {total_kept}")
    print(f"  🗑️  Files to delete: {total_deleted}")
    print(f"  💰 Cost reduction: ~${total_deleted * 0.03:.2f} saved")
    
    # Ask for confirmation
    response = input("\n❓ Proceed with deletion? (yes/no): ").lower().strip()
    
    if response in ['yes', 'y']:
        print("\n🗑️  DELETING FILES...")
        
        for directory in directories:
            cleanup_directory(directory, dry_run=False)
        
        print(f"\n✅ CLEANUP COMPLETE!")
    else:
        print("❌ Cancelled.")

if __name__ == "__main__":
    main() 