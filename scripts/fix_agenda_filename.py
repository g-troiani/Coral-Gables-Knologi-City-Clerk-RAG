#!/usr/bin/env python3
"""Fix the agenda_unknown.md filename."""

import json
from pathlib import Path
import re
import shutil

def fix_agenda_filename():
    """Rename agenda_unknown.md to proper date format."""
    
    # Find the JSON file for the agenda
    json_dir = Path("city_clerk_documents/extracted_text")
    markdown_dir = Path("city_clerk_documents/extracted_markdown")
    
    # Look for agenda JSON files
    for json_file in json_dir.glob("Agenda*.json"):
        print(f"Found JSON: {json_file.name}")
        
        # Extract date from filename
        date_match = re.search(r'Agenda[_ ](\d{1,2})\.(\d{1,2})\.(\d{4})', json_file.name)
        if date_match:
            month = date_match.group(1).zfill(2)
            day = date_match.group(2).zfill(2) 
            year = date_match.group(3)
            
            # Check if agenda_unknown.md exists
            unknown_file = markdown_dir / "agenda_unknown.md"
            if unknown_file.exists():
                # New filename
                new_name = f"agenda_{month}_{day}_{year}.md"
                new_path = markdown_dir / new_name
                
                print(f"Renaming: agenda_unknown.md → {new_name}")
                shutil.move(unknown_file, new_path)
                
                print(f"✅ Fixed agenda filename to: {new_name}")
                return
    
    print("❌ Could not determine correct date for agenda")

if __name__ == "__main__":
    fix_agenda_filename() 