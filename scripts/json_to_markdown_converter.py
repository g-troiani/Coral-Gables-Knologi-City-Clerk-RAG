#!/usr/bin/env python3
"""Convert existing JSON extractions to markdown."""

import json
from pathlib import Path
import sys
sys.path.append('.')

from scripts.graph_stages.agenda_pdf_extractor import AgendaPDFExtractor

def convert_jsons_to_markdown():
    """Convert all existing JSON files to markdown."""
    
    json_dir = Path("city_clerk_documents/extracted_text")
    
    # Process agenda JSONs
    print("Converting agenda JSONs to markdown...")
    extractor = AgendaPDFExtractor()
    
    for json_file in json_dir.glob("Agenda*_extracted.json"):
        print(f"Processing: {json_file.name}")
        
        with open(json_file, 'r') as f:
            agenda_data = json.load(f)
        
        # Call the markdown save method
        extractor._save_agenda_as_markdown(agenda_data, json_file)
    
    print("✅ Conversion complete!")
    
    # Check results
    markdown_dir = Path("city_clerk_documents/extracted_markdown")
    md_files = list(markdown_dir.glob("*.md"))
    print(f"\nMarkdown files: {len(md_files)}")
    print("- Agendas:", len([f for f in md_files if f.name.startswith('agenda_')]))
    print("- Ordinances:", len([f for f in md_files if f.name.startswith('ordinance_')]))
    print("- Resolutions:", len([f for f in md_files if f.name.startswith('resolution_')]))
    print("- Verbatims:", len([f for f in md_files if f.name.startswith('verbatim_')]))

if __name__ == "__main__":
    convert_jsons_to_markdown() 