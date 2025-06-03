#!/usr/bin/env python3
"""
Debug script to analyze ordinance 2024-02 and find why agenda item extraction failed.
"""

import PyPDF2
from pathlib import Path
import re

def analyze_ordinance_2024_02():
    """Analyze the problematic ordinance document."""
    
    # Find the ordinance file
    ordinance_dir = Path("city_clerk_documents/global/City Comissions 2024/Ordinances/2024")
    ordinance_files = list(ordinance_dir.glob("2024-02*.pdf"))
    
    if not ordinance_files:
        print("❌ Could not find ordinance 2024-02")
        return
    
    ordinance_path = ordinance_files[0]
    print(f"📄 Analyzing: {ordinance_path.name}")
    
    # Extract text
    try:
        with open(ordinance_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            full_text = ""
            
            print(f"📊 Document has {len(reader.pages)} pages")
            
            # Extract all text
            for i, page in enumerate(reader.pages):
                page_text = page.extract_text()
                full_text += page_text
                
                # Check last few pages for agenda items
                if i >= len(reader.pages) - 3:
                    print(f"\n--- Page {i+1} (checking for agenda items) ---")
                    # Look for agenda item patterns
                    agenda_patterns = [
                        r'\(Agenda Item[:\s]*([A-Z][\.-]?\d+)\)',
                        r'Agenda Item[:\s]*([A-Z][\.-]?\d+)',
                        r'Item[:\s]*([A-Z][\.-]?\d+)',
                        r'([A-Z][\.-]?\d+[\.-]?)\s*\n',  # E.-2. at end of line
                    ]
                    
                    for pattern in agenda_patterns:
                        matches = re.findall(pattern, page_text, re.IGNORECASE)
                        if matches:
                            print(f"🎯 Found potential agenda items: {matches}")
            
            # Check the last 2000 characters
            print(f"\n--- Last 2000 characters of document ---")
            print(full_text[-2000:])
            
            # Search entire document for agenda patterns
            print(f"\n--- Searching entire document for agenda patterns ---")
            print(f"Total document length: {len(full_text)} characters")
            
            # More comprehensive search
            comprehensive_patterns = [
                r'\(Agenda\s+Item[:\s]*([A-Z][\.-]?\d+[\.-]?)\)',
                r'Agenda\s+Item[:\s]*([A-Z][\.-]?\d+[\.-]?)',
                r'Item[:\s]*([A-Z][\.-]?\d+[\.-]?)',
                r'([A-Z])\.-(\d+)\.',  # E.-2.
                r'([A-Z])-(\d+)',      # E-2
            ]
            
            for pattern in comprehensive_patterns:
                matches = re.findall(pattern, full_text, re.IGNORECASE | re.MULTILINE)
                if matches:
                    print(f"Pattern '{pattern}' found: {matches}")
            
            # Save full text for manual inspection
            debug_dir = Path("city_clerk_documents/graph_json/debug")
            debug_dir.mkdir(exist_ok=True)
            
            with open(debug_dir / "ordinance_2024_02_full_text.txt", 'w', encoding='utf-8') as f:
                f.write(full_text)
            
            print(f"\n✅ Full text saved to: {debug_dir / 'ordinance_2024_02_full_text.txt'}")
            
    except Exception as e:
        print(f"❌ Error analyzing document: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_ordinance_2024_02() 