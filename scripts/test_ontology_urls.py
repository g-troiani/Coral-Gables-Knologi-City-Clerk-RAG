#!/usr/bin/env python3
"""
Test to verify ontology extractor preserves URLs from pre-extracted data
"""

import sys
from pathlib import Path
sys.path.append('scripts')

from graph_stages.ontology_extractor import OntologyExtractor
import json

def test_ontology_url_preservation():
    """Test that ontology extractor preserves URLs from extracted data."""
    
    # Load the extracted JSON with URLs
    extracted_file = Path("city_clerk_documents/extracted_text/Agenda 01.9.2024_extracted.json")
    
    if not extracted_file.exists():
        print("❌ Extracted file not found. Run URL extraction test first.")
        return False
    
    with open(extracted_file, 'r') as f:
        extracted_data = json.load(f)
    
    agenda_items = extracted_data.get('agenda_items', [])
    
    # Find E-1 in the extracted data
    e1_item = None
    for item in agenda_items:
        if item.get('item_code') == 'E-1':
            e1_item = item
            break
    
    if not e1_item:
        print("❌ E-1 not found in extracted data")
        return False
    
    print(f"✅ Found E-1 in extracted data with {len(e1_item.get('urls', []))} URLs")
    
    # Now test the ontology extractor
    agenda_file = Path("city_clerk_documents/global/City Comissions 2024/Agendas/Agenda 01.9.2024.pdf")
    extractor = OntologyExtractor()
    
    ontology_data = extractor.extract_ontology(agenda_file)
    
    # Find E-1 in the ontology data
    e1_ontology = None
    for section in ontology_data.get('sections', []):
        for item in section.get('items', []):
            if item.get('item_code') == 'E-1':
                e1_ontology = item
                break
        if e1_ontology:
            break
    
    if not e1_ontology:
        print("❌ E-1 not found in ontology data")
        return False
    
    urls_in_ontology = e1_ontology.get('urls', [])
    print(f"✅ Found E-1 in ontology data with {len(urls_in_ontology)} URLs")
    
    if len(urls_in_ontology) > 0:
        print("🎉 SUCCESS: URLs are being preserved through ontology extraction!")
        print(f"   URLs: {urls_in_ontology}")
        return True
    else:
        print("❌ FAIL: URLs are being lost in ontology extraction")
        return False

if __name__ == "__main__":
    print("Testing ontology URL preservation...")
    success = test_ontology_url_preservation()
    sys.exit(0 if success else 1) 