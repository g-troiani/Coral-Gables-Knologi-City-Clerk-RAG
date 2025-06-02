#!/usr/bin/env python3
"""
Debug script to check what agenda items were extracted from the agenda.
"""

import json
from pathlib import Path

def check_agenda_items():
    """Check what agenda items were extracted."""
    
    # Find the ontology file
    ontology_dir = Path("city_clerk_documents/graph_json")
    ontology_files = list(ontology_dir.glob("*01.9.2024*_ontology.json"))
    
    if not ontology_files:
        print("❌ No ontology file found for 01.09.2024")
        return
    
    ontology_file = ontology_files[0]
    print(f"📄 Checking ontology: {ontology_file.name}")
    
    # Load the ontology
    with open(ontology_file, 'r') as f:
        ontology = json.load(f)
    
    print(f"\n📅 Meeting date: {ontology.get('meeting_date')}")
    
    # Extract all agenda items
    all_items = []
    agenda_structure = ontology.get('agenda_structure', [])
    
    print(f"\n📑 Found {len(agenda_structure)} sections:")
    
    for section in agenda_structure:
        section_name = section.get('section_name', 'Unknown')
        items = section.get('items', [])
        print(f"\n  Section: {section_name}")
        print(f"  Items in section: {len(items)}")
        
        for item in items:
            item_code = item.get('item_code', 'NO_CODE')
            title = item.get('title', 'No title')[:80]
            all_items.append({
                'code': item_code,
                'title': title,
                'section': section_name
            })
            print(f"    - {item_code}: {title}...")
    
    print(f"\n📊 Total agenda items found: {len(all_items)}")
    
    # Check specifically for E items
    e_items = [item for item in all_items if item['code'].startswith('E')]
    print(f"\n🔍 E-section items found:")
    for item in sorted(e_items, key=lambda x: x['code']):
        print(f"  - {item['code']}: {item['title'][:60]}...")
    
    # Check if E-2 exists
    e2_exists = any(item['code'] in ['E-2', 'E.-2.', 'E.-2', 'E.2'] for item in all_items)
    print(f"\n❓ Does E-2 exist in agenda? {e2_exists}")
    
    # Save all items for inspection
    debug_dir = Path("city_clerk_documents/graph_json/debug")
    debug_dir.mkdir(exist_ok=True)
    
    with open(debug_dir / "all_agenda_items.json", 'w') as f:
        json.dump(all_items, f, indent=2)
    
    print(f"\n✅ All agenda items saved to: {debug_dir / 'all_agenda_items.json'}")

if __name__ == "__main__":
    check_agenda_items() 