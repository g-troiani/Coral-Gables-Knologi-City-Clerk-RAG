# scripts/check_extracted_urls.py
import json
from pathlib import Path

extracted_file = Path("city_clerk_documents/extracted_text/Agenda 01.9.2024_extracted.json")

if extracted_file.exists():
    with open(extracted_file, 'r') as f:
        data = json.load(f)
    
    print(f"Total hyperlinks: {len(data.get('hyperlinks', []))}")
    print(f"Total agenda items: {len(data.get('agenda_items', []))}")
    
    # Check E-1 specifically
    for item in data.get('agenda_items', []):
        if item.get('item_code') == 'E-1':
            print(f"\nE-1 Item found:")
            print(f"  Title: {item.get('title', '')[:50]}...")
            print(f"  URLs: {item.get('urls', 'NO URLS FIELD')}")
            if 'urls' in item:
                print(f"  URL count: {len(item['urls'])}")
                for url in item['urls']:
                    print(f"    - {url}")
else:
    print(f"File not found: {extracted_file}") 