#!/usr/bin/env python3
"""
Quick test of structural enhancement for agenda completeness
"""
import sys
sys.path.append('scripts')

from pathlib import Path
from microsoft_framework.structural_query_enhancer import StructuralQueryEnhancer

def test_enhancement():
    print("🧪 Testing Structural Query Enhancement")
    print("=" * 40)
    
    # Initialize enhancer
    extracted_text_dir = Path("city_clerk_documents/extracted_text")
    enhancer = StructuralQueryEnhancer(extracted_text_dir)
    
    # Test query
    query = "what are all the items presented in the agenda of jan 9 2024?"
    
    # Test pattern detection
    is_completeness = enhancer.is_agenda_completeness_query(query)
    print(f"📋 Query: {query}")
    print(f"🎯 Detected as completeness query: {is_completeness}")
    
    # Test date extraction
    extracted_date = enhancer.extract_date_from_query(query)
    print(f"📅 Extracted date: {extracted_date}")
    
    # Get complete agenda items for that date
    if extracted_date:
        agenda_data = enhancer.get_complete_agenda_items(extracted_date)
        if agenda_data["found"]:
            print(f"📊 Found {agenda_data['total_items']} agenda items for {extracted_date}")
            
            print("\n📜 RESOLUTIONS:")
            for res in agenda_data['resolutions']:
                print(f"  - {res.get('item_code', 'Unknown')}: {res.get('title', 'No title')}")
                
            print("\n📋 ORDINANCES:")
            for ord_item in agenda_data['ordinances']:
                print(f"  - {ord_item.get('item_code', 'Unknown')}: {ord_item.get('title', 'No title')}")
                
            # Check if Resolution 2024-05 is found
            res_2024_05 = None
            for res in agenda_data['resolutions']:
                if '2024-05' in res.get('title', '') or '2024-05' in res.get('item_code', ''):
                    res_2024_05 = res
                    break
            
            if res_2024_05:
                print(f"\n✅ Found Resolution 2024-05: {res_2024_05.get('title', 'No title')}")
            else:
                print(f"\n❌ Resolution 2024-05 NOT found in {len(agenda_data['resolutions'])} resolutions")
                print("Available resolutions:")
                for res in agenda_data['resolutions']:
                    print(f"  - {res}")
        else:
            print(f"❌ No agenda found for {extracted_date}")
    
    print("\n🎯 Enhancement test completed!")

if __name__ == "__main__":
    test_enhancement() 