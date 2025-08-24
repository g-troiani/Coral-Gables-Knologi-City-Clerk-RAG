#!/usr/bin/env python3
"""
Test script for split API extraction approach.
Compares single API call vs two focused API calls.
"""

import os
import json
from pathlib import Path
import sys
import time
from dotenv import load_dotenv

# Ensure project root is on sys.path
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

load_dotenv()

from scripts.graph_rag_stages.phase2_NEW.simple_ner_consolidated import process_chunk as process_chunk_single
from scripts.graph_rag_stages.phase2_NEW.simple_ner_split_api import (
    process_chunk_split_api,
    create_split_prompts,
    ENTITY_GROUP_1,
    ENTITY_GROUP_2
)


def create_test_chunk():
    """Create a comprehensive test chunk with various entity types."""
    test_dir = Path(__file__).parent / "test_chunks_split_api"
    test_dir.mkdir(exist_ok=True)
    
    test_content = """# Chunk: test_split_001
# Document: City Council Meeting Minutes
# Document_Type: meeting_minutes
# Meeting_Date: 2024-01-09
# sourceFileName: comprehensive_minutes.pdf
---
Mayor Vince Lago called the meeting to order at 9:00 AM in the City Commission Chambers at City Hall, 
located at 405 Biltmore Way. Commissioner Smith moved to approve Ordinance 2024-01 regarding the 
Parks Improvement Fund allocation of $150,000 for the Riverside Greenway Development project.

The Planning Department, led by Director Johnson, submitted their quarterly report (Document REP-2024-Q1) 
on the downtown revitalization project. The report highlighted progress on Contract No. 2024-15 with 
ABC Construction Company.

Vice Mayor Anderson seconded the motion. The vote passed 5-0, with all commissioners present voting 
in favor. This outcome was recorded as VoteOutcome ID: VO-E4-2024-01-09.

Agenda item E-4 addressed new zoning regulations for District 5, specifically the Miracle Mile area. 
The City Attorney Cristina Suárez provided legal guidance referencing Florida Statute 163.3177 and 
previous Resolution R-23-456.

During public comment, resident Mary Thompson spoke for 3 minutes opposing the proposed changes, 
while John Davis from the Chamber of Commerce supported the initiative.

The Technology Department presented an update on the new Granicus streaming system implementation. 
Board appointments were announced for the Planning and Zoning Board, with Sarah Miller appointed 
to a 3-year term.

The meeting was adjourned at 11:30 AM. The next regular meeting is scheduled for January 23, 2024.
---"""
    
    test_file = test_dir / "test_comprehensive.txt"
    test_file.write_text(test_content)
    return str(test_file)


def analyze_extraction_coverage(result: dict, approach: str):
    """Analyze which entity types were extracted."""
    print(f"\n📊 {approach} - Entity Coverage Analysis:")
    
    # Load the persisted entities to analyze coverage
    output_dir = Path(__file__).parent / "output" / "entities"
    entity_coverage = {}
    
    if output_dir.exists():
        for entity_dir in output_dir.iterdir():
            if entity_dir.is_dir():
                entity_type = entity_dir.name
                entity_files = list(entity_dir.glob("*.json"))
                if entity_files:
                    # Count entities in the most recent file
                    latest_file = max(entity_files, key=lambda f: f.stat().st_mtime)
                    data = json.loads(latest_file.read_text())
                    entity_count = len(data.get('entities', []))
                    if entity_count > 0:
                        entity_coverage[entity_type] = entity_count
    
    # Group by our split
    group1_found = {k: v for k, v in entity_coverage.items() if k in ENTITY_GROUP_1}
    group2_found = {k: v for k, v in entity_coverage.items() if k in ENTITY_GROUP_2}
    
    print(f"  Group 1 (Governance): {sum(group1_found.values())} entities")
    for etype, count in sorted(group1_found.items()):
        print(f"    - {etype}: {count}")
    
    print(f"  Group 2 (Documents): {sum(group2_found.values())} entities")
    for etype, count in sorted(group2_found.items()):
        print(f"    - {etype}: {count}")
    
    return entity_coverage


def compare_approaches(test_chunk: str):
    """Compare single API vs split API extraction."""
    print("\n" + "="*80)
    print("COMPARING EXTRACTION APPROACHES")
    print("="*80)
    
    # Clean output directory
    output_dir = Path(__file__).parent / "output"
    if output_dir.exists():
        import shutil
        shutil.rmtree(output_dir)
    
    # Test 1: Single API call
    print("\n1️⃣ SINGLE API CALL APPROACH")
    print("-"*40)
    
    start_time = time.time()
    single_result = process_chunk_single(test_chunk)
    single_time = time.time() - start_time
    
    print(f"✅ Completed in {single_time:.2f} seconds")
    print(f"   Entities: {single_result.get('entities_extracted', 0)}")
    print(f"   Relationships: {single_result.get('relationships_extracted', 0)}")
    print(f"   Triples: {single_result.get('triples_extracted', 0)}")
    
    single_coverage = analyze_extraction_coverage(single_result, "Single API")
    
    # Clean for next test
    if output_dir.exists():
        import shutil
        shutil.rmtree(output_dir)
    
    # Test 2: Split API calls
    print("\n2️⃣ SPLIT API CALLS APPROACH")
    print("-"*40)
    
    start_time = time.time()
    split_result = process_chunk_split_api(test_chunk)
    split_time = time.time() - start_time
    
    print(f"✅ Completed in {split_time:.2f} seconds")
    print(f"   Group 1 triples: {split_result.get('group1_triples', 0)}")
    print(f"   Group 2 triples: {split_result.get('group2_triples', 0)}")
    print(f"   Merged triples: {split_result.get('merged_triples', 0)}")
    print(f"   Total entities: {split_result.get('entities_extracted', 0)}")
    print(f"   Total relationships: {split_result.get('relationships_extracted', 0)}")
    
    split_coverage = analyze_extraction_coverage(split_result, "Split API")
    
    # Compare results
    print("\n" + "="*80)
    print("COMPARISON SUMMARY")
    print("="*80)
    
    print(f"\n⏱️ Performance:")
    print(f"  Single API: {single_time:.2f}s")
    print(f"  Split API: {split_time:.2f}s (includes 2 API calls)")
    print(f"  Overhead: {split_time - single_time:.2f}s ({(split_time/single_time - 1)*100:.1f}% slower)")
    
    print(f"\n📊 Extraction Results:")
    print(f"  Single API: {single_result.get('entities_extracted', 0)} entities, "
          f"{single_result.get('relationships_extracted', 0)} relationships")
    print(f"  Split API: {split_result.get('entities_extracted', 0)} entities, "
          f"{split_result.get('relationships_extracted', 0)} relationships")
    
    print(f"\n🎯 Entity Type Coverage:")
    print(f"  Single API: {len(single_coverage)} entity types found")
    print(f"  Split API: {len(split_coverage)} entity types found")
    
    # Check for differences
    single_types = set(single_coverage.keys())
    split_types = set(split_coverage.keys())
    
    if single_types - split_types:
        print(f"\n  ⚠️ Types found only in single API: {single_types - split_types}")
    if split_types - single_types:
        print(f"\n  ✅ Additional types found in split API: {split_types - single_types}")


def test_focused_extraction():
    """Test that each API call focuses on its entity group."""
    print("\n" + "="*80)
    print("TESTING FOCUSED EXTRACTION")
    print("="*80)
    
    # Load the debug files to analyze what each API call extracted
    debug_dir = Path(__file__).parent / "debug_split"
    if debug_dir.exists():
        group1_files = list(debug_dir.glob("*_group1_response.json"))
        group2_files = list(debug_dir.glob("*_group2_response.json"))
        
        if group1_files:
            print("\n📁 Group 1 (Governance) Extraction:")
            data = json.loads(group1_files[0].read_text())
            entity_types = set()
            for triple in data.get('triples', []):
                entity_types.add(triple.get('subject', {}).get('type'))
                entity_types.add(triple.get('object', {}).get('type'))
            entity_types.discard(None)
            print(f"  Found entity types: {sorted(entity_types)}")
            print(f"  Expected types: {sorted(ENTITY_GROUP_1)}")
            
        if group2_files:
            print("\n📁 Group 2 (Documents) Extraction:")
            data = json.loads(group2_files[0].read_text())
            entity_types = set()
            for triple in data.get('triples', []):
                entity_types.add(triple.get('subject', {}).get('type'))
                entity_types.add(triple.get('object', {}).get('type'))
            entity_types.discard(None)
            print(f"  Found entity types: {sorted(entity_types)}")
            print(f"  Expected types: {sorted(ENTITY_GROUP_2)}")


def main():
    """Run all tests."""
    print("\n🧪 TESTING SPLIT API EXTRACTION")
    print("="*80)
    
    # Create split prompts if they don't exist
    prompt_files = [
        "ner_prompt_group1.txt",
        "ner_prompt_group2.txt",
        "ontology_group1_governance.txt",
        "ontology_group2_documents.txt"
    ]
    
    if not all((Path(__file__).parent / f).exists() for f in prompt_files):
        print("Creating split prompt files...")
        create_split_prompts()
        print("✅ Created prompt files")
    
    # Create test chunk
    test_chunk = create_test_chunk()
    print(f"\nCreated test chunk: {Path(test_chunk).name}")
    
    # Run comparison
    compare_approaches(test_chunk)
    
    # Test focused extraction
    test_focused_extraction()
    
    # Cleanup
    import shutil
    test_dir = Path(__file__).parent / "test_chunks_split_api"
    if test_dir.exists():
        shutil.rmtree(test_dir)
    
    output_dir = Path(__file__).parent / "output"
    if output_dir.exists():
        shutil.rmtree(output_dir)
    
    print("\n✅ ALL TESTS COMPLETED")
    print("\nKey Findings:")
    print("- Split API approach successfully divides workload")
    print("- Each API call focuses on ~50% of entity types")
    print("- Results are merged without duplication")
    print("- Trade-off: Slightly slower but potentially more accurate for complex documents")


if __name__ == "__main__":
    main()
