#!/usr/bin/env python3
"""
Find entities that exist in the extraction folders but are missing from counts.
This helps identify parsing or counting issues.
"""

import json
import logging
from pathlib import Path
from collections import defaultdict
from typing import Dict, Set, List

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


def find_missing_entities(ner_output_dir: Path):
    """Find entities that exist in files but aren't being counted properly."""
    
    ner_output_dir = Path(ner_output_dir)
    
    # Track what we find
    all_files = defaultdict(list)  # entity_type -> list of files
    all_entity_ids = defaultdict(set)  # entity_type -> set of IDs found
    file_parse_errors = []
    empty_entity_files = []
    
    print("🔍 Scanning all entity files...\n")
    
    # Process each entity type directory
    for entity_dir in ner_output_dir.iterdir():
        if entity_dir.is_dir() and entity_dir.name not in ['document_chunks', 'relationships']:
            entity_type = entity_dir.name
            
            # Get all JSON files for this entity type
            json_files = list(entity_dir.glob("*.json"))
            all_files[entity_type] = json_files
            
            print(f"\n📁 {entity_type}: {len(json_files)} files")
            
            # Process each file
            for json_file in json_files:
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Check if entities exist
                    entities = data.get('entities', [])
                    if not entities:
                        empty_entity_files.append(json_file)
                        print(f"  ⚠️  EMPTY: {json_file.name}")
                        continue
                    
                    # Extract entity IDs
                    for entity in entities:
                        entity_id = extract_entity_id(entity, entity_type)
                        if entity_id:
                            all_entity_ids[entity_type].add(entity_id)
                            
                            # Check for specific missing entities
                            if "resolution_2024-05" in entity_id:
                                print(f"  ✅ FOUND: {entity_id} in {json_file.name}")
                        else:
                            print(f"  ❌ NO ID: Entity in {json_file.name} has no valid ID")
                            print(f"     Entity data: {json.dumps(entity, indent=2)[:200]}...")
                
                except json.JSONDecodeError as e:
                    file_parse_errors.append((json_file, str(e)))
                    print(f"  ❌ JSON ERROR: {json_file.name} - {e}")
                except Exception as e:
                    file_parse_errors.append((json_file, str(e)))
                    print(f"  ❌ ERROR: {json_file.name} - {e}")
    
    # Summary report
    print("\n" + "="*80)
    print("📊 EXTRACTION INTEGRITY REPORT")
    print("="*80)
    
    # File statistics
    print("\n📁 FILE STATISTICS:")
    total_files = sum(len(files) for files in all_files.values())
    print(f"  Total entity files: {total_files}")
    print(f"  Empty entity files: {len(empty_entity_files)}")
    print(f"  Files with parse errors: {len(file_parse_errors)}")
    
    # Entity counts by type
    print("\n🔢 ENTITY COUNTS BY TYPE:")
    for entity_type, ids in sorted(all_entity_ids.items()):
        print(f"  {entity_type}: {len(ids)} unique entities from {len(all_files[entity_type])} files")
    
    # Empty files detail
    if empty_entity_files:
        print("\n⚠️  EMPTY ENTITY FILES:")
        for f in empty_entity_files[:10]:  # Show first 10
            print(f"  - {f.relative_to(ner_output_dir)}")
        if len(empty_entity_files) > 10:
            print(f"  ... and {len(empty_entity_files) - 10} more")
    
    # Parse errors detail
    if file_parse_errors:
        print("\n❌ PARSE ERROR FILES:")
        for f, error in file_parse_errors[:10]:  # Show first 10
            print(f"  - {f.name}: {error}")
    
    # Search for specific patterns
    print("\n🔍 SEARCHING FOR SPECIFIC PATTERNS:")
    search_patterns = [
        "resolution_2024-05",
        "document_resolution",
        "document_ordinance",
        "document_unknown"
    ]
    
    for pattern in search_patterns:
        matches = []
        for entity_type, ids in all_entity_ids.items():
            for entity_id in ids:
                if pattern in entity_id.lower():
                    matches.append((entity_type, entity_id))
        
        print(f"\n  Pattern '{pattern}':")
        if matches:
            for entity_type, entity_id in matches[:5]:
                print(f"    - {entity_type}: {entity_id}")
            if len(matches) > 5:
                print(f"    ... and {len(matches) - 5} more")
        else:
            print(f"    ❌ NOT FOUND in any extracted entities")
    
    return all_entity_ids, file_parse_errors, empty_entity_files


def extract_entity_id(entity: Dict, entity_type: str) -> str:
    """Extract entity ID with multiple fallback strategies."""
    
    # Primary ID field mapping
    id_field_map = {
        'Person': 'personID',
        'Organization': 'orgID', 
        'Location': 'locationID',
        'Event': 'eventID',
        'Document': 'documentID',
        'AgendaItem': 'agendaItemID',
        'Policy': 'policyID',
        'Asset': 'assetID',
        'Contract': 'contractID',
        'Project': 'projectID',
        'Role': 'roleID',
        'Action': 'actionID',
        'Topic': 'topicID',
        'Section': 'sectionID',
        'Technology': 'technologyID',
        'VoteOutcome': 'voteOutcomeID'
    }
    
    # Try primary ID field
    primary_field = id_field_map.get(entity_type, f"{entity_type.lower()}ID")
    entity_id = entity.get(primary_field)
    
    # Fallback strategies
    if not entity_id:
        # Try common alternatives
        for field in ['id', '_id', 'ID', '_entity_id', 'entityID']:
            entity_id = entity.get(field)
            if entity_id:
                break
    
    # Last resort - try any field ending with 'ID'
    if not entity_id:
        for key, value in entity.items():
            if key.endswith('ID') and isinstance(value, str):
                entity_id = value
                break
    
    return entity_id or ""


def check_specific_file(file_path: Path):
    """Deep inspection of a specific file."""
    print(f"\n🔬 DEEP INSPECTION: {file_path}")
    
    if not file_path.exists():
        print(f"  ❌ File does not exist!")
        return
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print(f"  File size: {len(content)} bytes")
        print(f"  First 500 chars:\n{content[:500]}")
        
        # Try to parse as JSON
        data = json.loads(content)
        print(f"\n  JSON structure:")
        print(f"    Keys: {list(data.keys())}")
        
        if 'entities' in data:
            entities = data['entities']
            print(f"    Entities: {len(entities)} items")
            if entities:
                print(f"    First entity: {json.dumps(entities[0], indent=2)}")
        else:
            print(f"    ⚠️  No 'entities' key found!")
            print(f"    Full content: {json.dumps(data, indent=2)[:1000]}...")
            
    except Exception as e:
        print(f"  ❌ Error reading file: {e}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python find_missing_entities.py <ner_output_dir> [specific_file]")
        sys.exit(1)
    
    ner_dir = Path(sys.argv[1])
    
    # Run the scan
    find_missing_entities(ner_dir)
    
    # If a specific file is provided, inspect it
    if len(sys.argv) > 2:
        specific_file = Path(sys.argv[2])
        check_specific_file(specific_file) 