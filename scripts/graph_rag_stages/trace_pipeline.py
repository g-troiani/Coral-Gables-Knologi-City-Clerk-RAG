#!/usr/bin/env python3
"""Trace entities through the pipeline to find where they're being lost"""

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

def trace_pipeline():
    # 1. Count chunks
    chunks_dir = Path("simple_ner_graph/document_chunks")
    chunk_files = list(chunks_dir.glob("*.txt"))
    print(f"STAGE 1 - Chunks: {len(chunk_files)} chunk files")
    
    # 2. Count NER entities extracted
    entities_dir = Path("simple_ner_graph/entities")
    total_ner_entities = 0
    entities_by_type = {}
    
    if entities_dir.exists():
        for entity_type_dir in entities_dir.iterdir():
            if entity_type_dir.is_dir():
                entity_files = list(entity_type_dir.glob("*.json"))
                entity_count = 0
                
                for file in entity_files:
                    with open(file, 'r') as f:
                        data = json.load(f)
                        entities = data.get('entities', [])
                        entity_count += len(entities)
                
                entities_by_type[entity_type_dir.name] = entity_count
                total_ner_entities += entity_count
    
    print(f"\nSTAGE 2 - NER Extraction: {total_ner_entities} total entities")
    for entity_type, count in sorted(entities_by_type.items()):
        print(f"  {entity_type}: {count}")
    
    # 3. Count registry (taxonomy) entities
    registry_dir = Path("simple_ner_graph/registry")
    total_registry_entities = 0
    registry_by_type = {}
    
    if registry_dir.exists():
        for entity_type_dir in registry_dir.iterdir():
            if entity_type_dir.is_dir() and entity_type_dir.name != "relationships":
                entity_files = list(entity_type_dir.glob("*.json"))
                entity_count = 0
                
                for file in entity_files:
                    with open(file, 'r') as f:
                        data = json.load(f)
                        if 'entities' in data:
                            entity_count += len(data['entities'])
                        elif isinstance(data, list):
                            entity_count += len(data)
                        else:
                            # Single entity
                            entity_count += 1
                
                registry_by_type[entity_type_dir.name] = entity_count
                total_registry_entities += entity_count
    
    print(f"\nSTAGE 3 - Registry (Taxonomy): {total_registry_entities} total entities")
    for entity_type, count in sorted(registry_by_type.items()):
        print(f"  {entity_type}: {count}")
    
    # 4. Count merged entities
    merged_dir = Path("simple_ner_graph/merged/entities")
    total_merged_entities = 0
    merged_by_type = {}
    
    if merged_dir.exists():
        for entity_file in merged_dir.glob("*.json"):
            if entity_file.is_file():
                with open(entity_file, 'r') as f:
                    data = json.load(f)
                    entity_type = data.get('entity_type', entity_file.stem)
                    entity_count = len(data.get('entities', []))
                    merged_by_type[entity_type] = entity_count
                    total_merged_entities += entity_count
    
    print(f"\nSTAGE 4 - Merged (Deduplicated): {total_merged_entities} total entities")
    for entity_type, count in sorted(merged_by_type.items()):
        print(f"  {entity_type}: {count}")
    
    # Show what was lost
    print(f"\n=== ANALYSIS ===")
    print(f"Entities extracted from NER: {total_ner_entities}")
    print(f"Entities from taxonomy: {total_registry_entities}")
    print(f"Total before dedup: {total_ner_entities + total_registry_entities}")
    print(f"Total after dedup: {total_merged_entities}")
    print(f"Lost in deduplication: {(total_ner_entities + total_registry_entities) - total_merged_entities}")
    
    # Check for specific issues
    print(f"\n=== ISSUES ===")
    
    # Check if all chunks were processed
    processed_chunks = set()
    if entities_dir.exists():
        for entity_type_dir in entities_dir.iterdir():
            if entity_type_dir.is_dir():
                for file in entity_type_dir.glob("*.json"):
                    # Extract chunk ID from filename (just the hash part before underscore)
                    chunk_id = file.stem.split('_')[0]
                    processed_chunks.add(chunk_id)
    
    # Get chunk IDs from chunk files (just the hash part)
    chunk_ids = set()
    for f in chunk_files:
        chunk_id = f.stem.split('_')[0]
        chunk_ids.add(chunk_id)
    
    unprocessed = chunk_ids - processed_chunks
    if unprocessed:
        print(f"WARNING: {len(unprocessed)} chunks not processed:")
        print(f"  Processed: {len(processed_chunks)}/{len(chunk_ids)}")
        for chunk_id in sorted(unprocessed)[:10]:
            # Find the full filename for display
            for f in chunk_files:
                if f.stem.startswith(chunk_id):
                    print(f"  - {f.stem}")
                    break
    else:
        print(f"✅ All {len(chunk_files)} chunks were processed successfully!")

if __name__ == "__main__":
    trace_pipeline()
