#!/usr/bin/env python3
"""
Investigate the GraphRAG knowledge graph to debug data integrity issues.
This script reads the entities and relationships to find out what the graph knows about a specific item.
"""

import pandas as pd
from pathlib import Path

def investigate_entity(entity_name: str):
    """
    Investigate a specific entity in the GraphRAG output to find its connections.
    """
    print(f"🔍 Investigating entity: '{entity_name}'")
    print("=" * 60)
    
    # Define paths to GraphRAG output
    output_dir = Path("graphrag_data/output")
    entities_path = output_dir / "entities.parquet"
    relationships_path = output_dir / "relationships.parquet"
    
    # Check if files exist
    if not entities_path.exists() or not relationships_path.exists():
        print("❌ GraphRAG output files (entities.parquet, relationships.parquet) not found.")
        return
    
    # Load the data
    try:
        entities_df = pd.read_parquet(entities_path)
        relationships_df = pd.read_parquet(relationships_path)
        print(f"✅ Loaded {len(entities_df)} entities and {len(relationships_df)} relationships.")
    except Exception as e:
        print(f"❌ Error loading parquet files: {e}")
        return
    
    # Find the entity
    target_entity = entities_df[entities_df['title'].str.upper() == entity_name.upper()]
    
    if target_entity.empty:
        print(f"Entity '{entity_name}' not found in the knowledge graph.")
        return
    
    print(f"\n--- Entity Details for '{entity_name}' ---")
    print(target_entity.to_string())
    
    entity_id = target_entity.index[0]
    
    # Find all relationships involving this entity
    related_as_source = relationships_df[relationships_df['source'] == entity_id]
    related_as_target = relationships_df[relationships_df['target'] == entity_id]
    
    all_relations = pd.concat([related_as_source, related_as_target])
    
    if all_relations.empty:
        print(f"\n--- No relationships found for '{entity_name}' ---")
    else:
        print(f"\n--- Found {len(all_relations)} relationships for '{entity_name}' ---")
        
        # Get the names of the connected entities
        connected_entity_ids = set(all_relations['source']).union(set(all_relations['target']))
        connected_entity_ids.discard(entity_id) # Remove the entity itself
        
        connected_entities = entities_df[entities_df.index.isin(connected_entity_ids)]
        
        print("This entity is connected to:")
        for _, row in connected_entities.iterrows():
            print(f"  - {row['title']} (Type: {row['type']})")
        
        print("\nFull Relationship Details:")
        print(all_relations.to_string())

if __name__ == "__main__":
    # Investigate both E-1 and E-4 to see the difference
    investigate_entity("E-1")
    print("\n\n" + "="*80 + "\n\n")
    investigate_entity("E-4") 