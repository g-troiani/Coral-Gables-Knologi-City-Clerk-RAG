#!/usr/bin/env python3
"""Verify deduplication results."""

import pandas as pd
from pathlib import Path

def verify_deduplication():
    """Check deduplication results."""
    
    original_dir = Path("graphrag_data/output")
    dedup_dir = original_dir / "deduplicated"
    
    if not dedup_dir.exists():
        print("❌ No deduplicated data found!")
        return
    
    # Load data
    orig_entities = pd.read_parquet(original_dir / "entities.parquet")
    dedup_entities = pd.read_parquet(dedup_dir / "entities.parquet")
    
    print("📊 Deduplication Results:")
    print(f"   Original entities: {len(orig_entities)}")
    print(f"   Deduplicated entities: {len(dedup_entities)}")
    print(f"   Entities merged: {len(orig_entities) - len(dedup_entities)}")
    
    # Check for aliases
    if 'aliases' in dedup_entities.columns:
        entities_with_aliases = dedup_entities[dedup_entities['aliases'].notna() & (dedup_entities['aliases'] != '')]
        print(f"\n📝 Entities with aliases: {len(entities_with_aliases)}")
        
        print("\nExample merged entities:")
        for idx, entity in entities_with_aliases.head(5).iterrows():
            print(f"\n'{entity['title']}'")
            print(f"  Aliases: {entity['aliases']}")
            if '[Also known as:' in entity.get('description', ''):
                # Extract the alias info
                desc_lines = entity['description'].split('\n')
                for line in desc_lines:
                    if '[Also known as:' in line:
                        print(f"  {line.strip()}")

if __name__ == "__main__":
    verify_deduplication() 