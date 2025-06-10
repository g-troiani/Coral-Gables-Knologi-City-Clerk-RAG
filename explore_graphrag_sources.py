#!/usr/bin/env python3
"""
Explore GraphRAG data sources and trace entity lineage.
"""

import pandas as pd
from pathlib import Path
import sys

def explore_sources():
    """Explore the GraphRAG knowledge base to understand data sources."""
    
    graphrag_root = Path("graphrag_data")
    
    print("🔍 GraphRAG Data Source Explorer")
    print("=" * 50)
    
    # Load all the parquet files
    print("\n📊 Loading GraphRAG outputs...")
    
    entities_df = pd.read_parquet(graphrag_root / "output/entities.parquet")
    relationships_df = pd.read_parquet(graphrag_root / "output/relationships.parquet")
    docs_df = pd.read_csv(graphrag_root / "city_clerk_documents.csv")
    
    print(f"✅ Loaded {len(entities_df)} entities")
    print(f"✅ Loaded {len(relationships_df)} relationships")
    print(f"✅ Loaded {len(docs_df)} source documents")
    
    # Show entity type breakdown
    print("\n📈 Entity Types:")
    print(entities_df['type'].value_counts())
    
    # Interactive exploration
    while True:
        print("\n" + "="*50)
        print("Enter an entity to explore (e.g., E-1, 2024-01) or 'quit':")
        query = input("> ").strip()
        
        if query.lower() == 'quit':
            break
        
        # Find matching entities
        matches = entities_df[
            entities_df['title'].str.contains(query, case=False, na=False) |
            entities_df['description'].str.contains(query, case=False, na=False)
        ]
        
        if matches.empty:
            print(f"❌ No entities found matching '{query}'")
            continue
        
        print(f"\n📍 Found {len(matches)} matching entities:")
        
        for idx, entity in matches.iterrows():
            print(f"\n🏷️ Entity ID: {idx}")
            print(f"   Title: {entity['title']}")
            print(f"   Type: {entity['type']}")
            print(f"   Description: {entity['description'][:200]}...")
            
            # Find relationships
            related = relationships_df[
                (relationships_df['source'] == idx) | 
                (relationships_df['target'] == idx)
            ]
            
            if not related.empty:
                print(f"   🔗 Relationships: {len(related)}")
                for _, rel in related.head(3).iterrows():
                    print(f"      - {rel['description'][:100]}...")
            
            # Try to trace back to source document
            print("\n   📄 Possible Source Documents:")
            for _, doc in docs_df.iterrows():
                if query.upper() in str(doc['item_code']).upper() or \
                   query in str(doc['document_number']) or \
                   query.lower() in str(doc['title']).lower():
                    print(f"      - {doc['source_file']}")
                    print(f"        Type: {doc['document_type']}")
                    print(f"        Meeting: {doc['meeting_date']}")
                    break

if __name__ == "__main__":
    explore_sources() 