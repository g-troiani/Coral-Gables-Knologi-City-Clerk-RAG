#!/usr/bin/env python3
"""Quick test of our GraphRAG query capabilities"""

import sys
from pathlib import Path
sys.path.append('scripts/graphrag_breakdown')

from query_engine import CityClerkQueryEngine

def test_queries():
    print("🔍 Testing Coral Gables GraphRAG Query System")
    print("=" * 50)
    
    # Initialize query engine
    engine = CityClerkQueryEngine(Path('graphrag_data'))
    
    # Test queries
    queries = [
        ("Tell me about park development in Coral Gables", "local"),
        ("What are the main budget priorities?", "global"),
        ("Who serves on the planning board?", "local")
    ]
    
    for i, (query, method) in enumerate(queries, 1):
        print(f"\n🎯 Query {i}: {query}")
        print(f"📊 Method: {method}")
        print("-" * 30)
        
        try:
            result = engine.query(query, method=method)
            print(f"✅ Result:\n{result[:500]}{'...' if len(result) > 500 else ''}")
        except Exception as e:
            print(f"❌ Error: {str(e)}")
        
        print("\n" + "=" * 50)

if __name__ == "__main__":
    test_queries() 