#!/usr/bin/env python3
"""Quick test of our GraphRAG query capabilities"""

import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# Import the GraphRAG components properly
from scripts.graphrag_breakdown.query_engine import CityClerkQueryEngine

def test_queries():
    print("🔍 Testing Coral Gables GraphRAG Query System")
    print("=" * 50)
    
    # Initialize query engine
    try:
        engine = CityClerkQueryEngine(Path('graphrag_data'))
        print("✅ Query engine initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize query engine: {e}")
        return
    
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