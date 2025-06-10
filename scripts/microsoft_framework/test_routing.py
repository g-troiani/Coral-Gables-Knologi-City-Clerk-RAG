#!/usr/bin/env python3
"""
Test script for the enhanced query routing with multi-entity support.
"""

import asyncio
import sys
from pathlib import Path

# Add the parent directory to sys.path to import our modules
sys.path.append(str(Path(__file__).parent.parent.parent))

from scripts.microsoft_framework.query_router import SmartQueryRouter, QueryFocus, QueryIntent
from scripts.microsoft_framework.query_engine import CityClerkQueryEngine

def test_query_routing():
    """Test the query routing logic without actually executing GraphRAG queries."""
    print("=== Testing Enhanced Query Routing ===\n")
    
    router = SmartQueryRouter()
    
    # Test cases
    test_queries = [
        "What is agenda item E-1?",
        "What are E-1 and E-2?", 
        "Compare E-1 and E-2",
        "How do E-1 and E-2 relate?",
        "Tell me about E-1",
        "E-1 details",
        "What is the relationship between E-1 and E-2?",
        "E-1 vs E-2",
        "Show me E-1, E-2, and E-3 separately"
    ]
    
    for query in test_queries:
        print(f"Query: '{query}'")
        print("-" * 50)
        
        # Get routing decision
        route_info = router.determine_query_method(query)
        
        # Display routing results
        print(f"Method: {route_info['method']}")
        print(f"Intent: {route_info['intent'].value}")
        
        params = route_info['params']
        
        # Show entity detection
        if 'entity_filter' in params:
            entity = params['entity_filter']
            print(f"Single Entity: {entity['type']} = {entity['value']}")
        elif 'multiple_entities' in params:
            entities = params['multiple_entities']
            entity_strs = [f"{e['type']}={e['value']}" for e in entities]
            print(f"Multiple Entities: {entity_strs}")
            print(f"Entity Count: {len(entities)}")
        else:
            print("No specific entities detected")
        
        # Show query focus and settings
        if 'multiple_entities' in params:
            if params.get('comparison_mode'):
                print("Query Focus: COMPARISON")
            elif params.get('aggregate_results'):
                print("Query Focus: MULTIPLE_SPECIFIC")
            elif params.get('focus_on_relationships'):
                print("Query Focus: CONTEXTUAL (relationships)")
        else:
            focus = "SPECIFIC_ENTITY" if params.get('strict_entity_focus') else "CONTEXTUAL"
            print(f"Query Focus: {focus}")
        
        print(f"Community Context: {'DISABLED' if params.get('disable_community') else 'ENABLED'}")
        print(f"Top-K Entities: {params.get('top_k_entities', 'default')}")
        
        print("\n" + "="*60 + "\n")

async def test_full_query_execution():
    """Test with actual GraphRAG execution (requires GraphRAG setup)."""
    print("=== Testing Full Query Execution ===\n")
    
    # Check if GraphRAG data exists
    graphrag_root = Path("./graphrag_data")
    if not graphrag_root.exists():
        print("GraphRAG data directory not found. Please ensure GraphRAG is set up.")
        print("Expected path: ./graphrag_data")
        return
    
    engine = CityClerkQueryEngine(graphrag_root)
    
    # Test the specific query
    query = "What is agenda item E-1?"
    print(f"Executing query: '{query}'")
    print("-" * 50)
    
    try:
        result = await engine.query(query)
        
        print("Routing Decision:")
        metadata = result['routing_metadata']
        print(f"  Method: {metadata['query_method']}")
        print(f"  Detected Intent: {metadata['detected_intent']}")
        print(f"  Entity Count: {metadata['entity_count']}")
        print(f"  Community Context: {'ENABLED' if metadata['community_context_enabled'] else 'DISABLED'}")
        
        print(f"\nQuery Type: {result['query_type']}")
        
        if 'intent_detection' in result:
            intent = result['intent_detection']
            print(f"Specific Entity Focus: {intent.get('specific_entity_focus', False)}")
            print(f"Community Disabled: {intent.get('community_disabled', False)}")
        
        print(f"\nAnswer Preview:")
        answer = result['answer'][:500] + "..." if len(result['answer']) > 500 else result['answer']
        print(answer)
        
    except Exception as e:
        print(f"Error executing query: {e}")
        print("This might be expected if GraphRAG is not properly set up.")

if __name__ == "__main__":
    print("Testing Enhanced Query Router\n")
    
    # Test routing logic
    test_query_routing()
    
    # Ask user if they want to test full execution
    test_execution = input("Test full GraphRAG execution? (y/n): ").lower().strip()
    if test_execution == 'y':
        asyncio.run(test_full_query_execution())
    else:
        print("Skipping full execution test.") 