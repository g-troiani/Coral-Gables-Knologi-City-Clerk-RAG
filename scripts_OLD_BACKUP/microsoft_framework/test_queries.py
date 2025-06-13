#!/usr/bin/env python3
"""
Standalone query testing script for GraphRAG.

Use this after running the main pipeline to test queries interactively.
"""

import asyncio
import sys
from pathlib import Path

# Find the venv Python
def get_venv_python():
    venv_paths = [
        Path(__file__).parent.parent.parent / "venv/bin/python3",
        Path(__file__).parent.parent.parent / "venv/bin/python",
    ]
    for p in venv_paths:
        if p.exists():
            return str(p)
    return sys.executable

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from scripts.microsoft_framework import (
    CityClerkQueryEngine,
    SmartQueryRouter,
    QueryIntent
)

async def interactive_query_session():
    """Run an interactive query session."""
    print("🔍 GraphRAG Interactive Query Session")
    print("=" * 40)
    
    # Check if GraphRAG data exists
    graphrag_root = project_root / "graphrag_data"
    if not graphrag_root.exists():
        print("❌ GraphRAG data directory not found.")
        print("Please run the main pipeline first with:")
        print("python3 scripts/microsoft_framework/run_graphrag_pipeline.py")
        return
    
    # Initialize query engine and router
    query_engine = CityClerkQueryEngine(graphrag_root)
    router = SmartQueryRouter()
    
    print(f"📁 Using GraphRAG data from: {graphrag_root}")
    print("\nEnter your queries (type 'quit', 'exit', or Ctrl+C to stop)")
    print("=" * 40)
    
    # Example queries to show user
    examples = [
        "Who is Commissioner Smith?",
        "What are the main themes in city development?", 
        "How has the waterfront project evolved?",
        "Tell me about ordinance 2024-01",
        "What are the overall budget trends?"
    ]
    
    print("💡 Example queries you can try:")
    for i, example in enumerate(examples, 1):
        print(f"   {i}. {example}")
    print()
    
    while True:
        try:
            # Get user input
            query = input("🔍 Your query: ").strip()
            
            if query.lower() in ['quit', 'exit', 'q']:
                print("👋 Goodbye!")
                break
                
            if not query:
                continue
            
            # Show routing decision
            route_info = router.determine_query_method(query)
            print(f"🎯 Auto-selected method: {route_info['method']} ({route_info['intent'].value})")
            
            # Execute query
            print("⏳ Processing query...")
            result = await query_engine.query(query)
            
            # Display results
            print("\n📝 Answer:")
            print("-" * 20)
            print(result['answer'])
            print("-" * 20)
            print(f"🔧 Method used: {result['query_type']}")
            print(f"⚙️ Parameters: {result['parameters']}")
            print()
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            print()

async def run_example_queries():
    """Run a set of example queries to demonstrate functionality."""
    print("🔍 Running Example Queries")
    print("=" * 30)
    
    graphrag_root = project_root / "graphrag_data"
    if not graphrag_root.exists():
        print("❌ GraphRAG data directory not found.")
        return
    
    query_engine = CityClerkQueryEngine(graphrag_root)
    router = SmartQueryRouter()
    
    examples = [
        ("Who is Commissioner Smith?", "Entity-specific query"),
        ("What are the main development themes?", "Holistic query"),
        ("How has the waterfront project evolved?", "Temporal query"),
        ("Tell me about agenda item E-1", "Entity-specific query"),
        ("Overall budget trends in the city", "Holistic query")
    ]
    
    for query, description in examples:
        print(f"\n❓ Query: {query}")
        print(f"📋 Type: {description}")
        
        # Show routing
        route_info = router.determine_query_method(query)
        print(f"🎯 Method: {route_info['method']} ({route_info['intent'].value})")
        
        try:
            result = await query_engine.query(query)
            print(f"📝 Answer: {result['answer'][:150]}...")
        except Exception as e:
            print(f"❌ Error: {e}")

def show_query_help():
    """Show help for query types and patterns."""
    print("""
🔍 GraphRAG Query Guide

QUERY TYPES:
1. 🎯 LOCAL SEARCH - For specific entities
   - "Who is [person]?"
   - "What is [specific thing]?"
   - "Tell me about [entity]"
   - "Ordinance/Resolution [number]"
   
2. 🌐 GLOBAL SEARCH - For broad themes
   - "What are the main themes in [topic]?"
   - "Summarize [broad topic]"
   - "Overall [topic] trends"
   - "Patterns across [domain]"
   
3. 🔄 DRIFT SEARCH - For temporal/complex queries
   - "How has [thing] changed/evolved?"
   - "Timeline of [events]"
   - "History of [topic]"
   - "Development of [project] over time"

TIPS:
- Be specific for better results
- Use proper names and agenda item codes when known
- Ask follow-up questions to drill down into topics
- Try different phrasings if you don't get good results
    """)

async def main():
    """Main function."""
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg in ['-h', '--help', 'help']:
            show_query_help()
        elif arg in ['-e', '--examples', 'examples']:
            await run_example_queries()
        else:
            print(f"Unknown argument: {arg}")
            print("Use -h for help or -e for examples")
    else:
        await interactive_query_session()

if __name__ == "__main__":
    asyncio.run(main()) 