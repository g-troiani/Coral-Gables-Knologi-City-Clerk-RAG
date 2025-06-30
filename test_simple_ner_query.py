#!/usr/bin/env python3
"""
Test script for SimpleNERQueryEngine
"""

import asyncio
import sys
from pathlib import Path
import logging

# Add the correct path for the simple_ner module
sys.path.append('scripts/graph_rag_stages')

# Import from the simple_ner module
from simple_ner.simple_query_engine import SimpleNERQueryEngine

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

async def test_query():
    """Test the SimpleNERQueryEngine with a query."""
    log.info("Initializing SimpleNERQueryEngine...")
    
    # Initialize with the correct graph directory
    engine = SimpleNERQueryEngine("simple_ner_graph")
    
    log.info("Running query: 'What is agenda item E-1?'")
    result = await engine.query("What is agenda item E-1?")
    
    print("\n" + "="*50)
    print("QUERY RESULT")
    print("="*50)
    print(f"Answer: {result.get('answer', 'No answer provided')}")
    print(f"\nChunks Retrieved: {result.get('chunks_retrieved', 0)}")
    print(f"Retrieval Method: {result.get('retrieval_method', 'unknown')}")
    
    if result.get('sources'):
        print(f"\nSources ({len(result['sources'])}):")
        for i, source in enumerate(result['sources'][:3]):  # Show top 3 sources
            print(f"  {i+1}. {source.get('document', 'Unknown')} (Score: {source.get('relevance_score', 0):.3f})")
    
    print("="*50)
    
    return result

if __name__ == "__main__":
    asyncio.run(test_query()) 