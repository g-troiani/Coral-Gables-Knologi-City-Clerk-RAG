"""
Integration module for RAG web app with enhanced debugging.
"""

import asyncio
from pathlib import Path
from typing import Dict, Any, List
from scripts.graph_rag_stages.phase3_querying.debug_query_engine import DebugQueryEngine

# Global query engine instance
_query_engine = None

def initialize_query_engine(debug_mode: bool = True) -> DebugQueryEngine:
    """Initialize the query engine with debugging."""
    global _query_engine
    
    if _query_engine is None:
        print("🚀 Initializing Debug Query Engine...")
        _query_engine = DebugQueryEngine(
            graph_dir=Path("simple_ner_graph"),
            enable_debug=debug_mode
        )
        print("✅ Query Engine initialized successfully")
        
        # Print system stats
        stats = _query_engine.get_debug_stats()
        print(f"📊 System loaded with {stats['entities_count']} entities")
    
    return _query_engine

async def search_with_debug(question: str) -> Dict[str, Any]:
    """
    Search function that returns webapp-compatible results with 'doc' key.
    """
    engine = initialize_query_engine()
    
    # Execute query with full debugging
    result = await engine.query(question)
    
    # Ensure chunks have 'doc' key
    for chunk in result.get('chunks', []):
        if 'doc' not in chunk:
            chunk['doc'] = {
                'title': 'Knowledge Graph Result',
                'source': 'GraphRAG System'
            }
    
    return result

def search_sync(question: str) -> Dict[str, Any]:
    """Synchronous wrapper for async search."""
    return asyncio.run(search_with_debug(question))