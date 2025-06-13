"""
Query and Response Module

This module handles the query processing and response generation for the unified pipeline.
It provides interfaces to both the custom graph (Cosmos DB) and GraphRAG index.

Components:
- Query router for determining the best query method
- Query engine for GraphRAG operations
- Response enhancer for improving answers
- Source tracker for provenance
- City Clerk specific implementations for UI compatibility
"""

from .query_engine import QueryEngine
from .query_router import QueryRouter
from .response_enhancer import ResponseEnhancer
from .source_tracker import SourceTracker

# City Clerk specific implementations (for UI compatibility)
from .city_clerk_query_engine import CityClerkQueryEngine
from .smart_query_router import SmartQueryRouter, QueryIntent

import logging
from pathlib import Path

log = logging.getLogger(__name__)

def setup_query_engine(graphrag_input_dir: Path) -> QueryEngine:
    """
    Setup and initialize the query engine.
    
    Args:
        graphrag_input_dir: GraphRAG working directory
        
    Returns:
        Initialized QueryEngine instance
    """
    log.info(f"🔧 Setting up query engine with GraphRAG root: {graphrag_input_dir}")
    
    # Ensure the GraphRAG directory exists
    if not graphrag_input_dir.exists():
        log.warning(f"⚠️ GraphRAG directory does not exist: {graphrag_input_dir}")
        log.info("Creating GraphRAG directory...")
        graphrag_input_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize the query engine
    query_engine = QueryEngine(graphrag_input_dir)
    
    # Validate that GraphRAG output exists
    if query_engine.validate_graphrag_output():
        log.info("✅ GraphRAG output validated, query engine ready")
        
        # Log system statistics
        stats = query_engine.get_system_stats()
        log.info(f"📊 System ready with {stats['entities_count']} entities, "
                f"{stats['relationships_count']} relationships, "
                f"{stats['communities_count']} communities")
    else:
        log.warning("⚠️ GraphRAG output validation failed, some features may not work")
        log.info("💡 Run the GraphRAG indexing pipeline first to generate the required output files")
    
    return query_engine

__all__ = [
    'QueryEngine',
    'QueryRouter', 
    'ResponseEnhancer',
    'SourceTracker',
    'setup_query_engine',
    # City Clerk specific classes
    'CityClerkQueryEngine',
    'SmartQueryRouter',
    'QueryIntent'
] 