"""
Query and Response Module

This module handles the query processing and response generation for the unified pipeline.
It provides interfaces to both the custom graph (Cosmos DB) and NER-based querying.

Components:
- Query router for determining the best query method
- NER-based query engine for entity-based retrieval
- Response enhancer for improving answers
- Source tracker for provenance
- City Clerk specific implementations for UI compatibility
"""

from .ner.simple_query_engine import SimpleNERQueryEngine
from .query_router import QueryRouter
from .response_enhancer import ResponseEnhancer
from .source_tracker import SourceTracker
from .city_clerk_query_engine import CityClerkQueryEngine
from .smart_query_router import SmartQueryRouter, QueryIntent

import logging
from pathlib import Path

log = logging.getLogger(__name__)

def setup_query_engine(output_dir: Path) -> SimpleNERQueryEngine:
    """
    Setup and initialize the NER query engine.
    
    Args:
        output_dir: NER working directory
        
    Returns:
        Initialized SimpleNERQueryEngine instance
    """
    log.info(f"🔧 Setting up NER query engine with root: {output_dir}")
    
    # Ensure the output directory exists
    if not output_dir.exists():
        log.warning(f"⚠️ Output directory does not exist: {output_dir}")
        log.info("Creating output directory...")
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize the query engine
    query_engine = SimpleNERQueryEngine(output_dir)
    
    # Log system statistics
    stats = query_engine.get_system_stats()
    log.info(f"📊 System ready with {stats['entities_count']} entities, "
            f"{stats['relationships_count']} relationships")
    
    return query_engine

__all__ = [
    'SimpleNERQueryEngine',
    'QueryRouter',
    'ResponseEnhancer',
    'SourceTracker',
    'CityClerkQueryEngine',
    'SmartQueryRouter',
    'QueryIntent',
    'setup_query_engine'
] 