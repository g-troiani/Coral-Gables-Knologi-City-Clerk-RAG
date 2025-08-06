"""
Query and Response Module

This module handles query processing and response generation.
It provides interfaces to both Cosmos DB (graph) and Azure Search (vector).

Components:
- UnifiedQueryEngine for all query processing
- Response enhancement and source tracking
- City Clerk specific implementations
"""

from .unified_query_engine import UnifiedQueryEngine
from .response_enhancer import ResponseEnhancer
from .source_tracker import SourceTracker
from .city_clerk_query_engine import CityClerkQueryEngine

import logging
from pathlib import Path

log = logging.getLogger(__name__)

def setup_query_engine(output_dir: Path) -> UnifiedQueryEngine:
    """
    Setup and initialize the unified query engine.
    
    Args:
        output_dir: NER working directory
        
    Returns:
        Initialized UnifiedQueryEngine instance
    """
    log.info(f"🔧 Setting up query engine with root: {output_dir}")
    
    if not output_dir.exists():
        log.warning(f"⚠️ Output directory does not exist: {output_dir}")
        log.info("Creating output directory...")
        output_dir.mkdir(parents=True, exist_ok=True)
    
    query_engine = UnifiedQueryEngine(output_dir)
    
    stats = query_engine.get_system_stats()
    log.info(f"📊 System ready with {stats['entities_count']} entities, "
            f"{stats['relationships_count']} relationships")
    
    return query_engine

__all__ = [
    'UnifiedQueryEngine',
    'ResponseEnhancer',
    'SourceTracker',
    'CityClerkQueryEngine',
    'setup_query_engine'
]