"""
Graph Building Module

This module handles graph building approaches:
1. Custom graph building in Cosmos DB (cloud-based)
2. Local graph building with NetworkX (no cloud dependencies)

Updated to read from JSON extraction output instead of markdown.

Components:
- Custom graph builder for Cosmos DB
- Local graph builder for NetworkX
- Entity deduplication for enhanced results
"""

from .custom_graph_builder import CustomGraphBuilder
from .entity_deduplicator import EntityDeduplicator

import logging
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)



async def run_cosmos_graph_pipeline(
    json_source_dir: Path,
    cosmos_config: Optional[dict] = None
) -> None:
    """
    Run the Cosmos DB graph building pipeline.
    
    Args:
        json_source_dir: Directory containing JSON files
        cosmos_config: Optional Cosmos DB configuration
    """
    log.info("🌐 Starting Cosmos DB Graph Building Pipeline")
    
    try:
        builder = CustomGraphBuilder(cosmos_config)
        await builder.build_graph_from_json(json_source_dir)
        log.info("✅ Cosmos DB graph building completed")
    except Exception as e:
        log.error(f"❌ Cosmos DB graph building failed: {e}")
        raise

__all__ = [
    'CustomGraphBuilder',
    'EntityDeduplicator',
    'run_cosmos_graph_pipeline'
] 