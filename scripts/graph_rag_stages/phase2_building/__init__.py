"""
Graph Building Module

This module handles two parallel graph building approaches:
1. Custom graph building in Cosmos DB (from original graph_stages)
2. GraphRAG indexing pipeline (from original microsoft_framework)

Components:
- Custom graph builder for Cosmos DB
- GraphRAG adapter for data preparation
- GraphRAG indexer for Microsoft GraphRAG
- Entity deduplication for enhanced results
"""

from .custom_graph_builder import CustomGraphBuilder
from .graphrag_adapter import GraphRAGAdapter
from .graphrag_indexer import GraphRAGIndexer
from .entity_deduplicator import EntityDeduplicator

import logging
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

async def run_custom_graph_pipeline(
    markdown_source_dir: Path,
    cosmos_config: Optional[dict] = None
) -> None:
    """
    Run the custom graph building pipeline to populate Cosmos DB.
    
    Args:
        markdown_source_dir: Directory containing enriched markdown files
        cosmos_config: Optional Cosmos DB configuration
    """
    log.info("🔗 Starting Custom Graph Building Pipeline")
    
    try:
        builder = CustomGraphBuilder(cosmos_config)
        await builder.build_graph_from_markdown(markdown_source_dir)
        log.info("✅ Custom graph building completed")
    except Exception as e:
        log.error(f"❌ Custom graph building failed: {e}")
        raise

async def run_graphrag_indexing_pipeline(
    markdown_source_dir: Path,
    graphrag_input_dir: Path,
    force_reindex: bool = False,
    run_deduplication: bool = True,
    dedup_config_name: str = 'conservative'
) -> None:
    """
    Run the GraphRAG indexing pipeline.
    
    Args:
        markdown_source_dir: Directory containing enriched markdown files
        graphrag_input_dir: GraphRAG working directory
        force_reindex: Whether to force reindexing
        run_deduplication: Whether to run entity deduplication
        dedup_config_name: Deduplication configuration to use
    """
    log.info("📊 Starting GraphRAG Indexing Pipeline")
    
    try:
        # Step 1: Prepare data for GraphRAG
        log.info("📋 Step 1: Preparing data for GraphRAG...")
        adapter = GraphRAGAdapter()
        csv_path = adapter.create_graphrag_input_csv(markdown_source_dir, graphrag_input_dir)
        
        if csv_path is None:
            raise RuntimeError("Failed to create GraphRAG input CSV")
        
        # Validate the input data
        if not adapter.validate_input_data(csv_path):
            raise RuntimeError("GraphRAG input data validation failed")
        
        # Create GraphRAG settings if they don't exist
        settings_file = graphrag_input_dir / "settings.yaml"
        if not settings_file.exists():
            log.info("📝 Creating GraphRAG settings file...")
            adapter.create_graphrag_settings(graphrag_input_dir)
        
        # Step 2: Run GraphRAG indexing
        log.info("⚙️ Step 2: Running GraphRAG indexing...")
        indexer = GraphRAGIndexer()
        indexer.run_indexing_process(graphrag_input_dir, verbose=True, force=force_reindex)
        
        # Step 3: Entity deduplication (optional)
        if run_deduplication:
            log.info("🔄 Step 3: Running entity deduplication...")
            deduplicator = EntityDeduplicator(graphrag_input_dir)
            await deduplicator.run_deduplication(dedup_config_name)
        else:
            log.info("⏭️ Skipping entity deduplication")
        
        log.info("✅ GraphRAG indexing pipeline completed")
        
        # Log final statistics
        indexer_stats = indexer.check_status(graphrag_input_dir)
        log.info(f"📊 Final stats: {indexer_stats['entities_count']} entities, "
                f"{indexer_stats['relationships_count']} relationships, "
                f"{indexer_stats['communities_count']} communities")
        
    except Exception as e:
        log.error(f"❌ GraphRAG indexing pipeline failed: {e}")
        raise

__all__ = [
    'CustomGraphBuilder',
    'GraphRAGAdapter',
    'GraphRAGIndexer',
    'EntityDeduplicator',
    'run_custom_graph_pipeline',
    'run_graphrag_indexing_pipeline'
] 