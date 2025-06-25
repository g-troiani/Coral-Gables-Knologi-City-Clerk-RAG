"""
Graph Building Module

This module handles multiple graph building approaches:
1. Custom graph building in Cosmos DB (cloud-based)
2. Local graph building with NetworkX (no cloud dependencies)
3. GraphRAG indexing pipeline (Microsoft GraphRAG)

Updated to read from JSON extraction output instead of markdown.

Components:
- Custom graph builder for Cosmos DB
- Local graph builder for NetworkX
- GraphRAG adapter for data preparation
- GraphRAG indexer for Microsoft GraphRAG
- Entity deduplication for enhanced results
"""

from .custom_graph_builder import CustomGraphBuilder
from .local_graph_builder import LocalGraphBuilder
from .graphrag_adapter import GraphRAGAdapter
from .graphrag_indexer import GraphRAGIndexer
from .entity_deduplicator import EntityDeduplicator

import logging
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

async def run_local_graph_pipeline(
    json_source_dir: Path,
    output_dir: Optional[Path] = None
) -> None:
    """
    Run the local NetworkX graph building pipeline from JSON extraction output.
    
    Args:
        json_source_dir: Directory containing Stage 3 JSON files
        output_dir: Optional output directory for graph files
    """
    log.info("🔗 Starting Local Graph Building Pipeline (NetworkX)")
    
    try:
        builder = LocalGraphBuilder(output_dir)
        await builder.build_graph_from_json(json_source_dir)
        log.info("✅ Local graph building completed")
    except Exception as e:
        log.error(f"❌ Local graph building failed: {e}")
        raise

async def run_cosmos_graph_pipeline(
    json_source_dir: Path,
    cosmos_config: Optional[dict] = None
) -> None:
    """
    Run the Cosmos DB graph building pipeline from JSON extraction output.
    
    Args:
        json_source_dir: Directory containing Stage 3 JSON files
        cosmos_config: Optional Cosmos DB configuration
    """
    log.info("🔗 Starting Cosmos DB Graph Building Pipeline")
    log.warning("⚠️ Cosmos pipeline needs to be updated to read from JSON instead of markdown")
    # TODO: Update CustomGraphBuilder to read from JSON
    raise NotImplementedError("Cosmos pipeline needs JSON support")

# Keep the original function name for backward compatibility
async def run_custom_graph_pipeline(
    json_source_dir: Path,
    cosmos_config: Optional[dict] = None
) -> None:
    """
    DEPRECATED: Use run_cosmos_graph_pipeline instead.
    Kept for backward compatibility.
    """
    log.warning("⚠️ run_custom_graph_pipeline is deprecated. Use run_cosmos_graph_pipeline instead.")
    await run_cosmos_graph_pipeline(json_source_dir, cosmos_config)

# Backward compatibility for markdown-based approaches
async def run_local_graph_pipeline_from_markdown(
    markdown_source_dir: Path,
    output_dir: Optional[Path] = None
) -> None:
    """
    DEPRECATED: Run the local NetworkX graph building pipeline from markdown.
    Use run_local_graph_pipeline with JSON input instead.
    """
    log.warning("⚠️ Markdown-based graph building is deprecated. Use JSON-based approach.")
    
    try:
        builder = LocalGraphBuilder(output_dir)
        await builder.build_graph_from_markdown(markdown_source_dir)
        log.info("✅ Local graph building completed")
    except Exception as e:
        log.error(f"❌ Local graph building failed: {e}")
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
    Note: GraphRAG still requires markdown format.
    
    Args:
        markdown_source_dir: Directory containing enriched markdown files
        graphrag_input_dir: GraphRAG working directory
        force_reindex: Whether to force reindexing
        run_deduplication: Whether to run entity deduplication
        dedup_config_name: Deduplication configuration to use
    """
    log.info("📊 Starting GraphRAG Indexing Pipeline")
    
    try:
        # Step 1: Prepare data for GraphRAG (already parallelized in adapter)
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
    'LocalGraphBuilder',
    'GraphRAGAdapter',
    'GraphRAGIndexer',
    'EntityDeduplicator',
    'run_cosmos_graph_pipeline',
    'run_local_graph_pipeline',
    'run_custom_graph_pipeline',  # Deprecated but kept for compatibility
    'run_local_graph_pipeline_from_markdown',  # Deprecated but kept for compatibility
    'run_graphrag_indexing_pipeline'
] 