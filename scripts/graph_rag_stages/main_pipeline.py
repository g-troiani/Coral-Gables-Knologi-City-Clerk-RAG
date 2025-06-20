"""
Main orchestrator for the unified City Clerk GraphRAG pipeline.

This script controls the entire workflow, from data extraction to indexing and querying,
allowing major components to be enabled or disabled via boolean flags.
"""
import asyncio
from pathlib import Path
import logging
import argparse

# Import from renamed, valid package directories
from . import phase1_preprocessing as preprocessing
from . import phase2_building as building

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# --- PIPELINE CONTROL FLAGS ---
RUN_DATA_PREPROCESSING = True
# NOTE: The custom graph pipeline (Cosmos DB) is disabled by default to focus on the
# primary GraphRAG workflow. Set to True to enable it.
RUN_CUSTOM_GRAPH_PIPELINE = False
RUN_GRAPHRAG_INDEXING_PIPELINE = True

# --- SUB-COMPONENT FLAGS ---
FORCE_REINDEX = False
RUN_DEDUPLICATION = True
DEDUP_CONFIG = 'conservative'  # Options: 'conservative', 'aggressive', 'name_focused'

async def main(args):
    """Execute the unified data pipeline based on the configured flags."""
    log.info("🚀 Starting the Unified City Clerk Knowledge Graph Pipeline")
    
    # Fix: Go up 3 levels instead of 2 to reach actual project root
    # From: scripts/graph_rag_stages/main_pipeline.py -> project root
    project_root = Path(__file__).resolve().parent.parent.parent
    base_source_dir = project_root / args.source_dir
    markdown_output_dir = project_root / "city_clerk_documents/extracted_markdown"
    graphrag_input_dir = project_root / "graphrag_data"

    if RUN_DATA_PREPROCESSING:
        log.info("▶️ STAGE 1: Data Pre-processing & Extraction")
        await preprocessing.run_extraction_pipeline(base_source_dir, markdown_output_dir)
        log.info("✅ STAGE 1: Completed")

    if RUN_CUSTOM_GRAPH_PIPELINE:
        log.warning("Custom graph building is preserved but requires a full implementation to run.")
        
    if RUN_GRAPHRAG_INDEXING_PIPELINE:
        log.info("▶️ STAGE 2: Building GraphRAG Index")
        await building.run_graphrag_indexing_pipeline(
            markdown_source_dir=markdown_output_dir,
            graphrag_input_dir=graphrag_input_dir,
            force_reindex=FORCE_REINDEX,
            run_deduplication=RUN_DEDUPLICATION,
            dedup_config_name=DEDUP_CONFIG
        )
        log.info("✅ STAGE 2: Completed")
    
    log.info("🎉 Unified Pipeline Run Finished.")
    log.info("To query the graph, start the UI: `python -m ui.query_app`")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified City Clerk GraphRAG Pipeline")
    parser.add_argument(
        '--source-dir',
        type=str,
        default="city_clerk_documents/global/City Comissions 2024",
        help="Path to the root directory containing source PDFs, relative to the project root."
    )
    args = parser.parse_args()
    asyncio.run(main(args)) 