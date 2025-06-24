"""
Main orchestrator for the unified City Clerk GraphRAG pipeline.

Updated to use JSON extraction output instead of markdown.
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
RUN_DATA_PREPROCESSING = True   # Run the 3-stage extraction pipeline
RUN_CUSTOM_GRAPH_PIPELINE = True  # Build graph from extracted JSON
RUN_GRAPHRAG_INDEXING_PIPELINE = False  # Microsoft GraphRAG (requires markdown)

# --- GRAPH BUILDING FLAGS ---
BUILD_COSMOS_GRAPH = False  # Disable Cosmos DB graph building
BUILD_LOCAL_GRAPH = True    # Enable local graph building (NetworkX)

# --- SUB-COMPONENT FLAGS ---
FORCE_REINDEX = False
RUN_DEDUPLICATION = True
DEDUP_CONFIG = 'conservative'

async def main(args):
    """Execute the unified data pipeline based on the configured flags."""
    log.info("🚀 Starting the Unified City Clerk Knowledge Graph Pipeline")
    
    # Set up directories
    project_root = Path(__file__).resolve().parent.parent.parent
    base_source_dir = project_root / args.source_dir
    json_output_dir = project_root / "extracted_json"  # JSON output from extraction
    markdown_output_dir = project_root / "city_clerk_documents/extracted_markdown"  # For GraphRAG only
    graphrag_input_dir = project_root / "graphrag_data"

    if RUN_DATA_PREPROCESSING:
        log.info("▶️ STAGE 1: Data Pre-processing & Extraction (3-stage pipeline)")
        await preprocessing.run_extraction_pipeline(base_source_dir, json_output_dir)
        log.info("✅ STAGE 1: Completed - JSON files saved to extracted_json/")

    if RUN_CUSTOM_GRAPH_PIPELINE and (BUILD_COSMOS_GRAPH or BUILD_LOCAL_GRAPH):
        log.info("▶️ STAGE 2A: Custom Graph Building from JSON")
        
        if BUILD_COSMOS_GRAPH:
            log.info("🔷 Building graph in Cosmos DB...")
            log.warning("⚠️ Cosmos DB pipeline needs to be updated for JSON input")
            # await building.run_cosmos_graph_pipeline(json_output_dir)
            
        if BUILD_LOCAL_GRAPH:
            log.info("🔶 Building graph locally with NetworkX from JSON...")
            await building.run_local_graph_pipeline(json_output_dir)
            
        log.info("✅ STAGE 2A: Completed")
        
    if RUN_GRAPHRAG_INDEXING_PIPELINE:
        log.info("▶️ STAGE 2B: Building GraphRAG Index")
        log.warning("⚠️ GraphRAG requires markdown format. You'll need to convert JSON to markdown first.")
        # Note: GraphRAG still needs markdown, so you'd need to add a conversion step here
        # await building.run_graphrag_indexing_pipeline(
        #     markdown_source_dir=markdown_output_dir,
        #     graphrag_input_dir=graphrag_input_dir,
        #     force_reindex=FORCE_REINDEX,
        #     run_deduplication=RUN_DEDUPLICATION,
        #     dedup_config_name=DEDUP_CONFIG
        # )
        log.info("⏭️ STAGE 2B: Skipped (requires markdown conversion)")
    
    log.info("🎉 Unified Pipeline Run Finished.")
    log.info("📊 Results:")
    log.info(f"  - Extracted JSON: {json_output_dir}")
    log.info(f"  - Local graph: local_graph_data/")
    log.info("To query the graph, you can load it with NetworkX from local_graph_data/city_clerk_graph.graphml")

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