"""
Main orchestrator for the unified City Clerk GraphRAG pipeline.

Updated to use JSON extraction output instead of markdown.
"""
import asyncio
from pathlib import Path
import logging
import argparse

# Import using absolute paths to avoid relative import issues
import sys
from pathlib import Path
script_dir = Path(__file__).parent
sys.path.append(str(script_dir))
sys.path.append(str(script_dir.parent.parent))

import phase1_preprocessing as preprocessing
import phase2_building as building
from phase1_preprocessing.json_to_markdown_converter import convert_json_to_markdown
import simple_ner

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# --- PIPELINE CONTROL FLAGS ---
RUN_DATA_PREPROCESSING = False  # Enable preprocessing with OCR for new documents only
RUN_CUSTOM_GRAPH_PIPELINE = True  # Build graph from extracted JSON
RUN_GRAPHRAG_INDEXING_PIPELINE = False  # Microsoft GraphRAG (requires markdown)
RUN_SIMPLE_NER_PIPELINE = False  # Simple NER-based GraphRAG with entity extraction (all components now available)

# --- GRAPH BUILDING FLAGS ---
BUILD_COSMOS_GRAPH = True  # Disable Cosmos DB graph building
BUILD_LOCAL_GRAPH = False    # Enable local graph building (NetworkX)

# --- SUB-COMPONENT FLAGS ---
FORCE_REINDEX = False
RUN_DEDUPLICATION = False
DEDUP_CONFIG = 'conservative'

async def main(args):
    """Execute the unified data pipeline based on the configured flags."""
    log.info("🚀 Starting the Unified City Clerk Knowledge Graph Pipeline")
    
    # Set up directories
    project_root = Path(__file__).resolve().parent.parent.parent
    base_source_dir = project_root / args.source_dir
    json_output_dir = project_root / "city_clerk_documents/extracted_json"  # JSON output from extraction
    markdown_output_dir = project_root / "city_clerk_documents/extracted_markdown"  # For GraphRAG only
    graphrag_input_dir = project_root / "graphrag_data"
    simple_ner_output_dir = project_root / "simple_ner_graph"  # Output for simple NER pipeline

    if RUN_DATA_PREPROCESSING:
        log.info("▶️ STAGE 1: Data Pre-processing & Extraction (3-stage pipeline)")
        await preprocessing.run_extraction_pipeline(base_source_dir, json_output_dir)
        log.info("✅ STAGE 1: Completed - JSON files saved to city_clerk_documents/extracted_json/")

    # Always convert JSON to markdown if JSON files exist (needed for GraphRAG and Simple NER)
    if (RUN_GRAPHRAG_INDEXING_PIPELINE or RUN_SIMPLE_NER_PIPELINE) and json_output_dir.exists():
        log.info("▶️ STAGE 1.5: Converting JSON to Markdown for GraphRAG and Simple NER...")
        converted_files = convert_json_to_markdown(json_output_dir, markdown_output_dir)
        log.info(f"✅ STAGE 1.5: Converted {len(converted_files)} JSON files to markdown")

    if RUN_CUSTOM_GRAPH_PIPELINE and (BUILD_COSMOS_GRAPH or BUILD_LOCAL_GRAPH):
        log.info("▶️ STAGE 2A: Custom Graph Building from JSON")
        
        if BUILD_COSMOS_GRAPH:
            log.info("🔷 Building graph in Cosmos DB...")
            await building.run_cosmos_graph_pipeline(json_output_dir)
            
        if BUILD_LOCAL_GRAPH:
            log.info("🔶 Building graph locally with NetworkX from JSON...")
            await building.run_local_graph_pipeline(json_output_dir)
            
        log.info("✅ STAGE 2A: Completed")
        
    if RUN_GRAPHRAG_INDEXING_PIPELINE:
        log.info("▶️ STAGE 2B: Building GraphRAG Index")
        
        # Check if markdown directory exists, if not use a fallback
        if not markdown_output_dir.exists():
            log.warning("⚠️ Markdown directory not found, using city_clerk_documents directory")
            markdown_source_dir = project_root / "city_clerk_documents"
        else:
            markdown_source_dir = markdown_output_dir
            
        await building.run_graphrag_indexing_pipeline(
            markdown_source_dir=markdown_source_dir,
            graphrag_input_dir=graphrag_input_dir,
            force_reindex=FORCE_REINDEX,
            run_deduplication=RUN_DEDUPLICATION,
            dedup_config_name=DEDUP_CONFIG
        )
        log.info("✅ STAGE 2B: GraphRAG indexing completed")

    if RUN_SIMPLE_NER_PIPELINE:
        log.info("▶️ STAGE 2C: Simple NER Pipeline (Entity-based GraphRAG)")
        
        # Check if markdown directory exists, if not use a fallback
        if not markdown_output_dir.exists():
            log.warning("⚠️ Markdown directory not found, using city_clerk_documents directory")
            markdown_source_dir = project_root / "city_clerk_documents"
        else:
            markdown_source_dir = markdown_output_dir
            
        await simple_ner.run_simple_ner_pipeline(
            markdown_source_dir=markdown_source_dir,
            output_dir=simple_ner_output_dir,
            chunk_size=1000,
            chunk_overlap=100
        )
        log.info("✅ STAGE 2C: Simple NER pipeline completed")
    
    log.info("🎉 Unified Pipeline Run Finished.")
    log.info("📊 Results:")
    log.info(f"  - Extracted JSON: {json_output_dir}")
    if BUILD_COSMOS_GRAPH:
        log.info(f"  - Cosmos DB graph: Azure Cosmos DB (database: {project_root.parent.parent}/graph_database)")
        log.info("To query the Cosmos DB graph, use the CosmosGraphClient or Azure portal")
    if BUILD_LOCAL_GRAPH:
        log.info(f"  - Local graph: local_graph_data/")
        log.info("To query the graph, you can load it with NetworkX from local_graph_data/city_clerk_graph.graphml")
    if RUN_GRAPHRAG_INDEXING_PIPELINE:
        log.info(f"  - GraphRAG data: {graphrag_input_dir}")
        log.info("To query GraphRAG, use the GraphRAG query interface or check the output directory")
    if RUN_SIMPLE_NER_PIPELINE:
        log.info(f"  - Simple NER graph: {simple_ner_output_dir}")
        log.info("To query Simple NER, use: from scripts.graph_rag_stages.simple_ner import SimpleNERQueryEngine")

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