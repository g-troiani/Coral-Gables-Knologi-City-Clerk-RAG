"""
Main orchestrator for the unified City Clerk pipeline.

Updated to use JSON extraction output instead of markdown.
"""
import asyncio
from pathlib import Path
import logging
import argparse
import nest_asyncio
from datetime import datetime
import os
nest_asyncio.apply()  # Allow nested async loops for gremlin-python

# Import using absolute paths to avoid relative import issues
import sys
from pathlib import Path
script_dir = Path(__file__).parent
sys.path.append(str(script_dir))
sys.path.append(str(script_dir.parent.parent))

import phase1_preprocessing as preprocessing
import phase2_building as building
from phase1_preprocessing.json_to_markdown_converter import convert_json_to_markdown
from phase3_querying.ner import SimpleNERQueryEngine

def setup_logging():
    """Setup logging to both console and file."""
    # Get project root (3 levels up from this file)
    project_root = Path(__file__).resolve().parent.parent.parent
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Generate timestamp for log file
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = logs_dir / f"pipeline_run_{timestamp}.md"
    
    # Create markdown header for the log file
    with open(log_file, 'w') as f:
        f.write(f"""# Pipeline Run Log

**Date:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  
**Log File:** `{log_file.relative_to(project_root)}`  
**Working Directory:** `{project_root}`  
**User:** `{os.getenv('USER', 'unknown')}`  
**Command:** `python -m scripts.graph_rag_stages.main_pipeline`  

---

## Pipeline Output

```
""")
    
    # Set up logging to both console and file
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # Store log file path for finalization
    logger.log_file = log_file
    
    print(f"📝 Logging pipeline run to: {log_file}")
    return logger

def finalize_log(logger, start_time, exit_code=0):
    """Finalize the log file with run summary."""
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = "✅ SUCCESS" if exit_code == 0 else "❌ FAILED"
    
    with open(logger.log_file, 'a') as f:
        f.write(f"""
```

---

## Run Summary

- **Start Time:** {start_time}
- **End Time:** {end_time}
- **Exit Code:** {exit_code}
- **Status:** {status}

""")
    
    print(f"📋 Pipeline run logged to: {logger.log_file}")
    print(f"📊 Exit code: {exit_code}")

log = logging.getLogger(__name__)

# --- PIPELINE CONTROL FLAGS ---
RUN_DATA_PREPROCESSING = True  # Enable preprocessing with OCR for new documents only
RUN_CUSTOM_GRAPH_PIPELINE = True  # Build graph from extracted JSON
RUN_NER_PIPELINE = True  # NER-based pipeline with entity extraction

# --- GRAPH BUILDING FLAGS ---
BUILD_COSMOS_GRAPH = True  # Enable Cosmos DB graph building
BUILD_LOCAL_GRAPH = True  # Enable local graph building (NetworkX)

# --- SUB-COMPONENT FLAGS ---
RUN_DEDUPLICATION = False
DEDUP_CONFIG = 'conservative'

async def main(args):
    """Execute the unified data pipeline based on the configured flags."""
    # Setup logging to both console and file
    logger = setup_logging()
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        log.info("🚀 Starting the Unified City Clerk Knowledge Graph Pipeline")
        log.info("📁 Using organized JSON structure: stage1/, stage2/, stage3/, verbatim/, legal/")
        
        # Set up directories
        project_root = Path(__file__).resolve().parent.parent.parent
        base_source_dir = project_root / args.source_dir
        json_output_dir = project_root / "city_clerk_documents/extracted_json"  # JSON output from extraction
        markdown_output_dir = project_root / "city_clerk_documents/extracted_markdown"  # For NER pipeline
        simple_ner_output_dir = project_root / "simple_ner_graph"  # Output for NER pipeline

        if RUN_DATA_PREPROCESSING:
            log.info("▶️ STAGE 1: Data Pre-processing & Extraction (3-stage pipeline)")
            await preprocessing.run_extraction_pipeline(base_source_dir, json_output_dir)
            log.info("✅ STAGE 1: Completed - JSON files saved to organized subdirectories in city_clerk_documents/extracted_json/")

        # Convert JSON to markdown if JSON files exist (needed for NER)
        if RUN_NER_PIPELINE and json_output_dir.exists():
            log.info("▶️ STAGE 1.5: Converting JSON to Markdown for NER...")
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

        if RUN_NER_PIPELINE:
            log.info("▶️ STAGE 2B: NER Pipeline (Entity-based)")
            
            # Check if markdown directory exists, if not use a fallback
            if not markdown_output_dir.exists():
                log.warning("⚠️ Markdown directory not found, using city_clerk_documents directory")
                markdown_source_dir = project_root / "city_clerk_documents"
            else:
                markdown_source_dir = markdown_output_dir
                
            # Initialize and run NER pipeline
            query_engine = SimpleNERQueryEngine(simple_ner_output_dir)
            await query_engine.initialize_pipeline(
                markdown_source_dir=markdown_source_dir,
                chunk_size=1000,
                chunk_overlap=100
            )
            log.info("✅ STAGE 2B: NER pipeline completed")
        
        log.info("🎉 Unified Pipeline Run Finished.")
        log.info("📊 Results:")
        log.info(f"  - Extracted JSON (organized): {json_output_dir}/")
        log.info(f"    ├── stage1/ (OCR extraction)")
        log.info(f"    ├── stage2/ (agenda structure)")
        log.info(f"    ├── stage3/ (ontology enhancement)")
        log.info(f"    ├── verbatim/ (transcript processing)")
        log.info(f"    └── legal/ (enhanced legal documents)")
        if BUILD_COSMOS_GRAPH:
            log.info(f"  - Cosmos DB graph: Azure Cosmos DB (database: {project_root.parent.parent}/graph_database)")
            log.info("To query the Cosmos DB graph, use the CosmosGraphClient or Azure portal")
        if BUILD_LOCAL_GRAPH:
            log.info(f"  - Local graph: local_graph_data/")
            log.info("To query the graph, you can load it with NetworkX from local_graph_data/city_clerk_graph.graphml")
        if RUN_NER_PIPELINE:
            log.info(f"  - NER graph: {simple_ner_output_dir}")
            log.info("To query NER, use: from scripts.graph_rag_stages.phase3_querying.ner import SimpleNERQueryEngine")
        
        # Finalize log file with success
        finalize_log(logger, start_time, exit_code=0)
        
    except Exception as e:
        log.error(f"❌ Pipeline failed: {e}")
        # Finalize log file with error
        finalize_log(logger, start_time, exit_code=1)
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified City Clerk Pipeline")
    parser.add_argument(
        '--source-dir',
        type=str,
        default="city_clerk_documents/global/City Comissions 2024",
        help="Path to the root directory containing source PDFs, relative to the project root."
    )
    args = parser.parse_args()
    
    try:
        asyncio.run(main(args))
    except Exception as e:
        # If main() fails, we need to finalize the log with error status
        # We'll create a minimal logger just for this case
        logs_dir = Path(__file__).resolve().parent.parent.parent / "logs"
        log_files = list(logs_dir.glob("pipeline_run_*.md"))
        if log_files:
            # Get the most recent log file
            latest_log = max(log_files, key=lambda f: f.stat().st_mtime)
            
            # Create a minimal logger with the existing log file
            logger = logging.getLogger()
            logger.log_file = latest_log
            
            # Finalize with error
            finalize_log(logger, "unknown", exit_code=1)
        
        print(f"❌ Pipeline failed: {e}")
        raise 