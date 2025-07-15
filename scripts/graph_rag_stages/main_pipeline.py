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
import re
from typing import List
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

def generate_date_variations(date_str: str) -> List[str]:
    """Generate common date format variations for matching (e.g., '01.09.2024' -> ['01.09.2024', '1.9.2024', '01.9.2024', '1.09.2024', '01-09-2024', etc.])."""
    variations = set()
    if '.' in date_str:
        parts = date_str.split('.')
        if len(parts) == 3:
            month, day, year = parts
            # With/without leading zeros
            m_vars = [month, month.lstrip('0')] if month.startswith('0') else [month]
            d_vars = [day, day.lstrip('0')] if day.startswith('0') else [day]
            for m in m_vars:
                for d in d_vars:
                    variations.add(f"{m}.{d}.{year}")
                    variations.add(f"{m}-{d}-{year}")
                    variations.add(f"{m}_{d}_{year}")
    return list(variations)

def clean_redundant_jsons(json_output_dir: Path):
    """Delete redundant intermediate JSON files after final versions are created."""
    deleted_files = []
    
    # Clean agenda intermediates (delete stage1/stage2 if stage3 exists)
    stage3_dir = json_output_dir / "stage3"
    if stage3_dir.exists():
        for stage3_file in stage3_dir.glob("*_stage3_ontology.json"):
            # Extract base stem and date part
            full_stem = stage3_file.stem.replace('_stage3_ontology', '')
            # Assume format like "Agenda DATE", split to get prefix and date
            if ' ' in full_stem:
                prefix, date_str = full_stem.rsplit(' ', 1)
            else:
                prefix = ''
                date_str = full_stem
            date_vars = generate_date_variations(date_str)
            
            # Delete corresponding stage1 and stage2 files using variations
            for stage_num, stage_suffix in [('1', 'ocr'), ('2', 'agenda')]:
                stage_dir = json_output_dir / f"stage{stage_num}"
                if stage_dir.exists():
                    for date_var in date_vars:
                        stem_var = f"{prefix} {date_var}".strip() if prefix else date_var
                        patterns = [
                            f"{stem_var}_stage{stage_num}_{stage_suffix}.json",
                            f"{stem_var}*_stage{stage_num}_{stage_suffix}.json",
                            f"{stem_var.replace(' ', '_')}_stage{stage_num}_{stage_suffix}.json",  # Handle space vs underscore
                            f"{stem_var.replace(' ', ' - ')}_stage{stage_num}_{stage_suffix}.json"  # Handle space-dash-space
                        ]
                        for pattern in patterns:
                            for prev_file in stage_dir.glob(pattern):
                                try:
                                    prev_file.unlink()
                                    deleted_files.append(str(prev_file.name))
                                    log.debug(f"Deleted redundant {prev_file.name}")
                                except Exception as e:
                                    log.warning(f"Error deleting {prev_file.name}: {e}")
    
    # Clean legal/verbatim intermediates (delete stage1 if enhanced/final exists)
    for subdir in ['legal', 'verbatim']:
        sub_dir = json_output_dir / subdir
        if sub_dir.exists():
            for final_file in sub_dir.glob("*.json"):
                # Extract the base stem before suffix
                stem = None
                if '_enhanced_' in final_file.stem:
                    stem = final_file.stem.split('_enhanced_')[0]
                elif '_verbatim_transcript' in final_file.stem:
                    stem = final_file.stem.replace('_verbatim_transcript', '')
                elif 'verbatim' in subdir:
                    continue  # Skip if no clear pattern
                
                if stem:
                    # Handle date variations in legal/verbatim stems too
                    # Assume stem ends with date like "..._01_09_2024" or with spaces/dashes
                    date_match = re.search(r'(\d{1,2}[._-]\d{1,2}[._-]\d{4})$', stem)
                    if date_match:
                        date_str = date_match.group(1).replace('_', '.').replace('-', '.')
                        date_vars = generate_date_variations(date_str)
                    else:
                        date_vars = ['']
                    
                    # Look for corresponding stage1 file with variations
                    stage1_dir = json_output_dir / "stage1"
                    if stage1_dir.exists():
                        for date_var in date_vars:
                            if date_var:
                                stem_var = re.sub(r'(\d{1,2}[._-]\d{1,2}[._-]\d{4})$', date_var.replace('.', '_'), stem)
                            else:
                                stem_var = stem
                            patterns = [
                                f"{stem_var}_stage1_ocr.json",
                                f"{stem_var}*_stage1_ocr.json",
                                f"{stem_var.replace(' ', '_')}_stage1_ocr.json",
                                f"{stem_var.replace(' ', ' - ')}_stage1_ocr.json"
                            ]
                            for pattern in patterns:
                                for stage1_file in stage1_dir.glob(pattern):
                                    try:
                                        stage1_file.unlink()
                                        deleted_files.append(str(stage1_file.name))
                                        log.debug(f"Deleted redundant {stage1_file.name}")
                                    except Exception as e:
                                        log.warning(f"Error deleting {stage1_file.name}: {e}")
    
    # Keep special ordinances at stage1 that don't have enhanced versions (already handled)
    
    if deleted_files:
        log.info(f"🧹 Cleaned {len(deleted_files)} redundant JSON files")
        log.debug(f"Deleted files: {', '.join(deleted_files[:10])}" + 
                 (f"... and {len(deleted_files)-10} more" if len(deleted_files) > 10 else ""))
    else:
        log.info("🧹 No redundant JSON files to clean")

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

        # Clean up redundant JSON files after preprocessing
        if json_output_dir.exists():
            clean_redundant_jsons(json_output_dir)

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
                chunk_size=2000,  # Increase from 1000
                chunk_overlap=200  # Increase from 100
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