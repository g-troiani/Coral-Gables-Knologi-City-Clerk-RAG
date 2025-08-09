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
import json
from typing import List, Dict
from dotenv import load_dotenv
load_dotenv()  # This should be near the top of the file
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
from phase3_querying.ner import UnifiedQueryEngine
from phase2_building.custom_graph_builder import CustomGraphBuilder

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
RUN_NER_PIPELINE = False  # NER-based pipeline with entity extraction
PUSH_TO_VECTOR_DB = True  # Enable vector database push (required for application)

# --- GRAPH BUILDING FLAGS ---
BUILD_COSMOS_GRAPH = True  # Enable Cosmos DB graph building



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

def extract_phase1_entities(json_output_dir: Path) -> List[Dict]:
    """Extract Phase 1 entities from preprocessing output for NER context."""
    phase1_entities = []
    
    try:
        # MODIFIED: Check type-based directories first, then fallback to stage dirs
        subdirs = ['agenda', 'legal', 'verbatim', 'stage2', 'stage3']
        
        for subdir in subdirs:
            subdir_path = json_output_dir / subdir
            if not subdir_path.exists():
                continue
            
            for json_file in subdir_path.glob('*.json'):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Extract entities based on directory type
                    if subdir in ['agenda', 'stage3']:
                        # Extract ontology entities
                        entities = data.get('entities', [])
                        for entity in entities:
                            if isinstance(entity, dict) and 'name' in entity:
                                phase1_entities.append({
                                    'name': entity['name'],
                                    'type': entity.get('type', 'ENTITY'),
                                    'description': entity.get('description', ''),
                                    'source': 'phase1_ontology',
                                    'source_file': json_file.name
                                })
                    
                    # Extract section entities from all types
                    if 'sections' in data:
                        for section in data['sections']:
                            if section.get('section_name'):
                                phase1_entities.append({
                                    'name': section['section_name'],
                                    'type': 'SECTION',
                                    'order': section.get('section_order'),
                                    'source': 'phase1_structure',
                                    'source_file': json_file.name
                                })
                
                except Exception as e:
                    log.debug(f"Could not extract entities from {json_file.name}: {e}")
                    continue
    
    except Exception as e:
        log.warning(f"Could not extract Phase 1 entities: {e}")
    
    return phase1_entities

def clean_redundant_jsons(json_output_dir: Path):
    """Delete redundant intermediate JSON files after final versions are created."""
    deleted_files = []
    
    # Check if we have final agenda files in new location
    agenda_dir = json_output_dir / "agenda"
    has_agenda_files = agenda_dir.exists() and any(agenda_dir.glob("agenda_*.json"))
    
    # If we have final agenda files, delete only agenda-related files from stage directories
    if has_agenda_files:
        for stage_num in ['1', '2', '3']:
            stage_dir = json_output_dir / f"stage{stage_num}"
            if stage_dir.exists():
                for file in stage_dir.glob("*agenda*.json"):
                    try:
                        file.unlink()
                        deleted_files.append(str(file.name))
                        log.debug(f"Deleted stage{stage_num} agenda file: {file.name}")
                    except Exception as e:
                        log.warning(f"Error deleting {file.name}: {e}")
                
                # Try to remove empty directory
                try:
                    stage_dir.rmdir()
                    log.info(f"Removed empty stage{stage_num} directory")
                except:
                    pass
    
    # Clean legal/verbatim intermediates
    legal_dir = json_output_dir / "legal"
    if legal_dir.exists():
        for enhanced_file in legal_dir.glob("*_enhanced_*.json"):
            stem = enhanced_file.stem.replace('_enhanced_ordinance', '').replace('_enhanced_resolution', '')
            
            # Look for corresponding stage1 file and delete it
            stage1_dir = json_output_dir / "stage1"
            if stage1_dir.exists():
                for stage1_file in stage1_dir.glob(f"{stem}*_stage1_ocr.json"):
                    try:
                        stage1_file.unlink()
                        deleted_files.append(str(stage1_file.name))
                        log.debug(f"Deleted stage1 for enhanced: {stage1_file.name}")
                    except Exception as e:
                        log.warning(f"Error deleting {stage1_file.name}: {e}")
    
    # Delete any comprehensive_legal_document files (confirmed redundant)
    for pattern in ["comprehensive_legal_document*.json", "*_collection.json"]:
        for file in json_output_dir.rglob(pattern):
            try:
                file.unlink()
                deleted_files.append(str(file.name))
                log.info(f"Deleted redundant collection file: {file.name}")
            except Exception as e:
                log.warning(f"Error deleting {file.name}: {e}")
    
    if deleted_files:
        log.info(f"🧹 Cleaned {len(deleted_files)} redundant files")
        log.debug(f"Deleted files: {', '.join(deleted_files[:10])}" + 
                 (f"... and {len(deleted_files)-10} more" if len(deleted_files) > 10 else ""))
    else:
        log.info("🧹 No redundant files to clean")

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

        if RUN_CUSTOM_GRAPH_PIPELINE and BUILD_COSMOS_GRAPH:
            log.info("▶️ STAGE 2A: Custom Graph Building from JSON")
            
            if BUILD_COSMOS_GRAPH:
                log.info("🔷 Building graph in Cosmos DB...")
                await building.run_cosmos_graph_pipeline(json_output_dir)
                
            log.info("✅ STAGE 2A: Completed")

        if RUN_NER_PIPELINE:
            log.info("▶️ STAGE 2B: NER Pipeline (Entity-based)")
            
            # Check if markdown directory exists, if not use a fallback
            if not markdown_output_dir.exists():
                log.warning("⚠️ Markdown directory not found, using city_clerk_documents directory")
                markdown_source_dir = project_root / "city_clerk_documents"
            else:
                markdown_source_dir = markdown_output_dir
            
            # Extract Phase 1 entities for enhanced context
            phase1_entities = extract_phase1_entities(json_output_dir)
            log.info(f"📋 Extracted {len(phase1_entities)} Phase 1 entities for context")
                
            # Initialize and run enhanced unified pipeline with Phase 1 context
            query_engine = UnifiedQueryEngine(simple_ner_output_dir)
            await query_engine.initialize_pipeline(
                markdown_source_dir=markdown_source_dir,
                chunk_size=2000,  # Increase from 1000
                chunk_overlap=200,  # Increase from 100
                use_integrated_pipeline=True,
                phase1_entities=phase1_entities
            )
            log.info("✅ STAGE 2B: Enhanced NER pipeline completed")

            # Add deduplication step
            log.info("▶️ STAGE 2.5: Entity Deduplication")
            try:
                from scripts.graph_rag_stages.phase2_building.entity_deduplicator import EntityDeduplicator
                
                deduplicator = EntityDeduplicator(similarity_threshold=0.85)
                dedup_stats = await deduplicator.deduplicate_extracted_entities(simple_ner_output_dir)
                log.info(f"✅ Deduplication complete: {dedup_stats}")
                
                # Apply deduplication back to files
                await deduplicator.apply_deduplication_to_ner_output(simple_ner_output_dir)
                
                # Save merge mappings for reference
                merge_mappings_file = simple_ner_output_dir / "merge_mappings.json"
                with open(merge_mappings_file, 'w') as f:
                    json.dump({
                        'mappings': deduplicator.merge_mappings,
                        'stats': dedup_stats,
                        'timestamp': datetime.now().isoformat()
                    }, f, indent=2)
                    
            except Exception as e:
                log.error(f"❌ Deduplication failed: {e}")
                log.error(f"Continuing without deduplication...")
                # Don't fail the entire pipeline

        if PUSH_TO_VECTOR_DB:
            log.info("▶️ STAGE 2D: Pushing chunks to Vector Database (REQUIRED)")
            if not RUN_NER_PIPELINE:
                log.warning("⚠️ Skipping vector push because RUN_NER_PIPELINE=False (no chunks).")
                raise ValueError("Vector database push requires NER chunks; enable RUN_NER_PIPELINE or change the push source.")
            
            # Check if Azure Search credentials are configured
            search_endpoint = os.getenv("AZURE_SEARCH_ENDPOINT", "").strip()
            search_key = os.getenv("VECTOR_DATABASE_KEY", "").strip()
            
            if not search_endpoint or not search_key:
                log.error("❌ STAGE 2D: FAILED - Azure Search credentials not configured")
                log.error("   Vector database push is REQUIRED for this application to function.")
                log.error("   Please set these environment variables:")
                log.error("   - AZURE_SEARCH_ENDPOINT: Your Azure Cognitive Search endpoint")
                log.error("   - VECTOR_DATABASE_KEY: Your Azure Cognitive Search API key")
                log.error("   ")
                log.error("   Example .env file:")
                log.error("   AZURE_SEARCH_ENDPOINT=\"https://your-search-service.search.windows.net\"")
                log.error("   VECTOR_DATABASE_KEY=\"your-api-key-here\"")
                log.error("   ")
                raise ValueError("Vector database credentials are required but not configured")
            
            # Import here to avoid import errors when credentials aren't set
            from scripts.graph_rag_stages.phase2_building.vector_db_pusher import push_chunks_to_vector_db
            
            # Get chunks directory from NER output
            chunks_dir = simple_ner_output_dir / "document_chunks"
            
            if not chunks_dir.exists() or not any(chunks_dir.iterdir()):
                log.error("❌ STAGE 2D: FAILED - No chunks found to push to vector database")
                log.error(f"   Expected chunks directory: {chunks_dir}")
                raise ValueError("No document chunks available for vector database push")
            
            try:
                uploaded_count = await push_chunks_to_vector_db(chunks_dir, simple_ner_output_dir)
                if uploaded_count == 0:
                    log.error("❌ STAGE 2D: FAILED - No chunks were successfully uploaded")
                    raise ValueError("Vector database push failed - no documents uploaded")
                log.info(f"✅ STAGE 2D: Successfully pushed {uploaded_count} chunks to vector database")
            except Exception as e:
                log.error(f"❌ STAGE 2D: FAILED - {e}")
                raise  # Always fail since vector DB is integral

        if RUN_NER_PIPELINE and BUILD_COSMOS_GRAPH:
            log.info("▶️ STAGE 2C: Adding NER data to Cosmos graph (second pass)")
            
            # Check if NER data exists
            ner_data_exists = simple_ner_output_dir.exists() and any(simple_ner_output_dir.iterdir())
            
            if ner_data_exists:
                try:
                    # Import CustomGraphBuilder correctly
                    from scripts.graph_rag_stages.phase2_building.custom_graph_builder import CustomGraphBuilder
                    
                    # Initialize Cosmos graph builder with NER output directory
                    cosmos_builder = CustomGraphBuilder(
                        cosmos_config={
                            'cosmos_endpoint': os.getenv("COSMOS_ENDPOINT"),
                            'cosmos_key': os.getenv("COSMOS_KEY"),
                            'cosmos_database': os.getenv("COSMOS_DATABASE", "cgGraph"),
                            'cosmos_container': os.getenv("COSMOS_CONTAINER", "cityClerk"),
                        },
                        ner_output_dir=simple_ner_output_dir  # Pass NER output directory
                    )
                    
                    # Use async context manager for proper client lifecycle
                    async with cosmos_builder.cosmos_client:
                        log.info(f"🔄 Processing NER data from: {simple_ner_output_dir}")
                        
                        # Build graph from NER extraction
                        await cosmos_builder.build_graph_from_ner_extraction(simple_ner_output_dir)
                        
                        log.info("✅ STAGE 2C: NER entities successfully added to Cosmos graph")
                        
                except Exception as e:
                    log.error(f"❌ STAGE 2C Failed: {e}")
                    log.error(f"Error type: {type(e).__name__}")
                    log.error(f"Error details: {str(e)}")
                    import traceback
                    log.error(f"Traceback: {traceback.format_exc()}")
                    
                    # Don't raise - allow pipeline to continue
                    if not getattr(args, 'continue_on_error', True):
                        raise
            else:
                log.warning("⚠️ STAGE 2C: NER output directory not found or empty, skipping Cosmos graph update")
        
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
            if RUN_NER_PIPELINE and RUN_CUSTOM_GRAPH_PIPELINE:
                log.info("    ↳ Includes NER entities, relationships, and outcomes (added in second pass)")
            log.info("To query the Cosmos DB graph, use the CosmosGraphClient or Azure portal")

        if RUN_NER_PIPELINE:
            log.info(f"  - NER graph: {simple_ner_output_dir}")
            log.info("To query, use: from scripts.graph_rag_stages.phase3_querying.ner import UnifiedQueryEngine")
        
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
    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        help='Continue when a stage fails'
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