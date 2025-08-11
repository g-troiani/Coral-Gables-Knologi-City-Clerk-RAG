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
from scripts.graph_rag_stages.phase2_building.taxonomy_synthesizer import TaxonomySynthesizer
from scripts.graph_rag_stages.phase2_building.entity_deduplicator_extended import EntityDeduplicatorExtended
from scripts.graph_rag_stages.common.graph_entity_toolkit import GraphEntityToolkit

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
    """Close log file with final status."""
    with open(logger.log_file, 'a') as f:
        f.write(f"""
```

## Pipeline Summary

**Start Time:** {start_time}  
**End Time:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  
**Status:** {'✅ Success' if exit_code == 0 else '❌ Failed'}  
**Exit Code:** {exit_code}  


""")
    
    print(f"📋 Pipeline run logged to: {logger.log_file}")
    print(f"📊 Exit code: {exit_code}")

log = logging.getLogger(__name__)

# --- PIPELINE CONTROL FLAGS ---
RUN_DATA_PREPROCESSING = True  # Enable preprocessing with OCR for new documents only
RUN_CUSTOM_GRAPH_PIPELINE = True  # Build graph from extracted JSON
RUN_NER_PIPELINE = True  # NER-based pipeline with entity extraction
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
    entities = []
    
    if not json_output_dir.exists():
        log.warning(f"JSON output directory not found: {json_output_dir}")
        return entities
    
    # Process agenda files for structured entities
    agenda_dir = json_output_dir / "agenda"
    if agenda_dir.exists():
        agenda_files = list(agenda_dir.glob("agenda_*.json"))
        log.debug(f"Found {len(agenda_files)} agenda files for entity extraction")
        
        for agenda_file in agenda_files:
            try:
                with open(agenda_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                meeting_date = data.get('meeting_date', 'unknown')
                
                # Extract people from the agenda structure
                if 'full_text' in data:
                    full_text = data['full_text']
                    
                    # Look for commission members section
                    if 'City Commission' in full_text:
                        # Extract names after "City Commission" section
                        lines = full_text.split('\n')
                        commission_section = False
                        for line in lines:
                            if 'City Commission' in line:
                                commission_section = True
                                continue
                            if commission_section and line.strip():
                                # Stop at agenda content
                                if 'Agenda' in line or 'CALL TO ORDER' in line:
                                    break
                                # Extract person names
                                if any(title in line for title in ['Mayor', 'Commissioner', 'Vice Mayor']):
                                    name_match = re.search(r'(Mayor|Vice Mayor|Commissioner)\s+(.+)', line.strip())
                                    if name_match:
                                        title, name = name_match.groups()
                                        entities.append({
                                            'name': name.strip(),
                                            'title': title.strip(),
                                            'type': 'Person',
                                            'meeting_date': meeting_date,
                                            'source': 'agenda_structure'
                                        })
                
                # Extract agenda items as entities
                for section in data.get('sections', []):
                    section_name = section.get('section_name', '')
                    
                    # Add section as topic entity
                    entities.append({
                        'name': section_name,
                        'type': 'Topic',
                        'category': 'agenda_section',
                        'meeting_date': meeting_date,
                        'source': 'agenda_structure'
                    })
                    
                    # Process items
                    for item in section.get('items', []):
                        item_code = item.get('item_code', '')
                        title = item.get('title', '')
                        
                        if item_code and title:
                            entities.append({
                                'itemID': item_code,
                                'title': title,
                                'type': 'AgendaItem',
                                'section': section_name,
                                'meeting_date': meeting_date,
                                'source': 'agenda_structure'
                            })
                            
            except Exception as e:
                log.warning(f"Error extracting entities from {agenda_file}: {e}")
    
    # Process legal documents
    legal_dir = json_output_dir / "legal"
    if legal_dir.exists():
        legal_files = list(legal_dir.glob("*_enhanced_*.json"))
        log.debug(f"Found {len(legal_files)} legal files for entity extraction")
        
        for legal_file in legal_files:
            try:
                with open(legal_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Extract ordinance/resolution info
                if 'title' in data:
                    entities.append({
                        'title': data['title'],
                        'type': 'Policy',
                        'document_type': data.get('document_type', 'ordinance'),
                        'status': data.get('status', 'enacted'),
                        'source': 'legal_document'
                    })
                
            except Exception as e:
                log.warning(f"Error extracting entities from {legal_file}: {e}")
    
    log.info(f"Extracted {len(entities)} Phase 1 entities: {len([e for e in entities if e['type'] == 'Person'])} people, {len([e for e in entities if e['type'] == 'AgendaItem'])} agenda items, {len([e for e in entities if e['type'] == 'Topic'])} topics")
    return entities

def backfill_legal_documents_from_stage1(json_output_dir: Path):
    """Move misplaced legal documents from stage1 to legal/ directory."""
    stage1_dir = json_output_dir / "stage1"
    legal_dir = json_output_dir / "legal"
    
    if not stage1_dir.exists():
        return
        
    legal_dir.mkdir(exist_ok=True)
    
    # Find enhanced ordinance/resolution files in stage1
    enhanced_files = list(stage1_dir.glob("*_enhanced_ordinance.json")) + list(stage1_dir.glob("*_enhanced_resolution.json"))
    
    moved_count = 0
    for file in enhanced_files:
        target_file = legal_dir / file.name
        if not target_file.exists():
            file.rename(target_file)
            moved_count += 1
            log.debug(f"Moved {file.name} to legal/ directory")
    
    if moved_count > 0:
        log.info(f"🔧 Moved {moved_count} legal documents from stage1/ to legal/")

def clean_redundant_jsons(json_output_dir: Path):
    """Clean redundant JSON files to reduce storage and processing overhead."""
    
    redundant_patterns = [
        "*_stage1_*.json",     # Keep stage3 but remove stage1/stage2
        "*_stage2_*.json",
        "*_raw_text.json",     # Keep enhanced versions
        "*_extracted_text.json"
    ]
    
    deleted_files = []
    
    for pattern in redundant_patterns:
        # Search recursively in all subdirectories
        files_to_delete = list(json_output_dir.rglob(pattern))
        
        for file in files_to_delete:
            # Safety check: don't delete enhanced files
            if "_enhanced_" not in file.name and "_stage3_ontology" not in file.name:
                try:
                    file.unlink()
                    deleted_files.append(file.name)
                    log.debug(f"Deleted redundant file: {file.name}")
                except Exception as e:
                    log.warning(f"Could not delete {file}: {e}")
    
    if deleted_files:
        log.info(f"🧹 Cleaned {len(deleted_files)} redundant JSON files")
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
        json_output_dir = project_root / "city_clerk_documents/extracted_json"
        markdown_output_dir = project_root / "city_clerk_documents/extracted_markdown"
        simple_ner_output_dir = project_root / "simple_ner_graph"

        # ====================================================================
        # STAGE 1: Data Pre-processing & Extraction (unchanged)
        # ====================================================================
        if RUN_DATA_PREPROCESSING:
            log.info("▶️ STAGE 1: Data Pre-processing & Extraction (3-stage pipeline)")
            await preprocessing.run_extraction_pipeline(base_source_dir, json_output_dir)
            log.info("✅ STAGE 1: Completed - JSON files saved to organized subdirectories")

        # Fix and clean up redundant JSON files after preprocessing
        if json_output_dir.exists():
            backfill_legal_documents_from_stage1(json_output_dir)
            clean_redundant_jsons(json_output_dir)

        # ====================================================================
        # STAGE 1.5: Convert JSON to markdown (unchanged)
        # ====================================================================
        if RUN_NER_PIPELINE and json_output_dir.exists():
            log.info("▶️ STAGE 1.5: Converting JSON to Markdown for NER...")
            converted_files = convert_json_to_markdown(json_output_dir, markdown_output_dir)
            log.info(f"✅ STAGE 1.5: Converted {len(converted_files)} JSON files to markdown")

        # ====================================================================
        # STAGE 2: NER Pipeline (moved earlier, was 2B)
        # ====================================================================
        if RUN_NER_PIPELINE:
            log.info("▶️ STAGE 2: NER Pipeline (Entity-based)")
            
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
                chunk_size=2000,
                chunk_overlap=200,
                use_integrated_pipeline=True,
                phase1_entities=phase1_entities
            )
            log.info("✅ STAGE 2: NER pipeline completed")

        # ====================================================================
        # STAGE 3: Taxonomy Synthesis (NEW)
        # ====================================================================
        if RUN_CUSTOM_GRAPH_PIPELINE:
            log.info("▶️ STAGE 3: Taxonomy Synthesis")
            
            toolkit = GraphEntityToolkit()
            synthesizer = TaxonomySynthesizer(simple_ner_output_dir, toolkit)
            
            # Synthesize taxonomy from JSON
            taxonomy_stats = await synthesizer.synthesize_from_json(json_output_dir)
            log.info(f"   Synthesized: {taxonomy_stats}")
            
            # Create seed entities
            await synthesizer.create_seed_entities()
            log.info("✅ STAGE 3: Taxonomy synthesis completed")

        # ====================================================================
        # STAGE 4: Multi-Source Deduplication (NEW - replaces old 2.5)
        # ====================================================================
        if RUN_NER_PIPELINE or RUN_CUSTOM_GRAPH_PIPELINE:
            log.info("▶️ STAGE 4: Multi-Source Entity Deduplication")
            
            deduplicator = EntityDeduplicatorExtended(similarity_threshold=0.85)
            
            # Deduplicate across NER and taxonomy sources
            merge_map = await deduplicator.deduplicate_multi_source(
                simple_ner_output_dir,  # NER entities
                simple_ner_output_dir / "registry"  # Taxonomy entities
            )
            
            log.info(f"   Created merge map with {len(merge_map)} mappings")
            
            # Generate merged manifests
            await deduplicator.generate_merge_manifest(simple_ner_output_dir)
            
            # Save merge mappings for reference
            merge_mappings_file = simple_ner_output_dir / "merge_mappings.json"
            with open(merge_mappings_file, 'w') as f:
                json.dump({
                    'mappings': merge_map,
                    'stats': {'total_mappings': len(merge_map)},
                    'timestamp': datetime.now().isoformat()
                }, f, indent=2)
            
            log.info("✅ STAGE 4: Deduplication completed")

        # ====================================================================
        # STAGE 5: Unified Cosmos Push (NEW - replaces old 2A and 2C)
        # ====================================================================
        if BUILD_COSMOS_GRAPH:
            log.info("▶️ STAGE 5: Unified Cosmos DB Push")
            
            # Check if merged manifests exist
            merged_dir = simple_ner_output_dir / "merged"
            if not merged_dir.exists():
                log.error("❌ No merged manifests found. Run deduplication first.")
                raise ValueError("Merged manifests required for Cosmos push")
            
            # Initialize Cosmos builder
            cosmos_config = {
                'cosmos_endpoint': os.getenv("COSMOS_ENDPOINT"),
                'cosmos_key': os.getenv("COSMOS_KEY"),
                'cosmos_database': os.getenv("COSMOS_DATABASE", "cgGraph"),
                'cosmos_container': os.getenv("COSMOS_CONTAINER", "cityClerk"),
            }
            
            cosmos_builder = CustomGraphBuilder(cosmos_config)
            
            # Push from merged manifests (NEW METHOD)
            async with cosmos_builder.cosmos_client:
                push_stats = await cosmos_builder.push_from_merged_manifests(merged_dir)
                
                log.info(f"   Push statistics: {push_stats}")
            
            log.info("✅ STAGE 5: Cosmos push completed")

        # ====================================================================
        # STAGE 6: Vector Database Push (unchanged, was 2D)
        # ====================================================================
        if PUSH_TO_VECTOR_DB:
            log.info("▶️ STAGE 6: Pushing chunks to Vector Database")
            if not RUN_NER_PIPELINE:
                log.warning("⚠️ Skipping vector push because RUN_NER_PIPELINE=False (no chunks).")
                raise ValueError("Vector database push requires NER chunks")
            
            # Check if Azure Search credentials are configured
            search_endpoint = os.getenv("AZURE_SEARCH_ENDPOINT", "").strip()
            search_key = os.getenv("VECTOR_DATABASE_KEY", "").strip()
            
            if not search_endpoint or not search_key:
                log.error("❌ Azure Search credentials not configured")
                raise ValueError("Vector database credentials are required")
            
            from scripts.graph_rag_stages.phase2_building.vector_db_pusher import push_chunks_to_vector_db
            
            chunks_dir = simple_ner_output_dir / "document_chunks"
            
            if not chunks_dir.exists() or not any(chunks_dir.iterdir()):
                log.error("❌ No chunks found for vector database")
                raise ValueError("No document chunks available")
            
            try:
                uploaded_count = await push_chunks_to_vector_db(chunks_dir, simple_ner_output_dir)
                if uploaded_count == 0:
                    chunk_files = list(chunks_dir.glob("*.txt"))
                    if not chunk_files:
                        log.error("❌ No chunk files found to upload")
                        raise ValueError("Vector database push failed")
                    else:
                        log.info(f"✅ All {len(chunk_files)} chunks already in vector database")
                else:
                    log.info(f"✅ Successfully pushed {uploaded_count} new chunks")
            except Exception as e:
                log.error(f"❌ Vector DB push failed: {e}")
                raise

        # ====================================================================
        # Final Summary
        # ====================================================================
        log.info("🎉 Unified Pipeline Run Finished.")
        log.info("📊 Results:")
        log.info(f"  - Extracted JSON: {json_output_dir}/")
        log.info(f"    ├── agenda/ (meeting agendas)")
        log.info(f"    ├── legal/ (ordinances/resolutions)")
        log.info(f"    └── verbatim/ (transcripts)")
        
        if RUN_NER_PIPELINE or RUN_CUSTOM_GRAPH_PIPELINE:
            log.info(f"  - NER entities: {simple_ner_output_dir}/")
            log.info(f"  - Taxonomy entities: {simple_ner_output_dir}/registry/")
            log.info(f"  - Merged graph: {simple_ner_output_dir}/merged/")
        
        if BUILD_COSMOS_GRAPH:
            log.info(f"  - Cosmos DB: {cosmos_config['cosmos_database']}.{cosmos_config['cosmos_container']}")
            log.info("    ↳ Unified graph with deduplicated entities and relationships")
        
        log.info("To query: from scripts.graph_rag_stages.phase3_querying.ner import UnifiedQueryEngine")
        
        # Finalize log file with success
        finalize_log(logger, start_time, exit_code=0)
        
    except Exception as e:
        log.error(f"❌ Pipeline failed: {e}")
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
