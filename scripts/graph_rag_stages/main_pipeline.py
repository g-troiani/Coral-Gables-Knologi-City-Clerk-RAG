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
import sys
import functools
import inspect
from typing import List, Dict, Any, Callable
from dotenv import load_dotenv

# ==================================================
# COMPREHENSIVE DEBUGGING SYSTEM
# ==================================================

class PipelineDebugger:
    """Comprehensive debugging system to track all imports and function calls."""
    
    def __init__(self, logger):
        self.logger = logger
        self.imported_modules = set()
        self.called_functions = []
        self.file_usage = set()
        
    def log_import(self, module_name: str, file_path: str = None):
        """Log module imports."""
        if module_name not in self.imported_modules:
            self.imported_modules.add(module_name)
            if file_path:
                self.file_usage.add(file_path)
                self.logger.info(f"🔍 [IMPORT] Module: {module_name} from {file_path}")
            else:
                self.logger.info(f"🔍 [IMPORT] Module: {module_name}")
    
    def log_function_call(self, func_name: str, module_name: str, file_path: str = None):
        """Log function calls."""
        call_info = f"{module_name}.{func_name}"
        if call_info not in [call['name'] for call in self.called_functions]:
            call_data = {
                'name': call_info,
                'function': func_name,
                'module': module_name,
                'file': file_path
            }
            self.called_functions.append(call_data)
            if file_path:
                self.file_usage.add(file_path)
                self.logger.info(f"🎯 [CALL] Function: {func_name} in {module_name} from {file_path}")
            else:
                self.logger.info(f"🎯 [CALL] Function: {func_name} in {module_name}")
    
    def trace_function_calls(self, func: Callable) -> Callable:
        """Decorator to trace function calls."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get module and file info
            module_name = func.__module__ if hasattr(func, '__module__') else 'unknown'
            file_path = None
            try:
                file_path = inspect.getfile(func)
            except (TypeError, OSError):
                pass
            
            self.log_function_call(func.__name__, module_name, file_path)
            return func(*args, **kwargs)
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Get module and file info
            module_name = func.__module__ if hasattr(func, '__module__') else 'unknown'
            file_path = None
            try:
                file_path = inspect.getfile(func)
            except (TypeError, OSError):
                pass
            
            self.log_function_call(func.__name__, module_name, file_path)
            return await func(*args, **kwargs)
        
        return async_wrapper if inspect.iscoroutinefunction(func) else wrapper
    
    def log_file_access(self, file_path: str, operation: str = "ACCESS"):
        """Log file access operations."""
        self.file_usage.add(file_path)
        self.logger.info(f"📁 [FILE] {operation}: {file_path}")
    
    def print_summary(self):
        """Print comprehensive summary of all tracked usage."""
        self.logger.info("\n" + "="*80)
        self.logger.info("🔍 COMPREHENSIVE PIPELINE USAGE SUMMARY")
        self.logger.info("="*80)
        
        self.logger.info(f"\n📦 IMPORTED MODULES ({len(self.imported_modules)}):")
        for module in sorted(self.imported_modules):
            self.logger.info(f"  ✓ {module}")
        
        self.logger.info(f"\n🎯 CALLED FUNCTIONS ({len(self.called_functions)}):")
        for call in self.called_functions:
            if call['file']:
                self.logger.info(f"  ✓ {call['name']} from {call['file']}")
            else:
                self.logger.info(f"  ✓ {call['name']}")
        
        self.logger.info(f"\n📁 FILES USED ({len(self.file_usage)}):")
        for file_path in sorted(self.file_usage):
            self.logger.info(f"  ✓ {file_path}")
        
        self.logger.info("\n" + "="*80)

# Global debugger instance
debugger = None

def setup_import_tracking():
    """Set up import tracking by monkey-patching the import system."""
    original_import = __builtins__.__import__
    
    def tracked_import(name, globals=None, locals=None, fromlist=(), level=0):
        result = original_import(name, globals, locals, fromlist, level)
        
        if debugger and hasattr(result, '__file__') and result.__file__:
            debugger.log_import(name, result.__file__)
        elif debugger:
            debugger.log_import(name)
        
        return result
    
    __builtins__.__import__ = tracked_import

load_dotenv()  # This should be near the top of the file
nest_asyncio.apply()  # Allow nested async loops for gremlin-python

# Import using absolute paths to avoid relative import issues
script_dir = Path(__file__).parent
sys.path.append(str(script_dir))
sys.path.append(str(script_dir.parent.parent))

import phase1_preprocessing as preprocessing
from phase1_preprocessing.json_to_markdown_converter import convert_json_to_markdown
from phase3_querying.ner import UnifiedQueryEngine
from phase2_building.custom_graph_builder import CustomGraphBuilder
from scripts.graph_rag_stages.phase2_building.taxonomy_synthesizer import TaxonomySynthesizer
from scripts.graph_rag_stages.phase2_building.entity_deduplicator_extended import EntityDeduplicatorExtended
from scripts.graph_rag_stages.common.graph_entity_toolkit import GraphEntityToolkit

def setup_logging(debug_mode: bool = False):
    """Setup logging to both console and file with optional comprehensive debugging."""
    global debugger
    
    # Get project root (3 levels up from this file)
    project_root = Path(__file__).resolve().parent.parent.parent
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Generate timestamp for log file
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    debug_suffix = "_DEBUG" if debug_mode else ""
    log_file = logs_dir / f"pipeline_run_{timestamp}{debug_suffix}.md"
    
    # Create markdown header for the log file
    debug_status = " (DEBUG MODE)" if debug_mode else ""
    with open(log_file, 'w') as f:
        f.write(f"""# Pipeline Run Log{debug_status}

**Date:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  
**Log File:** `{log_file.relative_to(project_root)}`  
**Working Directory:** `{project_root}`  
**User:** `{os.getenv('USER', 'unknown')}`  
**Command:** `python -m scripts.graph_rag_stages.main_pipeline`  
**Debug Mode:** `{debug_mode}`  

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
    
    # Initialize comprehensive debugger only if debug mode is enabled
    if debug_mode:
        debugger = PipelineDebugger(logger)
        setup_import_tracking()
        
        # Log the current file being used
        debugger.log_file_access(__file__, "MAIN")
        
        print(f"📝 Logging pipeline run to: {log_file}")
        print(f"🔍 COMPREHENSIVE DEBUGGING ENABLED - tracking all imports and function calls")
    else:
        debugger = None
        print(f"📝 Logging pipeline run to: {log_file}")
        print(f"⚡ Running in normal mode (use --debug for comprehensive debugging)")
    
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

def debug_document_count(stage_name: str, directory: Path, pattern: str = "*.json", description: str = "documents") -> int:
    """Debug helper to count and log documents at each stage."""
    if not DEBUG_DOCUMENT_FLOW:
        return 0
    
    count = 0
    if directory.exists():
        if pattern == "*.json":
            # Count JSON files in all subdirectories
            for subdir in directory.iterdir():
                if subdir.is_dir():
                    json_files = list(subdir.glob("*.json"))
                    count += len(json_files)
                    if json_files:
                        log.info(f"🔍 DEBUG [{stage_name}] {subdir.name}/: {len(json_files)} JSON files")
        else:
            files = list(directory.glob(pattern))
            count = len(files)
    
    log.info(f"🔍 DEBUG [{stage_name}] TOTAL: {count} {description} in {directory}")
    return count

def debug_file_discovery(stage_name: str, directory: Path, description: str = ""):
    """Debug helper to trace file discovery issues."""
    if not DEBUG_FILE_DISCOVERY:
        return
        
    log.info(f"🔍 DEBUG [{stage_name}] FILE DISCOVERY{' - ' + description if description else ''}")
    log.info(f"🔍 DEBUG [{stage_name}] Directory: {directory}")
    log.info(f"🔍 DEBUG [{stage_name}] Exists: {directory.exists()}")
    
    if not directory.exists():
        return
    
    # List all subdirectories
    subdirs = [d for d in directory.iterdir() if d.is_dir()]
    log.info(f"🔍 DEBUG [{stage_name}] Subdirectories: {[d.name for d in subdirs]}")
    
    # Count files in each subdirectory
    for subdir in subdirs:
        json_files = list(subdir.glob("*.json"))
        md_files = list(subdir.glob("*.md"))
        log.info(f"🔍 DEBUG [{stage_name}] {subdir.name}/: {len(json_files)} JSON, {len(md_files)} MD files")
        
        if json_files and DEBUG_FILE_DISCOVERY:
            for json_file in json_files[:3]:  # Show first 3 files as examples
                log.info(f"🔍 DEBUG [{stage_name}]   Example: {json_file.name}")



def debug_stage_transition(from_stage: str, to_stage: str, document_counts: dict):
    """Debug helper for stage transitions and document flow."""
    if not DEBUG_DOCUMENT_FLOW:
        return
    
    log.info(f"🚦 DEBUG [TRANSITION] {from_stage} → {to_stage}")
    for location, count in document_counts.items():
        log.info(f"🚦 DEBUG [TRANSITION]   {location}: {count} documents")
    
    # Check for potential data loss
    if len(document_counts) > 1:
        counts = list(document_counts.values())
        max_count = max(counts)
        min_count = min(counts)
        if max_count > min_count:
            log.warning(f"🚨 DEBUG [TRANSITION] POTENTIAL DATA LOSS: {max_count - min_count} documents missing")

# --- PIPELINE CONTROL FLAGS ---
RUN_DATA_PREPROCESSING = True  # Skip - already done
RUN_CUSTOM_GRAPH_PIPELINE = True  # Build graph from extracted JSON
RUN_NER_PIPELINE = True  # Skip - already done  
PUSH_TO_VECTOR_DB = True  # Skip - already done

# --- DEBUG FLAGS ---
DEBUG_DOCUMENT_FLOW = False        # Enable detailed document flow tracing
DEBUG_RELATIONSHIP_LINKING = False # Enable relationship linking debugging
DEBUG_ENTITY_DEDUPLICATION = False # Enable entity deduplication debugging
DEBUG_FILE_DISCOVERY = False       # Enable file discovery debugging

# --- GRAPH BUILDING FLAGS ---
BUILD_COSMOS_GRAPH = True  # Enable Cosmos DB graph building



# --- SUB-COMPONENT FLAGS ---
RUN_DEDUPLICATION = False
DEDUP_CONFIG = 'conservative'



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
    global debugger
    
    # Setup logging to both console and file with optional debugging
    logger = setup_logging(debug_mode=args.debug)
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        log.info("🚀 Starting the Unified City Clerk Knowledge Graph Pipeline")
        log.info("📁 Using organized JSON structure: stage1/, stage2/, stage3/, verbatim/, legal/")
        
        # Echo effective stage plan for clarity
        log.info("\n" + "="*60)
        log.info("📋 EFFECTIVE STAGE EXECUTION PLAN:")
        log.info("="*60)
        log.info(f"Stage 1 - Data Preprocessing: {'✅ ENABLED' if RUN_DATA_PREPROCESSING else '⏭️ SKIPPED'}")
        log.info(f"Stage 2 - NER Pipeline: {'✅ ENABLED' if RUN_NER_PIPELINE else '⏭️ SKIPPED'}")
        log.info(f"Stage 3 - Custom Graph Pipeline: {'✅ ENABLED' if RUN_CUSTOM_GRAPH_PIPELINE else '⏭️ SKIPPED'}")
        log.info(f"Stage 4 - Vector DB Push: {'✅ ENABLED' if PUSH_TO_VECTOR_DB else '⏭️ SKIPPED'}")
        log.info("="*60 + "\n")
        
        if args.debug:
            log.info("🔍 DEBUGGING MODE: Tracking all imports, function calls, and file usage")
            
            # Log initial imports that are already loaded
            if debugger:
                debugger.log_import("asyncio")
                debugger.log_import("pathlib")
                debugger.log_import("logging") 
                debugger.log_import("argparse")
                debugger.log_import("nest_asyncio")
                debugger.log_import("datetime")
                debugger.log_import("os")
                debugger.log_import("re")
                debugger.log_import("json")
                debugger.log_import("sys")
                debugger.log_import("functools")
                debugger.log_import("inspect")
                debugger.log_import("dotenv")
        else:
            log.info("⚡ Running in normal mode (use --debug for comprehensive debugging)")
        
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
            
            # Debug: Initial source document count
            if base_source_dir.exists():
                source_pdfs = list(base_source_dir.glob("**/*.pdf"))
                debug_document_count("STAGE 1 INPUT", base_source_dir, "**/*.pdf", "source PDFs")
            
            # Track preprocessing module usage
            if debugger:
                debugger.log_import("phase1_preprocessing", str(preprocessing.__file__) if hasattr(preprocessing, '__file__') else None)
                debugger.log_function_call("run_extraction_pipeline", "phase1_preprocessing", str(preprocessing.__file__) if hasattr(preprocessing, '__file__') else None)
                debugger.log_file_access(str(base_source_dir), "INPUT_DIR")
                debugger.log_file_access(str(json_output_dir), "OUTPUT_DIR")
            
            await preprocessing.run_extraction_pipeline(base_source_dir, json_output_dir)
            
            # Debug: Post-extraction document count
            stage1_count = debug_document_count("STAGE 1 OUTPUT", json_output_dir, "*.json", "extracted JSON files")
            debug_file_discovery("STAGE 1 OUTPUT", json_output_dir, "Post-extraction file discovery")
            
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
            
            # Debug: Pre-conversion document count
            pre_conversion_count = debug_document_count("STAGE 1.5 INPUT", json_output_dir, "*.json", "JSON files for conversion")
            debug_file_discovery("STAGE 1.5 INPUT", json_output_dir, "Pre-conversion file discovery")
            
            # Track JSON to markdown converter usage
            if debugger:
                debugger.log_import("phase1_preprocessing.json_to_markdown_converter")
                debugger.log_function_call("convert_json_to_markdown", "phase1_preprocessing.json_to_markdown_converter")
                debugger.log_file_access(str(markdown_output_dir), "MARKDOWN_OUTPUT_DIR")
            
            converted_files = convert_json_to_markdown(json_output_dir, markdown_output_dir)
            
            # Debug: Post-conversion document count and stage transition
            post_conversion_count = len(converted_files)
            debug_document_count("STAGE 1.5 OUTPUT", markdown_output_dir, "*.md", "converted markdown files")
            debug_stage_transition("STAGE 1", "STAGE 1.5", {
                "JSON Input": pre_conversion_count,
                "Markdown Output": post_conversion_count
            })
            
            # Critical debug check for the first major loss point
            if pre_conversion_count > post_conversion_count:
                log.warning(f"🚨 CRITICAL: STAGE 1.5 DOCUMENT LOSS DETECTED!")
                log.warning(f"🚨   Input JSON files: {pre_conversion_count}")
                log.warning(f"🚨   Output MD files: {post_conversion_count}")
                log.warning(f"🚨   LOST: {pre_conversion_count - post_conversion_count} documents")
            
            log.info(f"✅ STAGE 1.5: Converted {len(converted_files)} JSON files to markdown")

        # ====================================================================
        # STAGE 2: NER Pipeline (moved earlier, was 2B)
        # ====================================================================
        if RUN_NER_PIPELINE:
            log.info("▶️ STAGE 2: NER Pipeline (Entity-based)")
            
            # Debug: Pre-NER document counts
            if markdown_output_dir.exists():
                ner_input_count = debug_document_count("STAGE 2 INPUT", markdown_output_dir, "*.md", "markdown files for NER")
            
            # Track NER pipeline usage
            if debugger:
                debugger.log_import("phase3_querying.ner")
                debugger.log_function_call("extract_phase1_entities", "__main__")
                debugger.log_file_access(str(simple_ner_output_dir), "NER_OUTPUT_DIR")
            
            # Check if markdown directory exists, if not use a fallback
            if not markdown_output_dir.exists():
                log.warning("⚠️ Markdown directory not found, using city_clerk_documents directory")
                markdown_source_dir = project_root / "city_clerk_documents"
                debug_file_discovery("STAGE 2 FALLBACK", markdown_source_dir, "Using fallback directory for NER")
            else:
                markdown_source_dir = markdown_output_dir
            
            if debugger:
                debugger.log_file_access(str(markdown_source_dir), "MARKDOWN_SOURCE_DIR")
            
            # Extract Phase 1 entities for enhanced context
            phase1_entities = extract_phase1_entities(json_output_dir)
            log.info(f"📋 Extracted {len(phase1_entities)} Phase 1 entities for context")
                
            # Initialize and run enhanced unified pipeline with Phase 1 context
            if debugger:
                debugger.log_function_call("UnifiedQueryEngine", "phase3_querying.ner")
                debugger.log_function_call("initialize_pipeline", "phase3_querying.ner.UnifiedQueryEngine")
            
            query_engine = UnifiedQueryEngine(simple_ner_output_dir)
            await query_engine.initialize_pipeline(
                markdown_source_dir=markdown_source_dir,
                chunk_size=2000,
                chunk_overlap=200,
                use_integrated_pipeline=False,
                phase1_entities=phase1_entities
            )
            log.info("✅ STAGE 2: NER pipeline completed")

        # ====================================================================
        # STAGE 3: Taxonomy Synthesis (NEW)
        # ====================================================================
        if RUN_CUSTOM_GRAPH_PIPELINE:
            log.info("▶️ STAGE 3: Taxonomy Synthesis")
            
            # Track taxonomy synthesis usage
            if debugger:
                debugger.log_import("scripts.graph_rag_stages.common.graph_entity_toolkit")
                debugger.log_import("scripts.graph_rag_stages.phase2_building.taxonomy_synthesizer")
                debugger.log_function_call("GraphEntityToolkit", "scripts.graph_rag_stages.common.graph_entity_toolkit")
                debugger.log_function_call("TaxonomySynthesizer", "scripts.graph_rag_stages.phase2_building.taxonomy_synthesizer")
            
            toolkit = GraphEntityToolkit()
            synthesizer = TaxonomySynthesizer(simple_ner_output_dir, toolkit)
            
            # Synthesize taxonomy from JSON
            if debugger:
                debugger.log_function_call("synthesize_from_json", "scripts.graph_rag_stages.phase2_building.taxonomy_synthesizer.TaxonomySynthesizer")
            
            taxonomy_stats = await synthesizer.synthesize_from_json(json_output_dir)
            log.info(f"   Synthesized: {taxonomy_stats}")
            
            # Create seed entities
            if debugger:
                debugger.log_function_call("create_seed_entities", "scripts.graph_rag_stages.phase2_building.taxonomy_synthesizer.TaxonomySynthesizer")
            
            await synthesizer.create_seed_entities()
            log.info("✅ STAGE 3: Taxonomy synthesis completed")

        # ====================================================================
        # STAGE 4: Multi-Source Deduplication (NEW - replaces old 2.5)
        # ====================================================================
        if RUN_NER_PIPELINE or RUN_CUSTOM_GRAPH_PIPELINE:
            log.info("▶️ STAGE 4: Multi-Source Entity Deduplication")
            
            # Track deduplication usage
            if debugger:
                debugger.log_import("scripts.graph_rag_stages.phase2_building.entity_deduplicator_extended")
                debugger.log_function_call("EntityDeduplicatorExtended", "scripts.graph_rag_stages.phase2_building.entity_deduplicator_extended")
            
            deduplicator = EntityDeduplicatorExtended(similarity_threshold=0.85)
            
            # Deduplicate across NER and taxonomy sources
            if debugger:
                debugger.log_function_call("deduplicate_multi_source", "scripts.graph_rag_stages.phase2_building.entity_deduplicator_extended.EntityDeduplicatorExtended")
                debugger.log_file_access(str(simple_ner_output_dir / "registry"), "TAXONOMY_ENTITIES_DIR")
            
            merge_map = await deduplicator.deduplicate_multi_source(
                simple_ner_output_dir,  # NER entities
                simple_ner_output_dir / "registry"  # Taxonomy entities
            )
            
            log.info(f"   Created merge map with {len(merge_map)} mappings")
            
            # Generate merged manifests
            if debugger:
                debugger.log_function_call("generate_merge_manifest", "scripts.graph_rag_stages.phase2_building.entity_deduplicator_extended.EntityDeduplicatorExtended")
            
            await deduplicator.generate_merge_manifest(simple_ner_output_dir)
            
            # Save merge mappings for reference
            merge_mappings_file = simple_ner_output_dir / "merge_mappings.json"
            if debugger:
                debugger.log_file_access(str(merge_mappings_file), "WRITE")
            
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
            
            # Track Cosmos DB usage
            if debugger:
                debugger.log_import("phase2_building.custom_graph_builder")
                debugger.log_function_call("CustomGraphBuilder", "phase2_building.custom_graph_builder")
            
            # Check if merged manifests exist
            merged_dir = simple_ner_output_dir / "merged"
            if debugger:
                debugger.log_file_access(str(merged_dir), "MERGED_MANIFESTS_DIR")
            
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
            if debugger:
                debugger.log_function_call("push_from_merged_manifests", "phase2_building.custom_graph_builder.CustomGraphBuilder")
            
            async with cosmos_builder.cosmos_client:
                push_stats = await cosmos_builder.push_from_merged_manifests(merged_dir)
                log.info(f"   Push statistics: {push_stats}")
            
            log.info("✅ STAGE 5: Cosmos push completed")

        # ====================================================================
        # STAGE 6: Vector Database Push (unchanged, was 2D)
        # ====================================================================
        if PUSH_TO_VECTOR_DB:
            log.info("▶️ STAGE 6: Pushing chunks to Vector Database")
            
            # Track vector DB usage
            if debugger:
                debugger.log_import("scripts.graph_rag_stages.phase2_building.vector_db_pusher")
                debugger.log_function_call("push_chunks_to_vector_db", "scripts.graph_rag_stages.phase2_building.vector_db_pusher")
            
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
            if debugger:
                debugger.log_file_access(str(chunks_dir), "CHUNKS_DIR")
            
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
        
        # Print comprehensive debugging summary
        if debugger:
            debugger.print_summary()
        
        # Finalize log file with success
        finalize_log(logger, start_time, exit_code=0)
        
    except Exception as e:
        log.error(f"❌ Pipeline failed: {e}")
        
        # Print debugging summary even on failure
        if debugger:
            log.error("🔍 Debugging summary (partial run):")
            debugger.print_summary()
        
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
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable comprehensive debugging mode - tracks all imports, function calls, and file usage'
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
