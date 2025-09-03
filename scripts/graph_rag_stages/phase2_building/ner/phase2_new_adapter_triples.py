"""
Adapter module to integrate phase2_NEW triple extraction with the main NER pipeline.
This version uses the new single-pass triple extraction instead of three-phase extraction.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import sys
import os
import time
import asyncio

# Ensure project root is on sys.path for package imports
_PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Import triple extraction functions
from scripts.graph_rag_stages.phase2_building.ner.core.simple_ner_consolidated import (
    extract_triples,
    convert_triples_to_entities_relationships,
    parse_chunk_file
)
from scripts.graph_rag_stages.phase2_building.ner.core.simple_ner_split import (
    _persist_phase2_new,
    _persist_relationships
)
from scripts.graph_rag_stages.common.document_linker import DocumentLinker
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
from scripts.graph_rag_stages.common.entity_factory import EntityFactory

log = logging.getLogger(__name__)


class Phase2NEWAdapterTriples:
    """Adapts phase2_NEW triple extraction to work with the main NER pipeline."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.entities_dir = self.output_dir / "entities"
        self.relationships_dir = self.output_dir / "relationships"
        
        # Create output directories
        self.entities_dir.mkdir(parents=True, exist_ok=True)
        self.relationships_dir.mkdir(parents=True, exist_ok=True)
        
        # Ensure triple extraction mode is enabled
        os.environ["USE_TRIPLE_EXTRACTION"] = "true"
    
    async def process_chunk(self, chunk_file: Path, phase1_entities: Optional[List[Dict]] = None) -> int:
        """
        Process a single chunk file using triple extraction.
        
        Args:
            chunk_file: Path to the chunk text file
            phase1_entities: Phase 1 entities for context (currently unused)
            
        Returns:
            Total number of entities extracted
        """
        log.info(f"🔍 [ADAPTER_TRIPLES] Starting process_chunk() for: {chunk_file.name}")
        
        try:
            # Parse chunk file
            log.info(f"📄 [ADAPTER_TRIPLES] Parsing chunk file: {chunk_file}")
            meta, text = parse_chunk_file(str(chunk_file))
            
            log.info(f"📋 [ADAPTER_TRIPLES] Parsed metadata: {meta}")
            log.info(f"📝 [ADAPTER_TRIPLES] Text length: {len(text)} characters")
            
            if not text or len(text.strip()) < 50:
                log.warning(f"⚠️ [ADAPTER_TRIPLES] Skipping chunk with insufficient text")
                return 0
            
            # Extract triples using 4 parallel calls focused on different ontology portions
            log.info(f"🔍 [ADAPTER_TRIPLES] Calling extract_triples() with 4-way parallel logic")
            log.info(f"   📄 Document type: {meta.get('documentType', 'unknown')}")
            log.info(f"   📅 Meeting date: {meta.get('meetingDate', 'unknown')}")
            log.info(f"   📁 Source file: {meta.get('sourceFileName', 'unknown')}")
            
            # Import entity groups for ontology splitting
            from scripts.graph_rag_stages.phase2_building.ner.core.simple_ner_split import (
                ENTITY_TYPE_GROUP_1, ENTITY_TYPE_GROUP_2, ENTITY_TYPE_GROUP_3, ENTITY_TYPE_GROUP_4,
                build_focused_ontology_context
            )
            
            import concurrent.futures
            
            def extract_triples_for_group(group_info):
                """Extract triples for a specific entity group."""
                group_entities, group_name, group_num = group_info
                log.info(f"🚀 [ADAPTER_TRIPLES] Starting Group {group_num} triple extraction: {group_name}")
                
                # Build focused ontology context for this group
                focused_ontology = build_focused_ontology_context(group_entities, group_name)
                
                # Create a modified chunk_meta with group info
                group_meta = meta.copy()
                group_meta['ontology_focus'] = group_name
                group_meta['entity_group'] = group_entities
                
                # Make the API call with focused ontology
                triples_data, raw_response = extract_triples(
                    text,
                    document_type=meta.get('documentType', 'unknown'),
                    meeting_date=meta.get('meetingDate', 'unknown'),
                    source_file=meta.get('sourceFileName', 'unknown'),
                    chunk_meta=group_meta,
                    ontology_override=focused_ontology
                )
                
                log.info(f"✅ [ADAPTER_TRIPLES] Group {group_num} completed: {len(triples_data.get('triples', []))} triples")
                return group_num, triples_data, raw_response
            
            # Prepare the 4 groups for parallel processing
            groups_info = [
                (ENTITY_TYPE_GROUP_1, "GROUP 1: PEOPLE & ROLES", 1),
                (ENTITY_TYPE_GROUP_2, "GROUP 2: GOVERNANCE & ACTIONS", 2), 
                (ENTITY_TYPE_GROUP_3, "GROUP 3: DOCUMENTS & POLICIES", 3),
                (ENTITY_TYPE_GROUP_4, "GROUP 4: CONTENT & RESOURCES", 4)
            ]
            
            # Execute 4 parallel triple extraction calls with enhanced performance monitoring
            log.info(f"🔥 [ADAPTER_TRIPLES] Starting 4 parallel triple extraction calls with anti-throttling delays")
            
            # Import performance monitoring from consolidated module
            from scripts.graph_rag_stages.phase2_building.ner.core.simple_ner_consolidated import PerformanceMonitor
            
            parallel_start_time = PerformanceMonitor.log_parallel_execution_start(log, 4)
            
            # Use asyncio for true async parallelism instead of blocking ThreadPoolExecutor
            loop = asyncio.get_event_loop()
            
            # Submit all tasks as async operations with timing and anti-throttling delays
            submit_start = time.time()
            tasks = []
            for i, group_info in enumerate(groups_info):
                # Add staggered delay to reduce Azure OpenAI throttling
                if i > 0:  # No delay for first call
                    delay_ms = i * 100  # 100ms, 200ms delays
                    log.info(f"   ⏱️  Adding {delay_ms}ms anti-throttling delay before Group {i+1}")
                    await asyncio.sleep(delay_ms / 1000.0)
                
                # Use run_in_executor to make the synchronous function non-blocking
                task = loop.run_in_executor(None, extract_triples_for_group, group_info)
                tasks.append(task)
            submit_time = time.time() - submit_start
            
            log.info(f"   🚀 All 3 extraction tasks submitted in {submit_time:.3f}s")
            
            # Collect results with detailed timing
            all_triples = []
            all_raw_responses = []
            group_timings = {}
            successful_groups = 0
            failed_groups = 0
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, result in enumerate(results):
                group_info = groups_info[i]
                group_start_time = time.time()
                
                try:
                    if isinstance(result, Exception):
                        raise result
                        
                    group_num, triples_data, raw_response = result
                    group_completion_time = time.time() - group_start_time
                    group_timings[f'group_{group_num}'] = group_completion_time
                    
                    # Merge triples from this group
                    group_triples = triples_data.get('triples', [])
                    all_triples.extend(group_triples)
                    all_raw_responses.append(f"=== GROUP {group_num} ===\n{raw_response}")
                    
                    successful_groups += 1
                    log.info(f"✅ [ADAPTER_TRIPLES] Group {group_num} completed in {group_completion_time:.2f}s: {len(group_triples)} triples")
                    
                except Exception as e:
                    group_name = group_info[1]
                    group_completion_time = time.time() - group_start_time
                    group_timings[group_name] = group_completion_time
                    
                    failed_groups += 1
                    log.error(f"❌ [ADAPTER_TRIPLES] Group {group_name} failed after {group_completion_time:.2f}s: {e}")
                    all_raw_responses.append(f"=== {group_name} (FAILED) ===\nError: {str(e)}")
            
            # Log parallel execution summary
            results_info = {
                'successful_groups': successful_groups,
                'failed_groups': failed_groups,
                'total_triples': len(all_triples),
                'total_response_chars': sum(len(resp) for resp in all_raw_responses),
                'group_timings': group_timings
            }
            
            PerformanceMonitor.log_parallel_execution_end(log, parallel_start_time, 3, results_info)
            
            # Combine all results
            triples_data = {"triples": all_triples}
            raw_response = "\n\n".join(all_raw_responses)
            
            log.info(f"✅ [ADAPTER_TRIPLES] extract_triples() completed")
            log.info(f"   🔗 Triples extracted: {len(triples_data.get('triples', []))}")
            
            # Convert triples to entities and relationships
            log.info(f"🔄 [ADAPTER_TRIPLES] Converting triples to entities and relationships")
            entities_by_type, relationships = convert_triples_to_entities_relationships(triples_data)
            
            # Log conversion results
            entities_summary = {entity_type: len(entities) for entity_type, entities in entities_by_type.items()}
            log.info(f"   📊 Entities by type: {entities_summary}")
            log.info(f"   🔗 Relationships: {len(relationships)}")
            
            # Create the format expected by persist functions
            entities_dict = {"entities": entities_by_type}
            all_entities = []
            for entity_type, entities in entities_by_type.items():
                for entity in entities:
                    entity['type'] = entity_type
                    all_entities.append(entity)
            
            # Persist entities
            log.info(f"💾 [ADAPTER_TRIPLES] Persisting entities")
            entity_files, entity_log = _persist_phase2_new(meta, entities_dict, raw_response, self.output_dir)
            
            # Persist relationships
            log.info(f"💾 [ADAPTER_TRIPLES] Persisting relationships")
            # Create document edges for provenance
            doc_edges = []
            if meta and meta.get('chunkId'):
                from scripts.graph_rag_stages.common.document_linker import DocumentLinker
                doc_edges = DocumentLinker.create_document_entity_relationships(all_entities, meta, meta['chunkId'])
            
            # Use correct function signature: _persist_relationships(rel_parsed, doc_edges, all_entities, meta, rel_text, output_root)
            rel_parsed = {"relationships": relationships}
            rel_log = _persist_relationships(rel_parsed, doc_edges, all_entities, meta, raw_response, self.output_dir)
            
            total_entities = sum(len(ents) for ents in entities_by_type.values())
            log.info(f"✅ [ADAPTER_TRIPLES] Persistence completed: {total_entities} entities, {len(relationships)} relationships")
            
            # Log persistence results
            if entity_log:
                log.info(f"   📊 Entity persistence stats: {entity_log.get('persisted_entities_count', 0)} persisted")
            if rel_log:
                log.info(f"   📊 Relationship persistence stats: {rel_log.get('persisted_relationships_count', 0)} persisted")
            
            return total_entities
            
        except Exception as e:
            log.error(f"❌ [ADAPTER_TRIPLES] Error processing chunk {chunk_file.name}: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def _get_all_entities(self) -> List[Dict]:
        """
        Read all entities from the entities directory.
        Returns a flat list of all entities with their type information.
        """
        all_entities = []
        
        for entity_file in self.entities_dir.glob("*.json"):
            try:
                with open(entity_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                # Handle both direct entity lists and type-bucketed format
                if isinstance(data, dict):
                    for entity_type, entities in data.items():
                        if isinstance(entities, list):
                            for entity in entities:
                                entity['type'] = entity_type
                                all_entities.append(entity)
                elif isinstance(data, list):
                    all_entities.extend(data)
                    
            except Exception as e:
                log.error(f"Error reading entity file {entity_file}: {e}")
                
        return all_entities
