"""
Phase2_NEW based extractor that replaces the three-pass extractor.
Uses the simpler phase2_NEW extraction logic while maintaining compatibility
with the main pipeline's expected interfaces and output formats.
"""

import logging
from pathlib import Path
from typing import List, Dict, Optional
import asyncio
import os
import time
import json

# Direct imports from core - no more adapter layer
from scripts.graph_rag_stages.phase2_building.ner.core.simple_ner_consolidated import (
    extract_triples,
    convert_triples_to_entities_relationships,
    parse_chunk_file
)
from scripts.graph_rag_stages.phase2_building.ner.core.simple_ner_split import (
    extract_entities_split,
    extract_relationships,
    _persist_phase2_new,
    _persist_relationships,
    ENTITY_TYPE_GROUP_1, ENTITY_TYPE_GROUP_2, ENTITY_TYPE_GROUP_3,
    build_focused_ontology_context
)

log = logging.getLogger(__name__)


class Phase2NEWExtractor:
    """
    Drop-in replacement for ThreePassExtractor using phase2_NEW logic.
    Maintains the same interface but uses the simpler extraction approach.
    """
    
    def __init__(self, output_dir: Path, use_triple_extraction: bool = True):
        """
        Initialize the extractor.
        
        Args:
            output_dir: Root directory for NER outputs (e.g., simple_ner_graph/)
            use_triple_extraction: If True, use single-call triple extraction (faster).
                                 If False, use legacy 3-phase extraction.
        """
        self.output_dir = Path(output_dir)
        self.chunks_dir = self.output_dir / "document_chunks"
        self.use_triple_extraction = use_triple_extraction
        
        # Set extraction mode
        if use_triple_extraction:
            log.info("🔥 [NER PIPELINE] Using TRIPLE EXTRACTION (single API call - much faster!)")
            os.environ["USE_TRIPLE_EXTRACTION"] = "true"
        else:
            log.info("⚙️ [NER PIPELINE] Using legacy 3-phase extraction (entities → relationships → attributes)")
        
        # Create necessary directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.entities_dir = self.output_dir / "entities"
        self.relationships_dir = self.output_dir / "relationships"
        self.entities_dir.mkdir(parents=True, exist_ok=True)
        self.relationships_dir.mkdir(parents=True, exist_ok=True)
    
    async def run_all(self, phase1_entities: Optional[List[Dict]] = None) -> int:
        """
        Process all chunks in the chunks directory.
        
        Args:
            phase1_entities: Phase 1 entities for context (passed through to adapter)
            
        Returns:
            Total number of entities extracted
        """
        log.info("🔍 [NER PIPELINE] Starting Phase2NEWExtractor.run_all()")
        log.info(f"   📁 Output directory: {self.output_dir}")
        log.info(f"   📁 Chunks directory: {self.chunks_dir}")
        log.info(f"   📋 Phase1 entities provided: {len(phase1_entities) if phase1_entities else 0}")
        
        if not self.chunks_dir.exists():
            log.error(f"❌ [NER PIPELINE] Chunks directory not found: {self.chunks_dir}")
            return 0
        
        chunk_files = list(self.chunks_dir.glob("*.txt"))
        if not chunk_files:
            log.warning(f"⚠️ [NER PIPELINE] No chunk files found in {self.chunks_dir}")
            return 0
        
        log.info(f"📄 [NER PIPELINE] Processing {len(chunk_files)} chunks with Phase2_NEW extractor")
        log.info(f"   📝 Sample chunk files: {[f.name for f in chunk_files[:3]]}")
        if len(chunk_files) > 3:
            log.info(f"   ... and {len(chunk_files) - 3} more chunks")
        
        # Log initial state of output directories
        entities_dir = self.output_dir / "entities"
        relationships_dir = self.output_dir / "relationships"
        log.info(f"   📁 Entities directory exists: {entities_dir.exists()}")
        log.info(f"   📁 Relationships directory exists: {relationships_dir.exists()}")
        
        # Process chunks with parallel execution
        total_entities = 0
        total_relationships = 0
        
        # Determine optimal batch size based on extraction mode - PERFORMANCE OPTIMIZED
        if self.use_triple_extraction:
            batch_size = 12  # Single API call mode - higher throughput
            log.info(f"🔥 [NER PIPELINE] Using TRIPLE extraction mode - PERFORMANCE OPTIMIZED batch size: {batch_size}")
        else:
            batch_size = 10  # Legacy 3-phase mode - more conservative
            log.info(f"⚙️ [NER PIPELINE] Using legacy 3-phase mode - PERFORMANCE OPTIMIZED batch size: {batch_size}")
        
        # Parallel batch processing optimization - ENHANCED FOR PERFORMANCE
        MAX_CONCURRENT_BATCHES = 6  # Increased from 3 to 6 for better throughput
        
        # Create batches
        batches = []
        for i in range(0, len(chunk_files), batch_size):
            batch = chunk_files[i:i + batch_size]
            batch_num = i // batch_size + 1
            batches.append((batch, batch_num))
        
        total_batches = len(batches)
        log.info(f"🚀 [NER PIPELINE] Processing {total_batches} batches with max {MAX_CONCURRENT_BATCHES} concurrent batches")
        
        # Semaphore to control concurrent batches
        batch_semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)
        
        async def process_single_batch(batch_data):
            """Process a single batch with concurrency control."""
            batch, batch_num = batch_data
            
            async with batch_semaphore:
                log.info(f"📦 [NER PIPELINE] Processing batch {batch_num}/{total_batches} ({len(batch)} chunks)...")
                log.info(f"   📝 Batch files: {[f.name for f in batch]}")
                
                # Process batch with parallel chunk processing - DIRECT CORE CALLS
                tasks = []
                for chunk_file in batch:
                    task = self._process_chunk_direct(chunk_file, phase1_entities)
                    tasks.append(task)
                
                # Wait for batch to complete
                log.info(f"⏳ [NER PIPELINE] Executing batch {batch_num} tasks in parallel...")
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Count successful extractions
                batch_entities = 0
                batch_failures = 0
                for j, result in enumerate(results):
                    if isinstance(result, Exception):
                        log.error(f"❌ [NER PIPELINE] Failed to process {batch[j].name}: {result}")
                        batch_failures += 1
                    else:
                        batch_entities += result
                        log.info(f"  ✅ [NER PIPELINE] {batch[j].name}: {result} entities extracted")
                
                log.info(f"📊 [NER PIPELINE] Batch {batch_num} summary:")
                log.info(f"   ✅ Successful: {len(batch) - batch_failures} chunks")
                log.info(f"   ❌ Failed: {batch_failures} chunks")
                log.info(f"   📈 Entities extracted: {batch_entities}")
                
                return batch_entities, batch_failures, len(batch)
        
        # Process all batches in parallel (with concurrency control)
        log.info(f"⚡ [NER PIPELINE] Starting parallel batch processing...")
        batch_tasks = [process_single_batch(batch_data) for batch_data in batches]
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        # Aggregate results
        total_failures = 0
        processed_chunks = 0
        for i, result in enumerate(batch_results):
            if isinstance(result, Exception):
                log.error(f"❌ [NER PIPELINE] Batch {i+1} failed completely: {result}")
                total_failures += len(batches[i][0])  # Count all chunks in failed batch
            else:
                batch_entities, batch_failures, chunk_count = result
                total_entities += batch_entities
                total_failures += batch_failures
                processed_chunks += chunk_count
        
        # Progress summary
        log.info(f"📊 [NER PIPELINE] Parallel batch processing complete:")
        log.info(f"   📦 Batches processed: {len(batches)}")
        log.info(f"   ✅ Successful chunks: {processed_chunks - total_failures}")
        log.info(f"   ❌ Failed chunks: {total_failures}")
        log.info(f"   📈 Total entities extracted: {total_entities}")
        
        # Final statistics
        log.info(f"📊 [NER PIPELINE] Phase2_NEW extraction complete - Final Statistics:")
        log.info(f"   📝 Total chunks processed: {len(chunk_files)}")
        log.info(f"   📈 Total entities extracted: {total_entities}")
        log.info(f"   📁 Output written to: {self.output_dir}")
        
        # Log output directory contents
        if entities_dir.exists():
            entity_types = [d.name for d in entities_dir.iterdir() if d.is_dir()]
            log.info(f"   📂 Entity types created: {entity_types}")
            for entity_type in entity_types:
                entity_files = list((entities_dir / entity_type).glob("*.json"))
                log.info(f"      {entity_type}: {len(entity_files)} files")
        
        if relationships_dir.exists():
            rel_files = list(relationships_dir.glob("*.json"))
            log.info(f"   🔗 Relationship files created: {len(rel_files)}")
        
        log.info(f"✅ [NER PIPELINE] Phase2NEWExtractor.run_all() completed successfully")
        return total_entities
    
    async def _process_chunk_direct(self, chunk_file: Path, phase1_entities: Optional[List[Dict]] = None) -> int:
        """
        Process a single chunk file using direct core function calls (no adapter layer).
        
        Args:
            chunk_file: Path to the chunk text file
            phase1_entities: Phase 1 entities for context (currently unused)
            
        Returns:
            Total number of entities extracted
        """
        log.info(f"🔍 [EXTRACTOR_DIRECT] Starting process_chunk_direct() for: {chunk_file.name}")
        
        try:
            # Parse chunk file
            log.info(f"📄 [EXTRACTOR_DIRECT] Parsing chunk file: {chunk_file}")
            meta, text = parse_chunk_file(str(chunk_file))
            
            log.info(f"📋 [EXTRACTOR_DIRECT] Parsed metadata: {meta}")
            log.info(f"📝 [EXTRACTOR_DIRECT] Text length: {len(text)} characters")
            
            if not text or len(text.strip()) < 50:
                log.warning(f"⚠️ [EXTRACTOR_DIRECT] Skipping chunk with insufficient text")
                return 0
            
            if self.use_triple_extraction:
                return await self._process_triple_extraction(chunk_file, meta, text)
            else:
                return await self._process_legacy_extraction(chunk_file, meta, text)
                
        except Exception as e:
            log.error(f"❌ [EXTRACTOR_DIRECT] Failed to process {chunk_file.name}: {e}")
            return 0
    
    async def _process_triple_extraction(self, chunk_file: Path, meta: Dict, text: str) -> int:
        """Process using single-call triple extraction (faster)."""
        log.info(f"🔥 [EXTRACTOR_DIRECT] Using triple extraction for: {chunk_file.name}")
        
        # Extract triples using 3 parallel calls focused on different ontology portions
        log.info(f"🔍 [EXTRACTOR_DIRECT] Calling extract_triples() with 3-way parallel logic")
        log.info(f"   📄 Document type: {meta.get('documentType', 'unknown')}")
        log.info(f"   📅 Meeting date: {meta.get('meetingDate', 'unknown')}")
        log.info(f"   📁 Source file: {meta.get('sourceFileName', 'unknown')}")
        
        import concurrent.futures
        
        def extract_triples_for_group(group_info):
            """Extract triples for a specific entity group."""
            group_num, entity_types, ontology_context = group_info
            log.info(f"🔍 [GROUP_{group_num}] Starting triple extraction for entity types: {entity_types}")
            
            try:
                triples_data, raw_response = extract_triples(
                    text, 
                    meta.get('documentType', 'unknown'),
                    meta.get('meetingDate', 'unknown'), 
                    meta.get('sourceFileName', 'unknown'),
                    {'chunkId': f'{chunk_file.stem}_group_{group_num}', 'document': chunk_file.stem},
                    ontology_override=ontology_context
                )
                log.info(f"✅ [GROUP_{group_num}] Triple extraction completed: {len(triples_data.get('triples', []))} triples")
                return group_num, triples_data, raw_response
            except Exception as e:
                log.error(f"❌ [GROUP_{group_num}] Triple extraction failed: {e}")
                return group_num, None, None
        
        # Build focused ontology contexts for 3 parallel calls
        ontology_context_1 = build_focused_ontology_context(ENTITY_TYPE_GROUP_1, "GROUP 1: GOVERNANCE & PEOPLE")
        ontology_context_2 = build_focused_ontology_context(ENTITY_TYPE_GROUP_2, "GROUP 2: DOCUMENTS & CONTENT")
        ontology_context_3 = build_focused_ontology_context(ENTITY_TYPE_GROUP_3, "GROUP 3: INFRASTRUCTURE & RESOURCES")
        
        group_tasks = [
            (1, ENTITY_TYPE_GROUP_1, ontology_context_1),
            (2, ENTITY_TYPE_GROUP_2, ontology_context_2),
            (3, ENTITY_TYPE_GROUP_3, ontology_context_3)
        ]
        
        # Execute 3 parallel triple extraction calls
        log.info(f"🔥 [EXTRACTOR_DIRECT] Starting 3 parallel triple extraction calls")
        
        from scripts.graph_rag_stages.phase2_building.ner.core.simple_ner_consolidated import PerformanceMonitor
        parallel_start_time = PerformanceMonitor.log_parallel_execution_start(log, 3)
        
        # Use asyncio for true async parallelism
        loop = asyncio.get_event_loop()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [loop.run_in_executor(executor, extract_triples_for_group, task) for task in group_tasks]
            results = await asyncio.gather(*futures)
        
        PerformanceMonitor.log_parallel_execution_end(log, parallel_start_time, 3, "triple extraction calls")
        
        # Combine and convert results
        all_triples = []
        for group_num, triples_data, raw_response in results:
            if triples_data and 'triples' in triples_data:
                all_triples.extend(triples_data['triples'])
                log.info(f"✅ [GROUP_{group_num}] Added {len(triples_data['triples'])} triples")
        
        log.info(f"🔗 [EXTRACTOR_DIRECT] Combined {len(all_triples)} total triples from 3 parallel calls")
        
        # Convert triples to entities and relationships
        log.info(f"🔄 [EXTRACTOR_DIRECT] Converting triples to entities and relationships")
        combined_triples_data = {'triples': all_triples}
        entities_data, relationships_data = convert_triples_to_entities_relationships(combined_triples_data)
        
        # Persist results
        log.info(f"💾 [EXTRACTOR_DIRECT] Persisting extraction results")
        
        # Use the proper persistence function from consolidated module for triple extraction
        from scripts.graph_rag_stages.phase2_building.ner.core.simple_ner_consolidated import _persist_entities_and_relationships
        all_entities, persistence_log, relationship_log = _persist_entities_and_relationships(
            meta, entities_data, relationships_data, text, self.output_dir
        )
        entities_count = persistence_log["persisted_entities_count"]
        
        log.info(f"✅ [EXTRACTOR_DIRECT] Triple extraction completed: {entities_count} entities extracted")
        return entities_count
    
    async def _process_legacy_extraction(self, chunk_file: Path, meta: Dict, text: str) -> int:
        """Process using legacy 3-phase extraction."""
        log.info(f"⚙️ [EXTRACTOR_DIRECT] Using legacy 3-phase extraction for: {chunk_file.name}")
        
        # Extract entities using legacy approach
        result, raw_text, rel_template, attr_template, sys_prompt = extract_entities_split(
            text,
            document_type=meta.get('documentType', 'unknown'),
            meeting_date=meta.get('meetingDate', 'unknown'),
            source_file=meta.get('sourceFileName', 'unknown'),
        )
        
        log.info(f"✅ [EXTRACTOR_DIRECT] extract_entities_split() completed")
        
        # Persist entities and get count
        entities_count = _persist_phase2_new(result, meta, text, self.output_dir)
        
        # Extract and persist relationships if entities were found
        if entities_count > 0:
            log.info(f"🔗 [EXTRACTOR_DIRECT] Extracting relationships for {entities_count} entities")
            # Note: Legacy relationship extraction temporarily disabled 
            # The main pipeline uses triple extraction which handles relationships automatically
            # Legacy mode only extracts entities for now
            log.info("⚠️ [EXTRACTOR_DIRECT] Legacy relationship extraction temporarily disabled")
        
        log.info(f"✅ [EXTRACTOR_DIRECT] Legacy extraction completed: {entities_count} entities extracted")
        return entities_count
    
    # Compatibility methods to match ThreePassExtractor interface
    
    async def extract_entities_from_chunk(self, chunk_file: Path, phase1_entities: Optional[List[Dict]] = None) -> int:
        """
        Extract entities from a single chunk (compatibility method).
        
        Args:
            chunk_file: Path to chunk file
            phase1_entities: Phase 1 entities for context
            
        Returns:
            Number of entities extracted
        """
        return await self._process_chunk_direct(chunk_file, phase1_entities)
    
    def get_output_stats(self) -> Dict[str, int]:
        """
        Get statistics about extraction output (compatibility method).
        
        Returns:
            Dictionary with entity counts by type
        """
        stats = {}
        entities_dir = self.output_dir / "entities"
        
        if entities_dir.exists():
            for entity_type_dir in entities_dir.iterdir():
                if entity_type_dir.is_dir():
                    entity_files = list(entity_type_dir.glob("*.json"))
                    stats[entity_type_dir.name] = len(entity_files)
        
        return stats
