#!/usr/bin/env python3
"""
Phase2NEWExtractor: Drop-in replacement for ThreePassExtractor that delegates to the original adapters.
This maintains EXACT original performance by using the adapters unchanged.
"""

from pathlib import Path
from typing import Dict, List, Any, Optional
import logging
import asyncio
import os
import time
import json

# Import the ORIGINAL adapters - no custom logic
from scripts.graph_rag_stages.phase2_building.ner.phase2_new_adapter_triples import Phase2NEWAdapterTriples
from scripts.graph_rag_stages.phase2_building.ner.phase2_new_adapter import Phase2NEWAdapter

log = logging.getLogger(__name__)


class Phase2NEWExtractor:
    """Drop-in replacement for ThreePassExtractor - delegates to ORIGINAL adapters."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        
        # Initialize with ORIGINAL adapter (EXACT original logic)
        log.info("🔥 [NER PIPELINE] Using TRIPLE EXTRACTION (single API call - much faster!)")
        self.adapter = Phase2NEWAdapterTriples(output_dir)
    
    async def process_chunk(self, chunk_file: Path, phase1_entities: Optional[List[Dict]] = None) -> int:
        """Process a chunk file using the ORIGINAL adapter (EXACT same logic)."""
        log.info(f"🔍 [EXTRACTOR] Starting chunk processing for: {chunk_file.name}")
        
        try:
            # Use the EXACT original adapter logic - no modifications
            return await self.adapter.process_chunk(chunk_file, phase1_entities)
            
        except Exception as e:
            log.error(f"❌ [EXTRACTOR] Error processing chunk {chunk_file.name}: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    async def run_all(self, phase1_entities: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """Process all chunks using the ORIGINAL adapter logic."""
        log.info("🔍 [NER PIPELINE] Starting Phase2NEWExtractor.run_all()")
        log.info(f"   📁 Output directory: {self.output_dir}")
        log.info(f"   📁 Chunks directory: {self.output_dir / 'document_chunks'}")
        log.info(f"   📋 Phase1 entities provided: {len(phase1_entities) if phase1_entities else 0}")
        
        chunks_dir = self.output_dir / "document_chunks"
        if not chunks_dir.exists():
            log.warning(f"⚠️ [NER PIPELINE] Chunks directory does not exist: {chunks_dir}")
            return {
                "total_entities_extracted": 0,
                "files_processed": 0,
                "entity_types_created": [],
                "relationship_files_created": 0
            }
        
        chunk_files = list(chunks_dir.glob("*.txt"))
        log.info(f"📄 [NER PIPELINE] Processing {len(chunk_files)} chunks with Phase2_NEW extractor")
        
        if chunk_files:
            log.info(f"   📝 Sample chunk files: {[f.name for f in chunk_files[:3]]}")
            if len(chunk_files) > 3:
                log.info(f"   ... and {len(chunk_files) - 3} more chunks")
        
        # Check if output directories exist
        log.info(f"   📁 Entities directory exists: {(self.output_dir / 'entities').exists()}")
        log.info(f"   📁 Relationships directory exists: {(self.output_dir / 'relationships').exists()}")
        
        # PARALLEL CHUNK PROCESSING - restored from original version
        batch_size = 12  # Original batch size for optimal performance
        MAX_CONCURRENT_BATCHES = 6  # Original concurrency limit
        MAX_CONCURRENT_CHUNKS = 3  # Limit concurrent chunk processing to 3 at a time
        
        # Create batches
        batches = []
        for i in range(0, len(chunk_files), batch_size):
            batch = chunk_files[i:i + batch_size]
            batch_num = i // batch_size + 1
            batches.append((batch, batch_num))
        
        total_batches = len(batches)
        log.info(f"🚀 [NER PIPELINE] Processing {total_batches} batches with max {MAX_CONCURRENT_BATCHES} concurrent batches")
        log.info(f"   📦 Batch size: {batch_size} chunks per batch")
        log.info(f"   📄 Max concurrent chunks: {MAX_CONCURRENT_CHUNKS} chunks at a time")
        log.info(f"   ⏱️  Anti-throttling: 100ms/200ms within-chunk + 300ms staggered chunk delays")
        
        # Semaphores to control concurrency
        batch_semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)
        chunk_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHUNKS)
        
        async def process_single_chunk(chunk_file, chunk_index_in_batch=0):
            """Process a single chunk with semaphore control and anti-throttling delay."""
            async with chunk_semaphore:
                # Apply cross-chunk anti-throttling delay INSIDE the semaphore
                if chunk_index_in_batch > 0:
                    cross_chunk_delay_ms = chunk_index_in_batch * 300
                    log.debug(f"   ⏱️  Applying {cross_chunk_delay_ms}ms cross-chunk delay for chunk {chunk_index_in_batch + 1}")
                    await asyncio.sleep(cross_chunk_delay_ms / 1000.0)
                
                return await self.process_chunk(chunk_file, phase1_entities)
        
        async def process_single_batch(batch_data):
            """Process a single batch with concurrency control."""
            batch, batch_num = batch_data
            
            async with batch_semaphore:
                log.info(f"📦 [NER PIPELINE] Processing batch {batch_num}/{total_batches} ({len(batch)} chunks)...")
                
                # Process batch with limited concurrent chunk processing and cross-chunk anti-throttling delays
                tasks = []
                for i, chunk_file in enumerate(batch):
                    # Create task with chunk index for proper delay application
                    task = process_single_chunk(chunk_file, chunk_index_in_batch=i)
                    tasks.append(task)
                
                # Wait for batch to complete with chunk-level concurrency control
                log.info(f"⏳ [NER PIPELINE] Executing batch {batch_num} tasks with max {MAX_CONCURRENT_CHUNKS} concurrent chunks...")
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Count successful extractions
                batch_entities = 0
                batch_failures = 0
                for i, result in enumerate(results):
                    chunk_file = batch[i]
                    if isinstance(result, Exception):
                        log.error(f"❌ [NER PIPELINE] Batch {batch_num} chunk {chunk_file.name} failed: {result}")
                        batch_failures += 1
                    else:
                        batch_entities += result
                
                log.info(f"✅ [NER PIPELINE] Batch {batch_num} completed: {batch_entities} entities, {batch_failures} failures")
                return batch_entities
        
        # Process all batches in parallel with controlled concurrency
        log.info(f"🔥 [NER PIPELINE] Starting parallel batch processing...")
        batch_tasks = [process_single_batch(batch_data) for batch_data in batches]
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        # Sum up all results
        total_entities = 0
        for result in batch_results:
            if isinstance(result, Exception):
                log.error(f"❌ [NER PIPELINE] Batch processing failed: {result}")
            else:
                total_entities += result
        
        log.info(f"🎉 [NER PIPELINE] All batches completed: {total_entities} total entities extracted")
        
        # Count output files (same as original)
        entities_dir = self.output_dir / "entities"
        relationships_dir = self.output_dir / "relationships"
        
        entity_types_created = []
        if entities_dir.exists():
            for entity_type_dir in entities_dir.iterdir():
                if entity_type_dir.is_dir() and list(entity_type_dir.glob("*.json")):
                    entity_types_created.append(entity_type_dir.name)
        
        relationship_files = 0
        if relationships_dir.exists():
            relationship_files = len(list(relationships_dir.glob("*.json")))
        
        log.info(f"   📁 Output written to: {self.output_dir}")
        log.info(f"   📂 Entity types created: {entity_types_created}")
        log.info(f"   🔗 Relationship files created: {relationship_files}")
        log.info("✅ [NER PIPELINE] Phase2NEWExtractor.run_all() completed successfully")
        
        return {
            "total_entities_extracted": total_entities,
            "files_processed": len(chunk_files),
            "entity_types_created": entity_types_created,
            "relationship_files_created": relationship_files
        }

    # Compatibility methods to match ThreePassExtractor interface
    def get_all_entities(self) -> List[Dict]:
        """Get all entities (compatibility method)."""
        return self.adapter._get_all_entities() if hasattr(self.adapter, '_get_all_entities') else []
    
    def get_entities_by_type(self, entity_type: str) -> List[Dict]:
        """Get entities by type (compatibility method).""" 
        return self.adapter._get_entities_by_type(entity_type) if hasattr(self.adapter, '_get_entities_by_type') else []
    
    def get_entity_summary(self) -> Dict[str, int]:
        """Get entity summary (compatibility method)."""
        return self.adapter._get_entity_summary() if hasattr(self.adapter, '_get_entity_summary') else {}