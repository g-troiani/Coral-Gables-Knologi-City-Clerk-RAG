"""
Phase2_NEW based extractor that replaces the three-pass extractor.
Uses the simpler phase2_NEW extraction logic while maintaining compatibility
with the main pipeline's expected interfaces and output formats.
"""

import logging
from pathlib import Path
from typing import List, Dict, Optional
import asyncio

from scripts.graph_rag_stages.phase2_building.ner.phase2_new_adapter import Phase2NEWAdapter

log = logging.getLogger(__name__)


class Phase2NEWExtractor:
    """
    Drop-in replacement for ThreePassExtractor using phase2_NEW logic.
    Maintains the same interface but uses the simpler extraction approach.
    """
    
    def __init__(self, output_dir: Path):
        """
        Initialize the extractor.
        
        Args:
            output_dir: Root directory for NER outputs (e.g., simple_ner_graph/)
        """
        self.output_dir = Path(output_dir)
        self.chunks_dir = self.output_dir / "document_chunks"
        self.adapter = Phase2NEWAdapter(output_dir)
        
        # Create necessary directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        entities_dir = self.output_dir / "entities"
        entities_dir.mkdir(parents=True, exist_ok=True)
        relationships_dir = self.output_dir / "relationships"
        relationships_dir.mkdir(parents=True, exist_ok=True)
    
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
        
        # Process chunks with concurrency control
        total_entities = 0
        total_relationships = 0
        batch_size = 10  # Process 10 chunks at a time
        
        log.info(f"🔄 [NER PIPELINE] Processing in batches of {batch_size}")
        
        for i in range(0, len(chunk_files), batch_size):
            batch = chunk_files[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(chunk_files) + batch_size - 1) // batch_size
            
            log.info(f"📦 [NER PIPELINE] Processing batch {batch_num}/{total_batches} ({len(batch)} chunks)...")
            log.info(f"   📝 Batch files: {[f.name for f in batch]}")
            
            # Process batch concurrently
            tasks = []
            for chunk_file in batch:
                task = self.adapter.process_chunk(chunk_file, phase1_entities)
                tasks.append(task)
            
            # Wait for batch to complete
            log.info(f"⏳ [NER PIPELINE] Executing batch {batch_num} tasks...")
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
                    total_entities += result
                    log.info(f"  ✅ [NER PIPELINE] {batch[j].name}: {result} entities extracted")
            
            log.info(f"📊 [NER PIPELINE] Batch {batch_num} summary:")
            log.info(f"   ✅ Successful: {len(batch) - batch_failures} chunks")
            log.info(f"   ❌ Failed: {batch_failures} chunks")
            log.info(f"   📈 Entities extracted: {batch_entities}")
            
            # Progress update
            processed = min(i + batch_size, len(chunk_files))
            log.info(f"   📊 Overall progress: {processed}/{len(chunk_files)} chunks processed ({processed/len(chunk_files)*100:.1f}%)")
        
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
        return await self.adapter.process_chunk(chunk_file, phase1_entities)
    
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
