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
        if not self.chunks_dir.exists():
            log.error(f"Chunks directory not found: {self.chunks_dir}")
            return 0
        
        chunk_files = list(self.chunks_dir.glob("*.txt"))
        if not chunk_files:
            log.warning(f"No chunk files found in {self.chunks_dir}")
            return 0
        
        log.info(f"📄 Processing {len(chunk_files)} chunks with Phase2_NEW extractor")
        
        # Process chunks with concurrency control
        total_entities = 0
        batch_size = 3  # Process 3 chunks at a time (reduced for stability)
        
        for i in range(0, len(chunk_files), batch_size):
            batch = chunk_files[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(chunk_files) + batch_size - 1) // batch_size
            
            log.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} chunks)...")
            
            # Process batch concurrently
            tasks = []
            for chunk_file in batch:
                task = self.adapter.process_chunk(chunk_file, phase1_entities)
                tasks.append(task)
            
            # Wait for batch to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count successful extractions
            batch_entities = 0
            for j, result in enumerate(results):
                if isinstance(result, Exception):
                    log.error(f"Failed to process {batch[j].name}: {result}")
                else:
                    batch_entities += result
                    total_entities += result
                    log.info(f"  ✓ {batch[j].name}: {result} entities")
            
            log.info(f"Batch {batch_num} complete: {batch_entities} entities extracted")
            
            # Progress update
            processed = min(i + batch_size, len(chunk_files))
            log.info(f"   Progress: {processed}/{len(chunk_files)} chunks processed")
        
        log.info(f"✅ Phase2_NEW extraction complete: {total_entities} entities extracted")
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
