"""
Parallel NER extractor with batched LLM calls and concurrent processing
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Any, Tuple
import json
from concurrent.futures import ThreadPoolExecutor
import os

from .enhanced_ner_extractor import EnhancedNERExtractor

log = logging.getLogger(__name__)


class ParallelNERExtractor(EnhancedNERExtractor):
    """Optimized NER extractor with parallel processing and batching."""
    
    def __init__(self, output_dir: Path, seed_entities=None):
        super().__init__(output_dir, seed_entities)
        
        # More aggressive parallel processing
        self.max_workers = min(os.cpu_count() or 4, 16)  # Was 8
        self.batch_size = int(os.getenv("NER_BATCH_SIZE", "10"))  # Was 5
        self.max_concurrent_llm = int(os.getenv("MAX_CONCURRENT_LLM", "20"))  # Was 10
        
        # Override parent's conservative semaphore
        self.max_concurrent = 20  # Parent has 5
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        
        log.info(f"Parallel NER initialized with {self.max_workers} workers, batch size {self.batch_size}")
    
    async def process_all_chunks(self) -> int:
        """Process all chunks with optimized parallel processing."""
        chunk_files = list(self.chunks_dir.glob("*.txt"))
        log.info(f"Found {len(chunk_files)} chunks to process")
        
        if not chunk_files:
            return 0
        
        # Group chunks into batches
        batches = [chunk_files[i:i + self.batch_size] 
                   for i in range(0, len(chunk_files), self.batch_size)]
        
        log.info(f"Processing {len(batches)} batches of ~{self.batch_size} chunks each")
        
        # Create semaphore for LLM rate limiting
        llm_semaphore = asyncio.Semaphore(self.max_concurrent_llm)
        
        # Process batches concurrently
        total_entities = 0
        batch_tasks = []
        
        for batch_idx, batch in enumerate(batches):
            task = self._process_batch(batch, batch_idx, llm_semaphore)
            batch_tasks.append(task)
        
        # Process all batches concurrently with progress tracking
        results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                log.error(f"Batch processing error: {result}")
            else:
                total_entities += result
        
        return total_entities
    
    async def _process_batch(self, chunk_files: List[Path], batch_idx: int, 
                           llm_semaphore: asyncio.Semaphore) -> int:
        """Process a batch of chunks with parallel extraction."""
        log.info(f"Processing batch {batch_idx + 1} with {len(chunk_files)} chunks")
        
        # Read all chunks in batch first (parallel I/O)
        chunk_data = await self._read_chunks_parallel(chunk_files)
        
        # Process chunks with optimized extraction
        total_entities = 0
        
        for chunk_file, (chunk_metadata, chunk_text) in zip(chunk_files, chunk_data):
            if not chunk_text:
                continue
                
            async with llm_semaphore:
                try:
                    # Run the 3 extraction prompts in parallel where possible
                    entities = await self._optimized_extraction(
                        chunk_text, chunk_metadata, chunk_file
                    )
                    
                    if entities > 0:
                        total_entities += entities
                        
                except Exception as e:
                    log.error(f"Failed to process {chunk_file.name}: {e}")
        
        log.info(f"Batch {batch_idx + 1} completed: {total_entities} entities")
        return total_entities
    
    async def _read_chunks_parallel(self, chunk_files: List[Path]) -> List[Tuple[Dict, str]]:
        """Read multiple chunk files in parallel."""
        async def read_chunk(chunk_file: Path) -> Tuple[Dict, str]:
            try:
                # Get metadata and content
                filename_parts = chunk_file.stem.split("_", 1)
                chunk_id = filename_parts[0]
                doc_name = filename_parts[1] if len(filename_parts) > 1 else "unknown"
                
                # Read file
                with open(chunk_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Parse metadata
                chunk_metadata = self._read_chunk_metadata(chunk_file)
                chunk_metadata['chunk_id'] = chunk_id
                chunk_metadata['document'] = doc_name
                chunk_metadata['chunk_file'] = chunk_file.name
                
                # Extract text
                if "---" in content:
                    parts = content.split("---")
                    chunk_text = ""
                    for i, part in enumerate(parts):
                        if i == 0:
                            continue
                        cleaned_part = part.strip()
                        if cleaned_part and not cleaned_part.startswith('- '):
                            chunk_text = cleaned_part
                            break
                    if not chunk_text and len(parts) > 1:
                        chunk_text = parts[-1].strip()
                else:
                    chunk_text = content
                
                return (chunk_metadata, chunk_text)
                
            except Exception as e:
                log.error(f"Error reading {chunk_file}: {e}")
                return ({}, "")
        
        # Read all files concurrently
        tasks = [read_chunk(f) for f in chunk_files]
        return await asyncio.gather(*tasks)
    
    async def _optimized_extraction(self, chunk_text: str, chunk_metadata: Dict, 
                                  chunk_file: Path) -> int:
        """Optimized extraction with parallel prompts where possible."""
        
        chunk_id = chunk_metadata.get('chunk_id', 'unknown')
        doc_name = chunk_metadata.get('document', 'unknown')
        
        # Skip if no meaningful content
        if not chunk_text or len(chunk_text) < 10:
            return 0
        
        # Run entity extraction first
        entities = await self._extract_entities_only(chunk_text, chunk_metadata)
        
        if not entities or all(not v for v in entities.values()):
            return 0
        
        # Run relationship extraction and attribute enhancement in parallel
        # since they don't depend on each other
        rel_task = self._extract_relationships_only(chunk_text, entities, chunk_metadata)
        attr_task = self._enhance_attributes_only(chunk_text, entities, chunk_metadata)
        
        relationships, enhanced_entities = await asyncio.gather(rel_task, attr_task)
        
        # Combine results
        extraction_result = {
            "entities": enhanced_entities,
            "relationships": relationships
        }
        
        # Save results
        entity_count = await self._save_extraction_results(
            chunk_id, doc_name, extraction_result, chunk_metadata
        )
        
        return entity_count
    
    async def _extract_entities_only(self, chunk_text: str, chunk_metadata: Dict) -> Dict:
        """Extract entities using the parent class method."""
        try:
            # Use the parent class's entity extraction logic
            return await super()._extract_entities_only(chunk_text, chunk_metadata)
        except Exception as e:
            log.error(f"Entity extraction failed: {e}")
            return {}
    
    async def _extract_relationships_only(self, chunk_text: str, entities: Dict, chunk_metadata: Dict) -> List:
        """Extract relationships using the parent class method."""
        try:
            # Use the parent class's relationship extraction logic
            return await super()._extract_relationships_only(chunk_text, entities, chunk_metadata)
        except Exception as e:
            log.error(f"Relationship extraction failed: {e}")
            return []
    
    async def _enhance_attributes_only(self, chunk_text: str, entities: Dict, chunk_metadata: Dict) -> Dict:
        """Enhance entity attributes using the parent class method."""
        try:
            # Use the parent class's attribute enhancement logic
            return await super()._enhance_attributes_only(chunk_text, entities, chunk_metadata)
        except Exception as e:
            log.error(f"Attribute enhancement failed: {e}")
            return entities
    
    async def _save_extraction_results(self, chunk_id: str, doc_name: str, 
                                     extraction_result: Dict, chunk_metadata: Dict) -> int:
        """Save extraction results using the parent class method."""
        try:
            # Use the parent class's save logic
            return await super()._save_extraction_results(chunk_id, doc_name, extraction_result, chunk_metadata)
        except Exception as e:
            log.error(f"Saving results failed: {e}")
            return 0 