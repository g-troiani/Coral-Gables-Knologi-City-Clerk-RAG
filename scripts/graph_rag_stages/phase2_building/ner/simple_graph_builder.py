"""
Simple graph builder that creates file-based indices for entity and chunk lookup.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Set, Any
from collections import defaultdict
import asyncio

log = logging.getLogger(__name__)


class SimpleGraphBuilder:
    """Builds entity and chunk indices for fast retrieval."""
    
    def __init__(self, output_dir: Path):
        """Initialize the graph builder."""
        self.output_dir = Path(output_dir)
        self.chunks_dir = self.output_dir / "document_chunks"
        
        # Index file paths
        self.entity_index_path = self.output_dir / "entity_index.json"
        self.chunk_index_path = self.output_dir / "chunk_index.json"
        
    async def build_indices(self) -> None:
        """Build both entity and chunk indices."""
        log.info("Building entity index...")
        entity_index = await self._build_entity_index()
        
        log.info("Building chunk index...")
        chunk_index = await self._build_chunk_index()
        
        # Save indices
        await self._save_indices(entity_index, chunk_index)
        
        # Log statistics
        self._log_index_stats(entity_index, chunk_index)
    
    async def _build_entity_index(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Build inverted index: entity -> list of chunk IDs.
        
        Returns:
            Dictionary with structure:
            {
                "people": {
                    "Commissioner Smith": ["chunk_id1", "chunk_id2"],
                    "Mayor Johnson": ["chunk_id3"]
                },
                "organizations": {...}
            }
        """
        entity_index = defaultdict(lambda: defaultdict(set))
        
        # Process each entity category
        for category_dir in self.output_dir.iterdir():
            if category_dir.is_dir() and category_dir.name not in ['document_chunks']:
                category = category_dir.name
                
                # Process each entity file in the category
                for entity_file in category_dir.glob("*.txt"):
                    chunk_id = entity_file.stem.split("_")[0]
                    
                    # Read entities from file
                    entities = self._read_entity_file(entity_file)
                    
                    # Add to index
                    for entity in entities:
                        entity_index[category][entity].add(chunk_id)
        
        # Convert sets to lists for JSON serialization
        final_index = {}
        for category, entities in entity_index.items():
            final_index[category] = {
                entity: sorted(list(chunk_ids))
                for entity, chunk_ids in entities.items()
            }
        
        return final_index
    
    async def _build_chunk_index(self) -> Dict[str, Dict[str, Any]]:
        """
        Build chunk metadata index.
        
        Returns:
            Dictionary with structure:
            {
                "chunk_id1": {
                    "document": "Agenda_01.09.2024",
                    "document_type": "agenda",
                    "meeting_date": "01.09.2024",
                    "chunk_index": 0,
                    "total_chunks": 5,
                    "entities": {
                        "people": ["Commissioner Smith"],
                        "dates": ["January 9, 2024"]
                    },
                    "text_preview": "First 200 chars..."
                }
            }
        """
        chunk_index = {}
        
        # Process each chunk file
        for chunk_file in self.chunks_dir.glob("*.txt"):
            chunk_id = chunk_file.stem.split("_")[0]
            doc_name = "_".join(chunk_file.stem.split("_")[1:])
            
            # Read chunk metadata
            metadata = self._read_chunk_metadata(chunk_file)
            
            # Get entities for this chunk
            chunk_entities = await self._get_chunk_entities(chunk_id)
            
            # Build chunk entry
            chunk_index[chunk_id] = {
                "document": doc_name,
                "document_type": metadata.get("document_type", "unknown"),
                "meeting_date": metadata.get("meeting_date", ""),
                "chunk_index": metadata.get("chunk_index", 0),
                "total_chunks": metadata.get("total_chunks", 1),
                "entities": chunk_entities,
                "text_preview": metadata.get("text_preview", ""),
                "source_file": metadata.get("source", "")
            }
        
        return chunk_index
    
    def _read_entity_file(self, entity_file: Path) -> List[str]:
        """Read entities from an entity file."""
        entities = []
        
        with open(entity_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Skip header and read entities
        if "---" in content:
            _, entity_section = content.split("---", 1)
            entity_section = entity_section.strip()
            
            # Each line is an entity
            for line in entity_section.split("\n"):
                line = line.strip()
                if line:
                    entities.append(line)
        
        return entities
    
    def _read_chunk_metadata(self, chunk_file: Path) -> Dict[str, Any]:
        """Read metadata from chunk file header."""
        metadata = {}
        
        with open(chunk_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Parse header
        if "---" in content:
            header, text = content.split("---", 1)
            
            # Extract metadata from header
            for line in header.strip().split("\n"):
                if line.startswith("#") and ":" in line:
                    key_value = line[1:].strip().split(":", 1)
                    if len(key_value) == 2:
                        key = key_value[0].strip().lower().replace(" ", "_")
                        value = key_value[1].strip()
                        
                        # Convert numeric values
                        if key == "index":
                            parts = value.split("/")
                            if len(parts) == 2:
                                metadata["chunk_index"] = int(parts[0]) - 1
                                metadata["total_chunks"] = int(parts[1])
                        else:
                            metadata[key] = value
            
            # Get text preview
            text = text.strip()
            metadata["text_preview"] = text[:200] + "..." if len(text) > 200 else text
        
        return metadata
    
    async def _get_chunk_entities(self, chunk_id: str) -> Dict[str, List[str]]:
        """Get all entities associated with a chunk."""
        chunk_entities = defaultdict(list)
        
        # Check each category directory
        for category_dir in self.output_dir.iterdir():
            if category_dir.is_dir() and category_dir.name not in ['document_chunks']:
                category = category_dir.name
                
                # Look for entity file for this chunk
                entity_files = list(category_dir.glob(f"{chunk_id}_*.txt"))
                if entity_files:
                    entities = self._read_entity_file(entity_files[0])
                    if entities:
                        chunk_entities[category] = entities
        
        return dict(chunk_entities)
    
    async def _save_indices(self, entity_index: Dict, chunk_index: Dict) -> None:
        """Save indices to JSON files."""
        # Save entity index
        with open(self.entity_index_path, 'w', encoding='utf-8') as f:
            json.dump(entity_index, f, indent=2, ensure_ascii=False)
        
        # Save chunk index
        with open(self.chunk_index_path, 'w', encoding='utf-8') as f:
            json.dump(chunk_index, f, indent=2, ensure_ascii=False)
        
        log.info(f"Saved indices to {self.output_dir}")
    
    def _log_index_stats(self, entity_index: Dict, chunk_index: Dict) -> None:
        """Log statistics about the built indices."""
        # Entity statistics
        total_entities = 0
        category_counts = {}
        
        for category, entities in entity_index.items():
            category_counts[category] = len(entities)
            total_entities += len(entities)
        
        log.info(f"Entity Index Statistics:")
        log.info(f"  Total unique entities: {total_entities}")
        for category, count in sorted(category_counts.items()):
            log.info(f"  {category}: {count} entities")
        
        # Chunk statistics
        log.info(f"Chunk Index Statistics:")
        log.info(f"  Total chunks: {len(chunk_index)}")
        
        # Document type distribution
        doc_types = defaultdict(int)
        for chunk_data in chunk_index.values():
            doc_types[chunk_data.get('document_type', 'unknown')] += 1
        
        log.info(f"  Document type distribution:")
        for doc_type, count in sorted(doc_types.items()):
            log.info(f"    {doc_type}: {count} chunks") 