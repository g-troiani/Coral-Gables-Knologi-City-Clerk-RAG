"""
Simple graph builder that creates file-based indices for entity and chunk lookup.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Set, Any, Optional
from collections import defaultdict
import asyncio
import hashlib
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards

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
        
        # New: Build relationship index
        log.info("Building relationship index...")
        rel_index = await self._build_relationship_index(entity_index, chunk_index)
        bidirectional_index = self._build_bidirectional_index(rel_index)  # New for bidirectional lookups
        
        # Save indices (add rel_index)
        await self._save_indices(entity_index, chunk_index, rel_index)
        
        # Save bidirectional index
        with open(self.output_dir / "bidirectional_relationship_index.json", 'w', encoding='utf-8') as f:
            json.dump(bidirectional_index, f, indent=2, ensure_ascii=False)
        
        # Build status index for outcomes
        log.info("Building status index...")
        status_index = self._build_status_index(entity_index)
        
        # Save status index
        with open(self.output_dir / "status_index.json", 'w', encoding='utf-8') as f:
            json.dump(status_index, f, indent=2, ensure_ascii=False)
        
        # Log statistics (extend)
        self._log_index_stats(entity_index, chunk_index, rel_index)
    
    async def _build_entity_index(self) -> Dict[str, Dict[str, Any]]:
        """
        Build inverted index with full entity data.
        """
        entity_index = defaultdict(lambda: defaultdict(dict))

        # Process each entity category
        for category_dir in self.output_dir.iterdir():
            if category_dir.is_dir() and category_dir.name not in ['document_chunks', 'relationships']:
                category = category_dir.name

                # Process each entity file in the category
                for entity_file in category_dir.glob("*.json"):
                    chunk_id = entity_file.stem.split("_")[0]

                    # Read entities from file
                    entities = self._read_entity_file(entity_file)

                    # Add to index with full entity data
                    for entity in entities:
                        if isinstance(entity, dict):
                            entity_id = entity.get('id')
                            if entity_id:
                                # Store full entity data, not just chunk IDs
                                if entity_id not in entity_index[category]:
                                    entity_index[category][entity_id] = {
                                        'entity_data': entity,
                                        'chunk_ids': []
                                    }
                                entity_index[category][entity_id]['chunk_ids'].append(chunk_id)

        # Convert to regular dict
        final_index = {}
        for category, entities in entity_index.items():
            final_index[category] = dict(entities)

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
                "Source_File_Name": metadata.get("Source_File_Name", metadata.get("source", ""))
            }
        
        return chunk_index
    
    async def _build_relationship_index(self, entity_index: Dict, chunk_index: Dict) -> Dict[str, Dict[str, Any]]:
        """Build relationship index with proper entity type validation."""
        rel_index = {}
        
        rel_dir = self.output_dir / "relationships"
        if not rel_dir.exists():
            log.warning("No relationships directory found")
            return rel_index
        
        # First, build a complete entity lookup with types
        entity_lookup = {}
        for category, entities in entity_index.items():
            for entity_id, entity_data in entities.items():
                entity_lookup[entity_id] = category
        
        for rel_file in rel_dir.glob("*.json"):
            chunk_id = rel_file.stem.split("_")[0]
            
            try:
                with open(rel_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                relationships = data.get('relationships', [])
                for rel in relationships:
                    rel_type = rel.get('type')
                    source = rel.get('source')
                    target = rel.get('target')
                    
                    if all([rel_type, source, target]):
                        # Check if both entities exist
                        source_exists = source in entity_lookup
                        target_exists = target in entity_lookup
                        
                        if source_exists and target_exists:
                            # Create unique relationship ID
                            rel_id = hashlib.sha256(
                                f"{source}_{rel_type}_{target}".encode()
                            ).hexdigest()[:12]
                            
                            # Store relationship with metadata
                            if rel_id not in rel_index:
                                rel_index[rel_id] = {
                                    "source_entity": source,
                                    "source_type": entity_lookup[source],
                                    "relation": rel_type,
                                    "target_entity": target,
                                    "target_type": entity_lookup[target],
                                    "chunk_ids": [],
                                    "attributes": rel.get('attributes', {})
                                }
                            rel_index[rel_id]["chunk_ids"].append(chunk_id)
                        else:
                            if not source_exists:
                                log.debug(f"Source entity not found: {source}")
                            if not target_exists:
                                log.debug(f"Target entity not found: {target}")
                        
            except (json.JSONDecodeError, FileNotFoundError) as e:
                log.warning(f"Error reading relationship file {rel_file}: {e}")
        
        return rel_index
    
    def _build_bidirectional_index(self, rel_index: Dict) -> Dict:
        """Build index for both source->target and target->source lookups (Fix for Issue 6)"""
        bidirectional = {"forward": defaultdict(list), "reverse": defaultdict(list)}
        for rel_hash, rel in rel_index.items():
            # Forward direction
            bidirectional["forward"][rel['source_entity']].append(rel)
            # Reverse direction
            bidirectional["reverse"][rel['target_entity']].append(rel)
        return bidirectional
    
    def _build_status_index(self, entity_index: Dict) -> Dict[str, List[str]]:
        """Build index for querying by status (Fix for Issue 5)"""
        status_index = {"passed": [], "failed": [], "tabled": [], "deferred": []}
        
        # Get outcomes from entity index
        outcomes = entity_index.get("outcomes", {})
        
        # For each outcome, read the full entity data from file
        for outcome_id, chunk_ids in outcomes.items():
            # Extract item_code from outcome_id (format: outcome_itemcode_meetingdate)
            if outcome_id.startswith("outcome_"):
                parts = outcome_id.split("_")
                if len(parts) >= 3:
                    item_code = parts[1]
                    
                    # Read the full outcome entity from file to get status
                    for chunk_id in chunk_ids:
                        outcome_file = self.output_dir / "outcomes" / f"{chunk_id}_{self._get_doc_name_from_chunk(chunk_id)}.txt"
                        if outcome_file.exists():
                            try:
                                with open(outcome_file, 'r', encoding='utf-8') as f:
                                    content = f.read()
                                    if "---" in content:
                                        _, entity_section = content.split("---", 1)
                                        for line in entity_section.strip().split("\n"):
                                            line = line.strip()
                                            if line:
                                                try:
                                                    outcome_obj = json.loads(line)
                                                    if outcome_obj.get('id') == outcome_id:
                                                        status = outcome_obj.get('status')
                                                        if status in status_index:
                                                            status_index[status].append(item_code)
                                                        break
                                                except json.JSONDecodeError:
                                                    continue
                            except Exception as e:
                                log.warning(f"Error reading outcome file {outcome_file}: {e}")
                            break
        
        return status_index
    
    def _get_doc_name_from_chunk(self, chunk_id: str) -> str:
        """Extract document name from chunk ID."""
        # This is a simplified implementation - may need adjustment based on actual file naming
        return "document"
    
    def _find_entity_category(self, entity_name: str, entities: Dict) -> Optional[str]:
        """Returns category of entity or None if not found (Fix for Issue 2)"""
        for category, ents in entities.items():
            if entity_name in ents:
                return category
        return None
    
    def _read_entity_file(self, entity_file: Path) -> List[Dict[str, Any]]:
        """Read entities from a JSON entity file - return full entity data."""
        entities = []
        
        try:
            with open(entity_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Check if this is a relationships file
            if "relationships" in str(entity_file):
                # For relationship files, return relationship objects
                relationships = data.get('relationships', [])
                return relationships  # Return full relationship objects
            else:
                # For entity files, return full entity objects with metadata
                entity_list = data.get('entities', [])
                entity_type = data.get('entity_type', '')
                chunk_id = data.get('chunk_id', '')
                
                # Use centralized ID field mapping
                id_field = EntityIDStandards.get_id_field(entity_type)
                
                for entity in entity_list:
                    # Normalize the entity
                    normalized_entity = EntityIDStandards.normalize_entity_id_fields(entity, entity_type)
                    
                    entity_id = normalized_entity.get(id_field) or normalized_entity.get('id')
                    if entity_id:
                        enriched_entity = {
                            'id': entity_id,
                            'type': entity_type,
                            'name': normalized_entity.get('name', entity_id),
                            'chunk_id': chunk_id,
                            **normalized_entity  # Include all properties
                        }
                        entities.append(enriched_entity)
                        
        except (json.JSONDecodeError, FileNotFoundError) as e:
            log.warning(f"Error reading entity file {entity_file}: {e}")
        
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
        
        # Standardize source key - ensure backward compatibility
        if "source" in metadata and "Source_File_Path" not in metadata:
            metadata["Source_File_Path"] = metadata.pop("source")  # Rename to consistent
        
        return metadata
    
    async def _get_chunk_entities(self, chunk_id: str) -> Dict[str, List[str]]:
        """Get all entities associated with a chunk."""
        chunk_entities = defaultdict(list)
        
        # Check each category directory
        for category_dir in self.output_dir.iterdir():
            if category_dir.is_dir() and category_dir.name not in ['document_chunks']:
                category = category_dir.name
                
                # Look for entity file for this chunk
                entity_files = list(category_dir.glob(f"{chunk_id}_*.json"))
                if entity_files:
                    entities = self._read_entity_file(entity_files[0])
                    if entities:
                        chunk_entities[category] = entities
        
        return dict(chunk_entities)
    
    async def _save_indices(self, entity_index: Dict, chunk_index: Dict, rel_index: Dict) -> None:
        """Save indices to JSON files."""
        
        # Transform entity index for compatibility
        # Create both a simple index (for backward compatibility) and enhanced index
        simple_entity_index = {}
        enhanced_entity_index = {}
        
        for category, entities in entity_index.items():
            simple_entity_index[category] = {}
            enhanced_entity_index[category] = {}
            
            for entity_id, entity_data in entities.items():
                # Simple index: just chunk IDs
                simple_entity_index[category][entity_id] = entity_data.get('chunk_ids', [])
                
                # Enhanced index: full entity data
                enhanced_entity_index[category][entity_id] = entity_data
        
        # Save simple entity index (for backward compatibility)
        with open(self.entity_index_path, 'w', encoding='utf-8') as f:
            json.dump(simple_entity_index, f, indent=2, ensure_ascii=False)
        
        # Save enhanced entity index
        enhanced_path = self.output_dir / "entity_index_enhanced.json"
        with open(enhanced_path, 'w', encoding='utf-8') as f:
            json.dump(enhanced_entity_index, f, indent=2, ensure_ascii=False)
        
        # Save chunk index
        with open(self.chunk_index_path, 'w', encoding='utf-8') as f:
            json.dump(chunk_index, f, indent=2, ensure_ascii=False)
        
        # Save relationship index
        with open(self.output_dir / "relationship_index.json", 'w', encoding='utf-8') as f:
            json.dump(rel_index, f, indent=2, ensure_ascii=False)
        
        log.info(f"Saved indices to {self.output_dir}")
    
    def _log_index_stats(self, entity_index: Dict, chunk_index: Dict, rel_index: Dict) -> None:
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
        
        # Relationship statistics
        log.info(f"Relationship Index Statistics:")
        log.info(f"  Total unique relationships: {len(rel_index)}")
    
    def test_relationship_index(self):
        mock_chunk_index = {"chunk1": {"document": "doc.md"}}
        mock_entity_index = {"people": {"John Smith": ["chunk1"]}}
        rel_index = asyncio.run(self._build_relationship_index(mock_entity_index, mock_chunk_index))
        assert len(rel_index) > 0  # Assuming mock files 