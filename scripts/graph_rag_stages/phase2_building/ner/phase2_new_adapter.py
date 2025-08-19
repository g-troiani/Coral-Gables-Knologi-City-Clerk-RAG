"""
Adapter module to integrate phase2_NEW simple_ner.py with the main NER pipeline.
Transforms phase2_NEW output format to match the expected format of the main pipeline.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import sys

# Ensure project root is on sys.path for package imports
_PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.graph_rag_stages.phase2_NEW.simple_ner import (
    extract_entities, 
    extract_relationships, 
    extract_attributes,
    parse_chunk_file,
    _group_by_type
)
from scripts.graph_rag_stages.common.document_linker import DocumentLinker
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
from scripts.graph_rag_stages.common.entity_factory import EntityFactory

log = logging.getLogger(__name__)


class Phase2NEWAdapter:
    """Adapts phase2_NEW simple_ner.py to work with the main NER pipeline."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.entities_dir = self.output_dir / "entities"
        self.relationships_dir = self.output_dir / "relationships"
        
        # Create output directories
        self.entities_dir.mkdir(parents=True, exist_ok=True)
        self.relationships_dir.mkdir(parents=True, exist_ok=True)
    
    async def process_chunk(self, chunk_file: Path, phase1_entities: Optional[List[Dict]] = None) -> int:
        """
        Process a single chunk file using phase2_NEW logic.
        
        Args:
            chunk_file: Path to the chunk text file
            phase1_entities: Phase 1 entities for context (currently unused by phase2_NEW)
            
        Returns:
            Total number of entities extracted
        """
        try:
            # Parse chunk file using phase2_NEW logic
            meta, text = parse_chunk_file(str(chunk_file))
            
            # Extract entities using phase2_NEW
            result, raw_text, rel_template, attr_template, sys_prompt = extract_entities(
                text,
                document_type=meta.get('documentType', 'unknown'),
                meeting_date=meta.get('meetingDate', 'unknown'),
                source_file=meta.get('sourceFileName', 'unknown'),
            )
            
            # Transform and persist entities
            total_entities = self._transform_and_persist_entities(
                chunk_file, meta, result, raw_text
            )
            
            # Extract and persist relationships if we have entities
            if total_entities > 0 and rel_template:
                # Get normalized entities for relationship extraction
                norm_flat = self._get_normalized_entities(chunk_file, meta)
                
                if norm_flat:
                    rel_parsed, rel_text = extract_relationships(
                        text, rel_template, sys_prompt, norm_flat
                    )
                    
                    self._transform_and_persist_relationships(
                        chunk_file, meta, rel_parsed, norm_flat
                    )
                    
                    # Extract attributes if template available
                    if attr_template:
                        by_type = _group_by_type(norm_flat)
                        enhanced_by_type, _ = extract_attributes(
                            text, attr_template, sys_prompt, by_type
                        )
                        
                        # Update entities with enhanced attributes
                        if enhanced_by_type:
                            self._update_entities_with_attributes(
                                chunk_file, meta, enhanced_by_type
                            )
            
            return total_entities
            
        except Exception as e:
            log.error(f"Error processing chunk {chunk_file}: {e}")
            return 0
    
    def _transform_and_persist_entities(
        self, chunk_file: Path, meta: Dict, result: Dict, raw_text: str
    ) -> int:
        """Transform phase2_NEW entity format to main pipeline format and persist."""
        total_entities = 0
        
        # Convert camelCase metadata to snake_case
        chunk_metadata = {
            "chunk_id": meta.get('chunkId', chunk_file.stem),
            "document": meta.get('document', 'unknown'),
            "document_type": meta.get('documentType', 'unknown'),
            "meeting_date": meta.get('meetingDate', ''),
            "index": "1/1",  # Default value as phase2_NEW doesn't track this
            "source": str(chunk_file),
            "source_file_name": meta.get('sourceFileName', chunk_file.name)
        }
        
        # Get entities from result
        entities_root = result.get('entities', {})
        if not isinstance(entities_root, dict):
            return 0
        
        # Process each entity type
        for entity_type, entities in entities_root.items():
            if not isinstance(entities, list) or not entities:
                continue
            
            # Skip Document entities as per main pipeline
            if entity_type == "Document":
                continue
            
            # Transform entities to match expected format
            transformed_entities = []
            for entity in entities:
                if not isinstance(entity, dict):
                    continue
                
                # Ensure proper ID field
                try:
                    validated_entity = EntityFactory.validate_entity({
                        **entity,
                        'type': entity_type
                    })
                    
                    # Add evidence placeholder (phase2_NEW doesn't extract evidence)
                    if '_evidence' not in validated_entity:
                        validated_entity['_evidence'] = []
                    
                    # Add confidence placeholder
                    if 'confidence' not in validated_entity:
                        validated_entity['confidence'] = 0.9  # Default high confidence
                    
                    # Ensure 'id' field matches the entity-specific ID field
                    id_field = EntityIDStandards.get_id_field(entity_type)
                    if id_field in validated_entity and 'id' not in validated_entity:
                        validated_entity['id'] = validated_entity[id_field]
                    
                    transformed_entities.append(validated_entity)
                    total_entities += 1
                    
                except ValueError as e:
                    log.warning(f"Invalid entity skipped: {e}")
                    continue
            
            if transformed_entities:
                # Create output structure matching main pipeline
                chunk_id = chunk_metadata['chunk_id']
                doc_name = chunk_metadata['document']
                
                file_data = {
                    "chunk_id": chunk_id,
                    "document": doc_name,
                    "source_file": chunk_metadata['source_file_name'],
                    "source_path": meta.get('sourceFilePath', 'unknown'),
                    "entity_type": entity_type,
                    "entities": transformed_entities,
                    "_chunk_metadata": chunk_metadata
                }
                
                # Save to file
                filename = f"{chunk_id}_{doc_name}.json"
                filepath = self.entities_dir / entity_type / filename
                filepath.parent.mkdir(parents=True, exist_ok=True)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(file_data, f, indent=2, ensure_ascii=False)
        
        return total_entities
    
    def _get_normalized_entities(self, chunk_file: Path, meta: Dict) -> List[Dict]:
        """Get all entities that were persisted for a chunk."""
        chunk_id = meta.get('chunkId', chunk_file.stem)
        doc_name = meta.get('document', 'unknown')
        filename = f"{chunk_id}_{doc_name}.json"
        
        all_entities = []
        
        # Read all entity files for this chunk
        for entity_dir in self.entities_dir.iterdir():
            if entity_dir.is_dir():
                entity_file = entity_dir / filename
                if entity_file.exists():
                    try:
                        with open(entity_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            entities = data.get('entities', [])
                            entity_type = entity_dir.name
                            
                            # Add type to each entity
                            for entity in entities:
                                entity_with_type = entity.copy()
                                entity_with_type['type'] = entity_type
                                all_entities.append(entity_with_type)
                    except Exception as e:
                        log.warning(f"Could not read entity file {entity_file}: {e}")
        
        return all_entities
    
    def _transform_and_persist_relationships(
        self, chunk_file: Path, meta: Dict, rel_parsed: Dict, all_entities: List[Dict]
    ) -> None:
        """Transform phase2_NEW relationship format to main pipeline format."""
        chunk_id = meta.get('chunkId', chunk_file.stem)
        doc_name = meta.get('document', 'unknown')
        
        # Get relationships from parsed result
        relationships = rel_parsed.get('relationships', [])
        if not isinstance(relationships, list):
            relationships = []
        
        # Transform relationships to match expected format
        transformed_relationships = []
        for rel in relationships:
            if not isinstance(rel, dict):
                continue
            
            # phase2_NEW uses 'relationship' field, main pipeline uses 'type'
            rel_type = rel.get('relationship') or rel.get('type')
            if not rel_type:
                continue
            
            transformed_rel = {
                "type": rel_type,
                "source": rel.get('source'),
                "target": rel.get('target')
            }
            
            # Add attributes if present
            if 'attributes' in rel:
                transformed_rel['attributes'] = rel['attributes']
            
            # Add evidence placeholder
            if '_evidence' not in transformed_rel:
                transformed_rel['_evidence'] = []
            
            # Add confidence placeholder
            if 'confidence' not in transformed_rel:
                transformed_rel['confidence'] = 0.9
            
            transformed_relationships.append(transformed_rel)
        
        # Create document provenance edges
        chunk_metadata = {
            "chunk_id": chunk_id,
            "document": doc_name,
            "document_type": meta.get('documentType', 'unknown'),
            "meeting_date": meta.get('meetingDate', ''),
            "source_file_name": meta.get('sourceFileName', chunk_file.name),
            "source_file_path": meta.get('sourceFilePath', 'unknown')
        }
        
        doc_edges = DocumentLinker.create_document_entity_relationships(
            all_entities, chunk_metadata, chunk_id
        )
        
        # Combine all relationships
        all_relationships = transformed_relationships + (doc_edges or [])
        
        if all_relationships:
            # Save relationships
            rel_data = {"relationships": all_relationships}
            rel_file = self.relationships_dir / f"{chunk_id}_{doc_name}.json"
            
            with open(rel_file, 'w', encoding='utf-8') as f:
                json.dump(rel_data, f, indent=2, ensure_ascii=False)
    
    def _update_entities_with_attributes(
        self, chunk_file: Path, meta: Dict, enhanced_by_type: Dict[str, List[Dict]]
    ) -> None:
        """Update persisted entities with enhanced attributes."""
        chunk_id = meta.get('chunkId', chunk_file.stem)
        doc_name = meta.get('document', 'unknown')
        filename = f"{chunk_id}_{doc_name}.json"
        
        for entity_type, enhanced_entities in enhanced_by_type.items():
            if not enhanced_entities:
                continue
            
            entity_file = self.entities_dir / entity_type / filename
            if entity_file.exists():
                try:
                    # Read existing data
                    with open(entity_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Update entities with enhanced versions
                    data['entities'] = enhanced_entities
                    data['_enhanced'] = True
                    
                    # Write back
                    with open(entity_file, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                        
                except Exception as e:
                    log.error(f"Failed to update {entity_type} entities: {e}")
