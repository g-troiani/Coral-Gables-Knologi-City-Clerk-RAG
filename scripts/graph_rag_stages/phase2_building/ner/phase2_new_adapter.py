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

from scripts.graph_rag_stages.phase2_NEW.simple_ner_split import (
    extract_entities_split as extract_entities, 
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
        log.info(f"🔍 [ADAPTER] Starting process_chunk() for: {chunk_file.name}")
        
        try:
            # Parse chunk file using phase2_NEW logic
            log.info(f"📄 [ADAPTER] Parsing chunk file: {chunk_file}")
            meta, text = parse_chunk_file(str(chunk_file))
            
            log.info(f"📋 [ADAPTER] Parsed metadata: {meta}")
            log.info(f"📝 [ADAPTER] Text length: {len(text)} characters")
            log.info(f"📝 [ADAPTER] Text preview: {text[:200]}...")
            
            # Extract entities using phase2_NEW
            log.info(f"🔍 [ADAPTER] Calling extract_entities() with phase2_NEW logic")
            log.info(f"   📄 Document type: {meta.get('documentType', 'unknown')}")
            log.info(f"   📅 Meeting date: {meta.get('meetingDate', 'unknown')}")
            log.info(f"   📁 Source file: {meta.get('sourceFileName', 'unknown')}")
            
            result, raw_text, rel_template, attr_template, sys_prompt = extract_entities(
                text,
                document_type=meta.get('documentType', 'unknown'),
                meeting_date=meta.get('meetingDate', 'unknown'),
                source_file=meta.get('sourceFileName', 'unknown'),
            )
            
            log.info(f"✅ [ADAPTER] extract_entities() completed")
            log.info(f"   📊 Result keys: {list(result.keys()) if isinstance(result, dict) else type(result)}")
            if isinstance(result, dict) and 'entities' in result:
                entities_summary = {}
                for entity_type, entities in result['entities'].items():
                    entities_summary[entity_type] = len(entities) if isinstance(entities, list) else 0
                log.info(f"   📈 Entities extracted: {entities_summary}")
            log.info(f"   🔗 Relationship template available: {bool(rel_template)}")
            log.info(f"   🏷️ Attribute template available: {bool(attr_template)}")
            
            # Transform and persist entities
            log.info(f"🔄 [ADAPTER] Transforming and persisting entities")
            total_entities = self._transform_and_persist_entities(
                chunk_file, meta, result, raw_text
            )
            log.info(f"✅ [ADAPTER] Entity persistence completed: {total_entities} entities saved")
            
            # Extract and persist relationships if we have entities
            if total_entities > 0 and rel_template:
                log.info(f"🔗 [ADAPTER] Processing relationships (entities: {total_entities}, template: available)")
                
                # Get normalized entities for relationship extraction
                log.info(f"📋 [ADAPTER] Getting normalized entities for relationship extraction")
                norm_flat = self._get_normalized_entities(chunk_file, meta)
                log.info(f"📊 [ADAPTER] Normalized entities count: {len(norm_flat)}")
                
                if norm_flat:
                    log.info(f"🔍 [ADAPTER] Calling extract_relationships()")
                    rel_parsed, rel_text = extract_relationships(
                        text, rel_template, sys_prompt, norm_flat
                    )
                    
                    relationships_count = 0
                    if isinstance(rel_parsed, dict) and 'relationships' in rel_parsed:
                        relationships_count = len(rel_parsed['relationships'])
                    log.info(f"✅ [ADAPTER] extract_relationships() completed: {relationships_count} relationships")
                    
                    log.info(f"💾 [ADAPTER] Persisting relationships")
                    self._transform_and_persist_relationships(
                        chunk_file, meta, rel_parsed, norm_flat
                    )
                    log.info(f"✅ [ADAPTER] Relationship persistence completed")
                    
                    # Extract attributes if template available
                    if attr_template:
                        log.info(f"🏷️ [ADAPTER] Processing attributes (template available)")
                        by_type = _group_by_type(norm_flat)
                        log.info(f"📊 [ADAPTER] Entities grouped by type: {list(by_type.keys())}")
                        
                        log.info(f"🔍 [ADAPTER] Calling extract_attributes()")
                        enhanced_by_type, _ = extract_attributes(
                            text, attr_template, sys_prompt, by_type
                        )
                        
                        enhanced_count = sum(len(entities) for entities in enhanced_by_type.values()) if enhanced_by_type else 0
                        log.info(f"✅ [ADAPTER] extract_attributes() completed: {enhanced_count} enhanced entities")
                        
                        # Update entities with enhanced attributes
                        if enhanced_by_type:
                            log.info(f"🔄 [ADAPTER] Updating entities with enhanced attributes")
                            self._update_entities_with_attributes(
                                chunk_file, meta, enhanced_by_type
                            )
                            log.info(f"✅ [ADAPTER] Attribute enhancement completed")
                        else:
                            log.info(f"ℹ️ [ADAPTER] No enhanced attributes to update")
                    else:
                        log.info(f"ℹ️ [ADAPTER] No attribute template available, skipping attribute extraction")
                else:
                    log.warning(f"⚠️ [ADAPTER] No normalized entities available for relationship extraction")
            else:
                if total_entities == 0:
                    log.warning(f"⚠️ [ADAPTER] No entities extracted, skipping relationship processing")
                if not rel_template:
                    log.info(f"ℹ️ [ADAPTER] No relationship template available, skipping relationship extraction")
            
            log.info(f"✅ [ADAPTER] process_chunk() completed successfully for {chunk_file.name}")
            log.info(f"   📊 Final result: {total_entities} entities processed")
            return total_entities
            
        except Exception as e:
            log.error(f"❌ [ADAPTER] Error processing chunk {chunk_file}: {e}")
            log.exception(f"❌ [ADAPTER] Full exception details:")
            return 0
    
    def _transform_and_persist_entities(
        self, chunk_file: Path, meta: Dict, result: Dict, raw_text: str
    ) -> int:
        """Transform phase2_NEW entity format to main pipeline format and persist."""
        log.info(f"🔄 [TRANSFORM] Starting entity transformation and persistence")
        
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
        
        log.info(f"📋 [TRANSFORM] Chunk metadata: {chunk_metadata}")
        
        # Get entities from result - handle both wrapped and direct formats
        if 'entities' in result and isinstance(result['entities'], dict):
            # Wrapped format: {"entities": {"Person": [...], "Organization": [...]}}
            entities_root = result['entities']
        elif isinstance(result, dict) and any(key in result for key in ['Person', 'Organization', 'Document', 'AgendaDocument', 'Section', 'AgendaItem', 'Policy', 'Contract', 'Technology', 'VoteOutcome', 'Event', 'Location', 'Asset', 'Project', 'Role', 'Topic', 'Action']):
            # Direct format: {"Person": [...], "Organization": [...]}
            entities_root = result
        else:
            log.warning(f"⚠️ [TRANSFORM] No entities found in result structure: {list(result.keys()) if isinstance(result, dict) else type(result)}")
            return 0
        
        log.info(f"📊 [TRANSFORM] Raw entity types found: {list(entities_root.keys())}")
        
        # Process each entity type
        for entity_type, entities in entities_root.items():
            if not isinstance(entities, list) or not entities:
                log.info(f"⏭️ [TRANSFORM] Skipping {entity_type}: no entities or not a list")
                continue
            
            log.info(f"🔄 [TRANSFORM] Processing {entity_type}: {len(entities)} raw entities")
            
            # Transform entities to match expected format
            transformed_entities = []
            validation_failures = 0
            
            for i, entity in enumerate(entities):
                if not isinstance(entity, dict):
                    log.warning(f"⚠️ [TRANSFORM] Skipping non-dict entity at index {i}")
                    continue
                
                # Ensure proper ID field
                try:
                    log.debug(f"🔍 [TRANSFORM] Validating entity {i+1}/{len(entities)}: {entity.get('name', 'unnamed')}")
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
                    
                    # Add source file attributes to entity
                    if chunk_metadata.get('source_file_name'):
                        validated_entity['Source_File_Name'] = chunk_metadata['source_file_name']
                    if meta.get('sourceFilePath'):
                        validated_entity['Source_File_Path'] = meta.get('sourceFilePath')
                    
                    transformed_entities.append(validated_entity)
                    total_entities += 1
                    
                except ValueError as e:
                    log.warning(f"⚠️ [TRANSFORM] Invalid entity skipped at index {i}: {e}")
                    validation_failures += 1
                    continue
            
            log.info(f"📊 [TRANSFORM] {entity_type} transformation results:")
            log.info(f"   ✅ Valid entities: {len(transformed_entities)}")
            log.info(f"   ❌ Validation failures: {validation_failures}")
            
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
                
                log.info(f"💾 [TRANSFORM] Saving {entity_type} entities to: {filepath}")
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(file_data, f, indent=2, ensure_ascii=False)
                
                log.info(f"✅ [TRANSFORM] Successfully saved {len(transformed_entities)} {entity_type} entities")
            else:
                log.info(f"⏭️ [TRANSFORM] No valid {entity_type} entities to save")
        
        log.info(f"✅ [TRANSFORM] Entity transformation completed: {total_entities} total entities processed")
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
        log.info(f"🔗 [REL_TRANSFORM] Starting relationship transformation and persistence")
        
        chunk_id = meta.get('chunkId', chunk_file.stem)
        doc_name = meta.get('document', 'unknown')
        
        log.info(f"📋 [REL_TRANSFORM] Processing relationships for chunk: {chunk_id}")
        log.info(f"📊 [REL_TRANSFORM] Available entities for linking: {len(all_entities)}")
        
        # Get relationships from parsed result
        relationships = rel_parsed.get('relationships', [])
        if not isinstance(relationships, list):
            relationships = []
        
        log.info(f"🔍 [REL_TRANSFORM] Raw relationships found: {len(relationships)}")
        
        # Transform relationships to match expected format
        transformed_relationships = []
        relationship_failures = 0
        
        for i, rel in enumerate(relationships):
            if not isinstance(rel, dict):
                log.warning(f"⚠️ [REL_TRANSFORM] Skipping non-dict relationship at index {i}")
                relationship_failures += 1
                continue
            
            # phase2_NEW uses 'relationship' field, main pipeline uses 'type'
            rel_type = rel.get('relationship') or rel.get('type')
            if not rel_type:
                log.warning(f"⚠️ [REL_TRANSFORM] Skipping relationship without type at index {i}")
                relationship_failures += 1
                continue
            
            log.debug(f"🔄 [REL_TRANSFORM] Processing relationship {i+1}: {rel_type} ({rel.get('source')} -> {rel.get('target')})")
            
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
        
        log.info(f"📊 [REL_TRANSFORM] Relationship transformation results:")
        log.info(f"   ✅ Valid relationships: {len(transformed_relationships)}")
        log.info(f"   ❌ Transformation failures: {relationship_failures}")
        
        # Create document provenance edges
        log.info(f"📄 [REL_TRANSFORM] Creating document provenance edges")
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
        
        doc_edges_count = len(doc_edges) if doc_edges else 0
        log.info(f"📄 [REL_TRANSFORM] Document provenance edges created: {doc_edges_count}")
        
        # Combine all relationships
        all_relationships = transformed_relationships + (doc_edges or [])
        
        log.info(f"📊 [REL_TRANSFORM] Total relationships to persist: {len(all_relationships)}")
        log.info(f"   🔗 Entity relationships: {len(transformed_relationships)}")
        log.info(f"   📄 Document provenance: {doc_edges_count}")
        
        if all_relationships:
            # Save relationships
            rel_data = {"relationships": all_relationships}
            rel_file = self.relationships_dir / f"{chunk_id}_{doc_name}.json"
            
            log.info(f"💾 [REL_TRANSFORM] Saving relationships to: {rel_file}")
            
            with open(rel_file, 'w', encoding='utf-8') as f:
                json.dump(rel_data, f, indent=2, ensure_ascii=False)
            
            log.info(f"✅ [REL_TRANSFORM] Successfully saved {len(all_relationships)} relationships")
        else:
            log.info(f"ℹ️ [REL_TRANSFORM] No relationships to save")
    
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

