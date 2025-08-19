"""
Unified entity builder for standardized entity creation across all pipelines.
This module ensures consistent entity creation, ID generation, and attribute validation.
"""

import logging
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
from datetime import datetime

from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
from scripts.graph_rag_stages.common.entity_factory import EntityFactory
from scripts.graph_rag_stages.common.standards import ensure_min_entity_props

log = logging.getLogger(__name__)


class UnifiedEntityBuilder:
    """
    Centralized entity creation for both taxonomy and NER pipelines.
    Ensures all entities follow the same standards regardless of source.
    """
    
    def __init__(self, registry_dir: Optional[Path] = None):
        """
        Initialize the unified entity builder.
        
        Args:
            registry_dir: Optional directory for entity registry (for deduplication)
        """
        self.registry_dir = registry_dir
        self.entity_factory = EntityFactory()
        self.created_entities = {}  # Track entities created in this session
        self.entity_registry = {}   # Global registry for deduplication
        
        # Load existing registry if provided
        if self.registry_dir:
            self._load_registry()
    
    def create_entity(self, 
                     entity_type: str, 
                     attributes: Dict[str, Any], 
                     source: str,
                     source_metadata: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], bool]:
        """
        Create a standardized entity with proper ID and validation.
        
        Args:
            entity_type: Ontology entity type (e.g., 'Person', 'Organization')
            attributes: Entity attributes
            source: Source identifier ('taxonomy', 'ner', etc.)
            source_metadata: Additional metadata about the source
            
        Returns:
            Tuple of (entity_dict, is_new) where is_new indicates if this is a new entity
        """
        # Validate entity type
        if entity_type not in UnifiedOntology.get_entity_categories():
            log.warning(f"Unknown entity type: {entity_type}")
            return None, False
        
        # Get or generate entity ID
        entity_id = self._get_or_generate_id(entity_type, attributes)
        
        # Check if entity already exists
        registry_key = f"{entity_type}:{entity_id}"
        if registry_key in self.entity_registry:
            existing = self.entity_registry[registry_key]
            # Merge attributes if needed
            merged = self._merge_entity_attributes(existing, attributes, source)
            return merged, False
        
        # Create new entity using factory
        entity = self.entity_factory.create_entity(
            entity_type=entity_type,
            entity_id=entity_id,
            attributes=attributes
        )
        
        # Add source tracking
        entity['_sources'] = [source]
        if source_metadata:
            entity['_source_metadata'] = {source: source_metadata}
        
        # Ensure minimum properties
        entity = ensure_min_entity_props(entity)
        
        # Register entity
        self.entity_registry[registry_key] = entity
        self.created_entities[registry_key] = entity
        
        log.debug(f"Created new {entity_type} entity: {entity_id} from {source}")
        
        return entity, True
    
    def create_relationship(self,
                          source_id: str,
                          target_id: str,
                          relationship_type: str,
                          attributes: Optional[Dict[str, Any]] = None,
                          source: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a standardized relationship.
        
        Args:
            source_id: Source entity ID
            target_id: Target entity ID  
            relationship_type: Type of relationship
            attributes: Optional relationship attributes
            source: Source identifier
            
        Returns:
            Relationship dictionary
        """
        # Validate entities exist
        source_type = self._get_entity_type(source_id)
        target_type = self._get_entity_type(target_id)
        
        if not source_type or not target_type:
            log.warning(f"Cannot create relationship - missing entities: {source_id} -> {target_id}")
            return None
        
        relationship = {
            'source': source_id,
            'source_type': source_type,
            'target': target_id,
            'target_type': target_type,
            'type': relationship_type,
            'attributes': attributes or {},
            '_source': source,
            '_created_at': datetime.utcnow().isoformat()
        }
        
        return relationship
    
    def _get_or_generate_id(self, entity_type: str, attributes: Dict[str, Any]) -> str:
        """Get existing ID or generate a new one based on entity type and attributes."""
        # Check if ID already provided
        id_field = EntityIDStandards.get_id_field(entity_type)
        if id_field in attributes and attributes[id_field]:
            return attributes[id_field]
        
        # Generate ID based on entity type
        if entity_type == 'Person':
            name = attributes.get('name', '')
            return EntityIDStandards.make_person_id(name)
        
        elif entity_type == 'Organization':
            name = attributes.get('name', '')
            return EntityIDStandards.make_organization_id(name)
        
        elif entity_type == 'Document':
            title = attributes.get('title', '')
            doc_type = attributes.get('documentType', 'document')
            return EntityIDStandards.make_document_id(title, doc_type)
        
        elif entity_type == 'Meeting':
            date = attributes.get('date', '')
            meeting_type = attributes.get('meetingType', 'meeting')
            return EntityIDStandards.make_meeting_id(date, meeting_type)
        
        elif entity_type == 'Policy':
            policy_type = attributes.get('policyType', 'policy')
            year = attributes.get('year', '')
            number = attributes.get('number', '')
            title = attributes.get('title', '')
            return EntityIDStandards.make_policy_id(policy_type, year, number, title)
        
        else:
            # Generic ID generation
            name = attributes.get('name') or attributes.get('title') or 'unknown'
            return f"{entity_type.lower()}_{EntityIDStandards._hash8(name)}"
    
    def _merge_entity_attributes(self, 
                               existing: Dict[str, Any], 
                               new_attrs: Dict[str, Any], 
                               source: str) -> Dict[str, Any]:
        """Merge attributes from different sources intelligently."""
        merged = existing.copy()
        
        # Track sources
        if '_sources' not in merged:
            merged['_sources'] = []
        if source not in merged['_sources']:
            merged['_sources'].append(source)
        
        # Merge attributes (prefer non-empty values)
        for key, value in new_attrs.items():
            if key.startswith('_'):
                continue  # Skip internal fields
                
            if key not in merged or not merged[key]:
                merged[key] = value
            elif isinstance(value, list) and isinstance(merged[key], list):
                # Merge lists (avoid duplicates)
                for item in value:
                    if item not in merged[key]:
                        merged[key].append(item)
            elif isinstance(value, dict) and isinstance(merged[key], dict):
                # Merge dicts recursively
                merged[key].update(value)
            # For other types, keep existing value unless new is more complete
            elif value and len(str(value)) > len(str(merged[key])):
                merged[key] = value
        
        return merged
    
    def _get_entity_type(self, entity_id: str) -> Optional[str]:
        """Get entity type for a given ID from registry."""
        for registry_key in self.entity_registry:
            entity_type, eid = registry_key.split(':', 1)
            if eid == entity_id:
                return entity_type
        return None
    
    def _load_registry(self):
        """Load existing entity registry from disk."""
        if not self.registry_dir or not self.registry_dir.exists():
            return
            
        # TODO: Implement registry loading from manifest files
        # This would read the deduplicated entity manifests
        pass
    
    def save_entities(self, output_dir: Path):
        """Save created entities to the specified directory."""
        for registry_key, entity in self.created_entities.items():
            entity_type, entity_id = registry_key.split(':', 1)
            
            # Create directory for entity type
            type_dir = output_dir / entity_type
            type_dir.mkdir(parents=True, exist_ok=True)
            
            # Save entity
            entity_file = type_dir / f"{entity_id}.json"
            import json
            with open(entity_file, 'w', encoding='utf-8') as f:
                json.dump(entity, f, indent=2, ensure_ascii=False)
        
        log.info(f"Saved {len(self.created_entities)} entities to {output_dir}")
    
    def get_statistics(self) -> Dict[str, int]:
        """Get statistics about created entities."""
        stats = {}
        for registry_key in self.created_entities:
            entity_type, _ = registry_key.split(':', 1)
            stats[entity_type] = stats.get(entity_type, 0) + 1
        return stats
