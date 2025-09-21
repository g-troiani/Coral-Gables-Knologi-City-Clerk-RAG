"""
Bidirectional Entity Resolution - Resolves entity references across Phase 1 and Phase 2
"""

from typing import Dict, List, Any, Optional
import logging
from .entity_bridge import EntityBridge

log = logging.getLogger(__name__)

class EntityResolver:
    """Resolves entity references across both phases."""
    
    def __init__(self, phase1_index: Dict, phase2_index: Dict):
        self.phase1_index = phase1_index
        self.phase2_index = phase2_index
        
    def resolve_entity(self, entity_ref: str) -> Optional[Dict]:
        """Resolve entity reference across both phases"""
        # Check Phase 2 first (more detailed)
        if entity_ref in self.phase2_index:
            return self.phase2_index[entity_ref]
            
        # Fall back to Phase 1
        if entity_ref in self.phase1_index:
            # Convert and return
            phase2_type, phase2_entity = EntityBridge.convert_phase1_to_phase2(
                self.phase1_index[entity_ref]
            )
            return phase2_entity
        
        return None
    
    def resolve_entity_by_name(self, entity_name: str, entity_type: Optional[str] = None) -> List[Dict]:
        """Resolve entity by name across both phases"""
        resolved_entities = []
        
        # Search Phase 2 first
        for entity_id, entity in self.phase2_index.items():
            # Check firstName + lastName combination for Person entities
            if entity.get('type') == 'Person':
                full_name = f"{entity.get('firstName', '')} {entity.get('lastName', '')}".strip()
                if full_name.lower() == entity_name.lower():
                    if entity_type is None or entity.get('type') == entity_type:
                        resolved_entities.append(entity)
            # For other entity types, check name field
            elif entity.get('name', '').lower() == entity_name.lower():
                if entity_type is None or entity.get('type') == entity_type:
                    resolved_entities.append(entity)
        
        # Search Phase 1 if no Phase 2 matches
        if not resolved_entities:
            for entity_id, entity in self.phase1_index.items():
                if entity.get('name', '').lower() == entity_name.lower():
                    if entity_type is None or entity.get('type') == entity_type:
                        # Convert to Phase 2 format
                        phase2_type, phase2_entity = EntityBridge.convert_phase1_to_phase2(entity)
                        resolved_entities.append(phase2_entity)
        
        return resolved_entities
    
    def get_entity_relationships(self, entity_ref: str) -> List[Dict]:
        """Get all relationships for an entity across both phases"""
        relationships = []
        
        # Check Phase 2 relationships
        entity = self.resolve_entity(entity_ref)
        if entity:
            # Look for relationships where this entity is involved
            entity_id = entity.get('personID') or entity.get('orgID') or entity.get('documentID')
            if entity_id:
                # This would need access to relationship indices
                # Implementation depends on how relationships are stored
                pass
        
        return relationships
    
    def find_related_entities(self, entity_ref: str, relationship_type: Optional[str] = None) -> List[Dict]:
        """Find entities related to the given entity"""
        related = []
        
        # This would traverse the relationship graph
        # Implementation depends on relationship storage structure
        
        return related
    
    def merge_entity_data(self, phase1_entity: Dict, phase2_entity: Dict) -> Dict:
        """Merge data from both phases for a single entity"""
        # Start with Phase 2 entity (more detailed)
        merged = phase2_entity.copy()
        
        # Add Phase 1 specific attributes that might not be in Phase 2
        phase1_only_fields = ['meeting_date', 'agenda_item_code', 'section_name']
        for field in phase1_only_fields:
            if field in phase1_entity and field not in merged:
                merged[field] = phase1_entity[field]
        
        # Add metadata about the merge
        merged['_merged_from_phases'] = [1, 2]
        merged['_phase1_source'] = phase1_entity.get('source', 'unknown')
        
        return merged
    
    def get_entity_provenance(self, entity_ref: str) -> Dict[str, Any]:
        """Get provenance information for an entity"""
        provenance = {
            'entity_id': entity_ref,
            'phases': [],
            'sources': [],
            'extraction_methods': []
        }
        
        # Check Phase 1
        if entity_ref in self.phase1_index:
            phase1_entity = self.phase1_index[entity_ref]
            provenance['phases'].append(1)
            provenance['sources'].append(phase1_entity.get('source', 'unknown'))
            provenance['extraction_methods'].append(phase1_entity.get('extraction_method', 'phase1'))
        
        # Check Phase 2
        if entity_ref in self.phase2_index:
            phase2_entity = self.phase2_index[entity_ref]
            provenance['phases'].append(2)
            provenance['sources'].append(phase2_entity.get('_source', 'unknown'))
            provenance['extraction_methods'].append(phase2_entity.get('extraction_method', 'phase2_ner'))
        
        return provenance 