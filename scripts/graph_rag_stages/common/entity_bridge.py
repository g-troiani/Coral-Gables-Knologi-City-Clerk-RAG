"""
Bridge between Phase 1 and Phase 2 entity representations
"""

from typing import Dict, List, Any, Optional, Tuple
from .unified_ontology import UnifiedOntology
import hashlib
import re

class EntityBridge:
    """Handles entity conversion and metadata preservation."""
    
    @staticmethod
    def convert_phase1_to_phase2(phase1_entity: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Convert Phase 1 entity to Phase 2 format preserving all metadata."""
        phase1_type = phase1_entity.get('type', '')
        phase2_type = UnifiedOntology.normalize_entity_type(phase1_type)
        
        # Generate consistent ID
        entity_name = phase1_entity.get('name', '')
        entity_id = EntityBridge._generate_entity_id(phase2_type, entity_name)
        id_field = UnifiedOntology.get_id_field_name(phase2_type)
        
        # Base entity with required fields
        phase2_entity = {
            id_field: entity_id,
            "name": entity_name,
            "_phase1_type": phase1_type,
            "_source": "phase1_extraction"
        }
        
        # Preserve all original attributes
        for key, value in phase1_entity.items():
            if key not in ['type', 'name'] and key not in phase2_entity:
                phase2_entity[key] = value
        
        # Add type-specific processing
        EntityBridge._enhance_entity_attributes(phase2_entity, phase2_type, entity_name)
        
        return phase2_type, phase2_entity
    
    @staticmethod
    def _generate_entity_id(entity_type: str, entity_name: str) -> str:
        """Generate consistent entity ID."""
        from .graph_entity_toolkit import GraphEntityToolkit
        return GraphEntityToolkit.generate_entity_id(entity_type, {'name': entity_name})
    
    @staticmethod
    def _enhance_entity_attributes(entity: Dict, entity_type: str, entity_name: str):
        """Add type-specific attribute enhancements."""
        if entity_type == 'Asset' and '$' in entity_name:
            # Extract monetary value
            amount_match = re.search(r'\$[\d,]+(?:\.\d+)?(?:\s*(?:million|billion))?', entity_name)
            if amount_match:
                entity["value"] = amount_match.group(0)
                entity["type"] = "financial"
        elif entity_type == 'Person' and 'description' in entity:
            # Try to extract title from description
            desc = entity.get('description', '')
            title_patterns = ['Mayor', 'Commissioner', 'Manager', 'Attorney', 'Clerk']
            for pattern in title_patterns:
                if pattern in desc:
                    entity["title"] = pattern
                    break 