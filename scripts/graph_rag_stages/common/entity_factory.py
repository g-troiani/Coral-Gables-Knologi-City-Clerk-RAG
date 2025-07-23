"""
Centralized entity creation to ensure consistent ID fields.
"""

from typing import Dict, Any, Optional
import hashlib
from .entity_id_standards import EntityIDStandards

class EntityFactory:
    """Factory for creating entities with consistent ID fields."""
    
    @staticmethod
    def create_entity(entity_type: str, name: str, **attributes) -> Dict[str, Any]:
        """Create an entity with the correct ID field."""
        # Get the correct ID field
        id_field = EntityIDStandards.get_id_field(entity_type)
        
        # Generate ID if not provided
        entity_id = attributes.get(id_field)
        if not entity_id:
            # Generate consistent ID
            entity_id = EntityFactory._generate_entity_id(entity_type, name)
        
        # Build entity with correct ID field
        entity = {
            'type': entity_type,
            'name': name,
            id_field: entity_id,  # Use the CORRECT ID field
            'id': entity_id,      # Also add generic 'id' for compatibility
            **attributes
        }
        
        # Remove any incorrect ID fields
        incorrect_fields = ['docID', 'agendaID']  # Add more as needed
        for field in incorrect_fields:
            if field in entity:
                del entity[field]
        
        return entity
    
    @staticmethod
    def _generate_entity_id(entity_type: str, name: str) -> str:
        """Generate consistent entity ID."""
        normalized_name = name.lower().replace(' ', '_')[:20]
        hash_part = hashlib.sha256(f"{entity_type}_{name}".encode()).hexdigest()[:6]
        return f"{entity_type.lower()}_{normalized_name}_{hash_part}"
    
    @staticmethod
    def validate_entity(entity: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and fix entity ID fields."""
        entity_type = entity.get('type')
        if not entity_type:
            raise ValueError("Entity must have a 'type' field")
        
        # Normalize ID fields
        return EntityIDStandards.normalize_entity_id_fields(entity, entity_type) 