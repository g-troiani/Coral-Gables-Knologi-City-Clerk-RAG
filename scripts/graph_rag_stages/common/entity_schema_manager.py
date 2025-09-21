"""
Manages flexible entity schemas across extraction phases
"""

from typing import Dict, List, Any, Set
import json

class EntitySchemaManager:
    """Manages entity schemas with flexibility for additional attributes."""
    
    def __init__(self):
        # Core attributes required by ontology
        self.core_attributes = {
            'Document': {'documentID', 'title', 'type'},
            'Person': {'personID', 'firstName', 'lastName'},
            'Organization': {'orgID', 'name'},
            # ... other entity types
        }
        
        # Extended attributes discovered during extraction
        self.extended_attributes = {
            'Document': {
                'Source_File_Name', 'Source_File_Path', 'meeting_date',
                'agenda_item_code', 'document_number', 'full_text', 'pages',
                'legal_metadata', 'outcome_status', 'vote_details'
            }
        }
    
    def merge_entity_schemas(self, phase1_entity: Dict, phase2_type: str) -> Dict[str, Any]:
        """Merge Phase 1 rich attributes with Phase 2 schema."""
        
        # Start with Phase 2 core attributes
        merged = {}
        
        # Map common attributes
        attribute_mappings = {
            'Document': {
                'document_number': 'documentID',
                'document_type': 'type',
                'title': 'title',
                'meeting_date': 'issueDate',
                'outcome_status': 'status'
            }
        }
        
        # Apply mappings
        if phase2_type in attribute_mappings:
            for p1_attr, p2_attr in attribute_mappings[phase2_type].items():
                if p1_attr in phase1_entity:
                    merged[p2_attr] = phase1_entity[p1_attr]
        
        # Preserve ALL Phase 1 attributes as extended attributes
        merged['_extended'] = {}
        for key, value in phase1_entity.items():
            if key not in merged:
                merged['_extended'][key] = value
        
        return merged

    def create_queryable_entity(self, entity_data: Dict, entity_type: str) -> Dict:
        """Create entity that preserves all attributes for querying."""
        queryable = {
            '_type': entity_type,
            '_core': {},
            '_extended': {}
        }
        
        # Separate core and extended attributes
        core_attrs = self.core_attributes.get(entity_type, set())
        
        for key, value in entity_data.items():
            if key in core_attrs:
                queryable['_core'][key] = value
            else:
                queryable['_extended'][key] = value
        
        # Flatten for easier querying
        flat_entity = {'_type': entity_type}
        flat_entity.update(queryable['_core'])
        flat_entity.update(queryable['_extended'])
        
        return flat_entity 