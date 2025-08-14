"""
Centralized entity ID field standards to ensure consistency across the pipeline.
"""

class EntityIDStandards:
    """Single source of truth for entity ID field names."""
    
    # Standard ID field for each entity type
    ID_FIELD_MAP = {
        'Person': 'personID',
        'Organization': 'orgID', 
        'Location': 'locationID',
        'Event': 'eventID',
        'Document': 'documentID',  # NOT docID
        'AgendaItem': 'agendaItemID',  # NOT agendaID
        'Policy': 'policyID',
        'Asset': 'assetID',
        'Contract': 'contractID',
        'Project': 'projectID',
        'Role': 'roleID',
        'Action': 'actionID',
        'Topic': 'topicID',
        'Section': 'sectionID',
        'Technology': 'technologyID',
        'VoteOutcome': 'voteOutcomeID',
        'Transcript': 'transcriptID',
        'Presentation': 'presentationID',
        'PublicComment': 'publicCommentID',
        'Issue': 'issueID',
        'Recommendation': 'recommendationID',
        'HistoricalReference': 'historicalReferenceID',
        'Appointment': 'appointmentID',
        'Board': 'boardID',
        'LegalReference': 'legalReferenceID',
        'AgendaDocument': 'agendaDocID'
    }
    
    @classmethod
    def get_id_field(cls, entity_type: str) -> str:
        """Get the standard ID field name for an entity type."""
        return cls.ID_FIELD_MAP.get(entity_type, f"{entity_type.lower()}ID")
    
    @classmethod
    def normalize_entity_id_fields(cls, entity: dict, entity_type: str) -> dict:
        """Normalize entity to use standard ID field."""
        normalized = entity.copy()
        id_field = cls.get_id_field(entity_type)
        
        # Handle various ID field variations
        possible_id_fields = [
            'id', 'ID', id_field,
            f"{entity_type.lower()}ID",
            f"{entity_type}ID",
            'docID' if entity_type == 'Document' else None,
            'agendaID' if entity_type == 'AgendaItem' else None
        ]
        
        # Find the ID value from possible fields
        id_value = None
        for field in possible_id_fields:
            if field and field in entity:
                id_value = entity[field]
                break
        
        if id_value:
            # Set the standard ID field
            normalized[id_field] = id_value
            # Also keep 'id' for compatibility
            normalized['id'] = id_value
            
            # Remove non-standard fields
            for field in possible_id_fields:
                if field and field != id_field and field != 'id' and field in normalized:
                    del normalized[field]
        
        return normalized 
    
    # Prefix to type mapping for ID inference
    PREFIX_TYPE_MAP = {
        "person_": "Person",
        "org_": "Organization",
        "document_": "Document",
        "policy_": "Policy",
        "agendaItem_": "AgendaItem",
        "agendaDoc_": "AgendaDocument",
        "section_": "Section",
        "event_": "Event",
        "action_": "Action",
        "location_": "Location",
        "topic_": "Topic",
        "asset_": "Asset",
        "contract_": "Contract",
        "technology_": "Technology",
        "vote": "VoteOutcome",      # e.g., "vote_outcome_..."
        "project_": "Project",
        "meeting_": "Meeting",
    }

    @staticmethod
    def infer_type_from_id(entity_id: str) -> str | None:
        if not entity_id:
            return None
        for prefix, t in EntityIDStandards.PREFIX_TYPE_MAP.items():
            if entity_id.startswith(prefix):
                return t
        return None

    @staticmethod
    def canonicalize_id(entity_id: str, known_ids: set[str]) -> str | None:
        if not entity_id:
            return None
        if entity_id in known_ids:
            return entity_id
        candidates = {
            entity_id.replace("-", "_"),
            entity_id.replace("_", "-"),
            entity_id.lower(),
        }
        for c in list(candidates):
            if c in known_ids:
                return c
            # try case-insensitive match
            for k in known_ids:
                if k.lower() == c:
                    return k
        return None