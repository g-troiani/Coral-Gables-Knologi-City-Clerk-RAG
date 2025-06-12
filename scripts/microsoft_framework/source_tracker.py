"""Source tracking component for GraphRAG queries."""

from typing import Dict, List, Any, Set, Tuple
import logging

logger = logging.getLogger(__name__)

class SourceTracker:
    """Track sources used during GraphRAG queries."""
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset tracking state."""
        self.entities_used: Dict[int, Dict[str, Any]] = {}
        self.relationships_used: Dict[int, Dict[str, Any]] = {}
        self.sources_used: Dict[int, Dict[str, Any]] = {}
        self.text_units_used: Dict[int, Dict[str, Any]] = {}
        self.communities_used: Dict[int, Dict[str, Any]] = {}
    
    def track_entity(self, entity_id: int, entity_data: Dict[str, Any]):
        """Track an entity being used."""
        self.entities_used[entity_id] = {
            'id': entity_id,
            'title': entity_data.get('title', 'Unknown'),
            'type': entity_data.get('type', 'Unknown'),
            'description': entity_data.get('description', '')[:200],
            'source_id': entity_data.get('source_id', '')
        }
    
    def track_relationship(self, rel_id: int, rel_data: Dict[str, Any]):
        """Track a relationship being used."""
        self.relationships_used[rel_id] = {
            'id': rel_id,
            'source': rel_data.get('source', ''),
            'target': rel_data.get('target', ''),
            'description': rel_data.get('description', '')[:200],
            'weight': rel_data.get('weight', 0)
        }
    
    def track_source(self, source_id: int, source_data: Dict[str, Any]):
        """Track a source document being used."""
        self.sources_used[source_id] = {
            'id': source_id,
            'title': source_data.get('title', 'Unknown'),
            'type': source_data.get('document_type', 'document'),
            'file': source_data.get('source_file', '')
        }
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all tracked sources."""
        return {
            'entities': list(self.entities_used.values()),
            'relationships': list(self.relationships_used.values()),
            'sources': list(self.sources_used.values()),
            'text_units': list(self.text_units_used.values()),
            'communities': list(self.communities_used.values())
        }
    
    def get_citation_map(self) -> Dict[str, List[int]]:
        """Get a map of content to source IDs for citations."""
        return {
            'entity_ids': list(self.entities_used.keys()),
            'relationship_ids': list(self.relationships_used.keys()),
            'source_ids': list(self.sources_used.keys())
        } 