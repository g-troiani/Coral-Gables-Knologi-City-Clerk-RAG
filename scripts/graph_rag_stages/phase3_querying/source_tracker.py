"""
Source tracking component for GraphRAG queries to provide provenance information.
"""

import logging
from typing import Dict, List, Any, Set
from pathlib import Path

log = logging.getLogger(__name__)


class SourceTracker:
    """Track sources used during GraphRAG queries for provenance."""
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset tracking state."""
        self.entities_used: Dict[str, Dict[str, Any]] = {}
        self.relationships_used: Dict[str, Dict[str, Any]] = {}
        self.sources_used: Dict[str, Dict[str, Any]] = {}
        self.documents_referenced: Set[str] = set()
    
    def track_entity(self, entity_id: str, entity_data: Dict[str, Any]):
        """Track an entity being used in the query."""
        self.entities_used[entity_id] = {
            'id': entity_id,
            'title': entity_data.get('title', 'Unknown'),
            'type': entity_data.get('type', 'Unknown'),
            'description': str(entity_data.get('description', ''))[:200]  # Truncate
        }
    
    def track_relationship(self, rel_id: str, rel_data: Dict[str, Any]):
        """Track a relationship being used in the query."""
        self.relationships_used[rel_id] = {
            'id': rel_id,
            'source': rel_data.get('source', ''),
            'target': rel_data.get('target', ''),
            'description': str(rel_data.get('description', ''))[:200],
            'weight': rel_data.get('weight', 0)
        }
    
    def track_source_document(self, doc_id: str, doc_data: Dict[str, Any]):
        """Track a source document being used."""
        self.sources_used[doc_id] = {
            'id': doc_id,
            'title': doc_data.get('title', 'Unknown'),
            'type': doc_data.get('document_type', 'document'),
            'source_file': doc_data.get('source_file', '')
        }
        
        # Also add to documents referenced set
        if doc_data.get('source_file'):
            self.documents_referenced.add(doc_data['source_file'])
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all tracked sources."""
        return {
            'entities_count': len(self.entities_used),
            'relationships_count': len(self.relationships_used),
            'sources_count': len(self.sources_used),
            'documents_referenced': list(self.documents_referenced),
            'entities': list(self.entities_used.values()),
            'relationships': list(self.relationships_used.values()),
            'sources': list(self.sources_used.values())
        }
    
    def get_citation_text(self) -> str:
        """Generate citation text for the sources used."""
        if not self.sources_used and not self.documents_referenced:
            return "No specific sources tracked for this query."
        
        citations = []
        
        # Add document references
        if self.documents_referenced:
            doc_list = sorted(self.documents_referenced)
            citations.append(f"Documents referenced: {', '.join(doc_list[:5])}")
            if len(doc_list) > 5:
                citations.append(f"... and {len(doc_list) - 5} more documents")
        
        # Add entity information
        if self.entities_used:
            entity_count = len(self.entities_used)
            citations.append(f"Based on {entity_count} entities from the knowledge graph")
        
        return "; ".join(citations)
    
    def get_top_entities(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the top entities used in the query."""
        entities = list(self.entities_used.values())
        return entities[:limit]
    
    def has_sources(self) -> bool:
        """Check if any sources were tracked."""
        return bool(self.entities_used or self.relationships_used or self.sources_used) 