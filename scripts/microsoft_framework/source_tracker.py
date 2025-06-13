"""Source tracking component for GraphRAG queries."""

from typing import Dict, List, Any, Set, Tuple, Optional
import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

class SourceTracker:
    """Track sources used during GraphRAG queries."""
    
    def __init__(self, linkage_file: Optional[Path] = None):
        self.linkage_file = linkage_file
        self.linkage_data: Optional[pd.DataFrame] = None
        self.reset()
        
        # Load linkage data if provided
        if linkage_file and linkage_file.exists():
            self.load_linkage_data(linkage_file)
    
    def reset(self):
        """Reset tracking state."""
        self.entities_used: Dict[int, Dict[str, Any]] = {}
        self.relationships_used: Dict[int, Dict[str, Any]] = {}
        self.sources_used: Dict[int, Dict[str, Any]] = {}
        self.text_units_used: Dict[int, Dict[str, Any]] = {}
        self.communities_used: Dict[int, Dict[str, Any]] = {}
    
    def track_entity(self, entity_id: int, entity_data: Dict[str, Any]):
        """Track an entity being used."""
        # Extract origin data for WP-7
        origin_data = {}
        if 'origin_chunk_id' in entity_data:
            origin_data['origin_chunk_id'] = entity_data['origin_chunk_id']
        if 'origin_section_id' in entity_data:
            origin_data['origin_section_id'] = entity_data['origin_section_id']
        if 'origin_doc_id' in entity_data:
            origin_data['origin_doc_id'] = entity_data['origin_doc_id']
        
        self.entities_used[entity_id] = {
            'id': entity_id,
            'title': entity_data.get('title', 'Unknown'),
            'type': entity_data.get('type', 'Unknown'),
            'description': entity_data.get('description', '')[:200],
            'source_id': entity_data.get('source_id', ''),
            **origin_data  # Include origin data for provenance
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
    
    def load_linkage_data(self, linkage_file: Path):
        """Load linkage data for entity-to-origin mapping."""
        try:
            self.linkage_data = pd.read_parquet(linkage_file)
            logger.info(f"Loaded linkage data with {len(self.linkage_data)} mappings")
        except Exception as e:
            logger.warning(f"Failed to load linkage data from {linkage_file}: {e}")
            self.linkage_data = None
    
    def get_entity_provenance(self, entity_id: str) -> Dict[str, Any]:
        """Get provenance information for an entity using linkage data."""
        if self.linkage_data is None:
            return {}
        
        # Find the entity in linkage data
        entity_rows = self.linkage_data[self.linkage_data['graphrag_entity_id'] == entity_id]
        if entity_rows.empty:
            return {}
        
        entity_row = entity_rows.iloc[0]
        return {
            'origin_doc_id': entity_row.get('origin_doc_id'),
            'origin_section_id': entity_row.get('origin_section_id'),
            'origin_chunk_id': entity_row.get('origin_chunk_id')
        }
    
    def get_entities_by_section(self, section_id: str) -> List[Dict[str, Any]]:
        """Get all entities from a specific section."""
        if self.linkage_data is None:
            return []
        
        section_entities = self.linkage_data[
            self.linkage_data['origin_section_id'] == section_id
        ]
        return section_entities.to_dict('records')
    
    def get_enhanced_summary(self) -> Dict[str, Any]:
        """Get enhanced summary with provenance information."""
        summary = self.get_summary()
        
        # Add provenance to entities if linkage data available
        if self.linkage_data is not None:
            for entity in summary['entities']:
                entity_id = str(entity.get('id', ''))
                provenance = self.get_entity_provenance(entity_id)
                entity.update(provenance)
        
        return summary 