"""
Links entities extracted from chunks back to their source documents
"""

from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
import json
import logging

log = logging.getLogger(__name__)

class DocumentLinker:
    """Creates relationships between extracted entities and source documents."""
    
    @staticmethod
    def extract_document_metadata(chunk_file: Path) -> Dict[str, str]:
        """Extract document metadata from chunk file header."""
        metadata = {}
        
        with open(chunk_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        if "---" in content:
            header, _ = content.split("---", 1)
            
            for line in header.strip().split("\n"):
                if line.startswith("#") and ":" in line:
                    key_value = line[1:].strip().split(":", 1)
                    if len(key_value) == 2:
                        key = key_value[0].strip()
                        value = key_value[1].strip()
                        metadata[key] = value
        
        return metadata
    
    @staticmethod
    def create_document_entity_relationships(
        entities: List[Dict], 
        chunk_metadata: Dict,
        chunk_id: str
    ) -> List[Dict[str, Any]]:
        """Create relationships between entities and their source document."""
        relationships = []
        
        # Extract document info
        source_file = chunk_metadata.get('Source_File_Name', '')
        doc_type = chunk_metadata.get('document_type', 'document')
        meeting_date = chunk_metadata.get('meeting_date', '')
        
        # Generate document ID based on type
        if doc_type == 'agenda':
            doc_id = f"document_agenda_{meeting_date.replace('.', '_')}"
        elif doc_type in ['ordinance', 'resolution']:
            doc_number = chunk_metadata.get('document_number', chunk_id)
            doc_id = f"document_{doc_type}_{doc_number}"
        else:
            doc_id = f"document_{source_file.replace('.pdf', '').replace(' ', '_')}"
        
        # Create relationships for each entity
        for entity in entities:
            entity_id = DocumentLinker._get_entity_id(entity)
            if entity_id:
                relationships.append({
                    "type": "extractedFrom",
                    "source": entity_id,
                    "target": doc_id,
                    "attributes": {
                        "chunk_id": chunk_id,
                        "extraction_method": "ner",
                        "source_file": source_file
                    }
                })
        
        # Add section relationships if present
        if 'section_name' in chunk_metadata and chunk_metadata['section_name']:
            section_id = f"section_{meeting_date.replace('.', '_')}_{chunk_metadata['section_name'].replace(' ', '_')}"
            
            # Document contains section
            relationships.append({
                "type": "hasSection", 
                "source": doc_id,
                "target": section_id,
                "attributes": {
                    "section_order": chunk_metadata.get('section_order', 0)
                }
            })
            
            # Entities belong to section
            for entity in entities:
                entity_id = DocumentLinker._get_entity_id(entity)
                if entity_id:
                    relationships.append({
                        "type": "belongsToSection",
                        "source": entity_id,
                        "target": section_id,
                        "attributes": {
                            "chunk_id": chunk_id
                        }
                    })
        
        return relationships
    
    @staticmethod
    def _get_entity_id(entity: Dict) -> Optional[str]:
        """Extract entity ID from various entity formats."""
        # Try common ID field patterns
        for field in ['id', 'personID', 'orgID', 'documentID', 'policyID', 
                      'assetID', 'projectID', 'locationID', 'agendaItemID']:
            if field in entity:
                return entity[field]
        
        # Generate from name if no ID
        if 'name' in entity and 'type' in entity:
            from scripts.graph_rag_stages.common.entity_bridge import EntityBridge
            return EntityBridge._generate_entity_id(entity['type'], entity['name'])
        
        return None 