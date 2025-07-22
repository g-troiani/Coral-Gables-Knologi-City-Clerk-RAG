"""
Links entities extracted from chunks back to their source documents
"""

import hashlib
from typing import Dict, List, Any
from pathlib import Path

class DocumentLinker:
    """Creates relationships between entities and their source documents."""
    
    @staticmethod
    def create_document_entity_relationships(
        entities: List[Dict[str, Any]], 
        chunk_metadata: Dict[str, Any],
        chunk_id: str
    ) -> List[Dict[str, Any]]:
        """Create relationships between entities and their source documents."""
        relationships = []
        
        # Generate document ID from metadata
        source_file = chunk_metadata.get('Source_File_Name', 
                                       chunk_metadata.get('source', 'unknown'))
        
        # Create document ID that matches the filename pattern
        doc_id = DocumentLinker._generate_document_id(source_file)
        
        # CRITICAL: Create the document entity first!
        doc_entity = DocumentLinker._create_document_entity(
            doc_id, source_file, chunk_metadata
        )
        
        # Add document entity to the entities list if not already there
        if doc_entity and not any(e.get('documentID') == doc_id for e in entities if e.get('type') == 'Document'):
            entities.append(doc_entity)
        
        # Create extractedFrom relationships for each entity
        for entity in entities:
            entity_type = entity.get('type', '')
            
            # Get the appropriate ID field
            id_field_map = {
                'Person': 'personID', 'Organization': 'orgID', 
                'Location': 'locationID', 'Event': 'eventID',
                'Document': 'documentID', 'AgendaItem': 'agendaItemID',
                'Policy': 'policyID', 'Asset': 'assetID',
                'Contract': 'contractID', 'Project': 'projectID',
                'Role': 'roleID', 'Action': 'actionID',
                'Topic': 'topicID', 'Technology': 'technologyID',
                'VoteOutcome': 'voteOutcomeID'
            }
            
            id_field = id_field_map.get(entity_type, f"{entity_type.lower()}ID")
            entity_id = entity.get(id_field) or entity.get('_entity_id')
            
            if entity_id and entity_id != doc_id:  # Don't link document to itself
                relationships.append({
                    'type': 'extractedFrom',
                    'source': entity_id,
                    'target': doc_id,
                    'attributes': {
                        'chunk_id': chunk_id,
                        'extraction_method': 'ner_extraction'
                    }
                })
        
        return relationships
    
    @staticmethod
    def _generate_document_id(source_file: str) -> str:
        """Generate document ID from source filename."""
        # Remove .pdf extension
        base_name = source_file.replace('.pdf', '')
        
        # Replace special chars with underscores
        normalized = base_name.replace(' - ', '_-_')
        normalized = normalized.replace(' ', '_')
        normalized = normalized.replace('.', '_')
        
        # Ensure it starts with 'document_'
        if not normalized.startswith('document_'):
            normalized = f'document_{normalized}'
        
        return normalized
    
    @staticmethod
    def _create_document_entity(
        doc_id: str, 
        source_file: str, 
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a document entity."""
        # Extract document type from filename
        doc_type = 'document'
        if 'verbatim' in source_file.lower():
            doc_type = 'transcript'
        elif 'ordinance' in source_file.lower():
            doc_type = 'ordinance'
        elif 'resolution' in source_file.lower():
            doc_type = 'resolution'
        elif 'agenda' in source_file.lower():
            doc_type = 'agenda'
        
        return {
            'documentID': doc_id,
            'name': source_file,
            'type': 'Document',
            'title': source_file.replace('.pdf', ''),
            'document_type': doc_type,
            'issueDate': metadata.get('meeting_date', ''),
            'status': 'processed',
            'sourceURL': metadata.get('Source_File_Path', ''),
            'summary': f"{doc_type.title()} document from {metadata.get('meeting_date', 'unknown date')}",
            'version': '1.0'
        } 