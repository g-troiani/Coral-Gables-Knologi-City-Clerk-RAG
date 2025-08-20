"""
Links entities extracted from chunks back to their source documents
"""

import hashlib
from typing import Dict, List, Any
from pathlib import Path
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards

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
        
        # Get source file name - prioritize original PDF source over JSON name
        source_file = (chunk_metadata.get('Source_File_Name') or      # Primary: original PDF name
                      chunk_metadata.get('sourceFileName') or          # Alt format
                      chunk_metadata.get('source_file_name') or        # Alt format
                      chunk_metadata.get('source_file') or
                      chunk_metadata.get('source') or
                      chunk_metadata.get('document', 'unknown.pdf'))   # Last resort: JSON name
        
        # Ensure we don't get "unknown" as filename
        if source_file in ['unknown', 'unknown.pdf', '']:
            # Try to extract from sourceFilePath or Source_File_Path
            source_path = chunk_metadata.get('sourceFilePath') or chunk_metadata.get('Source_File_Path', '')
            if source_path:
                source_file = Path(source_path).name
            else:
                # Last resort: use chunk_id-based name
                source_file = f"document_{chunk_id}.pdf"
        
        # Create document ID
        doc_id = DocumentLinker._generate_document_id(source_file)
        
        # CRITICAL: Create the document entity first!
        doc_entity = DocumentLinker._create_document_entity(
            doc_id, source_file, chunk_metadata
        )
        
        # Normalize the document entity
        doc_entity = EntityIDStandards.normalize_entity_id_fields(doc_entity, 'Document')
        
        # Add document entity to the entities list if not already there
        if doc_entity and not any(e.get('documentID') == doc_id for e in entities if e.get('type') == 'Document'):
            entities.append(doc_entity)
        
        # Create extractedFrom relationships for each entity
        for entity in entities:
            entity_type = entity.get('type', '')
            
            # Use centralized ID field mapping
            id_field = EntityIDStandards.get_id_field(entity_type)
            entity_id = entity.get(id_field) or entity.get('id')
            
            if entity_id and entity_id != doc_id:  # Don't link document to itself
                relationships.append({
                    'type': 'extractedFrom',
                    'source': entity_id,
                    'target': doc_id,
                    'attributes': {
                        'chunkId': chunk_id,
                        'extractionMethod': 'ner_extraction'
                    }
                })
        
        return relationships
    
    @staticmethod
    def _generate_document_id(source_file: str) -> str:
        """Generate document ID from source filename."""
        import re
        
        # Special handling for verbatim transcripts to match taxonomy format
        if 'verbatim' in source_file.lower() and 'transcript' in source_file.lower():
            # Extract meeting date from filename (e.g., "01_09_2024 - Verbatim Transcripts - E-4.pdf")
            date_match = re.search(r'(\d{2})[_\.](\d{2})[_\.](\d{4})', source_file)
            if date_match:
                meeting_date = f"{date_match.group(1)}.{date_match.group(2)}.{date_match.group(3)}"
                meeting_date_id = meeting_date.replace('.', '_')
                
                # Create slug matching taxonomy's _slug function
                slug = re.sub(r'[^a-z0-9_]+', '-', source_file.strip().lower())
                
                # Match taxonomy format: document_transcript_{slug}_{meeting_date}
                return f'document_transcript_{slug}_{meeting_date_id}'
        
        # Special handling for agenda documents to match taxonomy format
        if 'agenda' in source_file.lower():
            # Extract date and convert to taxonomy format (YYYY_MM_DD)
            date_match = re.search(r'(\d{2})[_\.](\d{2})[_\.](\d{4})', source_file)
            if date_match:
                month, day, year = date_match.groups()
                # Match taxonomy format: document_agenda_YYYY_MM_DD
                return f'document_agenda_{year}_{month.zfill(2)}_{day.zfill(2)}'
        
        # Special handling for ordinances and resolutions
        if any(term in source_file.lower() for term in ['ordinance', 'resolution']):
            # Extract policy type and number (e.g., "2024-02 - 01_09_2024.pdf" -> "ordinance", "2024", "02")
            match = re.search(r'(\d{4})-(\d{2,3})', source_file)
            if match:
                year, num = match.groups()
                policy_type = 'ordinance' if 'ordinance' in source_file.lower() else 'resolution'
                # Use EntityIDStandards to generate consistent policy ID
                from .entity_id_standards import EntityIDStandards
                return EntityIDStandards.make_policy_id(policy_type, year, num, source_file)
        
        # Standard processing for other documents
        # Remove .pdf extension and JSON suffixes
        base_name = source_file.replace('.pdf', '')
        # Remove common JSON processing suffixes
        for suffix in ['_enhanced_ordinance', '_enhanced_resolution', '_enhanced', '_processed']:
            base_name = base_name.replace(suffix, '')
        
        # Replace special chars with underscores
        normalized = base_name.replace(' - ', '_-_')
        normalized = normalized.replace(' ', '_')
        normalized = normalized.replace('.', '_')
        
        # Convert to lowercase to match taxonomy format
        normalized = normalized.lower()
        
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
        """Create a document entity with correct ID field."""
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
            'documentID': doc_id,  # Use correct field name
            'name': source_file,
            'type': 'Document',
            'title': source_file.replace('.pdf', ''),
            'document_type': doc_type,
            'issueDate': metadata.get('meetingDate') or metadata.get('meeting_date', ''),
            'status': 'processed',
            'sourceURL': metadata.get('sourceFilePath') or metadata.get('Source_File_Path', ''),
            'summary': f"{doc_type.title()} document from {metadata.get('meetingDate') or metadata.get('meeting_date', 'unknown date')}",
            'version': '1.0',
            'id': doc_id  # Also include generic 'id'
        } 