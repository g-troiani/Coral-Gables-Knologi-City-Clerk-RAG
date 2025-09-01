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
        
        # Get source file name with better fallback handling
        # Priority: Use actual source file from metadata, not the chunk filename
        source_file = (chunk_metadata.get('Source_File_Name') or
                      chunk_metadata.get('sourceFileName') or 
                      chunk_metadata.get('source_file') or
                      chunk_metadata.get('source'))
        
        # If no source file in metadata, try to extract from path
        if not source_file or source_file in ['unknown', 'unknown.pdf', '']:
            source_path = chunk_metadata.get('Source_File_Path') or chunk_metadata.get('sourceFilePath', '')
            if source_path:
                source_file = Path(source_path).name
            else:
                # Last resort: use document field but clean it up
                doc_name = chunk_metadata.get('document', 'unknown.pdf')
                # Remove enhanced_ordinance suffix if present
                if '_enhanced_ordinance' in doc_name:
                    doc_name = doc_name.replace('_enhanced_ordinance', '')
                # Add .pdf extension if missing
                if not doc_name.endswith('.pdf'):
                    doc_name = f"{doc_name}.pdf"
                source_file = doc_name
        
        # Create document ID with metadata for path-based detection
        doc_id = DocumentLinker._generate_document_id(source_file, chunk_metadata)
        
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
    def _generate_document_id(source_file: str, metadata: Dict[str, Any] = None) -> str:
        """Generate document ID from source filename and metadata."""
        import re
        from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
        
        # Special handling for ordinances and resolutions to match taxonomy format
        # Pattern: "2024-02 - 01_09_2024.pdf" or "2024-02.pdf"
        legal_doc_match = re.match(r'^(\d{4})-(\d{2})(?:\s*-\s*.*)?\.pdf$', source_file, re.IGNORECASE)
        if legal_doc_match:
            year = legal_doc_match.group(1)
            ordinal = legal_doc_match.group(2)
            
            # Determine if this is a resolution or ordinance using folder path
            doc_type = 'ordinance'  # default
            if metadata:
                source_path = (metadata.get('Source_File_Path') or 
                             metadata.get('sourceFilePath') or 
                             metadata.get('sourcePath') or '')
                source_path_lower = source_path.lower()
                
                # Check folder structure first (most reliable)
                if '/resolutions/' in source_path_lower or '\\resolutions\\' in source_path_lower:
                    doc_type = 'resolution'
                elif '/ordinances/' in source_path_lower or '\\ordinances\\' in source_path_lower:
                    doc_type = 'ordinance'
                # Then check filename patterns
                elif 'resolution' in source_file.lower():
                    doc_type = 'resolution'
                elif 'ordinance' in source_file.lower():
                    doc_type = 'ordinance'
            
            # Use the same ID generation as taxonomy
            return EntityIDStandards.make_policy_id(doc_type, year, ordinal, source_file)
        
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
        
        # Standard processing for non-transcript documents
        # Remove .pdf extension
        base_name = source_file.replace('.pdf', '')
        
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