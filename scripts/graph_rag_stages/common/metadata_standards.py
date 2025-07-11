"""Centralized metadata standards and validation."""

from datetime import datetime
from typing import Dict, Any, Optional
import re

class MetadataStandards:
    """Single source of truth for metadata formats and validation."""
    
    # Standard date format used throughout the system
    DATE_FORMAT = "%m.%d.%Y"  # 01.09.2024
    
    # Valid document types (order matters for classification)
    DOCUMENT_TYPES = {
        'verbatim_transcript': ['verbatim', 'transcript'],
        'agenda': ['agenda'],
        'ordinance': ['ordinance'],
        'resolution': ['resolution'], 
        'minutes': ['minutes'],
        'document': []  # fallback
    }
    
    @staticmethod
    def standardize_date(date_str: str) -> str:
        """Convert any date format to standard MM.DD.YYYY."""
        if not date_str:
            return ""
            
        # Already in correct format
        if re.match(r'^\d{2}\.\d{2}\.\d{4}$', date_str):
            return date_str
            
        # Convert MM_DD_YYYY to MM.DD.YYYY
        if re.match(r'^\d{2}_\d{2}_\d{4}$', date_str):
            return date_str.replace('_', '.')
            
        # Convert MM-DD-YYYY to MM.DD.YYYY
        if re.match(r'^\d{2}-\d{2}-\d{4}$', date_str):
            return date_str.replace('-', '.')
            
        return date_str  # Return as-is if no match
    
    @staticmethod
    def classify_document(filename: str, title: str = '') -> str:
        filename_lower = filename.lower()
        title_lower = title.lower() if title else ''
        DOCUMENT_TYPES = {
            'verbatim_transcript': ['verbatim', 'transcript'],
            'agenda': ['agenda'],
            'ordinance': ['ordinance'],
            'resolution': ['resolution'], 
            'minutes': ['minutes'],
        }
        
        # Filename check first
        for doc_type, keywords in DOCUMENT_TYPES.items():
            if any(keyword in filename_lower for keyword in keywords):
                return doc_type
        
        # If fallback to 'document' and title provided, check title
        if title_lower:
            for doc_type, keywords in DOCUMENT_TYPES.items():
                if any(keyword in title_lower for keyword in keywords):
                    return doc_type
        
        return 'document'
    
    @staticmethod
    def validate_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and standardize metadata."""
        validated = metadata.copy()
        
        # Standardize document type
        if 'document_type' in validated:
            doc_type = validated['document_type'].lower()
            if doc_type not in MetadataStandards.DOCUMENT_TYPES:
                validated['document_type'] = 'document'
        
        # Standardize date
        if 'meeting_date' in validated:
            validated['meeting_date'] = MetadataStandards.standardize_date(
                str(validated['meeting_date'])
            )
        
        return validated
    
    @staticmethod
    def ensure_graph_compatibility(node_data: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure node data is compatible with existing graph queries."""
        
        # The graph expects certain fields in specific formats
        if 'meeting_date' in node_data:
            # Graph queries might expect both formats
            standardized = MetadataStandards.standardize_date(node_data['meeting_date'])
            node_data['meeting_date'] = standardized
            # Also store with dashes for backward compatibility
            node_data['meeting_date_id'] = standardized.replace('.', '-')
        
        # Ensure document_type is lowercase for graph queries
        if 'document_type' in node_data:
            node_data['document_type'] = node_data['document_type'].lower()
            # Also store classification for queries
            node_data['document_classification'] = node_data['document_type']
        
        return node_data 