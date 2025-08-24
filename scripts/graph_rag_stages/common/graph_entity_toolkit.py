"""
Unified facade for entity and relationship creation.
Wraps existing standards to ensure consistency across taxonomy and NER pipelines.
"""

import hashlib
import json
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

from .entity_id_standards import EntityIDStandards
from .entity_factory import EntityFactory
from .document_linker import DocumentLinker
from .unified_ontology import UnifiedOntology
import re


class GraphEntityToolkit:
    """Single entry point for standardized entity/relationship creation."""
    
    @staticmethod
    def generate_entity_id(entity_type: str, key_attributes: Dict) -> str:
        """
        Generate deterministic entity ID used by both pipelines.
        
        Args:
            entity_type: Type from UnifiedOntology (e.g., "Person", "AgendaItem")
            key_attributes: Dict with key fields for ID generation
            
        Returns:
            Deterministic, sanitized entity ID
        """
        # Get the standard ID field for this entity type
        id_field = EntityIDStandards.get_id_field(entity_type)
        
        # Check if ID already provided
        if id_field in key_attributes and key_attributes[id_field]:
            return GraphEntityToolkit._sanitize_id(str(key_attributes[id_field])).lower()
        
        # Generate deterministic ID from key attributes
        base_component = None
        
        # For AgendaItem, prioritize itemNumber
        if entity_type == "AgendaItem":
            for field in ['itemNumber', 'itemID', 'name', 'title']:
                if field in key_attributes and key_attributes[field]:
                    base_component = str(key_attributes[field])
                    break
        else:
            # Primary identifiers for other entities
            for field in ['name', 'title', 'itemID', 'code', 'number']:
                if field in key_attributes and key_attributes[field]:
                    base_component = str(key_attributes[field])
                    break
        
        if not base_component:
            # Fallback to unknown
            base_component = "unknown"
        
        # Sanitize base component with special handling for AgendaItem
        prefix = entity_type.lower()
        if entity_type == "AgendaItem":
            # Standardize agenda item formats before sanitizing
            standardized = base_component.lower()
            # E-4, E.4, E 4 → e4
            standardized = re.sub(r'\b([a-z])\s*[-.\s]\s*(\d+)\b', r'\1\2', standardized)
            component = GraphEntityToolkit._sanitize_id(standardized)[:20]
        else:
            component = GraphEntityToolkit._sanitize_id(base_component)[:20]
        
        # Only AgendaItem gets a date suffix (no more hashes for other entities)
        if entity_type == "AgendaItem":
            # Try to get meeting date from key_attributes
            meeting_date = key_attributes.get('meetingDate', '') or key_attributes.get('meeting_date', '') or ''
            date_suffix = GraphEntityToolkit._format_date_suffix(meeting_date)
            return f"{prefix}_{component}_{date_suffix}".lower()
        else:
            # All other entities: no hash, no date suffix
            return f"{prefix}_{component}".lower()
    
    @staticmethod
    def _format_date_suffix(meeting_date: str) -> str:
        """Format meeting date as MM_DD_YYYY suffix for AgendaItems."""
        if not meeting_date:
            return "unknown_date"
        
        # Handle various date formats and convert to MM_DD_YYYY
        date_str = str(meeting_date).strip()
        
        # Common patterns: 01.09.2024, 2024-01-09, 01/09/2024, etc.
        import re
        
        # Pattern 1: DD.MM.YYYY (e.g., "01.09.2024")
        match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
        if match:
            day, month, year = match.groups()
            return f"{month.zfill(2)}_{day.zfill(2)}_{year}"
        
        # Pattern 2: YYYY-MM-DD (e.g., "2024-01-09")  
        match = re.match(r'(\d{4})-(\d{1,2})-(\d{1,2})', date_str)
        if match:
            year, month, day = match.groups()
            return f"{month.zfill(2)}_{day.zfill(2)}_{year}"
        
        # Pattern 3: MM/DD/YYYY (e.g., "01/09/2024")
        match = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
        if match:
            month, day, year = match.groups()
            return f"{month.zfill(2)}_{day.zfill(2)}_{year}"
        
        # Pattern 4: Already in MM_DD_YYYY format
        if re.match(r'\d{2}_\d{2}_\d{4}', date_str):
            return date_str
        
        # Fallback: try to extract year and use generic format
        year_match = re.search(r'(20\d{2})', date_str)
        if year_match:
            year = year_match.group(1)
            return f"01_09_{year}"  # Default to 01/09 if can't parse month/day
        
        return "unknown_date"

    @staticmethod
    def _sanitize_id(id_str: str) -> str:
        """
        Sanitize ID string to remove invalid characters.
        
        Args:
            id_str: Raw ID string
            
        Returns:
            Sanitized ID string
        """
        import re
        
        if not id_str:
            return "unknown"
        
        # Replace invalid characters with safe alternatives and lowercase
        sanitized = (id_str
                    .lower()  # Convert to lowercase
                    .replace('/', '-')
                    .replace('\\', '-')
                    .replace(' ', '-')
                    .replace(':', '-')
                    .replace('"', '')
                    .replace("'", '')
                    .replace('(', '')
                    .replace(')', '')
                    .replace('[', '')
                    .replace(']', '')
                    .replace('{', '')
                    .replace('}', '')
                    .replace('&', 'and')
                    .replace('%', 'pct')
                    .replace('#', 'num')
                    .replace('@', 'at')
                    .replace('?', '')
                    .replace('!', '')
                    .replace('*', '')
                    .replace('+', 'plus')
                    .replace('=', 'eq')
                    .replace('<', 'lt')
                    .replace('>', 'gt')
                    .replace('|', '-')
                    .replace(',', '')
                    .replace(';', ''))
        
        # Remove any remaining non-alphanumeric characters except dash and underscore
        sanitized = re.sub(r'[^a-zA-Z0-9\-_]', '', sanitized)
        
        # Ensure it doesn't start or end with dash/underscore
        sanitized = sanitized.strip('-_')
        
        # Ensure it's not empty after sanitization
        return sanitized if sanitized else "unknown"
    
    @staticmethod
    def validate_entity(entity_data: Dict, entity_type: str) -> Dict:
        """
        Validate and normalize entity using existing EntityFactory.
        
        Args:
            entity_data: Raw entity data
            entity_type: Entity type from UnifiedOntology
            
        Returns:
            Validated entity with correct ID field and attributes
        """
        # Ensure entity has type
        entity_data['type'] = entity_type
        
        # Validate using EntityFactory
        validated = EntityFactory.validate_entity(entity_data)
        
        # Ensure correct ID field is used
        id_field = EntityIDStandards.get_id_field(entity_type)
        
        # If missing proper ID field, generate one
        if id_field not in validated or not validated[id_field]:
            validated[id_field] = GraphEntityToolkit.generate_entity_id(
                entity_type, validated
            )
        
        # Normalize the ID
        validated[id_field] = GraphEntityToolkit._sanitize_id(validated[id_field])
        
        return validated
    
    @staticmethod
    def create_document_relationships(entities: List[Dict], 
                                     metadata: Dict,
                                     source_type: str = "unknown") -> List[Dict]:
        """
        Create document-entity relationships using DocumentLinker.
        
        Args:
            entities: List of entities to link
            metadata: Document/chunk metadata
            source_type: "taxonomy" or "ner" for provenance
            
        Returns:
            List of relationship dictionaries
        """
        # Use DocumentLinker to create relationships
        relationships = DocumentLinker.create_document_entity_relationships(
            entities, metadata, metadata.get('chunk_id', 'unknown')
        )
        
        # Add provenance to relationships
        for rel in relationships:
            if '_source' not in rel:
                rel['_source'] = source_type
            if '_created_at' not in rel:
                rel['_created_at'] = datetime.now().isoformat()
        
        return relationships
    
    @staticmethod
    def validate_relationship(rel_type: str, source_type: str, 
                            target_type: str) -> bool:
        """
        Validate relationship type against ontology.
        
        Args:
            rel_type: Relationship type
            source_type: Source entity type
            target_type: Target entity type
            
        Returns:
            True if relationship is valid per ontology
        """
        # Check if relationship type exists in the list
        if hasattr(UnifiedOntology, 'RELATIONSHIP_TYPES'):
            if isinstance(UnifiedOntology.RELATIONSHIP_TYPES, list):
                return rel_type in UnifiedOntology.RELATIONSHIP_TYPES
            elif isinstance(UnifiedOntology.RELATIONSHIP_TYPES, dict):
                if rel_type not in UnifiedOntology.RELATIONSHIP_TYPES:
                    return False
                
                rel_def = UnifiedOntology.RELATIONSHIP_TYPES.get(rel_type, {})
                
                # Check source type
                allowed_sources = rel_def.get('source', [])
                if isinstance(allowed_sources, str):
                    allowed_sources = [allowed_sources]
                if source_type not in allowed_sources:
                    return False
                
                # Check target type  
                allowed_targets = rel_def.get('target', [])
                if isinstance(allowed_targets, str):
                    allowed_targets = [allowed_targets]
                if target_type not in allowed_targets:
                    return False
                
                return True
        
        # If no ontology or unknown structure, allow all
        return True
    
    @staticmethod
    def create_entity(entity_type: str, attributes: Dict, 
                     source: str = "unknown") -> Dict:
        """
        Create a properly formatted entity.
        
        Args:
            entity_type: Type from UnifiedOntology
            attributes: Entity attributes
            source: Source identifier for provenance
            
        Returns:
            Complete, validated entity dict
        """
        # Get expected attributes from ontology
        expected_attrs = UnifiedOntology.ENTITY_TYPES.get(
            entity_type, {}
        ).get('attributes', [])
        
        # Build entity with all expected attributes
        entity = {
            'type': entity_type,
            '_source': source,
            '_created_at': datetime.now().isoformat()
        }
        
        # Add provided attributes
        entity.update(attributes)
        
        # Ensure all expected attributes exist (None if missing)
        for attr in expected_attrs:
            if attr not in entity:
                entity[attr] = None
        
        # Validate and add proper ID
        entity = GraphEntityToolkit.validate_entity(entity, entity_type)
        
        return entity
    
    @staticmethod
    def create_relationship(rel_type: str, source_id: str, target_id: str,
                          attributes: Dict = None, source: str = "unknown") -> Dict:
        """
        Create a properly formatted relationship.
        
        Args:
            rel_type: Relationship type from ontology
            source_id: Source entity ID
            target_id: Target entity ID
            attributes: Optional relationship attributes
            source: Source identifier for provenance
            
        Returns:
            Complete relationship dict
        """
        rel = {
            'type': rel_type,
            'source': source_id,
            'target': target_id,
            'attributes': attributes or {},
            '_source': source,
            '_created_at': datetime.now().isoformat()
        }
        
        # If RELATIONSHIP_TYPES is a dict with definitions, add expected attributes
        if hasattr(UnifiedOntology, 'RELATIONSHIP_TYPES'):
            if isinstance(UnifiedOntology.RELATIONSHIP_TYPES, dict):
                rel_def = UnifiedOntology.RELATIONSHIP_TYPES.get(rel_type, {})
                expected_attrs = rel_def.get('attributes', [])
                
                for attr in expected_attrs:
                    if attr not in rel['attributes']:
                        rel['attributes'][attr] = None
        
        return rel
    
    @staticmethod
    def generate_edge_id(source_id: str, rel_type: str, 
                        target_id: str, attributes: Dict = None) -> str:
        """
        Generate deterministic edge ID for idempotent operations.
        
        Args:
            source_id: Source vertex ID
            rel_type: Relationship type
            target_id: Target vertex ID
            attributes: Optional attributes to include in hash
            
        Returns:
            Deterministic edge ID
        """
        # Sort attributes for consistent hashing
        attr_str = ""
        if attributes:
            # Only include non-null, non-metadata attributes
            relevant_attrs = {
                k: v for k, v in attributes.items() 
                if v is not None and not k.startswith('_')
            }
            if relevant_attrs:
                attr_str = json.dumps(relevant_attrs, sort_keys=True)
        
        # Create deterministic ID with readable format and essential context
        # Include key context attributes (chunkId, extractionMethod) for differentiation
        context_parts = []
        if attributes:
            # Add key differentiating attributes
            for key in ['chunkId', 'extractionMethod', 'sourceFile']:
                if key in attributes and attributes[key]:
                    context_parts.append(str(attributes[key])[:10])  # Truncate for readability
        
        context_str = "_".join(context_parts) if context_parts else "base"
        # Create readable but unique ID
        return f"{rel_type}_{source_id[:15]}_{target_id[:15]}_{context_str}".replace(' ', '_')
    
    @staticmethod
    def merge_entities(primary: Dict, secondary: Dict) -> Dict:
        """
        Merge two entities, with primary taking precedence.
        
        Args:
            primary: Primary entity (e.g., from taxonomy)
            secondary: Secondary entity (e.g., from NER)
            
        Returns:
            Merged entity with combined attributes and sources
        """
        merged = primary.copy()
        
        # Merge non-null attributes from secondary
        for key, value in secondary.items():
            if key.startswith('_'):
                continue  # Skip metadata for now
            if key not in merged or merged[key] is None:
                merged[key] = value
        
        # Always preserve Source_File_Name and Source_File_Path if available
        # Priority: Use the one from the entity that originally created the node
        for source_attr in ['Source_File_Name', 'Source_File_Path']:
            # If primary has it, keep it
            if source_attr in primary and primary[source_attr]:
                merged[source_attr] = primary[source_attr]
            # Otherwise, use secondary's value
            elif source_attr in secondary and secondary[source_attr]:
                merged[source_attr] = secondary[source_attr]
        
        # Combine sources
        sources = set()
        if '_source' in primary:
            sources.add(primary['_source'])
        if '_sources' in primary:
            sources.update(primary['_sources'])
        if '_source' in secondary:
            sources.add(secondary['_source'])
        if '_sources' in secondary:
            sources.update(secondary['_sources'])
        
        merged['_sources'] = list(sources)
        merged['_merge_timestamp'] = datetime.now().isoformat()
        
        return merged
