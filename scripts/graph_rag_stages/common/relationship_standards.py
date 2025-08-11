# File: scripts/graph_rag_stages/common/relationship_standards.py

"""
Relationship Standards - Single source of truth for relationship naming.
Maps all relationship variations used in the codebase to canonical ontology names.
"""

import logging
from typing import Optional, Dict, Any, Tuple
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology

log = logging.getLogger(__name__)


class RelationshipStandards:
    """
    Centralizes all relationship name mappings and validation.
    This ensures consistent relationship names across the entire pipeline.
    """
    
    # Map ALL variations found in codebase to canonical ontology names
    RELATIONSHIP_MAPPING = {
        # From custom_graph_builder.py
        'HAS_AGENDA': 'hasAgenda',
        'HAS_SECTION': 'hasSection',
        'HAS_AGENDA_ITEM': 'hasAgendaItem',
        'IMPLEMENTS': 'implementedBy',  # Note: direction might need reversal
        'VOTED_ON': 'votedOn',
        'PRECEDES': 'precedes',
        'MOVED_BY': 'sponsors',  # Person who moves = primary sponsor
        'SECONDED_BY': 'sponsors',  # Person who seconds = secondary sponsor
        'HAS_HYPERLINK': 'references',  # Hyperlinks are references
        'CONTAINS': 'hasAgendaItem',  # Sections contain agenda items
        'DISCUSSED_IN': 'discusses',  # Event discusses items
        'HAS_TRANSCRIPT': 'recordedIn',  # Transcript records the event
        
        # From NER extraction patterns
        'EXTRACTED_FROM': None,  # Internal tracking only - don't push to Cosmos
        'CHUNK_SOURCE': None,  # Internal tracking only
        
        # Common variations that might appear
        'BELONGSTO': 'isPartOf',
        'BELONGS_TO': 'isPartOf',
        'IS_PART_OF': 'isPartOf',
        'IS_MEMBER_OF': 'isMemberOf',
        'MEMBER_OF': 'isMemberOf',
        'AUTHORED_BY': 'authoredBy',
        'LOCATED_AT': 'isLocatedAt',
        'IS_LOCATED_AT': 'isLocatedAt',
        'OCCURS_AT': 'occursAt',
        'ADDRESSES_TOPIC': 'addressesTopic',
        'ADDRESSES': 'addressesTopic',
        'RESULTS_IN': 'resultsIn',
        'GOVERNED_BY': 'governedBy',
        'AWARDED_TO': 'awardedTo',
        'PRESENTED_BY': 'presents',  # Note: might need direction reversal
        'IMPLEMENTED_BY': 'implementedBy',
        
        # Keep canonical names as-is (lowercase)
        'hasagenda': 'hasAgenda',
        'hassection': 'hasSection',
        'hasagendaitem': 'hasAgendaItem',
        'votedOn': 'votedOn',  # Already correct
        'sponsors': 'sponsors',
        'references': 'references',
        'discusses': 'discusses',
        'recordedIn': 'recordedIn',
    }
    
    # Relationships that need special handling for direction
    DIRECTIONAL_FIXES = {
        # (wrong_direction, correct_canonical, needs_reversal)
        'PRESENTED_BY': ('presents', True),  # If A presented_by B, then B presents A
        'IMPLEMENTS': ('implementedBy', False),  # Policy implements Document vs Document implementedBy Policy
    }
    
    # Relationships that should be filtered out (internal use only)
    INTERNAL_ONLY = {
        'EXTRACTED_FROM',
        'CHUNK_SOURCE',
        'DEBUG_LINK',
        'TEMP_RELATION'
    }
    
    @classmethod
    def normalize_relationship(cls, rel_type: str, 
                             source_type: Optional[str] = None,
                             target_type: Optional[str] = None) -> Optional[str]:
        """
        Get the canonical relationship name from any variation.
        
        Args:
            rel_type: The relationship type as found in code
            source_type: Optional source entity type for validation
            target_type: Optional target entity type for validation
            
        Returns:
            Canonical relationship name or None if should be filtered
        """
        if not rel_type:
            return None
        
        # Check if it's an internal-only relationship
        if rel_type.upper() in cls.INTERNAL_ONLY:
            log.debug(f"Filtering internal relationship: {rel_type}")
            return None
        
        # Check if already canonical (exact match)
        if rel_type in UnifiedOntology.RELATIONSHIP_TYPES:
            return rel_type
        
        # Try case-insensitive match against canonical names
        rel_lower = rel_type.lower()
        for canonical in UnifiedOntology.RELATIONSHIP_TYPES:
            if canonical.lower() == rel_lower:
                return canonical
        
        # Map from variation to canonical
        rel_upper = rel_type.upper()
        if rel_upper in cls.RELATIONSHIP_MAPPING:
            canonical = cls.RELATIONSHIP_MAPPING[rel_upper]
            if canonical:
                log.debug(f"Mapped relationship: {rel_type} → {canonical}")
            return canonical
        
        # Log unmapped relationship for future updates
        log.warning(f"⚠️ Unmapped relationship: '{rel_type}' (source: {source_type}, target: {target_type})")
        log.warning(f"   Add to RELATIONSHIP_MAPPING in relationship_standards.py")
        
        # Return as-is but lowercase (will likely fail validation later)
        return rel_type.lower()
    
    @classmethod
    def needs_direction_reversal(cls, rel_type: str) -> Tuple[bool, Optional[str]]:
        """
        Check if a relationship needs its direction reversed.
        
        Args:
            rel_type: The relationship type
            
        Returns:
            (needs_reversal, canonical_name) tuple
        """
        rel_upper = rel_type.upper()
        if rel_upper in cls.DIRECTIONAL_FIXES:
            canonical, needs_reversal = cls.DIRECTIONAL_FIXES[rel_upper]
            return needs_reversal, canonical
        return False, None
    
    @classmethod
    def validate_relationship(cls, rel_type: str, 
                            source_type: str, 
                            target_type: str) -> Tuple[bool, Optional[str]]:
        """
        Validate a relationship against the ontology.
        
        Args:
            rel_type: The relationship type (will be normalized)
            source_type: Source entity type
            target_type: Target entity type
            
        Returns:
            (is_valid, error_message) tuple
        """
        # Normalize the relationship name
        canonical = cls.normalize_relationship(rel_type, source_type, target_type)
        
        if not canonical:
            return False, f"Relationship {rel_type} is internal-only or unmapped"
        
        # Check if relationship exists in ontology
        if canonical not in UnifiedOntology.RELATIONSHIP_TYPES:
            return False, f"Relationship {canonical} not in ontology"
        
        # Get relationship definition
        rel_def = UnifiedOntology.RELATIONSHIP_DEFINITIONS.get(canonical)
        if not rel_def:
            # Relationship exists but no detailed definition
            log.warning(f"No definition for relationship: {canonical}")
            return True, None
        
        # Validate source type
        expected_sources = rel_def.get('source', [])
        if isinstance(expected_sources, str):
            expected_sources = [expected_sources]
        
        if source_type not in expected_sources:
            return False, f"{canonical} expects source types {expected_sources}, got {source_type}"
        
        # Validate target type
        expected_targets = rel_def.get('target', [])
        if isinstance(expected_targets, str):
            expected_targets = [expected_targets]
            
        if target_type not in expected_targets:
            return False, f"{canonical} expects target types {expected_targets}, got {target_type}"
        
        return True, None
    
    @classmethod
    def get_relationship_attributes(cls, rel_type: str) -> Dict[str, Any]:
        """
        Get expected attributes for a relationship type.
        
        Args:
            rel_type: The relationship type (will be normalized)
            
        Returns:
            Dictionary of expected attributes with descriptions
        """
        canonical = cls.normalize_relationship(rel_type)
        if not canonical:
            return {}
        
        rel_def = UnifiedOntology.RELATIONSHIP_DEFINITIONS.get(canonical, {})
        attributes = rel_def.get('attributes', [])
        
        # Convert list to dict with placeholders
        attr_dict = {}
        for attr in attributes:
            if attr == 'sponsorshipType' and 'SECONDED' in rel_type.upper():
                attr_dict[attr] = 'secondary'
            elif attr == 'sponsorshipType':
                attr_dict[attr] = 'primary'
            else:
                attr_dict[attr] = None
        
        return attr_dict
    
    @classmethod
    def standardize_relationship_creation(cls, rel_type: str, 
                                         source_id: str, 
                                         target_id: str,
                                         attributes: Optional[Dict] = None,
                                         source_type: Optional[str] = None,
                                         target_type: Optional[str] = None) -> Optional[Dict]:
        """
        Create a standardized relationship object with proper naming.
        
        Args:
            rel_type: Original relationship type from code
            source_id: Source entity ID
            target_id: Target entity ID
            attributes: Optional relationship attributes
            source_type: Optional source entity type for validation
            target_type: Optional target entity type for validation
            
        Returns:
            Standardized relationship dict or None if should be filtered
        """
        # Check for direction reversal needs
        needs_reversal, canonical_override = cls.needs_direction_reversal(rel_type)
        
        if needs_reversal and canonical_override:
            # Swap source and target
            canonical = canonical_override
            actual_source = target_id
            actual_target = source_id
            actual_source_type = target_type
            actual_target_type = source_type
            log.debug(f"Reversed relationship direction: {rel_type} → {canonical}")
        else:
            # Normal direction
            canonical = cls.normalize_relationship(rel_type, source_type, target_type)
            actual_source = source_id
            actual_target = target_id
            actual_source_type = source_type
            actual_target_type = target_type
        
        # Filter internal relationships
        if not canonical:
            return None
        
        # Validate if types provided
        if actual_source_type and actual_target_type:
            is_valid, error = cls.validate_relationship(
                canonical, actual_source_type, actual_target_type
            )
            if not is_valid:
                log.warning(f"Invalid relationship: {error}")
                # Still create it but log the issue
        
        # Get expected attributes and merge with provided
        expected_attrs = cls.get_relationship_attributes(canonical)
        final_attrs = expected_attrs.copy()
        
        if attributes:
            # Merge provided attributes, keeping only expected ones
            for key, value in attributes.items():
                if key in expected_attrs or not expected_attrs:
                    final_attrs[key] = value
        
        return {
            'type': canonical,
            'source': actual_source,
            'target': actual_target,
            'attributes': final_attrs
        }
    
    @classmethod
    def get_unmapped_report(cls) -> Dict[str, Any]:
        """
        Generate a report of all relationship mappings for documentation.
        
        Returns:
            Dictionary with mapping statistics and details
        """
        mapped_to_canonical = {}
        unmapped = []
        internal = list(cls.INTERNAL_ONLY)
        
        # Group by canonical name
        for variation, canonical in cls.RELATIONSHIP_MAPPING.items():
            if canonical:
                if canonical not in mapped_to_canonical:
                    mapped_to_canonical[canonical] = []
                mapped_to_canonical[canonical].append(variation)
            else:
                unmapped.append(variation)
        
        return {
            'total_mappings': len(cls.RELATIONSHIP_MAPPING),
            'canonical_relationships': len(mapped_to_canonical),
            'internal_only': internal,
            'unmapped': unmapped,
            'mapping_details': mapped_to_canonical,
            'ontology_relationships': sorted(UnifiedOntology.RELATIONSHIP_TYPES)
        }


# Utility function for easy integration
def fix_relationship(rel_type: str, source_id: str, target_id: str, 
                     attributes: Optional[Dict] = None) -> Optional[Dict]:
    """
    Quick helper to fix a relationship during creation.
    
    Usage:
        # Instead of:
        rel = {'type': 'HAS_AGENDA', 'source': meeting_id, 'target': doc_id}
        
        # Use:
        rel = fix_relationship('HAS_AGENDA', meeting_id, doc_id)
    """
    return RelationshipStandards.standardize_relationship_creation(
        rel_type, source_id, target_id, attributes
    )
