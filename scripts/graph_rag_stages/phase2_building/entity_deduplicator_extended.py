"""
Extended EntityDeduplicator with multi-source support.
This extends the existing deduplicator to handle both NER and taxonomy sources.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any, Optional
from collections import defaultdict
import hashlib
import re
import os

from scripts.graph_rag_stages.common.graph_entity_toolkit import GraphEntityToolkit
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
from scripts.graph_rag_stages.common.standards import ensure_min_document_props, ensure_min_entity_props
from scripts.graph_rag_stages.common.standards import make_policy_id

log = logging.getLogger(__name__)

# Import debug flags from main pipeline
# Debug imports removed to avoid circular dependencies
DEBUG_ENTITY_DEDUPLICATION = False
DEBUG_RELATIONSHIP_LINKING = False


class EntityDeduplicatorExtended:
    """Extended deduplicator that handles multiple sources."""
    MERGE_DEBUG_ON = os.getenv("MERGE_DEBUG", "").lower() in ("1", "true", "yes")
    _TYPE_MAP = {t.lower(): t for t in UnifiedOntology.get_entity_categories()}

    def _canon_type(self, t: Optional[str]) -> Optional[str]:
        if not t:
            return t
        return self._TYPE_MAP.get(str(t).lower(), t)
    
    # --- Canonicalize ontology types (case-insensitive) ---
    def _canon_entity_type(self, t: Optional[str]) -> Optional[str]:
        if not t or not isinstance(t, str):
            return t
        m = {
            'person':'Person','organization':'Organization','document':'Document','policy':'Policy',
            'event':'Event','location':'Location','agendaitem':'AgendaItem','asset':'Asset',
            'project':'Project','role':'Role','topic':'Topic','section':'Section','contract':'Contract',
            'technology':'Technology','voteoutcome':'VoteOutcome','agendadocument':'AgendaDocument'
        }
        return m.get(t.lower(), t)

    
    # --- New helpers for preferred IDs ---
    def _hash8(self, s: str) -> str:
        return hashlib.sha256(s.encode("utf-8")).hexdigest()[:8]



    def _extract_e_code(self, entity: Dict) -> Optional[str]:
        # Delegate to centralized standard
        return EntityIDStandards._extract_e_code(entity)

    def _extract_ordres_number(self, entity: Dict) -> Tuple[Optional[str], Optional[str]]:
        # Delegate to centralized standard
        return EntityIDStandards._extract_ordres_number(entity)

    def _preferred_policy_id(self, entity: Dict) -> Optional[str]:
        # Single source of truth
        return EntityIDStandards.preferred_policy_id(entity)

    def _preferred_agendaitem_id(self, entity: Dict) -> Optional[str]:
        # Single source of truth
        return EntityIDStandards.preferred_agendaitem_id(entity)
    
    def _preferred_document_id(self, entity: Dict) -> Optional[str]:
        """Generate preferred document ID using standardized format."""
        import re
        
        doc_type = (entity.get('documentType') or entity.get('document_type') or 
                   entity.get('type', '')).lower()
        
        # Extract date from various fields (used by multiple document types)
        date_str = (entity.get('meetingDate') or entity.get('meeting_date') or 
                   entity.get('issueDate') or entity.get('date') or '')
        
        # For agenda documents, use standard format: document_agenda_YYYY_MM_DD
        if 'agenda' in doc_type or 'agenda' in (entity.get('title') or '').lower():
            if date_str:
                # Convert to YYYY_MM_DD format (US format: MM.DD.YYYY)
                date_match = re.search(r'(\d{1,2})[._\-](\d{1,2})[._\-](\d{4})', str(date_str))
                if date_match:
                    m, d, y = date_match.groups()
                    return f"document_agenda_{y}_{m.zfill(2)}_{d.zfill(2)}"
                
                # Handle YYYY-MM-DD format
                date_match2 = re.search(r'(\d{4})[._\-](\d{1,2})[._\-](\d{1,2})', str(date_str))
                if date_match2:
                    y, m, d = date_match2.groups()
                    return f"document_agenda_{y}_{m.zfill(2)}_{d.zfill(2)}"
        
        # For verbatim transcripts, use standard format: document_verbatim_transcript_YYYY_MM_DD
        elif ('transcript' in doc_type or 'verbatim' in doc_type or 
              'transcript' in (entity.get('title') or '').lower() or
              'verbatim' in (entity.get('title') or '').lower()):
            if date_str:
                # Convert to YYYY_MM_DD format
                date_match = re.search(r'(\d{1,2})[._\-](\d{1,2})[._\-](\d{4})', str(date_str))
                if date_match:
                    m, d, y = date_match.groups()
                    return f"document_verbatim_transcript_{y}_{m.zfill(2)}_{d.zfill(2)}"
                
                # Handle YYYY-MM-DD format
                date_match2 = re.search(r'(\d{4})[._\-](\d{1,2})[._\-](\d{1,2})', str(date_str))
                if date_match2:
                    y, m, d = date_match2.groups()
                    return f"document_verbatim_transcript_{y}_{m.zfill(2)}_{d.zfill(2)}"
        
        # For ordinances and resolutions, use standard format
        elif ('ordinance' in doc_type or 'resolution' in doc_type or
              'ordinance' in (entity.get('title') or '').lower() or
              'resolution' in (entity.get('title') or '').lower()):
            # Extract ordinance/resolution number
            title_text = f"{entity.get('title') or ''} {entity.get('name') or ''}"
            
            # Look for ordinance/resolution numbers
            ord_match = re.search(r'ordinance\s+(?:no\.?\s*)?(\d{4}[-/]\d+|\d+)', title_text, re.I)
            res_match = re.search(r'resolution\s+(?:no\.?\s*)?([rR]?[-]?\d{4}[-/]\d+|\d+)', title_text, re.I)
            
            if ord_match:
                number = ord_match.group(1).replace('/', '-')
                return f"document_ordinance_{number.replace('-', '_')}"
            elif res_match:
                number = res_match.group(1).replace('/', '-').replace('R-', '').replace('r-', '')
                return f"document_resolution_{number.replace('-', '_')}"
        
        # For other document types, standardize based on type and date
        elif doc_type and date_str:
            # Convert to YYYY_MM_DD format
            date_match = re.search(r'(\d{1,2})[._\-](\d{1,2})[._\-](\d{4})', str(date_str))
            if date_match:
                m, d, y = date_match.groups()
                clean_type = re.sub(r'[^a-z0-9]', '_', doc_type.lower())
                return f"document_{clean_type}_{y}_{m.zfill(2)}_{d.zfill(2)}"
        
        # For other document types, keep existing ID if it follows standards
        current_id = entity.get('documentID') or entity.get('id')
        if current_id and current_id.startswith('document_'):
            return current_id
        
        return None
    
    def _preferred_event_id(self, entity: Dict) -> Optional[str]:
        """Generate preferred event ID using standardized YYYY_MM_DD format."""
        import re
        
        # For Events, use standard format: event_{name_slug}_YYYY_MM_DD
        name = entity.get('name') or ''
        if name and ('commission meeting' in name.lower() or 'meeting' in name.lower()):
            # Extract date from various fields
            date_str = (entity.get('dateTime') or entity.get('meetingDate') or 
                       entity.get('meeting_date') or entity.get('date') or '')
            
            if date_str:
                # Convert to YYYY_MM_DD format (US format: MM.DD.YYYY)
                date_match = re.search(r'(\d{1,2})[._\-](\d{1,2})[._\-](\d{4})', str(date_str))
                if date_match:
                    m, d, y = date_match.groups()
                    date_suffix = f"{y}_{m.zfill(2)}_{d.zfill(2)}"
                    # Generate standard Event ID
                    return f"event_city_commission_meeting_{date_suffix}"
                
                # Handle YYYY-MM-DD format
                date_match2 = re.search(r'(\d{4})[._\-](\d{1,2})[._\-](\d{1,2})', str(date_str))
                if date_match2:
                    y, m, d = date_match2.groups()
                    date_suffix = f"{y}_{m.zfill(2)}_{d.zfill(2)}"
                    return f"event_city_commission_meeting_{date_suffix}"
        
        # For other event types, keep existing ID if it follows standards
        current_id = entity.get('eventID') or entity.get('id')
        if current_id and current_id.startswith('event_'):
            return current_id
        
        return None
    
    
    def _fix_entity_prefix_id(self, entity_id: str, entity: Dict) -> Optional[str]:
        """Fix incorrectly typed entities with 'entity_' prefix."""
        import re
        
        # Extract the meaningful part after 'entity_'
        if entity_id.startswith('entity_'):
            suffix = entity_id[7:]  # Remove 'entity_' prefix
            
            # Determine correct entity type based on content
            name = (entity.get('name') or '').lower()
            title = (entity.get('title') or '').lower()
            content = f"{name} {title} {suffix}".lower()
            
            # Board/Organization entities
            if 'board' in content or 'committee' in content or 'commission' in content:
                return f"org_{suffix}"
            
            # Document entities
            elif 'minutes' in content or 'document' in content or 'report' in content:
                return f"document_{suffix}"
            
            # Location entities
            elif 'building' in content or 'address' in content or 'location' in content:
                return f"location_{suffix}"
            
            # Default: convert to organization (most generic)
            else:
                return f"org_{suffix}"
        
        return None
    
    def _preferred_organization_id(self, entity: Dict) -> Optional[str]:
        """Generate preferred organization ID using standardized format."""
        import re
        # Use name as primary identifier
        name = entity.get('name') or entity.get('title') or ''
        if name:
            # Clean and standardize name
            clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', name.lower())
            clean_name = re.sub(r'\s+', '_', clean_name.strip())
            return f"org_{clean_name}"
        
        # Keep existing ID if it follows standards
        current_id = entity.get('orgID') or entity.get('organizationID') or entity.get('id')
        if current_id and current_id.startswith('org_'):
            return current_id
        
        return None
    
    def _preferred_person_id(self, entity: Dict) -> Optional[str]:
        """Generate preferred person ID using standardized format."""
        import re
        # Use name as primary identifier
        name = entity.get('name') or entity.get('title') or ''
        if name:
            # Clean and standardize name
            clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', name.lower())
            clean_name = re.sub(r'\s+', '_', clean_name.strip())
            return f"person_{clean_name}"
        
        # Keep existing ID if it follows standards
        current_id = entity.get('personID') or entity.get('id')
        if current_id and current_id.startswith('person_'):
            return current_id
        
        return None
    
    def _preferred_location_id(self, entity: Dict) -> Optional[str]:
        """Generate preferred location ID using standardized format."""
        import re
        # Use name or address as primary identifier
        identifier = entity.get('name') or entity.get('address') or ''
        if identifier:
            # Clean and standardize identifier
            clean_id = re.sub(r'[^a-zA-Z0-9\s]', '', identifier.lower())
            clean_id = re.sub(r'\s+', '_', clean_id.strip())
            return f"location_{clean_id}"
        
        # Keep existing ID if it follows standards
        current_id = entity.get('locationID') or entity.get('id')
        if current_id and current_id.startswith('location_'):
            return current_id
        
        return None
    
    # ===== ENHANCED NORMALIZATION & SIMILARITY METHODS =====
    
    def _generate_semantic_aliases(self, entity: Dict, entity_type: str) -> str:
        """Generate semantic aliases for entity names to improve matching."""
        
        name = entity.get('name') or entity.get('title') or ''
        if not name:
            return ''
        
        # Create normalized versions
        aliases = set()
        clean_name = name.lower().strip()
        aliases.add(clean_name)
        
        # Remove common organization suffixes/prefixes
        if entity_type == 'Organization':
            normalized_org = self._normalize_org_name(clean_name)
            if normalized_org:
                aliases.add(normalized_org)
        
        # Remove common person title prefixes  
        elif entity_type == 'Person':
            normalized_person = self._normalize_person_name(clean_name)
            if normalized_person:
                aliases.add(normalized_person)
        
        # Create abbreviation variants
        abbreviated = self._create_abbreviations(clean_name)
        if abbreviated:
            aliases.add(abbreviated)
        
        return '|'.join(sorted(filter(None, aliases)))
    
    def _normalize_org_name(self, name: str) -> str:
        """Normalize organization names for better matching."""
        if not name:
            return ''
        
        # Remove common prefixes/suffixes without hardcoding specific values
        patterns = [
            r'\bcity\s+of\s+',           # "City of" prefix
            r'\s+commission\b',          # " Commission" suffix  
            r'\s+department\b',          # " Department" suffix
            r'\s+board\b',               # " Board" suffix
            r'\s+committee\b'            # " Committee" suffix
        ]
        
        normalized = name
        for pattern in patterns:
            normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE).strip()
        
        return normalized if normalized else name
    
    def _normalize_person_name(self, name: str) -> str:
        """Normalize person names for better matching."""
        if not name:
            return ''
        
        # Remove titles without hardcoding specific ones
        title_patterns = [
            r'^(mayor|commissioner|vice\s+mayor|mr\.?|ms\.?|mrs\.?|dr\.?)\s+',
            r'\s+(jr\.?|sr\.?|iii?|iv)$'
        ]
        
        normalized = name
        for pattern in title_patterns:
            normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE).strip()
        
        return normalized if normalized else name
    
    def _create_abbreviations(self, name: str) -> str:
        """Create common abbreviations for better matching."""
        if not name or len(name) < 5:
            return ''
        
        # Create acronym from words
        words = name.split()
        if len(words) > 1:
            acronym = ''.join(word[0] for word in words if word)
            return acronym.lower()
        
        return ''
    
    def _calculate_entity_similarity(self, entity1: Dict, entity2: Dict, entity_type: str) -> float:
        """Calculate similarity score considering attribute variations."""
        
        # Special handling for VoteOutcome entities to detect taxonomy vs NER duplicates
        if entity_type == 'VoteOutcome':
            return self._voteoutcome_similarity(entity1, entity2)
        
        # Core name similarity (most important)
        name_sim = self._name_similarity(entity1, entity2, entity_type)
        
        # Attribute overlap similarity
        attr_sim = self._attribute_similarity(entity1, entity2, entity_type)
        
        # Contextual similarity (dates, references)
        context_sim = self._contextual_similarity(entity1, entity2, entity_type)
        
        # Weighted combination
        weights = self._get_similarity_weights(entity_type)
        
        return (name_sim * weights['name'] + 
                attr_sim * weights['attributes'] + 
                context_sim * weights['context'])
    
    def _voteoutcome_similarity(self, entity1: Dict, entity2: Dict) -> float:
        """Enhanced VoteOutcome similarity to detect taxonomy vs NER duplicates."""
        
        # Extract document context for comparison (primary method)
        outcome1 = entity1.get('outcomeID', '')
        outcome2 = entity2.get('outcomeID', '')
        
        import re
        
        # Extract document info from outcome IDs
        match1 = re.search(r'outcome_(ordinance|resolution)_(\d{4})_(\d+)', outcome1)
        match2 = re.search(r'outcome_(ordinance|resolution)_(\d{4})_(\d+)', outcome2)
        
        if match1 and match2:
            type1, year1, num1 = match1.groups()
            type2, year2, num2 = match2.groups()
            
            # Same document type, year, and number = likely same vote outcome
            if type1 == type2 and year1 == year2 and num1 == num2:
                return 0.98  # Very high similarity - likely duplicates
        
        # Check meeting date if both available
        meeting1 = entity1.get('meetingDate', '')
        meeting2 = entity2.get('meetingDate', '')
        
        if meeting1 and meeting2 and meeting1 == meeting2:
            # Same meeting + similar outcome IDs = high similarity
            if outcome1 and outcome2:
                # Check for partial ID matches (e.g., outcome_ordinance_2024_01 vs outcome_ordinance_2024_01_09)
                base1 = outcome1.rstrip('_0123456789')  # Remove trailing numbers/underscores
                base2 = outcome2.rstrip('_0123456789')
                
                if base1 == base2 and base1:  # Same base pattern
                    return 0.97  # High similarity
        
        # Fallback to regular similarity calculation
        name_sim = self._name_similarity(entity1, entity2, 'VoteOutcome')
        attr_sim = self._attribute_similarity(entity1, entity2, 'VoteOutcome')  
        context_sim = self._contextual_similarity(entity1, entity2, 'VoteOutcome')
        
        weights = self._get_similarity_weights('VoteOutcome')
        
        return (name_sim * weights['name'] + 
                attr_sim * weights['attributes'] + 
                context_sim * weights['context'])
    
    def _get_voteoutcome_normalization_key(self, entity: Dict) -> str:
        """Generate normalization key for VoteOutcome to group similar entities."""
        
        outcome_id = entity.get('outcomeID', entity.get('id', ''))
        
        # Extract document type and number from outcome ID for grouping
        import re
        match = re.search(r'outcome_(ordinance|resolution)_(\d{4})_(\d+)', outcome_id)
        
        if match:
            doc_type, year, number = match.groups()
            # Use only document context for grouping (ignore variable meeting dates)
            # This will group outcome_ordinance_2024_01 and outcome_ordinance_2024_01_09 together
            return f"{doc_type}_{year}_{number}"
        
        # Fallback to original logic for non-standard IDs
        agenda_id = entity.get('agendaItemID', '')
        return f"{agenda_id}|{outcome_id}"
    
    def _should_keep_relationship(self, rel: Dict) -> bool:
        """Filter semantically incorrect relationships for merged entities."""
        
        source_id = rel.get('source', '')
        rel_type = rel.get('type', '')
        
        # For VoteOutcome entities, prefer votedOn over extractedFrom
        if 'outcome_' in source_id and rel_type == 'extractedFrom':
            # Check if a better votedOn relationship exists for this entity
            # This prevents extractedFrom from overriding correct semantic relationships
            rel_source = rel.get('_source', '')
            if 'ner_' in rel_source:
                # NER-generated extractedFrom relationship - should be filtered
                # if taxonomy votedOn relationships exist
                return False
        
        return True  # Keep all other relationships
    
    def _name_similarity(self, entity1: Dict, entity2: Dict, entity_type: str) -> float:
        """Calculate name similarity using fuzzy matching."""
        
        # Get normalized names
        name1 = entity1.get('name') or entity1.get('title') or ''
        name2 = entity2.get('name') or entity2.get('title') or ''
        
        if not name1 or not name2:
            return 0.0
        
        # Original names (for basic comparison)
        orig1 = name1.lower().strip()
        orig2 = name2.lower().strip()
        
        # Exact match
        if orig1 == orig2:
            return 1.0
        
        # Check containment first (before normalization)
        if orig1 in orig2 or orig2 in orig1:
            return 0.9
        
        # Special handling for abbreviations and short names
        # If one name is very short and contained in the other, likely same entity
        min_len = min(len(orig1), len(orig2))
        max_len = max(len(orig1), len(orig2))
        
        if min_len <= 10 and max_len >= min_len * 2:  # One is much shorter
            shorter = orig1 if len(orig1) < len(orig2) else orig2
            longer = orig2 if len(orig1) < len(orig2) else orig1
            
            # Check if shorter is likely an abbreviation of longer
            if (shorter in longer or 
                any(word.startswith(shorter) for word in longer.split()) or
                any(shorter.startswith(word) for word in longer.split() if len(word) > 2)):
                return 0.8  # High similarity for likely abbreviations
        
        # Normalize both names for further comparison
        norm1 = self._normalize_org_name(orig1) if entity_type == 'Organization' else self._normalize_person_name(orig1)
        norm2 = self._normalize_org_name(orig2) if entity_type == 'Organization' else self._normalize_person_name(orig2)
        
        # Exact match after normalization
        if norm1 and norm2 and norm1 == norm2:
            return 0.85
        
        # Check containment after normalization
        if norm1 and norm2:
            if norm1 in norm2 or norm2 in norm1:
                return 0.8
        
        # Word overlap similarity
        words1 = set(orig1.split())
        words2 = set(orig2.split())
        if words1 and words2:
            # Check if one is a subset of the other (high similarity)
            if words1.issubset(words2) or words2.issubset(words1):
                return 0.85  # High similarity for subset relationships
            
            # Regular word overlap
            word_overlap = len(words1 & words2) / len(words1 | words2)
            if word_overlap > 0.6:
                return 0.75 + (word_overlap * 0.15)  # 0.75-0.9 range
            elif word_overlap > 0.4:
                return 0.6 + (word_overlap * 0.15)   # 0.6-0.75 range
        
        # Character-based similarity (fallback)
        char_sim = self._string_similarity(norm1, norm2)
        return char_sim * 0.6  # Reduce weight for character-only similarity
    
    def _attribute_similarity(self, entity1: Dict, entity2: Dict, entity_type: str) -> float:
        """Calculate attribute overlap similarity."""
        
        # Get expected attributes for this entity type
        expected_attrs = UnifiedOntology.ENTITY_TYPES.get(entity_type, {}).get('attributes', [])
        
        if not expected_attrs:
            return 0.5  # Default similarity for types without defined attributes
        
        # Count matching non-null attributes
        matches = 0
        total_comparable = 0
        
        for attr in expected_attrs:
            val1 = entity1.get(attr)
            val2 = entity2.get(attr)
            
            # Skip if both are null/empty
            if not val1 and not val2:
                continue
                
            total_comparable += 1
            
            # Check for match (handle null variations)
            if self._values_match(val1, val2):
                matches += 1
        
        return matches / total_comparable if total_comparable > 0 else 0.5
    
    def _contextual_similarity(self, entity1: Dict, entity2: Dict, entity_type: str) -> float:
        """Calculate contextual similarity (dates, sources, references)."""
        
        context_score = 0.0
        
        # Same source file boosts similarity
        source1 = entity1.get('Source_File_Name', '')
        source2 = entity2.get('Source_File_Name', '')
        if source1 and source2 and source1 == source2:
            context_score += 0.3
        
        # Same meeting date boosts similarity
        date1 = entity1.get('meetingDate') or entity1.get('dateTime') or ''
        date2 = entity2.get('meetingDate') or entity2.get('dateTime') or ''
        if date1 and date2 and self._dates_match(date1, date2):
            context_score += 0.4
        
        # Same extraction chunk boosts similarity
        chunk1 = entity1.get('extraction_chunk_id', '')
        chunk2 = entity2.get('extraction_chunk_id', '')
        if chunk1 and chunk2 and chunk1 == chunk2:
            context_score += 0.3
        
        return min(context_score, 1.0)  # Cap at 1.0
    
    def _get_similarity_weights(self, entity_type: str) -> Dict[str, float]:
        """Get similarity calculation weights by entity type."""
        
        # Default weights (configurable via environment)
        defaults = {
            'name': 0.6,
            'attributes': 0.3,
            'context': 0.1
        }
        
        # Allow environment override without hardcoding
        name_weight = float(os.getenv(f"{entity_type.upper()}_NAME_WEIGHT", defaults['name']))
        attr_weight = float(os.getenv(f"{entity_type.upper()}_ATTR_WEIGHT", defaults['attributes']))  
        context_weight = float(os.getenv(f"{entity_type.upper()}_CONTEXT_WEIGHT", defaults['context']))
        
        # Normalize to sum to 1.0
        total = name_weight + attr_weight + context_weight
        if total > 0:
            return {
                'name': name_weight / total,
                'attributes': attr_weight / total,
                'context': context_weight / total
            }
        else:
            return defaults
    
    def _string_similarity(self, str1: str, str2: str) -> float:
        """Calculate character-based string similarity."""
        if not str1 or not str2:
            return 0.0
        
        if str1 == str2:
            return 1.0
        
        # Simple character overlap similarity
        set1 = set(str1.lower())
        set2 = set(str2.lower())
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        return intersection / union if union > 0 else 0.0
    
    def _values_match(self, val1, val2) -> bool:
        """Check if two values match, handling null variations."""
        # Handle null/empty variations
        if not val1 and not val2:
            return True
        if not val1 or not val2:
            return False
        
        # String comparison
        if isinstance(val1, str) and isinstance(val2, str):
            return val1.strip().lower() == val2.strip().lower()
        
        # Exact comparison for other types
        return val1 == val2
    
    def _dates_match(self, date1: str, date2: str) -> bool:
        """Check if two dates represent the same date."""
        if not date1 or not date2:
            return False
        
        # Use existing date normalization
        try:
            norm1 = EntityIDStandards.normalize_date_yyyymmdd(str(date1))
            norm2 = EntityIDStandards.normalize_date_yyyymmdd(str(date2))
            return norm1 == norm2 if norm1 and norm2 else False
        except Exception:
            return str(date1).strip() == str(date2).strip()
    
    def _cross_group_similarity_matching(self, name_groups: Dict, entity_type: str) -> Dict:
        """Find similar entities across different normalization groups."""
        
        additional_merges = {}
        group_keys = list(name_groups.keys())
        
        # Compare each group against others  
        for i, key1 in enumerate(group_keys):
            if key1 in additional_merges:  # Skip if already merged
                continue
                
            for j, key2 in enumerate(group_keys[i+1:], i+1):
                if key2 in additional_merges:  # Skip if already merged
                    continue
                
                # Get representative entities from each group
                entity1 = name_groups[key1][0] if name_groups[key1] else None
                entity2 = name_groups[key2][0] if name_groups[key2] else None
                
                if entity1 and entity2:
                    similarity = self._calculate_entity_similarity(entity1, entity2, entity_type)
                    
                    # If high similarity, merge groups
                    if similarity >= self.similarity_threshold:
                        # Merge group2 into group1
                        name_groups[key1].extend(name_groups[key2])
                        additional_merges[key2] = key1
                        
                        # Update merge map for all entities in group2
                        id_field = EntityIDStandards.get_id_field(entity_type)
                        canonical_id = entity1.get(id_field) or entity1.get('id')
                        
                        for entity in name_groups[key2]:
                            entity_id = entity.get(id_field) or entity.get('id')
                            if entity_id and canonical_id and entity_id != canonical_id:
                                self.merge_map[entity_id] = canonical_id
        
        # Remove merged groups
        for key in additional_merges:
            if key in name_groups:
                del name_groups[key]
        
        return additional_merges
    
    def __init__(self, similarity_threshold: float = 0.85):
        """
        Initialize deduplicator.
        
        Args:
            similarity_threshold: Minimum similarity for merging (0-1)
        """
        self.similarity_threshold = similarity_threshold
        self.toolkit = GraphEntityToolkit()
        self.merge_map = {}  # old_id -> canonical_id
        self.entity_groups = defaultdict(list)  # canonical_id -> [entities]
    
    async def deduplicate_multi_source(self, 
                                      ner_dir: Path, 
                                      registry_dir: Path) -> Dict[str, str]:
        """
        Deduplicate across NER and taxonomy sources.
        
        Args:
            ner_dir: Directory with NER extracted entities
            registry_dir: Directory with taxonomy entities
            
        Returns:
            Merge map: {old_id: canonical_id}
        """
        if DEBUG_ENTITY_DEDUPLICATION:
            log.info("🧹 DEBUG [DEDUPLICATION] Starting multi-source deduplication")
            log.info(f"🧹 DEBUG [DEDUPLICATION] NER directory: {ner_dir}")
            log.info(f"🧹 DEBUG [DEDUPLICATION] Registry directory: {registry_dir}")
        
        log.info("🔄 Starting multi-source deduplication")
        
        # Load all entities from both sources
        all_entities = {}
        
        # Load NER entities
        ner_entities = await self._load_entities_from_dir(ner_dir, "ner")
        if DEBUG_ENTITY_DEDUPLICATION:
            ner_count = sum(len(entities) for entities in ner_entities.values())
            log.info(f"🧹 DEBUG [DEDUPLICATION] Loaded {ner_count} NER entities from {len(ner_entities)} types")
            for entity_type, entities in ner_entities.items():
                log.info(f"🧹 DEBUG [DEDUPLICATION]   {entity_type}: {len(entities)} entities")
        
        for entity_type, entities in ner_entities.items():
            if entity_type not in all_entities:
                all_entities[entity_type] = []
            all_entities[entity_type].extend(entities)
        
        # Load taxonomy entities
        taxonomy_entities = await self._load_entities_from_dir(registry_dir, "taxonomy")
        if DEBUG_ENTITY_DEDUPLICATION:
            taxonomy_count = sum(len(entities) for entities in taxonomy_entities.values())
            log.info(f"🧹 DEBUG [DEDUPLICATION] Loaded {taxonomy_count} taxonomy entities from {len(taxonomy_entities)} types")
            for entity_type, entities in taxonomy_entities.items():
                log.info(f"🧹 DEBUG [DEDUPLICATION]   {entity_type}: {len(entities)} entities")
        
        for entity_type, entities in taxonomy_entities.items():
            if entity_type not in all_entities:
                all_entities[entity_type] = []
            all_entities[entity_type].extend(entities)
        
        # Deduplicate each entity type
        total_before = sum(len(entities) for entities in all_entities.values())
        
        for entity_type, entities in all_entities.items():
            before_count = len(entities)
            if DEBUG_ENTITY_DEDUPLICATION:
                log.info(f"🧹 DEBUG [DEDUPLICATION] Processing {entity_type}: {before_count} entities before deduplication")
            
            log.info(f"Deduplicating {len(entities)} {entity_type} entities")
            await self._deduplicate_entity_type(entity_type, entities)
        
        log.info(f"✅ Created merge map with {len(self.merge_map)} mappings")
        return self.merge_map
    
    async def _load_entities_from_dir(self, base_dir: Path, 
                                     source_label: str) -> Dict[str, List[Dict]]:
        """
        Load all entities from a directory.
        
        Args:
            base_dir: Base directory containing entity subdirectories
            source_label: Label for source tracking
            
        Returns:
            Dict of entity_type -> list of entities
        """
        entities_by_type = defaultdict(list)
        
        if not base_dir.exists():
            log.warning(f"Directory not found: {base_dir}")
            return entities_by_type
        
        # Helper function to process entity files
        def _process_entity_file(json_file: Path, entity_type: str):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Extract entities from file
                file_entities = []
                # Accept grouped format: {"entities": [...]}
                if isinstance(data, dict) and isinstance(data.get('entities'), list):
                    file_entities = data.get('entities', [])
                # Accept single-entity JSON files (SimpleNERWriter format): {...}
                elif isinstance(data, dict):
                    id_field_guess = EntityIDStandards.get_id_field(entity_type)
                    if data.get(id_field_guess) or data.get('id'):
                        file_entities = [data]
                # Accept list-of-entities format: [{...}, {...}]
                elif isinstance(data, list):
                    file_entities = [e for e in data if isinstance(e, dict)]
                
                # Add source tracking
                for entity in file_entities:
                    # Work on a per-entity alias; do NOT mutate directory-level entity_type
                    etype = entity_type
                    if '_sources' not in entity:
                        entity['_sources'] = []
                    entity['_sources'].append(f"{source_label}_{json_file.stem}")
                    # keep a stable .type for downstream rules if missing
                    entity.setdefault('type', etype)
                    # --- Canonicalize common fields on ingest ---
                    if 'document_type' in entity and 'documentType' not in entity:
                        entity['documentType'] = entity.pop('document_type')
                    if 'Document_Type' in entity and 'documentType' not in entity:
                        entity['documentType'] = entity.pop('Document_Type')
                    if 'meeting_date' in entity and 'meetingDate' not in entity:
                        entity['meetingDate'] = entity.pop('meeting_date')
                    if 'Meeting_Date' in entity and 'meetingDate' not in entity:
                        entity['meetingDate'] = entity.pop('Meeting_Date')
                    # Agenda codes → keep 'code' and mirror to 'itemID'
                    if 'itemCode' in entity and 'code' not in entity:
                        entity['code'] = entity['itemCode']
                    if 'agendaCode' in entity and 'code' not in entity:
                        entity['code'] = entity['agendaCode']
                    if entity.get('code') and not entity.get('itemID'):
                        entity['itemID'] = entity['code']
                    # Policy number ↔ policyType mirroring (best-effort, no data loss)
                    if entity.get('policyType') == 'ordinance' and not entity.get('ordinanceNumber') and entity.get('resolutionNumber'):
                        # Copy rather than pop to avoid losing the original field
                        entity['ordinanceNumber'] = entity.get('resolutionNumber')
                    if entity.get('policyType') == 'resolution' and not entity.get('resolutionNumber') and entity.get('ordinanceNumber'):
                        entity['resolutionNumber'] = entity.get('ordinanceNumber')

                    # Create-correct-at-origin ethos: do NOT rewrite types here.
                    # If upstream emitted non-canonical types, warn so we can fix at origin.
                    if etype == 'Meeting':
                        log.warning("Create-at-origin policy: encountered type 'Meeting' in %s; upstream should emit 'Event'. Leaving unchanged.", json_file.name)
                    if etype == 'Topic' and str(entity.get('category','')).lower() in {'meeting section','agenda_section'}:
                        log.warning("Create-at-origin policy: encountered Topic{category=agenda_section} in %s; upstream should emit 'Section'. Leaving unchanged.", json_file.name)

                    # Normalize ID fields AFTER final etype decision (no in-method retagging)
                    entity = EntityIDStandards.normalize_entity_id_fields(entity, etype)
                    # Ensure entity has the right ID field present
                    id_field = EntityIDStandards.get_id_field(etype)
                    if id_field not in entity and 'id' in entity:
                        entity[id_field] = entity['id']
                    
                    entities_by_type[etype].append(entity)
                    
            except Exception as e:
                log.error(f"Error loading {json_file}: {e}")

        # Helper: infer entity_type from an aggregated file (e.g., Person.json)
        def _infer_entity_type_from_file(json_file: Path) -> Optional[str]:
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                et = data.get("entity_type") or data.get("type")
                if isinstance(et, str) and et.strip():
                    return et.strip()
            except Exception:
                pass
            stem = json_file.stem
            return stem[:1].upper() + stem[1:] if stem else None

        # Iterate through entity type directories **and** support aggregated files in base_dir
        for entity_dir in base_dir.iterdir():
            if not entity_dir.is_dir():
                # Support aggregated per-type files directly under base_dir (e.g., Person.json)
                if entity_dir.suffix.lower() == ".json":
                    inferred_type = _infer_entity_type_from_file(entity_dir)
                    if inferred_type:
                        _process_entity_file(entity_dir, inferred_type)
                    else:
                        log.warning("Skipping aggregated entity file with unknown type: %s", entity_dir.name)
                continue
            # skip non-entity buckets in root
            if entity_dir.name in {"relationships", "registry", "merged", "document_chunks", "indices"}:
                continue
            
            if entity_dir.name == "entities":
                # Walk one more level: entities/<Type>/*.json **and** aggregated files (entities/Person.json)
                for typed_dir in entity_dir.iterdir():
                    # Aggregated per-type file under 'entities' (e.g., entities/Person.json)
                    if typed_dir.is_file() and typed_dir.suffix.lower() == ".json":
                        inferred_type = _infer_entity_type_from_file(typed_dir)
                        if inferred_type:
                            _process_entity_file(typed_dir, inferred_type)
                        else:
                            log.warning("Skipping aggregated entity file with unknown type: %s", typed_dir.name)
                        continue
                    if not typed_dir.is_dir():
                        continue
                    # Skip potential indices or other non-entity subdirs
                    if typed_dir.name in {"indices", "merged"}:
                        continue
                    entity_type = typed_dir.name
                    for json_file in typed_dir.glob("*.json"):
                        _process_entity_file(json_file, entity_type)
                # Done with ./entities container; continue to next top-level dir
                continue
            
            entity_type = entity_dir.name
            
            # Load all JSON files in this entity directory
            for json_file in entity_dir.glob("*.json"):
                _process_entity_file(json_file, entity_type)
        
        # Optional compact debug summary (counts + a few IDs per type)
        try:
            if DEBUG_ENTITY_DEDUPLICATION or self.MERGE_DEBUG_ON:
                debug_dir = base_dir / "debug"
                debug_dir.mkdir(parents=True, exist_ok=True)
                summary = {"source": source_label, "root": str(base_dir), "by_type": {}}
                total = 0
                for etype, ents in entities_by_type.items():
                    id_field = EntityIDStandards.get_id_field(etype)
                    ids = []
                    for e in ents[:10]:
                        ids.append(e.get("id") or e.get(id_field))
                    # AgendaItem completeness (helps spot ID/date/code issues fast)
                    ag_stats = None
                    if etype == "AgendaItem":
                        have_both = sum(1 for e in ents if (e.get("itemID") or e.get("code")) and (e.get("meetingDate") or e.get("meeting_date")))
                        ag_stats = {"count": len(ents), "have_item_code_and_meeting_date": have_both}
                    summary["by_type"][etype] = {
                        "count": len(ents),
                        "sample_ids": [i for i in ids if i],
                        **({"agenda_item_stats": ag_stats} if ag_stats else {})
                    }
                    total += len(ents)
                summary["total_loaded"] = total
                with open(debug_dir / f"load_{source_label}.json", "w", encoding="utf-8") as f:
                    json.dump(summary, f, indent=2)
        except Exception:
            pass

        return dict(entities_by_type)
    
    async def _deduplicate_entity_type(self, entity_type: str, 
                                      entities: List[Dict]) -> None:
        """Deduplicate entities of a specific type."""
        if not entities:
            return
        
        # Get ID field for this entity type
        id_field = EntityIDStandards.get_id_field(entity_type)
        
        # First pass: Group by normalized name
        name_groups = defaultdict(list)
        
        for entity in entities:
            entity_id = entity.get(id_field) or entity.get('id')
            if not entity_id:
                continue
            
            norm_key = self._get_normalization_key(entity, entity_type)
            name_groups[norm_key].append(entity)
        
        # Second pass: Check for XXX duplicates across groups
        xxx_merge_candidates = self._find_xxx_duplicates(entities, entity_type, id_field)
        
        # Merge XXX duplicates into existing groups
        for xxx_id, canonical_id in xxx_merge_candidates.items():
            # Find which group contains the xxx entity
            xxx_entity = None
            canonical_entity = None
            
            for entity in entities:
                eid = entity.get(id_field) or entity.get('id')
                if eid == xxx_id:
                    xxx_entity = entity
                elif eid == canonical_id:
                    canonical_entity = entity
            
            if xxx_entity and canonical_entity:
                # Add to merge map
                self.merge_map[xxx_id] = canonical_id
                
                # Add xxx entity to canonical's group
                canonical_key = self._get_normalization_key(canonical_entity, entity_type)
                if xxx_entity not in name_groups[canonical_key]:
                    name_groups[canonical_key].append(xxx_entity)
        
        # Continue with existing group processing...
        for norm_key, group in name_groups.items():
            if len(group) == 1:
                entity = group[0]
                entity_id = entity.get(id_field) or entity.get('id')
                self.entity_groups[entity_id] = [entity]
                continue
            
            canonical = self._select_canonical_entity(group)
            canonical_id = canonical.get(id_field) or canonical.get('id')
            
            for entity in group:
                entity_id = entity.get(id_field) or entity.get('id')
                if entity_id != canonical_id:
                    self.merge_map[entity_id] = canonical_id
            
            # Ensure canonical is first so it becomes the merge base
            group_sorted = [canonical] + [e for e in group 
                                          if (e.get(id_field) or e.get('id')) != canonical_id]
            self.entity_groups[canonical_id] = group_sorted
        
        # Enhanced similarity matching across groups (optional)
        if os.getenv("ENABLE_CROSS_GROUP_MATCHING", "false").lower() == "true":
            log.debug(f"🔍 Applying enhanced cross-group similarity matching for {entity_type}")
            additional_merges = self._cross_group_similarity_matching(name_groups, entity_type)
            if additional_merges:
                log.info(f"✨ Enhanced matching found {len(additional_merges)} additional merges for {entity_type}")
            else:
                log.debug(f"🔍 No additional merges found for {entity_type}")
    
    def _get_normalization_key(self, entity: Dict, entity_type: str) -> str:
        """
        Get normalized key for entity grouping.
        
        Args:
            entity: Entity dict
            entity_type: Entity type
            
        Returns:
            Normalized key string
        """
        # Enhanced Document normalization - MORE SPECIFIC
        if entity_type == 'Document':
            return self._get_document_normalization_key(entity)
        elif entity_type == 'AgendaItem':
            # Prefer E-code + meeting date (even if old IDs didn't have it)
            e_code = self._extract_e_code(entity)
            code_norm = EntityIDStandards.clean_agenda_code(e_code).lower()
            date_norm = EntityIDStandards.normalize_date_yyyymmdd(entity.get('meetingDate') or entity.get('meeting_date') or entity.get('date') or "")
            if code_norm and date_norm:
                return f"{code_norm}|{date_norm}"
            # Fallback
            id_field = EntityIDStandards.get_id_field(entity_type)
            return entity.get(id_field) or entity.get('id', 'unknown')
        elif entity_type == 'VoteOutcome':
            # Enhanced VoteOutcome grouping to enable duplicate detection
            return self._get_voteoutcome_normalization_key(entity)
        
        # Priority fields for normalization
        key_fields = {
            'Person': ['name'],
            'Organization': ['name'],
            'Document': ['title', 'documentID'],
            'Policy': ['ordinanceNumber', 'resolutionNumber', 'title', 'policyID'],
            'AgendaItem': ['itemID', 'title'],
            'Event': ['name', 'dateTime'],
            'Location': ['name', 'address'],
            'Asset': ['name', 'assetID'],
            'Project': ['name', 'projectID'],
            'Role': ['title'],
            'Topic': ['name'],
            'Contract': ['contractID', 'title'],
            'Technology': ['name', 'vendor'],
            'VoteOutcome': ['agendaItemID', 'outcomeID']
        }
        
        fields = key_fields.get(entity_type, ['name'])
        
        # Build key from available fields
        key_parts = []
        for field in fields:
            if field in entity and entity[field]:
                value = str(entity[field]).lower().strip()
                # Normalize common variations
                value = value.replace(',', '').replace('.', '').replace('-', ' ')
                key_parts.append(value)
        
        if key_parts:
            return '|'.join(key_parts)
        
        # Fallback to entity ID
        id_field = EntityIDStandards.get_id_field(entity_type)
        return entity.get(id_field, 'unknown')
    
    def _get_document_normalization_key(self, entity: Dict) -> str:
        """Generate unique normalization key for documents including distinguishing details."""
        import re
        
        # Get identifying fields (ensure no None values)
        doc_id = entity.get('documentID') or ''
        name = entity.get('name') or ''
        title = entity.get('title') or ''
        doc_type = (entity.get('documentType') or entity.get('document_type') or '')
        doc_type = doc_type.lower() if isinstance(doc_type, str) else ''
        if not doc_type:
            t = entity.get('type')
            if isinstance(t, str):
                doc_type = t.lower()
            elif entity.get('documentID') or entity.get('entity_type') in ('Document', 'AgendaDocument'):
                doc_type = 'document'
        
        # Extract date
        text = f"{doc_id} {name} {title}"
        date_match = re.search(r'(\d{1,2})[._\-](\d{1,2})[._\-](\d{4})', text)
        if date_match:
            m, d, y = date_match.groups()
            date_key = f"{y}{m.zfill(2)}{d.zfill(2)}"
        else:
            date_match2 = re.search(r'(\d{4})[._\-](\d{1,2})[._\-](\d{1,2})', text)
            if date_match2:
                y, m, d = date_match2.groups()
                date_key = f"{y}{m.zfill(2)}{d.zfill(2)}"
            else:
                date_key = "unknown"
        
        # Extract unique identifiers based on document type
        unique_part = ""
        
        # For ordinances/resolutions, include document number
        if 'ordinance' in doc_type or 'resolution' in doc_type:
            # Look for document number pattern (e.g., "2024-01", "SOE-123")
            num_match = re.search(r'(\d{4}-\d+|SOE-\d+|CG-\d+|EO-\d+|CAO-\d+|\d{6})', text)
            if num_match:
                unique_part = num_match.group(1).replace('-', '_')
        
        # For agenda documents, include meeting identifier
        elif 'agenda' in doc_type or 'agenda' in doc_id.lower() or 'agenda' in (title or '').lower():
            # Look for agenda item code (e.g., "E-1", "C-2")
            item_match = re.search(r'([A-Z]-?\d+)', text)
            if item_match:
                unique_part = item_match.group(1).replace('-', '_')
            # Check if it's the main agenda document
            elif ('main' in title.lower() or doc_id.endswith(date_key) or 
                  'commission agenda' in title.lower() or 'city commission' in title.lower() or
                  doc_id.startswith('agenda_') or doc_id.startswith('document_agenda_')):
                unique_part = "main"
            else:
                # Use part of title as unique identifier
                unique_part = re.sub(r'[^a-zA-Z0-9]', '_', title.lower())[:20]
        
        # For verbatim transcripts, MUST include item codes to distinguish them
        elif 'transcript' in doc_type or 'verbatim' in doc_type or 'verbatim_transcript' in doc_type:
            # Extract ALL item codes from the title/name (e.g., "Verbatim Transcript - E-1, E-2, E-3")
            codes_match = re.findall(r'([A-Z]-?\d+)', text)
            if codes_match:
                # Sort codes for consistency and join them
                codes_sorted = sorted(set(codes_match))
                unique_part = '_'.join(codes_sorted).replace('-', '')
            else:
                # Check for item codes in JSON array format (from title field)
                # e.g., "Verbatim Transcript - ['E-1', 'E-2']"
                array_match = re.search(r'\[(.*?)\]', title)
                if array_match:
                    codes_text = array_match.group(1)
                    codes = re.findall(r'[A-Z]-?\d+', codes_text)
                    if codes:
                        codes_sorted = sorted(set(codes))
                        unique_part = '_'.join(codes_sorted).replace('-', '')
                
                # If still no codes found, use document ID or title hash
                if not unique_part:
                    if 'transcript' in doc_id:
                        # Extract unique part from doc_id
                        parts = doc_id.split('_')
                        for part in parts:
                            if part not in ['transcript', 'verbatim', date_key]:
                                unique_part = part
                                break
                    
                    if not unique_part:
                        # Use hash of title for uniqueness
                        import hashlib
                        unique_part = hashlib.sha256(title.encode()).hexdigest()[:8]
        
        # For other documents, use part of the document ID or title
        if not unique_part:
            if doc_id and doc_id != 'unknown':
                # Use last significant part of doc_id
                parts = doc_id.split('_')
                for part in reversed(parts):
                    if part not in ['document', date_key, 'unknown']:
                        unique_part = part[:20]
                        break
            
            if not unique_part and title:
                # Use sanitized title part
                unique_part = re.sub(r'[^a-zA-Z0-9]', '_', title.lower())[:20]
        
        # Build final key with type, date, and unique part
        if unique_part:
            return f"{doc_type}_{date_key}_{unique_part}"
        else:
            # Fallback: use hash of full content to ensure uniqueness
            import hashlib
            content_hash = hashlib.sha256(f"{doc_id}{name}{title}".encode()).hexdigest()[:8]
            return f"{doc_type}_{date_key}_{content_hash}"
    
    def _select_canonical_entity(self, group: List[Dict]) -> Dict:
        """
        Select the canonical entity from a group.
        Priority: taxonomy > ner, then most complete.
        
        Args:
            group: List of duplicate entities
            
        Returns:
            Selected canonical entity
        """
        # Sort by source priority and completeness
        def entity_score(entity):
            score = 0
            
            # Source priority
            sources = entity.get('_sources', [])
            if any('taxonomy' in s for s in sources):
                score += 1000
            elif any('seed' in s for s in sources):
                score += 500
            
            # Prefer new naming patterns for canonical IDs
            eid = str(entity.get('id') or '')
            if re.match(r'^policy_(ordinance|resolution)_\d{4}_\d+_[0-9a-f]{8}$', eid):
                score += 200
            # Strongly prefer date-based AgendaItem IDs (agenda_item_E4_2024_01_09)
            if re.match(r'^agenda_item_[A-Z]\d+_\d{4}_\d{2}_\d{2}$', eid):
                score += 220
            # Still give some credit to legacy hash-based IDs to avoid regressions
            if re.match(r'^agendaitem_[A-Z]\d+_[0-9a-f]{8}$', eid):
                score += 120

            # Completeness (non-null attributes)
            for key, value in entity.items():
                if not key.startswith('_') and value is not None:
                    score += 1
            
            return score
        
        return max(group, key=entity_score)
    
    def _apply_id_naming_upgrades(self, entities_by_type: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
        out: Dict[str, List[Dict]] = {}
        for etype, ents in entities_by_type.items():
            etype_canon = self._canon_type(etype)
            id_field = EntityIDStandards.get_id_field(etype_canon)
            bucket: Dict[str, Dict] = {}
            for e in ents:
                # normalize type on the entity itself
                e['type'] = self._canon_type(e.get('type') or etype_canon)
                cur_id = e.get('id') or e.get(id_field)
                new_id = None
                if etype_canon == 'Policy':
                    new_id = self._preferred_policy_id(e)
                elif etype_canon == 'AgendaItem':
                    new_id = self._preferred_agendaitem_id(e)
                elif etype_canon == 'Document':
                    new_id = self._preferred_document_id(e)
                elif etype_canon == 'Event':
                    new_id = self._preferred_event_id(e)
                elif etype_canon == 'LegalDocument':
                    # LegalDocument should be treated as Document
                    new_id = self._preferred_document_id(e)
                elif etype_canon == 'Organization':
                    new_id = self._preferred_organization_id(e)
                elif etype_canon == 'Person':
                    new_id = self._preferred_person_id(e)
                elif etype_canon == 'Location':
                    new_id = self._preferred_location_id(e)
                
                # Handle incorrectly typed entities with "entity_" prefix
                if not new_id and cur_id and cur_id.startswith('entity_'):
                    new_id = self._fix_entity_prefix_id(cur_id, e)
                # If we can compute a preferred new id and it differs, map & rewrite
                target_id = new_id if (new_id and new_id != cur_id) else cur_id
                if not target_id:
                    # skip entities without any usable ID
                    continue
                if target_id != cur_id:
                    self.merge_map[cur_id] = target_id
                    e['id'] = target_id
                    e[id_field] = target_id
                # Collapse duplicates under the same target_id
                if target_id in bucket:
                    bucket[target_id] = self.toolkit.merge_entities(bucket[target_id], e)
                else:
                    bucket[target_id] = e
            out[etype_canon] = list(bucket.values())
        return out
    
    async def generate_merge_manifest(self, output_dir: Path) -> None:
        """
        Generate merged entity and relationship manifests.
        
        Args:
            output_dir: Directory to write merged manifests
        """
        merged_dir = Path(output_dir) / "merged"
        entities_dir = merged_dir / "entities"
        entities_dir.mkdir(parents=True, exist_ok=True)
        
        log.info("📝 Generating merged manifests")
        
        # Process entities by type (pre-merge groups formed earlier)
        entities_by_type = defaultdict(list)
        
        for canonical_id, group in self.entity_groups.items():
            if not group:
                continue
            
            # Merge all entities in group
            merged = group[0].copy()
            for entity in group[1:]:
                merged = self.toolkit.merge_entities(merged, entity)
            
            # Determine entity type (canonicalized)
            entity_type = self._canon_type(merged.get('type'))
            if not entity_type:
                # Try to infer from ID field
                for etype in ['Person', 'Organization', 'Document', 'Policy', 
                            'Event', 'Location', 'AgendaItem', 'Asset', 
                            'Project', 'Role', 'Topic', 'Contract', 
                            'Technology', 'VoteOutcome']:
                    id_field = EntityIDStandards.get_id_field(etype)
                    if id_field in merged:
                        entity_type = self._canon_type(etype)
                        break
            if not entity_type:
                # Fallback: infer from any present ID value
                any_id = (
                    merged.get('id') or merged.get('documentID') or merged.get('policyID') or
                    merged.get('agendaItemID') or merged.get('personID') or merged.get('orgID') or
                    merged.get('eventID') or merged.get('locationID') or merged.get('sectionID')
                )
                if any_id:
                    try:
                        entity_type = EntityIDStandards.infer_type_from_id(str(any_id))
                    except Exception:
                        entity_type = None

            # Last-mile guard: if it has a documentID, treat it as a Document
            if not entity_type and (merged.get('documentID') or merged.get('document_id')):
                entity_type = 'Document'
            
            if entity_type:
                # normalize the in-entity type as well
                merged['type'] = entity_type
                # NEW: Pad ontology attributes for all entity types
                try:
                    ensure_min_entity_props(merged, entity_type)
                except Exception:
                    pass
                # Keep the existing Document minimums logic
                if entity_type == 'Document' or merged.get('documentID'):
                    ensure_min_document_props(merged)
                entities_by_type[entity_type].append(merged)

        # --- New: Upgrade IDs to the preferred naming and collapse duplicates ---
        entities_by_type = self._apply_id_naming_upgrades(entities_by_type)
        
        # Save merged entities by type
        for entity_type, entities in entities_by_type.items():
            filepath = entities_dir / f"{entity_type}.json"
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump({
                    "entity_type": entity_type,
                    "count": len(entities),
                    "entities": entities,
                    "_metadata": {
                        "merge_timestamp": self._get_timestamp(),
                        "source_counts": self._count_sources(entities)
                    }
                }, f, indent=2, ensure_ascii=False)
            
            log.info(f"  Saved {len(entities)} {entity_type} entities")
        
        # Process relationships (uses merge_map including our renames)
        await self._merge_relationships(output_dir, merged_dir)
        
        # Save merge map
        merge_map_file = merged_dir / "merge_map.json"
        with open(merge_map_file, 'w', encoding='utf-8') as f:
            json.dump({
                "mappings": self.merge_map,
                "statistics": {
                    "total_mappings": len(self.merge_map),
                    "canonical_entities": len(self.entity_groups)
                },
                "timestamp": self._get_timestamp()
            }, f, indent=2, ensure_ascii=False)
        
        log.info(f"✅ Merged manifests saved to {merged_dir}")
    
    async def _merge_relationships(self, source_dir: Path, merged_dir: Path) -> None:
        """
        Merge relationships and update IDs based on merge map.
        
        Args:
            source_dir: Source directory with NER/taxonomy data
            merged_dir: Output directory for merged data
        """
        all_relationships = []
        
        if DEBUG_RELATIONSHIP_LINKING:
            log.info("🔗 DEBUG [RELATIONSHIPS] Starting relationship merging")
            log.info(f"🔗 DEBUG [RELATIONSHIPS] Source directory: {source_dir}")
            log.info(f"🔗 DEBUG [RELATIONSHIPS] Merged directory: {merged_dir}")
        
        # Shared normalizer (kept in common/)
        from scripts.graph_rag_stages.common.relationship_labels import normalize_rel_label

        # Load relationships from NER
        ner_rel_dir = source_dir / "relationships"
        if DEBUG_RELATIONSHIP_LINKING:
            log.info(f"🔗 DEBUG [RELATIONSHIPS] Checking NER relationships: {ner_rel_dir}")
            log.info(f"🔗 DEBUG [RELATIONSHIPS] NER rel dir exists: {ner_rel_dir.exists()}")
        
        if ner_rel_dir.exists():
            ner_rel_files = list(ner_rel_dir.glob("*.json"))
            if DEBUG_RELATIONSHIP_LINKING:
                log.info(f"🔗 DEBUG [RELATIONSHIPS] Found {len(ner_rel_files)} NER relationship files")
                
            for rel_file in ner_rel_files:
                try:
                    with open(rel_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    # Accept both dict payloads with "relationships" and raw list payloads
                    if isinstance(data, list):
                        relationships = data
                    else:
                        relationships = data.get('relationships', [])

                    cleaned = []
                    skipped = 0
                    for rel in relationships:
                        if not isinstance(rel, dict):
                            skipped += 1
                            continue

                        if '_source' not in rel:
                            rel['_source'] = f"ner_{rel_file.stem}"

                        # align older payloads that might use 'properties'
                        if 'attributes' not in rel and 'properties' in rel:
                            rel['attributes'] = rel.pop('properties')
                        # normalize relationship type to canonical
                        rel['type'] = normalize_rel_label((rel.get('type') or "").strip())

                        # ensure attributes is a dict (prevents crashes later)
                        if rel.get('attributes') is not None and not isinstance(rel.get('attributes'), dict):
                            rel['attributes'] = {}

                        cleaned.append(rel)

                    if skipped:
                        log.warning("Skipped %d malformed relationship entries in %s", skipped, rel_file.name)

                    all_relationships.extend(cleaned)
                    
                    if DEBUG_RELATIONSHIP_LINKING:
                        log.info(f"🔗 DEBUG [RELATIONSHIPS] {rel_file.name}: {len(cleaned)} relationships")
                    
                except Exception as e:
                    log.error(f"Error loading relationships from {rel_file}: {e}")
                    if DEBUG_RELATIONSHIP_LINKING:
                        log.error(f"🔗 DEBUG [RELATIONSHIPS] ❌ Failed to load {rel_file.name}: {e}")
        elif DEBUG_RELATIONSHIP_LINKING:
            log.warning(f"🔗 DEBUG [RELATIONSHIPS] ❌ NER relationships directory does not exist")
        
        # Load relationships from taxonomy
        tax_rel_dir = source_dir / "registry" / "relationships"
        if tax_rel_dir.exists():
            for rel_file in tax_rel_dir.glob("*.json"):
                try:
                    with open(rel_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    if isinstance(data, dict):
                        relationships = data.get('relationships', [])
                    elif isinstance(data, list):
                        relationships = data
                    else:
                        log.warning("Skipping %s: unexpected JSON type %s", rel_file.name, type(data).__name__)
                        relationships = []

                    cleaned = []
                    skipped = 0
                    for rel in relationships:
                        if not isinstance(rel, dict):
                            skipped += 1
                            continue

                        if '_source' not in rel:
                            rel['_source'] = f"taxonomy_{rel_file.stem}"

                        if 'attributes' not in rel and 'properties' in rel:
                            rel['attributes'] = rel.pop('properties')

                        rel_type_str = str(rel.get('type') or "").strip()
                        rel['type'] = normalize_rel_label(rel_type_str)

                        if rel.get('attributes') is not None and not isinstance(rel.get('attributes'), dict):
                            rel['attributes'] = {}

                        cleaned.append(rel)

                    if skipped:
                        log.warning("Skipped %d malformed relationship entries in %s", skipped, rel_file.name)

                    all_relationships.extend(cleaned)
                except Exception as e:
                    log.error(f"Error loading relationships from {rel_file}: {e}")
        
        # Update relationship IDs based on merge map
        updated_relationships = []
        seen_edges = set()
        # canonical set from the merged entities just written to disk
        canonical_ids = set()
        entities_dir = merged_dir / "entities"
        if entities_dir.exists():
            for f in entities_dir.glob("*.json"):
                try:
                    with open(f, "r", encoding="utf-8") as fh:
                        data = json.load(fh)
                    etype = f.stem
                    id_field = EntityIDStandards.get_id_field(etype)
                    for ent in data.get("entities", []):
                        eid = ent.get("id") or ent.get(id_field)
                        if eid:
                            canonical_ids.add(eid)
                except Exception:
                    pass
        rewired_edges = 0
        by_type = defaultdict(int)
        unresolved_by_type = defaultdict(int)
        attrs_nonempty_by_type = defaultdict(int)
        attrs_keys_sum_by_type = defaultdict(int)
        unresolved_edges = []
        unresolved_seen = set()

        def _resolve_canonical(eid):
            """Follow merge chain A->B->C until it stabilizes; protect against loops."""
            if not eid:
                return None
            seen = set()
            cur = eid
            while cur in self.merge_map and cur not in seen:
                seen.add(cur)
                cur = self.merge_map[cur]
            return cur
        
        # allow toggling unresolved-edge retention (defaults to keep)
        keep_unresolved = os.getenv("MERGE_KEEP_UNRESOLVED_EDGES", "true").lower() in ("1", "true", "yes")

        for rel in all_relationships:
            # Rewire endpoints using transitive merge resolution
            source_id_original = rel.get('source')
            target_id_original = rel.get('target')
            source_id = _resolve_canonical(source_id_original)
            target_id = _resolve_canonical(target_id_original)
            if source_id != source_id_original or target_id != target_id_original:
                rewired_edges += 1
            rel['source'] = source_id
            rel['target'] = target_id

            # If an endpoint isn't present in the canonical set, keep the edge,
            # but mark it as unresolved so downstream can decide how to handle it.
            unresolved = []
            if source_id not in canonical_ids:
                unresolved.append(source_id)
            if target_id not in canonical_ids:
                unresolved.append(target_id)
            if unresolved:
                rel.setdefault('_notes', {})
                rel['_notes']['unresolved_endpoints'] = unresolved
                key = (source_id, rel.get('type'), target_id)
                if key not in unresolved_seen:
                    unresolved_seen.add(key)
                    unresolved_edges.append({
                        "source": source_id,
                        "type": rel.get('type'),
                        "target": target_id,
                        "_source": rel.get('_source')
                    })
                # optionally drop unresolved edges so the graph push doesn't choke
                if not keep_unresolved:
                    continue
            
            # Strip volatile attrs before edge ID
            attrs = rel.get('attributes') or {}
            if not isinstance(attrs, dict):
                attrs = {}
            for k in list(attrs.keys()):
                if k.startswith('Source_') or k.startswith('_') or k in {'created_at','_created_at','timestamp'}:
                    attrs.pop(k, None)
            rel['attributes'] = attrs

            # Generate edge ID for deduplication (post-rewire)
            edge_id = self.toolkit.generate_edge_id(
                rel['source'], 
                rel['type'], 
                rel['target'],
                rel.get('attributes', {})
            )
            
            # Skip duplicate edges
            if edge_id in seen_edges:
                continue
            
            seen_edges.add(edge_id)
            rel['_edge_id'] = edge_id
            
            # Filter semantically incorrect relationships for merged entities
            if not self._should_keep_relationship(rel):
                continue
                
            updated_relationships.append(rel)
            # stats
            rtype = rel.get('type') or 'UNKNOWN'
            by_type[rtype] += 1
            if rel.get('_notes', {}).get('unresolved_endpoints'):
                unresolved_by_type[rtype] += 1
            ak = len(rel.get('attributes') or {})
            if ak > 0:
                attrs_nonempty_by_type[rtype] += 1
            attrs_keys_sum_by_type[rtype] += ak
        
        # Save merged relationships
        rel_file = merged_dir / "relationships.json"
        with open(rel_file, 'w', encoding='utf-8') as f:
            json.dump({
                "count": len(updated_relationships),
                "relationships": updated_relationships,
                "_metadata": {
                    "merge_timestamp": self._get_timestamp(),
                    "duplicate_edges_removed": len(all_relationships) - len(updated_relationships),
                    "rewired_edges": rewired_edges,
                    "unresolved_edges": len(unresolved_edges),
                    "by_type": by_type,
                    "unresolved_by_type": unresolved_by_type,
                    "attributes_nonempty_by_type": attrs_nonempty_by_type,
                    "avg_attribute_keys_by_type": {
                        k: (attrs_keys_sum_by_type[k] / by_type[k]) if by_type[k] else 0.0
                        for k in by_type
                    }
                }
            }, f, indent=2, ensure_ascii=False)

        # Sidecar report with unresolved endpoints for follow-up (optional placeholder creation)
        if unresolved_edges:
            sidecar = merged_dir / "relationships_unresolved.json"
            with open(sidecar, 'w', encoding='utf-8') as f:
                json.dump({
                    "count": len(unresolved_edges),
                    "edges": unresolved_edges,
                    "_metadata": {
                        "note": "These edges reference IDs not present in the canonical entity set. Consider creating placeholders or improving extraction for these IDs."
                    }
                }, f, indent=2, ensure_ascii=False)

        log.info(f"  Saved {len(updated_relationships)} relationships "
                 f"(removed {len(all_relationships) - len(updated_relationships)} duplicates, "
                 f"rewired {rewired_edges}, unresolved {len(unresolved_edges)})")
    
    def _find_xxx_duplicates(self, entities: List[Dict], entity_type: str, 
                             id_field: str) -> Dict[str, str]:
        """
        Find entities that are duplicates except for 'xxx' suffix.
        Returns mapping of xxx_id -> canonical_id
        """
        xxx_mappings = {}
        
        # Build lookup by ID
        entities_by_id = {}
        for entity in entities:
            eid = entity.get(id_field) or entity.get('id')
            if eid:
                entities_by_id[eid] = entity
        
        # Check each entity with 'xxx' in its ID
        for entity_id, entity in entities_by_id.items():
            if 'xxx' not in entity_id.lower():
                continue
            
            # Extract base ID without xxx
            base_id = self._extract_base_id(entity_id)
            if not base_id:
                continue
            
            # Look for matching entity without xxx
            for other_id, other_entity in entities_by_id.items():
                if other_id == entity_id or 'xxx' in other_id.lower():
                    continue
                
                # Check if this could be a match
                if self._is_xxx_duplicate(entity, other_entity, entity_type, base_id, other_id):
                    xxx_mappings[entity_id] = other_id
                    log.info(f"Found XXX duplicate: {entity_id} -> {other_id}")
                    break
        
        return xxx_mappings

    def _extract_base_id(self, entity_id: str) -> str:
        """
        Extract base ID without xxx suffix.
        Examples:
            'person_smith_xxx' -> 'person_smith'
            'agenda_item_e1_xxx' -> 'agenda_item_e1'
            'document_agenda_xxx_2024' -> 'document_agenda'
        """
        import re
        
        # Remove various xxx patterns
        patterns = [
            r'_xxx\d*$',  # _xxx or _xxx123 at end
            r'_xxx_',      # _xxx_ in middle
            r'xxx\d*$',    # xxx or xxx123 at end without underscore
        ]
        
        base_id = entity_id
        for pattern in patterns:
            base_id = re.sub(pattern, '', base_id)
        
        # Also try removing hash-like suffixes (6-8 alphanumeric chars)
        base_id = re.sub(r'_[a-f0-9]{6,8}$', '', base_id)
        
        return base_id if base_id != entity_id else None

    def _is_xxx_duplicate(self, xxx_entity: Dict, other_entity: Dict, 
                          entity_type: str, xxx_base_id: str, other_id: str) -> bool:
        """
        Check if xxx_entity is a duplicate of other_entity.
        Requires at least 2 matching fields for Documents, 1 for others.
        """
        matches = 0
        
        # Special handling for Documents - need type AND date match
        if entity_type == 'Document':
            # Check document type
            xxx_type = (xxx_entity.get('document_type') or 
                       xxx_entity.get('type') or '').lower()
            other_type = (other_entity.get('document_type') or 
                         other_entity.get('type') or '').lower()
            
            if xxx_type and other_type:
                # Both must be agenda, or both ordinance, etc.
                if xxx_type == other_type:
                    matches += 1
                elif 'agenda' in xxx_type and 'agenda' in other_type:
                    matches += 1
                elif 'ordinance' in xxx_type and 'ordinance' in other_type:
                    matches += 1
                elif 'resolution' in xxx_type and 'resolution' in other_type:
                    matches += 1
                elif 'transcript' in xxx_type and 'transcript' in other_type:
                    matches += 1
            
            # Special case: Check if both are agenda documents by ID pattern
            # This handles cases where type fields might be different but IDs indicate same document
            xxx_is_agenda = ('agenda' in xxx_base_id.lower() or 
                           xxx_entity.get('documentType') == 'agenda' or
                           'agenda' in (xxx_entity.get('title') or '').lower())
            other_is_agenda = ('agenda' in other_id.lower() or
                             other_entity.get('documentType') == 'agenda' or
                             'agenda' in (other_entity.get('title') or '').lower())
            
            if xxx_is_agenda and other_is_agenda:
                matches += 1
            
            # Check date match
            xxx_date = self._extract_date_from_entity(xxx_entity)
            other_date = self._extract_date_from_entity(other_entity)
            
            if xxx_date and other_date and xxx_date == other_date:
                matches += 1
            
            # For documents, require both type AND date (2 matches)
            return matches >= 2
        
        # For AgendaItems - check item code and meeting date
        elif entity_type == 'AgendaItem':
            # Check item code
            # Prefer normalized E-code (handles code vs itemID vs agendaCode)
            xxx_code = (self._extract_e_code(xxx_entity) or xxx_entity.get('itemID') or '').lower().replace('-', '').replace('_', '')
            other_code = (self._extract_e_code(other_entity) or other_entity.get('itemID') or '').lower().replace('-', '').replace('_', '')
            
            if xxx_code and other_code and xxx_code == other_code:
                matches += 1
            
            # Check meeting date (support camel + snake and normalize)
            from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
            xxx_date = (xxx_entity.get('meetingDate') or xxx_entity.get('meeting_date') or '')
            other_date = (other_entity.get('meetingDate') or other_entity.get('meeting_date') or '')
            xxx_norm = EntityIDStandards.normalize_date_yyyymmdd(xxx_date) if xxx_date else ''
            other_norm = EntityIDStandards.normalize_date_yyyymmdd(other_date) if other_date else ''
            if xxx_norm and other_norm and xxx_norm == other_norm:
                matches += 1
            
            return matches >= 2
        
        # For Person/Organization - check name similarity
        elif entity_type in ['Person', 'Organization']:
            xxx_name = (xxx_entity.get('name', '') or '').lower().strip()
            other_name = (other_entity.get('name', '') or '').lower().strip()
            
            if xxx_name and other_name:
                # Remove common titles for comparison
                for title in ['commissioner', 'mayor', 'vice', 'mr', 'ms', 'mrs', 'dr']:
                    xxx_name = xxx_name.replace(title, '').strip()
                    other_name = other_name.replace(title, '').strip()
                
                # Check if names are similar enough
                if xxx_name == other_name:
                    return True
                
                # Check if one is substring of other (e.g., "smith" in "john smith")
                if xxx_name in other_name or other_name in xxx_name:
                    return True
        
        # For other entity types, check if base ID matches part of other ID
        else:
            # Generic check - does the base ID appear in the other ID?
            if xxx_base_id:
                xxx_base_clean = xxx_base_id.replace('_', '').lower()
                other_clean = other_id.replace('_', '').lower()
                
                if xxx_base_clean in other_clean or other_clean in xxx_base_clean:
                    # At least one other field should match
                    for field in ['name', 'title', 'type', 'status']:
                        if field in xxx_entity and field in other_entity:
                            if str(xxx_entity[field]).lower() == str(other_entity[field]).lower():
                                return True
        
        return False

    def _extract_date_from_entity(self, entity: Dict) -> Optional[str]:
        """Extract and normalize date from entity fields."""
        import re
        
        # Check various date fields
        date_fields = ['meetingDate', 'meeting_date', 'issueDate', 'dateTime', 'date', 'Date']
        
        for field in date_fields:
            if field in entity and entity[field]:
                date_str = str(entity[field])
                # Normalize to YYYYMMDD for comparison
                match = re.search(r'(\d{1,2})[._\-](\d{1,2})[._\-](\d{4})', date_str)
                if match:
                    m, d, y = match.groups()
                    return f"{y}{m.zfill(2)}{d.zfill(2)}"
                
                match = re.search(r'(\d{4})[._\-](\d{1,2})[._\-](\d{1,2})', date_str)
                if match:
                    y, m, d = match.groups()
                    return f"{y}{m.zfill(2)}{d.zfill(2)}"
        
        # Check in title/name
        text = str(entity.get('title', '')) + str(entity.get('name', ''))
        match = re.search(r'(\d{1,2})[._\-](\d{1,2})[._\-](\d{4})', text)
        if match:
            m, d, y = match.groups()
            return f"{y}{m.zfill(2)}{d.zfill(2)}"
        
        return None
    
    def _count_sources(self, entities: List[Dict]) -> Dict[str, int]:
        """Count entities by source."""
        source_counts = defaultdict(int)
        for entity in entities:
            sources = entity.get('_sources', [])
            for source in sources:
                if 'taxonomy' in source:
                    source_counts['taxonomy'] += 1
                elif 'ner' in source:
                    source_counts['ner'] += 1
                elif 'seed' in source:
                    source_counts['seed'] += 1
                else:
                    source_counts['other'] += 1
        return dict(source_counts)
    
    def _get_timestamp(self) -> str:
        """Get current timestamp."""
        from datetime import datetime
        return datetime.now().isoformat()
