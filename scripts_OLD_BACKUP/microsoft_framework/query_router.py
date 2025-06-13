from enum import Enum
from typing import Dict, Any, Optional, List, Tuple
import re
import logging

logger = logging.getLogger(__name__)

class QueryIntent(Enum):
    ENTITY_SPECIFIC = "entity_specific"  # Use Local
    HOLISTIC = "holistic"               # Use Global  
    EXPLORATORY = "exploratory"         # Use DRIFT
    TEMPORAL = "temporal"               # Use DRIFT

class QueryFocus(Enum):
    SPECIFIC_ENTITY = "specific_entity"          # User wants info about ONE specific item
    MULTIPLE_SPECIFIC = "multiple_specific"      # User wants info about MULTIPLE specific items
    COMPARISON = "comparison"                    # User wants to compare entities
    CONTEXTUAL = "contextual"                    # User wants relationships/context
    GENERAL = "general"                          # No specific entity mentioned

class SmartQueryRouter:
    """Automatically route queries to the optimal search method with intelligent intent detection."""
    
    def __init__(self):
        # Entity extraction patterns
        self.entity_patterns = {
            'agenda_item': [
                r'(?:agenda\s+)?(?:item|items)\s+([A-Z]-?\d+)',
                r'(?:item|items)\s+([A-Z]-?\d+)',
                r'([A-Z]-\d+)(?:\s+agenda)?',
                r'\b([A-Z]-\d+)\b'  # Just the code itself
            ],
            'ordinance': [
                r'ordinance(?:\s+(?:number|no\.?|#))?\s*(\d{4}-\d+|\d+)',
                r'(?:city\s+)?ordinance\s+(\d{4}-\d+|\d+)',
                r'\b(\d{4}-\d+)\b(?=.*ordinance)',
                r'ordinance\s+(\w+)'
            ],
            'resolution': [
                r'resolution(?:\s+(?:number|no\.?|#))?\s*(\d{4}-\d+|\d+)',
                r'(?:city\s+)?resolution\s+(\d{4}-\d+|\d+)',
                r'\b(\d{4}-\d+)\b(?=.*resolution)',
                r'resolution\s+(\w+)'
            ]
        }
        
        # Intent indicators
        self.specific_indicators = {
            'singular_determiners': ['the', 'this', 'that', 'a', 'an'],
            'identity_verbs': ['is', 'are', 'was', 'were', 'means', 'mean', 'refers to', 'refer to', 'concerns', 'concern', 'about'],
            'detail_nouns': ['details', 'information', 'content', 'text', 'provision', 'provisions', 'summary', 'summaries', 'description', 'descriptions'],
            'specific_question_words': ['what', 'which', 'show', 'tell', 'explain', 'describe', 'list'],
            'limiting_adverbs': ['only', 'just', 'specifically', 'exactly', 'precisely', 'individually', 'separately']
        }
        
        self.comparison_indicators = {
            'comparison_verbs': ['compare', 'contrast', 'differ', 'differentiate', 'distinguish'],
            'comparison_words': ['versus', 'vs', 'against', 'compared to', 'difference', 'differences', 'similarity', 'similarities'],
            'comparison_phrases': ['how do', 'what is the difference', 'what are the differences']
        }
        
        self.contextual_indicators = {
            'plural_forms': ['items', 'ordinances', 'resolutions', 'documents'],
            'relationship_words': ['related', 'connected', 'associated', 'linked', 'relationship', 
                                  'connections', 'references', 'mentions', 'together', 'context',
                                  'affects', 'impacts', 'influences', 'between', 'among'],
            'exploration_verbs': ['explore', 'analyze', 'understand', 'investigate'],
            'scope_expanders': ['all', 'other', 'various', 'multiple', 'several', 'any']
        }
        
        # Holistic patterns for global search
        self.holistic_patterns = [
            r"what are the (?:main|top|key) (themes|topics|issues)",
            r"summarize (?:the|all) (.*)",
            r"overall (.*)",
            r"trends in (.*)",
            r"patterns across (.*)"
        ]
        
        # Temporal patterns for drift search
        self.temporal_patterns = [
            r"how has (.*) (?:changed|evolved)",
            r"timeline of (.*)",
            r"history of (.*)",
            r"development of (.*) over time",
            r"evolution of (.*)",
            r"changes in (.*)"
        ]
    
    def determine_query_method(self, query: str) -> Dict[str, Any]:
        """Determine query method with source tracking enabled."""
        query_lower = query.lower()
        
        # First check for holistic queries (global search)
        for pattern in self.holistic_patterns:
            if re.search(pattern, query_lower):
                result = {
                    "method": "global",
                    "intent": QueryIntent.HOLISTIC,
                    "params": {
                        "community_level": self._determine_community_level(query),
                        "response_type": "multiple paragraphs"
                    }
                }
                # Add source tracking to params
                result['params']['track_sources'] = True
                result['params']['include_source_metadata'] = True
                result['params']['citation_style'] = 'inline'
                return result
        
        # Check for temporal/exploratory queries (drift search)
        for pattern in self.temporal_patterns:
            if re.search(pattern, query_lower):
                result = {
                    "method": "drift",
                    "intent": QueryIntent.TEMPORAL,
                    "params": {
                        "initial_community_level": 2,
                        "max_follow_ups": 5
                    }
                }
                # Add source tracking to params
                result['params']['track_sources'] = True
                result['params']['include_source_metadata'] = True
                result['params']['citation_style'] = 'inline'
                return result
        
        # Extract ALL entity references
        all_entities = self._extract_all_entities(query)
        
        if not all_entities:
            # No specific entity found - use local search as default
            result = {
                "method": "local",
                "intent": QueryIntent.EXPLORATORY,
                "params": {
                    "top_k_entities": 10,
                    "include_community_context": True
                }
            }
            # Add source tracking to params
            result['params']['track_sources'] = True
            result['params']['include_source_metadata'] = True
            result['params']['citation_style'] = 'inline'
            return result
        
        # Handle single entity
        if len(all_entities) == 1:
            entity_info = all_entities[0]
            query_focus = self._determine_single_entity_focus(query_lower, entity_info)
            
            if query_focus == QueryFocus.SPECIFIC_ENTITY:
                result = {
                    "method": "local",
                    "intent": QueryIntent.ENTITY_SPECIFIC,
                    "params": {
                        "entity_filter": {
                            "type": entity_info['type'].upper(),
                            "value": entity_info['value']
                        },
                        "top_k_entities": 1,
                        "include_community_context": False,
                        "strict_entity_focus": True,
                        "disable_community": True
                    }
                }
            else:  # CONTEXTUAL
                result = {
                    "method": "local",
                    "intent": QueryIntent.ENTITY_SPECIFIC,
                    "params": {
                        "entity_filter": {
                            "type": entity_info['type'].upper(),
                            "value": entity_info['value']
                        },
                        "top_k_entities": 10,
                        "include_community_context": True,
                        "strict_entity_focus": False,
                        "disable_community": False
                    }
                }
            
            # Add source tracking to params
            result['params']['track_sources'] = True
            result['params']['include_source_metadata'] = True
            result['params']['citation_style'] = 'inline'
            return result
        
        # Handle multiple entities
        else:
            query_focus = self._determine_multi_entity_focus(query_lower, all_entities)
            
            if query_focus == QueryFocus.MULTIPLE_SPECIFIC:
                # User wants specific info about each entity separately
                result = {
                    "method": "local",
                    "intent": QueryIntent.ENTITY_SPECIFIC,
                    "params": {
                        "multiple_entities": all_entities,
                        "top_k_entities": 1,
                        "include_community_context": False,
                        "strict_entity_focus": True,
                        "disable_community": True,
                        "aggregate_results": True  # Combine results for each entity
                    }
                }
            elif query_focus == QueryFocus.COMPARISON:
                # User wants to compare entities
                result = {
                    "method": "local",
                    "intent": QueryIntent.ENTITY_SPECIFIC,
                    "params": {
                        "multiple_entities": all_entities,
                        "top_k_entities": 5,
                        "include_community_context": True,  # Need context for comparison
                        "strict_entity_focus": False,
                        "disable_community": False,
                        "comparison_mode": True
                    }
                }
            else:  # CONTEXTUAL
                # User wants relationships between entities
                result = {
                    "method": "local",
                    "intent": QueryIntent.ENTITY_SPECIFIC,
                    "params": {
                        "multiple_entities": all_entities,
                        "top_k_entities": 10,
                        "include_community_context": True,
                        "strict_entity_focus": False,
                        "disable_community": False,
                        "focus_on_relationships": True
                    }
                }
            
            # Add source tracking to params
            result['params']['track_sources'] = True
            result['params']['include_source_metadata'] = True
            result['params']['citation_style'] = 'inline'
            return result
    
    def _extract_all_entities(self, query: str) -> List[Dict[str, str]]:
        """Extract ALL entity references from query."""
        query_lower = query.lower()
        entities = []
        found_positions = {}  # Track positions to avoid duplicates
        
        for entity_type, patterns in self.entity_patterns.items():
            for pattern in patterns:
                for match in re.finditer(pattern, query_lower, re.IGNORECASE):
                    value = match.group(1)
                    position = match.start()
                    
                    # Normalize value
                    if entity_type == 'agenda_item':
                        value = value.upper()
                        if not '-' in value and len(value) > 1:
                            value = f"{value[0]}-{value[1:]}"
                    
                    # Check if we already found an entity at this position
                    if position not in found_positions:
                        found_positions[position] = True
                        entities.append({
                            'type': entity_type,
                            'value': value,
                            'position': position
                        })
        
        # Sort by position and remove position info
        entities = sorted(entities, key=lambda x: x['position'])
        for entity in entities:
            del entity['position']
        
        # Remove duplicates while preserving order
        seen = set()
        unique_entities = []
        for entity in entities:
            key = f"{entity['type']}:{entity['value']}"
            if key not in seen:
                seen.add(key)
                unique_entities.append(entity)
        
        return unique_entities
    
    def _determine_single_entity_focus(self, query_lower: str, entity_info: Dict) -> QueryFocus:
        """Determine focus for single entity queries."""
        specific_score = 0
        contextual_score = 0
        
        tokens = query_lower.split()
        
        # Check for limiting words
        for word in self.specific_indicators['limiting_adverbs']:
            if word in tokens:
                specific_score += 3
        
        # Check for relationship words
        for word in self.contextual_indicators['relationship_words']:
            if word in tokens:
                contextual_score += 3
        
        # Simple "what is X" patterns
        if re.match(r'^(what|whats|what\'s)\s+(is|are)\s+', query_lower):
            specific_score += 2
        
        # Very short queries tend to be specific
        if len(tokens) <= 3:
            specific_score += 2
        
        # Check for detail-seeking patterns
        for noun in self.specific_indicators['detail_nouns']:
            if noun in query_lower:
                specific_score += 1
        
        logger.info(f"Single entity focus scores - Specific: {specific_score}, Contextual: {contextual_score}")
        
        return QueryFocus.SPECIFIC_ENTITY if specific_score > contextual_score else QueryFocus.CONTEXTUAL
    
    def _determine_multi_entity_focus(self, query_lower: str, entities: List[Dict]) -> QueryFocus:
        """Determine focus for multi-entity queries."""
        tokens = query_lower.split()
        
        # Check for comparison indicators
        comparison_score = 0
        for verb in self.comparison_indicators['comparison_verbs']:
            if verb in tokens:
                comparison_score += 3
        
        for word in self.comparison_indicators['comparison_words']:
            if word in query_lower:
                comparison_score += 2
        
        for phrase in self.comparison_indicators['comparison_phrases']:
            if phrase in query_lower:
                comparison_score += 2
        
        # Check for specific information indicators
        specific_score = 0
        
        # "What are E-1 and E-2?" suggests wanting specific info
        if re.match(r'^(what|whats|what\'s)\s+(are|is)\s+', query_lower):
            specific_score += 2
        
        # Check for "separately" or "individually"
        if any(word in tokens for word in ['separately', 'individually', 'each']):
            specific_score += 3
        
        # Check for detail nouns with plural entities
        for noun in self.specific_indicators['detail_nouns']:
            if noun in query_lower:
                specific_score += 1
        
        # Check for contextual/relationship indicators
        contextual_score = 0
        for word in self.contextual_indicators['relationship_words']:
            if word in tokens:
                contextual_score += 2
        
        # Check for "and" patterns that suggest relationships
        # e.g., "relationship between E-1 and E-2"
        if re.search(r'between.*and', query_lower):
            contextual_score += 3
        
        logger.info(f"Multi-entity focus scores - Comparison: {comparison_score}, Specific: {specific_score}, Contextual: {contextual_score}")
        
        # Determine focus based on highest score
        if comparison_score >= specific_score and comparison_score >= contextual_score:
            return QueryFocus.COMPARISON
        elif specific_score > contextual_score:
            return QueryFocus.MULTIPLE_SPECIFIC
        else:
            return QueryFocus.CONTEXTUAL
    
    def _determine_community_level(self, query: str) -> int:
        """Determine optimal community level based on query scope."""
        if any(word in query.lower() for word in ["entire", "all", "overall", "whole"]):
            return 0  # Highest level
        elif any(word in query.lower() for word in ["department", "district", "area"]):
            return 1  # Mid level
        else:
            return 2  # Lower level for more specific summaries 