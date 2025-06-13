"""
Query router that determines the best query method based on query characteristics.
"""

import logging
from typing import Dict, Any
import re

log = logging.getLogger(__name__)


class QueryRouter:
    """Routes queries to the most appropriate GraphRAG method."""
    
    def __init__(self):
        # Keywords that suggest global vs local queries
        self.global_keywords = [
            'overview', 'summary', 'trend', 'pattern', 'analysis', 'report',
            'all', 'overall', 'general', 'comprehensive', 'throughout',
            'across', 'between', 'comparison', 'statistics', 'data'
        ]
        
        self.local_keywords = [
            'specific', 'particular', 'exactly', 'precisely', 'detail',
            'what is', 'who is', 'when', 'where', 'how', 'item',
            'agenda item', 'ordinance', 'resolution', 'meeting'
        ]

    def determine_query_method(self, query_text: str) -> Dict[str, Any]:
        """
        Determine the best query method based on query characteristics.
        
        Args:
            query_text: The user's query
            
        Returns:
            Dictionary with routing information
        """
        query_lower = query_text.lower()
        
        # Score for global vs local
        global_score = 0
        local_score = 0
        
        # Check for global keywords
        for keyword in self.global_keywords:
            if keyword in query_lower:
                global_score += 1
        
        # Check for local keywords
        for keyword in self.local_keywords:
            if keyword in query_lower:
                local_score += 1
        
        # Check for specific patterns
        if self._has_specific_entity_reference(query_lower):
            local_score += 2
        
        if self._has_broad_analysis_terms(query_lower):
            global_score += 2
        
        # Determine method
        if global_score > local_score:
            method = 'global'
            confidence = min(global_score / (global_score + local_score + 1), 0.9)
            intent = 'broad_analysis'
        elif local_score > global_score:
            method = 'local'
            confidence = min(local_score / (global_score + local_score + 1), 0.9)
            intent = 'specific_lookup'
        else:
            # Default to local for balanced or unclear queries
            method = 'local'
            confidence = 0.5
            intent = 'balanced'
        
        return {
            'method': method,
            'confidence': confidence,
            'intent': intent,
            'global_score': global_score,
            'local_score': local_score,
            'reasoning': self._generate_reasoning(method, global_score, local_score)
        }

    def _has_specific_entity_reference(self, query_lower: str) -> bool:
        """Check if query references specific entities."""
        patterns = [
            r'item [a-z]-\d+',  # Agenda items like "item e-1"
            r'ordinance \d+',   # Ordinance numbers
            r'resolution \d+',  # Resolution numbers
            r'\d{2}\.\d{2}\.\d{4}',  # Specific dates
        ]
        
        for pattern in patterns:
            if re.search(pattern, query_lower):
                return True
        
        return False

    def _has_broad_analysis_terms(self, query_lower: str) -> bool:
        """Check if query contains terms suggesting broad analysis."""
        broad_terms = [
            'trends', 'patterns', 'analysis', 'overview', 'summary',
            'all meetings', 'all items', 'general', 'overall'
        ]
        
        for term in broad_terms:
            if term in query_lower:
                return True
        
        return False

    def _generate_reasoning(self, method: str, global_score: int, local_score: int) -> str:
        """Generate human-readable reasoning for the routing decision."""
        if method == 'global':
            return f"Routed to global search (score: {global_score}) - query appears to need broad analysis across multiple documents"
        elif method == 'local':
            if local_score > global_score:
                return f"Routed to local search (score: {local_score}) - query appears to seek specific information"
            else:
                return "Routed to local search (default) - query intent unclear, using targeted search"
        else:
            return "Default routing applied" 