"""
Response enhancer that improves GraphRAG responses with additional context and formatting.
"""

import logging
from typing import Dict, Any
import re

log = logging.getLogger(__name__)


class ResponseEnhancer:
    """Enhances GraphRAG responses with additional context and formatting."""
    
    def __init__(self):
        pass

    async def enhance_response(self, query: str, raw_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhance the raw GraphRAG response.
        
        Args:
            query: The original user query
            raw_response: Raw response from GraphRAG
            
        Returns:
            Enhanced response dictionary
        """
        log.debug("🔧 Enhancing GraphRAG response")
        
        # Start with the raw response
        enhanced = raw_response.copy()
        
        # Clean and format the answer
        if 'answer' in enhanced:
            enhanced['answer'] = self._clean_answer_text(enhanced['answer'])
        
        # Add query context
        enhanced['query'] = query
        enhanced['enhanced'] = True
        
        # Add helpful context
        enhanced['context'] = self._generate_context_hints(query)
        
        return enhanced

    def _clean_answer_text(self, answer: str) -> str:
        """Clean and format the answer text."""
        if not answer:
            return "I couldn't find a specific answer to your question."
        
        # Remove common artifacts
        cleaned = answer.strip()
        
        # Remove timestamp artifacts if present
        cleaned = re.sub(r'\[\d{4}-\d{2}-\d{2}.*?\]', '', cleaned)
        
        # Clean up extra whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Ensure proper sentence ending
        if cleaned and not cleaned.endswith(('.', '!', '?')):
            cleaned += '.'
        
        return cleaned

    def _generate_context_hints(self, query: str) -> Dict[str, Any]:
        """Generate helpful context hints based on the query."""
        hints = {
            'query_type': self._classify_query_type(query),
            'suggestions': []
        }
        
        # Add suggestions based on query type
        query_lower = query.lower()
        
        if 'agenda' in query_lower or 'item' in query_lower:
            hints['suggestions'].append("For specific agenda items, try including the item code (e.g., 'E-1', 'H-2')")
        
        if 'meeting' in query_lower:
            hints['suggestions'].append("For meeting-specific information, try including the date (MM.DD.YYYY)")
        
        if 'ordinance' in query_lower or 'resolution' in query_lower:
            hints['suggestions'].append("For legislation, try searching by document number or title")
        
        return hints

    def _classify_query_type(self, query: str) -> str:
        """Classify the type of query for context."""
        query_lower = query.lower()
        
        if any(word in query_lower for word in ['agenda', 'item']):
            return 'agenda_inquiry'
        elif any(word in query_lower for word in ['ordinance', 'resolution']):
            return 'legislation_inquiry'
        elif any(word in query_lower for word in ['meeting', 'discussion']):
            return 'meeting_inquiry'
        elif any(word in query_lower for word in ['overview', 'summary', 'trend']):
            return 'analysis_inquiry'
        else:
            return 'general_inquiry' 