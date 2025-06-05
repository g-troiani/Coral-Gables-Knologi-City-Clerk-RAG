from enum import Enum
from typing import Dict, Any, Optional
import re

class QueryIntent(Enum):
    ENTITY_SPECIFIC = "entity_specific"  # Use Local
    HOLISTIC = "holistic"               # Use Global  
    EXPLORATORY = "exploratory"         # Use DRIFT
    TEMPORAL = "temporal"               # Use DRIFT

class SmartQueryRouter:
    """Automatically route queries to the optimal search method."""
    
    def __init__(self):
        # Intent patterns
        self.entity_patterns = [
            r"who is (\w+)",
            r"what (?:is|are) (?:the )?(.*?) (?:of|for|in)",
            r"tell me about (.*?)",
            r"(?:ordinance|resolution|agenda item) (\S+)"
        ]
        
        self.holistic_patterns = [
            r"what are the (?:main|top|key) (themes|topics|issues)",
            r"summarize (?:the|all) (.*)",
            r"overall (.*)",
            r"trends in (.*)",
            r"patterns across (.*)"
        ]
        
        self.temporal_patterns = [
            r"how has (.*) (?:changed|evolved)",
            r"timeline of (.*)",
            r"history of (.*)",
            r"development of (.*) over time"
        ]
    
    def determine_query_method(self, query: str) -> Dict[str, Any]:
        """Determine the best query method for a given question."""
        query_lower = query.lower()
        
        # Check for entity-specific queries
        for pattern in self.entity_patterns:
            if re.search(pattern, query_lower):
                return {
                    "method": "local",
                    "intent": QueryIntent.ENTITY_SPECIFIC,
                    "params": {
                        "top_k_entities": 10,
                        "include_community_context": True
                    }
                }
        
        # Check for holistic queries
        for pattern in self.holistic_patterns:
            if re.search(pattern, query_lower):
                return {
                    "method": "global",
                    "intent": QueryIntent.HOLISTIC,
                    "params": {
                        "community_level": self._determine_community_level(query),
                        "response_type": "multiple paragraphs"
                    }
                }
        
        # Check for temporal/exploratory queries
        for pattern in self.temporal_patterns:
            if re.search(pattern, query_lower):
                return {
                    "method": "drift",
                    "intent": QueryIntent.TEMPORAL,
                    "params": {
                        "initial_community_level": 2,
                        "max_follow_ups": 5
                    }
                }
        
        # Default to DRIFT for complex queries
        return {
            "method": "drift",
            "intent": QueryIntent.EXPLORATORY,
            "params": {
                "initial_community_level": 1,
                "max_follow_ups": 3
            }
        }
    
    def _determine_community_level(self, query: str) -> int:
        """Determine optimal community level based on query scope."""
        if any(word in query.lower() for word in ["entire", "all", "overall", "whole"]):
            return 0  # Highest level
        elif any(word in query.lower() for word in ["department", "district", "area"]):
            return 1  # Mid level
        else:
            return 2  # Lower level for more specific summaries 