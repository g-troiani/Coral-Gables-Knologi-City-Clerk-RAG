"""
Handles ambiguous queries by presenting clarification options to users.
"""

import logging
from typing import Dict, List, Any, Optional

log = logging.getLogger(__name__)


class DisambiguationHandler:
    """Handles unclear queries by asking for clarification."""
    
    def generate_clarification(
        self,
        query: str,
        entities: List[Dict[str, str]],
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate clarification request for ambiguous query.
        
        Args:
            query: Original ambiguous query
            entities: List of entities found in query
            context: Optional additional context
            
        Returns:
            {
                "needs_clarification": True,
                "message": "Clarification message",
                "options": [{"id": 1, "text": "...", "entity": {...}}],
                "original_query": "..."
            }
        """
        
        # Group entities by type
        entities_by_type = {}
        for entity in entities:
            entity_type = entity.get("type", "Unknown")
            if entity_type not in entities_by_type:
                entities_by_type[entity_type] = []
            entities_by_type[entity_type].append(entity)
        
        # Find potential matches for ambiguous entities
        clarification_needed = []
        
        for entity_type, type_entities in entities_by_type.items():
            for entity in type_entities:
                # Check if entity needs clarification
                if self._is_ambiguous(entity):
                    matches = self._find_possible_matches(entity, context)
                    if matches:
                        clarification_needed.append({
                            "entity": entity,
                            "matches": matches
                        })
        
        # Generate response
        if clarification_needed:
            return self._format_clarification_response(
                query,
                clarification_needed
            )
        else:
            # No clarification needed but query still unclear
            return self._format_general_clarification(query, entities)
    
    def _is_ambiguous(self, entity: Dict[str, str]) -> bool:
        """Check if entity reference is ambiguous."""
        value = entity.get("value", "").lower()
        
        # Common ambiguous patterns
        ambiguous_terms = [
            "that", "this", "it", "them", "those",
            "the thing", "stuff", "one", "other"
        ]
        
        # Check for very short names (likely incomplete)
        if entity["type"] == "Person" and len(value.split()) == 1:
            return True
        
        # Check for ambiguous terms
        for term in ambiguous_terms:
            if term in value:
                return True
        
        return False
    
    def _find_possible_matches(
        self,
        entity: Dict[str, str],
        context: Optional[Dict[str, Any]]
    ) -> List[Dict[str, str]]:
        """Find possible matches for ambiguous entity."""
        
        # In real implementation, this would query the database
        # For now, return mock examples based on entity type
        
        entity_type = entity["type"]
        value = entity["value"]
        
        if entity_type == "Person" and "smith" in value.lower():
            return [
                {"name": "Commissioner John Smith", "title": "City Commissioner", "id": "person_smith_john"},
                {"name": "Attorney Sarah Smith", "title": "City Attorney", "id": "person_smith_sarah"},
                {"name": "Director Mike Smith", "title": "Planning Director", "id": "person_smith_mike"}
            ]
        
        elif entity_type == "Document":
            if "ordinance" in value.lower():
                return [
                    {"title": "Ordinance 2024-01: Parking Regulations", "date": "2024-01-09", "id": "doc_ord_2024_01"},
                    {"title": "Ordinance 2024-02: Zoning Amendment", "date": "2024-01-23", "id": "doc_ord_2024_02"},
                    {"title": "Ordinance 2023-15: Budget Allocation", "date": "2023-12-15", "id": "doc_ord_2023_15"}
                ]
        
        elif entity_type == "Topic" and any(term in value.lower() for term in ["thing", "it", "that"]):
            return [
                {"topic": "Parking Policy Discussion", "date": "2024-01-09", "id": "topic_parking_policy"},
                {"topic": "Budget Amendment Proposal", "date": "2024-01-09", "id": "topic_budget_amendment"},
                {"topic": "Zoning Variance Request", "date": "2024-01-09", "id": "topic_zoning_variance"}
            ]
        
        return []
    
    def _format_clarification_response(
        self,
        query: str,
        clarification_needed: List[Dict]
    ) -> Dict[str, Any]:
        """Format clarification response with numbered options."""
        
        message_parts = ["I need more information to answer your query."]
        all_options = []
        option_id = 1
        
        for item in clarification_needed:
            entity = item["entity"]
            matches = item["matches"]
            
            message_parts.append(f"\nWhen you mentioned '{entity['value']}', did you mean:")
            
            for match in matches:
                option_text = self._format_match_text(entity["type"], match)
                all_options.append({
                    "id": option_id,
                    "text": option_text,
                    "entity_type": entity["type"],
                    "entity_data": match
                })
                message_parts.append(f"{option_id}. {option_text}")
                option_id += 1
        
        message_parts.append("\nPlease select the number of the item you're interested in.")
        
        return {
            "needs_clarification": True,
            "message": "\n".join(message_parts),
            "options": all_options,
            "original_query": query,
            "clarification_type": "entity_disambiguation"
        }
    
    def _format_match_text(self, entity_type: str, match: Dict) -> str:
        """Format a match option for display."""
        
        if entity_type == "Person":
            return f"{match['name']} - {match['title']}"
        
        elif entity_type == "Document":
            return f"{match['title']} (Date: {match['date']})"
        
        elif entity_type == "Topic":
            return f"{match['topic']} (Discussed: {match['date']})"
        
        else:
            # Generic format
            return str(match.get('name') or match.get('title') or match)
    
    def _format_general_clarification(
        self,
        query: str,
        entities: List[Dict]
    ) -> Dict[str, Any]:
        """Format general clarification for unclear queries."""
        
        message = "I'm having trouble understanding your query. Could you please be more specific?\n\n"
        
        if entities:
            message += "I detected these elements in your query:\n"
            for entity in entities:
                message += f"- {entity['type']}: {entity['value']}\n"
            message += "\n"
        
        message += "You might try:\n"
        message += "- Using full names (e.g., 'Commissioner John Smith' instead of 'Smith')\n"
        message += "- Specifying document numbers (e.g., 'Ordinance 2024-01')\n"
        message += "- Including dates or time periods\n"
        message += "- Being more specific about what information you need"
        
        return {
            "needs_clarification": True,
            "message": message,
            "options": [],
            "original_query": query,
            "clarification_type": "general_guidance"
        }
    
    def process_user_selection(
        self,
        selection: int,
        clarification_response: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process user's selection from clarification options.
        
        Args:
            selection: Option number selected by user
            clarification_response: Previous clarification response
            
        Returns:
            {
                "resolved": True/False,
                "selected_entity": {...},
                "refined_query": "..."
            }
        """
        
        options = clarification_response.get("options", [])
        
        # Find selected option
        selected_option = None
        for option in options:
            if option["id"] == selection:
                selected_option = option
                break
        
        if not selected_option:
            return {
                "resolved": False,
                "error": f"Invalid selection: {selection}"
            }
        
        # Build refined query
        original_query = clarification_response.get("original_query", "")
        entity_data = selected_option["entity_data"]
        
        # Replace ambiguous reference with specific one
        refined_query = self._refine_query(
            original_query,
            selected_option["entity_type"],
            entity_data
        )
        
        return {
            "resolved": True,
            "selected_entity": entity_data,
            "entity_type": selected_option["entity_type"],
            "refined_query": refined_query
        }
    
    def _refine_query(
        self,
        original_query: str,
        entity_type: str,
        entity_data: Dict
    ) -> str:
        """Refine query with specific entity information."""
        
        # Simple refinement - in production this would be more sophisticated
        if entity_type == "Person":
            specific_ref = entity_data.get("name", "")
        elif entity_type == "Document":
            specific_ref = entity_data.get("title", "").split(":")[0]  # Just the doc number
        else:
            specific_ref = entity_data.get("name") or entity_data.get("title", "")
        
        # Replace common ambiguous terms
        refined = original_query
        ambiguous_terms = ["that", "this", "the thing", "it"]
        
        for term in ambiguous_terms:
            if term in refined.lower():
                refined = refined.lower().replace(term, specific_ref.lower())
                break
        
        return refined 