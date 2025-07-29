"""
Generates Gremlin queries based on classified query intent and entities.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional
from openai import AzureOpenAI
from .query_classifier import QueryType

log = logging.getLogger(__name__)


class GraphQueryGenerator:
    """Generates Gremlin queries using LLM with ontology context."""
    
    # Graph schema from the ontology
    GRAPH_SCHEMA = {
        "vertices": {
            "person": {
                "properties": ["name", "title", "affiliation", "contactInfo"],
                "description": "Individuals in government (mayors, commissioners, staff)"
            },
            "organization": {
                "properties": ["name", "type", "jurisdiction", "address"],
                "description": "Departments, boards, committees, companies"
            },
            "document": {
                "properties": ["title", "document_type", "Source_File_Name", "meeting_date", "document_number"],
                "description": "Ordinances, resolutions, agendas, minutes, transcripts"
            },
            "policy": {
                "properties": ["title", "status", "effectiveDate", "policyID", "document_number"],
                "description": "Formal rules, laws, regulations"
            },
            "event": {
                "properties": ["name", "type", "dateTime", "status", "outcome"],
                "description": "Meetings, hearings, workshops"
            },
            "meeting": {
                "properties": ["date", "type", "doc_id"],
                "description": "City commission meetings"
            },
            "agendaitem": {
                "properties": ["code", "title", "type", "document_reference", "meeting_date"],
                "description": "Specific agenda items like E-1, F-10"
            },
            "voteoutcome": {
                "properties": ["status", "yesVotes", "noVotes", "agendaItemID"],
                "description": "Voting results on agenda items"
            },
            "location": {
                "properties": ["name", "address", "type"],
                "description": "Physical locations, addresses"
            },
            "asset": {
                "properties": ["name", "value", "currency", "fiscalYear"],
                "description": "Financial resources, funds"
            }
        },
        "edges": {
            "isMemberOf": "Person → Organization",
            "sponsors": "Person → Policy/Document",
            "authoredBy": "Document → Person/Organization",
            "votedOn": "Document/Policy → Meeting",
            "discusses": "Event → AgendaItem",
            "resultsIn": "AgendaItem → VoteOutcome",
            "HAS_AGENDA": "Meeting → Document",
            "HAS_SECTION": "Document → Section",
            "HAS_AGENDA_ITEM": "Section → AgendaItem",
            "IMPLEMENTS": "AgendaItem → Document/Policy",
            "occursAt": "Event → Location",
            "performsAction": "Person → Action"
        }
    }
    
    def __init__(self):
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
    
    def generate_query(
        self, 
        query_type: QueryType,
        entities: List[Dict[str, str]],
        original_query: str,
        constraints: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate Gremlin query based on classified intent.
        
        Returns:
            {
                "query": "g.V().hasLabel('person')...",
                "explanation": "This query finds...",
                "expected_output": "count or list of vertices"
            }
        """
        prompt = self._build_query_prompt(query_type, entities, original_query, constraints)
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You generate Gremlin queries for Cosmos DB. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=1000
            )
            
            result = json.loads(response.choices[0].message.content)
            return result
            
        except Exception as e:
            log.error(f"Query generation failed: {e}")
            return {
                "query": "",
                "explanation": f"Failed to generate query: {str(e)}",
                "expected_output": "error"
            }
    
    def _build_query_prompt(
        self,
        query_type: QueryType,
        entities: List[Dict[str, str]],
        original_query: str,
        constraints: Optional[Dict[str, Any]] = None
    ) -> str:
        """Build prompt with full ontology context."""
        
        # Format entities for prompt
        entity_str = "\n".join([
            f"- {e['type']}: '{e['value']}' (normalized: '{e['normalized']}')"
            + (f", subtype: {e['subtype']}" if 'subtype' in e else "")
            for e in entities
        ])
        
        # Build schema description
        schema_str = self._format_schema()
        
        return f"""Generate a Gremlin query for this user question using the graph schema below.

GRAPH SCHEMA:
{schema_str}

USER QUERY: "{original_query}"
QUERY TYPE: {query_type.value}
EXTRACTED ENTITIES:
{entity_str}

GREMLIN QUERY PATTERNS:

For COUNTS:
- g.V().hasLabel('document').has('document_type', 'ordinance').count()
- g.V().hasLabel('person').out('sponsors').hasLabel('document').count()

For SPECIFIC LOOKUPS:
- g.V().hasLabel('person').has('name', containing('Smith')).valueMap(true)
- g.V().hasLabel('document').has('document_number', '2024-01').valueMap(true)

For RELATIONSHIPS:
- g.V().hasLabel('person').has('name', containing('Smith')).out('sponsors').valueMap(true)
- g.V().hasLabel('meeting').has('date', '2024-01-09').out('HAS_AGENDA').out('HAS_SECTION').out('HAS_AGENDA_ITEM').valueMap(true)

For DATE RANGES:
- g.V().hasLabel('document').has('meeting_date', gte('2024-01-01')).has('meeting_date', lte('2024-12-31'))

IMPORTANT RULES:
1. Use lowercase labels (person, document, agendaitem, not Person, Document, AgendaItem)
2. For partial name matches use: containing('name')
3. For exact matches use: has('property', 'value')
4. Always use valueMap(true) to get properties
5. For documents, check document_type property for ordinance/resolution/agenda
6. Dates are stored as strings in 'YYYY-MM-DD' format

Return JSON:
{{
  "query": "the complete Gremlin query",
  "explanation": "what this query does",
  "expected_output": "count|list|single_vertex|graph_path"
}}

Examples:
- "How many ordinances in 2024?" → 
  {{"query": "g.V().hasLabel('document').has('document_type', 'ordinance').has('meeting_date', gte('2024-01-01')).has('meeting_date', lte('2024-12-31')).count()", "expected_output": "count"}}

- "Show Commissioner Smith's sponsored ordinances" →
  {{"query": "g.V().hasLabel('person').has('name', containing('Smith')).has('title', containing('Commissioner')).out('sponsors').hasLabel('document').has('document_type', 'ordinance').valueMap(true)", "expected_output": "list"}}"""
    
    def _format_schema(self) -> str:
        """Format the schema for the prompt."""
        lines = ["VERTICES (with properties):"]
        
        for label, info in self.GRAPH_SCHEMA["vertices"].items():
            props = ", ".join(info["properties"])
            lines.append(f"- {label}: {info['description']}")
            lines.append(f"  Properties: {props}")
        
        lines.append("\nEDGE RELATIONSHIPS:")
        for edge, desc in self.GRAPH_SCHEMA["edges"].items():
            lines.append(f"- {edge}: {desc}")
        
        return "\n".join(lines)
    
    def validate_query(self, gremlin_query: str) -> bool:
        """Basic validation of generated query."""
        if not gremlin_query:
            return False
        
        # Check for basic Gremlin structure
        if not gremlin_query.startswith("g."):
            return False
        
        # Check for dangerous operations
        dangerous_ops = ["drop()", "property(", "addV(", "addE(", "remove("]
        for op in dangerous_ops:
            if op in gremlin_query:
                log.warning(f"Dangerous operation detected in query: {op}")
                return False
        
        return True 