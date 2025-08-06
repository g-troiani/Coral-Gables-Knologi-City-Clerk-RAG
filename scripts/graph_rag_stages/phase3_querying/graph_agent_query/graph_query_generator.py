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
    """Generates Gremlin queries for Cosmos DB based on query classification."""
    
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
                "properties": ["date", "type", "doc_id", "Source_File_Name", "name", "location"],
                "description": "City commission meetings"
            },
            "agendaitem": {
                "properties": ["code", "title", "type", "document_reference", "meeting_date"],
                "description": "Specific agenda items like E-1, F-10"
            },
            "voteoutcome": {
                "properties": ["outcome", "voteCount", "dateTime", "motion_title"],
                "description": "Results of voting on agenda items"
            },
            "action": {
                "properties": ["type", "title", "status", "date"],
                "description": "Actions taken (approvals, deferrals, etc.)"
            },
            "asset": {
                "properties": ["name", "value", "type", "status"],
                "description": "Financial assets, budgets, funds"
            },
            "project": {
                "properties": ["name", "status", "budget", "timeline"],
                "description": "City projects and initiatives"
            },
            "location": {
                "properties": ["address", "district", "coordinates"],
                "description": "Physical locations and districts"
            },
            "role": {
                "properties": ["title", "responsibilities", "department"],
                "description": "Positions and roles in government"
            },
            "topic": {
                "properties": ["name", "category", "priority"],
                "description": "Subjects and themes discussed"
            }
        },
        "edges": {
            "sponsors": {"from": "person", "to": "document"},
            "votes_on": {"from": "person", "to": "voteoutcome"},
            "discusses": {"from": "meeting", "to": "topic"},
            "has_agenda": {"from": "meeting", "to": "document"},
            "has_section": {"from": "document", "to": "agendaitem"},
            "has_agenda_item": {"from": "meeting", "to": "agendaitem"},
            "creates": {"from": "person", "to": "policy"},
            "implements": {"from": "organization", "to": "policy"},
            "relates_to": {"from": "document", "to": "topic"}
        }
    }
    
    def __init__(self):
        """Initialize the query generator."""
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
    ) -> Dict[str, str]:
        """
        Generate a Gremlin query based on classification and entities.
        
        Returns:
            {"query": "gremlin query", "explanation": "what it does", "expected_output": "format"}
        """
        
        try:
            prompt = self._build_query_prompt(query_type, entities, original_query, constraints)
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert at generating Gremlin queries for Azure Cosmos DB. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=1000
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Parse JSON response
            try:
                result = json.loads(result_text)
                return {
                    "query": result.get("query", ""),
                    "explanation": result.get("explanation", ""),
                    "expected_output": result.get("expected_output", "list")
                }
            except json.JSONDecodeError as e:
                log.error(f"Failed to parse JSON response: {e}")
                log.error(f"Response text: {result_text}")
                return {
                    "query": "",
                    "explanation": f"JSON parsing failed: {str(e)}",
                    "expected_output": "error"
                }
                
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
        """Build prompt for LLM to generate Gremlin query."""
        
        entities_str = json.dumps(entities, indent=2)
        
        return f"""Generate a Gremlin query for Azure Cosmos DB based on this request.

USER QUERY: "{original_query}"
QUERY TYPE: {query_type.value}
EXTRACTED ENTITIES: {entities_str}

CRITICAL RULES - NEVER HARDCODE DATES:
1. NEVER assume a specific year unless explicitly mentioned in the query
2. "last meeting" = order by date descending, take first (NO date filter)
3. "recent" = order by date descending, limit to reasonable number (NO date filter)
4. "all" = no date filter at all
5. Only filter by date if the user explicitly mentions a date/year
6. The system contains data from 2015-2025 - work across all years

COSMOS DB SYNTAX:
- Use 'decr' for descending order (not 'desc')
- Use 'incr' for ascending order (not 'asc')
- Meeting date property is called 'date' (for meetings)
- Agenda item date property is called 'meeting_date' (for agenda items)
- Agenda item label is 'agendaItem' (case sensitive)
- Meeting label is 'meeting' (lowercase)
- Always use valueMap(true) to get actual property values
- Graph path for agenda items: meeting → HAS_AGENDA → HAS_SECTION → HAS_AGENDA_ITEM → agendaItem

CRITICAL DATE FORMATS:
- Dates are stored as MM-DD-YYYY format (e.g., '01-09-2024' for January 9, 2024)
- When user mentions dates like "January 9 2024", convert to '01-09-2024'
- When user mentions dates like "2024-01-09", convert to '01-09-2024'
- Use 'containing()' for date searches, not exact matches

EXAMPLES OF CORRECT QUERIES:
- "Find agenda items from the last meeting":
  {{"query": "g.V().hasLabel('meeting').order().by('date', decr).limit(1).out('HAS_AGENDA').out('HAS_SECTION').out('HAS_AGENDA_ITEM').valueMap(true)", "explanation": "Get agenda items from most recent meeting, no date filter"}}

- "Show all meetings":
  {{"query": "g.V().hasLabel('meeting').valueMap(true)", "explanation": "Get all meetings, no date filter"}}

- "What are the dates of all meetings":
  {{"query": "g.V().hasLabel('meeting').order().by('date', decr).valueMap(true)", "explanation": "Get all meetings with dates, no date filter"}}

- "Find meetings in 2024" (user explicitly mentioned year):
  {{"query": "g.V().hasLabel('meeting').has('date', containing('2024')).valueMap(true)", "explanation": "Filter by 2024 because user specifically asked"}}

- "Recent agenda items":
  {{"query": "g.V().hasLabel('agendaItem').order().by('meeting_date', decr).limit(10).valueMap(true)", "explanation": "Get recent agenda items by ordering, no date filter"}}

- "Agenda items from January 9 2024":
  {{"query": "g.V().hasLabel('agendaItem').has('meeting_date', containing('01-09-2024')).valueMap(true)", "explanation": "Get agenda items from specific date using MM-DD-YYYY format"}}

NEVER generate queries like:
❌ .has('date', gte('2023-01-01')).has('date', lte('2023-12-31'))  # Don't assume year
❌ .has('date', containing('2024'))  # Unless user specifically said "2024"
❌ .has('meeting_date', containing('2024-01-09'))  # Wrong date format, use '01-09-2024'

Generate the query:"""
    
    def _format_schema(self) -> str:
        """Format the schema for the prompt."""
        lines = ["VERTICES (with properties):"]
        
        for label, info in self.GRAPH_SCHEMA["vertices"].items():
            props = ", ".join(info["properties"])
            lines.append(f"- {label}: {props}")
            lines.append(f"  {info['description']}")
        
        lines.append("\nEDGES:")
        for edge, info in self.GRAPH_SCHEMA["edges"].items():
            lines.append(f"- {edge}: {info['from']} -> {info['to']}")
        
        return "\n".join(lines)