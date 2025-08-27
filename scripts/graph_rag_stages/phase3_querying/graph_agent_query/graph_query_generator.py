"""
Generates Gremlin queries based on classified query intent and entities.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional
from openai import AzureOpenAI
from .query_classifier import QueryType
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
from scripts.graph_rag_stages.common.relationship_standards import RelationshipStandards


log = logging.getLogger(__name__)


class GraphQueryGenerator:
    """Generates Gremlin queries for Cosmos DB based on query classification."""
    
    # Dynamic schema generated from unified ontology
    
    def __init__(self):
        """Initialize the query generator."""
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
        
        # Generate dynamic schema from unified ontology
        self.GRAPH_SCHEMA = self._build_dynamic_schema()
    
    def _build_dynamic_schema(self) -> Dict[str, Any]:
        """Build graph schema dynamically from UnifiedOntology."""
        vertices = {}
        edges = {}
        
        # Build vertices from entity types
        for entity_type, entity_config in UnifiedOntology.ENTITY_TYPES.items():
            # Convert PascalCase to lowercase for Cosmos DB labels
            label = entity_type.lower()
            
            # Extract properties and description from the entity config
            properties = []
            description = f"{entity_type} entities in city governance"
            
            if isinstance(entity_config, dict):
                # Get properties from attributes list
                properties = entity_config.get('attributes', [])
                description = entity_config.get('definition', description)
            
            # Fallback to essential properties if none found
            if not properties:
                properties = self._get_default_properties(entity_type)
            
            vertices[label] = {
                "properties": properties,
                "description": description
            }
        
        # Build edges from relationship definitions
        for rel_name, rel_config in UnifiedOntology.RELATIONSHIP_DEFINITIONS.items():
            # Convert relationship name for compatibility
            edge_name = RelationshipStandards.RELATIONSHIP_MAPPING.get(
                rel_name.upper(), rel_name.lower()
            )
            if edge_name is None:  # Skip internal-only relationships
                continue
                
            source_types = rel_config.get('source', [])
            target_types = rel_config.get('target', [])
            
            # Handle both single strings and lists
            if isinstance(source_types, str):
                source_types = [source_types]
            if isinstance(target_types, str):
                target_types = [target_types]
            
            # Create edge for each source-target combination
            for source in source_types:
                for target in target_types:
                    edges[edge_name] = {
                        "from": source.lower(),
                        "to": target.lower()
                    }
        
        return {"vertices": vertices, "edges": edges}
    
    def _get_default_properties(self, entity_type: str) -> List[str]:
        """Get default properties for entity types."""
        defaults = {
            'Person': ['name', 'title', 'affiliation'],
            'Organization': ['name', 'type', 'jurisdiction'], 
            'Document': ['title', 'document_type', 'meeting_date'],
            'Event': ['name', 'dateTime', 'status'],
            'Action': ['type', 'outcome', 'date'],
            'Policy': ['title', 'status', 'effectiveDate'],
            'AgendaItem': ['code', 'title', 'meeting_date'],
            'Location': ['address', 'district'],
            'Asset': ['name', 'value', 'type'],
            'Project': ['name', 'status', 'budget']
        }
        return defaults.get(entity_type, ['name', 'type', 'id'])
    
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
  {{"query": "g.V().hasLabel('event').has('type', 'Regular Meeting').order().by('dateTime', decr).limit(1).out('HAS_AGENDA').out('HAS_SECTION').out('HAS_AGENDA_ITEM').valueMap(true)", "explanation": "Get agenda items from most recent meeting, no date filter"}}

- "Show all meetings":
  {{"query": "g.V().hasLabel('event').has('type', 'Regular Meeting').valueMap(true)", "explanation": "Get all meetings, no date filter"}}

- "What are the dates of all meetings":
  {{"query": "g.V().hasLabel('event').has('type', 'Regular Meeting').order().by('dateTime', decr).valueMap(true)", "explanation": "Get all meetings with dates, no date filter"}}

- "Find meetings in 2024" (user explicitly mentioned year):
  {{"query": "g.V().hasLabel('event').has('type', 'Regular Meeting').has('dateTime', containing('2024')).valueMap(true)", "explanation": "Filter by 2024 because user specifically asked"}}

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