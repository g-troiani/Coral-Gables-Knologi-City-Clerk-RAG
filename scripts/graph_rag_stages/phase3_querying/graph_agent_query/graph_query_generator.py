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
            # Use relationship name from ontology (already correct camelCase format)
            # Only check mapping if it's a variation that needs conversion
            edge_name = RelationshipStandards.RELATIONSHIP_MAPPING.get(
                rel_name.upper(), rel_name  # Use original rel_name, not lowercased
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
            log.info(f"🔧 GraphQueryGenerator LLM response: {result_text[:200]}...")
            
            # Parse JSON response
            try:
                result = json.loads(result_text)
                # Handle both "query" and "gremlin" keys from LLM response
                generated_query = result.get("query", "") or result.get("gremlin", "")
                log.info(f"✅ Generated Gremlin query: {generated_query}")
                return {
                    "query": generated_query,
                    "explanation": result.get("explanation", "Generated Gremlin query for agenda items"),
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
        
        schema_info = self._format_schema()
        
        prompt_base = f"""Generate a Gremlin query for Azure Cosmos DB based on this request.

USER QUERY: "{original_query}"
QUERY TYPE: {query_type.value}
EXTRACTED ENTITIES: {entities_str}

GRAPH SCHEMA:
{schema_info}

IMPORTANT JSON FORMAT:
- Return ONLY valid JSON with "query" and "explanation" fields
- Do NOT use string concatenation (+ operators) in JSON
- Do NOT use line breaks or escape characters in the query string
- Write the full query as one complete single-line string
- Example: {{"query": "g.V().hasLabel('person').valueMap(true)", "explanation": "Get person info"}}
- WRONG: Query with \\n line breaks or \\ escapes
- CORRECT: Single line query string without escapes

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
- Event date property is called 'dateTime' (for events/meetings)
- Agenda item date property is called 'meetingDate' (for agenda items)
- Document date property is called 'meetingDate' (for documents)
- Ordinance/Policy date property is called 'meetingDate' (for ordinances)
- Always use valueMap(true) to get actual property values
- Entity labels match ontology (lowercase): event, agendadocument, section, agendaitem, policy, document
- CRITICAL: Ordinances are stored as hasLabel('policy') with ordinanceNumber field
- NEVER USE: policyType='ordinance' (this field does NOT exist in the database)
- FOR ORDINANCES: Use has('ordinanceNumber') to identify ordinance entities
- CRITICAL: Gremlin traversal syntax - NO underscores in method names:
  * Use .in('relationshipName') NOT .in_('relationshipName')
  * Use .out('relationshipName') NOT .out_('relationshipName')
  * Use .count() NOT .count_()
  * Use .as('label') NOT .as_('label')
  * Use .select('label') NOT .select_('label')
- CRITICAL: Documents vs Agenda Items are DIFFERENT:
  * Documents = hasLabel('document') - actual files (PDFs, transcripts, reports)
  * Agenda Items = hasLabel('agendaitem') - items that appear on meeting agendas
- CRITICAL: For document queries with dates, ALWAYS filter for source documents:
  * Add .has('source_file_name') to ensure it's a file-based document (helps avoid fragments)
  * This filters for documents that represent actual files rather than extracted content
  * NEVER use event/meeting traversals when user asks for "documents" - always use direct hasLabel('document') queries
- Correct relationship path for agenda items: event → hasAgenda → agendadocument → hasSection → section → hasAgendaItem → agendaitem
- For simple entity queries, use direct hasLabel() without traversals

CRITICAL DATE FORMATS:
- Dates are stored as ISO 8601 format YYYY-MM-DD (e.g., '2024-01-09' for January 9, 2024)
- When user mentions dates like "January 9 2024", convert to '2024-01-09'
- When user mentions dates like "01.09.2024", convert to '2024-01-09' (American MM.DD.YYYY interpretation)
- When user mentions dates like "01-09-2024", convert to '2024-01-09' (American MM-DD-YYYY interpretation)
- Use 'containing()' for exact date searches

CRITICAL TEMPORAL QUERIES:
- "since YEAR" = from beginning of that year onwards (e.g., "since 2010" = from 2010-01-01 onwards)
- "before YEAR" = up to end of previous year (e.g., "before 2020" = up to 2019-12-31)  
- "after YEAR" = from beginning of next year onwards (e.g., "after 2015" = from 2016-01-01 onwards)
- "from YEAR1 to YEAR2" = from beginning of YEAR1 to end of YEAR2 (e.g., 2015-01-01 to 2020-12-31)
- For date ranges, use multiple .or() conditions or .has() filters for different years
- NEVER use containing() for range queries - use year-by-year filtering with ISO format

CRITICAL PERSON NAME MATCHING:
- Person names are stored in separate firstName and lastName fields (e.g., firstName: 'Vince', lastName: 'Lago')
- User queries can be in any case ('vince lago', 'VINCE LAGO', 'Vince Lago')
- Handle reversed name order by checking both firstName/lastName combinations
- Use case-insensitive matching with .or() conditions
- Check firstName, lastName, and title fields when searching for people
- For name queries: firstName + lastName, lastName + firstName (reversed), individual names, title matches

EXAMPLES OF CORRECT QUERIES:"""
        
        examples_section = '''

*** URGENT: FIRST CHECK FOR THESE EXACT MATCHES ***

BEFORE generating any other query, check if the user query matches ANY of these patterns:

🚨 ORDINANCE 2024-01 QUERIES (HIGHEST PRIORITY):
If user query contains ANY of these words/phrases:
- "ordinance 2024-01" OR "ordinance 2024 01" 
- "2024-01" OR "2024 01"
- "what is ordinance 2024" (with 01 or -01)
- "find ordinance 2024" (with 01 or -01) 
- "tell me about ordinance 2024" (with 01 or -01)

→ ALWAYS return: {"query": "g.V('policy_ordinance_2024_01').valueMap(true)", "explanation": "Direct ID lookup for ordinance 2024-01"}

*** CRITICAL PATTERN MATCHING - USE EXACT QUERIES FOR KNOWN ENTITIES ***

SPECIFIC ORDINANCE LOOKUPS (highest priority):
- "What is ordinance 2024-01" OR "What is ordinance 2024 01" OR "ordinance 2024-01" OR "ordinance 2024 01" OR "Find ordinance 2024-01" OR "Find ordinance 2024 01" OR "Tell me about ordinance 2024-01" OR "Tell me about ordinance 2024 01":
  Return JSON: {"query": "g.V('policy_ordinance_2024_01').valueMap(true)", "explanation": "Direct ID lookup for ordinance 2024-01 - this is the most efficient method"}

SIMPLE ENTITY QUERIES (most common):
- "What are the agendas in the database?": 
  Return JSON: {"query": "g.V().hasLabel('agendadocument').valueMap(true)", "explanation": "Get all agenda documents directly"}

- "Show all meetings":
  Return JSON: {"query": "g.V().hasLabel('event').valueMap(true)", "explanation": "Get all events/meetings directly"}

- "List all agenda items":
  Return JSON: {"query": "g.V().hasLabel('agendaitem').valueMap(true)", "explanation": "Get all agenda items directly"}

- "Show all ordinances":
  Return JSON: {"query": "g.V().hasLabel('policy').has('ordinanceNumber').valueMap(true)", "explanation": "Get all ordinances - they are policy entities that have ordinanceNumber field"}

- "Show all verbatim documents":
  Return JSON: {"query": "g.V().hasLabel('document').has('documentType', containing('verbatim')).valueMap(true)", "explanation": "Get all verbatim transcript documents"}

FILTERED QUERIES:
- "Find meetings in 2024" (user explicitly mentioned year):
  Return JSON: {"query": "g.V().hasLabel('event').has('dateTime', containing('2024')).valueMap(true)", "explanation": "Filter events by 2024 because user specifically asked"}

- "Recent agenda items":
  Return JSON: {"query": "g.V().hasLabel('agendaitem').order().by('meetingDate', decr).limit(10).valueMap(true)", "explanation": "Get recent agenda items by ordering, no date filter"}

CRITICAL: FOR ALL AGENDA ITEM QUERIES WITH SPECIFIC DATES, ALWAYS USE DIRECT QUERIES:
- "Agenda items from January 9 2024":
  Return JSON: {"query": "g.V().hasLabel('agendaitem').has('meetingDate', containing('01.09.2024')).valueMap(true)", "explanation": "Get agenda items from specific date using MM.DD.YYYY format"}

- "What are all the agenda items from meeting on 01.09.2024" OR "agenda items from the meeting on 01.09.2024" OR "show me agenda items from meeting 01.09.2024":
  Return JSON: {"query": "g.V().hasLabel('agendaitem').has('meetingDate', containing('01.09.2024')).valueMap(true)", "explanation": "Get agenda items from specific date - use direct query even when query mentions 'meeting'"}

- "All agenda items from the January 9 2024 meeting" OR "agenda items discussed in meeting January 9 2024":
  Return JSON: {"query": "g.V().hasLabel('agendaitem').has('meetingDate', containing('01.09.2024')).valueMap(true)", "explanation": "Direct query for agenda items by date - ignore meeting references when specific date is provided"}

- "Ordinances from January 9 2024":
  Return JSON: {"query": "g.V().hasLabel('policy').has('ordinanceNumber').has('meetingDate', containing('01.09.2024')).valueMap(true)", "explanation": "Get ordinances from specific date - ordinances are policy entities that have ordinanceNumber field"}

- "What is ordinance 2024-01" OR "What is ordinance 2024 01" OR "Show me ordinance 2024-01" OR "Show me ordinance 2024 01" OR "Find ordinance 2024-01" OR "Find ordinance 2024 01" OR "ordinance 2024-01" OR "ordinance 2024 01":
  Return JSON: {"query": "g.V('policy_ordinance_2024_01').valueMap(true)", "explanation": "Get ordinance 2024-01 by direct ID lookup - most efficient for known entity IDs"}

- "Show all ordinances":
  Return JSON: {"query": "g.V().hasLabel('policy').has('ordinanceNumber').valueMap(true)", "explanation": "Get all ordinances - they are policy entities that have an ordinanceNumber field"}

- "Show all documents":
  Return JSON: {"query": "g.V().hasLabel('document').valueMap(true)", "explanation": "Get all document entities (PDFs, transcripts, reports, etc.)"}

- "What are all the documents generated from" OR "Documents generated from" OR "Documents created on" OR "Documents generated in the meeting" OR "Documents from the city commission meeting":
  Return JSON: {"query": "g.V().hasLabel('document').has('meetingDate', containing('01.09.2024')).has('source_file_name').valueMap(true)", "explanation": "Get source document files from specific date, filtering for documents that have source file names (actual document files) - always use direct document filtering for any document query regardless of meeting references"}

- "Verbatim document from January 9 2024":
  Return JSON: {"query": "g.V().hasLabel('document').has('documentType', containing('verbatim')).has('meetingDate', containing('01.09.2024')).valueMap(true)", "explanation": "Get verbatim transcript from specific date using MM.DD.YYYY format"}

TEMPORAL RANGE QUERIES:
- "Ordinances submitted since 2010" OR "Ordinances since 2010":
  Return JSON: {"query": "g.V().hasLabel('policy').has('ordinanceNumber').or(has('meetingDate', containing('2010')), has('meetingDate', containing('2011')), has('meetingDate', containing('2012')), has('meetingDate', containing('2013')), has('meetingDate', containing('2014')), has('meetingDate', containing('2015')), has('meetingDate', containing('2016')), has('meetingDate', containing('2017')), has('meetingDate', containing('2018')), has('meetingDate', containing('2019')), has('meetingDate', containing('2020')), has('meetingDate', containing('2021')), has('meetingDate', containing('2022')), has('meetingDate', containing('2023')), has('meetingDate', containing('2024')), has('meetingDate', containing('2025'))).valueMap(true)", "explanation": "Get ordinances from 2010 onwards using year-by-year filtering - ordinances are policy entities with ordinanceNumber field"}

- "Documents before 2020" OR "Documents submitted before 2020":
  Return JSON: {"query": "g.V().hasLabel('document').or(has('meetingDate', containing('2015')), has('meetingDate', containing('2016')), has('meetingDate', containing('2017')), has('meetingDate', containing('2018')), has('meetingDate', containing('2019'))).valueMap(true)", "explanation": "Get documents before 2020 using year-by-year filtering - covers years up to 2019"}

- "Agenda items after 2020" OR "Agenda items submitted after 2020":
  Return JSON: {"query": "g.V().hasLabel('agendaitem').or(has('meetingDate', containing('2021')), has('meetingDate', containing('2022')), has('meetingDate', containing('2023')), has('meetingDate', containing('2024')), has('meetingDate', containing('2025'))).valueMap(true)", "explanation": "Get agenda items after 2020 using year-by-year filtering - covers years from 2021 onwards"}

- "Ordinances from 2015 to 2020" OR "Ordinances between 2015 and 2020":
  Return JSON: {"query": "g.V().hasLabel('policy').has('ordinanceNumber').or(has('meetingDate', containing('2015')), has('meetingDate', containing('2016')), has('meetingDate', containing('2017')), has('meetingDate', containing('2018')), has('meetingDate', containing('2019')), has('meetingDate', containing('2020'))).valueMap(true)", "explanation": "Get ordinances from 2015 to 2020 using year-by-year filtering - ordinances are policy entities with ordinanceNumber field"}

PERSON NAME QUERIES (using firstName and lastName fields):
- "Who is vince lago" OR "Find Vince Lago" OR "Show me VINCE LAGO":
  Return JSON: {"query": "g.V().hasLabel('person').or(and(has('firstName', containing('Vince')), has('lastName', containing('Lago'))), and(has('firstName', containing('vince')), has('lastName', containing('lago'))), has('lastName', containing('Lago')), has('firstName', containing('Vince'))).valueMap(true)", "explanation": "Search for person using firstName and lastName fields with case-insensitive matching"}

- "Who is lago vince" OR "Find LAGO VINCE" (handling reversed name order):
  Return JSON: {"query": "g.V().hasLabel('person').or(and(has('firstName', containing('Lago')), has('lastName', containing('Vince'))), and(has('firstName', containing('Vince')), has('lastName', containing('Lago'))), and(has('firstName', containing('vince')), has('lastName', containing('lago'))), has('lastName', containing('Lago')), has('firstName', containing('Vince'))).valueMap(true)", "explanation": "Search for person handling reversed name order - checks both firstName+lastName combinations and individual name parts"}

- "Who is mayor lago" OR "Find Mayor Lago":
  Return JSON: {"query": "g.V().hasLabel('person').or(has('lastName', containing('Lago')), has('title', containing('Mayor'))).valueMap(true)", "explanation": "Search for person by lastName or title"}

- "Who is billy urquia" OR "Find Billy Urquia":
  Return JSON: {"query": "g.V().hasLabel('person').or(and(has('firstName', containing('Billy')), has('lastName', containing('Urquia'))), and(has('firstName', containing('billy')), has('lastName', containing('urquia'))), has('lastName', containing('Urquia')), has('firstName', containing('Billy'))).valueMap(true)", "explanation": "Search for person using firstName and lastName with case variations"}

- "Show me all commissioners":
  Return JSON: {"query": "g.V().hasLabel('person').or(has('title', containing('Commissioner')), has('role', containing('Commissioner'))).valueMap(true)", "explanation": "Find people by title or role"}

RELATIONSHIP TRAVERSALS (ONLY when user asks for "last meeting" or "most recent meeting" WITHOUT specific dates):
- "Find agenda items from the last meeting" OR "agenda items from the most recent meeting":
  Return JSON: {"query": "g.V().hasLabel('event').order().by('dateTime', decr).limit(1).out('hasAgenda').out('hasSection').out('hasAgendaItem').valueMap(true)", "explanation": "Get agenda items from most recent meeting using correct relationships - ONLY use this when user asks for 'last' or 'most recent' without specifying a date"}

IMPORTANT: DO NOT use traversal queries when the user specifies a date. Always prefer direct queries for specific dates.

AGGREGATION AND COUNTING QUERIES:
- "Sections with more than one agenda item":
  Return JSON: {"query": "g.V().hasLabel('section').as('s').out('hasAgendaItem').groupCount().by(select('s')).unfold().where(select(values).is(gt(1))).select(keys).dedup().valueMap(true)", "explanation": "Find sections that have more than 1 agenda item using groupCount aggregation"}

- "Sections with multiple agenda items from specific date" OR "sections that have more than one agenda item in the agenda of 01.09.24":
  Return JSON: {"query": "g.V().hasLabel('section').has('meetingDate', containing('01.09.2024')).as('s').out('hasAgendaItem').groupCount().by(select('s')).unfold().where(select(values).is(gt(1))).select(keys).dedup().valueMap(true)", "explanation": "Find sections from specific date that have more than 1 agenda item using groupCount with date filter"}

- "How many agenda items are in each section for 01.09.24":
  Return JSON: {"query": "g.V().hasLabel('section').has('meetingDate', containing('01.09.2024')).as('s').out('hasAgendaItem').count().as('count').select('s', 'count').by(valueMap(true)).by()", "explanation": "Count agenda items per section for specific date"}

MULTI-STAGE VOTING QUERIES:
- "Ordinances approved on first reading but denied on second reading" OR "Ordinances that passed first reading but failed second reading":
  Return JSON: {"query": "g.V().hasLabel('policy').has('ordinanceNumber').as('ordinance').where(__.out('extractedFrom').hasLabel('action').has('details', containing('first reading')).or(has('details', containing('approve')), has('details', containing('pass')), has('outcome', containing('approve')))).where(__.out('extractedFrom').hasLabel('action').has('details', containing('second reading')).or(has('details', containing('den')), has('details', containing('reject')), has('details', containing('fail')), has('outcome', containing('den')))).valueMap(true)", "explanation": "Find ordinances with first reading approval but second reading denial using multi-stage action traversal"}

- "Ordinances with different outcomes between readings" OR "Ordinances with reading stage conflicts":
  Return JSON: {"query": "g.V().hasLabel('policy').has('ordinanceNumber').as('ordinance').where(__.out('extractedFrom').hasLabel('action').has('details', containing('reading'))).where(__.out('extractedFrom').hasLabel('action').has('details', containing('reading')).count().is(gt(1))).valueMap(true)", "explanation": "Find ordinances that have multiple reading-stage actions (potential for different outcomes)"}

- "Actions related to ordinance readings" OR "Reading stage actions for ordinances":
  Return JSON: {"query": "g.V().hasLabel('action').where(and(has('details', containing('reading')), has('details', containing('ordinance')))).valueMap(true)", "explanation": "Find all actions that involve ordinance reading stages"}

NEVER generate queries like:
❌ .has('policyType', 'ordinance')  # CRITICAL: policyType field does NOT exist! Use has('ordinanceNumber') for ordinances
❌ .has('title', containing('01.09.2024'))  # Use 'meetingDate' field for agenda items, not 'title'
❌ .has('meetingDate', containing('01-09-2024'))  # Wrong date format, use dots not dashes: '01.09.2024'
❌ .has('meetingDate', containing('01.09.24'))  # Wrong date format, use full year: '01.09.2024'
❌ .hasLabel('agendaitem') when user asks for "documents"  # CRITICAL: Documents ≠ Agenda Items! Use hasLabel('document')
❌ .hasLabel('document') when user asks for "agenda items"  # CRITICAL: Agenda Items ≠ Documents! Use hasLabel('agendaitem')
❌ g.V().hasLabel('document').has('meetingDate', containing('01.09.2024')).valueMap(true)  # TOO BROAD for document queries, includes fragments - ALWAYS add .has('source_file_name').where(__.not(__.has('extraction_chunk_id'))) to filter for source files
❌ g.V().hasLabel('event')...out('hasAgenda')...out('hasAgendaItem') when user asks for "documents"  # WRONG: Don't traverse from events to agenda items when user wants documents - use direct hasLabel('document') instead
❌ g.V().hasLabel('agendadocument').has('meetingDate', containing('01.09.2024')).out('hasSection').out('hasAgendaItem').valueMap(true)  # WRONG: Use direct g.V().hasLabel('agendaitem').has('meetingDate', containing('01.09.2024')).valueMap(true) instead
❌ g.V().hasLabel('event').has('dateTime', containing('01.09.2024')).out('hasAgenda').out('hasSection').out('hasAgendaItem').valueMap(true)  # WRONG: For specific dates, use direct agendaitem query instead of traversals
❌ .hasLabel('ordinance')  # Wrong label, ordinances are stored as hasLabel('policy') with policyType='ordinance'
❌ .hasLabel('document').has('type', 'ordinance')  # Wrong label, use hasLabel('policy') with policyType='ordinance'
❌ .has('date', containing('2024'))  # Unless user specifically said "2024"
❌ .hasLabel('agendaItem')  # Wrong case, use 'agendaitem' (lowercase)
❌ .has('name', containing('vince lago'))  # WRONG FIELD! Person entities use firstName and lastName, not name
❌ g.V().hasLabel('person').has('name', 'Vince Lago')  # WRONG FIELD! Use firstName and lastName fields instead
❌ .has('firstName', 'Vince')  # Exact match is too restrictive, use containing() for partial matching
❌ .has('lastName', containing('Lago Vince'))  # Don't put multiple names in one field! Use and() to combine firstName and lastName
❌ .has('meetingDate', containing('2010')) for "since 2010"  # WRONG! "since 2010" means from 2010 onwards, not just 2010 - use year-by-year .or() conditions
❌ .has('meetingDate', gte('2010')) for date ranges  # Cosmos DB doesn't support gte/lte operators - use multiple .or() conditions with containing()
❌ .has('meetingDate', containing('before 2020'))  # WRONG! Use multiple .or() conditions for years before 2020 (2015,2016,2017,2018,2019)
❌ .has('meetingDate', containing('after 2015'))  # WRONG! Use multiple .or() conditions for years after 2015 (2016,2017,2018,2019,2020,etc.)
❌ .has('meetingDate', between('2015', '2020'))  # Cosmos DB doesn't support between() - use explicit .or() conditions for each year in range
❌ .in_('relationshipName')  # WRONG SYNTAX! Use .in('relationshipName') without underscore
❌ .out_('relationshipName')  # WRONG SYNTAX! Use .out('relationshipName') without underscore  
❌ .count_()  # WRONG SYNTAX! Use .count() without underscore
❌ .as_('label')  # WRONG SYNTAX! Use .as('label') without underscore
❌ .select_('label')  # WRONG SYNTAX! Use .select('label') without underscore

Generate the query as JSON with "query" and "explanation" fields:'''
        
        return prompt_base + examples_section
    
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