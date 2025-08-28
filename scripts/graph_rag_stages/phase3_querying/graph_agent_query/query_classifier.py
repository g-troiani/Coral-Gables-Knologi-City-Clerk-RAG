"""
Query classifier that uses LLM to classify query types and extract entities.
"""

import os
import json
import logging
from enum import Enum
from typing import Dict, List, Tuple, Any
from openai import AzureOpenAI
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology

log = logging.getLogger(__name__)


class QueryType(Enum):
    SPECIFIC_FACT = "specific_fact"
    GENERAL_INFO = "general_info"
    COMPLEX_HYBRID = "complex_hybrid"
    UNCLEAR = "unclear"


class QueryClassifier:
    """Classifies queries and extracts entities using LLM."""
    
    # Dynamic entity types from unified ontology
    ENTITY_TYPES = list(UnifiedOntology.ENTITY_TYPES.keys())
    
    # Document subtypes for better recognition
    DOCUMENT_SUBTYPES = {
        "ordinance": "legislative document",
        "resolution": "legislative document", 
        "agenda": "meeting document",
        "minutes": "meeting record",
        "verbatim": "transcript document",
        "verbatim_transcript": "transcript document",
        "transcript": "transcript document",
        "report": "administrative document",
        "memo": "administrative document",
        "proclamation": "ceremonial document",
        "contract": "legal document",
        "agreement": "legal document"
    }
    
    def __init__(self):
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
    
    def classify(self, query: str) -> Tuple[QueryType, float, List[Dict[str, str]]]:
        """
        Classify query and extract entities.
        
        Returns:
            (query_type, confidence, entities)
            where entities = [{"type": "Person", "value": "Commissioner Smith", "normalized": "smith"}]
        """
        prompt = self._build_classification_prompt(query)
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You classify queries and extract entities. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=1000
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # Parse classification
            query_type = QueryType(result.get("query_type", "unclear"))
            confidence = float(result.get("confidence", 0.5))
            
            # Parse entities
            entities = []
            for entity in result.get("entities", []):
                entity_dict = {
                    "type": entity["type"],
                    "value": entity["value"],
                    "normalized": entity.get("normalized", entity["value"].lower())
                }
                # Add subtype for documents
                if entity["type"] == "Document" and "subtype" in entity:
                    entity_dict["subtype"] = entity["subtype"]
                entities.append(entity_dict)
            
            return query_type, confidence, entities
            
        except Exception as e:
            log.error(f"Classification failed: {e}")
            return QueryType.UNCLEAR, 0.0, []
    
    def _build_classification_prompt(self, query: str) -> str:
        """Build prompt for classification and entity extraction."""
        
        return f"""Analyze this query and classify it into one of these categories:

QUERY TYPES:
- specific_fact: Asking for specific information (counts, particular items, exact details)
- general_info: Open-ended questions about topics, policies, or procedures
- complex_hybrid: Multi-part questions requiring both specific lookups and general context
- unclear: Ambiguous queries needing clarification

ENTITY TYPES TO EXTRACT:
- Person: Officials, commissioners, mayors, staff members
- Organization: Departments, committees, boards, companies
- Document: ANY mention of documents including:
  * Ordinances (legislative documents)
  * Resolutions (legislative documents)
  * Agendas (meeting planning documents)
  * Minutes (meeting record documents)
  * Verbatim transcripts (word-for-word meeting records)
  * Reports, memos, proclamations
  * Any document type reference
- Policy: Specific policies, rules, or regulations (but NOT ordinances/resolutions - those are Documents)
- Event: General events, hearings, workshops (use Meeting for specific city meetings)
- Meeting: Formal city government meetings (City Council, committee meetings)
- Action: Votes, approvals, motions
- Asset: Funds, budgets, monetary amounts
- Project: Initiatives, developments, programs
- Location: Addresses, buildings, districts
- Role: Positions, titles, functions
- Topic: Subjects, issues, themes
- Section: Agenda sections (like "Consent Agenda", "Regular Business")
- AgendaItem: Items like E-1, F-10, agenda references
- AgendaDocument: Complete agenda documents for meetings
- Contract: Agreements, RFPs, bids
- Technology: Systems, software, platforms
- VoteOutcome: Voting results, decisions
- Presentation: Presentations given during meetings
- PublicComment: Comments from members of the public
- Board: Boards and committees (when referring to the body itself)
- Appointment: Appointments to boards, positions, or roles
- LegalReference: Citations to laws, codes, or regulations

IMPORTANT RECOGNITION RULES:
- "ordinances" → Document (subtype: ordinance)
- "resolutions" → Document (subtype: resolution)
- "agenda" or "agendas" → Document (subtype: agenda) or AgendaDocument
- "minutes" → Document (subtype: minutes)
- "verbatim" or "transcript" or "verbatim transcript" → Document (subtype: verbatim)
- "report" → Document (subtype: report)
- "consent agenda", "regular business" → Section
- "city council meeting", "committee meeting" → Meeting
- "presentation" → Presentation
- "public comment" → PublicComment
- "board", "committee" (as entity) → Board
- "appointment" → Appointment
- "section", "chapter", "code" (legal) → LegalReference
- Any plural or singular form should be recognized

Query: "{query}"

Return JSON:
{{
  "query_type": "specific_fact|general_info|complex_hybrid|unclear",
  "confidence": 0.0-1.0,
  "entities": [
    {{
      "type": "Entity type from list above",
      "value": "Exact text from query",
      "normalized": "normalized form (lowercase, no titles)",
      "subtype": "for Document entities only - the document subtype"
    }}
  ],
  "reasoning": "Brief explanation of classification"
}}

Examples:
- "How many ordinances were passed in January 2024?" 
  → specific_fact, entities: [{{"type": "Document", "value": "ordinances", "subtype": "ordinance"}}, {{"type": "Event", "value": "January 2024"}}]

- "Show me all resolutions about parking"
  → specific_fact, entities: [{{"type": "Document", "value": "resolutions", "subtype": "resolution"}}, {{"type": "Topic", "value": "parking"}}]

- "What was discussed in the verbatim transcript?"
  → general_info, entities: [{{"type": "Document", "value": "verbatim transcript", "subtype": "verbatim"}}]

- "Find agenda items from the last meeting"
  → specific_fact, entities: [{{"type": "AgendaItem", "value": "agenda items"}}, {{"type": "Meeting", "value": "last meeting"}}]

- "Search the meeting agendas for budget discussions"
  → specific_fact, entities: [{{"type": "Document", "value": "meeting agendas", "subtype": "agenda"}}, {{"type": "Topic", "value": "budget discussions"}}]

- "Show me the consent agenda section from yesterday's meeting"
  → specific_fact, entities: [{{"type": "Section", "value": "consent agenda section"}}, {{"type": "Meeting", "value": "yesterday's meeting"}}]

- "What presentations were given during the city council meeting?"
  → specific_fact, entities: [{{"type": "Presentation", "value": "presentations"}}, {{"type": "Meeting", "value": "city council meeting"}}]

- "List all board appointments made this year"
  → specific_fact, entities: [{{"type": "Appointment", "value": "board appointments"}}, {{"type": "Board", "value": "board"}}]

- "Find public comments about traffic"
  → specific_fact, entities: [{{"type": "PublicComment", "value": "public comments"}}, {{"type": "Topic", "value": "traffic"}}]

- "What does Section 5.1 of the City Charter say?"
  → general_info, entities: [{{"type": "LegalReference", "value": "Section 5.1 of the City Charter"}}]""" 