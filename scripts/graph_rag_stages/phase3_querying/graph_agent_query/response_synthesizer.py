"""
Synthesizes natural language responses from graph and vector search results.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from openai import AzureOpenAI
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology

log = logging.getLogger(__name__)


class ResponseSynthesizer:
    """Synthesizes coherent responses from multiple data sources."""
    
    def __init__(self):
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
    
    async def synthesize_response(
        self,
        query: str,
        graph_results: List[Dict[str, Any]] = None,
        vector_results: List[Dict[str, Any]] = None,
        query_context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Synthesize natural language response from multiple sources.
        
        Args:
            query: Original user query
            graph_results: Results from graph database queries
            vector_results: Results from vector/semantic search
            query_context: Additional context (entities, query type, etc.)
            
        Returns:
            {
                "answer": "Natural language response",
                "citations": [{"source": "graph", "id": "...", "text": "..."}],
                "confidence": 0.0-1.0
            }
        """
        
        # ADD DEBUGGING HERE
        log.info("="*80)
        log.info("🔍 RESPONSE SYNTHESIZER DEBUG")
        log.info("="*80)
        log.info(f"Query: {query}")
        log.info(f"Graph results count: {len(graph_results) if graph_results else 0}")
        
        # Log first few graph results for inspection
        if graph_results:
            log.info("Sample graph results:")
            for i, result in enumerate(graph_results[:3]):
                log.info(f"  Result {i+1}: {json.dumps(result, default=str)[:500]}")
        else:
            log.info("No graph results provided")
        
        log.info(f"Vector results count: {len(vector_results) if vector_results else 0}")
        log.info("-"*80)
        
        # Format results for synthesis
        formatted_graph = self._format_graph_results(graph_results or [])
        formatted_vector = self._format_vector_results(vector_results or [])
        
        # LOG FORMATTED RESULTS
        log.info("Formatted graph results:")
        for item in formatted_graph[:3]:
            log.info(f"  {item}")
        
        # DEBUG: Log the actual synthesis prompt being sent to LLM
        synthesis_prompt = self._build_synthesis_prompt(query, formatted_graph, formatted_vector, query_context)
        log.info("SYNTHESIS PROMPT:")
        log.info("=" * 50)
        log.info(synthesis_prompt[:1000] + "..." if len(synthesis_prompt) > 1000 else synthesis_prompt)
        log.info("=" * 50)
        log.info("-"*80)
        
        # Build synthesis prompt
        prompt = self._build_synthesis_prompt(
            query,
            formatted_graph,
            formatted_vector,
            query_context
        )
        
        # Generate response
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You synthesize information from city government data sources into clear, accurate answers."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=2000
            )
            
            answer = response.choices[0].message.content
            
            # Extract citations
            citations = self._extract_citations(
                answer,
                formatted_graph,
                formatted_vector
            )
            
            # Calculate confidence
            confidence = self._calculate_confidence(
                graph_results,
                vector_results,
                answer
            )
            
            return {
                "answer": answer,
                "citations": citations,
                "confidence": confidence,
                "sources_used": {
                    "graph": len(graph_results or []),
                    "vector": len(vector_results or [])
                }
            }
            
        except Exception as e:
            log.error(f"Response synthesis failed: {e}")
            return {
                "answer": "I encountered an error while processing your request.",
                "citations": [],
                "confidence": 0.0,
                "error": str(e)
            }
    
    def _format_graph_results(self, graph_results: List[Dict]) -> List[Dict]:
        """Convert graph vertices/edges to readable format."""
        formatted = []
        
        log.info(f"Formatting {len(graph_results)} graph results")
        
        # CRITICAL: Handle Cosmos DB nested list structure
        # Cosmos DB returns [[{dict}, {dict}]] but we need [{dict}, {dict}]
        flattened_results = []
        for result in graph_results:
            if isinstance(result, list):
                # Flatten nested lists from Cosmos DB
                flattened_results.extend(result)
            elif isinstance(result, dict):
                # Single dict
                flattened_results.append(result)
        
        log.info(f"Flattened to {len(flattened_results)} individual results")
        
        # SPECIAL HANDLING: Group document entities by sourceFileName for document queries
        # This prevents multiple extracted entities from the same file appearing as separate documents
        if flattened_results and all(isinstance(item, dict) and item.get('label') == 'document' for item in flattened_results):
            log.info("Detected document query - grouping entities by sourceFileName")
            grouped_documents = {}
            
            for result in flattened_results:
                source_file = result.get('sourceFileName', ['Unknown'])[0] if isinstance(result.get('sourceFileName'), list) else result.get('sourceFileName', 'Unknown')
                
                if source_file not in grouped_documents:
                    # Use the first entity for each source file as the representative
                    grouped_documents[source_file] = result
                    
            # Replace flattened_results with grouped documents
            flattened_results = list(grouped_documents.values())
            log.info(f"Grouped to {len(flattened_results)} unique source documents")
        
        # Determine appropriate limit based on query context
        # For agenda items, show more results since users expect complete lists
        limit = 50 if any(item.get('label') == 'agendaitem' for item in flattened_results if isinstance(item, dict)) else 10
        
        for i, result in enumerate(flattened_results[:limit]):
            formatted_item = {
                "id": f"G{i+1}",
                "type": "graph",
                "content": ""
            }
            
            # CRITICAL: Handle Cosmos DB result format
            # Results from valueMap(true) have properties as arrays
            if isinstance(result, dict):
                # Extract the actual values from Cosmos DB format
                # Properties come as arrays in valueMap(true) results
                
                # Get the label
                label = result.get("label")
                if isinstance(label, list) and label:
                    label = label[0]
                
                # Get the ID
                item_id = result.get("id")
                if isinstance(item_id, list) and item_id:
                    item_id = item_id[0]
                
                # Handle event/meeting formatting
                if label == "event":
                    # Extract event properties (they come as arrays)
                    date_time = result.get("dateTime", [""])[0] if isinstance(result.get("dateTime"), list) else result.get("dateTime", "")
                    event_type = result.get("type", [""])[0] if isinstance(result.get("type"), list) else result.get("type", "")
                    source_file = result.get("Source_File_Name", [""])[0] if isinstance(result.get("Source_File_Name"), list) else result.get("Source_File_Name", "")
                    
                    formatted_item["content"] = f"Meeting on {date_time} ({event_type})"
                    formatted_item["label"] = "event"
                    formatted_item["properties"] = f"Date: {date_time}, Type: {event_type}, Source: {source_file}"
                    
                    log.info(f"  Formatted event: {date_time} - {event_type}")
                
                # Handle agenda item formatting
                elif label == "agendaitem":
                    # Extract properties (they come as arrays) - try multiple field names for item code
                    code = ""
                    for code_field in ["code", "itemID", "agendaItemID"]:
                        field_value = result.get(code_field, [""])
                        if isinstance(field_value, list) and field_value and field_value[0]:
                            code = field_value[0]
                            break
                        elif field_value:
                            code = field_value
                            break
                    
                    title = result.get("title", [""])[0] if isinstance(result.get("title"), list) else result.get("title", "")
                    meeting_date = result.get("meetingDate", [""])[0] if isinstance(result.get("meetingDate"), list) else result.get("meetingDate", "")
                    
                    formatted_item["content"] = f"Agenda Item {code}: {title}"
                    formatted_item["label"] = "agendaitem"
                    formatted_item["properties"] = f"Date: {meeting_date}, Code: {code}"
                    
                    log.info(f"  Formatted agenda item: {code} - {title}")
                
                # Handle policy/ordinance formatting
                elif label == "policy":
                    # Extract properties (they come as arrays)
                    title = result.get("title", [""])[0] if isinstance(result.get("title"), list) else result.get("title", "")
                    policy_type = result.get("policyType", [""])[0] if isinstance(result.get("policyType"), list) else result.get("policyType", "")
                    ordinance_number = result.get("ordinanceNumber", [""])[0] if isinstance(result.get("ordinanceNumber"), list) else result.get("ordinanceNumber", "")
                    resolution_number = result.get("resolutionNumber", [""])[0] if isinstance(result.get("resolutionNumber"), list) else result.get("resolutionNumber", "")
                    meeting_date = result.get("meetingDate", [""])[0] if isinstance(result.get("meetingDate"), list) else result.get("meetingDate", "")
                    status = result.get("status", [""])[0] if isinstance(result.get("status"), list) else result.get("status", "")
                    description = result.get("description", [""])[0] if isinstance(result.get("description"), list) else result.get("description", "")
                    
                    # Improve policy type detection and formatting
                    if not policy_type and ordinance_number:
                        policy_type = "ordinance"
                    elif not policy_type and resolution_number:
                        policy_type = "resolution"
                    elif not policy_type:
                        policy_type = "policy"
                    
                    # Use title as description if description is missing but title exists
                    if not description and title:
                        description = title
                    
                    # CONSISTENT FORMATTING: Always use the same format for ordinances and resolutions
                    if ordinance_number:
                        display_title = f"Ordinance {ordinance_number}"
                    elif resolution_number:
                        display_title = f"Resolution {resolution_number}"
                    else:
                        display_title = title or "Policy"
                    
                    # Format content with consistent structure - always include colon for ordinances/resolutions with description
                    if description and (ordinance_number or resolution_number):
                        formatted_item["content"] = f"{display_title}: {description}"
                    elif ordinance_number or resolution_number:
                        formatted_item["content"] = f"{display_title}"
                    elif description:
                        formatted_item["content"] = f"{display_title}: {description}"
                    else:
                        formatted_item["content"] = display_title
                        
                    formatted_item["label"] = "policy"
                    formatted_item["properties"] = f"Date: {meeting_date}, Status: {status}, Type: {policy_type}"
                    
                    log.info(f"  Formatted policy: {policy_type} {display_title}")
                
                # Handle new entity types with specific formatting
                elif label == "section":
                    name = result.get("name", [""])[0] if isinstance(result.get("name"), list) else result.get("name", "")
                    meeting_date = result.get("meetingDate", [""])[0] if isinstance(result.get("meetingDate"), list) else result.get("meetingDate", "")
                    formatted_item["content"] = f"Section: {name}"
                    formatted_item["label"] = "section"
                    formatted_item["properties"] = f"Date: {meeting_date}, Name: {name}"
                
                elif label == "document":
                    title = result.get("title", [""])[0] if isinstance(result.get("title"), list) else result.get("title", "")
                    source_file = result.get("sourceFileName", [""])[0] if isinstance(result.get("sourceFileName"), list) else result.get("sourceFileName", "")
                    meeting_date = result.get("meetingDate", [""])[0] if isinstance(result.get("meetingDate"), list) else result.get("meetingDate", "")
                    
                    # Format as source document file
                    if source_file:
                        formatted_item["content"] = f"Document: {source_file}"
                        formatted_item["properties"] = f"Source File: {source_file}; Date: {meeting_date}"
                    else:
                        formatted_item["content"] = f"Document: {title}"
                        formatted_item["properties"] = f"Title: {title}; Date: {meeting_date}"
                    formatted_item["label"] = "document"
                
                elif label == "agendadocument":
                    title = result.get("title", [""])[0] if isinstance(result.get("title"), list) else result.get("title", "")
                    meeting_date = result.get("meetingDate", [""])[0] if isinstance(result.get("meetingDate"), list) else result.get("meetingDate", "")
                    formatted_item["content"] = f"Agenda Document: {title}"
                    formatted_item["label"] = "agendadocument"
                    formatted_item["properties"] = f"Date: {meeting_date}, Title: {title}"
                
                elif label == "board":
                    name = result.get("name", [""])[0] if isinstance(result.get("name"), list) else result.get("name", "")
                    board_type = result.get("type", [""])[0] if isinstance(result.get("type"), list) else result.get("type", "")
                    formatted_item["content"] = f"Board: {name}"
                    formatted_item["label"] = "board"
                    formatted_item["properties"] = f"Name: {name}, Type: {board_type}"
                
                elif label == "appointment":
                    appointee = result.get("appointeeName", [""])[0] if isinstance(result.get("appointeeName"), list) else result.get("appointeeName", "")
                    board_name = result.get("boardName", [""])[0] if isinstance(result.get("boardName"), list) else result.get("boardName", "")
                    formatted_item["content"] = f"Appointment: {appointee} to {board_name}"
                    formatted_item["label"] = "appointment"
                    formatted_item["properties"] = f"Appointee: {appointee}, Board: {board_name}"
                
                elif label == "presentation":
                    title = result.get("title", [""])[0] if isinstance(result.get("title"), list) else result.get("title", "")
                    presenter = result.get("presenter", [""])[0] if isinstance(result.get("presenter"), list) else result.get("presenter", "")
                    formatted_item["content"] = f"Presentation: {title}"
                    formatted_item["label"] = "presentation"
                    formatted_item["properties"] = f"Title: {title}, Presenter: {presenter}"
                
                elif label == "publiccomment":
                    speaker = result.get("speaker", [""])[0] if isinstance(result.get("speaker"), list) else result.get("speaker", "")
                    topic = result.get("topic", [""])[0] if isinstance(result.get("topic"), list) else result.get("topic", "")
                    formatted_item["content"] = f"Public Comment by {speaker}"
                    formatted_item["label"] = "publiccomment"
                    formatted_item["properties"] = f"Speaker: {speaker}, Topic: {topic}"
                
                else:
                    # Enhanced generic formatting using unified ontology
                    # Get entity type definition for better formatting
                    entity_type_title = label.title()
                    entity_config = UnifiedOntology.ENTITY_TYPES.get(entity_type_title, {})
                    
                    # Try to extract name/title for content
                    content_value = item_id
                    for name_field in ['name', 'title', 'code']:
                        field_value = result.get(name_field)
                        if isinstance(field_value, list) and field_value:
                            content_value = field_value[0]
                            break
                        elif field_value:
                            content_value = field_value
                            break
                    
                    formatted_item["content"] = f"{entity_type_title}: {content_value}"
                    formatted_item["label"] = label
                    
                    # Extract key properties, prioritizing important ones
                    important_props = ['name', 'title', 'type', 'date', 'status', 'dateTime', 'meetingDate']
                    props = []
                    
                    # First add important properties
                    for prop in important_props:
                        if prop in result:
                            value = result[prop]
                            if isinstance(value, list) and value:
                                value = value[0]
                            if value:
                                props.append(f"{prop}: {value}")
                    
                    # Then add other properties up to limit
                    for key, value in result.items():
                        if key not in ["id", "label", "_id", "partitionKey"] + important_props:
                            if isinstance(value, list) and value:
                                value = value[0]
                            if value and len(props) < 5:
                                props.append(f"{key}: {value}")
                    
                    if props:
                        formatted_item["properties"] = "; ".join(props[:5])
            
            formatted.append(formatted_item)
        
        log.info(f"Formatted {len(formatted)} items")
        return formatted
    
    def _format_vector_results(self, vector_results: List[Dict]) -> List[Dict]:
        """Format vector search results."""
        formatted = []
        
        for i, result in enumerate(vector_results[:10]):  # Limit results
            text = result.get("text", "")
            similarity = result.get("similarity", 0)
            metadata = result.get("metadata", {})
            
            formatted_item = {
                "id": f"V{i+1}",
                "type": "vector",
                "content": text[:500] + "..." if len(text) > 500 else text,
                "similarity": similarity,
                "source": metadata.get("title", "Unknown Document"),
                "date": metadata.get("date", "Unknown Date")
            }
            
            formatted.append(formatted_item)
        
        return formatted
    
    def _build_synthesis_prompt(
        self,
        query: str,
        graph_data: List[Dict],
        vector_data: List[Dict],
        context: Optional[Dict]
    ) -> str:
        """Build prompt for response synthesis."""
        
        # Format graph data section
        graph_section = "GRAPH DATABASE RESULTS:\n"
        if graph_data:
            for item in graph_data:
                graph_section += f"\n[{item['id']}] {item['content']}"
                if "properties" in item:
                    graph_section += f"\n     Properties: {item['properties']}"
                graph_section += "\n"
        else:
            graph_section += "No graph results found.\n"
        
        # Format vector data section  
        vector_section = "\nTEXT SEARCH RESULTS:\n"
        if vector_data:
            for item in vector_data:
                vector_section += f"\n[{item['id']}] From '{item['source']}' ({item['similarity']}% match):\n"
                vector_section += f"     {item['content']}\n"
        else:
            vector_section += "No text results found.\n"
        
        # Detect if this is an agenda items query to adjust formatting
        is_agenda_query = "agenda item" in query.lower() or any("agendaitem" in str(item.get("label", "")) for item in graph_data if isinstance(item, dict))
        
        # Build examples based on query type
        example1 = "A-1: • [Item description] [Citation]" if is_agenda_query else "• [Item 1] [Citation]"
        example2 = "E-4: • [Item description] [Citation]" if is_agenda_query else "• [Item 2] [Citation]"
        example3 = "F-10: • [Item description] [Citation]" if is_agenda_query else "• [Item 3] [Citation]"
        
        # Add special instructions for consistent formatting
        special_instruction = ""
        if is_agenda_query:
            special_instruction = "\n8. CRITICAL FOR AGENDA ITEMS: Extract the item code from entries like 'Agenda Item A-1: [title]' and format bullet points as '[CODE]: • [description] [Citation]' (e.g., 'A-2: • Recognition of Coral Gables Fire Department Rescue 4 Crew [G1]')"
        
        # Add general formatting consistency instruction
        consistency_instruction = """\n9. CONSISTENT ATTRIBUTE FORMATTING: For all entity types, use consistent formatting patterns:
   - For Ordinances: Always use "Ordinance [NUMBER]:" format (e.g., "Ordinance 2024-02:" or "Ordinance 2551:")  
   - For Resolutions: Always use "Resolution [NUMBER]:" format
   - For Policies: Always use "Policy [IDENTIFIER]:" format if identifier exists
   - For Appointments: Always use "Appointment of [NAME] to [BOARD]:" format
   - For all entities: If presenting lists, ensure the same primary attribute is shown for each item of the same type"""
        
        # Build complete prompt
        prompt = f"""Synthesize a comprehensive answer from the following data sources.

USER QUERY: "{query}"

{graph_section}

{vector_section}

INSTRUCTIONS:
1. Structure your answer with an EXECUTIVE SUMMARY at the top, followed by detailed bullet points
2. Use this exact format:

## Executive Summary
[Brief 2-3 sentence overview of the key findings]

## Detailed Breakdown
### [Category 1 Name]
{example1}
{example2}
{example3}

### [Category 2 Name]  
• [Item 1] [Citation]
• [Item 2] [Citation]

3. Use inline citations like [G1] for graph data or [V1] for text data after each bullet point
4. Group related items into logical categories (e.g., "Awards & Recognition", "Appointments", "Ordinances", "Updates", etc.)
5. Be specific with names, dates, and numbers from the data
6. Keep bullet points concise but informative
7. If data sources contradict, mention both perspectives{special_instruction}{consistency_instruction}

Write the synthesized answer using the structured format above:"""
        
        return prompt
    
    def _extract_citations(
        self,
        answer: str,
        graph_data: List[Dict],
        vector_data: List[Dict]
    ) -> List[Dict[str, Any]]:
        """Extract and format citations from the answer."""
        import re
        
        citations = []
        
        # Find all citation markers in the answer
        citation_pattern = r'\[([GV]\d+)\]'
        found_citations = re.findall(citation_pattern, answer)
        
        for citation_id in set(found_citations):  # Unique citations only
            citation_type = citation_id[0]  # 'G' or 'V'
            citation_num = int(citation_id[1:]) - 1  # Convert to 0-based index
            
            if citation_type == 'G' and citation_num < len(graph_data):
                item = graph_data[citation_num]
                citations.append({
                    "id": citation_id,
                    "source": "graph",
                    "type": item.get("label", "unknown"),
                    "text": item["content"],
                    "properties": item.get("properties", "")
                })
                
            elif citation_type == 'V' and citation_num < len(vector_data):
                item = vector_data[citation_num]
                citations.append({
                    "id": citation_id,
                    "source": "vector",
                    "document": item["source"],
                    "text": item["content"][:200] + "...",
                    "similarity": item["similarity"]
                })
        
        return citations
    
    def _calculate_confidence(
        self,
        graph_results: List[Dict],
        vector_results: List[Dict],
        answer: str
    ) -> float:
        """Calculate confidence score for the response."""
        
        confidence = 0.5  # Base confidence
        
        # Boost for having both types of results
        if graph_results and vector_results:
            confidence += 0.2
        
        # Boost for high similarity vector results
        if vector_results:
            avg_similarity = sum(r.get("similarity", 0) for r in vector_results) / len(vector_results)
            confidence += (avg_similarity / 100) * 0.2  # Up to 0.2 boost
        
        # Boost for specific graph results
        if graph_results:
            confidence += min(len(graph_results) * 0.05, 0.2)  # Up to 0.2 boost
        
        # Check if answer has citations
        import re
        citations = re.findall(r'\[[GV]\d+\]', answer)
        if citations:
            confidence += 0.1
        
        return min(confidence, 1.0)  # Cap at 1.0 