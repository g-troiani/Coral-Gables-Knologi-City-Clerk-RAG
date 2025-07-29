"""
Synthesizes natural language responses from graph and vector search results.
"""

import os
import logging
from typing import Dict, List, Any, Optional, Tuple
from openai import AzureOpenAI

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
        
        # Format results for synthesis
        formatted_graph = self._format_graph_results(graph_results or [])
        formatted_vector = self._format_vector_results(vector_results or [])
        
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
        
        for i, result in enumerate(graph_results[:10]):  # Limit to prevent prompt overflow
            formatted_item = {
                "id": f"G{i+1}",
                "type": "graph",
                "content": ""
            }
            
            if isinstance(result, dict):
                # Handle vertex data
                if "label" in result:
                    formatted_item["label"] = result["label"]
                    
                    # Extract key information based on vertex type
                    if result["label"] == "person":
                        name = result.get("name", "Unknown")
                        title = result.get("title", "")
                        formatted_item["content"] = f"Person: {name} ({title})"
                        
                    elif result["label"] == "document":
                        title = result.get("title", "Untitled")
                        doc_type = result.get("document_type", "document")
                        doc_num = result.get("document_number", "")
                        formatted_item["content"] = f"{doc_type.title()}: {doc_num} - {title}"
                        
                    elif result["label"] == "policy":
                        title = result.get("title", "Untitled Policy")
                        status = result.get("status", "unknown")
                        formatted_item["content"] = f"Policy: {title} (Status: {status})"
                        
                    else:
                        # Generic format
                        formatted_item["content"] = f"{result['label']}: {result.get('id', 'unknown')}"
                    
                    # Add all properties
                    props = []
                    for key, value in result.items():
                        if key not in ["id", "label", "_id", "partitionKey", "embedding"]:
                            props.append(f"{key}: {value}")
                    
                    if props:
                        formatted_item["properties"] = "; ".join(props[:5])  # Limit properties
                
                # Handle edge data
                elif "source" in result and "target" in result:
                    rel_type = result.get("type", "related_to")
                    formatted_item["content"] = f"Relationship: {result['source']} -{rel_type}-> {result['target']}"
            
            formatted.append(formatted_item)
        
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
        
        # Build complete prompt
        prompt = f"""Synthesize a comprehensive answer from the following data sources.

USER QUERY: "{query}"

{graph_section}

{vector_section}

INSTRUCTIONS:
1. Provide a clear, direct answer to the user's query
2. Combine information from both graph and text sources
3. Use inline citations like [G1] for graph data or [V1] for text data
4. Be specific with names, dates, and numbers from the data
5. If data sources contradict, mention both perspectives
6. Structure the answer with paragraphs for readability

Write the synthesized answer:"""
        
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