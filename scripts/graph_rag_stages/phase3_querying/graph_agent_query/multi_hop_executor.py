"""
Multi-hop query executor for complex questions requiring iterative retrieval.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from openai import AzureOpenAI
from .query_classifier import QueryClassifier, QueryType
from .graph_query_generator import GraphQueryGenerator

log = logging.getLogger(__name__)


class MultiHopExecutor:
    """Executes complex queries through multiple retrieval hops."""
    
    def __init__(self, cosmos_client=None, vector_search_fn=None):
        """
        Initialize with data sources.
        
        Args:
            cosmos_client: Cosmos DB client for graph queries
            vector_search_fn: Function for vector/semantic search
        """
        self.cosmos_client = cosmos_client
        self.vector_search_fn = vector_search_fn
        self.classifier = QueryClassifier()
        self.graph_generator = GraphQueryGenerator()
        
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
    
    async def execute_multi_hop(
        self,
        original_query: str,
        max_hops: int = 4  # Changed from 3 to 4
    ) -> Dict[str, Any]:
        """
        Execute multi-hop query resolution.
        Maximum 2 graph hops and 2 vector hops.
        
        Returns:
            {
                "answer": "Combined answer",
                "hops": [hop details],
                "sources": {graph and vector sources}
            }
        """
        hops = []
        accumulated_context = []
        current_query = original_query
        
        # Track hop counts by type
        graph_hop_count = 0
        vector_hop_count = 0
        
        for hop_num in range(max_hops):
            log.info(f"Executing hop {hop_num + 1} (Graph: {graph_hop_count}/2, Vector: {vector_hop_count}/2)")
            
            # Determine which type of hop we can still do
            can_do_graph = graph_hop_count < 2
            can_do_vector = vector_hop_count < 2
            
            if not can_do_graph and not can_do_vector:
                log.info("Reached hop limits (2 graph, 2 vector)")
                break
            
            # Execute current hop with type limits
            hop_result = await self._execute_single_hop(
                current_query,
                accumulated_context,
                hop_num,
                can_do_graph,
                can_do_vector
            )
            
            # Update hop counts
            if hop_result["source_type"] == "graph":
                graph_hop_count += 1
            else:
                vector_hop_count += 1
            
            hops.append(hop_result)
            accumulated_context.append(hop_result)
            
            # Check if we have enough information
            if hop_result["is_complete"]:
                log.info(f"Query resolved in {hop_num + 1} hops")
                break
            
            # Generate next query based on gaps
            current_query = hop_result["next_query"]
            if not current_query:
                log.warning("No follow-up query generated")
                break
        
        # Synthesize final answer
        final_answer = await self._synthesize_final_answer(
            original_query,
            hops
        )
        
        return {
            "answer": final_answer,
            "hops": hops,
            "hop_count": len(hops),
            "graph_hops": graph_hop_count,
            "vector_hops": vector_hop_count,
            "sources": self._extract_all_sources(hops)
        }
    
    async def _execute_single_hop(
        self,
        query: str,
        previous_context: List[Dict],
        hop_num: int,
        can_do_graph: bool,  # Added parameter
        can_do_vector: bool  # Added parameter
    ) -> Dict[str, Any]:
        """Execute a single hop of retrieval with type limits."""
        
        # Classify the current query
        query_type, confidence, entities = self.classifier.classify(query)
        
        # Decide retrieval method based on limits and query type
        if query_type == QueryType.SPECIFIC_FACT and can_do_graph:
            # Prefer graph for specific facts
            results = await self._execute_graph_query(query, entities)
            source_type = "graph"
        elif can_do_vector:
            # Use vector if available
            results = await self._execute_vector_search(query)
            source_type = "vector"
        elif can_do_graph:
            # Fall back to graph if vector exhausted
            results = await self._execute_graph_query(query, entities)
            source_type = "graph"
        else:
            # Should not reach here due to outer loop check
            log.warning("No hop types available")
            results = []
            source_type = "none"
        
        # Analyze completeness
        analysis = await self._analyze_results(
            query,
            results,
            previous_context
        )
        
        return {
            "hop_number": hop_num + 1,
            "query": query,
            "query_type": query_type.value,
            "entities": entities,
            "source_type": source_type,
            "results": results,
            "is_complete": analysis["is_complete"],
            "missing_info": analysis["missing_info"],
            "next_query": analysis["next_query"]
        }
    
    async def _execute_graph_query(
        self,
        query: str,
        entities: List[Dict]
    ) -> List[Dict]:
        """Execute graph query and return results."""
        if not self.cosmos_client:
            return []
        
        # Generate Gremlin query
        query_result = self.graph_generator.generate_query(
            QueryType.SPECIFIC_FACT,
            entities,
            query
        )
        
        if not query_result["query"]:
            return []
        
        try:
            # Execute against Cosmos
            raw_results = await self.cosmos_client._execute_query(
                query_result["query"]
            )
            
            # Format results
            formatted = []
            for item in raw_results[:10]:  # Limit results
                formatted.append({
                    "type": "graph_vertex",
                    "data": item,
                    "query": query_result["query"]
                })
            
            return formatted
            
        except Exception as e:
            log.error(f"Graph query failed: {e}")
            return []
    
    async def _execute_vector_search(self, query: str) -> List[Dict]:
        """Execute vector search and return results."""
        if not self.vector_search_fn:
            return []
        
        try:
            # Call vector search function
            results = self.vector_search_fn(query, limit=5)
            
            # Format results
            formatted = []
            for item in results:
                formatted.append({
                    "type": "vector_chunk",
                    "text": item.get("text", ""),
                    "similarity": item.get("similarity", 0),
                    "metadata": item.get("doc", {})
                })
            
            return formatted
            
        except Exception as e:
            log.error(f"Vector search failed: {e}")
            return []
    
    async def _analyze_results(
        self,
        query: str,
        current_results: List[Dict],
        previous_context: List[Dict]
    ) -> Dict[str, Any]:
        """Analyze if results are complete and identify gaps."""
        
        prompt = self._build_analysis_prompt(
            query,
            current_results,
            previous_context
        )
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You analyze query results for completeness. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=1000
            )
            
            return json.loads(response.choices[0].message.content)
            
        except Exception as e:
            log.error(f"Result analysis failed: {e}")
            return {
                "is_complete": True,  # Stop on error
                "missing_info": "Analysis failed",
                "next_query": None
            }
    
    def _build_analysis_prompt(
        self,
        query: str,
        current_results: List[Dict],
        previous_context: List[Dict]
    ) -> str:
        """Build prompt for result analysis."""
        
        # Format current results
        current_str = self._format_results_for_prompt(current_results)
        
        # Format previous context
        context_str = ""
        for i, hop in enumerate(previous_context):
            context_str += f"\nHop {i+1}: {hop['query']}\n"
            context_str += f"Found: {len(hop.get('results', []))} results\n"
        
        return f"""Analyze if we have enough information to answer the query.

ORIGINAL QUERY: "{query}"

PREVIOUS HOPS:
{context_str if context_str else "None"}

CURRENT RESULTS:
{current_str}

Determine:
1. Do we have enough information to fully answer the original query?
2. What specific information is still missing?
3. What follow-up query would get the missing information?

Return JSON:
{{
  "is_complete": true/false,
  "missing_info": "description of what's missing",
  "next_query": "follow-up query to get missing info (or null if complete)",
  "reasoning": "brief explanation"
}}

Examples:
- If query asks for "environmental impacts of approved projects" and we only have project list, we need impacts
- If query asks for "Smith's votes on ordinances" and we have the votes, we're complete
- If query asks for "budget allocations" and we have partial data, we need the rest"""
    
    def _format_results_for_prompt(self, results: List[Dict]) -> str:
        """Format results for LLM prompt."""
        if not results:
            return "No results found"
        
        lines = []
        for i, result in enumerate(results[:5]):  # Limit for prompt
            if result["type"] == "graph_vertex":
                data = result["data"]
                if isinstance(data, dict):
                    lines.append(f"{i+1}. Graph: {data.get('label', 'vertex')} - {data.get('id', 'unknown')}")
                else:
                    lines.append(f"{i+1}. Graph result: {str(data)[:100]}")
            else:  # vector_chunk
                text_preview = result["text"][:150] + "..." if len(result["text"]) > 150 else result["text"]
                lines.append(f"{i+1}. Text: {text_preview}")
        
        return "\n".join(lines)
    
    async def _synthesize_final_answer(
        self,
        original_query: str,
        hops: List[Dict]
    ) -> str:
        """Synthesize final answer from all hops."""
        
        # Collect all results
        all_results = []
        for hop in hops:
            all_results.extend(hop.get("results", []))
        
        if not all_results:
            return "I couldn't find enough information to answer your query."
        
        # Build synthesis prompt
        prompt = f"""Synthesize a complete answer from multiple data sources.

ORIGINAL QUERY: "{original_query}"

HOP SEQUENCE:
{self._format_hop_sequence(hops)}

ALL RESULTS:
{self._format_all_results(all_results)}

Create a comprehensive answer that:
1. Directly addresses the original query
2. Combines information from all hops
3. Cites which hop provided which information
4. Is clear and well-structured

Answer:"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You synthesize information from multiple sources into clear answers."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=2000
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            log.error(f"Answer synthesis failed: {e}")
            return "Error synthesizing answer from multiple sources."
    
    def _format_hop_sequence(self, hops: List[Dict]) -> str:
        """Format hop sequence for prompt."""
        lines = []
        for hop in hops:
            lines.append(f"Hop {hop['hop_number']}: {hop['query']} ({hop['source_type']})")
            if hop['missing_info']:
                lines.append(f"  Missing: {hop['missing_info']}")
        return "\n".join(lines)
    
    def _format_all_results(self, results: List[Dict]) -> str:
        """Format all results for synthesis."""
        lines = []
        for i, result in enumerate(results[:20]):  # Limit total results
            if result["type"] == "graph_vertex":
                lines.append(f"\n[Graph Data {i+1}]")
                data = result["data"]
                if isinstance(data, dict):
                    for key, value in data.items():
                        if key not in ['embedding', '_id']:  # Skip large fields
                            lines.append(f"  {key}: {value}")
            else:
                lines.append(f"\n[Text Chunk {i+1}]")
                lines.append(f"  {result['text'][:300]}...")
        
        return "\n".join(lines)
    
    def _extract_all_sources(self, hops: List[Dict]) -> Dict[str, List]:
        """Extract all sources used across hops."""
        sources = {
            "graph_queries": [],
            "vector_chunks": [],
            "entities": []
        }
        
        for hop in hops:
            # Collect entities
            sources["entities"].extend(hop.get("entities", []))
            
            # Collect graph queries
            for result in hop.get("results", []):
                if result["type"] == "graph_vertex" and "query" in result:
                    sources["graph_queries"].append(result["query"])
                elif result["type"] == "vector_chunk":
                    sources["vector_chunks"].append({
                        "text": result["text"][:100] + "...",
                        "similarity": result["similarity"]
                    })
        
        # Deduplicate
        sources["entities"] = list({e["value"]: e for e in sources["entities"]}.values())
        sources["graph_queries"] = list(set(sources["graph_queries"]))
        
        return sources 