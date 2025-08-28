"""
Main orchestrator for agent-based query planning and execution.
"""

import json
import logging
from typing import Dict, Any, Optional, Callable
from .query_classifier import QueryClassifier, QueryType
from .graph_query_generator import GraphQueryGenerator
from .multi_hop_executor import MultiHopExecutor
from .disambiguation_handler import DisambiguationHandler
from .response_synthesizer import ResponseSynthesizer

log = logging.getLogger(__name__)


class AgentQueryPlanner:
    """Orchestrates query processing through appropriate execution paths."""
    
    def __init__(
        self,
        cosmos_client=None,
        vector_search_fn: Optional[Callable] = None
    ):
        """
        Initialize with data sources.
        
        Args:
            cosmos_client: CosmosGraphClient instance
            vector_search_fn: Function for vector search (semantic_search)
        """
        self.cosmos_client = cosmos_client
        self.vector_search_fn = vector_search_fn
        
        # Initialize components
        self.classifier = QueryClassifier()
        self.graph_generator = GraphQueryGenerator()
        self.multi_hop_executor = MultiHopExecutor(cosmos_client, vector_search_fn)
        self.disambiguation_handler = DisambiguationHandler()
        self.synthesizer = ResponseSynthesizer()
    
    async def plan_and_execute(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Main entry point for query processing.
        
        Args:
            query: User query string
            context: Optional context (e.g., previous clarification)
            
        Returns:
            {
                "answer": "Natural language response",
                "query_type": "specific_fact|general_info|complex_hybrid|unclear",
                "execution_path": "graph|vector|multi_hop|disambiguation",
                "metadata": {...},
                "confidence": 0.0-1.0
            }
        """
        
        # Step 1: Classify query
        query_type, confidence, entities = self.classifier.classify(query)
        
        log.info(f"Query classified as {query_type.value} (confidence: {confidence})")
        log.info(f"Entities found: {len(entities)}")
        
        # Step 2: Route to appropriate handler
        if query_type == QueryType.UNCLEAR:
            return await self._handle_unclear_query(query, entities)
            
        elif query_type == QueryType.SPECIFIC_FACT:
            return await self._handle_specific_fact_query(query, entities)
            
        elif query_type == QueryType.GENERAL_INFO:
            return await self._handle_general_info_query(query, entities)
            
        elif query_type == QueryType.COMPLEX_HYBRID:
            return await self._handle_complex_hybrid_query(query, entities)
        
        else:
            # Fallback
            return {
                "answer": "I'm not sure how to process this query.",
                "query_type": "unknown",
                "execution_path": "error",
                "confidence": 0.0
            }
    
    async def _handle_unclear_query(
        self,
        query: str,
        entities: list
    ) -> Dict[str, Any]:
        """Handle unclear/ambiguous queries."""
        
        clarification = self.disambiguation_handler.generate_clarification(
            query,
            entities
        )
        
        return {
            "answer": clarification["message"],
            "query_type": "unclear",
            "execution_path": "disambiguation",
            "needs_clarification": True,
            "clarification_options": clarification["options"],
            "metadata": {
                "entities_found": entities,
                "clarification_type": clarification["clarification_type"]
            },
            "confidence": 0.0
        }
    
    async def _handle_specific_fact_query(
        self,
        query: str,
        entities: list
    ) -> Dict[str, Any]:
        """Handle specific fact queries using graph database or local data fallback."""
        
        if not self.cosmos_client:
            # Fallback to local entity data search
            log.info("No Cosmos DB available, using local data fallback")
            return await self._handle_local_specific_fact_query(query, entities)
        
        # Generate Gremlin query
        query_result = self.graph_generator.generate_query(
            QueryType.SPECIFIC_FACT,
            entities,
            query
        )
        
        if not query_result["query"]:
            return {
                "answer": "Failed to generate a valid query for your request.",
                "query_type": "specific_fact",
                "execution_path": "graph",
                "error": query_result.get("explanation", "Query generation failed"),
                "confidence": 0.0
            }
        
        # VALIDATE BEFORE EXECUTION
        from .query_validator import QueryValidator
        validated_query, is_valid = QueryValidator.validate_and_fix(
            query_result["query"], 
            query  # original user query for comparison
        )
        
        if not is_valid:
            log.error("Query validation failed - contains hardcoded dates")
            return {
                "answer": "Query generation failed due to hardcoded date parameters. Please try rephrasing your question.",
                "query_type": "specific_fact",
                "execution_path": "graph",
                "error": "Hardcoded dates detected",
                "confidence": 0.0
            }
        
        # Execute validated query
        try:
            graph_results = await self.cosmos_client._execute_query(validated_query)
            
            # ADD DEBUGGING HERE
            log.info(f"📊 Graph query returned {len(graph_results)} results")
            if graph_results:
                log.info(f"Sample result structure: {json.dumps(graph_results[0], default=str)[:500]}")
            
            # Synthesize response
            synthesis = await self.synthesizer.synthesize_response(
                query,
                graph_results=graph_results,  # Make sure this is being passed
                query_context={"entities": entities, "query_type": "specific_fact"}
            )
            
            return {
                "answer": synthesis["answer"],
                "query_type": "specific_fact",
                "execution_path": "graph",
                "metadata": {
                    "gremlin_query": validated_query,
                    "result_count": len(graph_results),
                    "entities": entities
                },
                "citations": synthesis.get("citations", []),
                "confidence": synthesis.get("confidence", 0.5)
            }
            
        except Exception as e:
            log.error(f"Graph query execution failed: {e}")
            return {
                "answer": f"Error executing query: {str(e)}",
                "query_type": "specific_fact",
                "execution_path": "graph",
                "error": str(e),
                "confidence": 0.0
            }
    
    async def _handle_general_info_query(
        self,
        query: str,
        entities: list
    ) -> Dict[str, Any]:
        """Handle general information queries using vector search."""
        
        if not self.vector_search_fn:
            return {
                "answer": "Vector search is not available for general queries.",
                "query_type": "general_info",
                "execution_path": "vector",
                "error": "No vector search function",
                "confidence": 0.0
            }
        
        # Execute vector search
        try:
            vector_results = self.vector_search_fn(query, limit=8)
            
            if not vector_results:
                return {
                    "answer": "I couldn't find relevant information for your query.",
                    "query_type": "general_info",
                    "execution_path": "vector",
                    "metadata": {"entities": entities},
                    "confidence": 0.0
                }
            
            # Synthesize response
            synthesis = await self.synthesizer.synthesize_response(
                query,
                vector_results=vector_results,
                query_context={"entities": entities, "query_type": "general_info"}
            )
            
            return {
                "answer": synthesis["answer"],
                "query_type": "general_info",
                "execution_path": "vector",
                "metadata": {
                    "result_count": len(vector_results),
                    "entities": entities,
                    "avg_similarity": sum(r.get("similarity", 0) for r in vector_results) / len(vector_results)
                },
                "citations": synthesis.get("citations", []),
                "confidence": synthesis.get("confidence", 0.5)
            }
            
        except Exception as e:
            log.error(f"Vector search failed: {e}")
            return {
                "answer": f"Error during search: {str(e)}",
                "query_type": "general_info",
                "execution_path": "vector",
                "error": str(e),
                "confidence": 0.0
            }
    
    async def _handle_complex_hybrid_query(
        self,
        query: str,
        entities: list
    ) -> Dict[str, Any]:
        """Handle complex queries requiring multiple hops."""
        
        if not self.cosmos_client and not self.vector_search_fn:
            return {
                "answer": "Neither graph nor vector search is available for complex queries.",
                "query_type": "complex_hybrid",
                "execution_path": "multi_hop",
                "error": "No data sources available",
                "confidence": 0.0
            }
        
        # Execute multi-hop resolution
        try:
            multi_hop_result = await self.multi_hop_executor.execute_multi_hop(
                query,
                max_hops=4
            )
            
            return {
                "answer": multi_hop_result["answer"],
                "query_type": "complex_hybrid",
                "execution_path": "multi_hop",
                "metadata": {
                    "hop_count": multi_hop_result["hop_count"],
                    "graph_hops": multi_hop_result.get("graph_hops", 0),
                    "vector_hops": multi_hop_result.get("vector_hops", 0),
                    "entities": entities,
                    "sources": multi_hop_result["sources"]
                },
                "hops": multi_hop_result["hops"],
                "confidence": 0.8  # Multi-hop usually has good coverage
            }
            
        except Exception as e:
            log.error(f"Multi-hop execution failed: {e}")
            return {
                "answer": f"Error during complex query processing: {str(e)}",
                "query_type": "complex_hybrid",
                "execution_path": "multi_hop",
                "error": str(e),
                "confidence": 0.0
            }
    
    async def handle_clarification_response(
        self,
        selection: int,
        previous_response: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Handle user's response to clarification request.
        
        Args:
            selection: User's selection number
            previous_response: Previous response that requested clarification
            
        Returns:
            New query result after clarification
        """
        
        if not previous_response.get("needs_clarification"):
            return {
                "answer": "No clarification was pending.",
                "error": "Invalid state",
                "confidence": 0.0
            }
        
        # Process selection
        clarification_result = self.disambiguation_handler.process_user_selection(
            selection,
            {
                "options": previous_response.get("clarification_options", []),
                "original_query": previous_response.get("metadata", {}).get("original_query", "")
            }
        )
        
        if not clarification_result["resolved"]:
            return {
                "answer": clarification_result.get("error", "Invalid selection"),
                "error": "Clarification failed",
                "confidence": 0.0
            }
        
        # Re-execute with refined query
        refined_query = clarification_result["refined_query"]
        return await self.plan_and_execute(refined_query) 