import subprocess
import sys
import os
import json
import re
from pathlib import Path
from typing import Dict, Any, List
from enum import Enum
import logging
from .query_router import SmartQueryRouter, QueryIntent

logger = logging.getLogger(__name__)

class QueryType(Enum):
    LOCAL = "local"
    GLOBAL = "global"
    DRIFT = "drift"

class CityClerkQueryEngine:
    """Enhanced query engine for GraphRAG-indexed city clerk documents with multi-entity support."""
    
    def __init__(self, graphrag_root: Path):
        self.graphrag_root = Path(graphrag_root)
        self.router = SmartQueryRouter()
        
    def _get_python_executable(self):
        """Get the correct Python executable."""
        from pathlib import Path
        
        current_file = Path(__file__)
        project_root = current_file.parent.parent.parent
        
        venv_python = project_root / "venv" / "bin" / "python3"
        if venv_python.exists():
            return str(venv_python)
        
        return sys.executable
        
    async def query(self, 
                    question: str,
                    method: str = None,
                    **kwargs) -> Dict[str, Any]:
        """Execute a query with intelligent routing and multi-entity support."""
        
        # Auto-route if method not specified
        if method is None:
            route_info = self.router.determine_query_method(question)
            method = route_info['method']
            params = route_info['params']
            intent = route_info['intent']
            
            # Log the routing decision
            logger.info(f"Query: {question}")
            logger.info(f"Routed to: {method} (intent: {intent.value})")
            
            # Check if multiple entities detected
            if 'multiple_entities' in params:
                entity_count = len(params['multiple_entities'])
                logger.info(f"Detected {entity_count} entities in query")
                logger.info(f"Query focus: {'comparison' if params.get('comparison_mode') else 'specific' if params.get('strict_entity_focus') else 'contextual'}")
        else:
            params = kwargs
            intent = None
        
        # Execute query based on method
        if method == "global":
            result = await self._execute_global_query(question, params)
        elif method == "local":
            result = await self._execute_local_query(question, params)
        elif method == "drift":
            result = await self._execute_drift_query(question, params)
        else:
            raise ValueError(f"Unknown query method: {method}")
        
        # Add routing metadata to result
        result['routing_metadata'] = {
            'detected_intent': self._get_intent_type(params),
            'community_context_enabled': params.get('include_community_context', True),
            'query_method': method,
            'entity_count': len(params.get('multiple_entities', [])) if 'multiple_entities' in params else 1 if 'entity_filter' in params else 0
        }
        
        return result
    
    def _get_intent_type(self, params: Dict) -> str:
        """Determine intent type from parameters."""
        if params.get('comparison_mode'):
            return 'comparison'
        elif params.get('strict_entity_focus'):
            return 'specific_entity'
        elif params.get('focus_on_relationships'):
            return 'relationships'
        else:
            return 'contextual'
    
    async def _execute_global_query(self, question: str, params: Dict) -> Dict[str, Any]:
        """Execute a global search query."""
        cmd = [
            self._get_python_executable(),
            "-m", "graphrag", "query",
            "--root", str(self.graphrag_root),
            "--method", "global"
        ]
        
        if "community_level" in params:
            cmd.extend(["--community-level", str(params["community_level"])])
        
        cmd.extend(["--query", question])
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        return {
            "query": question,
            "query_type": "global",
            "answer": result.stdout,
            "context": self._extract_context(result.stdout),
            "parameters": params
        }
    
    async def _execute_local_query(self, question: str, params: Dict) -> Dict[str, Any]:
        """Execute a local search query with available GraphRAG options."""
        
        # Handle multiple entities
        if "multiple_entities" in params:
            return await self._execute_multi_entity_query(question, params)
        
        # Single entity query - use available GraphRAG options
        cmd = [
            self._get_python_executable(),
            "-m", "graphrag", "query",
            "--root", str(self.graphrag_root),
            "--method", "local"
        ]
        
        # Use community-level to control context (available option)
        if params.get("disable_community", False):
            # Use highest community level to get most specific results
            cmd.extend(["--community-level", "3"])  
            logger.info("Using high community level (3) for specific entity query")
        else:
            # Use default community level for broader context
            cmd.extend(["--community-level", "2"])
            logger.info("Using default community level (2) for contextual query")
        
        # If we have entity filtering request, modify the query to be more specific
        if "entity_filter" in params:
            filter_info = params["entity_filter"]
            entity_type = filter_info['type'].replace('_', ' ').lower()
            entity_value = filter_info['value']
            
            if params.get("strict_entity_focus", False):
                # Make query more specific to focus on just this entity
                enhanced_question = f"Tell me specifically about {entity_type} {entity_value}. Focus only on {entity_value} and do not include information about other items."
                logger.info(f"Enhanced query for strict focus on {entity_value}")
            else:
                # Keep original query but mention the entity
                enhanced_question = f"{question} (specifically about {entity_type} {entity_value})"
                logger.info(f"Enhanced query for contextual information about {entity_value}")
            
            question = enhanced_question
        
        cmd.extend(["--query", question])
        
        logger.debug(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        answer = result.stdout
        
        # Post-process if strict entity focus is requested
        if params.get("strict_entity_focus", False) and "entity_filter" in params:
            answer = self._filter_to_specific_entity(answer, params["entity_filter"]["value"])
        
        return {
            "query": question,
            "query_type": "local",
            "answer": answer,
            "context": self._extract_context(answer),
            "parameters": params,
            "intent_detection": {
                "specific_entity_focus": params.get("strict_entity_focus", False),
                "community_level_used": 3 if params.get("disable_community") else 2,
                "query_enhanced": "entity_filter" in params
            }
        }
    
    async def _execute_multi_entity_query(self, question: str, params: Dict) -> Dict[str, Any]:
        """Execute queries for multiple entities using available GraphRAG options."""
        entities = params["multiple_entities"]
        all_results = []
        
        # Determine query strategy based on intent
        if params.get("aggregate_results") and params.get("strict_entity_focus"):
            # Query each entity separately with high community level
            logger.info(f"Executing separate queries for {len(entities)} entities")
            
            for entity in entities:
                cmd = [
                    self._get_python_executable(),
                    "-m", "graphrag", "query",
                    "--root", str(self.graphrag_root),
                    "--method", "local",
                    "--community-level", "3"  # High level for specific results
                ]
                
                # Create entity-specific query
                entity_type = entity['type'].replace('_', ' ').lower()
                entity_query = f"Tell me specifically about {entity_type} {entity['value']}. Focus only on {entity['value']}."
                cmd.extend(["--query", entity_query])
                
                result = subprocess.run(cmd, capture_output=True, text=True)
                all_results.append({
                    "entity": entity,
                    "answer": result.stdout
                })
            
            # Combine results
            combined_answer = self._format_multiple_entity_results(all_results, params)
            
        elif params.get("comparison_mode"):
            # Query with all entities for comparison
            logger.info(f"Executing comparison query for entities: {[e['value'] for e in entities]}")
            
            entity_values = [e['value'] for e in entities]
            comparison_query = f"Compare and contrast {' and '.join(entity_values)}. What are the similarities and differences between these items?"
            
            cmd = [
                self._get_python_executable(),
                "-m", "graphrag", "query",
                "--root", str(self.graphrag_root),
                "--method", "local",
                "--community-level", "2",  # Medium level for comparison context
                "--query", comparison_query
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            combined_answer = self._format_comparison_results(result.stdout, entities)
            
        else:
            # Query for relationships between entities
            logger.info(f"Executing relationship query for entities: {[e['value'] for e in entities]}")
            
            entity_values = [e['value'] for e in entities]
            relationship_query = f"How do {' and '.join(entity_values)} relate to each other? What connections exist between these items?"
            
            cmd = [
                self._get_python_executable(),
                "-m", "graphrag", "query",
                "--root", str(self.graphrag_root),
                "--method", "local",
                "--community-level", "1",  # Lower level for broader relationships
                "--query", relationship_query
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            combined_answer = result.stdout
        
        return {
            "query": question,
            "query_type": "local",
            "answer": combined_answer,
            "context": self._extract_context(combined_answer),
            "parameters": params,
            "intent_detection": {
                "multi_entity_query": True,
                "entity_count": len(entities),
                "query_mode": "comparison" if params.get("comparison_mode") else "aggregate" if params.get("aggregate_results") else "relationships"
            }
        }
    
    def _format_multiple_entity_results(self, results: List[Dict], params: Dict) -> str:
        """Format results from multiple individual entity queries."""
        formatted = []
        
        formatted.append(f"Information about {len(results)} requested items:\n")
        
        for i, result in enumerate(results, 1):
            entity = result['entity']
            answer = result['answer'].strip()
            
            formatted.append(f"\n{i}. {entity['type'].replace('_', ' ').title()} {entity['value']}:")
            formatted.append("-" * 50)
            
            # Clean and format the answer
            if answer:
                # Remove any GraphRAG metadata/headers if present
                clean_answer = self._clean_graphrag_output(answer)
                formatted.append(clean_answer)
            else:
                formatted.append(f"No information found for {entity['value']}")
        
        return "\n".join(formatted)
    
    def _format_comparison_results(self, raw_answer: str, entities: List[Dict]) -> str:
        """Format comparison results to highlight differences and similarities."""
        # This could be enhanced with more sophisticated formatting
        formatted = [f"Comparison of {', '.join([e['value'] for e in entities])}:\n"]
        formatted.append(raw_answer)
        
        return "\n".join(formatted)
    
    def _clean_graphrag_output(self, output: str) -> str:
        """Remove GraphRAG metadata and format output cleanly."""
        # Remove common GraphRAG headers/footers
        lines = output.split('\n')
        cleaned_lines = []
        
        for line in lines:
            # Skip metadata lines
            if line.startswith('INFO:') or line.startswith('WARNING:') or line.startswith('DEBUG:'):
                continue
            # Skip empty lines at start/end
            if not line.strip() and (not cleaned_lines or len(cleaned_lines) == len(lines) - 1):
                continue
            cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines).strip()
    
    async def _execute_drift_query(self, question: str, params: Dict) -> Dict[str, Any]:
        """Execute a DRIFT search query."""
        cmd = [
            self._get_python_executable(),
            "-m", "graphrag", "query",
            "--root", str(self.graphrag_root),
            "--method", "drift"
        ]
        
        cmd.extend(["--query", question])
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        return {
            "query": question,
            "query_type": "drift",
            "answer": result.stdout,
            "context": self._extract_context(result.stdout),
            "parameters": params
        }
    
    def _filter_to_specific_entity(self, response: str, target_entity: str) -> str:
        """Aggressively filter response to ONLY information about the target entity."""
        if not target_entity or not response:
            return response
        
        # Split response into sentences
        sentences = response.split('.')
        filtered_sentences = []
        
        for sentence in sentences:
            # Only keep sentences that explicitly mention the target entity
            if target_entity in sentence:
                # Check if any other entity codes are mentioned
                other_entities = re.findall(r'\b[A-Z]-\d+\b', sentence)
                other_entities = [e for e in other_entities if e != target_entity]
                
                # Only keep if no other entities are mentioned
                if not other_entities:
                    filtered_sentences.append(sentence.strip())
        
        if filtered_sentences:
            filtered_response = '. '.join(filtered_sentences) + '.'
            filtered_response = f"Information specifically about {target_entity}:\n\n{filtered_response}"
        else:
            filtered_response = f"Specific information about {target_entity} only."
        
        return filtered_response
    
    def _is_paragraph_about_target(self, paragraph: str, target: str, all_entities: set) -> bool:
        """Determine if a paragraph should be kept in filtered response."""
        if target not in paragraph:
            return False
        
        other_entities = all_entities - {target}
        if not any(entity in paragraph for entity in other_entities):
            return True
        
        target_count = paragraph.count(target)
        other_counts = sum(paragraph.count(entity) for entity in other_entities)
        
        return target_count >= other_counts
    
    def _extract_context(self, response: str) -> List[Dict]:
        """Extract context and sources from response."""
        context = []
        # Parse response for entity references and sources
        return context

# Legacy compatibility class
class CityClerkGraphRAGQuery(CityClerkQueryEngine):
    """Legacy compatibility wrapper."""
    
    async def query(self, 
                    question: str, 
                    query_type: QueryType = QueryType.LOCAL,
                    community_level: int = 0) -> Dict[str, Any]:
        """Legacy query method for backward compatibility."""
        return await super().query(
            question=question,
            method=query_type.value,
            community_level=community_level
        )

# Example usage function
async def handle_user_query(question: str, graphrag_root: Path = None):
    """Handle user query with intelligent routing."""
    if graphrag_root is None:
        graphrag_root = Path("./graphrag_data")
    
    engine = CityClerkQueryEngine(graphrag_root)
    result = await engine.query(question)
    
    print(f"Query: {question}")
    print(f"Selected method: {result['query_type']}")
    print(f"Detected entities: {result['routing_metadata'].get('entity_count', 0)}")
    print(f"Query intent: {result['routing_metadata'].get('detected_intent', 'unknown')}")
    
    return result 