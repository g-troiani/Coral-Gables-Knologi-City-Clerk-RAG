import subprocess
import json
from pathlib import Path
from typing import Dict, Any, List
from enum import Enum
from .query_router import SmartQueryRouter, QueryIntent

class QueryType(Enum):
    LOCAL = "local"
    GLOBAL = "global"
    DRIFT = "drift"

class CityClerkQueryEngine:
    """Enhanced query engine for GraphRAG-indexed city clerk documents."""
    
    def __init__(self, graphrag_root: Path):
        self.graphrag_root = Path(graphrag_root)
        self.router = SmartQueryRouter()
        
    async def query(self, 
                    question: str,
                    method: str = None,  # Auto-route if None
                    **kwargs) -> Dict[str, Any]:
        """Execute a query with specified or auto-determined method."""
        
        # Auto-route if method not specified
        if method is None:
            route_info = self.router.determine_query_method(question)
            method = route_info['method']
            params = route_info['params']
            intent = route_info['intent']
        else:
            params = kwargs
            intent = None
        
        # Execute query based on method
        if method == "global":
            return await self._execute_global_query(question, params)
        elif method == "local":
            return await self._execute_local_query(question, params)
        elif method == "drift":
            return await self._execute_drift_query(question, params)
        else:
            raise ValueError(f"Unknown query method: {method}")
    
    async def _execute_global_query(self, question: str, params: Dict) -> Dict[str, Any]:
        """Execute a global search query."""
        cmd = [
            "graphrag", "query",
            "--root", str(self.graphrag_root),
            "--method", "global"
        ]
        
        # Add community level if specified
        if "community_level" in params:
            cmd.extend(["--community-level", str(params["community_level"])])
        
        cmd.extend(["--query", question])
        
        # Run query
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        return {
            "query": question,
            "query_type": "global",
            "answer": result.stdout,
            "context": self._extract_context(result.stdout),
            "parameters": params
        }
    
    async def _execute_local_query(self, question: str, params: Dict) -> Dict[str, Any]:
        """Execute a local search query."""
        cmd = [
            "graphrag", "query",
            "--root", str(self.graphrag_root),
            "--method", "local"
        ]
        
        cmd.extend(["--query", question])
        
        # Run query
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        return {
            "query": question,
            "query_type": "local",
            "answer": result.stdout,
            "context": self._extract_context(result.stdout),
            "parameters": params
        }
    
    async def _execute_drift_query(self, question: str, params: Dict) -> Dict[str, Any]:
        """Execute a DRIFT search query."""
        cmd = [
            "graphrag", "query",
            "--root", str(self.graphrag_root),
            "--method", "drift"
        ]
        
        cmd.extend(["--query", question])
        
        # Run query
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        return {
            "query": question,
            "query_type": "drift",
            "answer": result.stdout,
            "context": self._extract_context(result.stdout),
            "parameters": params
        }
    
    def _extract_context(self, response: str) -> List[Dict]:
        """Extract context and sources from response."""
        # Parse response for entity references and sources
        context = []
        
        # This would parse the GraphRAG response format
        # and extract referenced entities and communities
        
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
    """Example usage in your system."""
    if graphrag_root is None:
        graphrag_root = Path("./graphrag_data")
    
    # Initialize engine
    engine = CityClerkQueryEngine(graphrag_root)
    
    # Auto-route and execute query
    result = await engine.query(question)
    
    print(f"Query: {question}")
    print(f"Selected method: {result['query_type']}")
    print(f"Parameters: {result['parameters']}")
    
    return result 