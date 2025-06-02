"""
Azure Cosmos DB Gremlin client for city clerk graph database.
Provides async operations for graph manipulation.
"""

from __future__ import annotations
import asyncio
import logging
from typing import Any, Dict, List, Optional, Union
import os
from gremlin_python.driver import client, serializer
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.structure.graph import Graph
from dotenv import load_dotenv
import json

load_dotenv()

log = logging.getLogger('cosmos_graph_client')


class CosmosGraphClient:
    """Async client for Azure Cosmos DB Gremlin API."""
    
    def __init__(self, 
                 endpoint: Optional[str] = None,
                 key: Optional[str] = None,
                 database: Optional[str] = None,
                 container: Optional[str] = None,
                 partition_value: str = "demo"):
        """Initialize Cosmos DB client."""
        self.endpoint = endpoint or os.getenv("COSMOS_ENDPOINT")
        self.key = key or os.getenv("COSMOS_KEY")
        self.database = database or os.getenv("COSMOS_DATABASE", "cgGraph")
        self.container = container or os.getenv("COSMOS_CONTAINER", "cityClerk")
        self.partition_value = partition_value
        
        if not all([self.endpoint, self.key, self.database, self.container]):
            raise ValueError("Missing required Cosmos DB configuration")
        
        self._client = None
        self._loop = asyncio.get_event_loop()
    
    async def connect(self) -> None:
        """Establish connection to Cosmos DB."""
        try:
            self._client = client.Client(
                f"{self.endpoint}/gremlin",
                "g",
                username=f"/dbs/{self.database}/colls/{self.container}",
                password=self.key,
                message_serializer=serializer.GraphSONSerializersV2d0()
            )
            log.info(f"✅ Connected to Cosmos DB: {self.database}/{self.container}")
        except Exception as e:
            log.error(f"❌ Failed to connect to Cosmos DB: {e}")
            raise
    
    async def _execute_query(self, query: str, bindings: Optional[Dict] = None) -> List[Any]:
        """Execute a Gremlin query asynchronously."""
        if not self._client:
            await self.connect()
        
        try:
            # Run synchronous operation in thread pool
            future = self._loop.run_in_executor(
                None,
                lambda: self._client.submit(query, bindings or {})
            )
            callback = await future
            
            # Collect results
            results = []
            for result in callback:
                results.extend(result)
            
            return results
        except Exception as e:
            log.error(f"Query execution failed: {query[:100]}... Error: {e}")
            raise
    
    async def clear_graph(self) -> None:
        """Clear all vertices and edges from the graph."""
        log.warning("🗑️  Clearing entire graph...")
        try:
            # Drop all vertices (edges are automatically removed)
            await self._execute_query("g.V().drop()")
            log.info("✅ Graph cleared successfully")
        except Exception as e:
            log.error(f"Failed to clear graph: {e}")
            raise
    
    async def create_vertex(self, 
                          label: str,
                          vertex_id: str,
                          properties: Dict[str, Any]) -> None:
        """Create a vertex with properties."""
        # Build property chain
        prop_chain = ""
        for key, value in properties.items():
            if value is not None:
                # Handle different value types
                if isinstance(value, bool):
                    prop_chain += f".property('{key}', {str(value).lower()})"
                elif isinstance(value, (int, float)):
                    prop_chain += f".property('{key}', {value})"
                elif isinstance(value, list):
                    # Convert list to JSON string
                    json_val = json.dumps(value).replace("'", "\\'")
                    prop_chain += f".property('{key}', '{json_val}')"
                else:
                    # Escape string values
                    escaped_val = str(value).replace("'", "\\'").replace('"', '\\"')
                    prop_chain += f".property('{key}', '{escaped_val}')"
        
        # Always add partition key
        prop_chain += f".property('partitionKey', '{self.partition_value}')"
        
        query = f"g.addV('{label}').property('id', '{vertex_id}'){prop_chain}"
        
        await self._execute_query(query)
    
    async def create_edge(self,
                         from_id: str,
                         to_id: str,
                         edge_type: str,
                         properties: Optional[Dict[str, Any]] = None) -> None:
        """Create an edge between two vertices."""
        # Build property chain for edge
        prop_chain = ""
        if properties:
            for key, value in properties.items():
                if value is not None:
                    if isinstance(value, bool):
                        prop_chain += f".property('{key}', {str(value).lower()})"
                    elif isinstance(value, (int, float)):
                        prop_chain += f".property('{key}', {value})"
                    else:
                        escaped_val = str(value).replace("'", "\\'")
                        prop_chain += f".property('{key}', '{escaped_val}')"
        
        query = f"g.V('{from_id}').addE('{edge_type}').to(g.V('{to_id}')){prop_chain}"
        
        try:
            await self._execute_query(query)
        except Exception as e:
            log.error(f"Failed to create edge {from_id} -> {to_id}: {e}")
            raise
    
    async def vertex_exists(self, vertex_id: str) -> bool:
        """Check if a vertex exists."""
        result = await self._execute_query(f"g.V('{vertex_id}').count()")
        return result[0] > 0 if result else False
    
    async def get_vertex(self, vertex_id: str) -> Optional[Dict]:
        """Get a vertex by ID."""
        result = await self._execute_query(f"g.V('{vertex_id}').valueMap(true)")
        return result[0] if result else None
    
    async def close(self) -> None:
        """Close the connection."""
        if self._client:
            try:
                # Don't use run_until_complete in an async context
                # Just close the client synchronously
                self._client.close()
            except Exception as e:
                log.warning(f"Error closing client: {e}")
            finally:
                self._client = None
                log.info("Connection closed")
    
    async def __aenter__(self):
        await self.connect()
        return self
    
    async def __aexit__(self, *args):
        await self.close() 