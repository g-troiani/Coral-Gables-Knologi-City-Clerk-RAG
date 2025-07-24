"""
Azure Cosmos DB Gremlin client for city clerk graph database.
Provides async operations for graph manipulation.
"""

from __future__ import annotations
import asyncio
import concurrent.futures
import logging
from typing import Any, Dict, List, Optional, Union
import os
from gremlin_python.driver import client, serializer
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.structure.graph import Graph
from dotenv import load_dotenv
import json

load_dotenv()

log = logging.getLogger(__name__)


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
        self._loop = None
        self.ru_total = 0.0
        self.query_count = 0  # For RU sampling
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)  # Reuse single executor
    
    async def connect(self) -> None:
        """Establish connection to Cosmos DB."""
        try:
            self._loop = asyncio.get_running_loop()
            
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
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                self._executor,  # Reuse executor
                lambda: self._client.submit(query, bindings or {})
            )
            values = await loop.run_in_executor(self._executor, lambda: list(result))
            ru = result.status_attributes.get('x-ms-request-charge', 3.0) if hasattr(result, 'status_attributes') else 3.0
            self.ru_total += float(ru)
            self.query_count += 1
            if self.query_count % 10 == 0:
                log.debug(f"Sampled RU total: {self.ru_total}")
            log.debug(f"RU used: {ru}, total: {self.ru_total}")
            return values
        except Exception as e:
            log.error(f"Query execution failed: {query[:100]}... Error: {e}")
            raise
    
    async def clear_graph(self) -> None:
        """Clear all vertices and edges using small batches to avoid timeouts."""
        log.warning("🗑️ Clearing entire graph using small batches...")
        print("🗑️ Clearing Cosmos DB graph using small batches...")
        
        try:
            # Get initial counts
            vertex_result = await self._execute_query("g.V().count()")
            edge_result = await self._execute_query("g.E().count()")
            
            initial_vertices = vertex_result[0][0] if vertex_result and vertex_result[0] else 0
            initial_edges = edge_result[0][0] if edge_result and edge_result[0] else 0
            
            print(f"   Starting with {initial_vertices} vertices and {initial_edges} edges")
            
            # Use very small batch size to avoid timeouts
            batch_size = 100
            max_iterations = 1000  # Safety limit
            
            # Delete edges first
            edge_iterations = 0
            edges_deleted = 0
            while edge_iterations < max_iterations:
                edge_iterations += 1
                
                # Check current count
                count_result = await self._execute_query("g.E().count()")
                edge_count = count_result[0][0] if count_result and count_result[0] else 0
                
                if edge_count == 0:
                    print(f"   ✅ All edges deleted after {edge_iterations} iterations")
                    break
                
                # Print progress every 10 iterations
                if edge_iterations % 10 == 1:
                    print(f"   Deleting edges... {initial_edges - edge_count} / {initial_edges} deleted ({edge_count} remaining)")
                
                try:
                    # Delete a small batch
                    await self._execute_query(f"g.E().limit({batch_size}).drop()")
                    edges_deleted += batch_size
                except Exception as e:
                    log.warning(f"Edge batch deletion error: {e}")
                    # Try even smaller batch
                    try:
                        await self._execute_query(f"g.E().limit(10).drop()")
                        edges_deleted += 10
                    except:
                        pass
                
                # Small delay to avoid overwhelming the server
                await asyncio.sleep(0.1)
            
            # Delete vertices
            vertex_iterations = 0
            vertices_deleted = 0
            while vertex_iterations < max_iterations:
                vertex_iterations += 1
                
                # Check current count
                count_result = await self._execute_query("g.V().count()")
                vertex_count = count_result[0][0] if count_result and count_result[0] else 0
                
                if vertex_count == 0:
                    print(f"   ✅ All vertices deleted after {vertex_iterations} iterations")
                    break
                
                # Print progress every 10 iterations
                if vertex_iterations % 10 == 1:
                    print(f"   Deleting vertices... {initial_vertices - vertex_count} / {initial_vertices} deleted ({vertex_count} remaining)")
                
                try:
                    # Delete a small batch
                    await self._execute_query(f"g.V().limit({batch_size}).drop()")
                    vertices_deleted += batch_size
                except Exception as e:
                    log.warning(f"Vertex batch deletion error: {e}")
                    # Try even smaller batch
                    try:
                        await self._execute_query(f"g.V().limit(10).drop()")
                        vertices_deleted += 10
                    except:
                        pass
                
                # Small delay to avoid overwhelming the server
                await asyncio.sleep(0.1)
            
            # Final verification
            print("   Verifying deletion...")
            await asyncio.sleep(2)  # Wait for consistency
            
            final_v_result = await self._execute_query("g.V().count()")
            final_e_result = await self._execute_query("g.E().count()")
            
            final_vertices = final_v_result[0][0] if final_v_result and final_v_result[0] else 0
            final_edges = final_e_result[0][0] if final_e_result and final_e_result[0] else 0
            
            if final_vertices == 0 and final_edges == 0:
                print(f"   ✅ Graph completely cleared!")
                print(f"   Deleted {initial_vertices} vertices and {initial_edges} edges")
                log.info(f"   Total RU consumed: {self.ru_total:.2f}")
            else:
                raise Exception(f"Failed to completely clear graph. {final_vertices} vertices and {final_edges} edges remain")
                
        except Exception as e:
            log.error(f"Failed to clear graph: {e}")
            raise
    
    async def create_vertex_with_retry(self, 
                                   label: str,
                                   vertex_id: str,
                                   properties: Dict[str, Any],
                                   update_if_exists: bool = True,
                                   max_retries: int = 3) -> None:
        """Create a vertex with retry logic for PreconditionFailed errors."""
        for attempt in range(max_retries):
            try:
                await self.create_vertex(label, vertex_id, properties, update_if_exists)
                return
            except Exception as e:
                if "PreconditionFailed" in str(e) and attempt < max_retries - 1:
                    wait_time = 0.1 * (2 ** attempt)  # Exponential backoff
                    log.warning(f"PreconditionFailed on attempt {attempt + 1}, retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                raise

    async def create_vertex(self, 
                          label: str,
                          vertex_id: str,
                          properties: Dict[str, Any],
                          update_if_exists: bool = True) -> None:
        """Create a vertex with properties, optionally updating if exists."""
        
        if await self.vertex_exists(vertex_id):
            if update_if_exists:
                await self.update_vertex(vertex_id, properties)
                log.info(f"Updated existing vertex: {vertex_id}")
            else:
                log.info(f"Vertex already exists, skipping: {vertex_id}")
            return
        
        prop_chain = ""
        for key, value in properties.items():
            if value is not None:
                if isinstance(value, bool):
                    prop_chain += f".property('{key}', {'true' if value else 'false'})"
                elif isinstance(value, (int, float)):
                    prop_chain += f".property('{key}', {value})"
                elif isinstance(value, list):
                    # Custom JSON serialization to handle boolean values correctly for Gremlin
                    json_val = json.dumps(value).replace("'", "\\'").replace('True', 'true').replace('False', 'false')
                    prop_chain += f".property('{key}', '{json_val}')"
                else:
                    escaped_val = str(value).replace("'", "\\'").replace('"', '\\"')
                    prop_chain += f".property('{key}', '{escaped_val}')"
        
        prop_chain += f".property('partitionKey', '{self.partition_value}')"
        
        query = f"g.addV('{label}').property('id', '{vertex_id}'){prop_chain}"
        
        await self._execute_query(query)

    async def update_vertex(self, vertex_id: str, properties: Dict[str, Any]) -> None:
        """Update properties of an existing vertex."""
        prop_chain = ""
        for key, value in properties.items():
            if value is not None:
                if isinstance(value, bool):
                    prop_chain += f".property('{key}', {'true' if value else 'false'})"
                elif isinstance(value, (int, float)):
                    prop_chain += f".property('{key}', {value})"
                elif isinstance(value, list):
                    # Custom JSON serialization to handle boolean values correctly for Gremlin
                    json_val = json.dumps(value).replace("'", "\\'").replace('True', 'true').replace('False', 'false')
                    prop_chain += f".property('{key}', '{json_val}')"
                else:
                    escaped_val = str(value).replace("'", "\\'").replace('"', '\\"')
                    prop_chain += f".property('{key}', '{escaped_val}')"
        
        query = f"g.V('{vertex_id}'){prop_chain}"
        
        try:
            await self._execute_query(query)
            log.info(f"Updated vertex {vertex_id}")
        except Exception as e:
            log.error(f"Failed to update vertex {vertex_id}: {e}")
            raise

    async def upsert_vertex(self, 
                           label: str,
                           vertex_id: str,
                           properties: Dict[str, Any]) -> bool:
        """Create or update a vertex. Returns True if created, False if updated."""
        if await self.vertex_exists(vertex_id):
            await self.update_vertex(vertex_id, properties)
            return False
        else:
            await self.create_vertex(label, vertex_id, properties)
            return True

    async def create_edge(self,
                         from_id: str,
                         to_id: str,
                         edge_type: str,
                         properties: Optional[Dict[str, Any]] = None) -> None:
        """Create an edge between two vertices."""
        prop_chain = ""
        if properties:
            for key, value in properties.items():
                if value is not None:
                    if isinstance(value, bool):
                        prop_chain += f".property('{key}', {'true' if value else 'false'})"
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
    
    async def create_edge_if_not_exists(self,
                                       from_id: str,
                                       to_id: str,
                                       edge_type: str,
                                       properties: Optional[Dict[str, Any]] = None) -> bool:
        """Create an edge if it doesn't already exist. Returns True if created."""
        check_query = f"g.V('{from_id}').outE('{edge_type}').where(inV().hasId('{to_id}')).count()"
        
        try:
            result = await self._execute_query(check_query)
            exists = result[0] > 0 if result else False
            
            if not exists:
                await self.create_edge(from_id, to_id, edge_type, properties)
                return True
            else:
                log.debug(f"Edge already exists: {from_id} -[{edge_type}]-> {to_id}")
                return False
        except Exception as e:
            log.error(f"Failed to check/create edge: {e}")
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
        """Close the connection properly."""
        if self._client:
            try:
                # For gremlin-python, ensure proper async cleanup
                if hasattr(self._client, 'close'):
                    if asyncio.iscoroutinefunction(self._client.close):
                        await self._client.close()
                    else:
                        self._client.close()
                self._client = None
                if hasattr(self, '_executor'):
                    self._executor.shutdown(wait=True)  # Proper cleanup
                log.info("Connection closed")
            except Exception as e:
                log.warning(f"Error during client close: {e}")
    
    async def __aenter__(self):
        await self.connect()
        return self
    
    async def __aexit__(self, *args):
        await self.close() 