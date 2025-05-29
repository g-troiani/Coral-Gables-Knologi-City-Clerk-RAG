"""
Cosmos DB Graph Client
=====================
Handles all graph database operations using Gremlin API.
"""
import asyncio
import logging
from typing import Dict, List, Optional, Any, Set
from gremlin_python.driver import client, serializer
from gremlin_python.driver.protocol import GremlinServerError
import uuid
import concurrent.futures

log = logging.getLogger(__name__)

class CosmosGraphClient:
    """Async client for Cosmos DB Graph operations."""
    
    def __init__(self, endpoint: str, username: str, password: str, 
                 partition_key: str = "partitionKey", partition_value: str = "demo"):
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.partition_key = partition_key
        self.partition_value = partition_value
        self.client = None
        
    async def connect(self):
        """Initialize Gremlin client connection."""
        try:
            self.client = client.Client(
                f"{self.endpoint}/gremlin",
                "g",
                username=self.username,
                password=self.password,
                message_serializer=serializer.GraphSONSerializersV2d0()
            )
            log.info("Connected to Cosmos DB Graph")
        except Exception as e:
            log.error(f"Failed to connect to Cosmos DB: {e}")
            raise
    
    async def close(self):
        """Close client connection."""
        if self.client:
            # The gremlin client's close method has its own event loop management
            # We need to handle this carefully in an async context
            try:
                # Run the close in a thread to avoid event loop conflicts
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    await loop.run_in_executor(pool, self.client.close)
            except Exception as e:
                log.warning(f"Error closing client: {e}")
                # Force close if normal close fails
                self.client = None
    
    def _execute_query_sync(self, query: str, bindings: Optional[Dict] = None) -> List:
        """Execute a Gremlin query synchronously."""
        try:
            result = self.client.submit(query, bindings or {})
            result_list = result.all().result()
            
            # Log successful write operations
            if any(keyword in query for keyword in ['addV', 'addE', 'property']):
                log.debug(f"Write query executed: {query[:100]}...")
                if result_list:
                    log.debug(f"Result: {result_list}")
            
            return result_list
        except Exception as e:
            log.error(f"Gremlin query error: {e}")
            log.error(f"Failed query: {query}")
            raise

    async def _execute_query(self, query: str, bindings: Optional[Dict] = None) -> List:
        """Execute a Gremlin query asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._execute_query_sync, query, bindings)
    
    # ===== Node Creation Methods =====
    
    async def create_document(self, doc_data: Dict) -> str:
        """Create a Document node."""
        doc_id = doc_data.get('id', f"doc-{uuid.uuid4()}")
        
        # Build query matching relationOPENAI.py style
        query = f"""g.V('{doc_id}').fold().coalesce(unfold(),
            addV('Document').property(id,'{doc_id}')
            .property('{self.partition_key}','{self.partition_value}')"""
        
        # Add properties
        for prop, value in doc_data.items():
            if prop not in ['id', 'partitionKey'] and value is not None:
                if isinstance(value, str):
                    # Escape single quotes
                    clean_value = value.replace("'", "\\'")
                    query += f".property('{prop}','{clean_value}')"
                elif isinstance(value, (int, float, bool)):
                    query += f".property('{prop}',{value})"
                elif isinstance(value, list) and prop == 'keywords':
                    # Add multiple property values for lists
                    for item in value:
                        clean_item = str(item).replace("'", "\\'")
                        query += f".property('{prop}','{clean_item}')"
        
        query += ")"
        
        await self._execute_query(query)
        log.info(f"Created Document node: {doc_id}")
        return doc_id
    
    async def create_person(self, person_data: Dict) -> str:
        """Create a Person node."""
        person_id = person_data.get('id', f"person-{uuid.uuid4()}")
        
        # Use string-based query like create_document for consistency
        name = person_data['name'].replace("'", "\\'")  # Escape quotes
        query = f"""g.V().has('Person', 'name', '{name}').fold().coalesce(unfold(),
            addV('Person').property(id,'{person_id}')
            .property('{self.partition_key}','{self.partition_value}')
            .property('nodeType','Person')
            .property('name','{name}')"""
        
        # Add roles
        if 'roles' in person_data:
            for role in person_data['roles']:
                clean_role = role.replace("'", "\\'")
                query += f".property('roles','{clean_role}')"
        
        query += ")"
        
        await self._execute_query(query)
        log.info(f"Created/Retrieved Person node: {person_id} ({person_data['name']})")
        return person_id
    
    async def create_meeting(self, meeting_data: Dict) -> str:
        """Create a Meeting node."""
        meeting_id = meeting_data.get('id', f"meeting-{uuid.uuid4()}")
        
        # Use string-based query for consistency
        date = meeting_data['date'].replace("'", "\\'")
        meeting_type = meeting_data['type'].replace("'", "\\'")
        location = meeting_data['location'].replace("'", "\\'")
        
        query = f"""g.V().has('Meeting', 'date', '{date}').fold().coalesce(unfold(),
            addV('Meeting').property(id,'{meeting_id}')
            .property('{self.partition_key}','{self.partition_value}')
            .property('nodeType','Meeting')
            .property('date','{date}')
            .property('type','{meeting_type}')
            .property('location','{location}')
        )"""
        
        await self._execute_query(query)
        log.info(f"Created Meeting node: {meeting_id}")
        return meeting_id
    
    async def create_chunk(self, chunk_data: Dict) -> str:
        """Create a DocumentChunk node."""
        chunk_id = chunk_data.get('id', f"chunk-{uuid.uuid4()}")
        
        # Escape the text content for Gremlin query
        text = chunk_data['text'].replace("'", "\\'").replace("\n", "\\n")[:1000]  # Limit text length
        chunk_index = chunk_data['chunk_index']
        page_start = chunk_data.get('page_start', 1)
        page_end = chunk_data.get('page_end', 1)
        
        query = f"""g.addV('DocumentChunk')
            .property(id,'{chunk_id}')
            .property('{self.partition_key}','{self.partition_value}')
            .property('nodeType','DocumentChunk')
            .property('chunk_index',{chunk_index})
            .property('text','{text}')
            .property('page_start',{page_start})
            .property('page_end',{page_end})"""
        
        await self._execute_query(query)
        return chunk_id
    
    # ===== Edge Creation Methods =====
    
    async def create_edge(
        self, 
        from_id: str, 
        to_id: str, 
        edge_type: str,
        properties: Optional[Dict] = None
    ):
        """Create an edge between two nodes."""
        # First verify both vertices exist
        from_exists = await self._execute_query(f"g.V('{from_id}').count()")
        to_exists = await self._execute_query(f"g.V('{to_id}').count()")
        
        if not from_exists or from_exists[0] == 0:
            log.error(f"Source vertex {from_id} does not exist!")
            return None
        
        if not to_exists or to_exists[0] == 0:
            log.error(f"Target vertex {to_id} does not exist!")
            return None
        
        # Use simpler edge creation syntax
        query = f"g.V('{from_id}').addE('{edge_type}').to(__.V('{to_id}'))"
        
        # Add edge properties
        if properties:
            for key, value in properties.items():
                if isinstance(value, str):
                    clean_value = value.replace("'", "\\'")
                    query += f".property('{key}','{clean_value}')"
                else:
                    query += f".property('{key}',{value})"
        
        try:
            result = await self._execute_query(query)
            log.info(f"✅ Created edge: {from_id} --[{edge_type}]--> {to_id}")
            return result
        except Exception as e:
            log.error(f"❌ Failed to create edge {from_id} --[{edge_type}]--> {to_id}: {e}")
            raise
    
    # ===== Query Methods =====
    
    async def get_all_persons(self) -> List[Dict]:
        """Get all Person nodes."""
        try:
            query = "g.V().hasLabel('Person').valueMap(true)"
            results = await self._execute_query(query)
            
            persons = []
            if results:
                for result in results:
                    person = {
                        'id': result.get('id', ''),
                        'name': result.get('name', [''])[0] if isinstance(result.get('name'), list) else result.get('name', ''),
                        'roles': result.get('roles', [])
                    }
                    persons.append(person)
            
            return persons
        except Exception as e:
            log.warning(f"Error getting persons (database might be empty): {e}")
            return []
    
    async def get_all_meetings(self) -> List[Dict]:
        """Get all Meeting nodes."""
        try:
            query = "g.V().hasLabel('Meeting').valueMap(true)"
            results = await self._execute_query(query)
            
            meetings = []
            if results:
                for result in results:
                    meeting = {
                        'id': result.get('id', ''),
                        'date': result.get('date', [''])[0] if isinstance(result.get('date'), list) else result.get('date', ''),
                        'type': result.get('type', ['Regular'])[0] if isinstance(result.get('type'), list) else result.get('type', 'Regular'),
                        'location': result.get('location', [''])[0] if isinstance(result.get('location'), list) else result.get('location', '')
                    }
                    meetings.append(meeting)
            
            return meetings
        except Exception as e:
            log.warning(f"Error getting meetings (database might be empty): {e}")
            return []
    
    async def check_meeting_exists(self, meeting_date: str) -> bool:
        """Check if a meeting already exists in the database."""
        try:
            query = f"g.V().has('Meeting', 'date', '{meeting_date}')"
            result = await self._execute_query(query)
            return bool(result)
        except Exception as e:
            log.error(f"Error checking meeting existence: {e}")
            return False

    async def get_processed_documents(self) -> Set[str]:
        """Get set of all processed document filenames."""
        try:
            query = "g.V().hasLabel('Meeting').values('source_file')"
            results = await self._execute_query(query)
            return set(results) if results else set()
        except Exception as e:
            log.error(f"Error getting processed documents: {e}")
            return set()

    async def mark_document_processed(self, meeting_id: str, filename: str):
        """Mark a document as processed by storing the source filename."""
        try:
            query = f"g.V('{meeting_id}').property('source_file', '{filename}')"
            await self._execute_query(query)
        except Exception as e:
            log.error(f"Error marking document as processed: {e}")
    
    async def clear_graph(self):
        """Clear all nodes and edges from the graph (use with caution!)."""
        log.warning("Clearing entire graph database...")
        
        # Drop all edges first
        await self._execute_query("g.E().drop()")
        
        # Then drop all vertices
        await self._execute_query("g.V().drop()")
        
        log.info("Graph database cleared") 