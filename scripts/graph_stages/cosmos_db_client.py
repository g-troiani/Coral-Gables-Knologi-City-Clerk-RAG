"""
Cosmos DB Graph Client
=====================
Handles all graph database operations using Gremlin API.
"""
import asyncio
import logging
from typing import Dict, List, Optional, Any
from gremlin_python.driver import client, serializer
from gremlin_python.driver.protocol import GremlinServerError
import uuid

log = logging.getLogger(__name__)

class CosmosGraphClient:
    """Async client for Cosmos DB Graph operations."""
    
    def __init__(self, endpoint: str, username: str, password: str):
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.client = None
        
    async def connect(self):
        """Initialize Gremlin client connection."""
        try:
            self.client = client.Client(
                self.endpoint,
                'g',
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
            self.client.close()
    
    async def _execute_query(self, query: str, bindings: Optional[Dict] = None) -> List:
        """Execute a Gremlin query."""
        try:
            callback = self.client.submitAsync(query, bindings or {})
            results = callback.result()
            return results.all().result()
        except GremlinServerError as e:
            log.error(f"Gremlin query error: {e}")
            raise
    
    # ===== Node Creation Methods =====
    
    async def create_document(self, doc_data: Dict) -> str:
        """Create a Document node."""
        doc_id = doc_data.get('id', f"doc-{uuid.uuid4()}")
        
        query = """
        g.addV('Document')
          .property('id', id)
          .property('partitionKey', partitionKey)
          .property('nodeType', nodeType)
          .property('documentClass', documentClass)
          .property('documentType', documentType)
          .property('title', title)
          .property('date', date)
          .property('source_pdf', source_pdf)
          .property('created_at', created_at)
        """
        
        # Add optional properties
        for prop in ['ordinance_number', 'resolution_number', 'reading', 'status']:
            if prop in doc_data:
                query += f".property('{prop}', {prop})"
        
        # Add list properties
        if 'keywords' in doc_data:
            for keyword in doc_data['keywords']:
                query += f".property('keywords', '{keyword}')"
        
        bindings = {
            'id': doc_id,
            'partitionKey': doc_data['partitionKey'],
            'nodeType': doc_data['nodeType'],
            'documentClass': doc_data['documentClass'],
            'documentType': doc_data['documentType'],
            'title': doc_data['title'],
            'date': doc_data['date'],
            'source_pdf': doc_data['source_pdf'],
            'created_at': doc_data['created_at'],
            **{k: v for k, v in doc_data.items() if k in ['ordinance_number', 'resolution_number', 'reading', 'status']}
        }
        
        await self._execute_query(query, bindings)
        log.info(f"Created Document node: {doc_id}")
        return doc_id
    
    async def create_person(self, person_data: Dict) -> str:
        """Create a Person node."""
        person_id = person_data.get('id', f"person-{uuid.uuid4()}")
        
        query = """
        g.V().has('Person', 'name', name).fold()
          .coalesce(
            unfold(),
            addV('Person')
              .property('id', id)
              .property('partitionKey', partitionKey)
              .property('nodeType', nodeType)
              .property('name', name)
          )
        """
        
        # Add roles
        if 'roles' in person_data:
            for role in person_data['roles']:
                query += f".property('roles', '{role}')"
        
        bindings = {
            'id': person_id,
            'partitionKey': person_data['partitionKey'],
            'nodeType': person_data['nodeType'],
            'name': person_data['name']
        }
        
        result = await self._execute_query(query, bindings)
        
        # Get the actual ID (might be existing node)
        get_id_query = "g.V().has('Person', 'name', name).id()"
        id_result = await self._execute_query(get_id_query, {'name': person_data['name']})
        
        actual_id = id_result[0] if id_result else person_id
        log.info(f"Created/Retrieved Person node: {actual_id} ({person_data['name']})")
        return actual_id
    
    async def create_meeting(self, meeting_data: Dict) -> str:
        """Create a Meeting node."""
        meeting_id = meeting_data.get('id', f"meeting-{uuid.uuid4()}")
        
        query = """
        g.V().has('Meeting', 'date', date).fold()
          .coalesce(
            unfold(),
            addV('Meeting')
              .property('id', id)
              .property('partitionKey', partitionKey)
              .property('nodeType', nodeType)
              .property('date', date)
              .property('type', type)
              .property('location', location)
          )
        """
        
        bindings = {
            'id': meeting_id,
            'partitionKey': meeting_data['partitionKey'],
            'nodeType': meeting_data['nodeType'],
            'date': meeting_data['date'],
            'type': meeting_data['type'],
            'location': meeting_data['location']
        }
        
        await self._execute_query(query, bindings)
        log.info(f"Created Meeting node: {meeting_id}")
        return meeting_id
    
    async def create_chunk(self, chunk_data: Dict) -> str:
        """Create a DocumentChunk node."""
        chunk_id = chunk_data.get('id', f"chunk-{uuid.uuid4()}")
        
        query = """
        g.addV('DocumentChunk')
          .property('id', id)
          .property('partitionKey', partitionKey)
          .property('nodeType', nodeType)
          .property('chunk_index', chunk_index)
          .property('text', text)
          .property('page_start', page_start)
          .property('page_end', page_end)
        """
        
        bindings = {
            'id': chunk_id,
            'partitionKey': chunk_data['partitionKey'],
            'nodeType': chunk_data['nodeType'],
            'chunk_index': chunk_data['chunk_index'],
            'text': chunk_data['text'],
            'page_start': chunk_data.get('page_start', 1),
            'page_end': chunk_data.get('page_end', 1)
        }
        
        await self._execute_query(query, bindings)
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
        query = f"""
        g.V(from_id).as('from')
          .V(to_id).as('to')
          .addE(edge_type).from('from').to('to')
        """
        
        bindings = {
            'from_id': from_id,
            'to_id': to_id,
            'edge_type': edge_type
        }
        
        # Add edge properties
        if properties:
            for key, value in properties.items():
                query += f".property('{key}', {key})"
                bindings[key] = value
        
        await self._execute_query(query, bindings)
        log.debug(f"Created edge: {from_id} --[{edge_type}]--> {to_id}")
    
    # ===== Query Methods =====
    
    async def get_all_persons(self) -> List[Dict]:
        """Get all Person nodes."""
        query = "g.V().hasLabel('Person').valueMap(true)"
        results = await self._execute_query(query)
        
        persons = []
        for result in results:
            person = {
                'id': result['id'],
                'name': result['name'][0] if result.get('name') else '',
                'roles': result.get('roles', [])
            }
            persons.append(person)
        
        return persons
    
    async def get_all_meetings(self) -> List[Dict]:
        """Get all Meeting nodes."""
        query = "g.V().hasLabel('Meeting').valueMap(true)"
        results = await self._execute_query(query)
        
        meetings = []
        for result in results:
            meeting = {
                'id': result['id'],
                'date': result['date'][0] if result.get('date') else '',
                'type': result['type'][0] if result.get('type') else 'Regular',
                'location': result['location'][0] if result.get('location') else ''
            }
            meetings.append(meeting)
        
        return meetings
    
    async def clear_graph(self):
        """Clear all nodes and edges from the graph (use with caution!)."""
        log.warning("Clearing entire graph database...")
        
        # Drop all edges first
        await self._execute_query("g.E().drop()")
        
        # Then drop all vertices
        await self._execute_query("g.V().drop()")
        
        log.info("Graph database cleared") 