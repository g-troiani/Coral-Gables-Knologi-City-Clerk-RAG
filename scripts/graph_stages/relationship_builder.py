"""
Relationship Builder Module
==========================
Builds complex relationships between graph entities.
"""
from typing import Dict, List, Optional, Set, Tuple
import logging

log = logging.getLogger(__name__)

class RelationshipBuilder:
    """Build relationships between graph entities."""
    
    def __init__(self, cosmos_client):
        self.cosmos_client = cosmos_client
    
    async def build_document_relationships(
        self,
        doc_id: str,
        doc_data: Dict,
        item_mapping: Optional[Dict] = None
    ):
        """Build all relationships for a document."""
        # 1. Extract and create reference relationships
        references = self._extract_references(doc_data)
        for ref in references:
            await self._create_reference_edge(doc_id, ref)
        
        # 2. Create authorship relationships from item mapping
        if item_mapping and 'sponsor' in item_mapping:
            await self._create_sponsor_relationship(doc_id, item_mapping['sponsor'])
        
        # 3. Extract and create topic relationships
        topics = self._extract_topics(doc_data)
        for topic in topics:
            await self._create_topic_relationship(doc_id, topic)
    
    def _extract_references(self, doc_data: Dict) -> List[Dict]:
        """Extract document references."""
        from .graph_extractor import extract_document_references
        return extract_document_references(doc_data)
    
    async def _create_reference_edge(self, from_doc_id: str, reference: Dict):
        """Create reference edge between documents."""
        # Find target document by number
        target_query = "g.V().has('Document', 'ordinance_number', doc_num)"
        target_query += ".or().has('Document', 'resolution_number', doc_num)"
        
        try:
            results = await self.cosmos_client._execute_query(
                target_query,
                {'doc_num': reference['document_number']}
            )
            
            if results:
                target_id = results[0]['id']
                await self.cosmos_client.create_edge(
                    from_id=from_doc_id,
                    to_id=target_id,
                    edge_type='REFERENCES',
                    properties={'reference_type': reference['reference_type']}
                )
                log.info(f"Created reference: {from_doc_id} -> {reference['document_number']}")
        except Exception as e:
            log.warning(f"Could not create reference to {reference['document_number']}: {e}")
    
    async def _create_sponsor_relationship(self, doc_id: str, sponsor_name: str):
        """Create sponsorship relationship."""
        # Find or create person
        person_query = "g.V().has('Person', 'name', name)"
        results = await self.cosmos_client._execute_query(
            person_query,
            {'name': sponsor_name}
        )
        
        if results:
            person_id = results[0]['id']
        else:
            # Create new person
            person_data = {
                'id': f"person-{sponsor_name.lower().replace(' ', '-')}",
                'partitionKey': 'person',
                'nodeType': 'Person',
                'name': sponsor_name,
                'roles': ['Sponsor']
            }
            person_id = await self.cosmos_client.create_person(person_data)
        
        # Create AUTHORED_BY edge
        await self.cosmos_client.create_edge(
            from_id=doc_id,
            to_id=person_id,
            edge_type='AUTHORED_BY',
            properties={'role': 'sponsor'}
        )
    
    def _extract_topics(self, doc_data: Dict) -> List[str]:
        """Extract topic keywords from document."""
        topics = set()
        
        # Use existing keywords
        topics.update(doc_data.get('keywords', []))
        
        # Extract additional topics from title and content
        title = doc_data.get('title', '').lower()
        
        topic_keywords = {
            'zoning': ['zoning', 'land use', 'development'],
            'budget': ['budget', 'fiscal', 'appropriation', 'expenditure'],
            'public safety': ['police', 'fire', 'emergency', 'safety'],
            'infrastructure': ['road', 'sewer', 'water', 'utility', 'infrastructure'],
            'parks': ['park', 'recreation', 'green space'],
            'transportation': ['traffic', 'transportation', 'parking', 'transit'],
        }
        
        for topic, keywords in topic_keywords.items():
            if any(keyword in title for keyword in keywords):
                topics.add(topic)
        
        return list(topics)
    
    async def _create_topic_relationship(self, doc_id: str, topic: str):
        """Create relationship to topic node."""
        # For now, just store as document property
        # In future, could create separate Topic nodes
        log.debug(f"Document {doc_id} relates to topic: {topic}")
    
    async def build_meeting_aggregations(self, meeting_id: str):
        """Build aggregate relationships for a meeting."""
        # Get all documents for this meeting
        query = """
        g.V(meeting_id)
          .in('PRESENTED_AT')
          .hasLabel('Document')
          .group()
            .by('documentType')
            .by(count())
        """
        
        results = await self.cosmos_client._execute_query(
            query,
            {'meeting_id': meeting_id}
        )
        
        if results:
            summary = results[0]
            log.info(f"Meeting {meeting_id} summary: {summary}")
            
            # Could store this as meeting properties
            # For future analytics 