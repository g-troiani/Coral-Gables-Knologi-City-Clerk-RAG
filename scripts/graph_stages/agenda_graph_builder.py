"""
Agenda Graph Builder
===================
Builds graph representation from extracted agenda ontology.
"""
import logging
from typing import Dict, List, Optional
from pathlib import Path
import hashlib
import json

from .cosmos_db_client import CosmosGraphClient

log = logging.getLogger(__name__)


class AgendaGraphBuilder:
    """Build comprehensive graph representation from agenda ontology."""
    
    def __init__(self, cosmos_client: CosmosGraphClient):
        self.cosmos = cosmos_client
        self.entity_id_cache = {}  # Cache for entity IDs
    
    async def build_graph_from_ontology(self, ontology: Dict, agenda_path: Path) -> Dict:
        """Build graph representation from extracted ontology."""
        log.info(f"🔨 Starting graph build for {agenda_path.name}")
        
        # Store hyperlinks for reference
        hyperlinks = ontology.get('hyperlinks', {})
        
        graph_data = {
            'nodes': {},
            'edges': [],
            'statistics': {
                'entities': {},
                'relationships': 0,
                'hyperlinks': len(hyperlinks)  # Track hyperlink count
            }
        }
        
        meeting_date = ontology['meeting_date']
        meeting_info = ontology['meeting_info']
        
        # 1. Create Meeting node as the root
        meeting_id = await self._create_meeting_node(meeting_date, meeting_info, agenda_path.name)
        log.info(f"✅ Created meeting node: {meeting_id}")
        
        graph_data['nodes'][meeting_id] = {
            'type': 'Meeting',
            'date': meeting_date,
            'info': meeting_info
        }
        
        # 2. Create nodes for officials present
        await self._create_official_nodes(meeting_info.get('officials_present', {}), meeting_id)
        
        # 3. Process agenda structure
        section_count = 0
        item_count = 0
        
        log.info(f"📑 Processing {len(ontology['agenda_structure'])} sections")
        
        for section_idx, section in enumerate(ontology['agenda_structure']):
            section_count += 1
            section_id = f"section-{meeting_date}-{section_idx}"
            
            # Create AgendaSection node
            await self._create_section_node(section_id, section, section_idx)
            log.info(f"✅ Created section {section_idx}: {section.get('section_name', 'Unknown')}")
            
            graph_data['nodes'][section_id] = {
                'type': 'AgendaSection',
                'name': section['section_name'],
                'order': section_idx
            }
            
            # Link section to meeting
            await self.cosmos.create_edge(
                from_id=meeting_id,
                to_id=section_id,
                edge_type='HAS_SECTION',
                properties={'order': section_idx}
            )
            log.info(f"✅ Created edge: {meeting_id} -> {section_id}")
            
            # Process items in section
            previous_item_id = None
            items = section.get('items', [])
            log.info(f"📌 Processing {len(items)} items in section {section_idx}")
            
            for item_idx, item in enumerate(items):
                if not item.get('item_code'):
                    log.warning(f"Skipping item without code in section {section['section_name']}")
                    continue
                    
                item_count += 1
                item_id = f"item-{meeting_date}-{item['item_code']}"
                
                # Create AgendaItem node with rich metadata
                await self._create_agenda_item_node(item_id, item, section.get('section_type', 'Unknown'))
                log.info(f"✅ Created item {item['item_code']}: {item.get('title', 'Unknown')}")
                
                graph_data['nodes'][item_id] = {
                    'type': 'AgendaItem',
                    'code': item['item_code'],
                    'title': item.get('title', 'Unknown')
                }
                
                # Link item to section
                await self.cosmos.create_edge(
                    from_id=section_id,
                    to_id=item_id,
                    edge_type='CONTAINS_ITEM',
                    properties={'order': item_idx}
                )
                log.info(f"✅ Created edge: {section_id} -> {item_id}")
                
                # Create sequential relationships
                if previous_item_id:
                    await self.cosmos.create_edge(
                        from_id=previous_item_id,
                        to_id=item_id,
                        edge_type='FOLLOWS',
                        properties={'sequence': item_idx}
                    )
                    log.info(f"✅ Created sequential edge: {previous_item_id} -> {item_id}")
                
                previous_item_id = item_id
                
                # Create sponsor relationship if exists
                if item.get('sponsor'):
                    await self._create_sponsor_relationship(item_id, item['sponsor'])
                    log.info(f"✅ Created sponsor relationship for {item_id}")
                
                # Create department relationship if exists
                if item.get('department'):
                    await self._create_department_relationship(item_id, item['department'])
                    log.info(f"✅ Created department relationship for {item_id}")
        
        # 4. Create entity nodes
        log.info(f"👥 Creating entity nodes from extracted entities")
        entity_count = await self._create_entity_nodes(ontology['entities'], meeting_id)
        
        # 5. Create relationships
        relationship_count = 0
        log.info(f"🔗 Creating {len(ontology['relationships'])} relationships")
        
        for rel in ontology['relationships']:
            await self._create_item_relationship(rel, meeting_date)
            relationship_count += 1
        
        # Update statistics
        graph_data['statistics'] = {
            'sections': section_count,
            'items': item_count,
            'entities': entity_count,
            'relationships': relationship_count,
            'meeting_date': meeting_date
        }
        
        log.info(f"🎉 Graph build complete for {agenda_path.name}")
        log.info(f"   - Sections: {section_count}")
        log.info(f"   - Items: {item_count}")
        log.info(f"   - Entities: {entity_count}")
        log.info(f"   - Relationships: {relationship_count}")
        
        return graph_data
    
    async def _create_meeting_node(self, meeting_date: str, meeting_info: Dict, source_file: str = None) -> str:
        """Create Meeting node with comprehensive metadata."""
        meeting_id = f"meeting-{meeting_date.replace('.', '-')}"
        
        # First check if it already exists
        check_query = f"g.V('{meeting_id}')"
        existing = await self.cosmos._execute_query(check_query)
        if existing:
            log.info(f"Meeting {meeting_id} already exists")
            return meeting_id
        
        location = meeting_info.get('location', {})
        if isinstance(location, dict):
            location_str = f"{location.get('name', '')} - {location.get('address', '')}"
        else:
            location_str = "405 Biltmore Way, Coral Gables, FL"
        
        # Escape the location string BEFORE using it in the f-string
        escaped_location = location_str.replace("'", "\\'")
        
        # Simplified query without fold/coalesce
        query = f"""g.addV('Meeting')
            .property('id','{meeting_id}')
            .property('partitionKey','demo')
            .property('nodeType','Meeting')
            .property('date','{meeting_date}')
            .property('type','{meeting_info.get('meeting_type', 'Regular Meeting')}')
            .property('time','{meeting_info.get('meeting_time', '')}')
            .property('location','{escaped_location}')"""
        
        if source_file:
            query += f".property('source_file','{source_file}')"
        
        try:
            result = await self.cosmos._execute_query(query)
            log.info(f"✅ Created Meeting node: {meeting_id}")
            return meeting_id
        except Exception as e:
            log.error(f"❌ Failed to create Meeting node {meeting_id}: {e}")
            raise
    
    async def _create_section_node(self, section_id: str, section: Dict, order: int) -> str:
        """Create AgendaSection node."""
        # Escape strings BEFORE using in f-string
        section_name = section.get('section_name', 'Unknown').replace("'", "\\'")
        section_type = section.get('section_type', 'OTHER').replace("'", "\\'")
        
        query = f"""g.addV('AgendaSection')
           .property('id', '{section_id}')
           .property('partitionKey', 'demo')
           .property('title', '{section_name}')
           .property('type', '{section_type}')
           .property('order', {order})"""
        
        # Add page range if available
        if 'page_start' in section:
            query += f".property('page_start', {section.get('page_start', 1)})"
        if 'page_end' in section:
            query += f".property('page_end', {section.get('page_end', section.get('page_start', 1))})"
        
        await self.cosmos._execute_query(query)
        return section_id
    
    async def _create_agenda_item_node(self, item_id: str, item: Dict, section_type: str) -> str:
        """Create AgendaItem node with all metadata including hyperlinks and page ranges."""
        # Escape strings BEFORE using in f-string
        title = (item.get('title') or 'Unknown').replace("'", "\\'")
        summary = (item.get('summary') or '').replace("'", "\\'")[:500]
        
        query = f"""g.addV('AgendaItem')
           .property('id', '{item_id}')
           .property('partitionKey', 'demo')
           .property('code', '{item['item_code']}')
           .property('title', '{title}')
           .property('type', '{item.get('item_type', 'Item')}')
           .property('section_type', '{section_type}')"""
        
        # Add page range information
        if 'page_start' in item:
            query += f".property('page_start', {item['page_start']})"
        if 'page_end' in item:
            query += f".property('page_end', {item['page_end']})"
        
        if summary:
            query += f".property('summary', '{summary}')"
        
        # Add document reference and URL if available
        if item.get('document_reference'):
            query += f".property('document_reference', '{item['document_reference']}')"
        
        if item.get('document_url'):
            escaped_url = item['document_url'].replace("'", "\\'")
            query += f".property('document_url', '{escaped_url}')"
            query += f".property('has_hyperlink', true)"
        
        # Add all hyperlinks as a JSON property if multiple exist
        if item.get('hyperlinks') and len(item['hyperlinks']) > 0:
            hyperlinks_json = json.dumps(item['hyperlinks']).replace("'", "\\'")
            query += f".property('hyperlinks_json', '{hyperlinks_json}')"
        
        await self.cosmos._execute_query(query)
        return item_id
    
    async def _create_official_nodes(self, officials: Dict, meeting_id: str):
        """Create nodes for city officials and link to meeting."""
        if not officials:
            return
            
        roles_mapping = {
            'mayor': 'Mayor',
            'vice_mayor': 'Vice Mayor',
            'city_attorney': 'City Attorney',
            'city_manager': 'City Manager',
            'city_clerk': 'City Clerk'
        }
        
        # Process standard officials
        for key, role in roles_mapping.items():
            if officials.get(key) and officials[key] != 'null':
                person_id = await self._ensure_person_node(officials[key], role)
                await self.cosmos.create_edge(
                    from_id=person_id,
                    to_id=meeting_id,
                    edge_type='ATTENDED',
                    properties={'role': role}
                )
        
        # Process commissioners
        commissioners = officials.get('commissioners', [])
        if isinstance(commissioners, list):
            for idx, commissioner in enumerate(commissioners):
                if commissioner and commissioner != 'null':
                    person_id = await self._ensure_person_node(commissioner, 'Commissioner')
                    await self.cosmos.create_edge(
                        from_id=person_id,
                        to_id=meeting_id,
                        edge_type='ATTENDED',
                        properties={'role': 'Commissioner', 'seat': idx + 1}
                    )
    
    async def _create_entity_nodes(self, entities: Dict[str, List[Dict]], meeting_id: str) -> Dict[str, int]:
        """Create nodes for all extracted entities."""
        entity_counts = {}
        
        # Create Person nodes
        for person in entities.get('people', []):
            if person.get('name'):
                person_id = await self._ensure_person_node(person['name'], person.get('role', 'Participant'))
                # Link to meeting if they're mentioned
                await self.cosmos.create_edge(
                    from_id=person_id,
                    to_id=meeting_id,
                    edge_type='MENTIONED_IN',
                    properties={'context': person.get('context', '')[:100]}
                )
        entity_counts['people'] = len(entities.get('people', []))
        
        # Create Organization nodes
        for org in entities.get('organizations', []):
            if org.get('name'):
                org_id = await self._ensure_organization_node(org['name'], org.get('type', 'Organization'))
                await self.cosmos.create_edge(
                    from_id=org_id,
                    to_id=meeting_id,
                    edge_type='MENTIONED_IN',
                    properties={'context': org.get('context', '')[:100]}
                )
        entity_counts['organizations'] = len(entities.get('organizations', []))
        
        # Create Location nodes
        for location in entities.get('locations', []):
            if location.get('name'):
                loc_id = await self._ensure_location_node(
                    location['name'], 
                    location.get('address', ''),
                    location.get('type', 'Location')
                )
                await self.cosmos.create_edge(
                    from_id=loc_id,
                    to_id=meeting_id,
                    edge_type='REFERENCED_IN',
                    properties={'context': location.get('context', '')[:100]}
                )
        entity_counts['locations'] = len(entities.get('locations', []))
        
        # Create FinancialItem nodes
        for amount in entities.get('monetary_amounts', []):
            if amount.get('amount'):
                fin_id = await self._create_financial_node(
                    amount['amount'],
                    amount.get('purpose', ''),
                    meeting_id
                )
        entity_counts['financial_items'] = len(entities.get('monetary_amounts', []))
        
        return entity_counts
    
    async def _ensure_person_node(self, name: str, role: str) -> str:
        """Create or retrieve person node."""
        clean_name = name.strip()
        # Clean the ID by removing invalid characters
        cleaned_id_part = clean_name.lower().replace(' ', '-').replace('.', '').replace("'", '').replace('"', '').replace('/', '-')
        person_id = f"person-{cleaned_id_part}"
        
        # Check cache first
        if person_id in self.entity_id_cache:
            return person_id
        
        # Check if exists in database
        try:
            result = await self.cosmos._execute_query(f"g.V('{person_id}')")
            if result:
                self.entity_id_cache[person_id] = True
                return person_id
        except:
            pass
        
        # Create new person - escape name BEFORE using in f-string
        escaped_name = clean_name.replace("'", "\\'").replace('"', '\\"')
        escaped_role = role.replace("'", "\\'").replace('"', '\\"')
        query = f"""g.addV('Person')
            .property('id', '{person_id}')
            .property('partitionKey', 'demo')
            .property('name', '{escaped_name}')
            .property('roles', '{escaped_role}')"""
        
        await self.cosmos._execute_query(query)
        self.entity_id_cache[person_id] = True
        return person_id
    
    async def _ensure_organization_node(self, name: str, org_type: str) -> str:
        """Create or retrieve organization node."""
        # Clean the ID by removing invalid characters
        cleaned_org_name = name.lower().replace(' ', '-').replace('.', '').replace("'", '').replace('"', '').replace('/', '-').replace(',', '')
        org_id = f"org-{cleaned_org_name}"
        
        if org_id in self.entity_id_cache:
            return org_id
        
        escaped_name = name.replace("'", "\\'").replace('"', '\\"')
        escaped_type = org_type.replace("'", "\\'").replace('"', '\\"')
        query = f"""g.V().has('Organization', 'name', '{escaped_name}').fold().coalesce(unfold(),
            addV('Organization')
            .property('id', '{org_id}')
            .property('partitionKey', 'demo')
            .property('name', '{escaped_name}')
            .property('type', '{escaped_type}')
        )"""
        
        await self.cosmos._execute_query(query)
        self.entity_id_cache[org_id] = True
        return org_id
    
    async def _ensure_location_node(self, name: str, address: str, loc_type: str) -> str:
        """Create or retrieve location node."""
        # Clean the ID by removing invalid characters
        cleaned_loc_name = name.lower().replace(' ', '-').replace('.', '').replace("'", '').replace('"', '').replace('/', '-').replace(',', '')
        loc_id = f"location-{cleaned_loc_name}"
        
        if loc_id in self.entity_id_cache:
            return loc_id
        
        escaped_name = name.replace("'", "\\'").replace('"', '\\"')
        escaped_address = address.replace("'", "\\'").replace('"', '\\"')
        escaped_type = loc_type.replace("'", "\\'").replace('"', '\\"')
        
        query = f"""g.V().has('Location', 'name', '{escaped_name}').fold().coalesce(unfold(),
            addV('Location')
            .property('id', '{loc_id}')
            .property('partitionKey', 'demo')
            .property('name', '{escaped_name}')
            .property('address', '{escaped_address}')
            .property('type', '{escaped_type}')
        )"""
        
        await self.cosmos._execute_query(query)
        self.entity_id_cache[loc_id] = True
        return loc_id
    
    async def _create_financial_node(self, amount: str, purpose: str, meeting_id: str) -> str:
        """Create financial item node."""
        fin_id = f"financial-{hashlib.md5(f'{amount}-{purpose}'.encode()).hexdigest()[:8]}"
        
        escaped_purpose = purpose.replace("'", "\\'")
        
        query = f"""g.addV('FinancialItem')
            .property('id', '{fin_id}')
            .property('partitionKey', 'demo')
            .property('amount', '{amount}')
            .property('purpose', '{escaped_purpose}')"""
        
        await self.cosmos._execute_query(query)
        
        # Link to meeting
        await self.cosmos.create_edge(
            from_id=fin_id,
            to_id=meeting_id,
            edge_type='DISCUSSED_IN'
        )
        
        return fin_id
    
    async def _create_sponsor_relationship(self, item_id: str, sponsor_name: str):
        """Create sponsorship relationship."""
        person_id = await self._ensure_person_node(sponsor_name, 'Sponsor')
        await self.cosmos.create_edge(
            from_id=person_id,
            to_id=item_id,
            edge_type='SPONSORS',
            properties={'role': 'sponsor'}
        )
    
    async def _create_department_relationship(self, item_id: str, department_name: str):
        """Create department origination relationship."""
        dept_id = await self._ensure_organization_node(department_name, 'Department')
        await self.cosmos.create_edge(
            from_id=dept_id,
            to_id=item_id,
            edge_type='ORIGINATES',
            properties={'role': 'originating_department'}
        )
    
    async def _create_item_relationship(self, rel: Dict, meeting_date: str):
        """Create relationship between agenda items."""
        from_id = f"item-{meeting_date}-{rel['from_code']}"
        to_id = f"item-{meeting_date}-{rel['to_code']}"
        
        # Check if both items exist
        try:
            from_result = await self.cosmos._execute_query(f"g.V('{from_id}')")
            to_result = await self.cosmos._execute_query(f"g.V('{to_id}')")
            
            if from_result and to_result:
                await self.cosmos.create_edge(
                    from_id=from_id,
                    to_id=to_id,
                    edge_type=rel['relationship_type'],
                    properties={
                        'description': rel.get('description', ''),
                        'strength': rel.get('strength', 'medium')
                    }
                )
        except Exception as e:
            log.warning(f"Could not create relationship {from_id} -> {to_id}: {e}") 