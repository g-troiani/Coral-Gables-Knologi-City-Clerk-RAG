"""
Agenda Graph Builder - FIXED VERSION
Builds graph representation from extracted agenda ontology with proper date handling.
"""
import logging
from typing import Dict, List, Optional
from pathlib import Path
import hashlib
import json
import calendar
import re

from .cosmos_db_client import CosmosGraphClient

log = logging.getLogger('pipeline_debug.graph_builder')


class AgendaGraphBuilder:
    """Build comprehensive graph representation from agenda ontology."""
    
    def __init__(self, cosmos_client: CosmosGraphClient, upsert_mode: bool = True):
        self.cosmos = cosmos_client
        self.upsert_mode = upsert_mode
        self.entity_id_cache = {}  # Cache for entity IDs
        self.partition_value = 'demo'  # Partition value property
        self.upsert_mode = upsert_mode  # New flag for upsert behavior
        
        # Track statistics
        self.stats = {
            'nodes_created': 0,
            'nodes_updated': 0,
            'edges_created': 0,
            'edges_skipped': 0
        }
    
    @staticmethod
    def normalize_item_code(code: str) -> str:
        """Normalize item codes to consistent format for matching with ordinances."""
        if not code:
            return code
        
        # Remove trailing dots: "E.-1." -> "E.-1"
        code = code.rstrip('.')
        
        # Remove dots between letter and dash: "E.-1" -> "E-1"
        code = re.sub(r'([A-Z])\.(-)', r'\1\2', code)
        
        # Also handle cases without dash: "E.1" -> "E-1"
        code = re.sub(r'([A-Z])\.(\d)', r'\1-\2', code)
        
        # Ensure we have a dash between letter and number
        code = re.sub(r'([A-Z])(\d)', r'\1-\2', code)
        
        return code
    
    @staticmethod
    def ensure_us_date_format(date_str: str) -> str:
        """Ensure date is in US format MM-DD-YYYY with dashes."""
        # Handle different input formats
        if '.' in date_str:
            # Format: 01.23.2024 -> 01-23-2024
            return date_str.replace('.', '-')
        elif '/' in date_str:
            # Format: 01/23/2024 -> 01-23-2024
            return date_str.replace('/', '-')
        elif re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            # ISO format: 2024-01-23 -> 01-23-2024
            parts = date_str.split('-')
            return f"{parts[1]}-{parts[2]}-{parts[0]}"
        else:
            # Already in correct format or unknown
            return date_str
    
    async def build_graph_from_ontology(self, ontology: Dict, agenda_path: Path) -> Dict:
        """Build graph representation from extracted ontology."""
        log.info(f"🔨 Starting graph build for {agenda_path.name}")
        log.info(f"🔧 Upsert mode: {'ENABLED' if self.upsert_mode else 'DISABLED'}")
        
        # Reset statistics
        self.stats = {
            'nodes_created': 0,
            'nodes_updated': 0,
            'edges_created': 0,
            'edges_skipped': 0
        }
        
        try:
            # Store hyperlinks for reference
            hyperlinks = ontology.get('hyperlinks', {})
            
            graph_data = {
                'nodes': {},
                'edges': [],
                'statistics': {
                    'entities': {},
                    'relationships': 0,
                    'hyperlinks': len(hyperlinks)
                }
            }
            
            # CRITICAL: Ensure meeting date is in US format
            meeting_date_original = ontology['meeting_date']
            meeting_date_us = self.ensure_us_date_format(meeting_date_original)
            meeting_info = ontology['meeting_info']
            
            log.info(f"📅 Meeting date: {meeting_date_original} -> {meeting_date_us}")
            
            # 1. Create Meeting node as the root
            meeting_id = f"meeting-{meeting_date_us}"
            await self._create_meeting_node(meeting_date_us, meeting_info, agenda_path.name)
            log.info(f"✅ Created meeting node: {meeting_id}")
            
            # 1.5 Create Date node and link to meeting
            try:
                date_id = await self._create_date_node(meeting_date_original, meeting_id)
                graph_data['nodes'][date_id] = {
                    'type': 'Date',
                    'date': meeting_date_original
                }
            except Exception as e:
                log.error(f"Failed to create date node: {e}")
            
            graph_data['nodes'][meeting_id] = {
                'type': 'Meeting',
                'date': meeting_date_us,
                'info': meeting_info
            }
            
            # 2. Create nodes for officials present
            await self._create_official_nodes(meeting_info.get('officials_present', {}), meeting_id)
            
            # 3. Process agenda structure
            section_count = 0
            item_count = 0
            
            log.info(f"📑 Processing {len(ontology['agenda_structure'])} sections")
            
            for section_idx, section in enumerate(ontology['agenda_structure']):
                try:
                    section_count += 1
                    section_id = f"section-{meeting_date_us}-{section_idx}"
                    
                    # Create AgendaSection node
                    await self._create_section_node(section_id, section, section_idx)
                    log.info(f"✅ Created section {section_idx}: {section.get('section_name', 'Unknown')}")
                    
                    graph_data['nodes'][section_id] = {
                        'type': 'AgendaSection',
                        'name': section['section_name'],
                        'order': section_idx
                    }
                    
                    # Link section to meeting
                    if await self.cosmos.create_edge_if_not_exists(
                        from_id=meeting_id,
                        to_id=section_id,
                        edge_type='HAS_SECTION',
                        properties={'order': section_idx}
                    ):
                        self.stats['edges_created'] += 1
                    else:
                        self.stats['edges_skipped'] += 1
                    
                    # Process items in section
                    previous_item_id = None
                    items = section.get('items', [])
                    
                    for item_idx, item in enumerate(items):
                        try:
                            if not item.get('item_code'):
                                log.warning(f"Skipping item without code in section {section['section_name']}")
                                continue
                                
                            item_count += 1
                            # Normalize the item code
                            normalized_code = self.normalize_item_code(item['item_code'])
                            # Use US date format for item ID
                            item_id = f"item-{meeting_date_us}-{normalized_code}"
                            
                            log.info(f"Creating item: {item_id} (from code: {item['item_code']})")
                            
                            # Create AgendaItem node
                            await self._create_agenda_item_node(item_id, item, section.get('section_type', 'Unknown'))
                            
                            graph_data['nodes'][item_id] = {
                                'type': 'AgendaItem',
                                'code': normalized_code,
                                'original_code': item['item_code'],
                                'title': item.get('title', 'Unknown')
                            }
                            
                            # Link item to section
                            if await self.cosmos.create_edge_if_not_exists(
                                from_id=section_id,
                                to_id=item_id,
                                edge_type='CONTAINS_ITEM',
                                properties={'order': item_idx}
                            ):
                                self.stats['edges_created'] += 1
                            else:
                                self.stats['edges_skipped'] += 1
                            
                            # Create sequential relationships
                            if previous_item_id:
                                if await self.cosmos.create_edge_if_not_exists(
                                    from_id=previous_item_id,
                                    to_id=item_id,
                                    edge_type='FOLLOWS',
                                    properties={'sequence': item_idx}
                                ):
                                    self.stats['edges_created'] += 1
                                else:
                                    self.stats['edges_skipped'] += 1
                            
                            previous_item_id = item_id
                            
                            # Create sponsor relationship if exists
                            if item.get('sponsor'):
                                await self._create_sponsor_relationship(item_id, item['sponsor'])
                            
                            # Create department relationship if exists
                            if item.get('department'):
                                await self._create_department_relationship(item_id, item['department'])
                                
                        except Exception as e:
                            log.error(f"Failed to process item {item.get('item_code', 'unknown')}: {e}")
                            
                except Exception as e:
                    log.error(f"Failed to process section {section.get('section_name', 'unknown')}: {e}")
            
            # 4. Create entity nodes
            entity_count = await self._create_entity_nodes(ontology['entities'], meeting_id)
            
            # 5. Create relationships
            relationship_count = 0
            for rel in ontology['relationships']:
                try:
                    await self._create_item_relationship(rel, meeting_date_us)
                    relationship_count += 1
                except Exception as e:
                    log.error(f"Failed to create relationship: {e}")
            
            # Update statistics
            graph_data['statistics'] = {
                'sections': section_count,
                'items': item_count,
                'entities': entity_count,
                'relationships': relationship_count,
                'meeting_date': meeting_date_us
            }
            
            log.info(f"🎉 Graph build complete for {agenda_path.name}")
            log.info(f"   📊 Statistics:")
            log.info(f"      - Nodes created: {self.stats['nodes_created']}")
            log.info(f"      - Nodes updated: {self.stats['nodes_updated']}")
            log.info(f"      - Edges created: {self.stats['edges_created']}")
            log.info(f"      - Edges skipped: {self.stats['edges_skipped']}")
            log.info(f"   - Sections: {section_count}")
            log.info(f"   - Items: {item_count}")
            log.info(f"   - Entities: {entity_count}")
            log.info(f"   - Relationships: {relationship_count}")
            
            return graph_data
            
        except Exception as e:
            log.error(f"CRITICAL ERROR in build_graph_from_ontology: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    async def _create_meeting_node(self, meeting_date: str, meeting_info: Dict, source_file: str = None) -> str:
        """Create or update Meeting node with comprehensive metadata."""
        meeting_id = f"meeting-{meeting_date}"
        
        location = meeting_info.get('location', {})
        if isinstance(location, dict):
            location_str = f"{location.get('name', '')} - {location.get('address', '')}"
        else:
            location_str = "405 Biltmore Way, Coral Gables, FL"
        
        properties = {
            'nodeType': 'Meeting',
            'date': meeting_date,
            'type': meeting_info.get('meeting_type', 'Regular Meeting'),
            'time': meeting_info.get('meeting_time', ''),
            'location': location_str
        }
        
        if source_file:
            properties['source_file'] = source_file
        
        # Use upsert instead of create
        if self.upsert_mode:
            created = await self.cosmos.upsert_vertex('Meeting', meeting_id, properties)
            if created:
                self.stats['nodes_created'] += 1
                log.info(f"✅ Created Meeting node: {meeting_id}")
            else:
                self.stats['nodes_updated'] += 1
                log.info(f"📝 Updated Meeting node: {meeting_id}")
        else:
            await self.cosmos.create_vertex('Meeting', meeting_id, properties)
            self.stats['nodes_created'] += 1
            log.info(f"✅ Created Meeting node: {meeting_id}")
        
        return meeting_id
    
    async def _create_date_node(self, date_str: str, meeting_id: str) -> str:
        """Create a Date node and link it to the meeting."""
        from datetime import datetime
        
        # Parse date from MM.DD.YYYY format
        parts = date_str.split('.')
        if len(parts) != 3:
            log.error(f"Invalid date format: {date_str}")
            return None
            
        month, day, year = int(parts[0]), int(parts[1]), int(parts[2])
        
        # Create consistent date ID in ISO format
        date_id = f"date-{year:04d}-{month:02d}-{day:02d}"
        
        # Check if date already exists
        if await self.cosmos.vertex_exists(date_id):
            log.info(f"Date {date_id} already exists")
            # Still create the relationship
            await self.cosmos.create_edge(
                from_id=meeting_id,
                to_id=date_id,
                edge_type='OCCURRED_ON',
                properties={'primary_date': True}
            )
            return date_id
        
        # Get day of week
        date_obj = datetime(year, month, day)
        day_of_week = date_obj.strftime('%A')
        
        # Create date node
        properties = {
            'nodeType': 'Date',
            'full_date': date_str,
            'year': year,
            'month': month,
            'day': day,
            'quarter': (month - 1) // 3 + 1,
            'month_name': calendar.month_name[month],
            'day_of_week': day_of_week,
            'iso_date': f'{year:04d}-{month:02d}-{day:02d}'
        }
        
        await self.cosmos.create_vertex('Date', date_id, properties)
        log.info(f"✅ Created Date node: {date_id}")
        
        # Create relationship: Meeting -> OCCURRED_ON -> Date
        await self.cosmos.create_edge(
            from_id=meeting_id,
            to_id=date_id,
            edge_type='OCCURRED_ON',
            properties={'primary_date': True}
        )
        
        return date_id
    
    async def _create_section_node(self, section_id: str, section: Dict, order: int) -> str:
        """Create or update AgendaSection node."""
        properties = {
            'title': section.get('section_name', 'Unknown'),
            'type': section.get('section_type', 'OTHER'),
            'order': order
        }
        
        # Add page range if available
        if 'page_start' in section:
            properties['page_start'] = section.get('page_start', 1)
        if 'page_end' in section:
            properties['page_end'] = section.get('page_end', section.get('page_start', 1))
        
        if self.upsert_mode:
            created = await self.cosmos.upsert_vertex('AgendaSection', section_id, properties)
            if created:
                self.stats['nodes_created'] += 1
            else:
                self.stats['nodes_updated'] += 1
        else:
            if await self.cosmos.vertex_exists(section_id):
                log.info(f"Section {section_id} already exists, skipping creation")
                return section_id
            await self.cosmos.create_vertex('AgendaSection', section_id, properties)
            self.stats['nodes_created'] += 1
        
        return section_id
    
    async def _create_agenda_item_node(self, item_id: str, item: Dict, section_type: str) -> str:
        """Create or update AgendaItem node with all metadata."""
        # Store both original and normalized codes
        original_code = item.get('item_code', '')
        normalized_code = self.normalize_item_code(original_code)
        
        properties = {
            'code': normalized_code,
            'original_code': original_code,
            'title': item.get('title', 'Unknown'),
            'type': item.get('item_type', 'Item'),
            'section_type': section_type
        }
        
        # Add page range if available
        if 'page_start' in item:
            properties['page_start'] = item['page_start']
        if 'page_end' in item:
            properties['page_end'] = item['page_end']
        
        # Add summary if available
        if item.get('summary'):
            properties['summary'] = item['summary'][:500]
        
        # Add document reference and URL if available
        if item.get('document_reference'):
            properties['document_reference'] = item['document_reference']
        
        if item.get('document_url'):
            properties['document_url'] = item['document_url']
            properties['has_hyperlink'] = True
        
        # Add all hyperlinks as JSON if multiple exist
        if item.get('hyperlinks') and len(item['hyperlinks']) > 0:
            properties['hyperlinks_json'] = json.dumps(item['hyperlinks'])
        
        if self.upsert_mode:
            created = await self.cosmos.upsert_vertex('AgendaItem', item_id, properties)
            if created:
                self.stats['nodes_created'] += 1
            else:
                self.stats['nodes_updated'] += 1
        else:
            await self.cosmos.create_vertex('AgendaItem', item_id, properties)
            self.stats['nodes_created'] += 1
        
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
    
    async def _create_entity_nodes(self, entities: Dict[str, List[Dict]], meeting_id: str) -> int:
        """Create nodes for all extracted entities."""
        entity_counts = 0
        
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
                entity_counts += 1
        
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
                entity_counts += 1
        
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
                entity_counts += 1
        
        # Create FinancialItem nodes
        for amount in entities.get('monetary_amounts', []):
            if amount.get('amount'):
                fin_id = await self._create_financial_node(
                    amount['amount'],
                    amount.get('purpose', ''),
                    meeting_id
                )
                entity_counts += 1
        
        return entity_counts
    
    async def _ensure_person_node(self, name: str, role: str) -> str:
        """Create or retrieve person node with upsert support."""
        clean_name = name.strip()
        # Clean the ID by removing invalid characters
        cleaned_id_part = clean_name.lower().replace(' ', '-').replace('.', '').replace("'", '').replace('"', '').replace('/', '-')
        person_id = f"person-{cleaned_id_part}"
        
        # Check cache first
        if person_id in self.entity_id_cache:
            return person_id
        
        # Check if exists in database
        if await self.cosmos.vertex_exists(person_id):
            self.entity_id_cache[person_id] = True
            return person_id
        
        # Create new person
        properties = {
            'name': clean_name,
            'roles': role
        }
        
        if self.upsert_mode:
            created = await self.cosmos.upsert_vertex('Person', person_id, properties)
            if created:
                self.stats['nodes_created'] += 1
            else:
                self.stats['nodes_updated'] += 1
        else:
            if not await self.cosmos.vertex_exists(person_id):
                await self.cosmos.create_vertex('Person', person_id, properties)
                self.stats['nodes_created'] += 1
        
        self.entity_id_cache[person_id] = True
        return person_id
    
    async def _ensure_organization_node(self, name: str, org_type: str) -> str:
        """Create or retrieve organization node."""
        # Clean the ID
        cleaned_org_name = name.lower().replace(' ', '-').replace('.', '').replace("'", '').replace('"', '').replace('/', '-').replace(',', '')
        org_id = f"org-{cleaned_org_name}"
        
        if org_id in self.entity_id_cache:
            return org_id
        
        if await self.cosmos.vertex_exists(org_id):
            self.entity_id_cache[org_id] = True
            return org_id
        
        properties = {
            'name': name,
            'type': org_type
        }
        
        await self.cosmos.create_vertex('Organization', org_id, properties)
        self.entity_id_cache[org_id] = True
        return org_id
    
    async def _ensure_location_node(self, name: str, address: str, loc_type: str) -> str:
        """Create or retrieve location node."""
        # Clean the ID
        cleaned_loc_name = name.lower().replace(' ', '-').replace('.', '').replace("'", '').replace('"', '').replace('/', '-').replace(',', '')
        loc_id = f"location-{cleaned_loc_name}"
        
        if loc_id in self.entity_id_cache:
            return loc_id
        
        if await self.cosmos.vertex_exists(loc_id):
            self.entity_id_cache[loc_id] = True
            return loc_id
        
        properties = {
            'name': name,
            'address': address,
            'type': loc_type
        }
        
        await self.cosmos.create_vertex('Location', loc_id, properties)
        self.entity_id_cache[loc_id] = True
        return loc_id
    
    async def _create_financial_node(self, amount: str, purpose: str, meeting_id: str) -> str:
        """Create financial item node."""
        fin_id = f"financial-{hashlib.md5(f'{amount}-{purpose}'.encode()).hexdigest()[:8]}"
        
        properties = {
            'amount': amount,
            'purpose': purpose
        }
        
        await self.cosmos.create_vertex('FinancialItem', fin_id, properties)
        
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
        from_code = self.normalize_item_code(rel['from_code'])
        to_code = self.normalize_item_code(rel['to_code'])
        
        from_id = f"item-{meeting_date}-{from_code}"
        to_id = f"item-{meeting_date}-{to_code}"
        
        # Check if both items exist
        try:
            from_exists = await self.cosmos.vertex_exists(from_id)
            to_exists = await self.cosmos.vertex_exists(to_id)
            
            if from_exists and to_exists:
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
    
    async def process_linked_documents(self, linked_docs: Dict, meeting_id: str, meeting_date: str):
        """Process and create nodes for linked documents with FIXED date format."""
        # CRITICAL FIX: Extract US date format from meeting_id
        # meeting_id is ALWAYS "meeting-01-23-2024" format
        date_from_meeting_id = meeting_id.replace("meeting-", "")  # "01-23-2024"
        
        log.info(f"\n🔗 Processing linked documents")
        log.info(f"   Meeting ID: {meeting_id}")
        log.info(f"   Date format for items: {date_from_meeting_id}")
        
        missing_items = []
        created_count = 0
        
        for doc_type, documents in linked_docs.items():
            log.info(f"\n📄 Processing {len(documents)} {doc_type}")
            
            for doc in documents:
                if doc_type in ['ordinances', 'resolutions']:
                    log.info(f"\n   Processing {doc_type[:-1]} {doc.get('document_number', 'unknown')}")
                    log.info(f"      Item code: {doc.get('item_code', 'MISSING')}")
                    
                    # Create document node
                    doc_id = await self._create_document_node(doc, doc_type, meeting_date)
                    
                    if doc_id:
                        created_count += 1
                        log.info(f"      ✅ Created document node: {doc_id}")
                        
                        # Link to meeting
                        await self.cosmos.create_edge(
                            from_id=doc_id,
                            to_id=meeting_id,
                            edge_type='PRESENTED_AT',
                            properties={'date': meeting_date}
                        )
                        
                        # Check if agenda item exists
                        if doc.get('item_code'):
                            # Normalize the item code
                            normalized_code = self.normalize_item_code(doc['item_code'])
                            
                            # USE THE FIXED DATE FORMAT
                            item_id = f"item-{date_from_meeting_id}-{normalized_code}"
                            
                            log.info(f"      Looking for item: {item_id}")
                            
                            # Check if item exists
                            item_exists = await self.cosmos.vertex_exists(item_id)
                            
                            if not item_exists:
                                log.warning(f"      ❌ Agenda item NOT FOUND: {item_id}")
                                
                                missing_item_info = {
                                    'item_code': normalized_code,
                                    'original_code': doc['item_code'],
                                    'document_number': doc.get('document_number'),
                                    'document_type': doc_type,
                                    'title': doc.get('title', 'Unknown'),
                                    'expected_item_id': item_id
                                }
                                missing_items.append(missing_item_info)
                            else:
                                log.info(f"      ✅ Agenda item found, creating link")
                                # Item exists, create the edge
                                await self.cosmos.create_edge(
                                    from_id=item_id,
                                    to_id=doc_id,
                                    edge_type='REFERENCES_DOCUMENT',
                                    properties={'document_type': doc_type}
                                )
                                log.info(f"      ✅ Linked agenda item to document")
        
        log.info(f"\n📊 Document processing complete")
        log.info(f"   Documents created: {created_count}")
        log.info(f"   Missing items: {len(missing_items)}")
        
        return missing_items

    async def _create_document_node(self, doc_info: Dict, doc_type: str, meeting_date: str) -> str:
        """Create or update an Ordinance or Resolution node."""
        doc_number = doc_info.get('document_number', 'unknown')
        doc_id = f"{doc_type[:-1]}-{doc_number}"  # Remove 's' from type
        
        # Get full title without truncation
        title = doc_info.get('title', '')
        if not title and doc_info.get('parsed_data', {}).get('title'):
            title = doc_info['parsed_data']['title']
        
        if title is None:
            title = f"Untitled {doc_type.capitalize()} {doc_number}"
            log.warning(f"No title found for {doc_type} {doc_number}, using default")
        
        properties = {
            'document_number': doc_number,
            'full_title': title,
            'title': title[:200] if len(title) > 200 else title,
            'document_type': doc_type[:-1],  # Remove 's'
            'meeting_date': meeting_date
        }
        
        # Add parsed metadata
        parsed_data = doc_info.get('parsed_data', {})
        
        if parsed_data.get('date_passed'):
            properties['date_passed'] = parsed_data['date_passed']
        
        if parsed_data.get('agenda_item'):
            properties['agenda_item'] = parsed_data['agenda_item']
        
        # Add vote details as JSON
        if parsed_data.get('vote_details'):
            properties['vote_details'] = json.dumps(parsed_data['vote_details'])
        
        # Add signatories
        if parsed_data.get('signatories', {}).get('mayor'):
            properties['mayor_signature'] = parsed_data['signatories']['mayor']
        
        if self.upsert_mode:
            created = await self.cosmos.upsert_vertex(doc_type[:-1].capitalize(), doc_id, properties)
            if created:
                self.stats['nodes_created'] += 1
                log.info(f"✅ Created document node: {doc_id}")
            else:
                self.stats['nodes_updated'] += 1
                log.info(f"📝 Updated document node: {doc_id}")
        else:
            await self.cosmos.create_vertex(doc_type[:-1].capitalize(), doc_id, properties)
            self.stats['nodes_created'] += 1
        
        # Create edges for sponsors
        if parsed_data.get('motion', {}).get('moved_by'):
            person_id = await self._ensure_person_node(
                parsed_data['motion']['moved_by'], 
                'Commissioner'
            )
            if await self.cosmos.create_edge_if_not_exists(person_id, doc_id, 'MOVED'):
                self.stats['edges_created'] += 1
            else:
                self.stats['edges_skipped'] += 1
        
        return doc_id 