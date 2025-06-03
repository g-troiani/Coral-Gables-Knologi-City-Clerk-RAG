"""
Enhanced Agenda Graph Builder - RICH VERSION
Builds comprehensive graph representation from LLM-extracted agenda ontology.
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
    """Build comprehensive graph representation from rich agenda ontology."""
    
    def __init__(self, cosmos_client: CosmosGraphClient, upsert_mode: bool = True):
        self.cosmos = cosmos_client
        self.upsert_mode = upsert_mode
        self.entity_id_cache = {}  # Cache for entity IDs
        self.partition_value = 'demo'  # Partition value property
        
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

    async def build_graph(self, ontology_file: Path, linked_docs: Optional[Dict] = None, upsert: bool = True) -> Dict:
        """Build graph from ontology file - main entry point."""
        # Load ontology
        with open(ontology_file, 'r', encoding='utf-8') as f:
            ontology = json.load(f)
        
        return await self.build_graph_from_ontology(ontology, ontology_file, linked_docs)

    async def build_graph_from_ontology(self, ontology: Dict, source_path: Path, linked_docs: Optional[Dict] = None) -> Dict:
        """Build comprehensive graph representation from rich ontology."""
        log.info(f"🔨 Starting enhanced graph build for {source_path.name}")
        log.info(f"🔧 Upsert mode: {'ENABLED' if self.upsert_mode else 'DISABLED'}")
        
        # Reset statistics
        self.stats = {
            'nodes_created': 0,
            'nodes_updated': 0,
            'edges_created': 0,
            'edges_skipped': 0
        }
        
        try:
            graph_data = {
                'nodes': {},
                'edges': [],
                'statistics': {}
            }
            
            # CRITICAL: Ensure meeting date is in US format
            meeting_date_original = ontology['meeting_date']
            meeting_date_us = self.ensure_us_date_format(meeting_date_original)
            meeting_info = ontology['meeting_info']
            
            log.info(f"📅 Meeting date: {meeting_date_original} -> {meeting_date_us}")
            
            # 1. Create Meeting node as the root
            meeting_id = f"meeting-{meeting_date_us}"
            await self._create_meeting_node(meeting_date_us, meeting_info, source_path.name)
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
            await self._create_official_nodes(meeting_info, meeting_id)
            
            # 3. Process sections and agenda items
            section_count = 0
            item_count = 0
            
            sections = ontology.get('sections', [])
            log.info(f"📑 Processing {len(sections)} sections")
            
            for section_idx, section in enumerate(sections):
                try:
                    section_count += 1
                    section_id = f"section-{meeting_date_us}-{section_idx}"
                    
                    # Create Section node
                    await self._create_section_node(section_id, section, section_idx)
                    log.info(f"✅ Created section {section_idx}: {section.get('section_name', 'Unknown')}")
                    
                    graph_data['nodes'][section_id] = {
                        'type': 'Section',
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
                            
                            # Create enhanced AgendaItem node
                            await self._create_enhanced_agenda_item_node(item_id, item, section)
                            
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
                            
                            # Create rich relationships for this item
                            await self._create_item_relationships(item, item_id, meeting_date_us)
                            
                            # Create URL nodes and relationships
                            await self._create_url_relationships(item, item_id)
                                
                        except Exception as e:
                            log.error(f"Failed to process item {item.get('item_code', 'unknown')}: {e}")
                            
                except Exception as e:
                    log.error(f"Failed to process section {section.get('section_name', 'unknown')}: {e}")
            
            # 4. Create entity nodes from extracted entities
            entity_count = await self._create_entity_nodes(ontology.get('entities', []), meeting_id)
            
            # 5. Create relationships from ontology
            relationship_count = 0
            for rel in ontology.get('relationships', []):
                try:
                    await self._create_ontology_relationship(rel, meeting_date_us)
                    relationship_count += 1
                except Exception as e:
                    log.error(f"Failed to create relationship: {e}")
            
            # 6. Process linked documents if available
            if linked_docs:
                await self.process_linked_documents(linked_docs, meeting_id, meeting_date_us)
            
            # Update statistics
            graph_data['statistics'] = {
                'sections': section_count,
                'items': item_count, 
                'entities': entity_count,
                'relationships': relationship_count,
                'meeting_date': meeting_date_us
            }
            
            log.info(f"🎉 Enhanced graph build complete for {source_path.name}")
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
        
        # Handle location - could be string or dict
        location = meeting_info.get('location', 'City Commission Chambers')
        if isinstance(location, dict):
            location_str = f"{location.get('name', 'City Commission Chambers')}"
            if location.get('address'):
                location_str += f" - {location['address']}"
        else:
            location_str = str(location) if location else "City Commission Chambers"
        
        properties = {
            'nodeType': 'Meeting',
            'date': meeting_date,
            'type': meeting_info.get('type', 'Regular Meeting'),
            'time': meeting_info.get('time', '5:30 PM'),
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
        """Create or update Section node."""
        properties = {
            'nodeType': 'Section',
            'title': section.get('section_name', 'Unknown'),
            'type': section.get('section_type', 'OTHER'),
            'description': section.get('description', ''),
            'order': order
        }
        
        if self.upsert_mode:
            created = await self.cosmos.upsert_vertex('Section', section_id, properties)
            if created:
                self.stats['nodes_created'] += 1
            else:
                self.stats['nodes_updated'] += 1
        else:
            if await self.cosmos.vertex_exists(section_id):
                log.info(f"Section {section_id} already exists, skipping creation")
                return section_id
            await self.cosmos.create_vertex('Section', section_id, properties)
            self.stats['nodes_created'] += 1
        
        return section_id
    
    async def _create_enhanced_agenda_item_node(self, item_id: str, item: Dict, section: Dict) -> str:
        """Create or update AgendaItem node with rich metadata from LLM extraction."""
        # Store both original and normalized codes
        original_code = item.get('item_code', '')
        normalized_code = self.normalize_item_code(original_code)
        
        properties = {
            'nodeType': 'AgendaItem',
            'code': normalized_code,
            'original_code': original_code,
            'title': item.get('title', 'Unknown'),
            'type': item.get('item_type', 'Item'),
            'section': section.get('section_name', 'Unknown'),
            'section_type': section.get('section_type', 'other')
        }
        
        # Add enhanced details from LLM extraction
        if item.get('description'):
            properties['description'] = item['description'][:500]  # Limit length
        
        if item.get('document_reference'):
            properties['document_reference'] = item['document_reference']
        
        # Add sponsors as JSON array
        if item.get('sponsors'):
            properties['sponsors_json'] = json.dumps(item['sponsors'])
        
        # Add departments as JSON array  
        if item.get('departments'):
            properties['departments_json'] = json.dumps(item['departments'])
        
        # Add actions as JSON array
        if item.get('actions'):
            properties['actions_json'] = json.dumps(item['actions'])
        
        # Add stakeholders as JSON array
        if item.get('stakeholders'):
            properties['stakeholders_json'] = json.dumps(item['stakeholders'])
        
        # Add URLs as JSON array
        if item.get('urls'):
            properties['urls_json'] = json.dumps(item['urls'])
            properties['has_urls'] = True
        
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
    
    async def _create_official_nodes(self, meeting_info: Dict, meeting_id: str):
        """Create nodes for city officials and link to meeting."""
        officials = meeting_info.get('officials', {})
        commissioners = meeting_info.get('commissioners', [])
        
        # Create official nodes
        for role, name in officials.items():
            if name and name != 'null':
                person_id = await self._ensure_person_node(name, role.replace('_', ' ').title())
                await self.cosmos.create_edge_if_not_exists(
                    from_id=person_id,
                    to_id=meeting_id,
                    edge_type='ATTENDED',
                    properties={'role': role.replace('_', ' ').title()}
                )
        
        # Create commissioner nodes
        for idx, commissioner in enumerate(commissioners):
            if commissioner and commissioner != 'null':
                person_id = await self._ensure_person_node(commissioner, 'Commissioner')
                await self.cosmos.create_edge_if_not_exists(
                    from_id=person_id,
                    to_id=meeting_id,
                    edge_type='ATTENDED',
                    properties={'role': 'Commissioner', 'seat': idx + 1}
                )
    
    async def _create_entity_nodes(self, entities: List[Dict], meeting_id: str) -> int:
        """Create nodes for all extracted entities."""
        entity_count = 0
        
        for entity in entities:
            try:
                entity_type = entity.get('type', 'unknown')
                entity_name = entity.get('name', '')
                entity_role = entity.get('role', '')
                entity_context = entity.get('context', '')
                
                if not entity_name:
                    continue
                
                if entity_type == 'person':
                    person_id = await self._ensure_person_node(entity_name, entity_role)
                    await self.cosmos.create_edge_if_not_exists(
                        from_id=person_id,
                        to_id=meeting_id,
                        edge_type='MENTIONED_IN',
                        properties={
                            'context': entity_context[:100],
                            'role': entity_role
                        }
                    )
                    entity_count += 1
                    
                elif entity_type == 'organization':
                    org_id = await self._ensure_organization_node(entity_name, entity_role)
                    await self.cosmos.create_edge_if_not_exists(
                        from_id=org_id,
                        to_id=meeting_id,
                        edge_type='MENTIONED_IN',
                        properties={
                            'context': entity_context[:100],
                            'org_type': entity_role
                        }
                    )
                    entity_count += 1
                    
                elif entity_type == 'department':
                    dept_id = await self._ensure_department_node(entity_name)
                    await self.cosmos.create_edge_if_not_exists(
                        from_id=dept_id,
                        to_id=meeting_id,
                        edge_type='INVOLVED_IN',
                        properties={'context': entity_context[:100]}
                    )
                    entity_count += 1
                    
                elif entity_type == 'location':
                    loc_id = await self._ensure_location_node(entity_name, entity_context)
                    await self.cosmos.create_edge_if_not_exists(
                        from_id=loc_id,
                        to_id=meeting_id,
                        edge_type='REFERENCED_IN',
                        properties={'context': entity_context[:100]}
                    )
                    entity_count += 1
                    
            except Exception as e:
                log.error(f"Failed to create entity node for {entity}: {e}")
        
        return entity_count
    
    async def _create_item_relationships(self, item: Dict, item_id: str, meeting_date: str):
        """Create rich relationships for agenda items."""
        
        # Sponsor relationships
        for sponsor in item.get('sponsors', []):
            try:
                person_id = await self._ensure_person_node(sponsor, 'Commissioner')
                await self.cosmos.create_edge_if_not_exists(
                    from_id=person_id,
                    to_id=item_id,
                    edge_type='SPONSORS',
                    properties={'role': 'sponsor'}
                )
                self.stats['edges_created'] += 1
            except Exception as e:
                log.error(f"Failed to create sponsor relationship: {e}")
        
        # Department relationships
        for dept in item.get('departments', []):
            try:
                dept_id = await self._ensure_department_node(dept)
                await self.cosmos.create_edge_if_not_exists(
                    from_id=dept_id,
                    to_id=item_id,
                    edge_type='RESPONSIBLE_FOR',
                    properties={'role': 'responsible_department'}
                )
                self.stats['edges_created'] += 1
            except Exception as e:
                log.error(f"Failed to create department relationship: {e}")
        
        # Stakeholder relationships  
        for stakeholder in item.get('stakeholders', []):
            try:
                org_id = await self._ensure_organization_node(stakeholder, 'Stakeholder')
                await self.cosmos.create_edge_if_not_exists(
                    from_id=org_id,
                    to_id=item_id,
                    edge_type='INVOLVED_IN',
                    properties={'role': 'stakeholder'}
                )
                self.stats['edges_created'] += 1
            except Exception as e:
                log.error(f"Failed to create stakeholder relationship: {e}")
        
        # Action relationships
        for action in item.get('actions', []):
            try:
                action_id = await self._ensure_action_node(action)
                await self.cosmos.create_edge_if_not_exists(
                    from_id=item_id,
                    to_id=action_id,
                    edge_type='REQUIRES_ACTION',
                    properties={'action_type': action}
                )
                self.stats['edges_created'] += 1
            except Exception as e:
                log.error(f"Failed to create action relationship: {e}")
    
    async def _create_url_relationships(self, item: Dict, item_id: str):
        """Create URL nodes and link to agenda items."""
        for url in item.get('urls', []):
            try:
                url_id = await self._ensure_url_node(url)
                await self.cosmos.create_edge_if_not_exists(
                    from_id=item_id,
                    to_id=url_id,
                    edge_type='HAS_URL',
                    properties={'url_type': 'document_link'}
                )
                self.stats['edges_created'] += 1
            except Exception as e:
                log.error(f"Failed to create URL relationship: {e}")
    
    async def _create_ontology_relationship(self, rel: Dict, meeting_date: str):
        """Create relationship from ontology data."""
        try:
            source = rel.get('source', '')
            target = rel.get('target', '')
            relationship = rel.get('relationship', '')
            source_type = rel.get('source_type', '')
            target_type = rel.get('target_type', '')
            
            # Determine source and target IDs based on type
            if source_type == 'person':
                source_id = await self._ensure_person_node(source, 'Participant')
            elif source_type == 'department':
                source_id = await self._ensure_department_node(source)
            elif source_type == 'organization':
                source_id = await self._ensure_organization_node(source, 'Organization')
            else:
                log.warning(f"Unknown source type: {source_type}")
                return
            
            if target_type == 'agenda_item':
                # Normalize target agenda item code
                normalized_target = self.normalize_item_code(target)
                target_id = f"item-{meeting_date}-{normalized_target}"
            else:
                log.warning(f"Unknown target type: {target_type}")
                return
            
            # Create the relationship
            await self.cosmos.create_edge_if_not_exists(
                from_id=source_id,
                to_id=target_id,
                edge_type=relationship.upper(),
                properties={
                    'source_type': source_type,
                    'target_type': target_type
                }
            )
            
        except Exception as e:
            log.error(f"Failed to create ontology relationship: {e}")
    
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
            'nodeType': 'Person',
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
            'nodeType': 'Organization',
            'name': name,
            'type': org_type
        }
        
        await self.cosmos.create_vertex('Organization', org_id, properties)
        self.entity_id_cache[org_id] = True
        self.stats['nodes_created'] += 1
        return org_id
    
    async def _ensure_department_node(self, name: str) -> str:
        """Create or retrieve department node."""
        cleaned_dept_name = name.lower().replace(' ', '-').replace('.', '').replace("'", '').replace('"', '').replace('/', '-')
        dept_id = f"dept-{cleaned_dept_name}"
        
        if dept_id in self.entity_id_cache:
            return dept_id
        
        if await self.cosmos.vertex_exists(dept_id):
            self.entity_id_cache[dept_id] = True
            return dept_id
        
        properties = {
            'nodeType': 'Department', 
            'name': name,
            'type': 'CityDepartment'
        }
        
        await self.cosmos.create_vertex('Department', dept_id, properties)
        self.entity_id_cache[dept_id] = True
        self.stats['nodes_created'] += 1
        return dept_id
    
    async def _ensure_location_node(self, name: str, context: str = '') -> str:
        """Create or retrieve location node."""
        cleaned_loc_name = name.lower().replace(' ', '-').replace('.', '').replace("'", '').replace('"', '').replace('/', '-').replace(',', '')
        loc_id = f"location-{cleaned_loc_name}"
        
        if loc_id in self.entity_id_cache:
            return loc_id
        
        if await self.cosmos.vertex_exists(loc_id):
            self.entity_id_cache[loc_id] = True
            return loc_id
        
        properties = {
            'nodeType': 'Location',
            'name': name,
            'context': context[:200] if context else '',
            'type': 'Location'
        }
        
        await self.cosmos.create_vertex('Location', loc_id, properties)
        self.entity_id_cache[loc_id] = True
        self.stats['nodes_created'] += 1
        return loc_id
    
    async def _ensure_action_node(self, action: str) -> str:
        """Create or retrieve action node."""
        cleaned_action = action.lower().replace(' ', '-').replace('.', '')
        action_id = f"action-{cleaned_action}"
        
        if action_id in self.entity_id_cache:
            return action_id
        
        if await self.cosmos.vertex_exists(action_id):
            self.entity_id_cache[action_id] = True
            return action_id
        
        properties = {
            'nodeType': 'Action',
            'name': action,
            'type': 'RequiredAction'
        }
        
        await self.cosmos.create_vertex('Action', action_id, properties)
        self.entity_id_cache[action_id] = True
        self.stats['nodes_created'] += 1
        return action_id
    
    async def _ensure_url_node(self, url: str) -> str:
        """Create or retrieve URL node."""
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        url_id = f"url-{url_hash}"
        
        if url_id in self.entity_id_cache:
            return url_id
        
        if await self.cosmos.vertex_exists(url_id):
            self.entity_id_cache[url_id] = True
            return url_id
        
        properties = {
            'nodeType': 'URL',
            'url': url,
            'domain': url.split('/')[2] if '://' in url else 'unknown',
            'type': 'Hyperlink'
        }
        
        await self.cosmos.create_vertex('URL', url_id, properties)
        self.entity_id_cache[url_id] = True
        self.stats['nodes_created'] += 1
        return url_id

    async def process_linked_documents(self, linked_docs: Dict, meeting_id: str, meeting_date: str):
        """Process and create nodes for linked documents."""
        log.info("📄 Processing linked documents...")
        
        created_count = 0
        missing_items = []
        
        for doc_type, docs in linked_docs.items():
            if not docs:
                continue
                
            log.info(f"\n   📂 Processing {len(docs)} {doc_type}")
            
            for doc in docs:
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
                        
                        # Try to link to agenda item if item_code exists
                        item_code = doc.get('item_code')
                        if item_code:
                            normalized_code = self.normalize_item_code(item_code)
                            item_id = f"item-{meeting_date}-{normalized_code}"
                            
                            # Check if agenda item exists
                            if await self.cosmos.vertex_exists(item_id):
                                await self.cosmos.create_edge(
                                    from_id=item_id,
                                    to_id=doc_id,
                                    edge_type='REFERENCES_DOCUMENT',
                                    properties={'document_type': doc_type[:-1]}
                                )
                                log.info(f"      🔗 Linked to agenda item: {item_id}")
                            else:
                                log.warning(f"      ❌ Agenda item not found: {item_id}")
                                missing_items.append({
                                    'document_number': doc.get('document_number'),
                                    'item_code': item_code,
                                    'normalized_code': normalized_code,
                                    'expected_item_id': item_id
                                })
                        else:
                            log.warning(f"      ⚠️  No item_code found for {doc.get('document_number')}")
        
        log.info(f"📄 Document processing complete: {created_count} documents created")
        if missing_items:
            log.warning(f"⚠️  {len(missing_items)} documents could not be linked to agenda items")
        
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
            'nodeType': doc_type[:-1].capitalize(),  # 'Ordinance' or 'Resolution'
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