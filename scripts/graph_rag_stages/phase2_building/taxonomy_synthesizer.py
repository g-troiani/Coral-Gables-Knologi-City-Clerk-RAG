"""
Synthesizes taxonomy entities from JSON extraction output into NER-compatible format.
Writes to simple_ner_graph/registry/ directory maintaining exact same structure as NER.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import asyncio

from scripts.graph_rag_stages.common.graph_entity_toolkit import GraphEntityToolkit
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology

log = logging.getLogger(__name__)


class TaxonomySynthesizer:
    """Synthesizes taxonomy entities from JSON into NER-compatible format."""
    
    def __init__(self, output_dir: Path, toolkit: GraphEntityToolkit = None):
        """
        Initialize synthesizer.
        
        Args:
            output_dir: Base directory (e.g., simple_ner_graph)
            toolkit: GraphEntityToolkit instance
        """
        self.output_dir = Path(output_dir)
        self.registry_dir = self.output_dir / "registry"
        self.toolkit = toolkit or GraphEntityToolkit()
        
        # Create registry directories for each entity type
        for entity_type in UnifiedOntology.get_entity_categories():
            (self.registry_dir / entity_type).mkdir(parents=True, exist_ok=True)
        
        # Create relationships directory
        (self.registry_dir / "relationships").mkdir(parents=True, exist_ok=True)
        
        # Track what we've created to avoid duplicates
        self.created_entities = {}
        self.created_relationships = []
    
    async def synthesize_from_json(self, json_dir: Path) -> Dict[str, int]:
        """
        Read ontology JSON files and synthesize entities/relationships.
        
        Args:
            json_dir: Directory containing extracted JSON files
            
        Returns:
            Statistics of created entities by type
        """
        log.info(f"🔄 Synthesizing taxonomy from {json_dir}")
        
        stats = {}
        
        # Process agenda files
        agenda_dir = json_dir / "agenda"
        if agenda_dir.exists():
            agenda_files = list(agenda_dir.glob("agenda_*.json"))
            log.info(f"Found {len(agenda_files)} agenda files")
            
            for agenda_file in agenda_files:
                await self._process_agenda_file(agenda_file)
        
        # Process legal documents
        legal_dir = json_dir / "legal"
        if legal_dir.exists():
            legal_files = list(legal_dir.glob("*_enhanced_*.json"))
            log.info(f"Found {len(legal_files)} legal documents")
            
            for legal_file in legal_files:
                await self._process_legal_file(legal_file)
        
        # Save all entities and relationships
        await self._save_all_entities()
        
        # Calculate statistics
        for entity_type, entities in self.created_entities.items():
            stats[entity_type] = len(entities)
        stats['relationships'] = len(self.created_relationships)
        
        log.info(f"✅ Synthesized: {stats}")
        return stats
    
    async def _process_agenda_file(self, agenda_file: Path) -> None:
        """Process an agenda JSON file to extract taxonomy entities."""
        try:
            with open(agenda_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Log the actual structure
            log.info(f"📋 Agenda JSON keys: {list(data.keys())}")
            
            # ADD DEBUG LOGGING
            log.info(f"📋 Processing agenda with meeting_date: {data.get('meeting_date')}")
            log.info(f"   Sections found: {len(data.get('sections', []))}")
            
            # Check if sections exist and are not empty
            sections = data.get('sections', [])
            if not sections:
                log.warning(f"⚠️ No sections found in {agenda_file.name}")
                log.info(f"   Available keys: {list(data.keys())}")
                return
            
            meeting_date = data.get('meeting_date', 'unknown')
            doc_id = data.get('doc_id', agenda_file.stem)
            source_file = data.get('Source_File_Name', agenda_file.name)
            
            # Create Meeting entity
            meeting_id = self._create_entity(
                'Event',
                {
                    'name': f"City Commission Meeting {meeting_date}",
                    'type': 'Regular Meeting',
                    'dateTime': meeting_date,
                    'status': 'Completed',
                    'outcome': 'Adjourned'
                },
                source=f"taxonomy_{agenda_file.stem}"
            )
            log.info(f"   Created Event: {meeting_id}")
            
            # Find existing NER document ID for this agenda instead of creating new one
            doc_entity_id = self._find_existing_document_id(meeting_date, 'agenda')
            
            if not doc_entity_id:
                # Fallback: create with specific ID pattern to match NER
                doc_entity_id = f"document_Agenda_{meeting_date.replace('.', '_')}"
                
                # Store the document entity for potential merging
                doc_entity = {
                    'documentID': doc_entity_id,
                    'name': f"Agenda {meeting_date}.pdf",
                    'title': f"Agenda {meeting_date}",
                    'document_type': 'agenda',
                    'type': 'agenda',
                    'status': 'Final',
                    'issueDate': meeting_date,
                    'sourceURL': data.get('hyperlinks', [{}])[0].get('url', '') if data.get('hyperlinks') else None,
                    'summary': f"Agenda document from {meeting_date}",
                    'version': '1.0',
                    '_source': f"taxonomy_{agenda_file.stem}",
                    '_created_at': datetime.now().isoformat()
                }
                
                # Store entity directly (don't use _create_entity to avoid ID generation)
                if 'Document' not in self.created_entities:
                    self.created_entities['Document'] = {}
                self.created_entities['Document'][doc_entity_id] = doc_entity
            
            # Process sections
            for section in data.get('sections', []):
                section_name = section.get('section_name', '')
                log.info(f"   Processing section: {section_name}")
                
                # Create Topic entity for section
                topic_id = self._create_entity(
                    'Topic',
                    {
                        'name': section_name,
                        'category': 'Meeting Section',
                        'description': f"Section {section.get('section_order', 0)}"
                    },
                    source=f"taxonomy_{agenda_file.stem}"
                )
                
                # Link document to topic
                self._create_relationship(
                    'addressesTopic',
                    doc_entity_id,
                    topic_id,
                    {'section_order': section.get('section_order', 0)}
                )
                
                # Process items in section
                items = section.get('items', [])
                log.info(f"      Found {len(items)} items in section")
                
                for item in items:
                    item_code = item.get('item_code', '')
                    log.info(f"      Processing agenda item: {item_code}")
                    
                    # Create AgendaItem entity
                    agenda_item_id = self._create_entity(
                        'AgendaItem',
                        {
                            'itemID': item_code,
                            'title': item.get('title', ''),
                            'type': item.get('type', ''),
                            'presenter': item.get('presenter'),
                            'estimatedDuration': item.get('estimatedDuration')
                        },
                        source=f"taxonomy_{agenda_file.stem}"
                    )
                    log.info(f"      Created AgendaItem: {agenda_item_id}")
                    
                    # Link agenda item to its agenda document (AgendaItem -> Document)
                    self._create_relationship(
                        'isPartOf',
                        agenda_item_id,
                        doc_entity_id,
                        {}
                    )
                    
                    # Link event to agenda item
                    self._create_relationship(
                        'discusses',
                        meeting_id,
                        agenda_item_id,
                        {'order': item.get('item_order', 0)}
                    )
            
            # Process entities (ordinances, resolutions, etc.)
            for entity in data.get('entities', []):
                entity_type = entity.get('type', '')
                
                if entity_type in ['ORDINANCE', 'RESOLUTION']:
                    # Create Policy entity
                    policy_id = self._create_entity(
                        'Policy',
                        {
                            'title': entity.get('name', ''),
                            'status': 'Proposed',
                            'effectiveDate': meeting_date,
                            'legalReferences': []
                        },
                        source=f"taxonomy_{agenda_file.stem}"
                    )
                    
                    # Process vote details
                    vote_details = entity.get('vote_details', {})
                    if vote_details:
                        # Create VoteOutcome entity
                        outcome_id = self._create_entity(
                            'VoteOutcome',
                            {
                                'agendaItemID': entity.get('related_item', ''),
                                'status': 'passed' if vote_details.get('yeas', 0) > vote_details.get('nays', 0) else 'failed',
                                'yesVotes': vote_details.get('yeas', 0),
                                'noVotes': vote_details.get('nays', 0),
                                'abstentions': 0,
                                'voteDetails': []
                            },
                            source=f"taxonomy_{agenda_file.stem}"
                        )
                        
                        # Link outcome to policy
                        self._create_relationship(
                            'votedOn',
                            outcome_id,
                            policy_id,
                            {}
                        )
                    
                    # Process motion details
                    motion = entity.get('motion', {})
                    if motion.get('moved_by'):
                        # Create Person entity for mover
                        person_id = self._create_entity(
                            'Person',
                            {
                                'name': motion['moved_by'],
                                'title': 'Commissioner',
                                'affiliation': 'City Council',
                                'contactInfo': None
                            },
                            source=f"taxonomy_{agenda_file.stem}"
                        )
                        
                        # Create sponsors relationship
                        self._create_relationship(
                            'sponsors',
                            person_id,
                            policy_id,
                            {'sponsorshipType': 'primary'}
                        )
                    
                    if motion.get('seconded_by'):
                        # Create Person entity for seconder
                        person_id = self._create_entity(
                            'Person',
                            {
                                'name': motion['seconded_by'],
                                'title': 'Commissioner',
                                'affiliation': 'City Council',
                                'contactInfo': None
                            },
                            source=f"taxonomy_{agenda_file.stem}"
                        )
                        
                        # Create sponsors relationship
                        self._create_relationship(
                            'sponsors',
                            person_id,
                            policy_id,
                            {'sponsorshipType': 'secondary'}
                        )
            
        except Exception as e:
            log.error(f"Error processing agenda file {agenda_file}: {e}")
    
    async def _process_legal_file(self, legal_file: Path) -> None:
        """Process an enhanced legal document JSON file."""
        try:
            with open(legal_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            doc_type = data.get('document_type', 'ordinance')
            doc_number = data.get('document_number', legal_file.stem)
            
            # Create Policy entity
            policy_id = self._create_entity(
                'Policy',
                {
                    'title': data.get('title', f"{doc_type} {doc_number}"),
                    'status': data.get('status', 'Enacted'),
                    'effectiveDate': data.get('effective_date'),
                    'expirationDate': data.get('expiration_date'),
                    'legalReferences': data.get('references', [])
                },
                source=f"taxonomy_{legal_file.stem}"
            )
            
            # Create Document entity
            doc_id = self._create_entity(
                'Document',
                {
                    'title': data.get('title', ''),
                    'type': doc_type,
                    'status': 'Final',
                    'issueDate': data.get('adoption_date'),
                    'sourceURL': None
                },
                source=f"taxonomy_{legal_file.stem}"
            )
            
            # Link document to policy
            self._create_relationship(
                'references',
                doc_id,
                policy_id,
                {}
            )
            
            # Process sponsors
            for sponsor in data.get('sponsors', []):
                person_id = self._create_entity(
                    'Person',
                    {
                        'name': sponsor.get('name', ''),
                        'title': sponsor.get('title', 'Commissioner'),
                        'affiliation': 'City Council',
                        'contactInfo': None
                    },
                    source=f"taxonomy_{legal_file.stem}"
                )
                
                self._create_relationship(
                    'sponsors',
                    person_id,
                    policy_id,
                    {'sponsorshipType': 'primary'}
                )
            
        except Exception as e:
            log.error(f"Error processing legal file {legal_file}: {e}")
    
    def _create_entity(self, entity_type: str, attributes: Dict, source: str) -> str:
        """
        Create an entity using the toolkit.
        
        Returns:
            Entity ID
        """
        # Create entity with toolkit
        entity = self.toolkit.create_entity(entity_type, attributes, source)
        
        # Get the ID field
        id_field = entity.get(f'{entity_type.lower()}ID') or \
                   entity.get(f'{entity_type}ID') or \
                   entity.get('id')
        
        if not id_field:
            # Generate ID if missing
            entity_id = self.toolkit.generate_entity_id(entity_type, attributes)
            entity[f'{entity_type.lower()}ID'] = entity_id
        else:
            entity_id = id_field
        
        # Store entity
        if entity_type not in self.created_entities:
            self.created_entities[entity_type] = {}
        
        # Merge if entity already exists
        if entity_id in self.created_entities[entity_type]:
            existing = self.created_entities[entity_type][entity_id]
            entity = self.toolkit.merge_entities(existing, entity)
        
        self.created_entities[entity_type][entity_id] = entity
        
        return entity_id
    
    def _create_relationship(self, rel_type: str, source_id: str, 
                           target_id: str, attributes: Dict) -> None:
        """Create a relationship using the toolkit."""
        rel = self.toolkit.create_relationship(
            rel_type, source_id, target_id, attributes, 
            source="taxonomy"
        )
        self.created_relationships.append(rel)
    
    def _find_existing_document_id(self, meeting_date: str, doc_type: str) -> Optional[str]:
        """
        Find existing document ID for reuse instead of creating duplicate.
        
        Args:
            meeting_date: Meeting date (e.g., "01.09.2024")
            doc_type: Document type (e.g., "agenda")
            
        Returns:
            Existing document ID or None if not found
        """
        # First, check if merged documents exist (post-deduplication)
        merged_docs_file = self.output_dir / "merged" / "entities" / "Document.json"
        if merged_docs_file.exists():
            try:
                with open(merged_docs_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                for entity in data.get('entities', []):
                    title = entity.get('title', '').lower()
                    name = entity.get('name', '').lower()
                    entity_type = entity.get('document_type', '').lower()
                    
                    # Look for date patterns in title/name
                    date_normalized = meeting_date.replace('.', '').replace('-', '').replace('_', '')
                    
                    if (doc_type.lower() in title or doc_type.lower() in name or 
                        doc_type.lower() == entity_type):
                        # Check if date matches
                        if (date_normalized in title.replace('.', '').replace('-', '').replace('_', '') or
                            date_normalized in name.replace('.', '').replace('-', '').replace('_', '')):
                            document_id = entity.get('documentID')
                            if document_id:
                                log.info(f"   Found existing merged document ID: {document_id}")
                                return document_id
                                
            except Exception as e:
                log.warning(f"Error reading merged documents file: {e}")
        
        # Fallback: Look in the raw NER extracted documents
        doc_dir = self.output_dir / "Document"
        if not doc_dir.exists():
            return None
        
        for doc_file in doc_dir.glob("*.json"):
            try:
                with open(doc_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                for entity in data.get('entities', []):
                    title = entity.get('title', '').lower()
                    name = entity.get('name', '').lower()
                    entity_type = entity.get('document_type', '').lower()
                    
                    date_normalized = meeting_date.replace('.', '').replace('-', '').replace('_', '')
                    
                    if (doc_type.lower() in title or doc_type.lower() in name or 
                        doc_type.lower() == entity_type):
                        if (date_normalized in title.replace('.', '').replace('-', '').replace('_', '') or
                            date_normalized in name.replace('.', '').replace('-', '').replace('_', '')):
                            document_id = entity.get('documentID')
                            if document_id:
                                log.info(f"   Found existing NER document ID: {document_id}")
                                return document_id
                                
            except Exception as e:
                log.warning(f"Error reading document file {doc_file}: {e}")
                continue
        
        return None
    
    async def _save_all_entities(self) -> None:
        """Save all created entities and relationships to files."""
        # Save entities by type
        for entity_type, entities in self.created_entities.items():
            if not entities:
                continue
            
            # Convert to list format matching NER output
            entity_list = list(entities.values())
            
            # Save to file in NER format
            filename = f"taxonomy_synthesis.json"
            filepath = self.registry_dir / entity_type / filename
            
            file_data = {
                "chunk_id": "taxonomy",
                "document": "taxonomy_synthesis",
                "source_file": "multiple",
                "entity_type": entity_type,
                "entities": entity_list,
                "_metadata": {
                    "synthesis_timestamp": datetime.now().isoformat(),
                    "entity_count": len(entity_list)
                }
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(file_data, f, indent=2, ensure_ascii=False)
        
        # Save relationships
        if self.created_relationships:
            filename = f"taxonomy_synthesis.json"
            filepath = self.registry_dir / "relationships" / filename
            
            file_data = {
                "chunk_id": "taxonomy",
                "document": "taxonomy_synthesis",
                "source_file": "multiple",
                "relationships": self.created_relationships,
                "_metadata": {
                    "synthesis_timestamp": datetime.now().isoformat(),
                    "relationship_count": len(self.created_relationships)
                }
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(file_data, f, indent=2, ensure_ascii=False)
    
    async def create_seed_entities(self) -> None:
        """
        Create minimal seed entities to ensure taxonomy exists.
        This guarantees core entities even if extraction is empty.
        """
        log.info("🌱 Creating seed entities")
        
        # Core organization
        city_id = self._create_entity(
            'Organization',
            {
                'name': 'City of Coral Gables',
                'type': 'Municipality',
                'jurisdiction': 'Coral Gables',
                'address': '405 Biltmore Way, Coral Gables, FL 33134'
            },
            source="seed"
        )
        
        # Core roles
        for role_title in ['Mayor', 'Vice Mayor', 'Commissioner', 'City Manager', 'City Attorney', 'City Clerk']:
            role_id = self._create_entity(
                'Role',
                {
                    'title': role_title,
                    'startDate': None,
                    'endDate': None
                },
                source="seed"
            )
        
        # Core location
        city_hall_id = self._create_entity(
            'Location',
            {
                'name': 'City Hall',
                'type': 'Building',
                'address': '405 Biltmore Way, Coral Gables, FL 33134',
                'coordinates': None
            },
            source="seed"
        )
        
        # Link organization to location
        self._create_relationship(
            'isLocatedAt',
            city_id,
            city_hall_id,
            {}
        )
        
        # Core topics
        for topic_name in ['Budget', 'Zoning', 'Public Safety', 'Infrastructure', 'Parks and Recreation']:
            topic_id = self._create_entity(
                'Topic',
                {
                    'name': topic_name,
                    'category': 'Governance',
                    'description': f"City governance topic: {topic_name}"
                },
                source="seed"
            )
        
        # Save seed entities
        await self._save_all_entities()
        
        log.info("✅ Seed entities created")
