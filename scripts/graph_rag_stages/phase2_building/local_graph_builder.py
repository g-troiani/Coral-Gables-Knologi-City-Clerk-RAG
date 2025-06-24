"""
Local graph builder using NetworkX for creating knowledge graphs from JSON extraction output.
"""

import logging
import re
import json
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import networkx as nx
from datetime import datetime
from ..common.utils import get_llm_client, extract_json_with_llm

log = logging.getLogger(__name__)


class LocalGraphBuilder:
    """Builds hierarchical knowledge graphs from extracted JSON documents."""
    
    def __init__(self, output_dir: Optional[Path] = None):
        self.output_dir = output_dir or Path("local_graph_data")
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize graph and LLM client
        self.graph = nx.DiGraph()
        self.llm_client = get_llm_client()
        self.model = "llama-3.3-70b-versatile"
        
        # Track nodes for deduplication and linking
        self.meetings = {}  # {meeting_date: meeting_id}
        self.sections = {}  # {(meeting_date, section_order): section_id}
        self.agenda_items = {}  # {(meeting_date, item_code): item_id}
        self.people = {}  # {person_name_normalized: person_id}
        self.documents = {}  # {(doc_type, doc_number): doc_id}
        
        # Track agenda structure from stage 3 JSON
        self.agenda_data = {}  # {meeting_date: full_agenda_data}
        
    async def build_graph_from_json(self, json_dir: Path) -> None:
        """Build hierarchical graph from Stage 3 JSON output."""
        log.info(f"🔗 Building hierarchical graph from JSON: {json_dir}")
        
        json_files = list(json_dir.glob("*_stage3_ontology.json"))
        log.info(f"Found {len(json_files)} Stage 3 JSON files")
        
        # Process agenda files first to build hierarchy
        agenda_files = [f for f in json_files if 'agenda' in f.name.lower()]
        for agenda_file in agenda_files:
            await self._process_agenda_json(agenda_file)
        
        # Process supporting documents
        for json_file in json_files:
            if json_file not in agenda_files:
                await self._process_supporting_document_json(json_file)
        
        # Process hierarchical verbatim transcript collections
        await self._process_verbatim_transcript_collections(json_dir)
        
        self._save_graph()
        stats = self.get_graph_stats()
        log.info(f"✅ Graph building completed. Stats: {stats}")
    
    async def _process_agenda_json(self, json_file: Path) -> None:
        """Process Stage 3 agenda JSON to create Meeting → Section → AgendaItem hierarchy."""
        log.info(f"📄 Processing agenda JSON: {json_file.name}")
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        meeting_date = data.get('meeting_date', 'unknown')
        self.agenda_data[meeting_date] = data
        
        # Create Meeting node (ROOT)
        meeting_info = data.get('meeting_info', {})
        meeting_id = self._create_meeting_node({
            'meeting_date': meeting_date,
            'type': meeting_info.get('type', 'Regular Meeting'),
            'time': meeting_info.get('time', 'Unknown'),
            'location': meeting_info.get('location', 'City Hall'),
            'officials': meeting_info.get('officials', {}),
            'commissioners': meeting_info.get('commissioners', [])
        })
        
        # Create official attendance relationships
        self._create_official_attendance(meeting_id, meeting_info)
        
        # Process sections from Stage 3 enhanced structure
        for section in data.get('sections', []):
            section_id = self._create_section_node(meeting_id, section, meeting_date)
            
            # Process items in section
            previous_item_id = None
            for item in section.get('items', []):
                item_id = self._create_agenda_item_from_json(section_id, item, meeting_date)
                
                # Create FOLLOWS relationship
                if previous_item_id:
                    self.graph.add_edge(previous_item_id, item_id,
                                       relationship='FOLLOWS',
                                       kind='STRUCTURAL',
                                       sequence=item.get('item_order', 0))
                
                previous_item_id = item_id
        
        # Process entities from Stage 3
        for entity in data.get('entities', []):
            self._process_entity(entity, meeting_date)
        
        # Process relationships from Stage 3
        for rel in data.get('relationships', []):
            self._process_relationship(rel, meeting_date)
    
    async def _process_supporting_document_json(self, json_file: Path) -> None:
        """Process ordinance/resolution/transcript JSON files."""
        log.info(f"📜 Processing supporting document: {json_file.name}")
        
        # For now, we'll handle these when we find them referenced in agenda items
        # This is a placeholder for future enhancement
        pass
    
    async def _process_verbatim_transcript_collections(self, json_dir: Path) -> None:
        """Process hierarchical verbatim transcript collections."""
        log.info(f"🎤 Processing hierarchical verbatim transcript collections from: {json_dir}")
        
        # Find verbatim transcript collection files
        transcript_collection_files = list(json_dir.glob("*_verbatim_transcript_collection.json"))
        individual_transcript_files = list(json_dir.glob("*_verbatim_transcript.json"))
        
        if not transcript_collection_files and not individual_transcript_files:
            log.info("📝 No verbatim transcript files found")
            return
        
        log.info(f"📄 Found {len(transcript_collection_files)} transcript collections and {len(individual_transcript_files)} individual transcripts")
        
        # Process transcript collections (preferred approach)
        for collection_file in transcript_collection_files:
            await self._process_transcript_collection(collection_file)
        
        # Process individual transcript files if no collections found
        if not transcript_collection_files:
            for transcript_file in individual_transcript_files:
                await self._process_individual_transcript(transcript_file)
    
    async def _process_transcript_collection(self, collection_file: Path) -> None:
        """Process a verbatim transcript collection with hierarchical relationships."""
        log.info(f"🎤 Processing transcript collection: {collection_file.name}")
        
        with open(collection_file, 'r', encoding='utf-8') as f:
            collection_data = json.load(f)
        
        meeting_date = collection_data.get('meeting_date')
        if not meeting_date:
            log.warning(f"No meeting date found in {collection_file.name}")
            return
        
        # Ensure meeting node exists
        meeting_id = self.meetings.get(meeting_date)
        if not meeting_id:
            log.warning(f"Meeting node not found for date {meeting_date}, creating basic meeting node")
            meeting_id = self._create_meeting_node({'meeting_date': meeting_date})
        
        # Process each transcript in the collection
        for transcript_data in collection_data.get('transcripts', []):
            await self._process_transcript_with_hierarchy(transcript_data, meeting_id, meeting_date)
        
        # Process pre-built hierarchical relationships
        for relationship in collection_data.get('hierarchical_relationships', []):
            self._add_hierarchical_relationship(relationship, meeting_date)
    
    async def _process_transcript_with_hierarchy(self, transcript_data: Dict, meeting_id: str, meeting_date: str) -> str:
        """Process individual transcript data and create hierarchical relationships."""
        transcript_id = transcript_data.get('id', f"transcript-{meeting_date.replace('.', '-')}-unknown")
        
        # Create transcript node
        self.graph.add_node(transcript_id,
            node_type='VerbatimTranscript',
            type='verbatim_transcript',
            title=transcript_data.get('source_file', 'Unknown Transcript'),
            source_file=transcript_data.get('source_file', ''),
            transcript_type=transcript_data.get('transcript_type', 'unknown'),
            item_codes=json.dumps(transcript_data.get('item_codes', [])),
            section_codes=json.dumps(transcript_data.get('section_codes', [])),
            page_count=len(transcript_data.get('pages', [])),
            meeting_date=meeting_date,
            full_text_length=len(transcript_data.get('full_text', '')),
            extraction_method=transcript_data.get('metadata', {}).get('extraction_method', 'filename_parsing_ocr')
        )
        
        log.info(f"📝 Created transcript node: {transcript_id}")
        
        # Create hierarchical relationships based on item codes
        for item_code in transcript_data.get('item_codes', []):
            agenda_item_id = f"agenda-item-{meeting_date.replace('.', '-')}-{item_code}"
            
            # Ensure agenda item exists (create if needed)
            if agenda_item_id not in self.graph.nodes():
                self._create_placeholder_agenda_item(agenda_item_id, item_code, meeting_date, meeting_id)
            
            # Create AgendaItem → VerbatimTranscript relationship
            self.graph.add_edge(agenda_item_id, transcript_id,
                               relationship='HAS_VERBATIM_TRANSCRIPT',
                               kind='HIERARCHICAL',
                               transcript_type=transcript_data.get('transcript_type'),
                               page_count=len(transcript_data.get('pages', [])),
                               filename=transcript_data.get('source_file', ''))
            
            log.debug(f"🔗 Created HAS_VERBATIM_TRANSCRIPT: {agenda_item_id} → {transcript_id}")
        
        # Create section-based relationships
        for section_code in transcript_data.get('section_codes', []):
            section_id = f"section-{meeting_date.replace('.', '-')}-{section_code}"
            
            # Ensure section exists (create if needed)
            if section_id not in self.graph.nodes():
                self._create_placeholder_section(section_id, section_code, meeting_date, meeting_id)
            
            # Create Section → VerbatimTranscript relationship
            self.graph.add_edge(section_id, transcript_id,
                               relationship='HAS_VERBATIM_TRANSCRIPT',
                               kind='HIERARCHICAL',
                               transcript_type=transcript_data.get('transcript_type'),
                               section_code=section_code)
            
            log.debug(f"🔗 Created section HAS_VERBATIM_TRANSCRIPT: {section_id} → {transcript_id}")
        
        return transcript_id
    
    async def _process_individual_transcript(self, transcript_file: Path) -> None:
        """Process individual transcript file (fallback when no collection exists)."""
        log.info(f"📝 Processing individual transcript: {transcript_file.name}")
        
        with open(transcript_file, 'r', encoding='utf-8') as f:
            transcript_data = json.load(f)
        
        meeting_date = transcript_data.get('meeting_date')
        if not meeting_date:
            log.warning(f"No meeting date found in {transcript_file.name}")
            return
        
        # Ensure meeting node exists
        meeting_id = self.meetings.get(meeting_date)
        if not meeting_id:
            meeting_id = self._create_meeting_node({'meeting_date': meeting_date})
        
        # Process transcript with hierarchy
        await self._process_transcript_with_hierarchy(transcript_data, meeting_id, meeting_date)
    
    def _add_hierarchical_relationship(self, relationship: Dict, meeting_date: str) -> None:
        """Add pre-built hierarchical relationship to the graph."""
        source = relationship.get('source')
        target = relationship.get('target')
        rel_type = relationship.get('relationship')
        properties = relationship.get('properties', {})
        
        if not all([source, target, rel_type]):
            log.warning(f"Incomplete relationship data: {relationship}")
            return
        
        # Add the relationship with all properties
        edge_attrs = {
            'relationship': rel_type,
            'kind': 'HIERARCHICAL',
            **properties
        }
        
        self.graph.add_edge(source, target, **edge_attrs)
        log.debug(f"🔗 Added hierarchical relationship: {source} -[{rel_type}]-> {target}")
    
    def _create_placeholder_agenda_item(self, item_id: str, item_code: str, meeting_date: str, meeting_id: str) -> None:
        """Create placeholder agenda item node when transcript exists but agenda item wasn't processed."""
        self.graph.add_node(item_id,
            node_type='AgendaItem',
            type='agenda_item',
            item_code=item_code,
            title=f"Agenda Item {item_code}",
            meeting_date=meeting_date,
            placeholder=True,  # Mark as placeholder
            description=f"Placeholder for agenda item {item_code} (transcript-derived)"
        )
        
        # Link to meeting if not already connected
        if not self.graph.has_edge(meeting_id, item_id):
            self.graph.add_edge(meeting_id, item_id,
                               relationship='HAS_AGENDA_ITEM',
                               kind='HIERARCHICAL',
                               item_code=item_code,
                               placeholder=True)
        
        log.debug(f"📋 Created placeholder agenda item: {item_id}")
    
    def _create_placeholder_section(self, section_id: str, section_code: str, meeting_date: str, meeting_id: str) -> None:
        """Create placeholder section node when transcript exists but section wasn't processed."""
        self.graph.add_node(section_id,
            node_type='Section',
            type='section',
            section_code=section_code,
            title=f"Section {section_code}",
            meeting_date=meeting_date,
            placeholder=True,  # Mark as placeholder
            section_name=f"Section {section_code} (transcript-derived)"
        )
        
        # Link to meeting if not already connected
        if not self.graph.has_edge(meeting_id, section_id):
            self.graph.add_edge(meeting_id, section_id,
                               relationship='HAS_SECTION',
                               kind='HIERARCHICAL',
                               section_code=section_code,
                               placeholder=True)
        
        log.debug(f"📂 Created placeholder section: {section_id}")
    
    def _create_meeting_node(self, meeting_info: Dict) -> str:
        """Create Meeting node with all properties."""
        meeting_date = meeting_info.get('meeting_date', 'unknown')
        
        if meeting_date in self.meetings:
            return self.meetings[meeting_date]
        
        meeting_id = f"meeting-{meeting_date.replace('.', '-')}"
        
        self.graph.add_node(meeting_id,
            node_type='Meeting',
            type='meeting',
            meeting_date=meeting_date,
            meeting_type=meeting_info.get('type', 'Regular Meeting'),
            location=meeting_info.get('location', 'City Hall'),
            time=meeting_info.get('time', 'Unknown'),
            title=f"City Commission Meeting - {meeting_date}"
        )
        
        self.meetings[meeting_date] = meeting_id
        log.info(f"Created Meeting node: {meeting_id}")
        return meeting_id
    
    def _create_section_node(self, meeting_id: str, section: Dict, meeting_date: str) -> str:
        """Create Section node from Stage 3 section data."""
        section_order = section.get('section_order', 0)
        key = (meeting_date, section_order)
        
        if key in self.sections:
            return self.sections[key]
        
        section_id = f"section-{meeting_date.replace('.', '-')}-{section_order}"
        
        self.graph.add_node(section_id,
            node_type='Section',
            type='section',
            section_name=section.get('section_name', f'Section {section_order}'),
            section_type=section.get('section_type', 'general'),
            order=section_order,
            title=section.get('section_name', f'Section {section_order}')
        )
        
        # Create HAS_SECTION relationship
        self.graph.add_edge(meeting_id, section_id,
                           relationship='HAS_SECTION',
                           kind='STRUCTURAL',
                           order=section_order,
                           section_type=section.get('section_type', 'general'))
        
        self.sections[key] = section_id
        return section_id
    
    def _create_section_node_markdown(self, meeting_id: str, section: Dict, order: int, meeting_date: str) -> str:
        """Create Section node and link to Meeting."""
        key = (meeting_date, order)
        
        if key in self.sections:
            return self.sections[key]
        
        # Format section ID like in the example
        section_id = f"section-{meeting_date.replace('.', '-')}-{order}"
        
        self.graph.add_node(section_id,
            node_type='Section',
            type='section',
            section_name=section.get('section_name', f'Section {order}'),
            section_type=section.get('section_type', 'regular'),
            order=order,
            title=section.get('section_name', f'Section {order}')
        )
        
        # Create HAS_SECTION relationship: (Meeting) -[:HAS_SECTION]-> (Section)
        self.graph.add_edge(meeting_id, section_id,
                           relationship='HAS_SECTION',
                           kind='STRUCTURAL',
                           order=order,
                           section_type=section.get('section_type', 'regular'))
        
        log.debug(f"🔗 Created HAS_SECTION: {meeting_id} → {section_id}")
        self.sections[key] = section_id
        return section_id
    
    def _create_agenda_item_from_json(self, section_id: str, item: Dict, meeting_date: str) -> str:
        """Create AgendaItem node from Stage 3 item data."""
        item_code = item.get('item_code', '')
        key = (meeting_date, item_code)
        
        if key in self.agenda_items:
            return self.agenda_items[key]
        
        item_id = f"item-{meeting_date.replace('.', '-')}-{item_code}"
        
        self.graph.add_node(item_id,
            node_type='AgendaItem',
            type='agenda_item',
            item_code=item_code,
            title=item.get('title', f'Agenda Item {item_code}'),
            description=item.get('description', ''),
            item_type=item.get('item_type', 'item'),
            document_reference=item.get('document_reference'),
            sponsors=json.dumps(item.get('sponsors', [])),
            fiscal_impact=item.get('fiscal_impact'),
            meeting_date=meeting_date,
            urls=json.dumps(item.get('urls', [])),
            has_hyperlinks=bool(item.get('urls', []))
        )
        
        # Create CONTAINS_ITEM relationship
        self.graph.add_edge(section_id, item_id,
                           relationship='CONTAINS_ITEM',
                           kind='STRUCTURAL',
                           order=item.get('item_order', 0))
        
        self.agenda_items[key] = item_id
        
        # Process any linked documents
        if item.get('document_reference'):
            self._create_document_reference(item_id, item, meeting_date)
        
        return item_id
    
    def _create_document_reference(self, item_id: str, item: Dict, meeting_date: str) -> None:
        """Create document nodes for referenced documents."""
        doc_ref = item.get('document_reference')
        if not doc_ref:
            return
        
        # Determine document type from item
        item_type = item.get('item_type', '').lower()
        if 'ordinance' in item_type:
            doc_type = 'ordinance'
        elif 'resolution' in item_type:
            doc_type = 'resolution'
        else:
            doc_type = 'document'
        
        # Create document node
        doc_id = f"{doc_type}-{doc_ref}"
        key = (doc_type, doc_ref)
        
        if key not in self.documents:
            self.graph.add_node(doc_id,
                node_type=doc_type.capitalize(),
                type=doc_type,
                document_number=doc_ref,
                title=item.get('title', ''),
                meeting_date=meeting_date
            )
            self.documents[key] = doc_id
        
        # Create REFERENCES_DOCUMENT relationship
        self.graph.add_edge(item_id, doc_id,
                           relationship='REFERENCES_DOCUMENT',
                           kind='STRUCTURAL',
                           document_type=doc_type,
                           document_number=doc_ref)
    
    def _process_entity(self, entity: Dict, meeting_date: str) -> None:
        """Process entities from Stage 3 extraction."""
        entity_type = entity.get('type', 'UNKNOWN')
        name = entity.get('name', '')
        
        if entity_type == 'AGENDA_ITEM':
            # Already handled in agenda processing
            return
        elif entity_type in ['PERSON', 'ORGANIZATION']:
            # Create person/org nodes
            normalized_name = self._normalize_entity_name(name)
            key = (entity_type, normalized_name)
            
            if key not in self.people:
                entity_id = f"{entity_type.lower()}-{normalized_name.replace(' ', '-').lower()}"
                self.graph.add_node(entity_id,
                    node_type=entity_type.capitalize(),
                    type=entity_type.lower(),
                    name=name,
                    title=name,
                    description=entity.get('description', '')
                )
                self.people[key] = entity_id
        # Add more entity types as needed
    
    def _process_relationship(self, rel: Dict, meeting_date: str) -> None:
        """Process relationships from Stage 3 extraction."""
        source = rel.get('source')
        target = rel.get('target')
        rel_type = rel.get('type', 'RELATED_TO')
        
        # Map Stage 3 relationship types to graph relationships
        if rel_type == 'references_document':
            # Already handled in document reference creation
            pass
        elif rel_type == 'contains_item':
            # Already handled in section/item creation
            pass
        else:
            # Generic relationship handling
            # This would need to be expanded based on your specific needs
            pass
    
    def _create_official_attendance(self, meeting_id: str, meeting_info: Dict):
        """Create Person nodes for officials and ATTENDED relationships."""
        officials = meeting_info.get('officials', {})
        
        # Process each official type
        for role, name in officials.items():
            if name and name != 'N/A':
                person_id = self._ensure_person_node(name)
                self.graph.add_edge(person_id, meeting_id,
                                   relationship='ATTENDED',
                                   kind='STRUCTURAL',
                                   role=role.replace('_', ' ').title())
        
        # Process commissioners
        for commissioner in meeting_info.get('commissioners', []):
            if commissioner and commissioner != 'N/A':
                person_id = self._ensure_person_node(commissioner)
                self.graph.add_edge(person_id, meeting_id,
                                   relationship='ATTENDED',
                                   kind='STRUCTURAL',
                                   role='Commissioner')
    
    def _ensure_person_node(self, name: str) -> str:
        """Create or get Person node."""
        normalized_name = self._normalize_entity_name(name)
        key = ('Person', normalized_name)
        
        if key in self.people:
            return self.people[key]
        
        person_id = f"person-{normalized_name.replace(' ', '-').lower()}"
        
        self.graph.add_node(person_id,
            node_type='Person',
            type='person',
            name=name,
            title=name
        )
        
        self.people[key] = person_id
        return person_id
    
    def _normalize_entity_name(self, name: str) -> str:
        """Normalize entity name for consistent identification."""
        return ' '.join(name.strip().split()).title()
    
    def _save_graph(self) -> None:
        """Save graph to multiple formats."""
        # Clean up None values for GraphML compatibility
        self._clean_graph_attributes()
        
        # Save as GraphML
        graphml_path = self.output_dir / "city_clerk_graph.graphml"
        nx.write_graphml(self.graph, str(graphml_path))
        log.info(f"💾 Saved graph as GraphML: {graphml_path}")
        
        # Save as JSON
        json_path = self.output_dir / "city_clerk_graph.json"
        data = nx.node_link_data(self.graph)
        with open(json_path, 'w') as f:
            json.dump(data, f, indent=2)
        log.info(f"💾 Saved graph as JSON: {json_path}")
        
        # Save graph statistics
        stats_path = self.output_dir / "graph_stats.json"
        with open(stats_path, 'w') as f:
            json.dump(self.get_graph_stats(), f, indent=2)
    
    def _clean_graph_attributes(self) -> None:
        """Clean up None values and other problematic attributes for GraphML export."""
        # Clean node attributes
        for node_id, attrs in self.graph.nodes(data=True):
            cleaned_attrs = {}
            for key, value in attrs.items():
                if value is None:
                    cleaned_attrs[key] = ""  # Replace None with empty string
                elif isinstance(value, (list, dict)):
                    cleaned_attrs[key] = str(value)  # Convert complex types to strings
                else:
                    cleaned_attrs[key] = value
            # Update node attributes
            self.graph.add_node(node_id, **cleaned_attrs)
        
        # Clean edge attributes
        for u, v, attrs in self.graph.edges(data=True):
            cleaned_attrs = {}
            for key, value in attrs.items():
                if value is None:
                    cleaned_attrs[key] = ""  # Replace None with empty string
                elif isinstance(value, (list, dict)):
                    cleaned_attrs[key] = str(value)  # Convert complex types to strings
                else:
                    cleaned_attrs[key] = value
            # Update edge attributes
            self.graph.add_edge(u, v, **cleaned_attrs)

    def get_graph_stats(self) -> Dict[str, Any]:
        """Get statistics about the graph."""
        stats = {
            'total_nodes': self.graph.number_of_nodes(),
            'total_edges': self.graph.number_of_edges(),
            'node_types': {}
        }
        
        # Count nodes by type
        for node, attrs in self.graph.nodes(data=True):
            node_type = attrs.get('node_type', 'Unknown')
            stats['node_types'][node_type] = stats['node_types'].get(node_type, 0) + 1
        
        return stats

    def load_graph(self, format: str = 'graphml') -> bool:
        """Load a previously saved graph."""
        try:
            if format == 'graphml':
                graphml_path = self.output_dir / "city_clerk_graph.graphml"
                self.graph = nx.read_graphml(str(graphml_path))
            elif format == 'json':
                json_path = self.output_dir / "city_clerk_graph.json"
                with open(json_path, 'r') as f:
                    data = json.load(f)
                self.graph = nx.node_link_graph(data)
            
            log.info(f"✅ Loaded graph from {format} format")
            return True
        except Exception as e:
            log.error(f"Failed to load graph: {e}")
            return False 