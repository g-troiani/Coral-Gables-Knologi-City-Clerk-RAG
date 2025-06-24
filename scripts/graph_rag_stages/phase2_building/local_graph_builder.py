"""
Local graph builder using NetworkX for creating knowledge graphs without cloud dependencies.
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
    """Builds hierarchical knowledge graphs with clear structural relationships."""
    
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
        
        # Track agenda structure for fallback matching
        self.agenda_structures = {}  # {meeting_date: agenda_structure}
        
    async def build_graph_from_markdown(self, markdown_dir: Path) -> None:
        """Build hierarchical graph with two-phase processing."""
        log.info(f"🔗 Building hierarchical graph from: {markdown_dir}")
        
        markdown_files = list(markdown_dir.glob("*.md"))
        log.info(f"Found {len(markdown_files)} markdown files")
        
        # Separate files by type
        agenda_files = [f for f in markdown_files if 'agenda' in f.name.lower()]
        ordinance_files = [f for f in markdown_files if 'ordinance' in f.name.lower()]
        resolution_files = [f for f in markdown_files if 'resolution' in f.name.lower()]
        transcript_files = [f for f in markdown_files if 'verbatim' in f.name.lower()]
        
        # Phase 1: Process agendas to build complete hierarchy
        log.info("📋 Phase 1: Building meeting hierarchy from agendas...")
        for agenda_file in agenda_files:
            await self._process_agenda_document(agenda_file)
        
        # Phase 2: Link supporting documents
        log.info("📜 Phase 2: Linking supporting documents...")
        
        # Process ordinances and resolutions
        for doc_file in ordinance_files + resolution_files:
            await self._process_legislative_document(doc_file)
        
        # Process transcripts
        for transcript_file in transcript_files:
            await self._process_transcript_document(transcript_file)
        
        self._save_graph()
        stats = self.get_graph_stats()
        log.info(f"✅ Graph building completed. Stats: {stats}")
    
    async def _process_agenda_document(self, md_file: Path) -> None:
        """Process agenda to create Meeting → Section → AgendaItem hierarchy."""
        log.info(f"📄 Processing agenda: {md_file.name}")
        
        content = md_file.read_text(encoding='utf-8')
        metadata = self._extract_document_metadata(content)
        meeting_date = self._normalize_date(metadata.get('meeting_date', ''))
        
        if not meeting_date or meeting_date == 'unknown':
            meeting_date = self._extract_meeting_date_from_content(content)
        
        # Extract agenda structure using LLM
        agenda_structure = await self._extract_agenda_ontology(content, meeting_date)
        
        # Override LLM meeting_date if it's N/A or invalid
        if not agenda_structure['meeting_info'].get('meeting_date') or agenda_structure['meeting_info']['meeting_date'] in ['N/A', 'unknown']:
            agenda_structure['meeting_info']['meeting_date'] = meeting_date
        
        # Use the consistent meeting date
        consistent_meeting_date = agenda_structure['meeting_info']['meeting_date']
        self.agenda_structures[consistent_meeting_date] = agenda_structure
        
        # Create Meeting node
        meeting_id = self._create_meeting_node(agenda_structure['meeting_info'])
        
        # Create Agenda document node and link to Meeting
        agenda_id = self._generate_document_id(md_file, metadata)
        self._add_document_node(agenda_id, metadata, md_file)
        self.graph.add_edge(agenda_id, meeting_id,
                           relationship='DOCUMENTS',
                           kind='STRUCTURAL')
        
        # Create official attendance relationships
        self._create_official_attendance(meeting_id, agenda_structure['meeting_info'])
        
        # Create hierarchy: Meeting → Sections → Items
        for section_idx, section in enumerate(agenda_structure.get('agenda_structure', [])):
            section_id = self._create_section_node(meeting_id, section, section_idx, consistent_meeting_date)
            
            # Track previous item within this section only
            previous_item_id = None
            
            for item_idx, item in enumerate(section.get('items', [])):
                item_id = self._create_agenda_item_node(section_id, item, consistent_meeting_date)
                
                # Create FOLLOWS relationship between consecutive items within this section
                # (Previous_AgendaItem) -[:FOLLOWS]-> (Current_AgendaItem)
                if previous_item_id:
                    self.graph.add_edge(previous_item_id, item_id,
                                       relationship='FOLLOWS',
                                       kind='STRUCTURAL')
                    log.debug(f"🔗 Created FOLLOWS: {previous_item_id} → {item_id}")
                previous_item_id = item_id
    
    async def _extract_agenda_ontology(self, content: str, meeting_date: str) -> Dict:
        """Extract complete agenda ontology using LLM."""
        prompt = """
        Extract the complete agenda structure from this city meeting document.
        Return a JSON object with:
        {
            "meeting_info": {
                "meeting_date": "MM.DD.YYYY",
                "meeting_type": "Regular Meeting|Special Meeting",
                "location": "meeting location",
                "officials_present": {
                    "mayor": "name",
                    "vice_mayor": "name",
                    "commissioners": ["name1", "name2"],
                    "city_attorney": "name",
                    "city_manager": "name",
                    "city_clerk": "name"
                }
            },
            "agenda_structure": [
                {
                    "section_name": "Section name (e.g., CONSENT AGENDA)",
                    "section_type": "consent|regular|special",
                    "order": 1,
                    "items": [
                        {
                            "item_code": "E-1",
                            "title": "Full item title",
                            "description": "Item description",
                            "item_type": "ordinance|resolution|discussion|presentation",
                            "document_reference": "2024-01 (if mentioned)",
                            "sponsors": ["name1", "name2"],
                            "related_codes": ["related item codes"],
                            "fiscal_impact": "amount if mentioned"
                        }
                    ]
                }
            ],
            "entities": {
                "people": ["mentioned people"],
                "organizations": ["mentioned orgs"],
                "locations": ["mentioned locations"],
                "projects": ["mentioned projects"]
            }
        }
        """
        
        messages = [
            {"role": "system", "content": prompt},
            {"role": "user", "content": content[:20000]}  # Increased context
        ]
        
        response = self.llm_client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0
        )
        
        try:
            result_text = response.choices[0].message.content.strip()
            log.info(f"🤖 LLM agenda ontology response: {result_text[:500]}...")
            
            # Try to extract JSON from code blocks if present
            if '```json' in result_text:
                result_text = result_text.split('```json')[1].split('```')[0].strip()
            elif '```' in result_text:
                # Find the first code block
                parts = result_text.split('```')
                if len(parts) >= 3:
                    result_text = parts[1].strip()
            
            # If it still starts with 'json', remove that
            if result_text.startswith('json'):
                result_text = result_text[4:].strip()
            
            result = json.loads(result_text)
            log.info(f"✅ Successfully parsed agenda ontology with {len(result.get('agenda_structure', []))} sections")
            
            # Ensure meeting date is set
            if not result.get('meeting_info', {}).get('meeting_date'):
                result['meeting_info']['meeting_date'] = meeting_date
            return result
        except Exception as e:
            log.error(f"❌ Failed to parse LLM agenda ontology: {e}")
            log.error(f"Raw LLM response: {result_text if 'result_text' in locals() else 'No response'}")
            # Return minimal structure on parse error
            return {
                "meeting_info": {"meeting_date": meeting_date},
                "agenda_structure": [],
                "entities": {}
            }
    
    def _create_meeting_node(self, meeting_info: Dict) -> str:
        """Create Meeting node with all properties."""
        meeting_date = meeting_info.get('meeting_date', 'unknown')
        
        if meeting_date in self.meetings:
            return self.meetings[meeting_date]
        
        # Format meeting ID like in the example
        meeting_id = f"meeting-{meeting_date.replace('.', '-')}"
        
        self.graph.add_node(meeting_id,
            node_type='Meeting',
            type='meeting',
            meeting_date=meeting_date,
            meeting_type=meeting_info.get('meeting_type', 'Regular Meeting'),
            location=meeting_info.get('location', 'City Hall'),
            title=f"City Commission Meeting - {meeting_date}"
        )
        
        self.meetings[meeting_date] = meeting_id
        log.info(f"Created Meeting node: {meeting_id}")
        return meeting_id
    
    def _create_official_attendance(self, meeting_id: str, meeting_info: Dict):
        """Create Person nodes for officials and ATTENDED relationships."""
        officials = meeting_info.get('officials_present', {})
        
        # Process each official type
        official_mappings = [
            ('mayor', officials.get('mayor'), 'Mayor'),
            ('vice_mayor', officials.get('vice_mayor'), 'Vice Mayor'),
            ('city_attorney', officials.get('city_attorney'), 'City Attorney'),
            ('city_manager', officials.get('city_manager'), 'City Manager'),
            ('city_clerk', officials.get('city_clerk'), 'City Clerk')
        ]
        
        for field, name, role in official_mappings:
            if name:
                person_id = self._ensure_person_node(name)
                # (Person) -[:ATTENDED]-> (Meeting)
                self.graph.add_edge(person_id, meeting_id,
                                   relationship='ATTENDED',
                                   kind='STRUCTURAL',
                                   role=role)
                log.debug(f"🔗 Created ATTENDED: {person_id} → {meeting_id} (role: {role})")
        
        # Process commissioners
        for commissioner in officials.get('commissioners', []):
            if commissioner:
                person_id = self._ensure_person_node(commissioner)
                # (Person) -[:ATTENDED]-> (Meeting)
                self.graph.add_edge(person_id, meeting_id,
                                   relationship='ATTENDED',
                                   kind='STRUCTURAL',
                                   role='Commissioner')
                log.debug(f"🔗 Created ATTENDED: {person_id} → {meeting_id} (role: Commissioner)")
    
    def _ensure_person_node(self, name: str) -> str:
        """Create or get Person node."""
        normalized_name = self._normalize_person_name(name)
        
        if normalized_name in self.people:
            return self.people[normalized_name]
        
        # Format person ID like in the example
        person_id = f"person-{normalized_name.replace(' ', '-').lower()}"
        
        self.graph.add_node(person_id,
            node_type='Person',
            type='person',
            name=name,
            title=name
        )
        
        self.people[normalized_name] = person_id
        return person_id
    
    def _create_section_node(self, meeting_id: str, section: Dict, order: int, meeting_date: str) -> str:
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
                           order=order)
        
        log.debug(f"🔗 Created HAS_SECTION: {meeting_id} → {section_id}")
        self.sections[key] = section_id
        return section_id
    
    def _create_agenda_item_node(self, section_id: str, item: Dict, meeting_date: str) -> str:
        """Create AgendaItem node and link to Section."""
        item_code = self._normalize_item_code(item['item_code'])
        key = (meeting_date, item_code)
        
        if key in self.agenda_items:
            return self.agenda_items[key]
        
        # Format item ID like in the example
        item_id = f"item-{meeting_date.replace('.', '-')}-{item_code}"
        
        self.graph.add_node(item_id,
            node_type='AgendaItem',
            type='agenda_item',
            item_code=item_code,
            title=item.get('title', f'Agenda Item {item_code}'),
            description=item.get('description', ''),
            item_type=item.get('item_type', 'item'),
            document_reference=item.get('document_reference'),
            sponsors=json.dumps(item.get('sponsors', [])),  # Convert list to JSON string
            fiscal_impact=item.get('fiscal_impact'),
            meeting_date=meeting_date
        )
        
        # Create CONTAINS_ITEM relationship: (Section) -[:CONTAINS_ITEM]-> (AgendaItem)
        self.graph.add_edge(section_id, item_id,
                           relationship='CONTAINS_ITEM',
                           kind='STRUCTURAL')
        
        log.debug(f"🔗 Created CONTAINS_ITEM: {section_id} → {item_id}")
        self.agenda_items[key] = item_id
        return item_id
    
    async def _process_legislative_document(self, md_file: Path) -> None:
        """Process ordinance or resolution with fallback matching."""
        log.info(f"📜 Processing legislative document: {md_file.name}")
        
        content = md_file.read_text(encoding='utf-8')
        metadata = self._extract_document_metadata(content)
        
        # Extract document number from filename
        doc_number = self._extract_document_number(md_file.name)
        doc_type = metadata.get('document_type', 'document').lower()
        meeting_date = self._normalize_date(metadata.get('meeting_date', ''))
        
        # Create document node
        doc_id = self._create_document_node({
            'document_type': doc_type,
            'document_number': doc_number,
            'title': metadata.get('title', ''),
            'file_path': str(md_file),
            'meeting_date': meeting_date
        })
        
        # Try to find associated agenda item
        agenda_item_code = self._extract_agenda_item_reference(content)
        
        # If no item code found, try fallback matching
        if not agenda_item_code and doc_number:
            # Try to find the agenda structure with any available meeting date
            for stored_date, agenda_structure in self.agenda_structures.items():
                agenda_item_code = self._fallback_document_matching(doc_number, agenda_structure)
                if agenda_item_code:
                    meeting_date = stored_date  # Use the consistent meeting date
                    break
        
        # Create REFERENCES_DOCUMENT relationship if item found
        if agenda_item_code and meeting_date != 'unknown':
            item_code_normalized = self._normalize_item_code(agenda_item_code)
            key = (meeting_date, item_code_normalized)
            
            if key in self.agenda_items:
                item_id = self.agenda_items[key]
                # (AgendaItem) -[:REFERENCES_DOCUMENT]-> (Document)
                self.graph.add_edge(item_id, doc_id,
                                   relationship='REFERENCES_DOCUMENT',
                                   kind='STRUCTURAL',
                                   document_number=doc_number,
                                   document_type=doc_type)
                log.info(f"🔗 Created REFERENCES_DOCUMENT: {item_code_normalized} → {doc_type} {doc_number}")
            else:
                log.warning(f"AgendaItem not found for {item_code_normalized} (document {doc_number})")
                # Log available items for debugging
                available_items = [key[1] for key in self.agenda_items.keys() if key[0] == meeting_date]
                log.debug(f"Available items for {meeting_date}: {available_items}")
    
    async def _process_transcript_document(self, md_file: Path) -> None:
        """Process verbatim transcript."""
        log.info(f"🎤 Processing transcript: {md_file.name}")
        
        content = md_file.read_text(encoding='utf-8')
        metadata = self._extract_document_metadata(content)
        meeting_date = self._normalize_date(metadata.get('meeting_date', ''))
        
        # Parse item codes from filename
        item_codes = self._parse_transcript_item_codes(md_file.name)
        
        # Create transcript node
        transcript_id = self._create_transcript_node({
            'meeting_date': meeting_date,
            'item_codes': item_codes,
            'file_path': str(md_file)
        })
        
        # Create HAS_TRANSCRIPT relationships  
        for item_code in item_codes:
            item_code_normalized = self._normalize_item_code(item_code)
            
            # Try to find the agenda item with any available meeting date
            found_key = None
            for stored_date in self.agenda_structures.keys():
                key = (stored_date, item_code_normalized)
                if key in self.agenda_items:
                    found_key = key
                    break
            
            if found_key:
                item_id = self.agenda_items[found_key]
                # (AgendaItem) -[:HAS_TRANSCRIPT]-> (Transcript)
                self.graph.add_edge(item_id, transcript_id,
                                   relationship='HAS_TRANSCRIPT',
                                   kind='STRUCTURAL',
                                   item_codes=json.dumps(item_codes))
                log.info(f"🔗 Created HAS_TRANSCRIPT: {item_code_normalized} → transcript")
            else:
                log.warning(f"AgendaItem not found for transcript item {item_code_normalized}")
    
    def _create_document_node(self, doc_info: Dict) -> str:
        """Create Document/Ordinance/Resolution node."""
        doc_type = doc_info['document_type']
        doc_number = doc_info['document_number']
        
        key = (doc_type, doc_number)
        if key in self.documents:
            return self.documents[key]
        
        # Format document ID like in the example
        doc_id = f"{doc_type}-{doc_number}" if doc_number else f"{doc_type}-{self._generate_hash(str(doc_info))[:8]}"
        
        node_type = doc_type.capitalize()
        self.graph.add_node(doc_id,
            node_type=node_type,
            type=doc_type,
            document_number=doc_number,
            title=doc_info['title'],
            file_path=doc_info['file_path'],
            meeting_date=doc_info.get('meeting_date')
        )
        
        self.documents[key] = doc_id
        return doc_id
    
    def _create_transcript_node(self, transcript_info: Dict) -> str:
        """Create Transcript node."""
        meeting_date = transcript_info['meeting_date']
        item_codes = transcript_info['item_codes']
        
        # Format transcript ID
        items_str = '-'.join(sorted(item_codes)).lower()
        transcript_id = f"transcript-{meeting_date.replace('.', '-')}-{items_str}"
        
        self.graph.add_node(transcript_id,
            node_type='Transcript',
            type='transcript',
            meeting_date=meeting_date,
            item_codes=json.dumps(item_codes),  # Convert list to JSON string
            file_path=transcript_info['file_path'],
            title=f"Verbatim Transcript - Items {', '.join(item_codes)}"
        )
        
        return transcript_id
    
    def _normalize_item_code(self, code: str) -> str:
        """Normalize item code to standard format (e.g., E-1)."""
        code = code.upper().strip()
        
        # Remove dots and extra characters
        code = code.replace('.', '').replace('ITEM', '').strip()
        
        # Add hyphen if missing
        if len(code) >= 2 and code[0].isalpha() and code[1].isdigit():
            if '-' not in code:
                code = f"{code[0]}-{code[1:]}"
        
        return code
    
    def _fallback_document_matching(self, doc_number: str, agenda_structure: Dict) -> Optional[str]:
        """Try to match document by reference number in agenda items."""
        for section in agenda_structure.get('agenda_structure', []):
            for item in section.get('items', []):
                # Check if document reference matches
                if item.get('document_reference') == doc_number:
                    return item['item_code']
                
                # Check if document number appears in title or description
                if doc_number in item.get('title', '') or doc_number in item.get('description', ''):
                    return item['item_code']
        
        return None
    
    def _normalize_person_name(self, name: str) -> str:
        """Normalize person name for consistent identification."""
        return ' '.join(name.strip().split()).title()
    
    def _normalize_date(self, date_str: str) -> str:
        """Normalize date to MM.DD.YYYY format."""
        if not date_str or date_str in ['N/A', 'unknown']:
            return 'unknown'
        
        # Handle different separators
        date_str = date_str.replace('_', '.').replace('-', '.').replace('/', '.')
        
        # Split into parts
        parts = date_str.split('.')
        if len(parts) == 3:
            # Handle YYYY.MM.DD format
            if len(parts[0]) == 4:
                year, month, day = parts[0], parts[1], parts[2]
            # Handle MM.DD.YYYY or M.D.YYYY format
            else:
                month, day, year = parts[0], parts[1], parts[2]
            
            # Ensure 2-digit month and day
            month = month.zfill(2)
            day = day.zfill(2)
            
            return f"{month}.{day}.{year}"
        
        return date_str
    
    def _generate_hash(self, text: str) -> str:
        """Generate hash for unique IDs."""
        return hashlib.sha1(text.encode()).hexdigest()
    
    def _extract_agenda_item_reference(self, content: str) -> Optional[str]:
        """Use LLM to find agenda item reference in document."""
        prompt = """
        Find the agenda item code referenced in this document.
        Look for patterns like "Agenda Item: E-1" or "Item F-2".
        Return ONLY the item code (e.g., "E-1") or null if not found.
        """
        
        messages = [
            {"role": "system", "content": prompt},
            {"role": "user", "content": content[:5000]}
        ]
        
        response = self.llm_client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0
        )
        
        result = response.choices[0].message.content.strip()
        if result and result != 'null':
            # Normalize format
            result = result.upper().strip('"')
            if '-' not in result and len(result) > 1:
                result = f"{result[0]}-{result[1:]}"
            return result
        return None

    def _parse_transcript_item_codes(self, filename: str) -> List[str]:
        """Parse agenda item codes from transcript filename."""
        codes = []
        
        # Extract the part after date that contains item codes
        match = re.search(r'\d{2}_\d{2}_\d{4}\s*-\s*.*?-\s*(.+?)\.', filename)
        if match:
            items_part = match.group(1)
            
            # Handle different formats
            if ' and ' in items_part.lower():
                parts = re.split(r'\s+and\s+', items_part, flags=re.IGNORECASE)
                for part in parts:
                    codes.extend(self._extract_codes_from_string(part))
            else:
                codes.extend(self._extract_codes_from_string(items_part))
        
        return codes

    def _extract_codes_from_string(self, s: str) -> List[str]:
        """Extract item codes from a string."""
        codes = []
        # Match patterns like E-1, F-2, etc.
        for match in re.finditer(r'([A-Z])-?(\d+)', s):
            code = f"{match.group(1)}-{match.group(2)}"
            codes.append(code)
        return codes

    def _extract_document_number(self, filename: str) -> Optional[str]:
        """Extract document number from filename."""
        match = re.search(r'(\d{4}-\d+)', filename)
        return match.group(1) if match else None

    def _extract_document_metadata(self, content: str) -> Dict[str, Any]:
        """Extract metadata from markdown header."""
        from ..common.utils import extract_metadata_from_header
        metadata = extract_metadata_from_header(content)
        
        # If meeting_date is N/A or missing, try to extract from content
        if not metadata.get('meeting_date') or metadata.get('meeting_date') == 'N/A':
            meeting_date = self._extract_meeting_date_from_content(content)
            if meeting_date:
                metadata['meeting_date'] = meeting_date
        
        return metadata

    def _extract_meeting_date_from_content(self, content: str) -> Optional[str]:
        """Extract meeting date from document content."""
        # Common date patterns in city documents
        patterns = [
            r'(\d{1,2}\.\d{1,2}\.\d{4})',  # MM.DD.YYYY or M.D.YYYY
            r'(\d{1,2}/\d{1,2}/\d{4})',   # MM/DD/YYYY or M/D/YYYY
            r'(\d{4}-\d{1,2}-\d{1,2})',   # YYYY-MM-DD or YYYY-M-D
            r'Meeting Date:\s*(\d{1,2}\.\d{1,2}\.\d{4})',  # Meeting Date: MM.DD.YYYY
            r'Date:\s*(\d{1,2}\.\d{1,2}\.\d{4})',  # Date: MM.DD.YYYY
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            if matches:
                return matches[0]  # Return first match
        
        return None

    def _generate_document_id(self, md_file: Path, metadata: Dict) -> str:
        """Generate unique document ID."""
        unique_string = f"{md_file.name}_{metadata.get('document_type', 'doc')}"
        hash_part = hashlib.sha1(unique_string.encode()).hexdigest()[:8]
        doc_type = metadata.get('document_type', 'doc').upper()
        return f"{doc_type}_{hash_part}"

    def _add_document_node(self, doc_id: str, metadata: Dict, md_file: Path) -> None:
        """Add document node to graph."""
        self.graph.add_node(doc_id, 
            node_type='Document',
            title=metadata.get('title', md_file.stem),
            document_type=metadata.get('document_type', 'document'),
            source_file=md_file.name,
            meeting_date=metadata.get('meeting_date', ''),
            created_at=metadata.get('extraction_timestamp', datetime.now().isoformat())
        )
        log.debug(f"Added document node: {doc_id}")

    def _save_graph(self) -> None:
        """Save graph to multiple formats."""
        # Save as GraphML (XML format, good for interoperability)
        graphml_path = self.output_dir / "city_clerk_graph.graphml"
        nx.write_graphml(self.graph, str(graphml_path))
        log.info(f"💾 Saved graph as GraphML: {graphml_path}")
        
        # Save as JSON (human-readable)
        json_path = self.output_dir / "city_clerk_graph.json"
        data = nx.node_link_data(self.graph)
        with open(json_path, 'w') as f:
            json.dump(data, f, indent=2)
        log.info(f"💾 Saved graph as JSON: {json_path}")
        
        # Save graph statistics
        stats_path = self.output_dir / "graph_stats.json"
        with open(stats_path, 'w') as f:
            json.dump(self.get_graph_stats(), f, indent=2)

    def get_graph_stats(self) -> Dict[str, Any]:
        """Get statistics about the graph."""
        stats = {
            'total_nodes': self.graph.number_of_nodes(),
            'total_edges': self.graph.number_of_edges(),
            'documents': 0,
            'agenda_items': 0,
            'agenda_sections': 0,
            'meetings': 0,
            'entities': 0,
            'node_types': {}
        }
        
        # Count nodes by type
        for node, attrs in self.graph.nodes(data=True):
            node_type = attrs.get('node_type', 'Unknown')
            stats['node_types'][node_type] = stats['node_types'].get(node_type, 0) + 1
            
            if node_type == 'Document':
                stats['documents'] += 1
            elif node_type == 'AgendaItem':
                stats['agenda_items'] += 1
            elif node_type == 'Section':
                stats['agenda_sections'] += 1
            elif node_type == 'Meeting':
                stats['meetings'] += 1
            elif node_type in ['Person', 'Organization', 'Location']:
                stats['entities'] += 1
        
        # Add graph metrics
        if self.graph.number_of_nodes() > 0:
            stats['average_degree'] = sum(dict(self.graph.degree()).values()) / self.graph.number_of_nodes()
            stats['density'] = nx.density(self.graph)
        
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