"""
Local graph builder using NetworkX for creating knowledge graphs from JSON extraction output.
Implements clean data model with standardized vertices and semantic relationships.
Enhanced with proper data models, error handling, and performance optimizations.
"""

import logging
import re
import json
import hashlib
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
import networkx as nx
from datetime import datetime, timedelta
from scripts.graph_rag_stages.common.utils import get_llm_client, extract_json_with_llm
from scripts.graph_rag_stages.common.temporal_utils import TemporalParser, TemporalIndex
from openai import AzureOpenAI

log = logging.getLogger(__name__)


class NodeType(Enum):
    """Standardized node types for consistent labeling."""
    MEETING = "meeting"
    SECTION = "section" 
    AGENDA_ITEM = "agenda_item"
    DOCUMENT = "document"
    PERSON = "person"
    ORGANIZATION = "organization"
    DEPARTMENT = "department"
    LOCATION = "location"


class EdgeType(Enum):
    """Standardized edge types for consistent relationships."""
    # Existing basic relationships
    HAS_AGENDA = "HAS_AGENDA"
    HAS_SECTION = "HAS_SECTION"
    CONTAINS_ITEM = "CONTAINS_ITEM"
    PRECEDES = "PRECEDES"
    RESULTS_IN = "RESULTS_IN"
    PASSED_AT = "PASSED_AT"
    DISCUSSED_IN = "DISCUSSED_IN"
    MENTIONS = "MENTIONS"
    ATTENDED_BY = "ATTENDED_BY"
    IS_AGENDA_FOR = "IS_AGENDA_FOR"
    
    # NEW: Rich semantic relationships
    SPONSORED_BY = "SPONSORED_BY"
    VOTED_YES = "VOTED_YES"
    VOTED_NO = "VOTED_NO"
    ABSTAINED = "ABSTAINED"
    MOVED_BY = "MOVED_BY"
    SECONDED_BY = "SECONDED_BY"
    PRESENTED_BY = "PRESENTED_BY"
    APPOINTED_TO = "APPOINTED_TO"
    REPRESENTS = "REPRESENTS"
    CHAIRS = "CHAIRS"
    MEMBER_OF = "MEMBER_OF"


@dataclass
class NodeProperties:
    """Base class for node properties with validation."""
    node_id: str
    node_type: Optional[NodeType] = None
    name: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for NetworkX node attributes."""
        result = {
            'label': self.node_type.value,  # Keep for compatibility
            'name': self.sanitize_string(self.name)
        }
        # Add other fields with consistent snake_case naming
        for field_name, field_value in self.__dict__.items():
            if field_name not in ['node_id', 'node_type', 'name'] and field_value is not None:
                # Convert camelCase/PascalCase to snake_case
                snake_field = self._to_snake_case(field_name)
                
                if isinstance(field_value, (list, dict)):
                    result[snake_field] = field_value  # Keep as native type
                elif isinstance(field_value, bool):
                    result[snake_field] = field_value  # Keep as boolean
                else:
                    result[snake_field] = self.sanitize_string(str(field_value))
        return result
    
    @staticmethod
    def _to_snake_case(name: str) -> str:
        """Convert camelCase/PascalCase to snake_case."""
        import re
        # Insert underscore before uppercase letters
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    
    @staticmethod
    def sanitize_string(value: str) -> str:
        """Sanitize string values for consistent storage."""
        if not isinstance(value, str):
            return str(value)
        return ' '.join(value.strip().split())


@dataclass
class MeetingProperties(NodeProperties):
    """Properties for meeting nodes - standardized."""
    meeting_date: str = ""
    meeting_type: str = "Regular Meeting"
    location: str = "City Commission Chambers"
    time: str = ""
    source_file: str = ""  # snake_case, not Source_File
    
    def __post_init__(self):
        if self.node_type is None:
            self.node_type = NodeType.MEETING


@dataclass
class SectionProperties(NodeProperties):
    """Properties for section nodes - standardized."""
    title: str = ""
    section_type: str = "GENERAL"  # snake_case, not Section_Type
    order: int = 0
    is_empty: bool = False  # snake_case, not Is_Empty
    description: str = ""
    
    def __post_init__(self):
        if self.node_type is None:
            self.node_type = NodeType.SECTION


@dataclass
class AgendaItemProperties(NodeProperties):
    """Properties for agenda item nodes - standardized."""
    item_code: str = ""  # Remove duplicate Original_Code
    title: str = ""
    description: str = ""
    document_reference: str = ""  # snake_case, not Document_Reference
    sponsors: List[str] = field(default_factory=list)
    fiscal_impact: str = ""
    section_name: str = ""
    section_type: str = ""
    urls: List[Dict] = field(default_factory=list)  # Not Urls_Json, remove Has_Urls
    submitted_by: str = ""
    outcome_status: str = "Pending"  # e.g., Passed, Failed, Deferred, Tabled
    is_tabled: bool = False
    is_proclamation: bool = False  # NEW: Boolean to indicate if this agenda item is a proclamation
    
    def __post_init__(self):
        if self.node_type is None:
            self.node_type = NodeType.AGENDA_ITEM


@dataclass
class DocumentProperties(NodeProperties):
    """Properties for document nodes - standardized."""
    document_type: str = ""
    document_number: str = ""
    title: str = ""
    file_name: str = ""
    meeting_date: str = ""
    page_count: int = 0
    vote_details: Dict = field(default_factory=dict)
    motion: Dict = field(default_factory=dict)
    url: Optional[str] = None
    document_classification: str = ""  # resolution, ordinance, or verbatim
    passed_first_reading: bool = False
    passed_second_reading: bool = False
    
    def __post_init__(self):
        if self.node_type is None:
            self.node_type = NodeType.DOCUMENT


@dataclass
class PersonProperties(NodeProperties):
    """Properties for person nodes - standardized."""
    roles: str = ""  # Use 'roles' not 'role' to be clear it can be multiple
    
    def __post_init__(self):
        if self.node_type is None:
            self.node_type = NodeType.PERSON


@dataclass
class OrganizationProperties(NodeProperties):
    """Properties for organization nodes - standardized."""
    organization_type: str = "Organization"  # Clear naming, not org_type
    
    def __post_init__(self):
        if self.node_type is None:
            self.node_type = NodeType.ORGANIZATION


@dataclass
class DepartmentProperties(NodeProperties):
    """Properties for department nodes - standardized."""
    
    def __post_init__(self):
        if self.node_type is None:
            self.node_type = NodeType.DEPARTMENT


@dataclass
class LocationProperties(NodeProperties):
    """Properties for location nodes - standardized."""
    address: str = ""
    context: str = ""  # What this location is referenced in context of
    
    def __post_init__(self):
        if self.node_type is None:
            self.node_type = NodeType.LOCATION


@dataclass
class TranscriptProperties(NodeProperties):
    """Properties for transcript nodes - standardized."""
    filename: str = ""
    transcript_type: str = "item"  # snake_case
    meeting_date: str = ""
    page_count: int = 0
    item_info: str = ""
    items_covered: List[str] = field(default_factory=list)  # Keep as list, not string
    sections_covered: List[str] = field(default_factory=list)  # Keep as list, not string
    document_classification: str = "verbatim"  # NEW: Always verbatim for transcripts
    
    def __post_init__(self):
        if self.node_type is None:
            self.node_type = NodeType.DOCUMENT  # Transcripts are documents


@dataclass
class EdgeProperties:
    """Properties for edges with validation."""
    edge_type: EdgeType
    order: Optional[int] = None
    weight: float = 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for NetworkX edge attributes."""
        result = {'label': self.edge_type.value, 'weight': self.weight}
        if self.order is not None:
            result['order'] = self.order
        return result


class GraphBuilder:
    """Enhanced graph builder with improved data models and error handling."""
    
    def __init__(self, output_dir: Optional[Path] = None):
        self.output_dir = output_dir or Path("local_graph_data")
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize graph and LLM client
        self.graph = nx.DiGraph()
        self.llm_client = get_llm_client()
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "").split(" #")[0].strip().strip('"')
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=endpoint
        )
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o")
        
        # Node registry for efficient lookups and deduplication
        self.node_registry = {
            NodeType.MEETING: {},
            NodeType.SECTION: {},
            NodeType.AGENDA_ITEM: {},
            NodeType.DOCUMENT: {},
            NodeType.PERSON: {},
            NodeType.ORGANIZATION: {},
            NodeType.DEPARTMENT: {},
            NodeType.LOCATION: {}
        }
        
        # Track relationships for efficient querying
        self.item_ordering = {}  # {section_id: [ordered_item_ids]}
        
        # Add temporal index
        self.temporal_index = TemporalIndex()
        
        # Configuration for LLM usage
        self.use_llm_validation = os.getenv("USE_LLM_VALIDATION", "true").lower() == "true"
        self.llm_confidence_threshold = float(os.getenv("LLM_CONFIDENCE_THRESHOLD", "0.8"))
        
        # Track processed ordinances to avoid duplicates
        self.processed_ordinances = set()
        
        # Dynamic mapping for ordinance reference numbers to final ordinance numbers
        # Structure: {meeting_date: {agenda_item_code: {reference_number: final_ordinance_number}}}
        self.ordinance_mapping = {}
        
        # Statistics tracking
        self.stats = {
            'nodes_created': 0,
            'edges_created': 0,
            'errors': 0,
            'skipped_duplicates': 0,
            'llm_validations': 0,
            'llm_corrections': 0
        }
    
    async def build_graph_from_json(self, json_dir: Path) -> None:
        """Build hierarchical graph from Stage 3 JSON output with enhanced error handling."""
        log.info(f"🔗 Building hierarchical graph from JSON: {json_dir}")
        
        try:
            # Reset processed ordinances and mapping for fresh build
            self.processed_ordinances.clear()
            self.ordinance_mapping.clear()
            
            # Look for stage3 files in the organized subdirectory structure
            stage3_dir = json_dir / "stage3"
            if stage3_dir.exists():
                json_files = list(stage3_dir.glob("*_stage3_ontology.json"))
            else:
                # Fallback to flat structure for backward compatibility
                json_files = list(json_dir.glob("*_stage3_ontology.json"))
            
            log.info(f"Found {len(json_files)} Stage 3 JSON files")
            
            # Process enhanced legal document collections FIRST (higher priority)
            await self._process_enhanced_legal_document_collections_safe(json_dir)
            
            # Process agenda files to build hierarchy
            agenda_files = [f for f in json_files if 'agenda' in f.name.lower()]
            for agenda_file in agenda_files:
                await self._process_agenda_json_safe(agenda_file)
            
            # Process supporting documents
            for json_file in json_files:
                if json_file not in agenda_files:
                    await self._process_supporting_document_json_safe(json_file)
            
            # Process hierarchical verbatim transcript collections
            await self._process_verbatim_transcript_collections_safe(json_dir)
            
            # Compute graph metrics
            self._compute_graph_metrics()
            
            # New: Load and add NER data if available
            ner_dir = Path("simple_ner_graph")  # Adjust to your NER output dir
            ner_data = await self._load_ner_data(ner_dir)
            if ner_data:
                await self._add_ner_to_graph(ner_data)
            
            # Save graph
            self._save_graph()
            
            stats = self.get_graph_stats()
            log.info(f"✅ Graph building completed. Stats: {stats}")
            
            # Report data quality improvements
            self._report_data_quality_improvements()
            
        except Exception as e:
            log.error(f"Failed to build graph: {e}")
            self.stats['errors'] += 1
            raise
    
    async def _load_ner_data(self, ner_dir: Path) -> Dict[str, Any]:
        if not ner_dir.exists():
            return {"entities": {}, "relationships": {}}
        
        entity_index = json.loads((ner_dir / "entity_index.json").read_text())
        rel_index = json.loads((ner_dir / "relationship_index.json").read_text())
        
        return {"entities": entity_index, "relationships": rel_index}
    
    async def _add_ner_to_graph(self, ner_data: Dict) -> None:
        entities = ner_data.get("entities", {})
        for category, ents in entities.items():
            if category == "outcomes":
                # For outcomes category, create outcome nodes with full details
                for outcome_id, chunk_ids in ents.items():
                    # Read the outcome entity from file to get full details
                    outcome_details = await self._load_outcome_details(outcome_id, chunk_ids)
                    if outcome_details:
                        self.graph.add_node(outcome_id, type="vote_outcome", 
                            status=outcome_details.get('status'),
                            yes_votes=outcome_details.get('yes_votes'),
                            no_votes=outcome_details.get('no_votes'),
                            item_code=outcome_details.get('item_code'),
                            meeting_date=outcome_details.get('meeting_date'),
                            details=outcome_details.get('details'))
            else:
                for ent_name, chunk_ids in ents.items():
                    ent_id = f"ner_{category}_{hashlib.sha256(ent_name.encode()).hexdigest()[:12]}"
                    self.graph.add_node(ent_id, label=category, name=ent_name, chunks=chunk_ids)
        
        rels = ner_data.get("relationships", {})
        for rel_hash, rel in rels.items():
            if rel['relation'] == "has_outcome":
                # Edges from relationships (link item to outcome)
                item_id = f"ner_agenda_items_{hashlib.sha256(rel['source_entity'].encode()).hexdigest()[:12]}"
                outcome_id = rel['target_entity']
                self.graph.add_edge(item_id, outcome_id, label="has_outcome", 
                    details=rel.get('details'))
            else:
                # Use resolution (Fix for Issue 2)
                source_cat = self._find_entity_category(rel['source_entity'], entities)
                target_cat = self._find_entity_category(rel['target_entity'], entities)
                if not source_cat or not target_cat:  # Fix for Issue 3
                    log.warning(f"Skipping relationship with missing entities: {rel}")
                    continue
                
                source_id = f"ner_{source_cat}_{hashlib.sha256(rel['source_entity'].encode()).hexdigest()[:12]}"
                target_id = f"ner_{target_cat}_{hashlib.sha256(rel['target_entity'].encode()).hexdigest()[:12]}"
                self.graph.add_edge(source_id, target_id, label=rel['relation'], 
                    chunks=rel['chunk_ids'], docs=rel['source_documents'])
    
    async def _load_outcome_details(self, outcome_id: str, chunk_ids: List[str]) -> Optional[Dict]:
        """Load outcome details from file."""
        for chunk_id in chunk_ids:
            outcome_file = Path("simple_ner_graph") / "outcomes" / f"{chunk_id}_document.txt"
            if outcome_file.exists():
                try:
                    with open(outcome_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                        if "---" in content:
                            _, entity_section = content.split("---", 1)
                            for line in entity_section.strip().split("\n"):
                                line = line.strip()
                                if line:
                                    try:
                                        outcome_obj = json.loads(line)
                                        if outcome_obj.get('id') == outcome_id:
                                            return outcome_obj
                                    except json.JSONDecodeError:
                                        continue
                except Exception as e:
                    log.warning(f"Error reading outcome file {outcome_file}: {e}")
        return None
    
    def _find_entity_category(self, entity_name: str, entities: Dict) -> Optional[str]:
        """Returns category of entity or None if not found (Fix for Issue 2)"""
        for category, ents in entities.items():
            if entity_name in ents:
                return category
        return None
    
    def add_node_safe(self, properties: NodeProperties) -> Optional[str]:
        """Safely add a node with validation and deduplication."""
        try:
            # Check for existing node
            registry_key = self._get_registry_key(properties)
            if registry_key in self.node_registry[properties.node_type]:
                self.stats['skipped_duplicates'] += 1
                return self.node_registry[properties.node_type][registry_key]
            
            # Validate properties
            if not properties.node_id or not properties.node_type:
                log.warning(f"Invalid node properties: {properties}")
                return None
            
            # Add node to graph
            self.graph.add_node(properties.node_id, **properties.to_dict())
            
            # Register for deduplication
            self.node_registry[properties.node_type][registry_key] = properties.node_id
            self.stats['nodes_created'] += 1
            
            # After successfully adding node, update temporal index
            if properties.node_id:
                # Extract date from properties
                date_str = None
                if hasattr(properties, 'meeting_date'):
                    date_str = properties.meeting_date
                elif 'meeting_date' in properties.__dict__:
                    date_str = properties.__dict__.get('meeting_date')
                
                if date_str:
                    self.temporal_index.add_node(properties.node_id, date_str)
            
            return properties.node_id
            
        except Exception as e:
            log.error(f"Failed to add node {properties.node_id}: {e}")
            self.stats['errors'] += 1
            return None
    
    def add_edge_safe(self, source_id: str, target_id: str, edge_props: EdgeProperties) -> bool:
        """Safely add an edge with validation."""
        try:
            # Validate nodes exist
            if source_id not in self.graph.nodes or target_id not in self.graph.nodes:
                log.warning(f"Cannot create edge: nodes {source_id} or {target_id} don't exist")
                return False
            
            # Check for existing edge
            if self.graph.has_edge(source_id, target_id):
                return True  # Edge already exists
            
            # Add edge
            self.graph.add_edge(source_id, target_id, **edge_props.to_dict())
            self.stats['edges_created'] += 1
            return True
            
        except Exception as e:
            log.error(f"Failed to add edge {source_id} -> {target_id}: {e}")
            self.stats['errors'] += 1
            return False
    
    def _get_registry_key(self, properties: NodeProperties) -> str:
        """Generate a unique registry key for node deduplication."""
        if properties.node_type == NodeType.MEETING:
            return properties.meeting_date if hasattr(properties, 'meeting_date') else properties.name
        elif properties.node_type == NodeType.SECTION:
            # Use node_id for sections since it includes meeting info: meeting-01-23-2024-section-1
            return properties.node_id
        elif properties.node_type == NodeType.AGENDA_ITEM:
            # Use node_id for agenda items since it includes meeting info: meeting-01-23-2024-item-A-1  
            return properties.node_id
        elif properties.node_type == NodeType.DOCUMENT:
            doc_type = getattr(properties, 'document_type', '')
            doc_number = getattr(properties, 'document_number', '')
            return f"{doc_type}-{doc_number}"
        else:
            # For person, organization, department, location - use normalized name
            return self._normalize_name(properties.name)
    
    async def _process_agenda_json_safe(self, json_file: Path) -> None:
        """Process Stage 3 agenda JSON with proper URL propagation."""
        try:
            log.info(f"📄 Processing agenda JSON: {json_file.name}")
            
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            meeting_date = data.get('meeting_date', 'unknown')
            
            # Create Meeting node
            meeting_info = data.get('meeting_info', {})
            meeting_id = self._create_meeting_node_safe(meeting_info, meeting_date, json_file.name)
            if not meeting_id:
                return
            
            # Process officials and commissioners
            self._process_meeting_attendees_safe(meeting_id, meeting_info)
            
            # CRITICAL: Get hyperlinks from original data for URL association
            hyperlinks = data.get('hyperlinks', [])  # From Stage 1 PDF extraction
            
            # Process sections and agenda items
            for section_data in data.get('sections', []):
                section_id = self._create_section_node_safe(meeting_id, section_data)
                if not section_id:
                    continue
                
                self.item_ordering[section_id] = []
                items = section_data.get('items', [])
                
                if items:
                    previous_item_id = None
                    for idx, item in enumerate(items):
                        # CRITICAL: Associate URLs with agenda items
                        item_with_urls = self._associate_urls_with_item(item, hyperlinks)
                        
                        item_id = self._create_agenda_item_node_safe(section_id, item_with_urls, meeting_id)
                        if item_id:
                            self.item_ordering[section_id].append(item_id)
                            
                            # Create PRECEDES relationship
                            if previous_item_id:
                                edge_props = EdgeProperties(EdgeType.PRECEDES, order=idx)
                                self.add_edge_safe(previous_item_id, item_id, edge_props)
                            
                            # Process item references
                            self._process_item_references_safe(item_id, item_with_urls, meeting_id)
                            previous_item_id = item_id
            
            # Process additional entities
            for entity in data.get('entities', []):
                self._process_extracted_entity_safe(entity, meeting_id)
                
        except Exception as e:
            log.error(f"Failed to process agenda JSON {json_file}: {e}")
            self.stats['errors'] += 1

    def _associate_urls_with_item(self, item: Dict, hyperlinks: List[Dict]) -> Dict:
        """Associate hyperlinks with agenda items based on document reference."""
        item_with_urls = item.copy()
        item_urls = []
        
        # Get item identifiers
        item_code = item.get('item_code', '')
        doc_ref = item.get('document_reference', '')
        
        # Find matching URLs
        for link in hyperlinks:
            link_text = link.get('text', '').strip()
            link_url = link.get('url', '')
            
            # Match by document reference (primary)
            if doc_ref and doc_ref in link_text:
                item_urls.append({
                    'url': link_url,
                    'text': link_text,
                    'page': link.get('page', 0),
                    'match_type': 'document_reference'
                })
            # Match by item code (secondary)
            elif item_code and item_code in link_text:
                item_urls.append({
                    'url': link_url,
                    'text': link_text,
                    'page': link.get('page', 0),
                    'match_type': 'item_code'
                })
        
        # Add URLs to item
        if item_urls:
            item_with_urls['urls'] = item_urls
            log.debug(f"Associated {len(item_urls)} URLs with item {item_code}")
        
        return item_with_urls
    
    def _create_meeting_node_safe(self, meeting_info: Dict, meeting_date: str, source_file: str) -> Optional[str]:
        """Create meeting node and its corresponding agenda document node."""
        try:
            # Convert date format
            iso_date = self._convert_date_format(meeting_date)
            meeting_id = f"meeting-{meeting_date.replace('.', '-')}"
            
            properties = MeetingProperties(
                node_id=meeting_id,
                name=f"Meeting {meeting_date}",
                meeting_date=iso_date,
                meeting_type=meeting_info.get('type', 'Regular Meeting'),
                location=meeting_info.get('location', 'City Commission Chambers'),
                time=meeting_info.get('time', ''),
                source_file=source_file  # snake_case, not Source_File
            )
            
            meeting_node_id = self.add_node_safe(properties)
            if meeting_node_id:
                # Create agenda_document node with page count lookup
                agenda_doc_id = f"agenda-{meeting_date.replace('.', '-')}"
                
                # Try to find the corresponding agenda JSON file to get page count
                page_count = self._lookup_agenda_page_count(source_file, meeting_date)
                
                agenda_props = DocumentProperties(
                    node_id=agenda_doc_id,
                    name=f"Agenda Document for {meeting_date}",
                    document_type="agenda_document",
                    title=f"Agenda for {meeting_info.get('type', 'Meeting')} of {meeting_date}",
                    file_name=source_file,
                    meeting_date=iso_date,
                    page_count=page_count
                )
                # NEW: Add parent meeting ID to agenda document
                setattr(agenda_props, 'parent_meeting_id', meeting_id)
                agenda_node_id = self.add_node_safe(agenda_props)
                if agenda_node_id:
                    # Create HAS_AGENDA edge from meeting to agenda_document
                    edge_props = EdgeProperties(EdgeType.HAS_AGENDA)
                    self.add_edge_safe(meeting_node_id, agenda_node_id, edge_props)
                    
                    # Store agenda_doc_id for section creation
                    setattr(self, '_current_agenda_doc_id', agenda_doc_id)
            return meeting_node_id
            
        except Exception as e:
            log.error(f"Failed to create meeting node: {e}")
            return None
    
    def _create_section_node_safe(self, meeting_id: str, section_data: Dict) -> Optional[str]:
        """Create section node with standardized properties."""
        try:
            section_order = section_data.get('section_order', 0)
            section_name = section_data.get('section_name', f'Section {section_order}')
            section_id = f"{meeting_id}-section-{section_order}"
            
            properties = SectionProperties(
                node_id=section_id,
                name=section_name,
                title=section_name,
                section_type=section_data.get('section_type', 'GENERAL'),
                order=section_order,
                is_empty=len(section_data.get('items', [])) == 0,  # snake_case
                description=section_data.get('description', 'No items in this section' if len(section_data.get('items', [])) == 0 else '')
            )
            # NEW: Add parent agenda document ID to section
            agenda_doc_id = getattr(self, '_current_agenda_doc_id', None)
            if agenda_doc_id:
                setattr(properties, 'parent_agenda_doc_id', agenda_doc_id)
            
            node_id = self.add_node_safe(properties)
            if node_id:
                # Get agenda_doc_id from stored attribute
                agenda_doc_id = getattr(self, '_current_agenda_doc_id', None)
                if agenda_doc_id:
                    # Create HAS_SECTION relationship from agenda_document to section
                    edge_props = EdgeProperties(EdgeType.HAS_SECTION, order=section_order)
                    self.add_edge_safe(agenda_doc_id, section_id, edge_props)
                else:
                    # Fallback to old behavior if agenda_doc_id not found
                    log.warning(f"No agenda_doc_id found for section {section_id}, falling back to meeting connection")
                    edge_props = EdgeProperties(EdgeType.HAS_SECTION, order=section_order)
                    self.add_edge_safe(meeting_id, section_id, edge_props)
            
            return node_id
            
        except Exception as e:
            log.error(f"Failed to create section node {section_id}: {e}")
            return None
    
    def _create_agenda_item_node_safe(self, section_id: str, item_data: Dict, meeting_id: str) -> Optional[str]:
        """Create agenda item node with standardized properties."""
        try:
            item_code = item_data.get('item_code', '')
            if not item_code or item_code == 'NONE':
                return None
            
            item_id = f"{meeting_id}-item-{item_code}"
            
            # Extract URLs properly
            urls = []
            if 'urls' in item_data and item_data['urls']:
                urls = item_data['urls'] if isinstance(item_data['urls'], list) else []
            
            # Determine outcome status from linked legal documents
            outcome_status = self._determine_outcome_status(item_data)
            
            # Determine if this is a proclamation
            is_proclamation = self._is_proclamation(item_data)
            
            properties = AgendaItemProperties(
                node_id=item_id,
                name=f"Item {item_code}",
                item_code=item_code,
                title=item_data.get('title', f'Agenda Item {item_code}'),
                description=item_data.get('description', ''),
                document_reference=item_data.get('document_reference', ''),
                sponsors=item_data.get('sponsors', []),
                fiscal_impact=item_data.get('fiscal_impact', ''),
                section_name=item_data.get('section_name', ''),
                section_type=item_data.get('section_type', ''),
                urls=urls,  # Standardized as list
                submitted_by=item_data.get('submitted_by', ''),
                outcome_status=outcome_status,
                is_tabled=item_data.get('is_tabled', False),
                is_proclamation=is_proclamation
            )
            # NEW: Add parent section ID to agenda item
            setattr(properties, 'parent_section_id', section_id)
            
            node_id = self.add_node_safe(properties)
            if node_id:
                # Create CONTAINS_ITEM relationship
                edge_props = EdgeProperties(EdgeType.CONTAINS_ITEM, order=item_data.get('item_order', 0))
                self.add_edge_safe(section_id, item_id, edge_props)
                
                # Extract and create rich semantic relationships
                self._create_item_relationships(item_id, item_data, meeting_id)
            
            return node_id
            
        except Exception as e:
            log.error(f"Failed to create agenda item node: {e}")
            return None
    
    def _determine_outcome_status(self, item_data: Dict) -> str:
        """Determine outcome status from agenda item data and context."""
        try:
            # Check if explicitly provided
            if 'outcome_status' in item_data and item_data['outcome_status'] != 'Pending':
                return item_data['outcome_status']
            
            # PHASE 1: Regex-based analysis
            text_to_check = f"{item_data.get('title', '')} {item_data.get('description', '')}"
            
            # Look for explicit status indicators in text
            status_patterns = {
                'Passed': [
                    r'passed\s+(?:and\s+)?adopted',
                    r'unanimously\s+(?:passed|adopted)',
                    r'approved',
                    r'ayes:.*unanimous',
                    r'unanimous.*vote'
                ],
                'Failed': [
                    r'failed',
                    r'defeated',
                    r'rejected',
                    r'nays:\s*.*yeas:\s*none'
                ],
                'Deferred': [
                    r'deferred',
                    r'postponed',
                    r'continued',
                    r'tabled'
                ],
                'First Reading Passed': [
                    r'passed\s+on\s+first\s+reading',
                    r'first\s+reading.*passed'
                ]
            }
            
            regex_outcome = "Pending"
            for status, patterns in status_patterns.items():
                for pattern in patterns:
                    if re.search(pattern, text_to_check, re.IGNORECASE):
                        regex_outcome = status
                        break
                if regex_outcome != "Pending":
                    break
            
            # Check for voting information in description  
            description = item_data.get('description', '')
            if description and regex_outcome == "Pending":
                # Look for unanimous votes
                if re.search(r'unanimous', description, re.IGNORECASE):
                    regex_outcome = "Passed"
                
                # Look for specific vote counts
                yeas_match = re.search(r'(?:ayes?|yeas?):\s*([^;.]+)', description, re.IGNORECASE)
                nays_match = re.search(r'(?:nays?|nos?):\s*([^;.]+)', description, re.IGNORECASE)
                
                if yeas_match:
                    yeas_text = yeas_match.group(1).strip().lower()
                    nays_text = nays_match.group(1).strip().lower() if nays_match else ""
                    
                    # If there are yeas and either no nays or nays is "none"
                    if yeas_text and (not nays_text or nays_text in ['none', 'absent', '']):
                        regex_outcome = "Passed"
                    elif yeas_text and nays_text and nays_text not in ['none', 'absent', '']:
                        # Count actual votes
                        yeas_count = len([name.strip() for name in yeas_text.split(',') if name.strip()])
                        nays_count = len([name.strip() for name in nays_text.split(',') if name.strip() and name.strip() not in ['none', 'absent']])
                        regex_outcome = "Passed" if yeas_count > nays_count else "Failed"
            
            # Check if it's tabled
            if item_data.get('is_tabled', False):
                regex_outcome = "Tabled"
            
            # Default based on document type
            if regex_outcome == "Pending":
                doc_ref = item_data.get('document_reference', '')
                if doc_ref:
                    # If it has a document reference (resolution/ordinance), it likely passed
                    regex_outcome = "Passed"
            
            # PHASE 2: LLM Validation (if enabled and there's substantial text to analyze)
            if self.use_llm_validation and len(text_to_check.strip()) > 50:  # Only use LLM if enabled and there's enough text
                try:
                    self.stats['llm_validations'] += 1
                    llm_outcome = self._validate_outcome_with_llm(item_data, regex_outcome)
                    if llm_outcome and llm_outcome != regex_outcome:
                        self.stats['llm_corrections'] += 1
                        log.info(f"LLM corrected outcome from '{regex_outcome}' to '{llm_outcome}' for item {item_data.get('item_code', 'unknown')}")
                        return llm_outcome
                except Exception as e:
                    log.debug(f"LLM validation failed for outcome status: {e}")
            
            return regex_outcome
            
        except Exception as e:
            log.error(f"Error determining outcome status: {e}")
            return "Pending"

    def _is_proclamation(self, item_data: Dict) -> bool:
        """Determine if this agenda item is a proclamation based on title."""
        try:
            title = item_data.get('title', '').lower()
            description = item_data.get('description', '').lower()
            section_name = item_data.get('section_name', '').lower()
            
            # Check for proclamation indicators in title
            proclamation_indicators = [
                'proclamation',
                'proclaiming',
                'proclaimed',
                'proclamation of',
                'recognition of',
                'honoring',
                'commemorating',
                'celebrating',
                'recognizing',
                'declaring',
                'week',
                'month',
                'day',
                'awareness',
                'appreciation'
            ]
            
            # Check title for proclamation indicators
            for indicator in proclamation_indicators:
                if indicator in title:
                    return True
            
            # Check description for proclamation indicators
            for indicator in proclamation_indicators:
                if indicator in description:
                    return True
            
            # Check section name for proclamation indicators
            if 'proclamation' in section_name:
                return True
            
            return False
            
        except Exception as e:
            log.error(f"Error determining if item is proclamation: {e}")
            return False

    def _validate_outcome_with_llm(self, item_data: Dict, regex_outcome: str) -> Optional[str]:
        """Use LLM to validate agenda item outcome status."""
        
        text_content = f"""
        Item Code: {item_data.get('item_code', 'N/A')}
        Title: {item_data.get('title', 'N/A')}
        Description: {item_data.get('description', 'N/A')}
        Document Reference: {item_data.get('document_reference', 'N/A')}
        Section: {item_data.get('section_name', 'N/A')}
        """
        
        prompt = f"""Analyze this City Commission agenda item to determine its outcome status.

AGENDA ITEM DETAILS:
{text_content.strip()}

REGEX ANALYSIS RESULT: {regex_outcome}

Determine the ACTUAL outcome status based on the text. Look for:
- Voting results (unanimous, ayes/nays counts)
- Status indicators (passed, failed, deferred, tabled)
- Reading information (first reading, final adoption)
- Motion results

Respond with ONLY one of these exact values:
- "Passed" (if item was approved/adopted)
- "Failed" (if item was rejected/defeated)  
- "First Reading Passed" (if passed first reading only)
- "Deferred" (if postponed/continued/tabled)
- "Pending" (if no clear outcome or not yet voted)

RESPOND WITH ONLY THE STATUS - NO EXPLANATION."""

        try:
            messages = [
                {
                    "role": "system",
                    "content": "You are analyzing City Commission agenda items to determine their outcome status. Be precise and only return the exact status value requested."
                },
                {
                    "role": "user", 
                    "content": prompt
                }
            ]
            
            result = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0,
                max_tokens=50
            )
            
            if result and result.choices:
                outcome = result.choices[0].message.content.strip().strip('"')
                
                # Validate the response
                valid_outcomes = ["Passed", "Failed", "First Reading Passed", "Deferred", "Pending"]
                if outcome in valid_outcomes:
                    return outcome
                    
        except Exception as e:
            log.debug(f"LLM outcome validation failed: {e}")
            
        return None

    def _create_item_relationships(self, item_id: str, item_data: Dict, meeting_id: str) -> None:
        """Create rich semantic relationships for agenda items."""
        try:
            log.info(f"Creating relationships for item: {item_id}")
            # Extract sponsors
            sponsors = item_data.get('sponsors', [])
            if sponsors:
                for sponsor in sponsors:
                    if sponsor and sponsor != 'N/A':
                        person_id = self._create_person_node_safe(sponsor, 'Sponsor')
                        if person_id:
                            edge_props = EdgeProperties(EdgeType.SPONSORED_BY)
                            self.add_edge_safe(item_id, person_id, edge_props)
                            log.info(f"  - Created SPONSORED_BY edge to {person_id}")
            else:
                log.info(f"  - No 'sponsors' field found for {item_id}")

            # Extract voting information from item text
            item_text = f"{item_data.get('title', '')} {item_data.get('description', '')}"
            if item_text.strip():
                log.info(f"  - Scanning text for voting, motion, and appointment relationships: '{item_text[:100]}...'")
                self._extract_voting_relationships(item_id, item_text)
                self._extract_appointment_relationships(item_id, item_data)
                self._extract_motion_relationships(item_id, item_text)
            else:
                log.info(f"  - No text available for relationship extraction for {item_id}")

        except Exception as e:
            log.error(f"Failed to create item relationships for {item_id}: {e}")

    def _extract_voting_relationships(self, item_id: str, text: str) -> None:
        """Extract voting relationships from text."""
        try:
            # Look for voting patterns
            vote_patterns = [
                r'Yeas?:\s*([^)]+)',
                r'Ayes?:\s*([^)]+)', 
                r'Yes:\s*([^)]+)',
                r'Nays?:\s*([^)]+)',
                r'No:\s*([^)]+)',
                r'Abstain[ed]*:\s*([^)]+)'
            ]
            
            for pattern in vote_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    voters = [name.strip() for name in match.split(',') if name.strip()]
                    for voter in voters:
                        person_id = self._create_person_node_safe(voter, 'Commissioner')
                        if person_id:
                            if 'yea' in pattern.lower() or 'aye' in pattern.lower() or 'yes' in pattern.lower():
                                edge_props = EdgeProperties(EdgeType.VOTED_YES)
                            elif 'nay' in pattern.lower() or 'no' in pattern.lower():
                                edge_props = EdgeProperties(EdgeType.VOTED_NO)
                            else:
                                edge_props = EdgeProperties(EdgeType.ABSTAINED)
                            
                            self.add_edge_safe(person_id, item_id, edge_props)
                            
        except Exception as e:
            log.error(f"Failed to extract voting relationships: {e}")

    def _extract_appointment_relationships(self, item_id: str, item_data: Dict) -> None:
        """Extract appointment and board membership relationships."""
        try:
            title = item_data.get('title', '').lower()
            description = item_data.get('description', '').lower()
            text = f"{title} {description}"
            
            # Extract appointments
            appointment_patterns = [
                r'appointing\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                r'appointment\s+of\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                r'nominated\s+by\s+([A-Z][a-z]+\s+[A-Z][a-z]+)'
            ]
            
            for pattern in appointment_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for person_name in matches:
                    person_id = self._create_person_node_safe(person_name, 'Appointee')
                    if person_id:
                        edge_props = EdgeProperties(EdgeType.APPOINTED_TO)
                        self.add_edge_safe(person_id, item_id, edge_props)
            
            # Extract board/committee relationships
            board_patterns = [
                r'([A-Z][a-z\s]+Board)',
                r'([A-Z][a-z\s]+Committee)', 
                r'([A-Z][a-z\s]+Commission)'
            ]
            
            for pattern in board_patterns:
                matches = re.findall(pattern, text)
                for board_name in matches:
                    dept_id = self._create_department_node_safe(board_name.strip())
                    if dept_id:
                        edge_props = EdgeProperties(EdgeType.MEMBER_OF)
                        self.add_edge_safe(item_id, dept_id, edge_props)
                        
        except Exception as e:
            log.error(f"Failed to extract appointment relationships: {e}")

    def _extract_motion_relationships(self, item_id: str, text: str) -> None:
        """Extract motion and seconding relationships."""
        try:
            # Extract who moved the motion
            motion_patterns = [
                r'Moved\s+by:\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                r'Motion\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+moved'
            ]
            
            for pattern in motion_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for person_name in matches:
                    person_id = self._create_person_node_safe(person_name, 'Commissioner')
                    if person_id:
                        edge_props = EdgeProperties(EdgeType.MOVED_BY)
                        self.add_edge_safe(item_id, person_id, edge_props)
            
            # Extract who seconded
            second_patterns = [
                r'Seconded\s+by:\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                r'Second\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+seconded'
            ]
            
            for pattern in second_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for person_name in matches:
                    person_id = self._create_person_node_safe(person_name, 'Commissioner')
                    if person_id:
                        edge_props = EdgeProperties(EdgeType.SECONDED_BY)
                        self.add_edge_safe(item_id, person_id, edge_props)
                        
        except Exception as e:
            log.error(f"Failed to extract motion relationships: {e}")
    
    def _lookup_agenda_page_count(self, source_file: str, meeting_date: str) -> int:
        """Look up page count for agenda documents from stage1 JSON files."""
        try:
            # Try to find the agenda JSON file for this meeting
            from pathlib import Path
            import json
            
            stage1_dir = Path('city_clerk_documents/extracted_json/stage1')
            if not stage1_dir.exists():
                return 0
            
            # Try different patterns to match agenda files (handle both single and double digit formats)
            # Convert 01.09.2024 to 01.9.2024 format that files actually use
            date_parts = meeting_date.split('.')
            if len(date_parts) == 3:
                # Remove leading zero from month: 01.09.2024 -> 01.9.2024
                normalized_date = f"{date_parts[0]}.{int(date_parts[1])}.{date_parts[2]}"
            else:
                normalized_date = meeting_date
            
            patterns = [
                f"Agenda {normalized_date}_stage1_ocr.json",  # "Agenda 01.9.2024_stage1_ocr.json"
                f"Agenda {meeting_date}_stage1_ocr.json",     # "Agenda 01.09.2024_stage1_ocr.json"  
                f"Agenda {meeting_date.replace('.', '_')}_stage1_ocr.json",  # "Agenda 01_09_2024_stage1_ocr.json"
                f"Agenda_{meeting_date.replace('.', '_')}_stage1_ocr.json",   # "Agenda_01_09_2024_stage1_ocr.json"
                f"agenda_{meeting_date.replace('.', '_')}_stage1_ocr.json",   # lowercase
            ]
            
            # Also try to find any agenda file that contains the date (case-insensitive)
            for agenda_file in stage1_dir.glob("*_stage1_ocr.json"):
                if ('agenda' in agenda_file.name.lower() and 
                    (meeting_date.replace('.', '.') in agenda_file.name or meeting_date.replace('.', '_') in agenda_file.name)):
                    patterns.append(agenda_file.name)
            
            for pattern in patterns:
                agenda_file = stage1_dir / pattern
                if agenda_file.exists():
                    try:
                        with open(agenda_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        # Use the same backward compatibility logic
                        page_count = self._calculate_page_count(data, pattern)
                        if page_count > 0:
                            log.debug(f"Found agenda page count from {pattern}: {page_count}")
                            return page_count
                    except Exception as e:
                        log.debug(f"Failed to read agenda file {pattern}: {e}")
                        continue
            
            log.debug(f"No agenda JSON file found for meeting {meeting_date}")
            return 0
            
        except Exception as e:
            log.error(f"Error looking up agenda page count: {e}")
            return 0

    def _calculate_page_count_for_referenced_document(self, doc_number: str, meeting_date: str, doc_type: str, item_data: Dict, file_name: str) -> int:
        """Calculate page count for documents referenced in agenda items but may not have JSON extraction files."""
        try:
            from pathlib import Path
            import json
            import fitz  # PyMuPDF for direct PDF page counting
            
            # PRIORITY 1: Try to find existing JSON extraction file
            stage1_dir = Path('city_clerk_documents/extracted_json/stage1')
            if stage1_dir.exists():
                # Try different naming patterns for the JSON file
                json_patterns = [
                    f"{doc_number} - {meeting_date.replace('.', '_')}_stage1_ocr.json",
                    f"{doc_number}_{meeting_date.replace('.', '_')}_stage1_ocr.json", 
                    f"{doc_number}_stage1_ocr.json",
                ]
                
                for pattern in json_patterns:
                    json_file = stage1_dir / pattern
                    if json_file.exists():
                        try:
                            with open(json_file, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            page_count = self._calculate_page_count(data, pattern)
                            if page_count > 0:
                                log.debug(f"Found page count from JSON {pattern}: {page_count}")
                                return page_count
                        except Exception as e:
                            log.debug(f"Failed to read JSON file {pattern}: {e}")
                            continue
            
            # PRIORITY 2: Try to find and count pages in actual PDF file
            pdf_patterns = [
                f"city_clerk_documents/**/resolutions/**/{doc_number}*.pdf",
                f"city_clerk_documents/**/ordinances/**/{doc_number}*.pdf", 
                f"city_clerk_documents/**/{doc_number}*.pdf",
                f"city_clerk_documents/**/*{doc_number}*.pdf",
            ]
            
            for pattern in pdf_patterns:
                pdf_files = list(Path('.').glob(pattern))
                if pdf_files:
                    pdf_file = pdf_files[0]  # Use first match
                    try:
                        with fitz.open(str(pdf_file)) as doc:
                            page_count = len(doc)
                        if page_count > 0:
                            log.info(f"🔍 Found PDF {pdf_file.name} for referenced document {doc_number}: {page_count} pages")
                            return page_count
                    except Exception as e:
                        log.debug(f"Failed to read PDF {pdf_file}: {e}")
                        continue
            
            # PRIORITY 3: Use agenda item metadata if available
            page_count = self._calculate_page_count(item_data, file_name)
            if page_count > 0:
                return page_count
            
            # PRIORITY 4: Document referenced but not available
            log.debug(f"📄 Referenced document {doc_number} not found in file system - marking as unavailable")
            return 0  # Keep as 0 to indicate missing document
            
        except Exception as e:
            log.error(f"Error calculating page count for referenced document {doc_number}: {e}")
            return 0

    def _calculate_page_count(self, data: Dict, fallback_file: str = "") -> int:
        """Calculate page count from metadata with backward compatibility."""
        try:
            # PRIORITY 1: Check metadata for page count (new format) or fallback to legacy formats
            metadata = data.get('metadata', {})
            page_count = metadata.get('page_count', 0) or metadata.get('actual_page_count', 0) or metadata.get('num_pages', 0)
            if page_count:
                log.debug(f"Using page count from metadata: {page_count}")
                return page_count
            
            # PRIORITY 2: Try to get from original stage1 OCR file
            if fallback_file and 'enhanced' in fallback_file:
                # Try to find the original stage1 OCR file
                ocr_file_name = fallback_file.replace('_enhanced_resolution.json', '_stage1_ocr.json').replace('_enhanced_ordinance.json', '_stage1_ocr.json')
                ocr_file_path = Path('city_clerk_documents/extracted_json') / ocr_file_name
                
                if ocr_file_path.exists():
                    try:
                        with open(ocr_file_path, 'r', encoding='utf-8') as f:
                            ocr_data = json.load(f)
                        
                        # Check for actual page count in OCR metadata
                        ocr_metadata = ocr_data.get('metadata', {})
                        if 'actual_page_count' in ocr_metadata:
                            actual_count = int(ocr_metadata['actual_page_count'])
                            log.debug(f"Found actual page count from OCR metadata: {actual_count}")
                            return actual_count
                        elif 'num_pages' in ocr_metadata:
                            num_pages = int(ocr_metadata['num_pages'])
                            log.debug(f"Found num_pages from OCR metadata: {num_pages}")
                            return num_pages
                            
                    except Exception as e:
                        log.debug(f"Could not read OCR file {ocr_file_path}: {e}")
            
            # PRIORITY 3: Estimate from text length if available
            if 'full_text' in data and data['full_text'] and data['full_text'] not in ['CONVERTED - JSON to markdown', 'SKIPPED - Already processed']:
                # Rough estimation: ~3000 characters per page for legal documents
                text_length = len(data['full_text'])
                estimated_pages = max(1, text_length // 3000)
                log.debug(f"Estimated {estimated_pages} pages from text length {text_length}")
                return estimated_pages
            
            # PRIORITY 4: Default to empty page count (per user preference)
            log.debug(f"Could not determine page count for {fallback_file}, leaving empty")
            return 0  # Leave empty rather than defaulting to 1
            
        except Exception as e:
            log.error(f"Error calculating page count: {e}")
            return 0  # Leave empty on error

    def _create_document_node_safe(self, doc_type: str, doc_number: str, title: str) -> Optional[str]:
        """Create document node with standardized properties."""
        try:
            doc_id = f"doc-{doc_type.lower()}-{doc_number}"
            
            properties = DocumentProperties(
                node_id=doc_id,
                name=f"{doc_type} {doc_number}",
                document_type=doc_type,
                document_number=doc_number,
                title=title,  # Single title field, no duplication
                page_count=1  # Default to 1 for basic documents
            )
            
            return self.add_node_safe(properties)
            
        except Exception as e:
            log.error(f"Failed to create document node: {e}")
            return None

    def _create_document_node_enhanced_safe(self, doc_type: str, doc_number: str, title: str, 
                                          meeting_date: str, url: Optional[str], item_data: Dict) -> Optional[str]:
        """Create document node with enhanced properties including all fields."""
        try:
            doc_id = f"doc-{doc_type.lower()}-{doc_number}"
            
            # Extract file name - try to construct from doc number and meeting date
            file_name = ""
            if doc_number and meeting_date:
                date_for_filename = meeting_date.replace('.', '_')
                file_name = f"{doc_number} - {date_for_filename}.pdf"
            
            # Determine document classification
            doc_classification = "document"
            if doc_type.lower() == 'resolution':
                doc_classification = "resolution"
            elif doc_type.lower() == 'ordinance':
                doc_classification = "ordinance"
            
            # Extract vote details and motion from item description
            vote_details = {}
            motion_details = {}
            description = item_data.get('description', '')
            if description:
                if 'unanimous' in description.lower():
                    vote_details['unanimous'] = True
                ayes_match = re.search(r'ayes?:\s*([^;.]+)', description, re.IGNORECASE)
                if ayes_match:
                    vote_details['ayes'] = ayes_match.group(1).strip()
                nays_match = re.search(r'nays?:\s*([^;.]+)', description, re.IGNORECASE)
                if nays_match:
                    vote_details['nays'] = nays_match.group(1).strip()
            
            # Extract agenda_item_type
            agenda_item_type = item_data.get('item_type', '')
            if not agenda_item_type:
                section_type = item_data.get('section_type', '')
                if 'ORDINANCE' in section_type.upper():
                    agenda_item_type = 'ORDINANCE_ITEM'
                elif 'RESOLUTION' in section_type.upper():
                    agenda_item_type = 'RESOLUTION_ITEM'
                else:
                    agenda_item_type = section_type or 'GENERAL'
            
            # Calculate page count using improved method
            # For documents created from agenda references, try to find actual PDF/JSON files
            page_count = self._calculate_page_count_for_referenced_document(doc_number, meeting_date, doc_type, item_data, file_name)
            
            # Determine reading status from item data or enhanced metadata
            passed_first_reading = item_data.get('passed_first_reading', False)
            passed_second_reading = item_data.get('passed_second_reading', False)
            
            # If not found in item_data, check section context and enhanced metadata extraction patterns
            if not passed_first_reading and not passed_second_reading:
                # PRIORITY 1: Check section name context (most reliable)
                section_name = item_data.get('section_name', '').upper()
                
                if 'ORDINANCES ON FIRST READING' in section_name:
                    passed_first_reading = True
                    log.debug(f"Set passed_first_reading=True for {doc_number} (section: {section_name})")
                elif 'ORDINANCES ON SECOND READING' in section_name:
                    passed_first_reading = True
                    passed_second_reading = True
                    log.debug(f"Set passed_second_reading=True for {doc_number} (section: {section_name})")
                
                # PRIORITY 2: Check text patterns if section context is not available
                if not passed_first_reading and not passed_second_reading:
                    text_to_check = f"{title} {item_data.get('description', '')}"
                    
                    # Check for first reading patterns
                    if re.search(r'passed\s+on\s+first\s+reading|first\s+reading.*passed', text_to_check, re.IGNORECASE):
                        passed_first_reading = True
                    
                    # Check for final passage patterns  
                    if re.search(r'passed\s+and\s+adopted|adopted\s+this.*day\s+of', text_to_check, re.IGNORECASE):
                        passed_second_reading = True
                    elif vote_details and vote_details.get('unanimous'):
                        # If unanimous vote and no explicit first reading, assume final passage
                        passed_second_reading = True
            
            # LOGIC FIX: If passed_second_reading is True, passed_first_reading must also be True
            if passed_second_reading and not passed_first_reading:
                passed_first_reading = True
                log.debug(f"Set passed_first_reading=True for {doc_number} (logic: second reading passed implies first reading passed)")
            
            properties = DocumentProperties(
                node_id=doc_id,
                name=f"{doc_type} {doc_number}",
                document_type=doc_type,
                document_number=doc_number,
                title=title,
                file_name=file_name,
                meeting_date=meeting_date,
                page_count=page_count,
                vote_details=vote_details,
                motion=motion_details,
                url=url,
                document_classification=doc_classification,
                passed_first_reading=passed_first_reading,
                passed_second_reading=passed_second_reading
            )
            
            # NEW: Add parent agenda item ID if available
            agenda_item_code = item_data.get('item_code') or item_data.get('agenda_item_code')
            if agenda_item_code:
                setattr(properties, 'parent_agenda_item_id', f"meeting-{meeting_date.replace('.', '-')}-item-{agenda_item_code}")
            else:
                setattr(properties, 'parent_agenda_item_id', None)
            
            node_id = self.add_node_safe(properties)
            if node_id and node_id in self.graph.nodes:
                self.graph.nodes[node_id]['agenda_item_type'] = agenda_item_type
            
            return node_id
            
        except Exception as e:
            log.error(f"Failed to create enhanced document node: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _create_person_node_safe(self, name: str, role: str = '') -> Optional[str]:
        """Create person node with standardized properties."""
        try:
            normalized_name = self._normalize_name(name)
            person_id = f"person-{normalized_name.lower().replace(' ', '-')}"
            
            # Check if person already exists and update roles
            existing_id = self.node_registry[NodeType.PERSON].get(normalized_name)
            if existing_id and role:
                current_roles = self.graph.nodes[existing_id].get('roles', '')
                roles_set = set(current_roles.split(', ') if current_roles else [])
                roles_set.add(role)
                self.graph.nodes[existing_id]['roles'] = ', '.join(sorted(roles_set))
                return existing_id
            
            properties = PersonProperties(
                node_id=person_id,
                name=name,
                roles=role  # Use 'roles' not 'role'
            )
            
            return self.add_node_safe(properties)
            
        except Exception as e:
            log.error(f"Failed to create person node: {e}")
            return None
    
    def _create_organization_node_safe(self, name: str, org_type: str = 'Organization') -> Optional[str]:
        """Create organization node with standardized properties."""
        try:
            normalized_name = self._normalize_name(name)
            org_id = f"org-{normalized_name.lower().replace(' ', '-')}"
            
            properties = OrganizationProperties(
                node_id=org_id,
                name=name,
                organization_type=org_type  # Clear naming
            )
            
            return self.add_node_safe(properties)
            
        except Exception as e:
            log.error(f"Failed to create organization node: {e}")
            return None
    
    def _create_department_node_safe(self, name: str) -> Optional[str]:
        """Safely create department node with validation."""
        try:
            normalized_name = self._normalize_name(name)
            dept_id = f"dept-{normalized_name.lower().replace(' ', '-')}"
            
            properties = DepartmentProperties(
                node_id=dept_id,
                name=name
            )
            
            return self.add_node_safe(properties)
            
        except Exception as e:
            log.error(f"Failed to create department node: {e}")
            return None
    
    def _create_location_node_safe(self, name: str, address: str = '', context: str = '') -> Optional[str]:
        """Create location node with standardized properties."""
        try:
            normalized_name = self._normalize_name(name)
            location_id = f"loc-{normalized_name.lower().replace(' ', '-')}"
            
            properties = LocationProperties(
                node_id=location_id,
                name=name,
                address=address,
                context=context  # What context this location appears in
            )
            
            return self.add_node_safe(properties)
            
        except Exception as e:
            log.error(f"Failed to create location node: {e}")
            return None
    
    def _process_meeting_attendees_safe(self, meeting_id: str, meeting_info: Dict) -> None:
        """Create people nodes and meaningful relationships to meeting."""
        try:
            # Process officials with specific roles and relationships
            officials = meeting_info.get('officials', {})
            role_mapping = {
                'mayor': 'Mayor',
                'vice_mayor': 'Vice Mayor', 
                'city_manager': 'City Manager',
                'city_attorney': 'City Attorney',
                'city_clerk': 'City Clerk'
            }
            
            for role_key, name in officials.items():
                if name and name != 'N/A':
                    role = role_mapping.get(role_key, role_key.replace('_', ' ').title())
                    person_id = self._create_person_node_safe(name, role)
                    if person_id:
                        # Create specific role-based relationship
                        if role_key == 'mayor':
                            edge_props = EdgeProperties(EdgeType.CHAIRS)
                        else:
                            edge_props = EdgeProperties(EdgeType.ATTENDED_BY)
                        self.add_edge_safe(meeting_id, person_id, edge_props)
            
            # Process commissioners with attendance relationships
            for commissioner in meeting_info.get('commissioners', []):
                if commissioner and commissioner != 'N/A':
                    person_id = self._create_person_node_safe(commissioner, 'Commissioner')
                    if person_id:
                        edge_props = EdgeProperties(EdgeType.ATTENDED_BY)
                        self.add_edge_safe(meeting_id, person_id, edge_props)
                        
        except Exception as e:
            log.error(f"Failed to process meeting attendees: {e}")
            self.stats['errors'] += 1
    
    def _process_item_references_safe(self, item_id: str, item_data: Dict, meeting_id: str) -> None:
        """Safely process item references with error handling."""
        try:
            # Handle document references
            doc_ref = item_data.get('document_reference')
            if doc_ref:
                doc_type = self._determine_document_type(item_data)
                log.info(f"Processing document reference {doc_ref}: determined type = '{doc_type}'")
                
                # Extract meeting date from meeting_id
                meeting_date = meeting_id.replace('meeting-', '').replace('-', '.')
                
                # DYNAMIC MAPPING: Check if this reference number maps to a final ordinance number
                item_code = item_data.get('item_code', '')
                should_skip = False
                mapped_final_number = None
                
                if item_code and meeting_date in self.ordinance_mapping:
                    # Check if this agenda item has a mapping
                    if item_code in self.ordinance_mapping[meeting_date]:
                        mapping_data = self.ordinance_mapping[meeting_date][item_code]
                        mapped_final_number = mapping_data.get('final_ordinance_number')
                        
                        if mapped_final_number and mapped_final_number in self.processed_ordinances:
                            should_skip = True
                            log.info(f"🔄 SKIPPING duplicate: Reference {doc_ref} (item {item_code}) maps to final number {mapped_final_number} which is already processed")
                
                # If we should skip, just create a reference to the existing enhanced document
                if should_skip and mapped_final_number:
                    enhanced_doc_id = f"doc-{doc_type.lower()}-{mapped_final_number}"
                    if enhanced_doc_id in self.graph.nodes:
                        log.info(f"✅ Using existing enhanced document: {enhanced_doc_id}")
                        
                        # Create relationships with existing enhanced document
                        edge_props = EdgeProperties(EdgeType.RESULTS_IN)
                        self.add_edge_safe(item_id, enhanced_doc_id, edge_props)
                        
                        edge_props = EdgeProperties(EdgeType.PASSED_AT)
                        self.add_edge_safe(enhanced_doc_id, meeting_id, edge_props)
                        
                        # Extract and link entities
                        self._extract_item_entities_safe(item_id, item_data)
                        return
                
                # CRITICAL FIX: Check if enhanced legal document already exists
                doc_id = f"doc-{doc_type.lower()}-{doc_ref}"
                
                # First, try to find existing enhanced legal document node
                if doc_id in self.graph.nodes:
                    log.info(f"Found existing enhanced legal document node: {doc_id}")
                    existing_node = self.graph.nodes[doc_id]
                    
                    # Check if it has enhanced metadata
                    if (existing_node.get('passed_first_reading') is not None or 
                        existing_node.get('passed_second_reading') is not None):
                        log.info(f"Using existing enhanced metadata for {doc_id}")
                        
                        # Create relationships with existing enhanced document
                        edge_props = EdgeProperties(EdgeType.RESULTS_IN)
                        self.add_edge_safe(item_id, doc_id, edge_props)
                        
                        edge_props = EdgeProperties(EdgeType.PASSED_AT)
                        self.add_edge_safe(doc_id, meeting_id, edge_props)
                        
                        # Extract and link entities
                        self._extract_item_entities_safe(item_id, item_data)
                        return
                
                # If no enhanced document exists, create from agenda item data with enhancement
                log.info(f"No enhanced legal document found, creating from agenda data: {doc_id}")
                
                # Extract URL from item data
                urls = item_data.get('urls', [])
                url = None
                if urls and isinstance(urls, list) and len(urls) > 0:
                    url = urls[0].get('url', '') if isinstance(urls[0], dict) else ''
                
                # Try to enhance agenda item data with legal document lookup
                enhanced_item_data = self._enhance_item_data_with_legal_metadata(item_data, doc_ref, doc_type, meeting_date)
                
                # Pass the enhanced item_data to the creation function
                doc_id = self._create_document_node_enhanced_safe(
                    doc_type=doc_type, 
                    doc_number=doc_ref, 
                    title=enhanced_item_data.get('title', ''),
                    meeting_date=meeting_date,
                    url=url,
                    item_data=enhanced_item_data  # Use enhanced data
                )
                
                if doc_id:
                    # Create relationships
                    edge_props = EdgeProperties(EdgeType.RESULTS_IN)
                    self.add_edge_safe(item_id, doc_id, edge_props)
                    
                    edge_props = EdgeProperties(EdgeType.PASSED_AT)
                    self.add_edge_safe(doc_id, meeting_id, edge_props)
            
            # Extract and link entities
            self._extract_item_entities_safe(item_id, item_data)
            
        except Exception as e:
            log.error(f"Failed to process item references: {e}")
            self.stats['errors'] += 1

    def _enhance_item_data_with_legal_metadata(self, item_data: Dict, doc_ref: str, doc_type: str, meeting_date: str) -> Dict:
        """Enhance agenda item data with legal document metadata if available."""
        enhanced_data = item_data.copy()
        
        try:
            # Look for enhanced legal document JSON files in organized structure
            json_dir = Path("city_clerk_documents/extracted_json")
            legal_dir = json_dir / "legal"
            
            # Try multiple filename patterns for enhanced legal documents
            potential_files = []
            
            # Check organized legal/ subdirectory first
            if legal_dir.exists():
                potential_files.extend([
                    legal_dir / f"{doc_ref}_{meeting_date.replace('.', '_')}_enhanced_{doc_type.lower()}.json",
                    legal_dir / f"{doc_ref} - {meeting_date.replace('.', '_')}_enhanced_{doc_type.lower()}.json",
                    legal_dir / f"{meeting_date.replace('.', '_')}_enhanced_legal_documents.json"
                ])
            
            # Fallback to flat structure for backward compatibility
            potential_files.extend([
                json_dir / f"{doc_ref}_{meeting_date.replace('.', '_')}_enhanced_{doc_type.lower()}.json",
                json_dir / f"{doc_ref} - {meeting_date.replace('.', '_')}_enhanced_{doc_type.lower()}.json",
                json_dir / f"{meeting_date.replace('.', '_')}_enhanced_legal_documents.json"
            ])
            
            for potential_file in potential_files:
                if potential_file.exists():
                    try:
                        with open(potential_file, 'r', encoding='utf-8') as f:
                            legal_data = json.load(f)
                        
                        # Handle different file structures
                        if isinstance(legal_data, dict):
                            if 'legal_metadata' in legal_data:
                                # Single document file
                                legal_metadata = legal_data['legal_metadata']
                                if legal_data.get('document_number') == doc_ref:
                                    enhanced_data.update(self._merge_legal_metadata(enhanced_data, legal_metadata))
                                    log.info(f"Enhanced agenda item data with legal metadata from {potential_file.name}")
                                    break
                            elif 'documents' in legal_data or 'all_documents' in legal_data:
                                # Collection file
                                documents = legal_data.get('all_documents', legal_data.get('documents', []))
                                for doc in documents:
                                    if doc.get('document_number') == doc_ref:
                                        legal_metadata = doc.get('legal_metadata', {})
                                        enhanced_data.update(self._merge_legal_metadata(enhanced_data, legal_metadata))
                                        log.info(f"Enhanced agenda item data with legal metadata from collection {potential_file.name}")
                                        break
                                        
                    except Exception as e:
                        log.debug(f"Could not read legal metadata from {potential_file}: {e}")
                        continue
                        
        except Exception as e:
            log.debug(f"Error enhancing item data with legal metadata: {e}")
        
        return enhanced_data
    
    def _merge_legal_metadata(self, item_data: Dict, legal_metadata: Dict) -> Dict:
        """Merge legal metadata into agenda item data."""
        merged = {}
        
        # Copy reading status if available
        if 'passed_first_reading' in legal_metadata:
            merged['passed_first_reading'] = legal_metadata['passed_first_reading']
        
        if 'passed_second_reading' in legal_metadata:
            merged['passed_second_reading'] = legal_metadata['passed_second_reading']
        
        if 'outcome_status' in legal_metadata:
            merged['outcome_status'] = legal_metadata['outcome_status']
        
        # Enhance vote details if better data available
        if 'vote_details' in legal_metadata and legal_metadata['vote_details']:
            existing_vote_info = f"{item_data.get('title', '')} {item_data.get('description', '')}"
            if not re.search(r'(?:ayes?|yeas?):', existing_vote_info, re.IGNORECASE):
                # If agenda item doesn't have detailed vote info, add description with vote details
                vote_details = legal_metadata['vote_details']
                vote_summary = []
                
                if vote_details.get('unanimous'):
                    vote_summary.append("Unanimous")
                if vote_details.get('yeas'):
                    vote_summary.append(f"Ayes: {vote_details['yeas']}")
                if vote_details.get('nays'):
                    vote_summary.append(f"Nays: {vote_details['nays']}")
                
                if vote_summary:
                    vote_text = " | ".join(vote_summary)
                    current_desc = item_data.get('description', '')
                    merged['description'] = f"{current_desc} [{vote_text}]" if current_desc else f"[{vote_text}]"
        
        # Add LLM analysis confidence for debugging
        if 'llm_analysis' in legal_metadata:
            merged['llm_confidence'] = legal_metadata['llm_analysis'].get('confidence', 0)
            merged['llm_reasoning'] = legal_metadata['llm_analysis'].get('reasoning', '')
        
        return merged
    
    def _extract_item_entities_safe(self, item_id: str, item_data: Dict) -> None:
        """Safely extract entities from agenda item text."""
        try:
            text = f"{item_data.get('title', '')} {item_data.get('description', '')}"
            
            # Extract departments
            dept_patterns = [
                'Development Services Department',
                'Fire Department',
                'Police Department',
                'Public Works Department',
                'Planning and Zoning Board',
                'Historic Preservation Board'
            ]
            
            for dept in dept_patterns:
                if dept.lower() in text.lower():
                    dept_id = self._create_department_node_safe(dept)
                    if dept_id:
                        edge_props = EdgeProperties(EdgeType.MENTIONS)
                        self.add_edge_safe(item_id, dept_id, edge_props)
            
            # Extract locations
            location_patterns = [
                r'Pittman Park',
                r'Miracle Mile',
                r'City Hall',
                r'\d+\s+[A-Z]\w+\s+(Avenue|Street|Road|Boulevard|Way)'
            ]
            
            for pattern in location_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    location_name = match if isinstance(match, str) else match[0]
                    location_id = self._create_location_node_safe(location_name)
                    if location_id:
                        edge_props = EdgeProperties(EdgeType.MENTIONS)
                        self.add_edge_safe(item_id, location_id, edge_props)
                        
        except Exception as e:
            log.error(f"Failed to extract item entities: {e}")
            self.stats['errors'] += 1
    
    def _process_extracted_entity_safe(self, entity: Dict, meeting_id: str) -> None:
        """Safely process extracted entities with error handling."""
        try:
            entity_type = entity.get('type', '').upper()
            name = entity.get('name', '')
            
            if not name:
                return
            
            if entity_type == 'PERSON':
                self._create_person_node_safe(name, entity.get('role', ''))
            elif entity_type == 'ORGANIZATION':
                self._create_organization_node_safe(name, entity.get('org_type', 'Organization'))
            elif entity_type == 'LOCATION':
                self._create_location_node_safe(name, entity.get('address', ''))
            elif entity_type == 'DEPARTMENT':
                self._create_department_node_safe(name)
                
        except Exception as e:
            log.error(f"Failed to process extracted entity: {e}")
            self.stats['errors'] += 1
    
    async def _process_verbatim_transcript_collections_safe(self, json_dir: Path) -> None:
        """Process verbatim transcript collections with proper data flow."""
        try:
            log.info("🎤 Processing verbatim transcript collections...")
            
            # Look for verbatim files in the organized subdirectory structure
            verbatim_dir = json_dir / "verbatim"
            if verbatim_dir.exists():
                transcript_files = list(verbatim_dir.glob("*_verbatim_transcript*.json"))
            else:
                # Fallback to flat structure for backward compatibility
                transcript_files = list(json_dir.glob("*_verbatim_transcript*.json"))
            
            for transcript_file in transcript_files:
                try:
                    with open(transcript_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Handle both single transcripts and collections
                    if 'transcripts' in data:
                        meeting_date = data.get('meeting_date')
                        for transcript in data.get('transcripts', []):
                            self._process_single_transcript_with_data(transcript, meeting_date)
                    else:
                        # Single transcript
                        meeting_date = data.get('meeting_date')
                        self._process_single_transcript_with_data(data, meeting_date)
                        
                except Exception as e:
                    log.error(f"Failed to process transcript file {transcript_file}: {e}")
                    self.stats['errors'] += 1
                    
        except Exception as e:
            log.error(f"Failed to process transcript collections: {e}")
            self.stats['errors'] += 1

    def _process_single_transcript_with_data(self, transcript_data: Dict, meeting_date: str) -> None:
        """Process a single transcript document ensuring all data flows through."""
        try:
            # Agent 2 Fix: Check for None meeting_date
            if meeting_date is None:
                log.error("Skipping transcript: missing meeting_date")
                return
            
            source_file = transcript_data.get('source_file', '')
            
            # Extract all relevant data
            item_codes = transcript_data.get('item_codes', [])
            section_codes = transcript_data.get('section_codes', [])
            transcript_type = transcript_data.get('transcript_type', 'item')
            item_info = transcript_data.get('item_info_raw', '')
            
            # Calculate page count using improved method
            page_count = self._calculate_page_count(transcript_data, source_file)
            
            # Generate transcript ID
            if item_codes:
                transcript_id_suffix = '-'.join(item_codes)
            elif section_codes:
                transcript_id_suffix = '-'.join(section_codes)
            else:
                transcript_id_suffix = 'unknown'
            
            # Safe meeting_date replacement 
            safe_meeting_date = meeting_date.replace('.', '-') if meeting_date else 'unknown'
            transcript_id = f"transcript-{safe_meeting_date}-{transcript_id_suffix}"
            
            properties = TranscriptProperties(
                node_id=transcript_id,
                name=f"Transcript {source_file}",
                filename=source_file,
                transcript_type=transcript_type,
                meeting_date=meeting_date,
                page_count=page_count,
                item_info=item_info,
                items_covered=item_codes,  # Proper list
                sections_covered=section_codes,  # Proper list
                document_classification="verbatim"  # NEW: Set classification
            )
            
            # NEW: Add parent agenda item ID if available
            if item_codes and len(item_codes) > 0:
                # Use the first item code as the primary agenda item
                setattr(properties, 'parent_agenda_item_id', f"meeting-{safe_meeting_date}-item-{item_codes[0]}")
            else:
                setattr(properties, 'parent_agenda_item_id', None)
            
            node_id = self.add_node_safe(properties)
            
            # Ensure transcripts have document_type = 'transcript'
            if node_id and node_id in self.graph.nodes:
                self.graph.nodes[node_id]['document_type'] = 'transcript'
            
            if node_id:
                # Link to specific agenda items
                meeting_id = f"meeting-{meeting_date.replace('.', '-')}"
                for item_code in item_codes:
                    item_id = f"{meeting_id}-item-{item_code}"
                    edge_props = EdgeProperties(EdgeType.DISCUSSED_IN)
                    self.add_edge_safe(item_id, transcript_id, edge_props)
                
                # Link to meeting
                edge_props = EdgeProperties(EdgeType.DISCUSSED_IN)
                self.add_edge_safe(meeting_id, transcript_id, edge_props)
                    
        except Exception as e:
            log.error(f"Failed to process single transcript: {e}")
            self.stats['errors'] += 1
    
    def _process_single_transcript_safe(self, transcript_data: Dict, meeting_date: str) -> None:
        """Process a single transcript document with standardized properties."""
        try:
            # Agent 2 Fix: Check for None meeting_date
            if meeting_date is None:
                log.error("Skipping transcript: missing meeting_date")
                return
            
            source_file = transcript_data.get('source_file', '')
            
            # Generate proper transcript ID
            safe_meeting_date = meeting_date.replace('.', '-') if meeting_date else 'unknown'
            transcript_id = f"transcript-{safe_meeting_date}-{self._generate_transcript_id(transcript_data)}"
            
            # Extract items and sections properly as lists
            items_covered = transcript_data.get('item_codes', [])
            sections_covered = transcript_data.get('section_codes', [])
            
            # Calculate page count using improved method
            page_count = self._calculate_page_count(transcript_data, source_file)
            
            properties = TranscriptProperties(
                node_id=transcript_id,
                name=f"Transcript {source_file}",
                filename=source_file,
                transcript_type=transcript_data.get('transcript_type', 'item'),
                meeting_date=meeting_date,
                page_count=page_count,
                item_info=transcript_data.get('item_info_raw', ''),
                items_covered=items_covered,  # Keep as list, not string
                sections_covered=sections_covered  # Keep as list, not string
            )
            
            node_id = self.add_node_safe(properties)
            
            # Link to agenda items
            for item_code in items_covered:
                meeting_id = f"meeting-{safe_meeting_date}"
                item_id = f"{meeting_id}-item-{item_code}"
                edge_props = EdgeProperties(EdgeType.DISCUSSED_IN)
                self.add_edge_safe(item_id, transcript_id, edge_props)
                
        except Exception as e:
            log.error(f"Failed to process single transcript: {e}")
            self.stats['errors'] += 1
    
    def _generate_transcript_id(self, transcript_data: Dict) -> str:
        """Generate unique transcript ID from transcript data."""
        items = transcript_data.get('item_codes', [])
        sections = transcript_data.get('section_codes', [])
        
        if items:
            return '-'.join(items)
        elif sections:
            return '-'.join(sections)
        else:
            return 'unknown'
    
    async def _process_enhanced_legal_document_collections_safe(self, json_dir: Path) -> None:
        """Safely process enhanced legal document collections."""
        try:
            log.info("📜 Processing enhanced legal document collections...")
            
            # Look for legal files in the organized subdirectory structure
            legal_dir = json_dir / "legal"
            if legal_dir.exists():
                legal_files = list(legal_dir.glob("*enhanced_legal_documents*.json"))
                legal_files.extend(legal_dir.glob("*enhanced_ordinance*.json"))
                legal_files.extend(legal_dir.glob("*enhanced_resolution*.json"))
            else:
                # Fallback to flat structure for backward compatibility
                legal_files = list(json_dir.glob("*enhanced_legal_documents*.json"))
                legal_files.extend(json_dir.glob("*enhanced_ordinance*.json"))
                legal_files.extend(json_dir.glob("*enhanced_resolution*.json"))
            
            for legal_file in legal_files:
                try:
                    with open(legal_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if isinstance(data, dict) and ('documents' in data or 'all_documents' in data):
                        documents = data.get('all_documents', data.get('documents', []))
                    else:
                        documents = [data]
                    
                    for doc in documents:
                        self._process_legal_document_safe(doc)
                        
                except Exception as e:
                    log.error(f"Failed to process legal file {legal_file}: {e}")
                    self.stats['errors'] += 1
                    
        except Exception as e:
            log.error(f"Failed to process legal document collections: {e}")
            self.stats['errors'] += 1
    
    def _process_legal_document_safe(self, doc_data: Dict) -> None:
        """Process legal document with proper vote details extraction."""
        try:
            # Log what we're receiving
            log.debug(f"Processing legal document with data keys: {list(doc_data.keys())}")
            log.info(f"Processing legal document with keys: {list(doc_data.keys())}")
            log.info(f"source_file: {doc_data.get('source_file', 'NOT FOUND')}")
            log.info(f"meeting_date: {doc_data.get('meeting_date', 'NOT FOUND')}")
            
            # Extract document type - ensure it's properly capitalized
            doc_type_raw = doc_data.get('document_type', '')
            if doc_type_raw.lower() == 'resolution':
                doc_type = 'Resolution'
            elif doc_type_raw.lower() == 'ordinance':
                doc_type = 'Ordinance'
            else:
                # Use the better _determine_document_type method for proper classification
                doc_type = self._determine_document_type(doc_data)
                # Ensure proper capitalization
                if doc_type.lower() == 'resolution':
                    doc_type = 'Resolution'
                elif doc_type.lower() == 'ordinance':
                    doc_type = 'Ordinance'
                else:
                    doc_type = 'Document'
            
            doc_number = doc_data.get('document_number', '')
            
            # If no document_number field, try to extract from source_file
            if not doc_number:
                source_file = doc_data.get('source_file', '')
                if source_file and '-' in source_file:
                    # Extract from filename like "2024-03 - 01_09_2024.pdf"
                    doc_number = source_file.split(' - ')[0] if ' - ' in source_file else source_file.split('.')[0]
            
            if not doc_number:
                log.warning(f"No document number found for {doc_data.get('source_file', 'unknown')}")
                return
            
            # BUILD DYNAMIC MAPPING: Extract agenda item code and meeting date to build reference mapping
            agenda_item_code = doc_data.get('agenda_item_code')
            meeting_date = doc_data.get('meeting_date', '')
            
            if agenda_item_code and meeting_date and doc_type.lower() in ['ordinance', 'resolution']:
                # Initialize meeting date mapping if not exists
                if meeting_date not in self.ordinance_mapping:
                    self.ordinance_mapping[meeting_date] = {}
                
                # Initialize agenda item mapping if not exists
                if agenda_item_code not in self.ordinance_mapping[meeting_date]:
                    self.ordinance_mapping[meeting_date][agenda_item_code] = {}
                
                # Store the final ordinance number with the agenda item code
                self.ordinance_mapping[meeting_date][agenda_item_code]['final_ordinance_number'] = doc_number
                
                log.debug(f"📋 Built mapping: {meeting_date} -> {agenda_item_code} -> {doc_number}")
            
            # Track this ordinance as processed to avoid duplicates
            if doc_number and doc_type.lower() in ['ordinance', 'resolution']:
                self.processed_ordinances.add(doc_number)
                log.debug(f"📝 Tracked {doc_type} {doc_number} as processed")
            
            doc_id = f"doc-{doc_type.lower()}-{doc_number}"
            
            # Extract source file and meeting date DIRECTLY
            source_file = doc_data.get('source_file', '')
            meeting_date = doc_data.get('meeting_date', '')
            
            # Extract vote details and motion
            vote_details = {}
            motion_details = {}
            
            # Get from legal_metadata if available
            legal_metadata = doc_data.get('legal_metadata', {})
            passed_first_reading = False
            passed_second_reading = False
            if isinstance(legal_metadata, dict):
                vote_details = legal_metadata.get('vote_details', {})
                motion_details = legal_metadata.get('motion', {})
                passed_first_reading = legal_metadata.get('passed_first_reading', False)
                passed_second_reading = legal_metadata.get('passed_second_reading', False)
                
                # LOGIC FIX: If passed_second_reading is True, passed_first_reading must also be True
                if passed_second_reading and not passed_first_reading:
                    passed_first_reading = True
                    log.debug(f"Set passed_first_reading=True for {doc_number} (logic: second reading passed implies first reading passed)")
            
            # If no metadata, try to extract from text
            if not vote_details and 'full_text' in doc_data:
                vote_details = self._extract_vote_details_from_text(doc_data['full_text'])
            
            if not motion_details and 'full_text' in doc_data:
                motion_details = self._extract_motion_details_from_text(doc_data['full_text'])
            
            # Extract URL - try multiple sources
            url = None
            
            # First, check if URL is directly in the document data
            if 'url' in doc_data:
                url = doc_data['url']
                log.debug(f"Found direct URL in doc_data: {url}")
            
            # If not found, try to get from agenda item
            if not url:
                agenda_item_code = doc_data.get('agenda_item_code')
                if agenda_item_code and meeting_date:
                    meeting_id = f"meeting-{meeting_date.replace('.', '-')}"
                    item_id = f"{meeting_id}-item-{agenda_item_code}"
                    
                    # Look for the agenda item in our graph
                    for node_id, attrs in self.graph.nodes(data=True):
                        if node_id == item_id:
                            urls_data = attrs.get('urls', [])
                            if urls_data:
                                if isinstance(urls_data, str):
                                    try:
                                        import json
                                        urls_list = json.loads(urls_data)
                                        if urls_list and isinstance(urls_list, list) and len(urls_list) > 0:
                                            url = urls_list[0].get('url', '') if isinstance(urls_list[0], dict) else ''
                                            log.debug(f"Found URL from agenda item {item_id}: {url}")
                                    except:
                                        log.debug(f"Failed to parse URLs JSON for {item_id}")
                                elif isinstance(urls_data, list) and len(urls_data) > 0:
                                    url = urls_data[0].get('url', '') if isinstance(urls_data[0], dict) else ''
                                    log.debug(f"Found URL from agenda item list {item_id}: {url}")
                            break
            
            # Determine document classification
            document_classification = doc_type.lower() if doc_type.lower() in ['resolution', 'ordinance'] else 'document'
            
            # Calculate page count using improved method
            page_count = self._calculate_page_count(doc_data, source_file)
            
            # Create properties with ALL fields properly set
            properties = DocumentProperties(
                node_id=doc_id,
                name=f"{doc_type} {doc_number}",
                document_type=doc_type,
                document_number=doc_number,
                title=doc_data.get('title', ''),
                file_name=source_file,
                meeting_date=meeting_date,
                page_count=page_count,
                vote_details=vote_details,
                motion=motion_details,
                url=url,
                document_classification=document_classification,
                passed_first_reading=passed_first_reading,
                passed_second_reading=passed_second_reading
            )
            
            # NEW: Add parent agenda item ID if available
            agenda_item_code = doc_data.get('agenda_item_code')
            if agenda_item_code:
                setattr(properties, 'parent_agenda_item_id', f"meeting-{meeting_date.replace('.', '-')}-item-{agenda_item_code}")
            else:
                setattr(properties, 'parent_agenda_item_id', None)
            
            # Log what we're about to save
            log.debug(f"Creating document node with properties: doc_type={doc_type}, file_name={source_file}, meeting_date={meeting_date}, url={url}")
            log.info(f"BEFORE NODE CREATION - DocumentProperties fields:")
            log.info(f"  file_name: '{properties.file_name}'")
            log.info(f"  meeting_date: '{properties.meeting_date}'")
            log.info(f"  document_classification: '{properties.document_classification}'")
            log.info(f"  motion: {properties.motion}")
            log.info(f"  vote_details: {properties.vote_details}")
            log.info(f"  url: '{properties.url}'")
            
            node_id = self.add_node_safe(properties)
            
            if node_id:
                # Verify the node was created with all properties
                if node_id in self.graph.nodes:
                    node_attrs = self.graph.nodes[node_id]
                    log.debug(f"Created node {node_id} with attributes: {list(node_attrs.keys())}")
                    log.info(f"AFTER NODE CREATION - Graph node attributes:")
                    log.info(f"  file_name: '{node_attrs.get('file_name', 'NOT FOUND')}'")
                    log.info(f"  meeting_date: '{node_attrs.get('meeting_date', 'NOT FOUND')}'")
                    log.info(f"  document_classification: '{node_attrs.get('document_classification', 'NOT FOUND')}'")
                    log.info(f"  motion: {node_attrs.get('motion', 'NOT FOUND')}")
                    log.info(f"  vote_details: {node_attrs.get('vote_details', 'NOT FOUND')}")
                    log.info(f"  url: '{node_attrs.get('url', 'NOT FOUND')}'")
                    log.info(f"  All attributes: {dict(node_attrs)}")
                
                # Extract relationships from the properly extracted data
                if vote_details:
                    self._extract_document_voting_relationships(doc_id, vote_details)
                
                if motion_details:
                    self._extract_document_motion_relationships(doc_id, motion_details)
                
                # Link to meeting and agenda item
                if meeting_date:
                    meeting_id = f"meeting-{meeting_date.replace('.', '-')}"
                    edge_props = EdgeProperties(EdgeType.PASSED_AT)
                    self.add_edge_safe(doc_id, meeting_id, edge_props)
                    
                    agenda_item_code = doc_data.get('agenda_item_code')
                    if agenda_item_code:
                        item_id = f"{meeting_id}-item-{agenda_item_code}"
                        edge_props = EdgeProperties(EdgeType.RESULTS_IN)
                        self.add_edge_safe(item_id, doc_id, edge_props)
                        
        except Exception as e:
            log.error(f"Failed to process legal document: {e}")
            import traceback
            traceback.print_exc()
            self.stats['errors'] += 1

    def _extract_vote_details_from_text(self, text: str) -> Dict:
        """Extract vote details from document text if not in metadata."""
        vote_details = {}
        
        try:
            # Look for voting patterns
            patterns = {
                'ayes': [r'Ayes?:\s*([^)]+)', r'Yes:\s*([^)]+)', r'Yeas?:\s*([^)]+)'],
                'nays': [r'Nays?:\s*([^)]+)', r'No:\s*([^)]+)'],
                'abstain': [r'Abstain[ed]*:\s*([^)]+)', r'Abstention:\s*([^)]+)']
            }
            
            for vote_type, vote_patterns in patterns.items():
                for pattern in vote_patterns:
                    matches = re.findall(pattern, text, re.IGNORECASE)
                    if matches:
                        # Clean up the match
                        voters = matches[0].strip()
                        if voters and voters.lower() not in ['none', 'null', '']:
                            vote_details[vote_type] = voters
                        break
            
            # Look for unanimous votes
            if re.search(r'unanimous', text, re.IGNORECASE):
                vote_details['unanimous'] = True
                
        except Exception as e:
            log.error(f"Failed to extract vote details from text: {e}")
        
        return vote_details

    def _extract_motion_details_from_text(self, text: str) -> Dict:
        """Extract motion details from document text if not in metadata."""
        motion_details = {}
        
        try:
            # Look for motion patterns
            moved_patterns = [
                r'Moved by:\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                r'Motion by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                r'Commissioner\s+([A-Z][a-z]+)\s+moved'
            ]
            
            for pattern in moved_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    motion_details['moved_by'] = matches[0].strip()
                    break
            
            # Look for seconded patterns
            second_patterns = [
                r'Seconded by:\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                r'Second by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                r'Commissioner\s+([A-Z][a-z]+)\s+seconded'
            ]
            
            for pattern in second_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    motion_details['seconded_by'] = matches[0].strip()
                    break
                    
        except Exception as e:
            log.error(f"Failed to extract motion details from text: {e}")
        
        return motion_details

    def _extract_document_voting_relationships(self, doc_id: str, vote_details: Dict) -> None:
        """Extract voting relationships from legal document metadata."""
        try:
            # Extract yes votes
            yeas = vote_details.get('yeas', vote_details.get('yes', ''))
            if yeas:
                voters = [name.strip() for name in str(yeas).split(',') if name.strip()]
                for voter in voters:
                    person_id = self._create_person_node_safe(voter, 'Commissioner')
                    if person_id:
                        edge_props = EdgeProperties(EdgeType.VOTED_YES)
                        self.add_edge_safe(person_id, doc_id, edge_props)
            
            # Extract no votes
            nays = vote_details.get('nays', vote_details.get('no', ''))
            if nays:
                voters = [name.strip() for name in str(nays).split(',') if name.strip()]
                for voter in voters:
                    person_id = self._create_person_node_safe(voter, 'Commissioner')
                    if person_id:
                        edge_props = EdgeProperties(EdgeType.VOTED_NO)
                        self.add_edge_safe(person_id, doc_id, edge_props)
                        
        except Exception as e:
            log.error(f"Failed to extract document voting relationships: {e}")

    def _extract_document_motion_relationships(self, doc_id: str, motion: Dict) -> None:
        """Extract motion relationships from legal document metadata."""
        try:
            # Extract who moved
            moved_by = motion.get('moved_by', '')
            if moved_by:
                person_id = self._create_person_node_safe(moved_by, 'Commissioner')
                if person_id:
                    edge_props = EdgeProperties(EdgeType.MOVED_BY)
                    self.add_edge_safe(doc_id, person_id, edge_props)
            
            # Extract who seconded
            seconded_by = motion.get('seconded_by', '')
            if seconded_by:
                person_id = self._create_person_node_safe(seconded_by, 'Commissioner') 
                if person_id:
                    edge_props = EdgeProperties(EdgeType.SECONDED_BY)
                    self.add_edge_safe(doc_id, person_id, edge_props)
                    
        except Exception as e:
            log.error(f"Failed to extract document motion relationships: {e}")
    
    async def _process_supporting_document_json_safe(self, json_file: Path) -> None:
        """Safely process other supporting documents."""
        try:
            log.info(f"📄 Processing supporting document: {json_file.name}")
            
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            filename_lower = json_file.name.lower()
            
            # Route to existing specialized processors - any year from 2014-2025
            import re
            year_pattern = re.match(r'^(201[4-9]|202[0-5])-', filename_lower)
            if ('enhanced_ordinance' in filename_lower or 'enhanced_resolution' in filename_lower or 
                (year_pattern and 'stage3_ontology' in filename_lower)):
                # Use existing legal document processor for ordinances/resolutions
                self._process_legal_document_safe(data)
            elif 'verbatim_transcript' in filename_lower:
                # Use existing transcript processor
                meeting_date = data.get('meeting_date')
                if meeting_date:
                    if 'transcripts' in data:
                        # Collection of transcripts
                        for transcript in data.get('transcripts', []):
                            self._process_single_transcript_with_data(transcript, meeting_date)
                    else:
                        # Single transcript
                        self._process_single_transcript_with_data(data, meeting_date)
                        
        except Exception as e:
            log.error(f"Failed to process supporting document {json_file}: {e}")
            self.stats['errors'] += 1
    
    def _compute_graph_metrics(self) -> None:
        """Compute graph metrics for enhanced analysis."""
        try:
            log.info("📊 Computing graph metrics...")
            
            # Compute centrality measures
            degree_centrality = nx.degree_centrality(self.graph)
            
            if len(self.graph) > 1:
                betweenness_centrality = nx.betweenness_centrality(self.graph)
            else:
                betweenness_centrality = {}
            
            # Add metrics as node attributes
            for node_id in self.graph.nodes():
                self.graph.nodes[node_id]['degree_centrality'] = degree_centrality.get(node_id, 0.0)
                self.graph.nodes[node_id]['betweenness_centrality'] = betweenness_centrality.get(node_id, 0.0)
                
        except Exception as e:
            log.error(f"Failed to compute graph metrics: {e}")
            self.stats['errors'] += 1
    
    def _determine_document_type(self, item_data: Dict) -> str:
        """Determine document type from item data and document reference."""
        # First check item_type
        item_type = item_data.get('item_type', '').lower()
        if 'ordinance' in item_type:
            return 'Ordinance'
        elif 'resolution' in item_type:
            return 'Resolution'
        
        # Get document reference - check multiple possible field names
        doc_ref = item_data.get('document_reference', item_data.get('document_number', ''))
        title = item_data.get('title', '').lower()
        description = item_data.get('description', '').lower()
        
        # Also check full_text for legal documents
        full_text = item_data.get('full_text', '').lower()
        
        # Check title patterns first (most reliable)
        if 'a resolution of the city commission' in title:
            return 'Resolution'
        elif 'an ordinance of the city commission' in title:
            return 'Ordinance'
        elif 'resolution' in title and 'city commission' in title:
            return 'Resolution'
        elif 'ordinance' in title and 'city commission' in title:
            return 'Ordinance'
        
        # Check description patterns
        if 'a resolution of the city commission' in description:
            return 'Resolution'
        elif 'an ordinance of the city commission' in description:
            return 'Ordinance'
        elif 'resolution' in description and 'city commission' in description:
            return 'Resolution'
        elif 'ordinance' in description and 'city commission' in description:
            return 'Ordinance'
        
        # Check full_text patterns for legal documents
        if 'a resolution of the city commission' in full_text:
            return 'Resolution'
        elif 'an ordinance of the city commission' in full_text:
            return 'Ordinance'
        elif 'resolution' in full_text and 'city commission' in full_text:
            return 'Resolution'
        elif 'ordinance' in full_text and 'city commission' in full_text:
            return 'Ordinance'
        
        # Check for resolution patterns with document reference - any year from 2014-2025
        import re
        if (re.match(r'^(201[4-9]|202[0-5])-', doc_ref) and 
            ('resolution' in title or 'resolution' in description or
             'a resolution of the city commission' in title.lower() or
             'a resolution of the city commission' in description.lower())):
            return 'Resolution'
        
        # Check for ordinance patterns - any year from 2014-2025
        if (re.match(r'^(201[4-9]|202[0-5])-', doc_ref) and 
            ('ordinance' in title or 'ordinance' in description or
             'an ordinance of the city commission' in title.lower() or
             'an ordinance of the city commission' in description.lower())):
            return 'Ordinance'
        
        # Check for transcript patterns
        if 'transcript' in title or 'verbatim' in title or 'transcript' in description:
            return 'Transcript'
            
        # Default fallback
        return 'Document'
    
    def _convert_date_format(self, meeting_date: str) -> str:
        """Convert date format from DD.MM.YYYY to YYYY-MM-DD."""
        try:
            parts = meeting_date.split('.')
            if len(parts) == 3:
                day, month, year = parts
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            else:
                return meeting_date
        except:
            return meeting_date
    
    def _normalize_name(self, name: str) -> str:
        """Normalize entity names for consistent identification."""
        if not isinstance(name, str):
            return str(name)
        return ' '.join(name.strip().split()).title()
    
    def _save_graph(self) -> None:
        """Save graph to multiple formats with enhanced metadata."""
        try:
            # Clean attributes for GraphML compatibility
            self._clean_graph_for_graphml()
            
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
            
            # Save comprehensive statistics
            stats_path = self.output_dir / "graph_stats.json"
            comprehensive_stats = {
                'build_stats': self.stats,
                'graph_stats': self.get_graph_stats(),
                'timestamp': datetime.now().isoformat()
            }
            with open(stats_path, 'w') as f:
                json.dump(comprehensive_stats, f, indent=2)
                
        except Exception as e:
            log.error(f"Failed to save graph: {e}")
            self.stats['errors'] += 1
    
    def _clean_graph_for_graphml(self) -> None:
        """Clean up properties for GraphML compatibility."""
        try:
            # Clean node attributes
            for node_id, attrs in self.graph.nodes(data=True):
                for key, value in list(attrs.items()):
                    if isinstance(value, (list, dict)):
                        attrs[key] = json.dumps(value) if value else ""
                    elif value is None:
                        attrs[key] = ""  # Convert None to empty string
                    elif isinstance(value, bool):
                        attrs[key] = str(value).lower()  # Convert bool to string
                    elif isinstance(value, (int, float)):
                        attrs[key] = str(value)
                    # Keep empty strings as empty strings, don't convert them to anything else
            
            # Clean edge attributes  
            for u, v, attrs in self.graph.edges(data=True):
                for key, value in list(attrs.items()):
                    if isinstance(value, (list, dict)):
                        attrs[key] = json.dumps(value) if value else ""
                    elif value is None:
                        attrs[key] = ""
                    elif isinstance(value, bool):
                        attrs[key] = str(value).lower()
                    elif isinstance(value, (int, float)):
                        attrs[key] = str(value)
                        
        except Exception as e:
            log.error(f"Failed to clean graph for GraphML: {e}")
            self.stats['errors'] += 1
    
    def get_graph_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics about the graph."""
        try:
            stats = {
                'total_nodes': self.graph.number_of_nodes(),
                'total_edges': self.graph.number_of_edges(),
                'node_types': {},
                'edge_types': {},
                'density': nx.density(self.graph),
                'is_connected': nx.is_weakly_connected(self.graph) if self.graph.number_of_nodes() > 0 else False
            }
            
            # Count nodes by type
            for node, attrs in self.graph.nodes(data=True):
                label = attrs.get('label', 'unknown')
                stats['node_types'][label] = stats['node_types'].get(label, 0) + 1
            
            # Count edges by type
            for u, v, attrs in self.graph.edges(data=True):
                label = attrs.get('label', 'unknown')
                stats['edge_types'][label] = stats['edge_types'].get(label, 0) + 1
            
            return stats
            
        except Exception as e:
            log.error(f"Failed to get graph stats: {e}")
            return {'error': str(e)}
    
    def load_graph(self, format: str = 'graphml') -> bool:
        """Load a previously saved graph with error handling."""
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
    
    # Add these new temporal query methods:
    
    def query_by_date_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Query nodes within a date range."""
        # Parse and normalize dates
        start_normalized = TemporalParser.normalize_date(start_date)
        end_normalized = TemporalParser.normalize_date(end_date)
        
        if not start_normalized or not end_normalized:
            log.warning(f"Invalid date range: {start_date} to {end_date}")
            return []
        
        results = []
        
        # Use temporal index for fast lookup
        node_ids = self.temporal_index.get_nodes_in_range(start_normalized, end_normalized)
        
        # Fallback to graph traversal if index is empty
        if not node_ids:
            for node_id, attrs in self.graph.nodes(data=True):
                node_date = attrs.get('meeting_date', '')
                if node_date:
                    normalized = TemporalParser.normalize_date(node_date)
                    if normalized and start_normalized <= normalized <= end_normalized:
                        node_ids.add(node_id)
        
        # Get full node data
        for node_id in node_ids:
            if node_id in self.graph.nodes:
                node_data = dict(self.graph.nodes[node_id])
                node_data['node_id'] = node_id
                node_data['connected_nodes'] = list(self.graph.neighbors(node_id))
                results.append(node_data)
        
        return sorted(results, key=lambda x: x.get('meeting_date', ''))
    
    def query_by_relative_date(self, relative_expr: str) -> List[Dict[str, Any]]:
        """Query nodes by relative date expression (e.g., 'last month', 'Q1 2024')."""
        # Try to parse as date range
        date_range = TemporalParser.extract_date_range(relative_expr)
        if date_range:
            return self.query_by_date_range(date_range[0], date_range[1])
        
        # Try to parse as single relative date
        single_date = TemporalParser.parse_relative_date(relative_expr)
        if single_date:
            date_str = single_date.strftime('%Y-%m-%d')
            return self.query_by_date_range(date_str, date_str)
        
        log.warning(f"Could not parse relative date expression: {relative_expr}")
        return []
    
    def get_temporal_progression(self, entity_type: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Get temporal progression of a specific entity type."""
        nodes_in_range = self.query_by_date_range(start_date, end_date)
        
        # Filter by entity type
        filtered = [n for n in nodes_in_range if n.get('label', '').lower() == entity_type.lower()]
        
        # Group by time periods
        progression = []
        for node in filtered:
            node_date = node.get('meeting_date', '')
            if node_date:
                progression.append({
                    'date': node_date,
                    'node_id': node['node_id'],
                    'title': node.get('title', node.get('name', '')),
                    'type': node.get('label', ''),
                    'connections': len(node.get('connected_nodes', []))
                })
        
        return sorted(progression, key=lambda x: x['date'])
    
    def find_related_by_time_window(self, node_id: str, days_before: int = 30, days_after: int = 30) -> List[Dict[str, Any]]:
        """Find nodes related to a given node within a time window."""
        if node_id not in self.graph.nodes:
            return []
        
        node_attrs = self.graph.nodes[node_id]
        node_date = node_attrs.get('meeting_date', '')
        
        if not node_date:
            return []
        
        center_date = TemporalParser.parse_date(node_date)
        if not center_date:
            return []
        
        start_date = (center_date - timedelta(days=days_before)).strftime('%Y-%m-%d')
        end_date = (center_date + timedelta(days=days_after)).strftime('%Y-%m-%d')
        
        # Get nodes in time window
        nodes_in_window = self.query_by_date_range(start_date, end_date)
        
        # Get directly connected nodes
        connected = set(self.graph.neighbors(node_id))
        
        # Enrich results with connection info
        results = []
        for node in nodes_in_window:
            if node['node_id'] != node_id:
                node_copy = node.copy()
                node_copy['is_connected'] = node['node_id'] in connected
                node_copy['days_apart'] = (TemporalParser.parse_date(node.get('meeting_date', '')) - center_date).days if node.get('meeting_date') else None
                results.append(node_copy)
        
        return sorted(results, key=lambda x: abs(x.get('days_apart', 999)) if x.get('days_apart') is not None else 999)

    def _report_data_quality_improvements(self) -> None:
        """Report on data quality improvements from enhanced extraction."""
        log.info("\n" + "="*60)
        log.info("📊 DATA QUALITY IMPROVEMENT REPORT")
        log.info("="*60)
        
        # Count nodes with enhanced status information
        enhanced_documents = 0
        enhanced_agenda_items = 0
        accurate_reading_status = 0
        accurate_outcome_status = 0
        
        for node_id, attrs in self.graph.nodes(data=True):
            if attrs.get('label') == 'document':
                # Check for enhanced reading status
                first_reading = attrs.get('passed_first_reading')
                second_reading = attrs.get('passed_second_reading')
                
                if first_reading is not None or second_reading is not None:
                    enhanced_documents += 1
                    
                    # Count accurate reading status (not hardcoded False)
                    if first_reading == True or second_reading == True:
                        accurate_reading_status += 1
                        
            elif attrs.get('label') == 'agenda_item':
                outcome_status = attrs.get('outcome_status', 'Pending')
                
                # Count non-default outcome status
                if outcome_status != 'Pending':
                    enhanced_agenda_items += 1
                    accurate_outcome_status += 1
        
        total_documents = sum(1 for _, attrs in self.graph.nodes(data=True) if attrs.get('label') == 'document')
        total_agenda_items = sum(1 for _, attrs in self.graph.nodes(data=True) if attrs.get('label') == 'agenda_item')
        
        log.info(f"📄 DOCUMENT ENHANCEMENTS:")
        log.info(f"  • Total documents: {total_documents}")
        log.info(f"  • Enhanced with reading status: {enhanced_documents} ({enhanced_documents/max(total_documents,1)*100:.1f}%)")
        log.info(f"  • With accurate reading data: {accurate_reading_status} ({accurate_reading_status/max(total_documents,1)*100:.1f}%)")
        
        log.info(f"\n🗳️  AGENDA ITEM ENHANCEMENTS:")
        log.info(f"  • Total agenda items: {total_agenda_items}")
        log.info(f"  • Enhanced with outcome status: {enhanced_agenda_items} ({enhanced_agenda_items/max(total_agenda_items,1)*100:.1f}%)")
        log.info(f"  • With accurate outcome data: {accurate_outcome_status} ({accurate_outcome_status/max(total_agenda_items,1)*100:.1f}%)")
        
        if self.use_llm_validation:
            log.info(f"\n🤖 LLM VALIDATION STATISTICS:")
            log.info(f"  • LLM validations performed: {self.stats.get('llm_validations', 0)}")
            log.info(f"  • LLM corrections made: {self.stats.get('llm_corrections', 0)}")
            correction_rate = self.stats.get('llm_corrections', 0) / max(self.stats.get('llm_validations', 1), 1) * 100
            log.info(f"  • Correction rate: {correction_rate:.1f}%")
        else:
            log.info(f"\n🤖 LLM VALIDATION: Disabled (using regex patterns only)")
        
        log.info(f"\n✅ IMPROVEMENT SUMMARY:")
        if enhanced_documents > 0 or enhanced_agenda_items > 0:
            log.info(f"  • Successfully replaced hardcoded values with real data")
            log.info(f"  • Reading status accuracy improved for {accurate_reading_status} documents")
            log.info(f"  • Outcome status accuracy improved for {accurate_outcome_status} agenda items")
        else:
            log.info(f"  • No enhancements detected - check document processing pipeline")
        
        log.info("="*60)


# Legacy compatibility - maintain LocalGraphBuilder as alias
LocalGraphBuilder = GraphBuilder 