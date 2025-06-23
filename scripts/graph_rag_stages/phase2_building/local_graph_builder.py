"""
Local graph builder using NetworkX for creating knowledge graphs without cloud dependencies.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import json
import pickle
import networkx as nx
from datetime import datetime
from ..common.utils import extract_metadata_from_header, ensure_directory_exists

log = logging.getLogger(__name__)


class LocalGraphBuilder:
    """Builds knowledge graphs locally using NetworkX."""
    
    def __init__(self, output_dir: Optional[Path] = None):
        """
        Initialize the local graph builder.
        
        Args:
            output_dir: Directory to save graph files
        """
        self.output_dir = output_dir or Path("local_graph_data")
        ensure_directory_exists(self.output_dir)
        
        # Initialize NetworkX graph
        self.graph = nx.DiGraph()
        log.info(f"📊 Initialized local graph builder with output directory: {self.output_dir}")

    async def build_graph_from_markdown(self, markdown_dir: Path) -> None:
        """
        Build knowledge graph from enriched markdown files.
        
        Args:
            markdown_dir: Directory containing enriched markdown files
        """
        log.info(f"🔗 Building local graph from markdown files in: {markdown_dir}")
        
        # Find all markdown files
        markdown_files = list(markdown_dir.glob("*.md"))
        log.info(f"Found {len(markdown_files)} markdown files to process")
        
        if not markdown_files:
            log.warning("No markdown files found for graph building")
            return
        
        # Process files and build graph
        for md_file in markdown_files:
            try:
                await self._process_document_for_graph(md_file)
            except Exception as e:
                log.error(f"Error processing {md_file.name} for graph: {e}")
                continue
        
        # Save the graph
        self._save_graph()
        
        # Generate statistics
        stats = self.get_graph_stats()
        log.info(f"✅ Local graph building completed. Stats: {stats}")

    async def _process_document_for_graph(self, md_file: Path) -> None:
        """Process a single markdown document and add to graph."""
        log.info(f"📄 Processing {md_file.name} for local graph")
        
        # Read the markdown content
        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract document metadata
        metadata = self._extract_document_metadata(content)
        
        # Create document node
        doc_id = self._generate_document_id(md_file, metadata)
        self._add_document_node(doc_id, metadata, md_file)
        
        # Process based on document type
        if metadata.get('document_type') == 'agenda':
            await self._process_agenda_document(doc_id, content, metadata)
        elif metadata.get('document_type') == 'verbatim_transcript':
            await self._process_transcript_document(doc_id, content, metadata)
        elif metadata.get('document_type') in ['ordinance', 'resolution']:
            await self._process_legislative_document(doc_id, content, metadata)

    def _extract_document_metadata(self, content: str) -> Dict[str, Any]:
        """Extract metadata from markdown header."""
        metadata = extract_metadata_from_header(content)
        
        # If meeting_date is N/A or missing, try to extract from content
        if not metadata.get('meeting_date') or metadata.get('meeting_date') == 'N/A':
            meeting_date = self._extract_meeting_date_from_content(content)
            if meeting_date:
                metadata['meeting_date'] = meeting_date
        
        return metadata

    def _generate_document_id(self, md_file: Path, metadata: Dict) -> str:
        """Generate unique document ID."""
        import hashlib
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

    async def _process_agenda_document(self, doc_id: str, content: str, metadata: Dict) -> None:
        """Process agenda document and create nodes/edges."""
        log.debug(f"Processing agenda document: {doc_id}")
        
        # Create meeting node
        meeting_date = metadata.get('meeting_date', 'unknown')
        meeting_id = None
        if meeting_date != 'unknown':
            meeting_id = f"MEETING_{meeting_date.replace('.', '_')}"
            self.graph.add_node(meeting_id,
                node_type='Meeting',
                date=meeting_date,
                meeting_type='city_commission_meeting'
            )
            # Add edge from document to meeting
            self.graph.add_edge(doc_id, meeting_id, relationship='DOCUMENTS')
        
        # Extract agenda items from both content and metadata
        agenda_items = self._extract_agenda_items_from_content(content)
        
        # Also check metadata for agenda items
        if 'agenda_items' in metadata:
            metadata_items = metadata['agenda_items']
            if isinstance(metadata_items, list):
                agenda_items.extend(metadata_items)
            elif isinstance(metadata_items, str):
                agenda_items.extend(metadata_items.split(','))
        
        # Remove duplicates
        agenda_items = list(set(agenda_items))
        
        for item_code in agenda_items:
            item_id = f"ITEM_{item_code}_{meeting_date.replace('.', '_')}"
            self._create_agenda_item_vertex(item_id, item_code, meeting_date)
            
            # Add edge from document to item (CONTAINS relationship)
            self.graph.add_edge(doc_id, item_id, relationship='CONTAINS')
            
            # Add edge from item to meeting (SCHEDULED_FOR relationship)
            if meeting_id:
                self.graph.add_edge(item_id, meeting_id, relationship='SCHEDULED_FOR')

    async def _process_transcript_document(self, doc_id: str, content: str, metadata: Dict) -> None:
        """Process transcript document."""
        log.debug(f"Processing transcript document: {doc_id}")
        
        # Extract mentioned items
        mentioned_items = self._extract_agenda_items_from_content(content)
        meeting_date = metadata.get('meeting_date', 'unknown')
        
        # Link transcript to agenda items with bidirectional relationships
        for item_code in mentioned_items:
            item_id = f"ITEM_{item_code}_{meeting_date.replace('.', '_')}"
            
            # Create agenda item vertex if it doesn't exist
            self._create_agenda_item_vertex(item_id, item_code, meeting_date)
            
            # Add bidirectional edges
            # Document DISCUSSES agenda item
            self.graph.add_edge(doc_id, item_id, relationship='DISCUSSES')
            # Agenda item DISCUSSED_IN document  
            self.graph.add_edge(item_id, doc_id, relationship='DISCUSSED_IN')

    async def _process_legislative_document(self, doc_id: str, content: str, metadata: Dict) -> None:
        """Process ordinance/resolution document."""
        log.debug(f"Processing legislative document: {doc_id}")
        
        # Link to agenda item if specified in metadata
        agenda_item = metadata.get('linked_agenda_item')
        meeting_date = metadata.get('meeting_date', 'unknown')
        
        # Also extract agenda items from content
        content_items = self._extract_agenda_items_from_content(content)
        
        # Combine metadata and content items
        all_items = []
        if agenda_item:
            all_items.append(agenda_item)
        all_items.extend(content_items)
        all_items = list(set(all_items))  # Remove duplicates
        
        # Create relationships with agenda items
        for item_code in all_items:
            item_id = f"ITEM_{item_code}_{meeting_date.replace('.', '_')}"
            
            # Create agenda item vertex if it doesn't exist
            self._create_agenda_item_vertex(item_id, item_code, meeting_date)
            
            # Add edge from item to document (IMPLEMENTS relationship)
            self.graph.add_edge(item_id, doc_id, relationship='IMPLEMENTS')

    def _create_agenda_item_vertex(self, item_id: str, item_code: str, meeting_date: str) -> None:
        """Create agenda item vertex if it doesn't exist, avoiding overwriting existing nodes."""
        if not self.graph.has_node(item_id):
            self.graph.add_node(item_id,
                node_type='AgendaItem',
                item_code=item_code,
                meeting_date=meeting_date,
                status='scheduled'
            )
        else:
            # Update existing node attributes if needed (merge, don't overwrite)
            existing_attrs = self.graph.nodes[item_id]
            if 'status' not in existing_attrs:
                self.graph.nodes[item_id]['status'] = 'scheduled'

    def _extract_agenda_items_from_content(self, content: str) -> List[str]:
        """Extract agenda item codes from content."""
        import re
        item_codes = []
        patterns = [
            r'AGENDA_ITEM:\s*([A-Z]\.?-\d+\.?)',  # AGENDA_ITEM: A.-1. or A-1
            r'Item\s+([A-Z]\.?-\d+\.?)',  # Item A.-1.
            r'([A-Z]\.?-\d+\.?)\s*:',  # A.-1.:
            r'\b([A-Z]\.?-\d+\.?)\b',  # General pattern for agenda items
            r'agenda\s+item\s+([A-Z]\.?-\d+\.?)',  # Case insensitive agenda item
            r'item\s+number\s+([A-Z]\.?-\d+\.?)',  # Item number format
            r'([A-Z]\.?-\d+\.?)\s*[-–—]\s*',  # Item followed by dash
            r'###\s*Agenda\s+Item\s+([A-Z]\.?-\d+\.?)',  # ### Agenda Item A.-1.
        ]
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            # Normalize the format - remove trailing dots and ensure consistent format
            normalized_matches = [match.upper().rstrip('.') for match in matches]
            item_codes.extend(normalized_matches)
        return list(set(item_codes))

    def _save_graph(self) -> None:
        """Save graph to multiple formats."""
        # Save as GraphML (XML format, good for interoperability)
        graphml_path = self.output_dir / "city_clerk_graph.graphml"
        nx.write_graphml(self.graph, str(graphml_path))
        log.info(f"💾 Saved graph as GraphML: {graphml_path}")
        
        # Save as pickle (preserves all attributes)
        pickle_path = self.output_dir / "city_clerk_graph.pkl"
        with open(pickle_path, 'wb') as f:
            pickle.dump(self.graph, f, pickle.HIGHEST_PROTOCOL)
        log.info(f"💾 Saved graph as pickle: {pickle_path}")
        
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
            'meetings': 0,
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
            elif node_type == 'Meeting':
                stats['meetings'] += 1
        
        # Add graph metrics
        if self.graph.number_of_nodes() > 0:
            stats['average_degree'] = sum(dict(self.graph.degree()).values()) / self.graph.number_of_nodes()
            stats['density'] = nx.density(self.graph)
        
        return stats

    def load_graph(self, format: str = 'pickle') -> bool:
        """Load a previously saved graph."""
        try:
            if format == 'pickle':
                pickle_path = self.output_dir / "city_clerk_graph.pkl"
                with open(pickle_path, 'rb') as f:
                    self.graph = pickle.load(f)
            elif format == 'graphml':
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