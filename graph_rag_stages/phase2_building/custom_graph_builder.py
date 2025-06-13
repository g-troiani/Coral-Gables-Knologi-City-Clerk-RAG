"""
Custom graph builder for creating knowledge graphs in Cosmos DB.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import asyncio
from ..common.cosmos_client import CosmosGraphClient
from ..common.config import get_config

log = logging.getLogger(__name__)


class CustomGraphBuilder:
    """Builds custom knowledge graphs in Cosmos DB from processed documents."""
    
    def __init__(self, cosmos_config: Optional[Dict] = None):
        """
        Initialize the graph builder with Cosmos DB configuration.
        
        Args:
            cosmos_config: Optional Cosmos DB configuration override
        """
        self.config = get_config()
        
        # Override with custom config if provided
        if cosmos_config:
            for key, value in cosmos_config.items():
                if hasattr(self.config, key):
                    setattr(self.config, key, value)
        
        # Initialize Cosmos client
        self.cosmos_client = CosmosGraphClient(
            endpoint=self.config.cosmos_endpoint,
            key=self.config.cosmos_key,
            database=self.config.cosmos_database,
            container=self.config.cosmos_container
        )

    async def build_graph_from_markdown(self, markdown_dir: Path) -> None:
        """
        Build knowledge graph from enriched markdown files.
        
        Args:
            markdown_dir: Directory containing enriched markdown files
        """
        log.info(f"🔗 Building custom graph from markdown files in: {markdown_dir}")
        
        # Find all markdown files
        markdown_files = list(markdown_dir.glob("*.md"))
        log.info(f"Found {len(markdown_files)} markdown files to process")
        
        if not markdown_files:
            log.warning("No markdown files found for graph building")
            return
        
        # Connect to Cosmos DB
        async with self.cosmos_client:
            # Process files and build graph
            for md_file in markdown_files:
                try:
                    await self._process_document_for_graph(md_file)
                except Exception as e:
                    log.error(f"Error processing {md_file.name} for graph: {e}")
                    continue
        
        log.info("✅ Custom graph building completed")

    async def _process_document_for_graph(self, md_file: Path) -> None:
        """
        Process a single markdown document and add its entities/relationships to the graph.
        
        Args:
            md_file: Path to markdown file
        """
        log.info(f"📄 Processing {md_file.name} for graph building")
        
        # Read the markdown content
        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract document metadata from the header
        metadata = self._extract_document_metadata(content)
        
        # Create document vertex
        doc_id = self._generate_document_id(md_file, metadata)
        await self._create_document_vertex(doc_id, metadata, md_file)
        
        # Extract and create entities based on document type
        if metadata.get('document_type') == 'agenda':
            await self._process_agenda_document(doc_id, content, metadata)
        elif metadata.get('document_type') == 'verbatim_transcript':
            await self._process_transcript_document(doc_id, content, metadata)
        elif metadata.get('document_type') in ['ordinance', 'resolution']:
            await self._process_legislative_document(doc_id, content, metadata)
        else:
            await self._process_generic_document(doc_id, content, metadata)

    def _extract_document_metadata(self, content: str) -> Dict[str, Any]:
        """Extract metadata from the markdown header."""
        metadata = {}
        
        # Look for metadata section between --- markers
        if content.startswith("---"):
            try:
                _, header_section, _ = content.split("---", 2)
                
                # Parse key-value pairs from header
                for line in header_section.strip().split("\n"):
                    line = line.strip()
                    if ":" in line and line.startswith("- "):
                        # Handle format like "- Document Type: AGENDA"
                        key_value = line[2:].split(":", 1)
                        if len(key_value) == 2:
                            key = key_value[0].strip().lower().replace(" ", "_")
                            value = key_value[1].strip()
                            metadata[key] = value
            except ValueError:
                pass  # No proper header found
        
        return metadata

    async def _create_document_vertex(self, doc_id: str, metadata: Dict, md_file: Path) -> None:
        """Create a vertex for the document."""
        properties = {
            'title': metadata.get('title', md_file.stem),
            'document_type': metadata.get('document_type', 'document'),
            'source_file': md_file.name,
            'meeting_date': metadata.get('meeting_date', ''),
            'created_at': metadata.get('extraction_timestamp', ''),
        }
        
        await self.cosmos_client.create_vertex('Document', doc_id, properties)
        log.debug(f"Created document vertex: {doc_id}")

    async def _process_agenda_document(self, doc_id: str, content: str, metadata: Dict) -> None:
        """Process agenda document and create agenda-specific entities."""
        log.debug(f"Processing agenda document: {doc_id}")
        
        # Create meeting vertex
        meeting_date = metadata.get('meeting_date', 'unknown')
        if meeting_date != 'unknown':
            meeting_id = f"MEETING_{meeting_date.replace('.', '_')}"
            meeting_properties = {
                'date': meeting_date,
                'type': 'city_commission_meeting'
            }
            await self.cosmos_client.create_vertex('Meeting', meeting_id, meeting_properties)
            
            # Link document to meeting
            await self.cosmos_client.create_edge_if_not_exists(
                doc_id, meeting_id, 'DOCUMENTS'
            )
        
        # Extract agenda items from content
        agenda_items = self._extract_agenda_items_from_content(content)
        for item in agenda_items:
            await self._create_agenda_item_vertex(item, doc_id, meeting_date)

    async def _process_transcript_document(self, doc_id: str, content: str, metadata: Dict) -> None:
        """Process transcript document and create transcript-specific entities."""
        log.debug(f"Processing transcript document: {doc_id}")
        
        # Extract agenda items mentioned in transcript
        mentioned_items = self._extract_agenda_items_from_content(content)
        
        # Link transcript to agenda items
        for item_code in mentioned_items:
            item_id = f"ITEM_{item_code}_{metadata.get('meeting_date', 'unknown').replace('.', '_')}"
            await self.cosmos_client.create_edge_if_not_exists(
                doc_id, item_id, 'DISCUSSES'
            )

    async def _process_legislative_document(self, doc_id: str, content: str, metadata: Dict) -> None:
        """Process ordinance/resolution document."""
        log.debug(f"Processing legislative document: {doc_id}")
        
        # Link to agenda item if specified
        agenda_item = metadata.get('linked_agenda_item')
        if agenda_item:
            item_id = f"ITEM_{agenda_item}_{metadata.get('meeting_date', 'unknown').replace('.', '_')}"
            await self.cosmos_client.create_edge_if_not_exists(
                item_id, doc_id, 'IMPLEMENTS'
            )

    async def _process_generic_document(self, doc_id: str, content: str, metadata: Dict) -> None:
        """Process generic document."""
        log.debug(f"Processing generic document: {doc_id}")
        # For now, just ensure the document vertex exists
        pass

    def _extract_agenda_items_from_content(self, content: str) -> List[str]:
        """Extract agenda item codes from document content."""
        import re
        
        item_codes = []
        
        # Look for agenda item patterns in the content
        patterns = [
            r'AGENDA_ITEM:\s*([A-Z]-\d+)',
            r'Item\s+([A-Z]-\d+)',
            r'([A-Z]-\d+)\s*:',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            item_codes.extend(matches)
        
        # Remove duplicates and return
        return list(set(item_codes))

    async def _create_agenda_item_vertex(self, item_code: str, doc_id: str, meeting_date: str) -> None:
        """Create a vertex for an agenda item."""
        item_id = f"ITEM_{item_code}_{meeting_date.replace('.', '_')}"
        
        properties = {
            'item_code': item_code,
            'meeting_date': meeting_date,
            'status': 'scheduled'
        }
        
        await self.cosmos_client.create_vertex('AgendaItem', item_id, properties)
        
        # Link agenda item to document
        await self.cosmos_client.create_edge_if_not_exists(
            doc_id, item_id, 'CONTAINS'
        )

    def _generate_document_id(self, md_file: Path, metadata: Dict) -> str:
        """Generate a unique document ID."""
        import hashlib
        
        # Use file path and some metadata to create unique ID
        unique_string = f"{md_file.name}_{metadata.get('document_type', 'doc')}"
        hash_part = hashlib.sha1(unique_string.encode()).hexdigest()[:8]
        
        doc_type = metadata.get('document_type', 'doc').upper()
        return f"{doc_type}_{hash_part}"

    async def clear_graph(self) -> None:
        """Clear all data from the graph (use with caution)."""
        log.warning("🗑️ Clearing entire graph database")
        
        async with self.cosmos_client:
            await self.cosmos_client.clear_graph()
        
        log.info("✅ Graph cleared")

    async def get_graph_stats(self) -> Dict[str, int]:
        """Get basic statistics about the graph."""
        stats = {
            'total_vertices': 0,
            'documents': 0,
            'agenda_items': 0,
            'meetings': 0
        }
        
        try:
            async with self.cosmos_client:
                # Count total vertices
                result = await self.cosmos_client._execute_query("g.V().count()")
                stats['total_vertices'] = result[0] if result else 0
                
                # Count by label
                for label in ['Document', 'AgendaItem', 'Meeting']:
                    result = await self.cosmos_client._execute_query(f"g.V().hasLabel('{label}').count()")
                    count = result[0] if result else 0
                    stats[label.lower() + 's'] = count
                    
        except Exception as e:
            log.error(f"Error getting graph stats: {e}")
        
        return stats 