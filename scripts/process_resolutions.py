#!/usr/bin/env python3
"""
Standalone Resolution Processor
Process all resolution documents for a given meeting or date range.
"""

import asyncio
import argparse
from pathlib import Path
import logging
from datetime import datetime
from typing import Dict
import re
from graph_stages.enhanced_document_linker import EnhancedDocumentLinker
from graph_stages.cosmos_db_client import CosmosGraphClient
from graph_stages.agenda_graph_builder import AgendaGraphBuilder

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger('resolution_processor')


class ResolutionProcessor:
    """Process resolution documents and update the graph."""
    
    def __init__(self):
        self.document_linker = EnhancedDocumentLinker()
        self.cosmos_client = None
        self.graph_builder = None
    
    async def initialize(self):
        """Initialize async components."""
        self.cosmos_client = CosmosGraphClient()
        await self.cosmos_client.connect()
        self.graph_builder = AgendaGraphBuilder(self.cosmos_client, upsert_mode=True)
        log.info("✅ Resolution processor initialized")
    
    async def process_resolutions_for_meeting(self, meeting_date: str, resolutions_dir: Path):
        """Process all resolutions for a specific meeting date."""
        log.info(f"📄 Processing resolutions for meeting: {meeting_date}")
        
        # Find and link resolution documents
        linked_docs = await self.document_linker.link_documents_for_meeting(
            meeting_date,
            Path("dummy"),  # We only care about resolutions
            resolutions_dir
        )
        
        resolutions = linked_docs.get("resolutions", [])
        log.info(f"Found {len(resolutions)} resolutions for {meeting_date}")
        
        # Update graph with resolution nodes and links
        for resolution in resolutions:
            await self._add_resolution_to_graph(resolution, meeting_date)
        
        return len(resolutions)
    
    async def _add_resolution_to_graph(self, resolution: Dict, meeting_date: str):
        """Add resolution node and link to agenda item."""
        doc_number = resolution.get('document_number')
        item_code = resolution.get('item_code')
        
        if not item_code:
            log.warning(f"Resolution {doc_number} has no agenda item code, skipping graph update")
            return
        
        # Create resolution node
        resolution_id = f"resolution-{doc_number}"
        
        properties = {
            'nodeType': 'Resolution',
            'document_number': doc_number,
            'title': resolution.get('title', 'Untitled Resolution'),
            'document_type': 'Resolution',
            'meeting_date': meeting_date
        }
        
        # Add parsed metadata
        parsed_data = resolution.get('parsed_data', {})
        if parsed_data.get('date_passed'):
            properties['date_passed'] = parsed_data['date_passed']
        
        # Create or update node
        created = await self.cosmos_client.upsert_vertex('Resolution', resolution_id, properties)
        log.info(f"{'Created' if created else 'Updated'} resolution node: {resolution_id}")
        
        # Link to agenda item
        normalized_code = self.graph_builder.normalize_item_code(item_code)
        item_id = f"item-{meeting_date.replace('.', '-')}-{normalized_code}"
        
        # Check if agenda item exists
        if await self.cosmos_client.vertex_exists(item_id):
            await self.cosmos_client.create_edge_if_not_exists(
                from_id=item_id,
                to_id=resolution_id,
                edge_type='REFERENCES_DOCUMENT',
                properties={'document_type': 'resolution'}
            )
            log.info(f"🔗 Linked resolution {doc_number} to agenda item {item_id}")
        else:
            log.warning(f"Agenda item {item_id} not found for resolution {doc_number}")
    
    async def cleanup(self):
        """Clean up resources."""
        if self.cosmos_client:
            await self.cosmos_client.close()


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Process resolution documents")
    parser.add_argument(
        "--meeting-date",
        help="Meeting date in MM.DD.YYYY format (e.g., 01.09.2024)"
    )
    parser.add_argument(
        "--resolutions-dir",
        type=Path,
        default=Path("city_clerk_documents/global/City Comissions 2024/Resolutions"),
        help="Directory containing resolution PDFs"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all resolutions found in directory"
    )
    
    args = parser.parse_args()
    
    processor = ResolutionProcessor()
    await processor.initialize()
    
    try:
        if args.meeting_date:
            # Process specific meeting
            count = await processor.process_resolutions_for_meeting(
                args.meeting_date,
                args.resolutions_dir
            )
            log.info(f"✅ Processed {count} resolutions for meeting {args.meeting_date}")
        
        elif args.all:
            # Process all resolutions
            log.info("Processing all resolutions in directory...")
            
            # Find all resolution files
            all_resolution_files = list(args.resolutions_dir.rglob("*.pdf"))
            
            # Extract unique meeting dates from filenames
            meeting_dates = set()
            date_pattern = re.compile(r'(\d{2})_(\d{2})_(\d{4})')
            
            for file in all_resolution_files:
                match = date_pattern.search(file.name)
                if match:
                    month, day, year = match.groups()
                    meeting_date = f"{month}.{day}.{year}"
                    meeting_dates.add(meeting_date)
            
            log.info(f"Found resolutions for {len(meeting_dates)} unique meeting dates")
            
            total_processed = 0
            for meeting_date in sorted(meeting_dates):
                log.info(f"\nProcessing meeting date: {meeting_date}")
                count = await processor.process_resolutions_for_meeting(
                    meeting_date,
                    args.resolutions_dir
                )
                total_processed += count
            
            log.info(f"\n✅ Processed {total_processed} total resolutions across {len(meeting_dates)} meetings")
        
    finally:
        await processor.cleanup()


if __name__ == "__main__":
    asyncio.run(main()) 