#!/usr/bin/env python3
"""
Graph Database Pipeline for City Clerk Documents
================================================
Extracts documents, identifies relationships, and populates CosmosDB graph.
"""
from __future__ import annotations
import argparse
import asyncio
import json
import logging
import pathlib
import re
from collections import defaultdict, Counter
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Set
import multiprocessing as mp

from dotenv import load_dotenv
import os

# Import shared extraction logic
from stages import extract_clean

# Import graph-specific stages
from graph_stages import (
    agenda_parser,
    graph_extractor,
    cosmos_db_client,
    entity_deduplicator,
    relationship_builder
)

load_dotenv()

# Azure Cosmos DB credentials (placeholders)
COSMOS_DB_ENDPOINT = os.getenv("COSMOS_DB_ENDPOINT", "YOUR_COSMOS_DB_URI")
COSMOS_DB_KEY = os.getenv("COSMOS_DB_KEY", "YOUR_COSMOS_DB_KEY")
COSMOS_DB_DATABASE = os.getenv("COSMOS_DB_DATABASE", "CityClerkGraph")
COSMOS_DB_CONTAINER = os.getenv("COSMOS_DB_CONTAINER", "CityClerkDocuments")

# Gremlin-specific settings
GREMLIN_ENDPOINT = os.getenv("GREMLIN_ENDPOINT", "YOUR_GREMLIN_ENDPOINT")
GREMLIN_USERNAME = os.getenv("GREMLIN_USERNAME", f"/dbs/{COSMOS_DB_DATABASE}/colls/{COSMOS_DB_CONTAINER}")
GREMLIN_PASSWORD = os.getenv("GREMLIN_PASSWORD", COSMOS_DB_KEY)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(name)s — %(levelname)s — %(message)s"
)
log = logging.getLogger("graph_pipeline")

class GraphPipeline:
    """Main pipeline for graph database population."""
    
    def __init__(self, base_dir: pathlib.Path):
        self.base_dir = base_dir
        self.cosmos_client = None
        self.stats = Counter()
        
        # Entity caches for deduplication
        self.persons_cache: Dict[str, str] = {}  # name -> node_id
        self.meetings_cache: Dict[str, str] = {}  # date -> node_id
        self.documents_cache: Dict[str, str] = {}  # doc_path -> node_id
        
    async def initialize(self):
        """Initialize connections and caches."""
        self.cosmos_client = cosmos_db_client.CosmosGraphClient(
            endpoint=GREMLIN_ENDPOINT,
            username=GREMLIN_USERNAME,
            password=GREMLIN_PASSWORD
        )
        await self.cosmos_client.connect()
        
        # Load existing entities for deduplication
        await self._load_existing_entities()
        
    async def _load_existing_entities(self):
        """Load existing entities from database for deduplication."""
        # Load persons
        persons = await self.cosmos_client.get_all_persons()
        self.persons_cache = {p['name']: p['id'] for p in persons}
        
        # Load meetings
        meetings = await self.cosmos_client.get_all_meetings()
        self.meetings_cache = {m['date']: m['id'] for m in meetings}
        
        log.info(f"Loaded {len(self.persons_cache)} existing persons")
        log.info(f"Loaded {len(self.meetings_cache)} existing meetings")
    
    async def process_batch(self, batch_date: str):
        """Process all documents for a specific date."""
        log.info(f"Processing documents for date: {batch_date}")
        
        # 1. Find all documents for this date
        documents = self._find_documents_by_date(batch_date)
        if not documents:
            log.warning(f"No documents found for date {batch_date}")
            return
            
        log.info(f"Found {len(documents)} documents for {batch_date}")
        
        # 2. Create or get Meeting node
        meeting_id = await self._ensure_meeting_node(batch_date, documents)
        
        # 3. Process agenda first to extract item mappings
        agenda_doc = documents.get('agenda')
        item_mappings = {}
        if agenda_doc:
            log.info(f"Processing agenda: {agenda_doc.name}")
            item_mappings = await self._process_agenda(agenda_doc, meeting_id)
        
        # 4. Process other documents with item mappings
        for doc_type, doc_path in documents.items():
            if doc_type == 'agenda':
                continue  # Already processed
            
            log.info(f"Processing {doc_type}: {doc_path.name}")
            await self._process_document(doc_path, doc_type, meeting_id, item_mappings)
        
        self.stats['meetings_processed'] += 1
        self.stats['documents_processed'] += len(documents)
    
    def _find_documents_by_date(self, date_str: str) -> Dict[str, pathlib.Path]:
        """Find all documents for a given date."""
        documents = {}
        
        # Parse date components
        # Handle both MM.DD.YYYY and MM_DD_YYYY formats
        date_pattern = date_str.replace('.', '_')
        
        # Find agenda
        agenda_pattern1 = f"Agenda {date_str}.pdf"  # M.DD.YYYY format
        agenda_pattern2 = f"Agenda {date_pattern}.pdf"  # MM_DD_YYYY format
        
        for agenda_file in (self.base_dir / "Agendas").glob("*.pdf"):
            if agenda_pattern1 in agenda_file.name or agenda_pattern2 in agenda_file.name:
                documents['agenda'] = agenda_file
                break
        
        # Find ordinances
        ordinances_dir = self.base_dir / "Ordinances" / "2024"
        for ord_file in ordinances_dir.glob(f"*{date_pattern}*.pdf"):
            ord_num = self._extract_document_number(ord_file.name)
            if ord_num:
                documents[f'ordinance_{ord_num}'] = ord_file
        
        # Find resolutions
        resolutions_dir = self.base_dir / "Resolutions" / "2024"
        for res_file in resolutions_dir.glob(f"*{date_pattern}*.pdf"):
            res_num = self._extract_document_number(res_file.name)
            if res_num:
                documents[f'resolution_{res_num}'] = res_file
        
        # Find verbatim items
        verbatim_dir = self.base_dir / "Verbatim Items"
        if verbatim_dir.exists():
            for verb_file in verbatim_dir.glob(f"{date_pattern}*.pdf"):
                # Extract item code from filename if present
                item_code = self._extract_verbatim_item_code(verb_file.name)
                documents[f'verbatim_{item_code}'] = verb_file
        
        return documents
    
    def _extract_document_number(self, filename: str) -> Optional[str]:
        """Extract document number from filename like '2024-66 - 04_16_2024.pdf'"""
        match = re.search(r'(\d{4}-\d+)', filename)
        return match.group(1) if match else None
    
    def _extract_verbatim_item_code(self, filename: str) -> str:
        """Extract item code from verbatim filename."""
        # Pattern: MM_DD_YYYY - [item code or description].pdf
        match = re.search(r'\d{2}_\d{2}_\d{4}\s*-\s*(.+?)\.pdf', filename)
        if match:
            item_text = match.group(1)
            # Try to extract item code like E-4, F-12, etc.
            code_match = re.search(r'([A-Z]-?\d+)', item_text)
            return code_match.group(1) if code_match else item_text[:20]
        return "unknown"
    
    async def _ensure_meeting_node(self, date_str: str, documents: Dict) -> str:
        """Create or retrieve Meeting node."""
        if date_str in self.meetings_cache:
            return self.meetings_cache[date_str]
        
        # Determine meeting type from documents
        meeting_type = "Regular"  # Default
        if any('special' in str(doc).lower() for doc in documents.values()):
            meeting_type = "Special"
        elif any('workshop' in str(doc).lower() for doc in documents.values()):
            meeting_type = "Workshop"
        
        meeting_data = {
            "id": f"meeting-{date_str.replace('.', '-')}",
            "partitionKey": "meeting",
            "nodeType": "Meeting",
            "date": date_str,
            "type": meeting_type,
            "location": "405 Biltmore Way, Coral Gables, FL"  # Default location
        }
        
        meeting_id = await self.cosmos_client.create_meeting(meeting_data)
        self.meetings_cache[date_str] = meeting_id
        return meeting_id
    
    async def _process_agenda(self, agenda_path: pathlib.Path, meeting_id: str) -> Dict[str, Dict]:
        """Process agenda document and extract item mappings."""
        # Extract PDF content
        json_path = await self._extract_pdf(agenda_path)
        
        # Parse agenda items
        agenda_data = json.loads(json_path.read_text())
        item_mappings = agenda_parser.parse_agenda_items(agenda_data)
        
        # Create Document node for agenda
        doc_id = await self._create_document_node(
            agenda_path, 
            agenda_data, 
            "Agenda",
            meeting_id
        )
        
        # Extract and create Person nodes from agenda
        await self._extract_persons_from_document(agenda_data, doc_id, meeting_id)
        
        # Create chunks
        await self._create_document_chunks(doc_id, agenda_data)
        
        return item_mappings
    
    async def _process_document(
        self, 
        doc_path: pathlib.Path, 
        doc_type: str,
        meeting_id: str,
        item_mappings: Dict[str, Dict]
    ):
        """Process a non-agenda document."""
        # Extract PDF content
        json_path = await self._extract_pdf(doc_path)
        doc_data = json.loads(json_path.read_text())
        
        # Determine actual document type from item mappings
        doc_number = self._extract_document_number(doc_path.name)
        actual_type = self._determine_document_type(doc_number, item_mappings, doc_type)
        
        # Create Document node
        doc_id = await self._create_document_node(
            doc_path,
            doc_data,
            actual_type,
            meeting_id
        )
        
        # Extract and create Person nodes
        await self._extract_persons_from_document(doc_data, doc_id, meeting_id)
        
        # Create chunks
        await self._create_document_chunks(doc_id, doc_data)
        
        # Create relationships based on item mappings
        if doc_number in item_mappings:
            mapping = item_mappings[doc_number]
            # Additional relationships based on agenda item info
            if 'sponsor' in mapping:
                await self._create_authorship(doc_id, mapping['sponsor'])
    
    def _determine_document_type(
        self, 
        doc_number: str, 
        item_mappings: Dict,
        default_type: str
    ) -> str:
        """Determine actual document type from agenda item mappings."""
        if doc_number in item_mappings:
            item_type = item_mappings[doc_number].get('type', '').lower()
            if 'ordinance' in item_type:
                return 'Ordinance'
            elif 'resolution' in item_type:
                return 'Resolution'
        
        # Fallback to default based on file location
        if 'ordinance' in default_type:
            return 'Ordinance'
        elif 'resolution' in default_type:
            return 'Resolution'
        elif 'verbatim' in default_type:
            return 'Verbatim'
        
        return 'Document'  # Generic fallback
    
    async def _extract_pdf(self, pdf_path: pathlib.Path) -> pathlib.Path:
        """Extract PDF using shared extraction logic."""
        # Reuse existing extraction from stages
        from stages.extract_clean import run_one
        json_path = run_one(pdf_path, enrich_llm=True)
        return json_path
    
    async def _create_document_node(
        self,
        doc_path: pathlib.Path,
        doc_data: Dict,
        doc_type: str,
        meeting_id: str
    ) -> str:
        """Create Document node in graph."""
        doc_node = {
            "id": f"doc-{doc_path.stem}",
            "partitionKey": "document",
            "nodeType": "Document",
            "documentClass": "Agenda",
            "documentType": doc_type,
            "title": doc_data.get("title", doc_path.stem),
            "date": doc_data.get("date", ""),
            "source_pdf": str(doc_path),
            "created_at": datetime.utcnow().isoformat() + "Z",
            "keywords": doc_data.get("keywords", [])
        }
        
        # Add type-specific fields
        if doc_type == "Ordinance":
            doc_node["ordinance_number"] = self._extract_document_number(doc_path.name)
            doc_node["reading"] = doc_data.get("reading", "First")
        elif doc_type == "Resolution":
            doc_node["resolution_number"] = self._extract_document_number(doc_path.name)
            doc_node["status"] = doc_data.get("status", "Proposed")
        elif doc_type == "Verbatim":
            doc_node["meeting_duration"] = doc_data.get("meeting_duration", "")
            doc_node["transcript_type"] = "Full"
        
        doc_id = await self.cosmos_client.create_document(doc_node)
        
        # Create PRESENTED_AT edge
        await self.cosmos_client.create_edge(
            from_id=doc_id,
            to_id=meeting_id,
            edge_type="PRESENTED_AT"
        )
        
        return doc_id
    
    async def _extract_persons_from_document(
        self, 
        doc_data: Dict,
        doc_id: str,
        meeting_id: str
    ):
        """Extract person entities and create nodes/relationships."""
        persons = []
        
        # Extract officials
        for role in ["mayor", "vice_mayor", "city_attorney", "city_manager", 
                     "city_clerk", "public_works_director"]:
            if doc_data.get(role):
                persons.append({
                    "name": doc_data[role],
                    "roles": [role.replace("_", " ").title()]
                })
        
        # Extract commissioners
        for commissioner in doc_data.get("commissioners", []):
            persons.append({
                "name": commissioner,
                "roles": ["Commissioner"]
            })
        
        # Create Person nodes and relationships
        for person_data in persons:
            person_id = await self._ensure_person_node(person_data)
            
            # Create ATTENDED edge to meeting
            await self.cosmos_client.create_edge(
                from_id=person_id,
                to_id=meeting_id,
                edge_type="ATTENDED",
                properties={"role": person_data["roles"][0]}
            )
    
    async def _ensure_person_node(self, person_data: Dict) -> str:
        """Create or retrieve Person node."""
        name = person_data["name"]
        
        if name in self.persons_cache:
            return self.persons_cache[name]
        
        person_node = {
            "id": f"person-{name.lower().replace(' ', '-')}",
            "partitionKey": "person",
            "nodeType": "Person",
            "name": name,
            "roles": person_data.get("roles", [])
        }
        
        person_id = await self.cosmos_client.create_person(person_node)
        self.persons_cache[name] = person_id
        return person_id
    
    async def _create_document_chunks(self, doc_id: str, doc_data: Dict):
        """Create DocumentChunk nodes for a document."""
        # Use existing chunking logic
        from stages.chunk_text import chunk_optimized
        
        # Save to temp JSON for chunking
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tf:
            json.dump(doc_data, tf)
            temp_path = pathlib.Path(tf.name)
        
        try:
            chunks = chunk_optimized(temp_path)
            
            for chunk in chunks:
                chunk_node = {
                    "id": f"chunk-{doc_id}-{chunk['chunk_index']:04d}",
                    "partitionKey": "chunk",
                    "nodeType": "DocumentChunk",
                    "chunk_index": chunk["chunk_index"],
                    "text": chunk["text"],
                    "embedding": [],  # Placeholder for embeddings
                    "page_start": chunk.get("page_start", 1),
                    "page_end": chunk.get("page_end", 1)
                }
                
                chunk_id = await self.cosmos_client.create_chunk(chunk_node)
                
                # Create HAS_CHUNK edge
                await self.cosmos_client.create_edge(
                    from_id=doc_id,
                    to_id=chunk_id,
                    edge_type="HAS_CHUNK",
                    properties={"chunk_order": chunk["chunk_index"]}
                )
        finally:
            temp_path.unlink()
    
    async def _create_authorship(self, doc_id: str, sponsor_name: str):
        """Create authorship relationship."""
        person_id = await self._ensure_person_node({
            "name": sponsor_name,
            "roles": ["Sponsor"]
        })
        
        await self.cosmos_client.create_edge(
            from_id=doc_id,
            to_id=person_id,
            edge_type="AUTHORED_BY",
            properties={"role": "sponsor"}
        )
    
    async def run(self):
        """Run the complete pipeline."""
        await self.initialize()
        
        try:
            # Find all unique dates
            dates = self._find_all_meeting_dates()
            log.info(f"Found {len(dates)} unique meeting dates")
            
            # Process each date
            for date in sorted(dates):
                try:
                    await self.process_batch(date)
                except Exception as e:
                    log.error(f"Failed to process date {date}: {e}")
                    self.stats['failed_dates'] += 1
            
            # Log final statistics
            log.info("Pipeline complete!")
            log.info(f"Statistics: {dict(self.stats)}")
            
        finally:
            if self.cosmos_client:
                await self.cosmos_client.close()
    
    def _find_all_meeting_dates(self) -> Set[str]:
        """Find all unique meeting dates from document filenames."""
        dates = set()
        
        # Extract dates from agendas
        for agenda in (self.base_dir / "Agendas").glob("*.pdf"):
            # Match patterns like "Agenda 6.11.2024.pdf" or "Agenda 06.11.2024.pdf"
            match = re.search(r'Agenda\s+(\d{1,2})\.(\d{2})\.(\d{4})', agenda.name)
            if match:
                month, day, year = match.groups()
                # Normalize to MM.DD.YYYY format
                date_str = f"{int(month):02d}.{day}.{year}"
                dates.add(date_str)
        
        return dates

async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="City Clerk Graph Database Pipeline")
    parser.add_argument(
        "--base-dir",
        type=pathlib.Path,
        default=pathlib.Path("city_clerk_documents/global/City Commissions 2024"),
        help="Base directory containing city documents"
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Process only documents for a specific date (MM.DD.YYYY)"
    )
    parser.add_argument(
        "--clear-db",
        action="store_true",
        help="Clear existing graph database before processing"
    )
    
    args = parser.parse_args()
    
    # Validate base directory
    if not args.base_dir.exists():
        log.error(f"Base directory not found: {args.base_dir}")
        return
    
    # Create pipeline
    pipeline = GraphPipeline(args.base_dir)
    
    # Clear database if requested
    if args.clear_db:
        log.warning("Clearing existing graph database...")
        # TODO: Implement database clearing
    
    # Run pipeline
    if args.date:
        # Process single date
        await pipeline.initialize()
        try:
            await pipeline.process_batch(args.date)
        finally:
            await pipeline.cosmos_client.close()
    else:
        # Process all dates
        await pipeline.run()

if __name__ == "__main__":
    asyncio.run(main()) 