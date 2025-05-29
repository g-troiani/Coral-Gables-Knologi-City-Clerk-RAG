#!/usr/bin/env python3
"""
City Clerk Agenda Graph Pipeline - Main Orchestrator
====================================================
Orchestrates the extraction of meaningful entities and relationships from agenda documents.
Similar to pipeline_modular_optimized.py but for graph database population.
"""
import json
import logging
import asyncio
from pathlib import Path
from typing import Dict, List, Optional
from collections import Counter

from dotenv import load_dotenv
from openai import AzureOpenAI
import os

# Import stages
from stages.extract_clean import extract_pdf, _make_converter
from graph_stages.cosmos_db_client import CosmosGraphClient
from graph_stages.agenda_ontology_extractor import CityClerkOntologyExtractor
from graph_stages.agenda_graph_builder import AgendaGraphBuilder

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("agenda_pipeline")

# Azure OpenAI Configuration
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")
DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")

# Initialize Azure OpenAI client
aoai = AzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION
)
aoai.deployment_name = DEPLOYMENT_NAME  # Add deployment name as attribute


class AgendaPipelineOrchestrator:
    """Main orchestrator for agenda document processing pipeline."""
    
    def __init__(self, cosmos_client: CosmosGraphClient):
        self.cosmos_client = cosmos_client
        self.ontology_extractor = CityClerkOntologyExtractor(aoai)
        self.graph_builder = AgendaGraphBuilder(cosmos_client)
        self.stats = Counter()
        
    async def process_agenda(self, agenda_path: Path) -> Dict:
        """Process a single agenda document through all stages."""
        log.info(f"Processing agenda: {agenda_path.name}")
        
        try:
            # Stage 1: Extract PDF content
            conv = _make_converter()
            json_path = extract_pdf(
                agenda_path,
                Path("city_clerk_documents/txt"),
                Path("city_clerk_documents/json"),
                conv,
                enrich_llm=False  # We'll do our own LLM extraction
            )
            
            agenda_data = json.loads(json_path.read_text())
            
            # Stage 2: Extract ontology using LLM
            log.info(f"Extracting ontology for {agenda_path.name}")
            ontology = await self.ontology_extractor.extract_agenda_ontology(
                agenda_data, 
                agenda_path.name
            )
            
            # Stage 3: Build graph from ontology
            log.info(f"Building graph for {agenda_path.name}")
            graph_data = await self.graph_builder.build_graph_from_ontology(
                ontology, 
                agenda_path
            )
            
            # Update statistics
            self._update_stats(graph_data)
            
            return graph_data
            
        except Exception as e:
            log.error(f"Failed to process {agenda_path.name}: {e}")
            self.stats['failed'] += 1
            raise
    
    async def process_batch(self, agenda_files: List[Path], batch_size: int = 3):
        """Process multiple agenda files in batches."""
        total_files = len(agenda_files)
        
        for i in range(0, total_files, batch_size):
            batch = agenda_files[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_files + batch_size - 1) // batch_size
            
            log.info(f"\n{'='*60}")
            log.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} files)")
            log.info(f"Overall progress: {i}/{total_files} files completed ({i/total_files*100:.1f}%)")
            log.info(f"{'='*60}")
            
            # Process each file in the batch
            for agenda_file in batch:
                try:
                    await self.process_agenda(agenda_file)
                except Exception as e:
                    log.error(f"Failed to process {agenda_file.name}: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Log batch statistics
            self._log_batch_stats(batch_num)
    
    def _update_stats(self, graph_data: Dict):
        """Update pipeline statistics."""
        stats = graph_data.get('statistics', {})
        self.stats['meetings'] += 1
        self.stats['sections'] += stats.get('sections', 0)
        self.stats['items'] += stats.get('items', 0)
        self.stats['relationships'] += stats.get('relationships', 0)
        
        # Update entity counts
        for entity_type, count in stats.get('entities', {}).items():
            self.stats[f'entity_{entity_type}'] = self.stats.get(f'entity_{entity_type}', 0) + count
    
    def _log_batch_stats(self, batch_num: int):
        """Log statistics for the current batch."""
        log.info(f"\nBatch {batch_num} Statistics:")
        log.info(f"  - Meetings processed: {self.stats['meetings']}")
        log.info(f"  - Total sections: {self.stats['sections']}")
        log.info(f"  - Total items: {self.stats['items']}")
        log.info(f"  - Total relationships: {self.stats['relationships']}")
        
        # Log entity counts
        entity_stats = {k.replace('entity_', ''): v for k, v in self.stats.items() if k.startswith('entity_')}
        if entity_stats:
            log.info(f"  - Entities: {entity_stats}")
    
    def get_final_stats(self) -> Dict:
        """Get final pipeline statistics."""
        return dict(self.stats)


async def find_agenda_files() -> List[Path]:
    """Find all agenda PDF files in the project."""
    # Start from the script's parent directory (project root)
    project_root = Path(__file__).parent.parent
    
    possible_paths = [
        project_root / "city_clerk_documents" / "global" / "City Commissions 2024" / "Agendas",
        project_root / "city_clerk_documents" / "Agendas",
        project_root / "Agendas",
        Path.cwd() / "city_clerk_documents" / "global" / "City Commissions 2024" / "Agendas",
        Path.cwd() / "city_clerk_documents" / "Agendas",
        Path.cwd() / "Agendas",
    ]
    
    # Also search recursively from project root
    agenda_dirs = list(project_root.rglob("**/Agendas"))
    possible_paths.extend(agenda_dirs)
    
    # Also search for any directory containing agenda PDFs
    agenda_pdfs = list(project_root.rglob("**/Agenda*.pdf"))
    if agenda_pdfs:
        # Get unique parent directories
        pdf_dirs = set(pdf.parent for pdf in agenda_pdfs)
        possible_paths.extend(pdf_dirs)
    
    agenda_files = []
    searched_paths = []
    
    for path in possible_paths:
        searched_paths.append(str(path))
        if path.exists() and path.is_dir():
            found_files = sorted(path.glob("Agenda *.pdf"))
            if found_files:
                log.info(f"✅ Found {len(found_files)} agenda files in: {path}")
                agenda_files = found_files
                break
    
    if not agenda_files:
        log.error("❌ No agenda files found! Searched in:")
        for path in searched_paths[:5]:  # Show first 5 paths searched
            log.error(f"   - {path}")
        
        # Try to find any PDF files at all
        all_pdfs = list(project_root.rglob("*.pdf"))
        if all_pdfs:
            log.info(f"Found {len(all_pdfs)} PDF files in project. First few:")
            for pdf in all_pdfs[:5]:
                log.info(f"   - {pdf.relative_to(project_root)}")
    
    return agenda_files


async def test_cosmos_connection(cosmos_client: CosmosGraphClient):
    """Test basic Cosmos DB operations."""
    log.info("🧪 Testing Cosmos DB connection...")
    
    try:
        # Test 1: Count vertices
        count_query = "g.V().count()"
        result = await cosmos_client._execute_query(count_query)
        log.info(f"✅ Vertex count: {result[0] if result else 0}")
        
        # Test 2: Create a test node
        test_id = "test-node-12345"
        create_query = f"g.addV('TestNode').property('id','{test_id}').property('partitionKey','demo')"
        result = await cosmos_client._execute_query(create_query)
        log.info(f"✅ Created test node")
        
        # Test 3: Query the test node
        query = f"g.V('{test_id}')"
        result = await cosmos_client._execute_query(query)
        log.info(f"✅ Found test node: {len(result)} nodes")
        
        # Clean up
        await cosmos_client._execute_query(f"g.V('{test_id}').drop()")
        
        log.info("✅ All tests passed!")
        
    except Exception as e:
        log.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


async def main():
    """Main entry point for the agenda pipeline."""
    # Initialize Cosmos DB client
    cosmos_client = CosmosGraphClient(
        endpoint=os.getenv("COSMOS_ENDPOINT"),
        username=f"/dbs/{os.getenv('COSMOS_DATABASE')}/colls/{os.getenv('COSMOS_CONTAINER')}",
        password=os.getenv("COSMOS_KEY"),
        partition_key="partitionKey",
        partition_value="demo"
    )
    
    await cosmos_client.connect()
    
    try:
        # Run connection test first
        await test_cosmos_connection(cosmos_client)
        
        # Clear existing data
        log.info("Clearing existing graph data...")
        await cosmos_client.clear_graph()
        
        # Find agenda files
        agenda_files = await find_agenda_files()
        if not agenda_files:
            return
        
        log.info(f"Found {len(agenda_files)} agenda files to process")
        
        # Create pipeline orchestrator
        pipeline = AgendaPipelineOrchestrator(cosmos_client)
        
        # Process files in batches
        await pipeline.process_batch(
            agenda_files[:3],  # Process only first 3 for testing
            batch_size=1  # Process one at a time for better error tracking
        )
        
        # Log final statistics
        final_stats = pipeline.get_final_stats()
        log.info(f"\n{'='*60}")
        log.info("PIPELINE COMPLETE - FINAL STATISTICS")
        log.info(f"{'='*60}")
        for key, value in sorted(final_stats.items()):
            log.info(f"  {key}: {value}")
        log.info(f"{'='*60}")
        
    finally:
        await cosmos_client.close()


if __name__ == "__main__":
    asyncio.run(main()) 