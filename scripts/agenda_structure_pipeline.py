#!/usr/bin/env python3
"""
City Clerk Agenda Graph Pipeline - Main Orchestrator
====================================================
Orchestrates the extraction of meaningful entities and relationships from agenda documents.
Now uses dedicated graph pipeline PDF extractor.
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

# ============================================================================
# PIPELINE STAGE CONTROLS - Set to False to skip specific stages
# ============================================================================
RUN_PDF_EXTRACT = True      # Stage 1: Extract PDF content with hierarchy
RUN_ONTOLOGY    = True      # Stage 2: Extract ontology using LLM
RUN_BUILD_GRAPH = True      # Stage 3: Build graph from ontology
RUN_CLEAR_GRAPH = False     # Clear existing graph data before processing
RUN_CONN_TEST   = True      # Run connection test before processing
INTERACTIVE     = True      # Ask for user input (set False for automation)

# ============================================================================

# Import graph stages (no longer using RAG pipeline stages)
from graph_stages.agenda_pdf_extractor import AgendaPDFExtractor
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
        self.pdf_extractor = AgendaPDFExtractor()
        self.ontology_extractor = CityClerkOntologyExtractor(aoai)
        self.graph_builder = AgendaGraphBuilder(cosmos_client)
        self.stats = Counter()
        
    async def process_agenda(self, agenda_path: Path) -> Dict:
        """Process a single agenda document through all stages."""
        log.info(f"Processing agenda: {agenda_path.name}")
        
        # Track which stages ran
        stages_run = []
        
        try:
            extracted_data = None
            ontology = None
            graph_data = None
            
            # Stage 1: Extract PDF content with hierarchy preservation
            if RUN_PDF_EXTRACT:
                log.info(f"Stage 1: Extracting PDF content for {agenda_path.name}")
                extracted_data = self.pdf_extractor.extract_agenda(agenda_path)
                
                # Log extraction statistics
                stats = self.pdf_extractor.get_extraction_stats(extracted_data)
                log.info(f"Extraction stats: {stats}")
                stages_run.append("PDF_EXTRACT")
            else:
                log.info("Stage 1: SKIPPED (RUN_PDF_EXTRACT=False)")
                # Try to load existing extraction if available
                json_path = self.pdf_extractor.output_dir / f"{agenda_path.stem}_extracted.json"
                if json_path.exists():
                    log.info(f"Loading existing extraction from: {json_path}")
                    extracted_data = json.loads(json_path.read_text())
                else:
                    log.warning(f"No existing extraction found for {agenda_path.name}")
                    return {'error': 'No extraction available', 'stages_run': stages_run}
            
            # Convert to format expected by ontology extractor
            agenda_data = self._convert_to_agenda_format(extracted_data)
            
            # Stage 2: Extract ontology using LLM
            if RUN_ONTOLOGY:
                log.info(f"Stage 2: Extracting ontology for {agenda_path.name}")
                ontology = await self.ontology_extractor.extract_agenda_ontology(
                    agenda_data, 
                    agenda_path.name
                )
                
                # Save ontology for debugging/reuse
                ontology_path = self.pdf_extractor.output_dir / f"{agenda_path.stem}_ontology.json"
                ontology_path.write_text(json.dumps(ontology, indent=2))
                log.info(f"Saved ontology to: {ontology_path}")
                stages_run.append("ONTOLOGY")
            else:
                log.info("Stage 2: SKIPPED (RUN_ONTOLOGY=False)")
                # Try to load existing ontology
                ontology_path = self.pdf_extractor.output_dir / f"{agenda_path.stem}_ontology.json"
                if ontology_path.exists():
                    log.info(f"Loading existing ontology from: {ontology_path}")
                    ontology = json.loads(ontology_path.read_text())
                else:
                    log.warning(f"No existing ontology found for {agenda_path.name}")
                    return {'error': 'No ontology available', 'stages_run': stages_run}
            
            # Stage 3: Build graph from ontology
            if RUN_BUILD_GRAPH:
                log.info(f"Stage 3: Building graph for {agenda_path.name}")
                graph_data = await self.graph_builder.build_graph_from_ontology(
                    ontology, 
                    agenda_path
                )
                stages_run.append("BUILD_GRAPH")
                
                # Update statistics
                self._update_stats(graph_data)
            else:
                log.info("Stage 3: SKIPPED (RUN_BUILD_GRAPH=False)")
                # Create minimal graph data for statistics
                graph_data = {
                    'statistics': {
                        'sections': len(ontology.get('agenda_structure', [])),
                        'items': len(ontology.get('item_codes', [])),
                        'entities': len(ontology.get('entities', {})),
                        'relationships': len(ontology.get('relationships', []))
                    }
                }
            
            # Add stages run to result
            if graph_data:
                graph_data['stages_run'] = stages_run
            
            return graph_data or {'stages_run': stages_run}
            
        except Exception as e:
            log.error(f"Failed to process {agenda_path.name}: {e}")
            self.stats['failed'] += 1
            raise
    
    def _convert_to_agenda_format(self, extracted_data: Dict) -> Dict:
        """Convert extracted data to format expected by ontology extractor."""
        # Build sections from the extracted hierarchy
        sections = []
        
        # Add title as first section
        if extracted_data.get('title'):
            sections.append({
                'section': 'Title',
                'text': extracted_data['title'],
                'page_number': 1
            })
        
        # Add preamble if exists
        if extracted_data.get('preamble'):
            preamble_text = '\n'.join([
                item['text'] for item in extracted_data['preamble']
            ])
            sections.append({
                'section': 'Preamble',
                'text': preamble_text,
                'page_number': 1
            })
        
        # Convert hierarchical sections
        for section in extracted_data.get('sections', []):
            section_text = section.get('title', '') + '\n\n'
            
            # Add section content
            for content in section.get('content', []):
                section_text += content.get('text', '') + '\n'
            
            # Add subsections
            for subsection in section.get('subsections', []):
                section_text += f"\n{subsection.get('title', '')}\n"
                for content in subsection.get('content', []):
                    section_text += content.get('text', '') + '\n'
            
            sections.append({
                'section': section.get('title', 'Untitled'),
                'text': section_text.strip(),
                'page_start': section.get('page_start', 1),
                'elements': section.get('content', [])
            })
        
        # Include agenda items as a special section
        if extracted_data.get('agenda_items'):
            items_text = "EXTRACTED AGENDA ITEMS:\n\n"
            for item in extracted_data['agenda_items']:
                items_text += f"{item['code']}: {item.get('title', item.get('context', '')[:100])}\n"
            
            sections.append({
                'section': 'Agenda Items Summary',
                'text': items_text
            })
        
        return {
            'sections': sections,
            'metadata': extracted_data.get('metadata', {}),
            'agenda_items': extracted_data.get('agenda_items', []),
            'hyperlinks': extracted_data.get('hyperlinks', {})
        }
    
    async def process_batch(self, agenda_files: List[Path], batch_size: int = 3):
        """Process multiple agenda files in batches."""
        total_files = len(agenda_files)
        
        # Log pipeline configuration
        log.info(f"\n{'='*60}")
        log.info("PIPELINE CONFIGURATION:")
        log.info(f"  PDF Extract: {'ENABLED' if RUN_PDF_EXTRACT else 'DISABLED'}")
        log.info(f"  Ontology:    {'ENABLED' if RUN_ONTOLOGY else 'DISABLED'}")
        log.info(f"  Build Graph: {'ENABLED' if RUN_BUILD_GRAPH else 'DISABLED'}")
        log.info(f"{'='*60}\n")
        
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
                    result = await self.process_agenda(agenda_file)
                    log.info(f"Completed {agenda_file.name} - Stages run: {result.get('stages_run', [])}")
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
    # Start from the project root (parent of scripts directory)
    project_root = Path(__file__).parent.parent
    
    log.info(f"Searching for agenda files from: {project_root}")
    
    # List of possible locations
    possible_paths = [
        project_root / "city_clerk_documents" / "global" / "City Commissions 2024" / "Agendas",
        project_root / "city_clerk_documents" / "Agendas",
        project_root / "Agendas",
    ]
    
    # Also search recursively
    log.info("Searching recursively for Agendas directories...")
    agenda_dirs = list(project_root.rglob("**/Agendas"))
    possible_paths.extend(agenda_dirs)
    
    agenda_files = []
    searched_paths = []
    
    for path in possible_paths:
        searched_paths.append(str(path))
        if path.exists() and path.is_dir():
            # Look for files matching "Agenda *.pdf" pattern
            found_files = sorted(path.glob("Agenda *.pdf"))
            if found_files:
                log.info(f"✅ Found {len(found_files)} agenda files in: {path}")
                agenda_files = found_files
                break
            else:
                # Also try without space
                found_files = sorted(path.glob("Agenda*.pdf"))
                if found_files:
                    log.info(f"✅ Found {len(found_files)} agenda files in: {path}")
                    agenda_files = found_files
                    break
    
    if not agenda_files:
        log.error("❌ No agenda files found! Searched in:")
        for path in searched_paths[:10]:  # Show first 10 paths
            log.error(f"   - {path}")
        
        # Try to find any PDF files to help debug
        all_pdfs = list(project_root.rglob("*.pdf"))
        if all_pdfs:
            log.info(f"\nFound {len(all_pdfs)} total PDF files. Showing some examples:")
            # Show PDFs that might be agendas
            agenda_like = [p for p in all_pdfs if 'agenda' in p.name.lower()]
            if agenda_like:
                log.info(f"Found {len(agenda_like)} PDFs with 'agenda' in name:")
                for pdf in agenda_like[:5]:
                    log.info(f"   - {pdf.relative_to(project_root)}")
            else:
                log.info("First few PDFs found:")
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
        # Run connection test if enabled
        if RUN_CONN_TEST:
            await test_cosmos_connection(cosmos_client)
        else:
            log.info("Connection test SKIPPED (RUN_CONN_TEST=False)")
        
        # Clear existing data if enabled
        if RUN_CLEAR_GRAPH:
            log.info("Clearing existing graph data...")
            await cosmos_client.clear_graph()
        else:
            log.info("Clear graph SKIPPED (RUN_CLEAR_GRAPH=False)")
            
        # Interactive mode check
        if INTERACTIVE and not RUN_CLEAR_GRAPH:
            clear_existing = input("\nClear existing graph data? (y/N): ").lower() == 'y'
            if clear_existing:
                log.info("Clearing existing graph data...")
                await cosmos_client.clear_graph()
        
        # Find agenda files
        agenda_files = await find_agenda_files()
        if not agenda_files:
            return
        
        log.info(f"Found {len(agenda_files)} agenda files to process")
        
        # Determine how many files to process
        num_to_process = 3  # Default
        if INTERACTIVE:
            user_input = input(f"\nHow many files to process? (1-{len(agenda_files)}, default=3): ")
            try:
                num_to_process = int(user_input)
            except:
                pass
        
        num_to_process = min(max(1, num_to_process), len(agenda_files))
        log.info(f"Will process {num_to_process} files")
        
        # Create pipeline orchestrator
        pipeline = AgendaPipelineOrchestrator(cosmos_client)
        
        # Process files in batches
        await pipeline.process_batch(
            agenda_files[:num_to_process],
            batch_size=1  # Process one at a time for better error tracking
        )
        
        # Log final statistics
        final_stats = pipeline.get_final_stats()
        log.info(f"\n{'='*60}")
        log.info("PIPELINE COMPLETE - FINAL STATISTICS")
        log.info(f"{'='*60}")
        log.info("Stages run configuration:")
        log.info(f"  PDF Extract: {'ENABLED' if RUN_PDF_EXTRACT else 'DISABLED'}")
        log.info(f"  Ontology:    {'ENABLED' if RUN_ONTOLOGY else 'DISABLED'}")
        log.info(f"  Build Graph: {'ENABLED' if RUN_BUILD_GRAPH else 'DISABLED'}")
        log.info("")
        log.info("Results:")
        for key, value in sorted(final_stats.items()):
            log.info(f"  {key}: {value}")
        log.info(f"{'='*60}")
        
    finally:
        await cosmos_client.close()


if __name__ == "__main__":
    # Display current configuration at startup
    print("\n" + "="*60)
    print("AGENDA GRAPH PIPELINE - CONFIGURATION")
    print("="*60)
    print(f"PDF Extract:    {'ENABLED' if RUN_PDF_EXTRACT else 'DISABLED'}")
    print(f"Ontology (LLM): {'ENABLED' if RUN_ONTOLOGY else 'DISABLED'}")
    print(f"Build Graph:    {'ENABLED' if RUN_BUILD_GRAPH else 'DISABLED'}")
    print(f"Clear Graph:    {'ENABLED' if RUN_CLEAR_GRAPH else 'DISABLED'}")
    print(f"Connection Test:{'ENABLED' if RUN_CONN_TEST else 'DISABLED'}")
    print(f"Interactive:    {'ENABLED' if INTERACTIVE else 'DISABLED'}")
    print("="*60 + "\n")
    
    asyncio.run(main()) 