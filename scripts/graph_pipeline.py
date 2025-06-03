"""
City Clerk Graph Pipeline - UPDATED FOR SUBDIRECTORY STRUCTURE
Orchestrates the complete pipeline from PDF extraction to graph building.
"""

import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import json
from datetime import datetime
import argparse

# Import pipeline components
from graph_stages.agenda_pdf_extractor import AgendaPDFExtractor
from graph_stages.ontology_extractor import OntologyExtractor
from graph_stages.agenda_graph_builder import AgendaGraphBuilder
from graph_stages.cosmos_db_client import CosmosGraphClient
from graph_stages.enhanced_document_linker import EnhancedDocumentLinker
from graph_stages.verbatim_transcript_linker import VerbatimTranscriptLinker

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger('graph_pipeline')

# Pipeline control flags (similar to RAG pipeline)
RUN_EXTRACT_PDF = True
RUN_EXTRACT_ONTOLOGY = True
RUN_LINK_DOCUMENTS = True
RUN_LINK_VERBATIM = True
RUN_BUILD_GRAPH = True
RUN_VALIDATE_LINKS = True
CLEAR_GRAPH_FIRST = False  # Warning: This will delete all existing data!
UPSERT_EXISTING_NODES = True  # Update existing nodes instead of failing
UPSERT_MODE = True  # Enable upsert functionality
SKIP_EXISTING_EDGES = True  # Don't recreate existing edges


class CityClerkGraphPipeline:
    """Main pipeline orchestrator for city clerk document graph."""
    
    def __init__(self, 
                 base_dir: Path = Path("city_clerk_documents/global"),
                 output_dir: Path = Path("city_clerk_documents/graph_json"),
                 upsert_mode: bool = True):
        self.base_dir = base_dir
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.upsert_mode = upsert_mode
        
        # Define subdirectory structure
        self.city_dir = self.base_dir / "City Comissions 2024"
        self.agenda_dir = self.city_dir / "Agendas"
        self.ordinances_dir = self.city_dir / "Ordinances" / "2024"
        self.resolutions_dir = self.city_dir / "Resolutions"
        self.verbatim_dir = self.city_dir / "Verbating Items" / "2024"  # Note: appears to be typo in folder name
        
        # Initialize components
        self.pdf_extractor = AgendaPDFExtractor(output_dir)
        self.ontology_extractor = OntologyExtractor(output_dir=output_dir)
        self.document_linker = EnhancedDocumentLinker()
        self.verbatim_linker = VerbatimTranscriptLinker()
        self.cosmos_client = None
        self.graph_builder = None
        
        # Enhanced statistics
        self.stats = {
            "agendas_processed": 0,
            "ordinances_linked": 0,
            "resolutions_linked": 0,
            "verbatim_linked": 0,
            "missing_links": 0,
            "errors": 0,
            "nodes_created": 0,
            "nodes_updated": 0,
            "edges_created": 0,
            "edges_skipped": 0
        }
        
        # Store all missing items for final report
        self.all_missing_items = {}
    
    async def initialize(self):
        """Initialize async components."""
        # Initialize Cosmos DB client
        self.cosmos_client = CosmosGraphClient()
        await self.cosmos_client.connect()
        
        # Initialize graph builder with upsert mode
        self.graph_builder = AgendaGraphBuilder(self.cosmos_client, upsert_mode=self.upsert_mode)
        
        log.info(f"✅ Pipeline initialized (Upsert mode: {'ON' if self.upsert_mode else 'OFF'})")
    
    async def process_agenda(self, agenda_path: Path) -> Dict[str, Any]:
        """Process a single agenda through all pipeline stages."""
        log.info(f"\n{'='*60}")
        log.info(f"📋 Processing agenda: {agenda_path.name}")
        log.info(f"{'='*60}")
        
        result = {
            "agenda": agenda_path.name,
            "status": "pending",
            "stages": {}
        }
        
        try:
            # Stage 1: Extract PDF
            if RUN_EXTRACT_PDF:
                log.info("📄 Stage 1: Extracting PDF content...")
                extracted_data = self.pdf_extractor.extract_agenda(agenda_path)
                result["stages"]["pdf_extraction"] = {
                    "status": "success",
                    "sections": len(extracted_data.get("sections", [])),
                    "hyperlinks": len(extracted_data.get("hyperlinks", {}))
                }
            else:
                log.info("⏭️  Skipping PDF extraction (RUN_EXTRACT_PDF=False)")
                # Load existing extracted data
                extracted_path = self.output_dir / f"{agenda_path.stem}_extracted.json"
                if extracted_path.exists():
                    with open(extracted_path, 'r') as f:
                        extracted_data = json.load(f)
                else:
                    raise FileNotFoundError(f"No extracted data found for {agenda_path.name}")
            
            # Stage 2: Extract Ontology
            if RUN_EXTRACT_ONTOLOGY:
                log.info("🧠 Stage 2: Extracting rich ontology with LLM...")
                ontology = self.ontology_extractor.extract_ontology(agenda_path)
                
                # Validate that all expected sections are present
                expected_sections = [
                    'PRESENTATIONS', 'PUBLIC_COMMENTS', 'CONSENT', 
                    'ORDINANCES', 'RESOLUTIONS', 'BOARDS_COMMITTEES',
                    'CITY_MANAGER', 'CITY_ATTORNEY', 'CITY_CLERK', 'DISCUSSION'
                ]

                for expected in expected_sections:
                    if not any(s.get('section_type') == expected for s in ontology.get('sections', [])):
                        # Add missing section
                        ontology['sections'].append({
                            'section_name': expected.replace('_', ' ').title(),
                            'section_type': expected,
                            'order': len(ontology['sections']) + 1,
                            'items': [],
                            'description': 'No items in this section'
                        })
                
                # Verify ontology extraction
                num_sections = len(ontology.get('sections', []))
                num_entities = len(ontology.get('entities', []))
                total_items = sum(len(s.get('items', [])) for s in ontology.get('sections', []))
                
                log.info(f"✅ Ontology extracted: {num_sections} sections, {total_items} items, {num_entities} entities")
                
                result["stages"]["ontology_extraction"] = {
                    "status": "success",
                    "meeting_date": ontology.get("meeting_date"),
                    "sections": num_sections,
                    "agenda_items": total_items,
                    "entities": num_entities,
                    "relationships": len(ontology.get('relationships', []))
                }
            else:
                log.info("⏭️  Skipping ontology extraction (RUN_EXTRACT_ONTOLOGY=False)")
                # Load existing ontology
                ontology_path = self.output_dir / f"{agenda_path.stem}_ontology.json"
                if ontology_path.exists():
                    with open(ontology_path, 'r') as f:
                        ontology = json.load(f)
                else:
                    raise FileNotFoundError(f"No ontology found for {agenda_path.name}")
            
            # Stage 3: Link Documents
            linked_docs = {}
            if RUN_LINK_DOCUMENTS:
                log.info("🔗 Stage 3: Linking ordinance AND resolution documents...")
                meeting_date = ontology.get("meeting_date")
                
                # Pass both directories to the enhanced document linker
                linked_docs = await self.document_linker.link_documents_for_meeting(
                    meeting_date,
                    self.ordinances_dir,      # Ordinances directory
                    self.resolutions_dir      # Resolutions directory
                )
                
                total_linked = len(linked_docs.get("ordinances", [])) + len(linked_docs.get("resolutions", []))
                self.stats["ordinances_linked"] += len(linked_docs.get("ordinances", []))
                self.stats["resolutions_linked"] = self.stats.get("resolutions_linked", 0) + len(linked_docs.get("resolutions", []))
                
                result["stages"]["document_linking"] = {
                    "status": "success",
                    "ordinances_found": len(linked_docs.get("ordinances", [])),
                    "resolutions_found": len(linked_docs.get("resolutions", [])),
                    "total_linked": total_linked
                }
            else:
                log.info("⏭️  Skipping document linking (RUN_LINK_DOCUMENTS=False)")
            
            # Stage 3.5: Link Verbatim Transcripts (NEW)
            if RUN_LINK_VERBATIM:
                log.info("🎤 Stage 3.5: Linking verbatim transcript documents...")
                meeting_date = ontology.get("meeting_date")
                
                try:
                    verbatim_transcripts = await self.verbatim_linker.link_transcripts_for_meeting(
                        meeting_date,
                        self.verbatim_dir
                    )
                    
                    # Add verbatim transcripts to linked_docs
                    linked_docs["verbatim_transcripts"] = verbatim_transcripts
                    
                    total_transcripts = sum(len(v) for v in verbatim_transcripts.values())
                    self.stats["verbatim_linked"] += total_transcripts
                    
                    result["stages"]["verbatim_linking"] = {
                        "status": "success",
                        "item_transcripts": len(verbatim_transcripts.get("item_transcripts", [])),
                        "public_comments": len(verbatim_transcripts.get("public_comments", [])),
                        "section_transcripts": len(verbatim_transcripts.get("section_transcripts", [])),
                        "total_linked": total_transcripts
                    }
                    
                    log.info(f"✅ Linked {total_transcripts} verbatim transcripts")
                except Exception as e:
                    log.error(f"❌ Error in verbatim linking: {e}")
                    result["stages"]["verbatim_linking"] = {
                        "status": "error",
                        "error": str(e)
                    }
            else:
                log.info("⏭️  Skipping verbatim transcript linking (RUN_LINK_VERBATIM=False)")
            
            # Stage 4: Build Graph
            if RUN_BUILD_GRAPH:
                log.info("🏗️  Stage 4: Building enhanced graph representation...")
                
                graph_data = await self.graph_builder.build_graph_from_ontology(
                    ontology, 
                    agenda_path,
                    linked_docs  # This now includes verbatim_transcripts
                )
                
                # Update stats from graph builder
                self.stats["nodes_created"] += self.graph_builder.stats.get("nodes_created", 0)
                self.stats["nodes_updated"] += self.graph_builder.stats.get("nodes_updated", 0)
                self.stats["edges_created"] += self.graph_builder.stats.get("edges_created", 0)
                self.stats["edges_skipped"] += self.graph_builder.stats.get("edges_skipped", 0)
                
                result["stages"]["graph_building"] = {
                    "status": "success",
                    "sections": graph_data.get("statistics", {}).get("sections", 0),
                    "agenda_items": graph_data.get("statistics", {}).get("items", 0),
                    "entities": graph_data.get("statistics", {}).get("entities", 0),
                    "relationships": graph_data.get("statistics", {}).get("relationships", 0),
                    "nodes_created": self.graph_builder.stats.get("nodes_created", 0),
                    "edges_created": self.graph_builder.stats.get("edges_created", 0)
                }
            else:
                log.info("⏭️  Skipping graph building (RUN_BUILD_GRAPH=False)")
            
            # Stage 5: Validate Links
            if RUN_VALIDATE_LINKS:
                log.info("✅ Stage 5: Validating document links...")
                validation_results = await self._validate_links(ontology, linked_docs)
                result["stages"]["link_validation"] = validation_results
            else:
                log.info("⏭️  Skipping link validation (RUN_VALIDATE_LINKS=False)")
            
            result["status"] = "success"
            self.stats["agendas_processed"] += 1
            
        except Exception as e:
            log.error(f"❌ Error processing {agenda_path.name}: {e}")
            import traceback
            traceback.print_exc()
            result["status"] = "error"
            result["error"] = str(e)
            self.stats["errors"] += 1
        
        return result
    
    async def _validate_links(self, ontology: Dict, linked_docs: Dict) -> Dict[str, Any]:
        """Validate that all agenda items are properly linked."""
        validation_results = {
            "status": "success",
            "total_items": 0,
            "linked_items": 0,
            "unlinked_items": []
        }
        
        # Get all agenda items from sections
        all_items = []
        for section in ontology.get("sections", []):
            all_items.extend(section.get("items", []))
        
        validation_results["total_items"] = len(all_items)
        
        # Get all linked document item codes
        linked_item_codes = set()
        for doc_type in ["ordinances", "resolutions"]:
            for doc in linked_docs.get(doc_type, []):
                if doc.get("item_code"):
                    normalized_code = self.graph_builder.normalize_item_code(doc["item_code"])
                    linked_item_codes.add(normalized_code)
        
        # Check each agenda item
        for item in all_items:
            normalized_code = self.graph_builder.normalize_item_code(item.get("item_code", ""))
            if normalized_code in linked_item_codes:
                validation_results["linked_items"] += 1
            else:
                validation_results["unlinked_items"].append({
                    "item_code": item.get("item_code"),
                    "title": item.get("title", "Unknown"),
                    "document_reference": item.get("document_reference")
                })
        
        return validation_results
    
    async def run(self, agenda_pattern: str = "Agenda*.pdf"):
        """Run the complete pipeline."""
        log.info("🚀 Starting City Clerk Graph Pipeline")
        log.info(f"📁 Base directory: {self.base_dir}")
        log.info(f"📁 Agenda directory: {self.agenda_dir}")
        log.info(f"📁 Ordinances directory: {self.ordinances_dir}")
        log.info(f"📁 Resolutions directory: {self.resolutions_dir}")
        log.info(f"📁 Verbatim directory: {self.verbatim_dir}")
        
        # Check directories exist
        if not self.agenda_dir.exists():
            log.error(f"❌ Agenda directory does not exist: {self.agenda_dir}")
            log.info(f"💡 Please ensure the directory structure exists")
            return
        
        if not self.ordinances_dir.exists():
            log.warning(f"⚠️  Ordinances directory does not exist: {self.ordinances_dir}")
            log.info(f"💡 Document linking may fail without ordinances")
        
        if not self.resolutions_dir.exists():
            log.warning(f"⚠️  Resolutions directory does not exist: {self.resolutions_dir}")
            log.info(f"💡 Resolution linking may fail without resolutions directory")
        
        if not self.verbatim_dir.exists():
            log.warning(f"⚠️  Verbatim directory does not exist: {self.verbatim_dir}")
            log.info(f"💡 Verbatim transcript linking may fail")
        
        log.info(f"📊 Pipeline stages enabled:")
        log.info(f"   - Extract PDF: {RUN_EXTRACT_PDF}")
        log.info(f"   - Extract Ontology: {RUN_EXTRACT_ONTOLOGY}")
        log.info(f"   - Link Documents: {RUN_LINK_DOCUMENTS}")
        log.info(f"   - Link Verbatim: {RUN_LINK_VERBATIM}")
        log.info(f"   - Build Graph: {RUN_BUILD_GRAPH}")
        log.info(f"   - Validate Links: {RUN_VALIDATE_LINKS}")
        
        # Initialize components
        await self.initialize()
        
        # Clear graph if requested
        if CLEAR_GRAPH_FIRST and self.cosmos_client:
            log.warning("🗑️  Clearing existing graph data...")
            await self.cosmos_client.clear_graph()
        
        # Find agenda files
        agenda_files = sorted(self.agenda_dir.glob(agenda_pattern))
        log.info(f"📋 Found {len(agenda_files)} agenda files")
        
        if agenda_files:
            log.info("📄 Agenda files found:")
            for f in agenda_files[:5]:
                log.info(f"   - {f.name}")
            if len(agenda_files) > 5:
                log.info(f"   ... and {len(agenda_files) - 5} more")
        
        # Process each agenda
        results = []
        for agenda_path in agenda_files:
            result = await self.process_agenda(agenda_path)
            results.append(result)
        
        # Generate summary report
        await self._generate_report(results)
        
        # Generate consolidated missing items report
        if self.all_missing_items:
            await self._generate_missing_items_report()
        
        # Cleanup
        if self.cosmos_client:
            await self.cosmos_client.close()
        
        log.info("✅ Pipeline complete!")
    
    async def _generate_report(self, results: List[Dict[str, Any]]):
        """Generate pipeline execution report."""
        report = {
            "pipeline_run": datetime.utcnow().isoformat(),
            "statistics": self.stats,
            "agenda_results": results
        }
        
        # Save report
        report_path = self.output_dir / f"pipeline_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        # Print summary
        log.info(f"\n{'='*60}")
        log.info("📊 Pipeline Summary:")
        log.info(f"   - Agendas processed: {self.stats['agendas_processed']}")
        log.info(f"   - Ordinances linked: {self.stats['ordinances_linked']}")
        log.info(f"   - Resolutions linked: {self.stats['resolutions_linked']}")
        log.info(f"   - Verbatim linked: {self.stats['verbatim_linked']}")
        log.info(f"   - Missing links: {self.stats['missing_links']}")
        log.info(f"   - Errors: {self.stats['errors']}")
        log.info(f"   - Report saved to: {report_path.name}")
        log.info(f"{'='*60}")
    
    async def _generate_missing_items_report(self):
        """Generate consolidated report of missing agenda items."""
        report = {
            "generated_at": datetime.utcnow().isoformat(),
            "summary": {
                "total_agendas_processed": self.stats["agendas_processed"],
                "total_missing_items": self.stats["missing_links"],
                "agendas_with_missing_items": len(self.all_missing_items)
            },
            "by_agenda": self.all_missing_items
        }
        
        report_path = self.output_dir / "consolidated_missing_items_report.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        log.info(f"📋 Missing items report saved to: {report_path.name}")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="City Clerk Graph Pipeline")
    parser.add_argument(
        "--base-dir", 
        type=Path,
        default=Path("city_clerk_documents/global"),
        help="Base directory containing City Comissions folder"
    )
    parser.add_argument(
        "--pattern",
        default="Agenda*.pdf",
        help="File pattern for agenda PDFs"
    )
    parser.add_argument(
        "--clear-graph",
        action="store_true",
        help="Clear existing graph before processing"
    )
    parser.add_argument(
        "--no-upsert",
        action="store_true",
        help="Disable upsert mode (fail on existing nodes)"
    )
    
    # Pipeline stage controls
    parser.add_argument("--skip-pdf-extract", action="store_true", help="Skip PDF extraction")
    parser.add_argument("--skip-ontology", action="store_true", help="Skip ontology extraction")
    parser.add_argument("--skip-linking", action="store_true", help="Skip document linking")
    parser.add_argument("--skip-verbatim", action="store_true", help="Skip verbatim transcript linking")
    parser.add_argument("--skip-graph", action="store_true", help="Skip graph building")
    parser.add_argument("--skip-validation", action="store_true", help="Skip link validation")
    
    args = parser.parse_args()
    
    # Override global flags based on arguments
    global CLEAR_GRAPH_FIRST, RUN_EXTRACT_PDF, RUN_EXTRACT_ONTOLOGY, RUN_LINK_DOCUMENTS, RUN_BUILD_GRAPH, RUN_VALIDATE_LINKS
    
    if args.clear_graph:
        CLEAR_GRAPH_FIRST = True
    
    if args.skip_pdf_extract:
        RUN_EXTRACT_PDF = False
    if args.skip_ontology:
        RUN_EXTRACT_ONTOLOGY = False
    if args.skip_linking:
        RUN_LINK_DOCUMENTS = False
    if args.skip_verbatim:
        RUN_LINK_VERBATIM = False
    if args.skip_graph:
        RUN_BUILD_GRAPH = False
    if args.skip_validation:
        RUN_VALIDATE_LINKS = False
    
    # Create and run pipeline
    pipeline = CityClerkGraphPipeline(
        base_dir=args.base_dir,
        upsert_mode=not args.no_upsert  # Default is True unless --no-upsert
    )
    await pipeline.run(agenda_pattern=args.pattern)


if __name__ == "__main__":
    asyncio.run(main()) 