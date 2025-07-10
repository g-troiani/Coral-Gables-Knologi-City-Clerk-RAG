#!/usr/bin/env python3
"""
Integration module for orchestrating the 3-stage extraction pipeline
within the graph_rag_stages framework.
Enhanced for parallel processing of multiple documents.

This module coordinates:
- Stage 1: PDF OCR with Docling + PyMuPDF hyperlinks
- Stage 2: LLM agenda extraction with regex fallbacks
- Stage 3: Ontology enhancement with entity extraction
"""

import logging
import asyncio
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import re

# Import the extraction pipeline stages (now within graph_rag_stages)
from .stage1_pdf_ocr import PDFOCRExtractor
from .stage2_agenda_extraction import AgendaItemExtractor
from .stage3_ontology_enhancement import OntologyEnhancer
from .verbatim_transcript_processor import VerbatimTranscriptProcessor
from .enhanced_document_linker import EnhancedDocumentLinker

# tqdm is a great library for progress bars, especially for long-running tasks
from tqdm.asyncio import tqdm_asyncio

log = logging.getLogger(__name__)


class ExtractionPipelineIntegration:
    """Orchestrates the complete 3-stage extraction pipeline with parallel processing."""

    # NEW: Define a concurrency limit for the pipeline
    MAX_CONCURRENT_DOCUMENTS = 8  # Process up to 8 documents at a time

    def __init__(self, output_dir: Path = Path("city_clerk_documents/extracted_json")):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)

        # Initialize the extraction stages
        self.stage1 = PDFOCRExtractor(output_dir)
        self.stage2 = AgendaItemExtractor(output_dir)
        self.stage3 = OntologyEnhancer(output_dir)
        self.verbatim_processor = VerbatimTranscriptProcessor(output_dir)
        self.enhanced_document_linker = EnhancedDocumentLinker(output_dir)

    async def run_extraction_pipeline(self, base_dir: Path) -> List[Dict[str, Any]]:
        """
        Run the complete 3-stage extraction pipeline on all PDFs,
        processing multiple documents in parallel.
        
        Args:
            base_dir: Base directory containing PDF subdirectories
            
        Returns:
            List of all extracted documents
        """
        log.info(f"🚀 Starting integrated extraction pipeline from: {base_dir}")

        # Discover PDF files by category (RESTORED ORIGINAL LOGIC)
        pdf_files = self._discover_pdf_files(base_dir)
        
        # Flatten categorized files into a list with document type information
        all_pdfs_with_types = []
        for pdf_type, pdf_list in pdf_files.items():
            for pdf_path in pdf_list:
                all_pdfs_with_types.append((pdf_path, pdf_type))

        if not all_pdfs_with_types:
            log.warning("No PDF files found to process.")
            return []

        # NEW: Set up a semaphore to limit concurrency
        semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_DOCUMENTS)

        # NEW: Create a helper function to process a single document within the semaphore's control
        async def process_with_semaphore(pdf_info):
            async with semaphore:
                pdf_path, pdf_type = pdf_info
                return await self.process_single_document(pdf_path, pdf_type)

        # NEW: Create a list of tasks to run in parallel
        tasks = [process_with_semaphore(pdf_info) for pdf_info in all_pdfs_with_types]

        # NEW: Use tqdm.asyncio.gather to run all tasks concurrently with a progress bar
        log.info(f"Processing documents in parallel (up to {self.MAX_CONCURRENT_DOCUMENTS} at a time)...")
        results = await tqdm_asyncio.gather(*tasks, desc="Extracting Documents")
        
        # Filter out None results from skipped or failed files
        successful_results = [res for res in results if res is not None]

        # Process verbatim transcripts using hierarchical approach
        verbatim_results = await self._process_verbatim_transcripts_hierarchically(base_dir, successful_results)
        
        # Process legal documents using enhanced hierarchical approach
        legal_results = await self._process_legal_documents_hierarchically(base_dir, successful_results)

        log.info(f"✅ Extraction pipeline completed: {len(successful_results)} documents processed")
        log.info(f"📝 Hierarchical transcript processing: {verbatim_results['summary']['total_transcripts']} transcripts")
        log.info(f"📜 Enhanced legal document processing: {legal_results['summary']['total_documents']} legal documents")
        
        self.stage1.print_processing_summary()
        return successful_results

    def _discover_pdf_files(self, base_dir: Path) -> Dict[str, List[Path]]:
        """Discover and categorize PDF files by document type. (RESTORED ORIGINAL METHOD)"""
        categorized_files = {
            'agenda': [],
            'ordinance': [],
            'resolution': [],
            'transcript': []
        }
        
        # Check for standard subdirectories
        agenda_dir = base_dir / "Agendas"
        if agenda_dir.exists():
            categorized_files['agenda'] = list(agenda_dir.glob("*.pdf"))
        
        ord_dir = base_dir / "Ordinances"
        if ord_dir.exists():
            categorized_files['ordinance'] = list(ord_dir.rglob("*.pdf"))
        
        res_dir = base_dir / "Resolutions"
        if res_dir.exists():
            categorized_files['resolution'] = list(res_dir.rglob("*.pdf"))
        
        # Check for verbatim directories (may have different names)
        for vdir_name in ["Verbatim Items", "Verbating Items"]:
            vdir = base_dir / vdir_name
            if vdir.exists():
                categorized_files['transcript'] = list(vdir.rglob("*.pdf"))
                break
        
        total_files = sum(len(files) for files in categorized_files.values())
        log.info(f"📊 Discovered {total_files} PDFs: "
                f"{len(categorized_files['agenda'])} agendas, "
                f"{len(categorized_files['ordinance'])} ordinances, "
                f"{len(categorized_files['resolution'])} resolutions, "
                f"{len(categorized_files['transcript'])} transcripts")
        
        return categorized_files

    async def process_single_document(self, pdf_path: Path, pdf_type: str) -> Optional[Dict[str, Any]]:
        """
        Processes a single document through the 3-stage pipeline.
        This core logic for one document remains unchanged.
        """
        log.debug(f"Processing document: {pdf_path.name}")
        try:
            log.info(f"📄 Processing {pdf_type}: {pdf_path.name}")
            
            # Stage 1: PDF OCR (all documents)
            ocr_result = self.stage1.extract_pdf(pdf_path)
            
            # Check if document was skipped due to existing processing
            if ocr_result.get('metadata', {}).get('extraction_method') == 'skipped_already_processed':
                log.info(f"⏭️  Document {pdf_path.name} was skipped - already processed")
                return None

            if pdf_type == 'agenda':
                # Full 3-stage processing for agenda documents
                agenda_result = self.stage2.extract_agenda_structure(ocr_result)
                ontology_result = self.stage3.enhance_agenda_ontology(agenda_result)
                return ontology_result
            else:
                # Basic processing for supporting documents
                enhanced_result = self._enhance_non_agenda_document(ocr_result, pdf_type)
                return enhanced_result
                
        except Exception as e:
            log.error(f"❌ Failed to process document {pdf_path.name}: {e}", exc_info=True)
            return None

    def _enhance_non_agenda_document(self, ocr_result: Dict[str, Any], doc_type: str) -> Dict[str, Any]:
        """Enhance non-agenda documents with basic metadata."""
        enhanced_result = ocr_result.copy()
        
        # Add document type and extracted metadata
        enhanced_result['document_type'] = doc_type
        enhanced_result['meeting_date'] = self._extract_meeting_date(ocr_result['source_file'], ocr_result['full_text'], ocr_result['metadata'])
        enhanced_result['document_number'] = self._extract_document_number(ocr_result['source_file'])
        
        # CRITICAL: Ensure meeting_date is in the metadata dict for YAML header generation
        if 'metadata' not in enhanced_result:
            enhanced_result['metadata'] = {}
        enhanced_result['metadata']['meeting_date'] = enhanced_result['meeting_date']
        enhanced_result['metadata']['document_type'] = doc_type
        enhanced_result['metadata']['processing_stage'] = 'stage1_enhanced'
        enhanced_result['metadata']['document_category'] = doc_type
        
        return enhanced_result
    
    def _extract_meeting_date(self, filename: str, full_text: str = None, metadata: Dict = None) -> str:
        """Extract meeting date from filename, source file, or document content."""
        # Try different date patterns on the filename first
        patterns = [
            r'(\d{2})\.(\d{2})\.(\d{4})',  # 01.09.2024
            r'(\d{2})_(\d{2})_(\d{4})',    # 01_09_2024
            r'(\d{2})-(\d{2})-(\d{4})'     # 01-09-2024
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                month, day, year = match.groups()
                return f"{month.zfill(2)}.{day.zfill(2)}.{year}"
        
        # Try to extract from source file field in metadata
        if metadata and 'source_file' in metadata:
            source_file = metadata['source_file']
            for pattern in patterns:
                match = re.search(pattern, source_file)
                if match:
                    month, day, year = match.groups()
                    return f"{month.zfill(2)}.{day.zfill(2)}.{year}"
        
        # Try to extract from document content for ordinances/resolutions
        if full_text and ('ordinance' in filename.lower() or 'resolution' in filename.lower()):
            # First, look for 'Source File:' pattern in document content
            source_file_match = re.search(r'Source File:\s*([^\n]+)', full_text, re.IGNORECASE)
            if source_file_match:
                source_file_line = source_file_match.group(1)
                for pattern in patterns:
                    match = re.search(pattern, source_file_line)
                    if match:
                        month, day, year = match.groups()
                        return f"{month.zfill(2)}.{day.zfill(2)}.{year}"
            
            # Look for common meeting date patterns in ordinance content
            content_patterns = [
                # Meeting date patterns in WHEREAS clauses
                r'(?:public hearing|meeting).*?held.*?(?:on|before).*?(\w+)\s+(\d{1,2}),?\s+(\d{4})',
                r'(\w+)\s+(\d{1,2}),?\s+(\d{4}).*?(?:meeting|hearing|commission)',
                # Adoption date patterns
                r'(?:adopted|passed).*?(?:on|this).*?(\w+)\s+(\d{1,2}),?\s+(\d{4})',
                # General date patterns
                r'(\w+)\s+(\d{1,2}),?\s+(\d{4})',  # January 9, 2024 or November 15, 2016
                r'(\d{1,2})/(\d{1,2})/(\d{4})',    # 1/9/2024
                r'(\d{1,2})-(\d{1,2})-(\d{4})',    # 1-9-2024
                r'(\d{2})\.(\d{2})\.(\d{4})',      # 01.09.2024
                r'(\d{2})_(\d{2})_(\d{4})',        # 01_09_2024
            ]
            
            for pattern in content_patterns:
                # Look in first 3000 characters for efficiency
                matches = re.finditer(pattern, full_text[:3000], re.IGNORECASE)
                for match in matches:
                    try:
                        groups = match.groups()
                        if len(groups) == 3:
                            # Handle month name format
                            if groups[0].isalpha():
                                month_map = {
                                    'january': '01', 'february': '02', 'march': '03', 'april': '04',
                                    'may': '05', 'june': '06', 'july': '07', 'august': '08',
                                    'september': '09', 'october': '10', 'november': '11', 'december': '12'
                                }
                                month = month_map.get(groups[0].lower())
                                if month:
                                    day = str(groups[1]).zfill(2)
                                    year = str(groups[2])
                                    # Validate it's a reasonable date for city documents
                                    if 2000 <= int(year) <= 2030 and 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
                                        return f"{month}.{day}.{year}"
                            else:
                                # Numeric format
                                month = str(groups[0]).zfill(2)
                                day = str(groups[1]).zfill(2)
                                year = str(groups[2])
                                # Validate it's a reasonable date
                                if 2000 <= int(year) <= 2030 and 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
                                    return f"{month}.{day}.{year}"
                    except (ValueError, IndexError):
                        continue
        
        return "unknown"
    
    def _extract_document_number(self, filename: str) -> Optional[str]:
        """Extract document number from filename."""
        # Look for patterns like "2024-01", "2024-123"
        match = re.search(r'(\d{4}-\d+)', filename)
        return match.group(1) if match else None

    async def _process_verbatim_transcripts_hierarchically(self, base_dir: Path, extracted_documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process verbatim transcripts using the hierarchical filename-based approach.
        
        This method identifies meeting dates from processed agenda documents and
        processes corresponding verbatim transcripts to create explicit hierarchical
        relationships between meetings, agenda items, and their verbatim discussions.
        """
        log.info("🎤 Starting hierarchical verbatim transcript processing...")
        
        # Extract meeting dates from processed agenda documents
        meeting_dates = set()
        for doc in extracted_documents:
            # Check if this is an agenda document by filename or document type
            source_file = doc.get('source_file', '')
            doc_type = doc.get('document_type', '')
            meeting_date = doc.get('meeting_date')
            
            if meeting_date and ('agenda' in source_file.lower() or doc_type == 'agenda'):
                meeting_dates.add(meeting_date)
        
        if not meeting_dates:
            log.warning("No meeting dates found in extracted agenda documents")
            return {"meeting_dates": [], "summary": {"total_transcripts": 0}}
        
        log.info(f"📅 Found {len(meeting_dates)} meeting dates for transcript processing")
        
        # Process transcripts for each meeting date
        all_verbatim_results = []
        total_transcripts = 0
        
        for meeting_date in meeting_dates:
            try:
                log.info(f"🎤 Processing verbatim transcripts for: {meeting_date}")
                verbatim_result = self.verbatim_processor.process_verbatim_transcripts(base_dir, meeting_date)
                
                if verbatim_result['transcripts']:
                    all_verbatim_results.append(verbatim_result)
                    total_transcripts += verbatim_result['summary']['total_transcripts']
                    log.info(f"✅ Processed {verbatim_result['summary']['total_transcripts']} transcripts for {meeting_date}")
                else:
                    log.info(f"📝 No verbatim transcripts found for {meeting_date}")
                    
            except Exception as e:
                log.error(f"❌ Failed to process verbatim transcripts for {meeting_date}: {e}")
                continue
        
        # Create comprehensive verbatim result
        comprehensive_result = {
            "extraction_method": "hierarchical_filename_parsing",
            "meeting_dates": list(meeting_dates),
            "verbatim_collections": all_verbatim_results,
            "summary": {
                "total_meetings": len(meeting_dates),
                "meetings_with_transcripts": len(all_verbatim_results),
                "total_transcripts": total_transcripts
            },
            "metadata": {
                "processed_at": datetime.now().isoformat(),
                "hierarchical_approach": True,
                "deterministic_parsing": True
            }
        }
        
        # Save comprehensive verbatim result
        self._save_comprehensive_verbatim_result(comprehensive_result)
        
        log.info(f"✅ Hierarchical verbatim processing complete: {total_transcripts} total transcripts")
        return comprehensive_result
    
    def _save_comprehensive_verbatim_result(self, result: Dict[str, Any]) -> None:
        """Save the comprehensive verbatim processing result."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"comprehensive_verbatim_transcripts_{timestamp}.json"
        verbatim_dir = self.output_dir / "verbatim"
        verbatim_dir.mkdir(parents=True, exist_ok=True)
        output_path = verbatim_dir / filename
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        log.info(f"💾 Saved comprehensive verbatim result: {output_path}")

    async def _process_legal_documents_hierarchically(self, base_dir: Path, extracted_documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process legal documents (ordinances and resolutions) using the enhanced hierarchical approach.
        
        This method identifies meeting dates from processed agenda documents and
        processes corresponding legal documents to create explicit hierarchical
        relationships between meetings, agenda items, and their implementing documents.
        """
        log.info("📜 Starting enhanced legal document processing...")
        
        # Extract meeting dates from processed agenda documents
        meeting_dates = set()
        for doc in extracted_documents:
            # Check if this is an agenda document by filename or document type
            source_file = doc.get('source_file', '')
            doc_type = doc.get('document_type', '')
            meeting_date = doc.get('meeting_date')
            
            if meeting_date and ('agenda' in source_file.lower() or doc_type == 'agenda'):
                meeting_dates.add(meeting_date)
        
        if not meeting_dates:
            log.warning("No meeting dates found in extracted agenda documents for legal document processing")
            return self._empty_legal_result()
        
        log.info(f"📅 Processing legal documents for {len(meeting_dates)} meetings: {sorted(meeting_dates)}")
        
        # Process legal documents for each meeting date
        all_legal_results = []
        for meeting_date in sorted(meeting_dates):
            log.info(f"🏛️ Processing legal documents for meeting: {meeting_date}")
            
            try:
                result = await self.enhanced_document_linker.process_legal_documents(base_dir, meeting_date)
                all_legal_results.append(result)
                
                # Log summary for this meeting
                summary = result['summary']
                log.info(f"✅ Meeting {meeting_date}: {summary['total_documents']} legal documents processed")
                
            except Exception as e:
                log.error(f"❌ Failed to process legal documents for {meeting_date}: {e}")
                continue
        
        # Build comprehensive legal document result
        comprehensive_result = self._build_comprehensive_legal_result(all_legal_results)
        
        # Save comprehensive legal document collection
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"comprehensive_legal_documents_{timestamp}.json"
        legal_dir = self.output_dir / "legal"
        legal_dir.mkdir(parents=True, exist_ok=True)
        output_path = legal_dir / filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(comprehensive_result, f, indent=2, ensure_ascii=False)
        
        log.info(f"💾 Saved comprehensive legal document result: {output_path}")
        
        return comprehensive_result
    
    def _empty_legal_result(self) -> Dict[str, Any]:
        """Return empty legal document result structure."""
        return {
            "document_type": "comprehensive_legal_document_collection",
            "meetings": [],
            "all_documents": [],
            "all_relationships": [],
            "summary": {
                "total_meetings": 0,
                "total_documents": 0,
                "by_type": {},
                "linked_to_agenda": 0
            },
            "metadata": {
                "extraction_method": "enhanced_hierarchical_legal_extraction",
                "processing_timestamp": datetime.now().isoformat()
            }
        }
    
    def _build_comprehensive_legal_result(self, all_legal_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build comprehensive result from all legal document processing results."""
        all_documents = []
        all_relationships = []
        meeting_summaries = []
        
        # Aggregate results from all meetings
        for result in all_legal_results:
            meeting_summaries.append({
                "meeting_date": result["meeting_date"],
                "summary": result["summary"]
            })
            all_documents.extend(result["documents"])
            all_relationships.extend(result["hierarchical_relationships"])
        
        # Build comprehensive summary
        total_documents = len(all_documents)
        by_type = {}
        linked_to_agenda = 0
        
        for doc in all_documents:
            doc_type = doc.get('document_type', 'unknown')
            by_type[doc_type] = by_type.get(doc_type, 0) + 1
            
            if doc.get('agenda_item_code'):
                linked_to_agenda += 1
        
        return {
            "document_type": "comprehensive_legal_document_collection",
            "meetings": meeting_summaries,
            "all_documents": all_documents,
            "all_relationships": all_relationships,
            "summary": {
                "total_meetings": len(all_legal_results),
                "total_documents": total_documents,
                "by_type": by_type,
                "linked_to_agenda": linked_to_agenda,
                "unlinked_documents": total_documents - linked_to_agenda
            },
            "metadata": {
                "extraction_method": "enhanced_hierarchical_legal_extraction",
                "processing_timestamp": datetime.now().isoformat(),
                "meetings_processed": [r["meeting_date"] for r in all_legal_results]
            }
        }


async def run_extraction_pipeline(base_dir: Path, output_dir: Path) -> None:
    """
    Main entry point for running the extraction pipeline.
    
    Args:
        base_dir: Source directory containing PDFs
        output_dir: Output directory for JSON files
    """
    integration = ExtractionPipelineIntegration(output_dir)
    await integration.run_extraction_pipeline(base_dir) 