#!/usr/bin/env python3
"""
Integration module for orchestrating the 3-stage extraction pipeline
within the graph_rag_stages framework.

This module coordinates:
- Stage 1: PDF OCR with Docling + PyMuPDF hyperlinks
- Stage 2: LLM agenda extraction with regex fallbacks
- Stage 3: Ontology enhancement with entity extraction
"""

import logging
import asyncio
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

# Import the extraction pipeline stages (now within graph_rag_stages)
from .stage1_pdf_ocr import PDFOCRExtractor
from .stage2_agenda_extraction import AgendaItemExtractor
from .stage3_ontology_enhancement import OntologyEnhancer
from .verbatim_transcript_processor import VerbatimTranscriptProcessor
from .enhanced_document_linker import EnhancedDocumentLinker

log = logging.getLogger(__name__)


class ExtractionPipelineIntegration:
    """Orchestrates the complete 3-stage extraction pipeline."""
    
    def __init__(self, output_dir: Path = Path("extracted_json")):
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
        Run the complete 3-stage extraction pipeline on all PDFs.
        
        Args:
            base_dir: Base directory containing PDF subdirectories
            
        Returns:
            List of all extracted documents
        """
        log.info(f"🚀 Starting integrated extraction pipeline from: {base_dir}")
        
        # Discover PDF files by category
        pdf_files = self._discover_pdf_files(base_dir)
        
        extracted_documents = []
        
        # Process each PDF through appropriate stages
        for pdf_type, pdf_list in pdf_files.items():
            for pdf_path in pdf_list:
                try:
                    log.info(f"📄 Processing {pdf_type}: {pdf_path.name}")
                    
                    # Stage 1: PDF OCR (all documents)
                    ocr_result = self.stage1.extract_pdf(pdf_path)
                    
                    if pdf_type == 'agenda':
                        # Full 3-stage processing for agenda documents
                        agenda_result = self.stage2.extract_agenda_structure(ocr_result)
                        ontology_result = self.stage3.enhance_agenda_ontology(agenda_result)
                        extracted_documents.append(ontology_result)
                    else:
                        # Basic processing for supporting documents
                        enhanced_result = self._enhance_non_agenda_document(ocr_result, pdf_type)
                        extracted_documents.append(enhanced_result)
                        
                except Exception as e:
                    log.error(f"❌ Failed to process {pdf_path.name}: {e}")
                    continue
        
        # Process verbatim transcripts using hierarchical approach
        verbatim_results = await self._process_verbatim_transcripts_hierarchically(base_dir, extracted_documents)
        
        # Process legal documents using enhanced hierarchical approach
        legal_results = await self._process_legal_documents_hierarchically(base_dir, extracted_documents)
        
        log.info(f"✅ Extraction pipeline completed: {len(extracted_documents)} documents processed")
        log.info(f"📝 Hierarchical transcript processing: {verbatim_results['summary']['total_transcripts']} transcripts")
        log.info(f"📜 Enhanced legal document processing: {legal_results['summary']['total_documents']} legal documents")
        
        return extracted_documents
    
    def _discover_pdf_files(self, base_dir: Path) -> Dict[str, List[Path]]:
        """Discover and categorize PDF files by document type."""
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
    
    def _enhance_non_agenda_document(self, ocr_result: Dict[str, Any], doc_type: str) -> Dict[str, Any]:
        """Enhance non-agenda documents with basic metadata."""
        enhanced_result = ocr_result.copy()
        
        # Add document type and extracted metadata
        enhanced_result['document_type'] = doc_type
        enhanced_result['meeting_date'] = self._extract_meeting_date(ocr_result['source_file'])
        enhanced_result['document_number'] = self._extract_document_number(ocr_result['source_file'])
        
        # Add processing metadata
        enhanced_result['metadata']['processing_stage'] = 'stage1_enhanced'
        enhanced_result['metadata']['document_category'] = doc_type
        
        return enhanced_result
    
    def _extract_meeting_date(self, filename: str) -> str:
        """Extract meeting date from filename using common patterns."""
        import re
        
        # Try different date patterns
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
        
        return "unknown"
    
    def _extract_document_number(self, filename: str) -> Optional[str]:
        """Extract document number from filename."""
        import re
        
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
        output_path = self.output_dir / filename
        
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
        output_path = self.output_dir / f"comprehensive_legal_documents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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