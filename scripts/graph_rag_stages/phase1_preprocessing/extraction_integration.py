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

# Import the extraction pipeline stages (now within graph_rag_stages)
from .stage1_pdf_ocr import PDFOCRExtractor
from .stage2_agenda_extraction import AgendaItemExtractor
from .stage3_ontology_enhancement import OntologyEnhancer

log = logging.getLogger(__name__)


class ExtractionPipelineIntegration:
    """Orchestrates the complete 3-stage extraction pipeline."""
    
    def __init__(self, output_dir: Path = Path("extracted_json")):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize the three stages
        self.stage1 = PDFOCRExtractor(output_dir)
        self.stage2 = AgendaItemExtractor(output_dir)
        self.stage3 = OntologyEnhancer(output_dir)
        
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
        
        log.info(f"✅ Extraction pipeline completed: {len(extracted_documents)} documents processed")
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


async def run_extraction_pipeline(base_dir: Path, output_dir: Path) -> None:
    """
    Main entry point for running the extraction pipeline.
    
    Args:
        base_dir: Source directory containing PDFs
        output_dir: Output directory for JSON files
    """
    integration = ExtractionPipelineIntegration(output_dir)
    await integration.run_extraction_pipeline(base_dir) 