#!/usr/bin/env python3
"""
Extraction Pipeline Integration - Orchestrates the 3-stage extraction process
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

from .stage1_pdf_ocr import PDFOCRExtractor
from .stage2_agenda_extraction import AgendaItemExtractor
from .stage3_ontology_enhancement import OntologyEnhancer
from .enhanced_document_linker import EnhancedDocumentLinker
from .verbatim_transcript_processor import VerbatimTranscriptProcessor

log = logging.getLogger(__name__)


class ExtractionPipelineIntegration:
    """Coordinates the full extraction pipeline across all document types."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize all processors
        self.pdf_extractor = PDFOCRExtractor(output_dir)
        self.agenda_extractor = AgendaItemExtractor(output_dir)
        self.ontology_enhancer = OntologyEnhancer(output_dir)
        self.document_linker = EnhancedDocumentLinker(output_dir)
        self.transcript_processor = VerbatimTranscriptProcessor(output_dir)
    
    async def run_extraction_pipeline(self, base_dir: Path) -> List[Dict[str, Any]]:
        """
        Run the complete extraction pipeline on all documents.
        
        NEW BEHAVIOR:
        - Agendas: Stage1 (no save) → Stage2 (in-memory) → Stage3 (save to agenda/)
        - Legal docs: Save to legal/
        - Transcripts: Save to verbatim/
        """
        log.info(f"🚀 Starting integrated extraction pipeline")
        log.info(f"📁 Source: {base_dir}")
        log.info(f"📁 Output: {self.output_dir}")
        
        extracted_documents = []
        
        # Extract meeting dates from filenames
        meeting_dates = self._discover_meeting_dates(base_dir)
        log.info(f"📅 Found {len(meeting_dates)} meeting dates to process")
        
        for meeting_date in sorted(meeting_dates):
            log.info(f"📅 Processing meeting: {meeting_date}")
            
            # Process agenda files
            agenda_files = self._find_agenda_files(base_dir, meeting_date)
            for agenda_file in agenda_files:
                result = await self._process_agenda_file(agenda_file, meeting_date)
                if result:
                    extracted_documents.append(result)
            
            # Process legal documents (ordinances and resolutions)
            try:
                legal_result = await self.document_linker.process_legal_documents(base_dir, meeting_date)
                if legal_result and legal_result.get("documents"):
                    extracted_documents.extend(legal_result["documents"])
                    log.info(f"✅ Processed {len(legal_result['documents'])} legal documents")
            except Exception as e:
                log.error(f"❌ Failed to process legal documents: {e}")
            
            # Process verbatim transcripts
            try:
                transcript_result = await self.transcript_processor.process_verbatim_transcripts(base_dir, meeting_date)
                if transcript_result and transcript_result.get("transcripts"):
                    extracted_documents.extend(transcript_result["transcripts"])
                    log.info(f"✅ Processed {len(transcript_result['transcripts'])} transcripts")
            except Exception as e:
                log.error(f"❌ Failed to process transcripts: {e}")
        
        log.info(f"✅ Extraction complete: {len(extracted_documents)} documents processed")
        return extracted_documents
    
    async def _process_agenda_file(self, agenda_file: Path, meeting_date: str) -> Optional[Dict[str, Any]]:
        """
        Process a single agenda file through all 3 stages.
        
        UPDATED BEHAVIOR:
        - Stage 1: OCR extraction WITHOUT saving (in-memory only for agendas)
        - Stage 2: Agenda extraction (in-memory only)
        - Stage 3: Ontology enhancement (saves to agenda/ directory)
        """
        try:
            log.info(f"📋 Processing agenda: {agenda_file.name}")
            
            # Stage 1: OCR extraction - DON'T SAVE for agendas
            ocr_result = self.pdf_extractor.extract_pdf(agenda_file, save_to_file=False)
            
            if not ocr_result or not ocr_result.get("full_text"):
                log.warning(f"No text extracted from {agenda_file.name}")
                return None
            
            # Stage 2: Agenda extraction (now in-memory only, no file save)
            agenda_data = self.agenda_extractor.extract_agenda_structure(ocr_result)
            
            if not agenda_data or not agenda_data.get("agenda_items"):
                log.warning(f"No agenda items extracted from {agenda_file.name}")
                return None
            
            # Stage 3: Ontology enhancement (saves to agenda/ directory)
            enhanced_data = self.ontology_enhancer.enhance_agenda_ontology(agenda_data)
            
            log.info(f"✅ Completed processing agenda: {agenda_file.name}")
            log.info(f"   - Entities: {len(enhanced_data.get('entities', []))}")
            log.info(f"   - Items: {len(enhanced_data.get('agenda_items', []))}")
            log.info(f"   - Saved to: agenda/agenda_{meeting_date.replace('.', '_')}.json")
            
            return enhanced_data
            
        except Exception as e:
            log.error(f"❌ Failed to process agenda {agenda_file}: {e}")
            import traceback
            log.error(traceback.format_exc())
            return None
    
    def _discover_meeting_dates(self, base_dir: Path) -> List[str]:
        """Discover all meeting dates from PDF filenames."""
        import re
        meeting_dates = set()
        
        for pdf_file in base_dir.rglob("*.pdf"):
            # Multiple date patterns
            patterns = [
                r'(\d{2})\.(\d{1,2})\.(\d{4})',  # MM.DD.YYYY
                r'(\d{2})_(\d{1,2})_(\d{4})',     # MM_DD_YYYY
                r'(\d{2})-(\d{1,2})-(\d{4})'      # MM-DD-YYYY
            ]
            
            for pattern in patterns:
                match = re.search(pattern, pdf_file.name)
                if match:
                    month, day, year = match.groups()
                    meeting_date = f"{month.zfill(2)}.{day.zfill(2)}.{year}"
                    meeting_dates.add(meeting_date)
                    break
        
        return sorted(list(meeting_dates))
    
    def _find_agenda_files(self, base_dir: Path, meeting_date: str) -> List[Path]:
        """Find agenda files for a specific meeting date."""
        agenda_files = []
        
        # Convert date format for matching
        date_variations = [
            meeting_date,                           # MM.DD.YYYY
            meeting_date.replace('.', '_'),         # MM_DD_YYYY
            meeting_date.replace('.', '-'),         # MM-DD-YYYY
            meeting_date.replace('.', ' ')          # MM DD YYYY
        ]
        
        patterns = []
        for date_var in date_variations:
            patterns.extend([
                f"Agenda {date_var}*.pdf",
                f"Agenda_{date_var}*.pdf",
                f"{date_var}*Agenda*.pdf",
                f"*{date_var}*agenda*.pdf"
            ])
        
        for pattern in patterns:
            for match in base_dir.rglob(pattern):
                if match not in agenda_files:
                    agenda_files.append(match)
        
        return agenda_files
