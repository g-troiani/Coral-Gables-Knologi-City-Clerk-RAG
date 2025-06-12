#!/usr/bin/env python3
"""
Direct extraction of all PDFs without relying on specific directory structure.
"""

import asyncio
from pathlib import Path
import logging
import re

from graph_stages.pdf_extractor import PDFExtractor
from graph_stages.agenda_pdf_extractor import AgendaPDFExtractor
from graph_stages.enhanced_document_linker import EnhancedDocumentLinker
from graph_stages.verbatim_transcript_linker import VerbatimTranscriptLinker

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

async def extract_all_pdfs():
    """Extract all PDFs found in city clerk documents."""
    
    base_dir = Path("city_clerk_documents/global")
    
    # Find ALL PDFs recursively
    all_pdfs = list(base_dir.rglob("*.pdf"))
    log.info(f"Found {len(all_pdfs)} total PDFs")
    
    # Categorize PDFs by type
    agendas = []
    ordinances = []
    resolutions = []
    verbatims = []
    unknown = []
    
    for pdf in all_pdfs:
        path_str = str(pdf).lower()
        filename = pdf.name.lower()
        
        if 'agenda' in filename and filename.startswith('agenda'):
            agendas.append(pdf)
        elif 'ordinance' in path_str:
            ordinances.append(pdf)
        elif 'resolution' in path_str:
            resolutions.append(pdf)
        elif 'verbat' in path_str or 'transcript' in filename:
            verbatims.append(pdf)
        else:
            unknown.append(pdf)
    
    log.info(f"\nCategorized PDFs:")
    log.info(f"  📋 Agendas: {len(agendas)}")
    log.info(f"  📜 Ordinances: {len(ordinances)}")
    log.info(f"  📜 Resolutions: {len(resolutions)}")
    log.info(f"  🎤 Verbatims: {len(verbatims)}")
    log.info(f"  ❓ Unknown: {len(unknown)}")
    
    # Process each type
    stats = {'success': 0, 'errors': 0}
    
    # 1. Process Agendas
    if agendas:
        log.info("\n📋 Processing Agendas...")
        extractor = AgendaPDFExtractor()
        for pdf in agendas:
            try:
                log.info(f"  Processing: {pdf.name}")
                agenda_data = extractor.extract_agenda(pdf)
                output_path = Path("city_clerk_documents/extracted_text") / f"{pdf.stem}_extracted.json"
                output_path.parent.mkdir(exist_ok=True)
                extractor.save_extracted_agenda(agenda_data, output_path)
                stats['success'] += 1
            except Exception as e:
                log.error(f"  Failed: {e}")
                stats['errors'] += 1
    
    # 2. Process Ordinances
    if ordinances:
        log.info("\n📜 Processing Ordinances...")
        linker = EnhancedDocumentLinker()
        for pdf in ordinances:
            try:
                log.info(f"  Processing: {pdf.name}")
                meeting_date = extract_date_from_path(pdf)
                if meeting_date:
                    doc_info = await linker._process_document(pdf, meeting_date, "ordinance")
                    if doc_info:
                        linker._save_extracted_text(pdf, doc_info, "ordinance")
                        stats['success'] += 1
                else:
                    log.warning(f"  No date found for: {pdf.name}")
            except Exception as e:
                log.error(f"  Failed: {e}")
                stats['errors'] += 1
    
    # 3. Process Resolutions
    if resolutions:
        log.info("\n📜 Processing Resolutions...")
        linker = EnhancedDocumentLinker()
        for pdf in resolutions:
            try:
                log.info(f"  Processing: {pdf.name}")
                meeting_date = extract_date_from_path(pdf)
                if meeting_date:
                    doc_info = await linker._process_document(pdf, meeting_date, "resolution")
                    if doc_info:
                        linker._save_extracted_text(pdf, doc_info, "resolution")
                        stats['success'] += 1
                else:
                    log.warning(f"  No date found for: {pdf.name}")
            except Exception as e:
                log.error(f"  Failed: {e}")
                stats['errors'] += 1
    
    # 4. Process Verbatims
    if verbatims:
        log.info("\n🎤 Processing Verbatim Transcripts...")
        transcript_linker = VerbatimTranscriptLinker()
        for pdf in verbatims:
            try:
                log.info(f"  Processing: {pdf.name}")
                meeting_date = extract_date_from_path(pdf)
                if meeting_date:
                    transcript_info = await transcript_linker._process_transcript(pdf, meeting_date)
                    if transcript_info:
                        transcript_linker._save_extracted_text(pdf, transcript_info)
                        stats['success'] += 1
                else:
                    log.warning(f"  No date found for: {pdf.name}")
            except Exception as e:
                log.error(f"  Failed: {e}")
                stats['errors'] += 1
    
    log.info(f"\n✅ Extraction complete:")
    log.info(f"   Success: {stats['success']}")
    log.info(f"   Errors: {stats['errors']}")

def extract_date_from_path(pdf_path: Path) -> str:
    """Extract date from filename or path."""
    # Try different date patterns
    patterns = [
        r'(\d{2})[._](\d{2})[._](\d{4})',  # MM_DD_YYYY or MM.DD.YYYY
        r'(\d{4})-\d+\s*-\s*(\d{2})_(\d{2})_(\d{4})',  # Ordinance pattern
    ]
    
    for pattern in patterns:
        match = re.search(pattern, pdf_path.name)
        if match:
            groups = match.groups()
            if len(groups) == 3:
                month, day, year = groups
            else:  # ordinance pattern
                year, month, day, year2 = groups
            return f"{month}.{day}.{year}"
    
    # Try parent directory for year
    if '2024' in str(pdf_path):
        # Default to a date if we know it's 2024
        return "01.01.2024"
    
    return None

if __name__ == "__main__":
    asyncio.run(extract_all_pdfs()) 