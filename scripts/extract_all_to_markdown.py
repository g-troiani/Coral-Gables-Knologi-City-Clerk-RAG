#!/usr/bin/env python3
"""
Extract all PDFs to markdown format for GraphRAG processing.
"""

import asyncio
from pathlib import Path
import logging
from datetime import datetime

from graph_stages.pdf_extractor import PDFExtractor
from graph_stages.agenda_pdf_extractor import AgendaPDFExtractor
from graph_stages.enhanced_document_linker import EnhancedDocumentLinker
from graph_stages.verbatim_transcript_linker import VerbatimTranscriptLinker

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

async def extract_all_documents():
    """Extract all city clerk documents to markdown format."""
    
    base_dir = Path("city_clerk_documents/global/City Comissions 2024")
    markdown_dir = Path("city_clerk_documents/extracted_markdown")
    markdown_dir.mkdir(exist_ok=True)
    
    if not base_dir.exists():
        log.error(f"❌ Base directory not found: {base_dir}")
        log.error(f"   Current working directory: {Path.cwd()}")
        return
    
    log.info(f"📁 Base directory found: {base_dir}")
    
    stats = {
        'agendas': 0,
        'ordinances': 0,
        'resolutions': 0,
        'transcripts': 0,
        'errors': 0
    }
    
    log.info("📋 Extracting Agendas...")
    agenda_dir = base_dir / "Agendas"
    if agenda_dir.exists():
        log.info(f"   Found agenda directory: {agenda_dir}")
        extractor = AgendaPDFExtractor()
        for pdf in agenda_dir.glob("*.pdf"):
            log.info(f"   Processing: {pdf.name}")
            try:
                agenda_data = extractor.extract_agenda(pdf)
                output_path = Path("city_clerk_documents/extracted_text") / f"{pdf.stem}_extracted.json"
                extractor.save_extracted_agenda(agenda_data, output_path)
                stats['agendas'] += 1
            except Exception as e:
                log.error(f"Failed to extract {pdf}: {e}")
                stats['errors'] += 1
    else:
        log.warning(f"⚠️  Agenda directory not found: {agenda_dir}")
    
    log.info("📜 Extracting Ordinances...")
    ord_dir = base_dir / "Ordinances"
    
    if ord_dir.exists():
        log.info(f"   Found ordinances directory: {ord_dir}")
        linker = EnhancedDocumentLinker()
        
        all_ord_pdfs = list(ord_dir.rglob("*.pdf"))
        log.info(f"   Found {len(all_ord_pdfs)} ordinance PDFs")
        
        for pdf_path in all_ord_pdfs:
            log.info(f"   Processing: {pdf_path.name}")
            try:
                meeting_date = extract_meeting_date_from_filename(pdf_path.name)
                if meeting_date:
                    doc_info = await linker._process_document(pdf_path, meeting_date, "ordinance")
                    if doc_info:
                        linker._save_extracted_text(pdf_path, doc_info, "ordinance")
                        stats['ordinances'] += 1
                else:
                    log.warning(f"   Could not extract date from: {pdf_path.name}")
            except Exception as e:
                log.error(f"   Failed to process {pdf_path.name}: {e}")
                stats['errors'] += 1
    else:
        log.warning(f"⚠️  Ordinances directory not found: {ord_dir}")
    
    log.info("📜 Extracting Resolutions...")
    res_dir = base_dir / "Resolutions"
    
    if res_dir.exists():
        log.info(f"   Found resolutions directory: {res_dir}")
        linker = EnhancedDocumentLinker()
        
        all_res_pdfs = list(res_dir.rglob("*.pdf"))
        log.info(f"   Found {len(all_res_pdfs)} resolution PDFs")
        
        for pdf_path in all_res_pdfs:
            log.info(f"   Processing: {pdf_path.name}")
            try:
                meeting_date = extract_meeting_date_from_filename(pdf_path.name)
                if meeting_date:
                    doc_info = await linker._process_document(pdf_path, meeting_date, "resolution")
                    if doc_info:
                        linker._save_extracted_text(pdf_path, doc_info, "resolution")
                        stats['resolutions'] += 1
                else:
                    log.warning(f"   Could not extract date from: {pdf_path.name}")
            except Exception as e:
                log.error(f"   Failed to process {pdf_path.name}: {e}")
                stats['errors'] += 1
    else:
        log.warning(f"⚠️  Resolutions directory not found: {res_dir}")
    
    log.info("🎤 Extracting Verbatim Transcripts...")
    verbatim_dirs = [
        base_dir / "Verbatim Items",
        base_dir / "Verbating Items"
    ]
    
    verbatim_dir = None
    for vdir in verbatim_dirs:
        if vdir.exists():
            verbatim_dir = vdir
            break
    
    if verbatim_dir:
        log.info(f"   Found verbatim directory: {verbatim_dir}")
        transcript_linker = VerbatimTranscriptLinker()
        
        all_verb_pdfs = list(verbatim_dir.rglob("*.pdf"))
        log.info(f"   Found {len(all_verb_pdfs)} verbatim PDFs")
        
        for pdf_path in all_verb_pdfs:
            log.info(f"   Processing: {pdf_path.name}")
            try:
                meeting_date = extract_meeting_date_from_verbatim(pdf_path.name)
                if meeting_date:
                    transcript_info = await transcript_linker._process_transcript(pdf_path, meeting_date)
                    if transcript_info:
                        transcript_linker._save_extracted_text(pdf_path, transcript_info)
                        stats['transcripts'] += 1
                else:
                    log.warning(f"   Could not extract date from: {pdf_path.name}")
            except Exception as e:
                log.error(f"   Failed to process {pdf_path.name}: {e}")
                stats['errors'] += 1
    else:
        log.warning(f"⚠️  Verbatim directory not found. Tried: {verbatim_dirs}")
    
    log.info("\n📊 Extraction Summary:")
    log.info(f"   Agendas: {stats['agendas']}")
    log.info(f"   Ordinances: {stats['ordinances']}")
    log.info(f"   Resolutions: {stats['resolutions']}")
    log.info(f"   Transcripts: {stats['transcripts']}")
    log.info(f"   Errors: {stats['errors']}")
    log.info(f"   Total: {sum(stats.values()) - stats['errors']}")
    
    log.info(f"\n✅ All documents extracted to:")
    log.info(f"   JSON: city_clerk_documents/extracted_text/")
    log.info(f"   Markdown: {markdown_dir}")

def extract_meeting_date_from_filename(filename: str) -> str:
    """Extract meeting date from ordinance/resolution filename."""
    import re
    
    match = re.search(r'(\d{2})_(\d{2})_(\d{4})', filename)
    if match:
        month, day, year = match.groups()
        return f"{month}.{day}.{year}"
    
    return None

def extract_meeting_date_from_verbatim(filename: str) -> str:
    """Extract meeting date from verbatim transcript filename."""
    import re
    
    match = re.match(r'(\d{2})_(\d{2})_(\d{4})', filename)
    if match:
        month, day, year = match.groups()
        return f"{month}.{day}.{year}"
    
    return None

if __name__ == "__main__":
    asyncio.run(extract_all_documents()) 