#!/usr/bin/env python3
"""
Extract all PDFs to markdown format for GraphRAG processing.
"""

import asyncio
from pathlib import Path
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

from graph_stages.pdf_extractor import PDFExtractor
from graph_stages.agenda_pdf_extractor import AgendaPDFExtractor
from graph_stages.enhanced_document_linker import EnhancedDocumentLinker
from graph_stages.verbatim_transcript_linker import VerbatimTranscriptLinker

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

async def extract_all_documents():
    """Extract all city clerk documents with parallel processing."""
    
    base_dir = Path("city_clerk_documents/global/City Comissions 2024")
    markdown_dir = Path("city_clerk_documents/extracted_markdown")
    markdown_dir.mkdir(exist_ok=True)
    
    if not base_dir.exists():
        log.error(f"❌ Base directory not found: {base_dir}")
        log.error(f"   Current working directory: {Path.cwd()}")
        return
    
    log.info(f"📁 Base directory found: {base_dir}")
    
    # Set max workers based on CPU cores (but limit to avoid overwhelming system)
    max_workers = min(os.cpu_count() or 4, 8)
    
    stats = {
        'agendas': 0,
        'ordinances': 0,
        'resolutions': 0,
        'transcripts': 0,
        'errors': 0
    }
    
    # Process Agendas in parallel
    log.info(f"📋 Extracting Agendas with {max_workers} workers...")
    agenda_dir = base_dir / "Agendas"
    if agenda_dir.exists():
        log.info(f"   Found agenda directory: {agenda_dir}")
        extractor = AgendaPDFExtractor()
        agenda_pdfs = list(agenda_dir.glob("*.pdf"))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_pdf = {
                executor.submit(process_agenda_pdf, extractor, pdf): pdf 
                for pdf in agenda_pdfs
            }
            
            for future in as_completed(future_to_pdf):
                pdf = future_to_pdf[future]
                try:
                    success = future.result()
                    if success:
                        stats['agendas'] += 1
                except Exception as e:
                    log.error(f"Failed to extract {pdf}: {e}")
                    stats['errors'] += 1
    else:
        log.warning(f"⚠️  Agenda directory not found: {agenda_dir}")
    
    # Process Ordinances and Resolutions in parallel
    log.info("📜 Extracting Ordinances and Resolutions in parallel...")
    ord_dir = base_dir / "Ordinances"
    res_dir = base_dir / "Resolutions"
    
    if ord_dir.exists() and res_dir.exists():
        linker = EnhancedDocumentLinker()
        
        # Combine ordinances and resolutions for parallel processing
        all_docs = []
        for pdf in ord_dir.rglob("*.pdf"):
            all_docs.append(('ordinance', pdf))
        for pdf in res_dir.rglob("*.pdf"):
            all_docs.append(('resolution', pdf))
        
        # Process in parallel with asyncio
        async def process_documents_batch(docs_batch):
            tasks = []
            for doc_type, pdf_path in docs_batch:
                meeting_date = extract_meeting_date_from_filename(pdf_path.name)
                if meeting_date:
                    task = process_document_async(linker, pdf_path, meeting_date, doc_type)
                    tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return results
        
        # Process in batches to avoid overwhelming the system
        batch_size = max_workers * 2
        for i in range(0, len(all_docs), batch_size):
            batch = all_docs[i:i + batch_size]
            results = await process_documents_batch(batch)
            
            for result, (doc_type, _) in zip(results, batch):
                if isinstance(result, Exception):
                    stats['errors'] += 1
                    log.error(f"Error: {result}")
                elif result:
                    stats['ordinances' if doc_type == 'ordinance' else 'resolutions'] += 1
    else:
        log.warning(f"⚠️  Ordinances or Resolutions directory not found: {ord_dir}, {res_dir}")
    
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

def process_agenda_pdf(extractor, pdf):
    """Process single agenda PDF (for thread pool)."""
    try:
        log.info(f"   Processing: {pdf.name}")
        agenda_data = extractor.extract_agenda(pdf)
        output_path = Path("city_clerk_documents/extracted_text") / f"{pdf.stem}_extracted.json"
        extractor.save_extracted_agenda(agenda_data, output_path)
        return True
    except Exception as e:
        log.error(f"Failed to extract {pdf}: {e}")
        return False

async def process_document_async(linker, pdf_path, meeting_date, doc_type):
    """Process document asynchronously."""
    try:
        doc_info = await linker._process_document(pdf_path, meeting_date, doc_type)
        if doc_info:
            linker._save_extracted_text(pdf_path, doc_info, doc_type)
            return True
        return False
    except Exception as e:
        raise e

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