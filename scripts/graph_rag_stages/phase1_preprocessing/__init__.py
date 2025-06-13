"""
Initializes the preprocessing module and provides the main orchestration
function to run the entire data extraction and enrichment process.
"""
import asyncio
import logging
from pathlib import Path
from typing import List, Coroutine

from .agenda_extractor import AgendaExtractor
from .document_linker import DocumentLinker
from .transcript_linker import TranscriptLinker

log = logging.getLogger(__name__)

async def run_extraction_pipeline(base_dir: Path, markdown_output_dir: Path):
    """
    High-level function to run the entire data extraction process.
    """
    log.info(f"Starting extraction pipeline for source directory: {base_dir}")
    markdown_output_dir.mkdir(parents=True, exist_ok=True)

    agenda_extractor = AgendaExtractor(markdown_output_dir)
    doc_linker = DocumentLinker(markdown_output_dir)
    transcript_linker = TranscriptLinker(markdown_output_dir)

    agenda_pdfs = list((base_dir / "Agendas").glob("*.pdf"))
    ordinance_pdfs = list((base_dir / "Ordinances").rglob("*.pdf"))
    resolution_pdfs = list((base_dir / "Resolutions").rglob("*.pdf"))
    
    verbatim_dir_path = base_dir / "Verbatim Items"
    if not verbatim_dir_path.exists():
        verbatim_dir_path = base_dir / "Verbating Items"
    transcript_pdfs = list(verbatim_dir_path.rglob("*.pdf")) if verbatim_dir_path.exists() else []

    log.info(f"Discovered {len(agenda_pdfs)} agendas, {len(ordinance_pdfs)} ordinances, "
             f"{len(resolution_pdfs)} resolutions, and {len(transcript_pdfs)} transcripts.")

    tasks: List[Coroutine] = []
    for pdf in agenda_pdfs: tasks.append(agenda_extractor.extract_and_save_agenda(pdf))
    for pdf in ordinance_pdfs: tasks.append(doc_linker.link_and_save_document(pdf, "ordinance"))
    for pdf in resolution_pdfs: tasks.append(doc_linker.link_and_save_document(pdf, "resolution"))
    for pdf in transcript_pdfs: tasks.append(transcript_linker.link_and_save_transcript(pdf))

    log.info(f"Executing {len(tasks)} extraction tasks in parallel...")
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    success_count = sum(1 for r in results if r is not None and not isinstance(r, Exception))
    error_count = len(results) - success_count
    
    log.info(f"Extraction pipeline finished. Successful: {success_count}, Failed: {error_count}.")
    if error_count > 0:
        log.warning("Some files failed to process. Check logs for details.")

__all__ = ["run_extraction_pipeline"] 