"""
Preprocessing module that integrates the sophisticated extraction_pipeline.

Uses the 3-stage extraction process:
- Stage 1: PDF OCR with Docling + PyMuPDF hyperlinks
- Stage 2: LLM agenda extraction with regex fallbacks  
- Stage 3: Ontology enhancement with entity extraction
"""
import asyncio
import logging
from pathlib import Path
from typing import List, Coroutine

from .extraction_integration import ExtractionPipelineIntegration

log = logging.getLogger(__name__)

async def run_extraction_pipeline(base_dir: Path, output_dir: Path):
    """
    High-level function to run the integrated extraction pipeline.
    
    Args:
        base_dir: Source directory containing PDFs
        output_dir: Output directory for JSON files (not markdown)
    """
    log.info(f"Starting integrated extraction pipeline")
    log.info(f"Source directory: {base_dir}")
    log.info(f"Output directory: {output_dir}")
    
    # Verify base directory exists
    if not base_dir.exists():
        log.error(f"Base directory does not exist: {base_dir}")
        return
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Use the integrated extraction pipeline
    integration = ExtractionPipelineIntegration(output_dir)
    extracted_documents = await integration.run_extraction_pipeline(base_dir)
    
    log.info(f"✅ Extraction pipeline finished. Processed {len(extracted_documents)} documents")
    log.info(f"📊 Output saved to: {output_dir}")

__all__ = ["run_extraction_pipeline"] 