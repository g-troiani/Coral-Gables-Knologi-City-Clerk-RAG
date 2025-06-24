"""
Integration module for using the sophisticated extraction_pipeline stages
within the graph_rag_stages framework.
"""

import logging
import asyncio
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys

# Add the parent directory to the path to import extraction_pipeline
sys.path.append(str(Path(__file__).parent.parent.parent))

try:
    from extraction_pipeline.stage1_pdf_ocr import PDFOCRExtractor
    from extraction_pipeline.stage2_agenda_extraction import AgendaItemExtractor
    from extraction_pipeline.stage3_ontology_enhancement import OntologyEnhancer
except ImportError as e:
    logging.error(f"Failed to import extraction_pipeline modules: {e}")
    logging.error("Make sure the extraction_pipeline directory is accessible")
    raise

log = logging.getLogger(__name__)


class ExtractionPipelineIntegration:
    """Integrates the 3-stage extraction pipeline into graph_rag_stages."""
    
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
        
        # Discover PDF files
        pdf_files = self._discover_pdf_files(base_dir)
        
        extracted_documents = []
        
        # Process each PDF through all 3 stages
        for pdf_type, pdf_list in pdf_files.items():
            for pdf_path in pdf_list:
                try:
                    log.info(f"📄 Processing {pdf_type}: {pdf_path.name}")
                    
                    # Stage 1: PDF OCR
                    ocr_result = self.stage1.extract_pdf(pdf_path)
                    
                    if pdf_type == 'agenda':
                        # Stage 2: Agenda extraction
                        agenda_result = self.stage2.extract_agenda_structure(ocr_result)
                        
                        # Stage 3: Ontology enhancement
                        ontology_result = self.stage3.enhance_agenda_ontology(agenda_result)
                        
                        extracted_documents.append(ontology_result)
                    else:
                        # For non-agenda documents, just add basic metadata
                        doc_info = {
                            **ocr_result,
                            'document_type': pdf_type,
                            'meeting_date': self._extract_meeting_date(pdf_path.name),
                            'document_number': self._extract_document_number(pdf_path.name)
                        }
                        
                        # Save the doc_info as JSON
                        output_file = self.output_dir / f"{pdf_path.stem}_stage1_{pdf_type}.json"
                        import json
                        with open(output_file, 'w', encoding='utf-8') as f:
                            json.dump(doc_info, f, indent=2, ensure_ascii=False)
                        
                        extracted_documents.append(doc_info)
                        
                except Exception as e:
                    log.error(f"❌ Failed to process {pdf_path.name}: {e}")
                    continue
        
        log.info(f"✅ Extraction pipeline completed: {len(extracted_documents)} documents processed")
        return extracted_documents
    
    def _discover_pdf_files(self, base_dir: Path) -> Dict[str, List[Path]]:
        """Discover and categorize PDF files by type."""
        categorized_files = {
            'agenda': [],
            'ordinance': [],
            'resolution': [],
            'transcript': []
        }
        
        # Check for subdirectories
        agenda_dir = base_dir / "Agendas"
        if agenda_dir.exists():
            categorized_files['agenda'] = list(agenda_dir.glob("*.pdf"))
        
        ord_dir = base_dir / "Ordinances"
        if ord_dir.exists():
            categorized_files['ordinance'] = list(ord_dir.rglob("*.pdf"))
        
        res_dir = base_dir / "Resolutions"
        if res_dir.exists():
            categorized_files['resolution'] = list(res_dir.rglob("*.pdf"))
        
        # Check both possible verbatim directory names
        for vdir_name in ["Verbatim Items", "Verbating Items"]:
            vdir = base_dir / vdir_name
            if vdir.exists():
                categorized_files['transcript'] = list(vdir.rglob("*.pdf"))
                break
        
        log.info(f"📊 Discovered: {len(categorized_files['agenda'])} agendas, "
                f"{len(categorized_files['ordinance'])} ordinances, "
                f"{len(categorized_files['resolution'])} resolutions, "
                f"{len(categorized_files['transcript'])} transcripts")
        
        return categorized_files
    
    def _extract_meeting_date(self, filename: str) -> str:
        """Extract meeting date from filename."""
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