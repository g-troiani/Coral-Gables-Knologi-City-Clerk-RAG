"""
Extract text from PDFs using Docling for accurate OCR and text extraction.
"""
from pathlib import Path
from typing import Tuple, List, Dict, Optional
import logging
import json
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions

log = logging.getLogger(__name__)

class PDFExtractor:
    """Extract text from PDFs using Docling for accurate OCR and text extraction."""
    
    def __init__(self, output_dir: Optional[Path] = None):
        """Initializes the PDF extractor with Docling."""
        if output_dir:
            self.output_dir = output_dir
        else:
            self.output_dir = Path.cwd() / "temp_extraction_output"
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.debug_dir = self.output_dir / "debug"
        self.debug_dir.mkdir(exist_ok=True)
        
        pipeline_options = PdfPipelineOptions(do_ocr=True, do_table_structure=True)
        self.converter = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
        )

    def extract_text_from_pdf(self, pdf_path: Path) -> Tuple[str, List[Dict[str, any]]]:
        """
        Extracts text and page data from a PDF using Docling.
        """
        log.info(f"Extracting text from: {pdf_path.name}")
        
        try:
            result = self.converter.convert(str(pdf_path))
            doc = result.document
            full_text = doc.export_to_markdown() or ""

            pages = []
            if hasattr(doc, 'pages') and doc.pages:
                for page_num, page in enumerate(doc.pages, 1):
                    # Get page text using correct methods
                    page_text = ""
                    if hasattr(page, 'text'):
                        page_text = page.text
                    elif hasattr(page, 'get_text'):
                        page_text = page.get_text()
                    else:
                        # Try to extract from elements
                        page_elements = []
                        if hasattr(page, 'elements'):
                            for element in page.elements:
                                if hasattr(element, 'text'):
                                    page_elements.append(element.text)
                        page_text = "\n".join(page_elements)
                    
                    if page_text:
                        pages.append({'text': page_text, 'page_num': page_num})
            
            if not pages and full_text:
                pages = [{'text': full_text, 'page_num': 1}]

            self._save_debug_info(pdf_path, len(pages), len(full_text))

            log.info(f"✅ Successfully extracted {len(pages)} pages from {pdf_path.name}")
            return full_text, pages

        except Exception as e:
            log.error(f"❌ Failed to extract text from {pdf_path.name}: {e}")
            return "", []

    def _save_debug_info(self, pdf_path: Path, num_pages: int, num_chars: int):
        """Saves a debug file with metadata about the extraction process."""
        debug_info = {
            'file': pdf_path.name,
            'total_pages': num_pages,
            'total_characters': num_chars,
        }
        debug_file = self.debug_dir / f"{pdf_path.stem}_extraction_debug.json"
        with open(debug_file, 'w', encoding='utf-8') as f:
            json.dump(debug_info, f, indent=2) 