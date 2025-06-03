# scripts/graph_stages/pdf_extractor.py

import os
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import logging
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat, DocumentStream
from docling.datamodel.pipeline_options import PdfPipelineOptions
import json
from io import BytesIO

# Set up logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

class PDFExtractor:
    """Extract text from PDFs using Docling for accurate OCR and text extraction."""
    
    def __init__(self, pdf_dir: Path, output_dir: Optional[Path] = None):
        """Initialize the PDF extractor with Docling."""
        self.pdf_dir = Path(pdf_dir)
        self.output_dir = output_dir or Path("city_clerk_documents/extracted_text")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create debug directory
        self.debug_dir = self.output_dir / "debug"
        self.debug_dir.mkdir(exist_ok=True)
        
        # Initialize Docling converter with OCR enabled
        # Configure for better OCR and structure detection
        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = True  # Enable OCR for better text extraction
        pipeline_options.do_table_structure = True  # Better table extraction
        
        self.converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
            }
        )
    
    def extract_text_from_pdf(self, pdf_path: Path) -> Tuple[str, List[Dict[str, str]]]:
        """
        Extract text from PDF using Docling with OCR support.
        
        Returns:
            Tuple of (full_text, pages) where pages is a list of dicts with 'text' and 'page_num'
        """
        log.info(f"📄 Extracting text from: {pdf_path.name}")
        
        # Convert PDF with Docling - just pass the path directly
        result = self.converter.convert(str(pdf_path))
        
        # Get the document
        doc = result.document
        
        # Get the markdown representation which preserves structure better
        full_markdown = doc.export_to_markdown()
        
        # Extract pages if available
        pages = []
        
        # Try to extract page-level content from the document structure
        if hasattr(doc, 'pages') and doc.pages:
            for page_num, page in enumerate(doc.pages, 1):
                # Get page text
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
                    pages.append({
                        'text': page_text,
                        'page_num': page_num
                    })
        
        # If we couldn't extract pages, create one page with all content
        if not pages and full_markdown:
            pages = [{
                'text': full_markdown,
                'page_num': 1
            }]
        
        # Use markdown as the full text (better structure preservation)
        full_text = full_markdown if full_markdown else ""
        
        # Save debug information
        debug_info = {
            'file': pdf_path.name,
            'total_pages': len(pages),
            'total_characters': len(full_text),
            'extraction_method': 'docling',
            'has_markdown': bool(full_markdown),
            'doc_attributes': list(dir(doc)) if doc else []
        }
        
        debug_file = self.debug_dir / f"{pdf_path.stem}_extraction_debug.json"
        with open(debug_file, 'w', encoding='utf-8') as f:
            json.dump(debug_info, f, indent=2)
        
        # Save the full extracted text for inspection
        text_file = self.debug_dir / f"{pdf_path.stem}_full_text.txt"
        with open(text_file, 'w', encoding='utf-8') as f:
            f.write(full_text)
        
        # Save markdown version separately for debugging
        if full_markdown:
            markdown_file = self.debug_dir / f"{pdf_path.stem}_markdown.md"
            with open(markdown_file, 'w', encoding='utf-8') as f:
                f.write(full_markdown)
        
        log.info(f"✅ Extracted {len(pages)} pages, {len(full_text)} characters")
        
        return full_text, pages
    
    def extract_all_pdfs(self) -> Dict[str, Dict[str, any]]:
        """Extract text from all PDFs in the directory."""
        pdf_files = list(self.pdf_dir.glob("*.pdf"))
        log.info(f"📚 Found {len(pdf_files)} PDF files to process")
        
        extracted_data = {}
        
        for pdf_path in pdf_files:
            try:
                full_text, pages = self.extract_text_from_pdf(pdf_path)
                
                extracted_data[pdf_path.stem] = {
                    'full_text': full_text,
                    'pages': pages,
                    'metadata': {
                        'filename': pdf_path.name,
                        'num_pages': len(pages),
                        'total_chars': len(full_text),
                        'extraction_method': 'docling'
                    }
                }
                
                # Save extracted text
                output_file = self.output_dir / f"{pdf_path.stem}_extracted.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(extracted_data[pdf_path.stem], f, indent=2, ensure_ascii=False)
                
                log.info(f"✅ Saved extracted text to: {output_file}")
                
            except Exception as e:
                log.error(f"❌ Failed to process {pdf_path.name}: {e}")
                import traceback
                traceback.print_exc()
                # No fallback - if Docling fails, we fail
                raise
        
        return extracted_data

# Add this to requirements.txt:
# unstructured[pdf]==0.10.30
# pytesseract>=0.3.10
# pdf2image>=1.16.3
# poppler-utils (system dependency - install with: brew install poppler) 