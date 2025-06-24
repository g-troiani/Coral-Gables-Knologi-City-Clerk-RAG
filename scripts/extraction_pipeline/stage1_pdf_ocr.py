#!/usr/bin/env python3
"""
Stage 1: PDF Extraction & OCR
Transforms raw PDF documents into structured text with hyperlink extraction
"""

import logging
import json
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import fitz  # PyMuPDF
try:
    from docling.document_converter import DocumentConverter
    from docling.datamodel.base_models import InputFormat
    DOCLING_AVAILABLE = True
except ImportError:
    DOCLING_AVAILABLE = False
    print("⚠️  Docling not available, using PyMuPDF fallback only")

log = logging.getLogger(__name__)

class PDFOCRExtractor:
    """Stage 1: Extract text and structure from PDF documents using Docling + PyMuPDF."""
    
    def __init__(self, output_dir: Path = Path("extracted_json")):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
        
        # Configure Docling if available
        if DOCLING_AVAILABLE:
            try:
                self.converter = DocumentConverter()
            except Exception as e:
                log.warning(f"⚠️  Failed to initialize Docling converter: {e}")
                self.converter = None
        else:
            self.converter = None
    
    def extract_pdf(self, pdf_path: Path) -> Dict[str, Any]:
        """
        Extract text, structure, and hyperlinks from PDF.
        
        Returns:
            Complete extraction data with OCR text, pages, and hyperlinks
        """
        log.info(f"📄 Extracting PDF: {pdf_path.name}")
        
        try:
            # Stage 1A: Docling OCR extraction (if available)
            if self.converter:
                docling_result = self._extract_with_docling(pdf_path)
            else:
                docling_result = self._fallback_text_extraction(pdf_path)
            
            # Stage 1B: PyMuPDF hyperlink extraction  
            hyperlinks = self._extract_hyperlinks(pdf_path)
            
            # Stage 1C: Combine results
            extraction_result = {
                "source_file": pdf_path.name,
                "file_path": str(pdf_path),
                "doc_id": self._generate_document_id(pdf_path),
                "full_text": docling_result["full_text"],
                "pages": docling_result["pages"],
                "hyperlinks": hyperlinks,
                "metadata": {
                    "extraction_method": "docling_ocr_pymupdf",
                    "num_pages": len(docling_result["pages"]),
                    "total_chars": len(docling_result["full_text"]),
                    "hyperlink_count": len(hyperlinks),
                    "extraction_timestamp": self._get_timestamp()
                }
            }
            
            # Save extraction result
            output_file = self.output_dir / f"{pdf_path.stem}_stage1_ocr.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extraction_result, f, indent=2, ensure_ascii=False)
            
            log.info(f"✅ Stage 1 complete: {len(docling_result['pages'])} pages, {len(hyperlinks)} hyperlinks")
            return extraction_result
            
        except Exception as e:
            log.error(f"❌ Stage 1 extraction failed for {pdf_path.name}: {e}")
            raise
    
    def _extract_with_docling(self, pdf_path: Path) -> Dict[str, Any]:
        """Extract text and structure using Docling OCR."""
        try:
            log.debug(f"🔍 Running Docling OCR on {pdf_path.name}")
            
            # Convert with structure preservation
            result = self.converter.convert(str(pdf_path))
            
            # Try to get markdown format
            try:
                full_markdown = result.document.export_to_markdown()
            except:
                # Fallback to text format
                full_markdown = str(result.document)
            
            # Extract page-by-page content
            pages = []
            
            # Try to split by pages (various possible separators)
            page_separators = ['\n\n---\n\n', '\n---\n', '\f', '\n\n\n']
            page_texts = [full_markdown]
            
            for separator in page_separators:
                if separator in full_markdown:
                    page_texts = full_markdown.split(separator)
                    break
            
            for i, page_text in enumerate(page_texts, 1):
                if page_text.strip():
                    pages.append({
                        "page_number": i,
                        "text": page_text.strip(),
                        "char_count": len(page_text)
                    })
            
            return {
                "full_text": full_markdown,
                "pages": pages
            }
            
        except Exception as e:
            log.error(f"❌ Docling extraction failed: {e}")
            # Fallback to basic text extraction
            return self._fallback_text_extraction(pdf_path)
    
    def _extract_hyperlinks(self, pdf_path: Path) -> List[Dict[str, Any]]:
        """Extract hyperlinks with coordinates using PyMuPDF."""
        hyperlinks = []
        
        try:
            log.debug(f"🔗 Extracting hyperlinks from {pdf_path.name}")
            
            with fitz.open(str(pdf_path)) as doc:
                for page_num in range(len(doc)):
                    page = doc[page_num]
                    links = page.get_links()
                    
                    for link in links:
                        if link.get('uri'):  # External URL
                            # Get text content in link area
                            rect = fitz.Rect(link['from'])
                            link_text = page.get_textbox(rect).strip()
                            
                            hyperlinks.append({
                                "url": link['uri'],
                                "text": link_text,
                                "page": page_num + 1,
                                "coordinates": {
                                    "x0": rect.x0,
                                    "y0": rect.y0, 
                                    "x1": rect.x1,
                                    "y1": rect.y1
                                }
                            })
            
            log.debug(f"🔗 Found {len(hyperlinks)} hyperlinks")
            return hyperlinks
            
        except Exception as e:
            log.error(f"❌ Hyperlink extraction failed: {e}")
            return []
    
    def _fallback_text_extraction(self, pdf_path: Path) -> Dict[str, Any]:
        """Fallback text extraction using PyMuPDF only."""
        log.warning(f"⚠️  Using fallback extraction for {pdf_path.name}")
        
        pages = []
        full_text = ""
        
        try:
            with fitz.open(str(pdf_path)) as doc:
                for page_num in range(len(doc)):
                    page = doc[page_num]
                    page_text = page.get_text()
                    
                    pages.append({
                        "page_number": page_num + 1,
                        "text": page_text,
                        "char_count": len(page_text)
                    })
                    
                    full_text += page_text + "\n\n"
            
            return {
                "full_text": full_text.strip(),
                "pages": pages
            }
            
        except Exception as e:
            log.error(f"❌ Fallback extraction failed: {e}")
            return {"full_text": "", "pages": []}
    
    def _generate_document_id(self, pdf_path: Path) -> str:
        """Generate canonical document ID based on filename."""
        # Use filename + size for consistent ID generation
        file_stats = pdf_path.stat()
        id_string = f"{pdf_path.name}_{file_stats.st_size}_{file_stats.st_mtime}"
        return f"DOC_{hashlib.sha1(id_string.encode()).hexdigest()[:12]}"
    
    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def process_directory(self, pdf_dir: Path) -> List[Dict[str, Any]]:
        """Process all PDF files in a directory."""
        log.info(f"🚀 Stage 1: Processing PDF directory: {pdf_dir}")
        
        pdf_files = list(pdf_dir.glob("*.pdf"))
        log.info(f"Found {len(pdf_files)} PDF files")
        
        results = []
        for pdf_file in pdf_files:
            try:
                result = self.extract_pdf(pdf_file)
                results.append(result)
            except Exception as e:
                log.error(f"❌ Failed to process {pdf_file.name}: {e}")
                continue
        
        log.info(f"✅ Stage 1 completed: {len(results)}/{len(pdf_files)} files processed")
        return results

def main():
    """Example usage of Stage 1 PDF extraction."""
    logging.basicConfig(level=logging.INFO)
    
    extractor = PDFOCRExtractor()
    
    # Example: Process a directory of PDFs
    # pdf_directory = Path("city_clerk_documents/pdfs")
    # results = extractor.process_directory(pdf_directory)
    
    print("🚀 Stage 1: PDF OCR Extractor ready!")
    print("✅ Features:")
    print("  - Docling OCR with table structure detection")
    print("  - PyMuPDF hyperlink extraction with coordinates") 
    print("  - Robust fallback mechanisms")
    print("  - Page-by-page content preservation")
    print("  - Canonical document ID generation")

if __name__ == "__main__":
    main() 