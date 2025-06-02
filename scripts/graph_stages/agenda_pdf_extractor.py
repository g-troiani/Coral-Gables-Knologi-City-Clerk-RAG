# scripts/graph_stages/agenda_pdf_extractor.py
"""
PDF Extractor for City Clerk Agenda Documents
Extracts text, structure, and hyperlinks from agenda PDFs.
"""

import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
import json
import re
import PyPDF2
import fitz  # PyMuPDF for hyperlink extraction
from unstructured.partition.pdf import partition_pdf
from datetime import datetime

log = logging.getLogger('agenda_pdf_extractor')


class AgendaPDFExtractor:
    """Extract structured content from city agenda PDFs."""
    
    def __init__(self, output_dir: Optional[Path] = None):
        self.output_dir = output_dir or Path("city_clerk_documents/graph_json")
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def extract_agenda(self, pdf_path: Path) -> Dict[str, Any]:
        """Extract complete content from agenda PDF."""
        log.info(f"📄 Extracting agenda from {pdf_path.name}")
        
        # Try multiple extraction methods
        sections = self._extract_with_unstructured(pdf_path)
        if not sections:
            log.warning("Unstructured extraction failed, trying PyPDF2")
            sections = self._extract_with_pypdf(pdf_path)
        
        # Extract hyperlinks
        hyperlinks = self._extract_hyperlinks(pdf_path)
        
        # Build result
        result = {
            "title": self._extract_title(sections),
            "sections": sections,
            "hyperlinks": hyperlinks,
            "metadata": {
                "source_pdf": str(pdf_path.absolute()),
                "extraction_method": "unstructured" if sections else "pypdf2",
                "extraction_date": datetime.utcnow().isoformat() + "Z",
                "total_pages": self._count_pages(pdf_path)
            }
        }
        
        # Save extracted data
        output_path = self.output_dir / f"{pdf_path.stem}_extracted.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        log.info(f"✅ Extraction complete: {len(sections)} sections, {len(hyperlinks)} hyperlinks")
        return result
    
    def _extract_with_unstructured(self, pdf_path: Path) -> List[Dict[str, Any]]:
        """Extract using unstructured library."""
        try:
            elements = partition_pdf(
                filename=str(pdf_path),
                strategy="hi_res",
                extract_images_in_pdf=False,
                infer_table_structure=True
            )
            
            # Group elements by page
            pages = {}
            for elem in elements:
                page_num = getattr(elem.metadata, "page_number", 1)
                if page_num not in pages:
                    pages[page_num] = []
                
                elem_dict = {
                    "type": elem.category or "paragraph",
                    "text": str(elem).strip()
                }
                pages[page_num].append(elem_dict)
            
            # Convert to sections
            sections = []
            for page_num in sorted(pages.keys()):
                section = {
                    "section": f"Page {page_num}",
                    "page_start": page_num,
                    "page_end": page_num,
                    "elements": pages[page_num],
                    "text": "\n".join(e["text"] for e in pages[page_num])
                }
                sections.append(section)
            
            return sections
            
        except Exception as e:
            log.error(f"Unstructured extraction failed: {e}")
            return []
    
    def _extract_with_pypdf(self, pdf_path: Path) -> List[Dict[str, Any]]:
        """Fallback extraction using PyPDF2."""
        sections = []
        try:
            with open(pdf_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                for page_num, page in enumerate(reader.pages, 1):
                    text = page.extract_text()
                    if text:
                        # Split into paragraphs
                        paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
                        
                        section = {
                            "section": f"Page {page_num}",
                            "page_start": page_num,
                            "page_end": page_num,
                            "elements": [
                                {"type": "paragraph", "text": p} for p in paragraphs
                            ],
                            "text": text
                        }
                        sections.append(section)
        except Exception as e:
            log.error(f"PyPDF2 extraction failed: {e}")
        
        return sections
    
    def _extract_hyperlinks(self, pdf_path: Path) -> Dict[str, Dict[str, Any]]:
        """Extract hyperlinks using PyMuPDF."""
        hyperlinks = {}
        
        try:
            doc = fitz.open(pdf_path)
            
            for page_num, page in enumerate(doc, 1):
                # Get all links on the page
                links = page.get_links()
                
                for link in links:
                    if link.get("uri"):  # External link
                        # Try to find the text around the link
                        rect = fitz.Rect(link["from"])
                        text = page.get_textbox(rect).strip()
                        
                        # Look for document reference numbers (e.g., 23-6887)
                        ref_match = re.search(r'\d{2}-\d{4}', text)
                        if ref_match:
                            ref_num = ref_match.group()
                            hyperlinks[ref_num] = {
                                "url": link["uri"],
                                "page": page_num,
                                "text": text
                            }
            
            doc.close()
            
        except Exception as e:
            log.warning(f"Hyperlink extraction failed: {e}")
        
        return hyperlinks
    
    def _extract_title(self, sections: List[Dict[str, Any]]) -> str:
        """Extract document title from first page."""
        if sections and sections[0]["elements"]:
            # Look for title-like elements in first few elements
            for elem in sections[0]["elements"][:5]:
                text = elem.get("text", "")
                if "agenda" in text.lower() and len(text) < 200:
                    return text
        
        return "City Commission Agenda"
    
    def _count_pages(self, pdf_path: Path) -> int:
        """Count total pages in PDF."""
        try:
            with open(pdf_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                return len(reader.pages)
        except:
            return 0
    
    # Alias for compatibility
    def extract(self, pdf_path: Path) -> Dict[str, Any]:
        """Alias for extract_agenda."""
        return self.extract_agenda(pdf_path) 