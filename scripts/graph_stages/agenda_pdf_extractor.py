# scripts/graph_stages/agenda_pdf_extractor.py
"""
PDF Extractor for City Clerk Agenda Documents
Extracts text, structure, and hyperlinks from agenda PDFs.
"""

import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json
import re
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions

log = logging.getLogger(__name__)


class AgendaPDFExtractor:
    """Extract structured content from agenda PDFs using Docling."""
    
    def __init__(self, output_dir: Optional[Path] = None):
        """Initialize the agenda PDF extractor."""
        self.output_dir = output_dir or Path("city_clerk_documents/extracted_text")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize Docling converter with OCR enabled
        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = True  # Enable OCR for better text extraction
        pipeline_options.do_table_structure = True  # Better table extraction
        
        self.converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
            }
        )
    
    def extract_agenda(self, pdf_path: Path) -> Dict[str, any]:
        """Extract agenda content from PDF."""
        log.info(f"📄 Extracting agenda from {pdf_path.name}")
        
        # Convert with Docling - pass path directly
        result = self.converter.convert(str(pdf_path))
        
        # Get the document
        doc = result.document
        
        # Get full text and markdown
        full_text = doc.export_to_markdown() or ""
        
        # Extract sections based on the document structure
        sections = self._extract_sections_from_text(full_text)
        
        # Extract hyperlinks if available
        hyperlinks = self._extract_hyperlinks(doc)
        
        # Create agenda data structure
        agenda_data = {
            'source_file': pdf_path.name,
            'full_text': full_text,
            'sections': sections,
            'hyperlinks': hyperlinks,
            'metadata': {
                'extraction_method': 'docling',
                'num_sections': len(sections),
                'num_hyperlinks': len(hyperlinks)
            }
        }
        
        # IMPORTANT: Save the extracted data with the filename expected by ontology extractor
        # The ontology extractor looks for "{pdf_stem}_extracted.json"
        output_file = self.output_dir / f"{pdf_path.stem}_extracted.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(agenda_data, f, indent=2, ensure_ascii=False)
        
        # Also save debug output
        debug_file = self.output_dir / f"{pdf_path.stem}_docling_extracted.json"
        with open(debug_file, 'w', encoding='utf-8') as f:
            json.dump(agenda_data, f, indent=2, ensure_ascii=False)
        
        # Also save just the full text for debugging
        text_file = self.output_dir / f"{pdf_path.stem}_full_text.txt"
        with open(text_file, 'w', encoding='utf-8') as f:
            f.write(full_text)
        
        log.info(f"✅ Extraction complete: {len(sections)} sections, {len(hyperlinks)} hyperlinks")
        log.info(f"✅ Saved extracted data to: {output_file}")
        
        return agenda_data
    
    def _extract_sections_from_text(self, text: str) -> List[Dict[str, str]]:
        """Extract sections from the text."""
        sections = []
        
        # Split text into lines for processing
        lines = text.split('\n')
        
        current_section = None
        section_text = []
        
        for line in lines:
            # Look for section headers
            if self._is_section_header(line):
                # Save previous section
                if current_section:
                    sections.append({
                        'title': current_section,
                        'text': '\n'.join(section_text).strip()
                    })
                # Start new section
                current_section = line.strip()
                section_text = []
            else:
                section_text.append(line)
        
        # Don't forget last section
        if current_section:
            sections.append({
                'title': current_section,
                'text': '\n'.join(section_text).strip()
            })
        
        # If no sections found, treat entire text as one section
        if not sections:
            sections = [{
                'title': 'Full Document',
                'text': text
            }]
        
        return sections
    
    def _is_section_header(self, line: str) -> bool:
        """Determine if a line is a section header."""
        # Customize these patterns based on your agenda format
        header_patterns = [
            r'^[A-Z\s]+$',  # All caps lines
            r'^\d+\.\s+[A-Z]',  # Numbered sections
            r'^[A-Z]\.\s+',  # Letter sections
            r'^(CONSENT|PUBLIC|ORDINANCES|RESOLUTIONS|AGENDA ITEMS)',  # Specific headers
        ]
        
        line = line.strip()
        if not line or len(line) < 3:
            return False
        
        return any(re.match(pattern, line) for pattern in header_patterns)
    
    def _extract_hyperlinks(self, doc) -> Dict[str, Dict[str, any]]:
        """Extract hyperlinks from the document."""
        hyperlinks = {}
        
        # Try to extract links from document structure
        # The exact attribute names may vary, so we'll try multiple approaches
        
        # Try standard attributes
        if hasattr(doc, 'links'):
            for link in doc.links:
                if hasattr(link, 'text') and hasattr(link, 'url'):
                    hyperlinks[link.text] = {
                        'url': link.url,
                        'page': getattr(link, 'page', 0)
                    }
        
        # Try to find hyperlinks in the document's JSON representation if available
        if hasattr(doc, 'to_dict'):
            try:
                doc_dict = doc.to_dict()
                # Look for hyperlink patterns in the dictionary
                # This is a fallback approach
            except:
                pass
            
        # Try to extract from markdown if links are preserved there
        if hasattr(doc, 'export_to_markdown'):
            markdown = doc.export_to_markdown()
            # Extract markdown links pattern [text](url)
            link_pattern = r'\[([^\]]+)\]\(([^)]+)\)'
            for match in re.finditer(link_pattern, markdown):
                text, url = match.groups()
                if text and url:
                    hyperlinks[text] = {
                        'url': url,
                        'page': 0  # We don't have page info from markdown
                    }
        
        return hyperlinks 