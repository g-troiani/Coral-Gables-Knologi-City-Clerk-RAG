#!/usr/bin/env python3
"""
Agenda PDF Extractor for Graph Pipeline
======================================
Specialized PDF extraction for city clerk agendas with focus on preserving
document hierarchy and structure. Prioritizes unstructured over docling.
"""
from __future__ import annotations

import json
import logging
import os
import pathlib
import re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from collections import defaultdict

# Core dependencies
import PyPDF2
from dotenv import load_dotenv

# Try to import unstructured first (preferred for hierarchy)
try:
    from unstructured.partition.pdf import partition_pdf
    from unstructured.documents.elements import (
        Title, NarrativeText, ListItem, Table, PageBreak,
        Header, Footer, Image, FigureCaption
    )
    UNSTRUCTURED_AVAILABLE = True
except ImportError:
    UNSTRUCTURED_AVAILABLE = False
    logging.warning("unstructured not available - falling back to other methods")

# Try to import docling as secondary option
try:
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption
    DOCLING_AVAILABLE = True
except ImportError:
    DOCLING_AVAILABLE = False
    logging.warning("docling not available")

# Try to import pdfplumber for OCR
try:
    import pdfplumber
    import pytesseract
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    logging.warning("OCR libraries not available (pdfplumber/pytesseract)")

load_dotenv()
log = logging.getLogger(__name__)


class AgendaPDFExtractor:
    """Extract structured content from agenda PDFs preserving hierarchy."""
    
    def __init__(self, output_dir: Optional[pathlib.Path] = None):
        self.output_dir = output_dir or pathlib.Path("city_clerk_documents/graph_json")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize converters if available
        self.docling_converter = self._init_docling() if DOCLING_AVAILABLE else None
        
    def _init_docling(self):
        """Initialize docling converter with optimal settings."""
        opts = PdfPipelineOptions()
        opts.do_ocr = True
        opts.do_table_structure = True
        return DocumentConverter({
            InputFormat.PDF: PdfFormatOption(pipeline_options=opts)
        })
    
    def extract_agenda(self, pdf_path: pathlib.Path, force_method: Optional[str] = None) -> Dict:
        """
        Extract agenda content with hierarchy preservation.
        
        Args:
            pdf_path: Path to PDF file
            force_method: Force specific extraction method ('unstructured', 'docling', 'pypdf')
            
        Returns:
            Dictionary with extracted content and hierarchy
        """
        log.info(f"Extracting agenda from: {pdf_path.name}")
        
        # Check if we already have extracted data
        json_path = self.output_dir / f"{pdf_path.stem}_extracted.json"
        if json_path.exists() and not force_method:
            log.info(f"Loading existing extraction from: {json_path}")
            return json.loads(json_path.read_text())
        
        # Determine extraction method
        if force_method:
            method = force_method
        elif UNSTRUCTURED_AVAILABLE:
            method = 'unstructured'
        elif DOCLING_AVAILABLE:
            method = 'docling'
        else:
            method = 'pypdf'
        
        log.info(f"Using extraction method: {method}")
        
        # Extract based on method
        if method == 'unstructured':
            result = self._extract_with_unstructured(pdf_path)
        elif method == 'docling':
            result = self._extract_with_docling(pdf_path)
        else:
            result = self._extract_with_pypdf(pdf_path)
        
        # Add metadata
        result['metadata'] = {
            'source_pdf': str(pdf_path.absolute()),
            'extraction_method': method,
            'extraction_date': datetime.utcnow().isoformat(),
            'filename': pdf_path.name
        }
        
        # Save extracted data
        json_path.write_text(json.dumps(result, indent=2, ensure_ascii=False))
        log.info(f"Saved extraction to: {json_path}")
        
        return result
    
    def _extract_with_unstructured(self, pdf_path: pathlib.Path) -> Dict:
        """Extract using unstructured library - best for hierarchy."""
        log.info("Extracting with unstructured (preferred for hierarchy)...")
        
        # Partition with detailed settings
        elements = partition_pdf(
            str(pdf_path),
            strategy="hi_res",  # Use high-resolution strategy
            infer_table_structure=True,
            include_page_breaks=True,
            extract_images_in_pdf=False,  # Skip images for now
            extract_forms=True
        )
        
        # Build hierarchical structure
        hierarchy = {
            'title': None,
            'sections': [],
            'raw_elements': [],
            'page_structure': defaultdict(list)
        }
        
        current_section = None
        current_subsection = None
        page_num = 1
        
        for element in elements:
            # Track page breaks
            if isinstance(element, PageBreak):
                page_num += 1
                continue
            
            # Store raw element
            element_data = {
                'type': element.category,
                'text': str(element),
                'page': page_num,
                'metadata': element.metadata.to_dict() if hasattr(element, 'metadata') else {}
            }
            hierarchy['raw_elements'].append(element_data)
            hierarchy['page_structure'][page_num].append(element_data)
            
            # Build hierarchy based on element type
            if isinstance(element, Title):
                # Check if this is the main title
                if not hierarchy['title'] and page_num == 1:
                    hierarchy['title'] = str(element)
                else:
                    # This is a section title
                    current_section = {
                        'title': str(element),
                        'page_start': page_num,
                        'subsections': [],
                        'content': []
                    }
                    hierarchy['sections'].append(current_section)
                    current_subsection = None
                    
            elif isinstance(element, Header) and current_section:
                # This might be a subsection
                current_subsection = {
                    'title': str(element),
                    'page': page_num,
                    'content': []
                }
                current_section['subsections'].append(current_subsection)
                
            elif isinstance(element, (ListItem, NarrativeText, Table)):
                # Add content to appropriate section
                content_item = {
                    'type': element.category,
                    'text': str(element),
                    'page': page_num
                }
                
                if isinstance(element, Table):
                    content_item['table_data'] = self._extract_table_data(element)
                
                if current_subsection:
                    current_subsection['content'].append(content_item)
                elif current_section:
                    current_section['content'].append(content_item)
                else:
                    # No section yet, might be preamble
                    if not hierarchy.get('preamble'):
                        hierarchy['preamble'] = []
                    hierarchy['preamble'].append(content_item)
        
        # Post-process to extract agenda items
        hierarchy['agenda_items'] = self._extract_agenda_items_from_hierarchy(hierarchy)
        
        return hierarchy
    
    def _extract_with_docling(self, pdf_path: pathlib.Path) -> Dict:
        """Extract using docling - fallback method."""
        log.info("Extracting with docling...")
        
        result = self.docling_converter.convert(str(pdf_path))
        doc = result.document
        
        hierarchy = {
            'title': doc.metadata.title if hasattr(doc.metadata, 'title') else None,
            'sections': [],
            'raw_elements': [],
            'page_structure': defaultdict(list)
        }
        
        current_section = None
        
        for item, level in doc.iterate_items():
            page_num = getattr(item.prov[0], 'page_no', 1) if item.prov else 1
            
            element_data = {
                'type': getattr(item, 'label', 'unknown'),
                'text': str(item.text) if hasattr(item, 'text') else str(item),
                'page': page_num,
                'level': level
            }
            
            hierarchy['raw_elements'].append(element_data)
            hierarchy['page_structure'][page_num].append(element_data)
            
            # Build sections based on level and type
            label = getattr(item, 'label', '').upper()
            if label in ('TITLE', 'SECTION_HEADER') and level <= 1:
                current_section = {
                    'title': element_data['text'],
                    'page_start': page_num,
                    'content': []
                }
                hierarchy['sections'].append(current_section)
            elif current_section:
                current_section['content'].append(element_data)
        
        # Extract agenda items
        hierarchy['agenda_items'] = self._extract_agenda_items_from_hierarchy(hierarchy)
        
        return hierarchy
    
    def _extract_with_pypdf(self, pdf_path: pathlib.Path) -> Dict:
        """Extract using PyPDF2 - basic fallback."""
        log.info("Extracting with PyPDF2 (basic method)...")
        
        hierarchy = {
            'title': None,
            'sections': [],
            'raw_elements': [],
            'page_structure': defaultdict(list)
        }
        
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            
            for page_num, page in enumerate(reader.pages, 1):
                text = page.extract_text()
                
                # Try OCR if text extraction fails
                if not text.strip() and OCR_AVAILABLE:
                    text = self._ocr_page(pdf_path, page_num - 1)
                
                # Split into paragraphs
                paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
                
                for para in paragraphs:
                    element_data = {
                        'type': 'paragraph',
                        'text': para,
                        'page': page_num
                    }
                    hierarchy['raw_elements'].append(element_data)
                    hierarchy['page_structure'][page_num].append(element_data)
                
                # Try to identify sections
                for para in paragraphs:
                    if self._is_section_header(para):
                        section = {
                            'title': para,
                            'page_start': page_num,
                            'content': []
                        }
                        hierarchy['sections'].append(section)
        
        # Extract title from first page
        if hierarchy['page_structure'][1]:
            hierarchy['title'] = hierarchy['page_structure'][1][0]['text']
        
        # Extract agenda items
        hierarchy['agenda_items'] = self._extract_agenda_items_from_hierarchy(hierarchy)
        
        return hierarchy
    
    def _extract_table_data(self, table_element) -> List[List[str]]:
        """Extract structured data from table element."""
        # This would need proper implementation based on unstructured's table format
        # For now, return string representation
        return [[str(table_element)]]
    
    def _ocr_page(self, pdf_path: pathlib.Path, page_index: int) -> str:
        """OCR a specific page."""
        try:
            with pdfplumber.open(str(pdf_path)) as pdf:
                page = pdf.pages[page_index]
                # Convert to image and OCR
                img = page.to_image(resolution=300)
                text = pytesseract.image_to_string(img.original)
                return text
        except Exception as e:
            log.error(f"OCR failed: {e}")
            return ""
    
    def _is_section_header(self, text: str) -> bool:
        """Detect if text is likely a section header."""
        # Common patterns for agenda sections
        patterns = [
            r'^[A-Z][.\s]+[A-Z\s]+$',  # ALL CAPS
            r'^[IVX]+\.\s+',  # Roman numerals
            r'^\d+\.\s+[A-Z]',  # Numbered sections
            r'^(CONSENT AGENDA|PUBLIC HEARING|ORDINANCE|RESOLUTION)',
            r'^(Call to Order|Invocation|Pledge|Minutes|Adjournment)'
        ]
        
        for pattern in patterns:
            if re.match(pattern, text.strip(), re.IGNORECASE):
                return True
        
        return False
    
    def _extract_agenda_items_from_hierarchy(self, hierarchy: Dict) -> List[Dict]:
        """Extract structured agenda items from the hierarchy."""
        items = []
        
        # Patterns for agenda items
        item_patterns = [
            re.compile(r'\b([A-Z])-(\d+)\b'),  # E-1, F-12
            re.compile(r'\b([A-Z])(\d+)\b'),   # E1, F12
            re.compile(r'Item\s+([A-Z])-(\d+)', re.IGNORECASE),
        ]
        
        # Search through all elements
        for element in hierarchy.get('raw_elements', []):
            text = element.get('text', '')
            
            for pattern in item_patterns:
                matches = pattern.finditer(text)
                for match in matches:
                    letter = match.group(1)
                    number = match.group(2)
                    code = f"{letter}-{number}"
                    
                    # Extract context
                    start = max(0, match.start() - 50)
                    end = min(len(text), match.end() + 500)
                    context = text[start:end].strip()
                    
                    item = {
                        'code': code,
                        'letter': letter,
                        'number': number,
                        'page': element.get('page', 1),
                        'context': context,
                        'full_text': text
                    }
                    
                    # Try to extract title
                    title_match = re.search(
                        rf'{re.escape(code)}[:\s]+([^\n]+)', 
                        context
                    )
                    if title_match:
                        item['title'] = title_match.group(1).strip()
                    
                    items.append(item)
        
        # Deduplicate and sort
        seen = set()
        unique_items = []
        for item in sorted(items, key=lambda x: (x['letter'], int(x['number']))):
            if item['code'] not in seen:
                seen.add(item['code'])
                unique_items.append(item)
        
        return unique_items
    
    def get_extraction_stats(self, extracted_data: Dict) -> Dict:
        """Get statistics about the extraction."""
        stats = {
            'pages': len(extracted_data.get('page_structure', {})),
            'sections': len(extracted_data.get('sections', [])),
            'agenda_items': len(extracted_data.get('agenda_items', [])),
            'total_elements': len(extracted_data.get('raw_elements', [])),
            'extraction_method': extracted_data.get('metadata', {}).get('extraction_method', 'unknown')
        }
        
        # Count element types
        element_types = defaultdict(int)
        for element in extracted_data.get('raw_elements', []):
            element_types[element.get('type', 'unknown')] += 1
        
        stats['element_types'] = dict(element_types)
        
        return stats


# Convenience function for direct use
def extract_agenda_pdf(pdf_path: pathlib.Path, output_dir: Optional[pathlib.Path] = None) -> Dict:
    """Extract agenda content from PDF."""
    extractor = AgendaPDFExtractor(output_dir)
    return extractor.extract_agenda(pdf_path) 