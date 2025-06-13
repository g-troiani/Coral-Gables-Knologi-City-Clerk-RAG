"""
Extracts structured content from agenda PDFs, including items and hyperlinks.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
import json
import re
import fitz  # PyMuPDF for hyperlink extraction
import hashlib
import asyncio
from datetime import datetime

from .pdf_extractor import PDFExtractor
from ..common.utils import get_llm_client, clean_json_response, call_llm_with_retry

log = logging.getLogger(__name__)


class AgendaExtractor:
    """Extracts structured content from agenda PDFs using OCR, PyMuPDF, and an LLM."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.pdf_extractor = PDFExtractor()
        self.llm_client = get_llm_client()
        self.model = "gpt-4"
        
        # Cache for extractions
        self._extraction_cache = {}

    async def extract_and_save_agenda(self, pdf_path: Path) -> None:
        """Orchestrates the full extraction and saving process for an agenda PDF."""
        log.info(f"📄 Extracting agenda from: {pdf_path.name}")
        
        # Check cache first
        file_hash = self._get_file_hash(pdf_path)
        if file_hash in self._extraction_cache:
            log.info(f"📋 Using cached extraction for {pdf_path.name}")
            agenda_data = self._extraction_cache[file_hash]
        else:
            # Extract text using base PDF extractor
            full_text, pages = self.pdf_extractor.extract_text_from_pdf(pdf_path)
            if not full_text:
                log.warning(f"No text extracted from {pdf_path.name}, skipping.")
                return

            # Extract structured agenda items using LLM
            items = await self._extract_agenda_items_with_llm(full_text)
            
            # Extract hyperlinks using PyMuPDF
            links = self._extract_hyperlinks_pymupdf(pdf_path)
            
            # Associate URLs with items
            self._associate_urls_with_items(items, links)
            
            # Extract meeting info
            meeting_info = self._extract_meeting_info(pdf_path, full_text)
            
            # Create agenda data structure
            agenda_data = {
                'source_file': pdf_path.name,
                'doc_id': self._generate_doc_id(pdf_path),
                'full_text': full_text,
                'agenda_items': items,
                'hyperlinks': links,
                'meeting_info': meeting_info,
                'metadata': {
                    'extraction_method': 'docling+llm+pymupdf',
                    'num_items': len(items),
                    'num_hyperlinks': len(links),
                    'extraction_timestamp': datetime.now().isoformat()
                }
            }
            
            # Cache result
            self._extraction_cache[file_hash] = agenda_data

        # Save as enriched markdown
        self._save_as_markdown(pdf_path, agenda_data)

    async def _extract_agenda_items_with_llm(self, text: str) -> List[Dict]:
        """Extract agenda items using LLM."""
        log.info("🧠 Using LLM to extract agenda structure...")
        
        # Prepare the extraction prompt
        messages = [
            {
                "role": "system",
                "content": """You are an expert at extracting structured data from city council agenda documents. 
Extract ALL agenda items from the document, looking for patterns like:
- Letter-Number format (e.g., H-1, A-2, B-3)
- Letter.-Number. format (e.g., H.-1., A.-2.)
- Items with references like "23-6819"

Return a JSON array of objects with these fields:
- item_code: The item identifier (e.g., "H-1")
- title: The item title/description
- section_name: The section this item belongs to
- document_reference: Any document reference number (e.g., "23-6819")
- item_type: Type of item (ordinance, resolution, report, etc.)

Be thorough and extract ALL items, even those marked as "None"."""
            },
            {
                "role": "user", 
                "content": f"Extract agenda items from this document:\n\n{text[:15000]}"  # Limit length
            }
        ]
        
        try:
            response = await call_llm_with_retry(
                self.llm_client,
                messages,
                model=self.model,
                temperature=0.1
            )
            
            # Parse the JSON response
            items = clean_json_response(response)
            
            if not isinstance(items, list):
                log.warning("LLM returned non-list response, wrapping in list")
                items = [items] if items else []
            
            # Add canonical IDs to items
            doc_id = hashlib.sha1(text[:1000].encode()).hexdigest()[:12]
            for i, item in enumerate(items):
                item['id'] = f"ITEM_{doc_id}_{i:03d}"
                item['urls'] = []  # Initialize empty URLs list
            
            log.info(f"✨ Extracted {len(items)} agenda items")
            return items
            
        except Exception as e:
            log.error(f"❌ Failed to extract agenda items with LLM: {e}")
            return []

    def _extract_hyperlinks_pymupdf(self, pdf_path: Path) -> List[Dict]:
        """Extract hyperlinks using PyMuPDF."""
        hyperlinks = []
        
        try:
            pdf_document = fitz.open(str(pdf_path))
            
            for page_num in range(len(pdf_document)):
                page = pdf_document[page_num]
                links = page.get_links()
                
                for link in links:
                    if link.get('uri'):  # External URL
                        rect = fitz.Rect(link['from'])
                        link_text = page.get_text(clip=rect).strip()
                        link_text = ' '.join(link_text.split())
                        
                        hyperlinks.append({
                            'url': link['uri'],
                            'text': link_text or 'Click here',
                            'page': page_num + 1,
                            'rect': {
                                'x0': link['from'].x0,
                                'y0': link['from'].y0,
                                'x1': link['from'].x1,
                                'y1': link['from'].y1
                            }
                        })
            
            pdf_document.close()
            log.info(f"🔗 Extracted {len(hyperlinks)} hyperlinks")
            
        except Exception as e:
            log.error(f"Failed to extract hyperlinks: {e}")
        
        return hyperlinks

    def _associate_urls_with_items(self, items: List[Dict], links: List[Dict]) -> None:
        """Associate extracted URLs with their corresponding agenda items."""
        for link in links:
            link_text = link.get('text', '').upper()
            
            # Try to match link to agenda items
            for item in items:
                item_code = item.get('item_code', '')
                if item_code and item_code in link_text:
                    item['urls'].append({
                        'url': link['url'],
                        'text': link['text'],
                        'page': link['page']
                    })
                    log.debug(f"🔗 Associated URL with item {item_code}")
                    break

    def _extract_meeting_info(self, pdf_path: Path, full_text: str) -> Dict:
        """Extract meeting information from the agenda."""
        meeting_info = {
            'date': 'N/A',
            'time': 'N/A',
            'location': 'N/A'
        }
        
        # Try to extract date from filename
        date_match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', pdf_path.name)
        if date_match:
            month, day, year = date_match.groups()
            meeting_info['date'] = f"{month}.{day}.{year}"
        
        # Try to extract time and location from text
        lines = full_text.split('\n')[:50]
        for line in lines:
            line = line.strip()
            
            # Look for time patterns
            time_match = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)', line, re.IGNORECASE)
            if time_match and meeting_info['time'] == 'N/A':
                meeting_info['time'] = time_match.group(1)
            
            # Look for location
            if 'city hall' in line.lower() or 'commission chamber' in line.lower():
                meeting_info['location'] = line[:100]
        
        return meeting_info

    def _save_as_markdown(self, pdf_path: Path, agenda_data: Dict) -> None:
        """Save agenda as enriched markdown for GraphRAG."""
        meeting_info = agenda_data.get('meeting_info', {})
        meeting_date = meeting_info.get('date', 'unknown')
        
        # If date not found, try to extract from filename
        if meeting_date == 'N/A' or meeting_date == 'unknown':
            filename_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', pdf_path.name)
            if filename_match:
                month = filename_match.group(1).zfill(2)
                day = filename_match.group(2).zfill(2)
                year = filename_match.group(3)
                meeting_date = f"{month}.{day}.{year}"
        
        # Build comprehensive header
        header = self._build_agenda_header(agenda_data)
        
        # Add detailed agenda items section
        items_section = self._build_agenda_items_section(agenda_data)
        
        # Combine with full text
        full_content = header + items_section + "\n\n# FULL AGENDA TEXT\n\n" + agenda_data.get('full_text', '')
        
        # Save markdown
        meeting_date_filename = meeting_date.replace('.', '_') if meeting_date != 'unknown' else 'unknown'
        md_filename = f"agenda_{meeting_date_filename}.md"
        md_path = self.output_dir / md_filename
        
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(full_content)
        
        log.info(f"📝 Saved agenda markdown to: {md_path}")

    def _build_agenda_header(self, agenda_data: Dict) -> str:
        """Build comprehensive agenda header."""
        meeting_info = agenda_data.get('meeting_info', {})
        agenda_items = agenda_data.get('agenda_items', [])
        
        all_item_codes = [item.get('item_code', '') for item in agenda_items if item.get('item_code')]
        
        header = f"""---
DOCUMENT METADATA AND CONTEXT
=============================

**DOCUMENT IDENTIFICATION:**
- Document Type: AGENDA
- Meeting Date: {meeting_info.get('date', 'N/A')}
- Meeting Time: {meeting_info.get('time', 'N/A')}
- Meeting Location: {meeting_info.get('location', 'N/A')}

**ENTITIES IN THIS DOCUMENT:**
{self._format_agenda_entities(all_item_codes)}

**SEARCHABLE IDENTIFIERS:**
- DOCUMENT_TYPE: AGENDA
{self._format_item_identifiers(all_item_codes)}

---

"""
        return header

    def _format_agenda_entities(self, item_codes: List[str]) -> str:
        """Format agenda item entities."""
        lines = []
        for code in item_codes[:10]:
            lines.append(f"- AGENDA_ITEM: {code}")
        if len(item_codes) > 10:
            lines.append(f"- ... and {len(item_codes) - 10} more items")
        return '\n'.join(lines)

    def _format_item_identifiers(self, item_codes: List[str]) -> str:
        """Format item identifiers."""
        lines = []
        for code in item_codes:
            lines.append(f"- AGENDA_ITEM: {code}")
        return '\n'.join(lines)

    def _build_agenda_items_section(self, agenda_data: Dict) -> str:
        """Build agenda items section."""
        lines = ["## AGENDA ITEMS QUICK REFERENCE\n"]
        
        for item in agenda_data.get('agenda_items', []):
            item_code = item.get('item_code', 'UNKNOWN')
            lines.append(f"### Agenda Item {item_code}")
            lines.append(f"**Title:** {item.get('title', 'N/A')}")
            lines.append(f"**Section:** {item.get('section_name', 'N/A')}")
            if item.get('document_reference'):
                lines.append(f"**Reference:** {item.get('document_reference')}")
            lines.append(f"\n**What is Item {item_code}?**")
            lines.append(f"Item {item_code} is '{item.get('title', 'N/A')}'")
            
            # Add URLs if available
            urls = item.get('urls', [])
            if urls:
                lines.append(f"\n**Related Documents:**")
                for url in urls:
                    lines.append(f"- [{url.get('text', 'Document')}]({url.get('url')})")
            
            lines.append("")
        
        return '\n'.join(lines)

    def _generate_doc_id(self, pdf_path: Path) -> str:
        """Generate canonical document ID."""
        return f"DOC_{hashlib.sha1(str(pdf_path.absolute()).encode()).hexdigest()[:12]}"

    def _get_file_hash(self, file_path: Path) -> str:
        """Get hash of file for caching."""
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest() 