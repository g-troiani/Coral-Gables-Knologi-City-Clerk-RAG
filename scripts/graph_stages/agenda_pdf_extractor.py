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
from groq import Groq
import os

log = logging.getLogger(__name__)


class AgendaPDFExtractor:
    """Extract structured content from agenda PDFs using Docling and LLM."""
    
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
        
        # Initialize Groq client for LLM extraction
        self.client = Groq(api_key=os.getenv("GROQ_API_KEY"))
        self.model = "qwen-qwq-32b"
    
    def extract_agenda(self, pdf_path: Path) -> Dict[str, any]:
        """Extract agenda content from PDF using Docling + LLM."""
        log.info(f"📄 Extracting agenda from {pdf_path.name}")
        
        # Convert with Docling - pass path directly
        result = self.converter.convert(str(pdf_path))
        
        # Get the document
        doc = result.document
        
        # Get full text and markdown
        full_text = doc.export_to_markdown() or ""
        
        # Use LLM to extract structured agenda items
        log.info("🧠 Using LLM to extract agenda structure...")
        extracted_items = self._extract_agenda_items_with_llm(full_text)
        
        # Build sections from extracted items
        sections = self._build_sections_from_items(extracted_items, full_text)
        
        # Extract hyperlinks if available
        hyperlinks = self._extract_hyperlinks(doc)
        
        # Create agenda data structure with both raw and structured data
        agenda_data = {
            'source_file': pdf_path.name,
            'full_text': full_text,
            'sections': sections,
            'agenda_items': extracted_items,  # Add structured items
            'hyperlinks': hyperlinks,
            'metadata': {
                'extraction_method': 'docling+llm',
                'num_sections': len(sections),
                'num_items': len(extracted_items),
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
        
        log.info(f"✅ Extraction complete: {len(sections)} sections, {len(extracted_items)} items, {len(hyperlinks)} hyperlinks")
        log.info(f"✅ Saved extracted data to: {output_file}")
        
        return agenda_data
    
    def _extract_agenda_items_with_llm(self, text: str) -> List[Dict[str, any]]:
        """Use LLM to extract agenda items from the text."""
        # Split text into chunks if too long
        max_chars = 30000
        chunks = []
        
        if len(text) > max_chars:
            # Split by lines to avoid breaking mid-sentence
            lines = text.split('\n')
            current_chunk = []
            current_length = 0
            
            for line in lines:
                if current_length + len(line) > max_chars and current_chunk:
                    chunks.append('\n'.join(current_chunk))
                    current_chunk = [line]
                    current_length = len(line)
                else:
                    current_chunk.append(line)
                    current_length += len(line) + 1
            
            if current_chunk:
                chunks.append('\n'.join(current_chunk))
        else:
            chunks = [text]
        
        all_items = []
        
        for i, chunk in enumerate(chunks):
            log.info(f"Processing chunk {i+1}/{len(chunks)}")
            
            prompt = """Extract ALL agenda items from this city council agenda document. Look for items with these EXACT formats:

- E.-1. 23-6723 (ordinances - with periods)
- F.-1. 23-6762 (city commission items - with periods)  
- H.-1. 23-6819 (city manager items - with periods)
- D.-1. 23-6830 (consent agenda items - with periods)

The format is: ## LETTER.-NUMBER. REFERENCE-NUMBER

For EACH item found, extract:
1. item_code: Just the letter-number part (e.g., "E-1", "F-10", "H-3") - REMOVE the periods
2. document_reference: The reference number (e.g., "23-6723")  
3. title: The full title/description that follows
4. item_type: "Ordinance" for E items, "Resolution" for F items, "Other" for everything else

IMPORTANT: Look for ALL items including E.-1., E.-2., E.-3., F.-1., F.-2., etc.

Return ONLY a valid JSON array in this format:
[
  {
    "item_code": "E-1",
    "document_reference": "23-6723", 
    "title": "An Ordinance of the City Commission...",
    "item_type": "Ordinance"
  }
]

Document text:
""" + chunk
            
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are an expert at extracting structured data from city government agenda documents. Return only valid JSON."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1
                )
                
                response_text = response.choices[0].message.content.strip()
                
                # Clean up response to ensure valid JSON
                if response_text.startswith('```json'):
                    response_text = response_text.replace('```json', '').replace('```', '')
                elif response_text.startswith('```'):
                    response_text = response_text.replace('```', '')
                
                response_text = response_text.strip()
                
                # Try to parse JSON
                try:
                    data = json.loads(response_text)
                    if isinstance(data, dict) and 'items' in data:
                        items = data['items']
                    elif isinstance(data, list):
                        items = data
                    else:
                        log.warning(f"Unexpected LLM response format: {type(data)}")
                        items = []
                        
                    all_items.extend(items)
                    log.info(f"Extracted {len(items)} items from chunk {i+1}")
                    
                except json.JSONDecodeError as e:
                    log.error(f"Failed to parse JSON from chunk {i+1}: {e}")
                    log.error(f"Raw response: {response_text[:200]}...")
                    # Try manual extraction as fallback
                    manual_items = self._manual_extract_items(chunk)
                    all_items.extend(manual_items)
                    log.info(f"Manual fallback extracted {len(manual_items)} items")
                    
            except Exception as e:
                log.error(f"LLM extraction failed for chunk {i+1}: {e}")
                # Fallback to manual extraction
                manual_items = self._manual_extract_items(chunk)
                all_items.extend(manual_items)
                log.info(f"Manual fallback extracted {len(manual_items)} items")
        
        # Deduplicate items by item_code
        seen_codes = set()
        unique_items = []
        for item in all_items:
            if item.get('item_code') and item['item_code'] not in seen_codes:
                seen_codes.add(item['item_code'])
                unique_items.append(item)
        
        log.info(f"Total unique items extracted: {len(unique_items)}")
        return unique_items
    
    def _manual_extract_items(self, text: str) -> List[Dict[str, any]]:
        """Manually extract agenda items using regex patterns."""
        items = []
        
        # Pattern to match agenda items in markdown format: ## E.-1. 23-6723
        # Also handle cases without markdown headers
        patterns = [
            # Markdown header format: ## E.-1. 23-6723
            r'^##\s*([A-Z])\.-(\d+)\.\s+(\d{2}-\d{4,5})\s*$',
            # Direct format: E.-1. 23-6723  
            r'^([A-Z])\.-(\d+)\.\s+(\d{2}-\d{4,5})\s*$',
            # Table format: | E.-1. | 23-6723 |
            r'^\|\s*([A-Z])\.-(\d+)\.\s*\|\s*(\d{2}-\d{4,5})\s*\|'
        ]
        
        lines = text.split('\n')
        
        for i, line in enumerate(lines):
            line = line.strip()
            
            for pattern in patterns:
                match = re.match(pattern, line)
                if match:
                    letter = match.group(1)
                    number = match.group(2)
                    doc_ref = match.group(3)
                    
                    # Get title from subsequent lines
                    title_lines = []
                    for j in range(i + 1, min(i + 5, len(lines))):
                        next_line = lines[j].strip()
                        if next_line and not re.match(r'^##\s*[A-Z]\.-\d+\.', next_line):
                            title_lines.append(next_line)
                        else:
                            break
                    
                    title = ' '.join(title_lines) if title_lines else f"{letter}-{number}"
                    
                    # Clean up title - remove markdown formatting
                    title = re.sub(r'^[-\*\#\|]+\s*', '', title)
                    title = title.replace('|', '').strip()
                    
                    # Determine item type
                    if letter == 'E':
                        item_type = "Ordinance"
                    elif letter == 'F':
                        item_type = "Resolution"
                    else:
                        item_type = "Other"
                    
                    items.append({
                        "item_code": f"{letter}-{number}",
                        "document_reference": doc_ref,
                        "title": title[:500],  # Limit title length
                        "item_type": item_type
                    })
                    break
        
        log.info(f"Manual regex extraction found {len(items)} items")
        return items
    
    def _build_sections_from_items(self, items: List[Dict], full_text: str) -> List[Dict[str, str]]:
        """Build sections structure from extracted items."""
        if not items:
            # If no items found, return the full document as one section
            return [{
                'title': 'Full Document',
                'text': full_text
            }]
        
        # Group items into sections
        sections = []
        
        # Create agenda items section
        agenda_section_text = []
        for item in items:
            item_text = f"{item['item_code']} - {item['document_reference']}\n{item['title']}\n"
            agenda_section_text.append(item_text)
        
        sections.append({
            'title': 'AGENDA ITEMS',
            'text': '\n'.join(agenda_section_text)
        })
        
        return sections
    
    def _extract_hyperlinks(self, doc) -> Dict[str, Dict[str, any]]:
        """Extract hyperlinks from the document."""
        hyperlinks = {}
        
        # Try to extract links from document structure
        if hasattr(doc, 'links'):
            for link in doc.links:
                if hasattr(link, 'text') and hasattr(link, 'url'):
                    hyperlinks[link.text] = {
                        'url': link.url,
                        'page': getattr(link, 'page', 0)
                    }
        
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