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
from openai import OpenAI
import os
import fitz  # PyMuPDF for hyperlink extraction

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
        
        # Initialize OpenAI client for LLM extraction
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = "gpt-4.1-mini-2025-04-14"
    
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
        
        # Extract hyperlinks using PyMuPDF
        hyperlinks = self._extract_hyperlinks_pymupdf(pdf_path)
        
        # Associate hyperlinks with agenda items
        agenda_items_with_urls = self._associate_urls_with_items(extracted_items, hyperlinks, full_text)
        
        # Create agenda data structure with both raw and structured data
        agenda_data = {
            'source_file': pdf_path.name,
            'full_text': full_text,
            'sections': sections,
            'agenda_items': agenda_items_with_urls,  # Updated with URLs
            'hyperlinks': hyperlinks,
            'meeting_info': self._extract_meeting_info(pdf_path, full_text),
            'metadata': {
                'extraction_method': 'docling+llm+pymupdf',
                'num_sections': len(sections),
                'num_items': self._count_items(agenda_items_with_urls),
                'num_hyperlinks': len(hyperlinks)
            }
        }
        
        # Save using the new save method
        output_file = self.output_dir / f"{pdf_path.stem}_extracted.json"
        self.save_extracted_agenda(agenda_data, output_file)
        
        log.info(f"✅ Extraction complete: {len(sections)} sections, {self._count_items(agenda_items_with_urls)} items, {len(hyperlinks)} hyperlinks")
        log.info(f"✅ Saved extracted data to: {output_file}")
        
        return agenda_data

    def save_extracted_agenda(self, agenda_data: dict, output_path: Path):
        """Save extracted agenda data to JSON file."""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(agenda_data, f, indent=2, ensure_ascii=False)
        
        log.info(f"✅ Saved extracted agenda to: {output_path}")
        
        # NEW: Also save as markdown
        self._save_agenda_as_markdown(agenda_data, output_path)

    def _save_agenda_as_markdown(self, agenda_data: dict, json_path: Path):
        """Save agenda as enhanced markdown for GraphRAG."""
        markdown_dir = json_path.parent.parent / "extracted_markdown"
        markdown_dir.mkdir(exist_ok=True)
        
        meeting_info = agenda_data.get('meeting_info', {})
        meeting_date = meeting_info.get('date', None)
        
        # If date not found in meeting_info, extract from filename
        if not meeting_date or meeting_date == 'N/A':
            import re
            # Try to extract from the JSON filename first
            # Pattern: "Agenda 01.9.2024_extracted.json"
            filename_match = re.search(r'Agenda[_ ](\d{1,2})\.(\d{1,2})\.(\d{4})', json_path.name)
            if filename_match:
                month = filename_match.group(1).zfill(2)
                day = filename_match.group(2).zfill(2)
                year = filename_match.group(3)
                meeting_date = f"{month}.{day}.{year}"
                log.info(f"📅 Extracted date from filename: {meeting_date}")
            else:
                # Last resort: check the original PDF name in agenda_data
                source_file = agenda_data.get('source_file', '')
                pdf_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', source_file)
                if pdf_match:
                    month = pdf_match.group(1).zfill(2)
                    day = pdf_match.group(2).zfill(2)
                    year = pdf_match.group(3)
                    meeting_date = f"{month}.{day}.{year}"
                    log.info(f"📅 Extracted date from source file: {meeting_date}")
                else:
                    meeting_date = 'unknown'
                    log.warning("⚠️ Could not extract meeting date from any source")
        
        # Convert to underscore format for filename
        if meeting_date != 'unknown':
            meeting_date_filename = meeting_date.replace('.', '_')
        else:
            meeting_date_filename = 'unknown'
        
        # Build comprehensive header
        header = self._build_agenda_header(agenda_data)
        
        # Add detailed agenda items section
        items_section = self._build_agenda_items_section(agenda_data)
        
        # Combine with full text
        full_content = header + items_section + "\n\n# FULL AGENDA TEXT\n\n" + agenda_data.get('full_text', '')
        
        # Save markdown
        md_filename = f"agenda_{meeting_date_filename}.md"
        md_path = markdown_dir / md_filename
        
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(full_content)
        
        log.info(f"📝 Saved agenda markdown to: {md_path}")

    def _build_agenda_header(self, agenda_data: dict) -> str:
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

**ENTITIES IN THIS DOCUMENT:**
{self._format_agenda_entities(all_item_codes)}

**SEARCHABLE IDENTIFIERS:**
- DOCUMENT_TYPE: AGENDA
{self._format_item_identifiers(all_item_codes)}

---

"""
        return header

    def _format_agenda_entities(self, item_codes: list) -> str:
        """Format agenda item entities."""
        lines = []
        for code in item_codes[:10]:
            lines.append(f"- AGENDA_ITEM: {code}")
        if len(item_codes) > 10:
            lines.append(f"- ... and {len(item_codes) - 10} more items")
        return '\n'.join(lines)

    def _format_item_identifiers(self, item_codes: list) -> str:
        """Format item identifiers."""
        lines = []
        for code in item_codes:
            lines.append(f"- AGENDA_ITEM: {code}")
        return '\n'.join(lines)

    def _build_agenda_items_section(self, agenda_data: dict) -> str:
        """Build agenda items section."""
        lines = ["## AGENDA ITEMS QUICK REFERENCE\n"]
        
        for item in agenda_data.get('agenda_items', []):
            item_code = item.get('item_code', 'UNKNOWN')
            lines.append(f"### Agenda Item {item_code}")
            lines.append(f"**Title:** {item.get('title', 'N/A')}")
            lines.append(f"\n**What is Item {item_code}?**")
            lines.append(f"Item {item_code} is '{item.get('title', 'N/A')}'")
            lines.append("")
        
        return '\n'.join(lines)

    def _extract_meeting_info(self, pdf_path: Path, full_text: str) -> dict:
        """Extract meeting information from the agenda."""
        meeting_info = {
            'date': 'N/A',
            'time': 'N/A',
            'location': 'N/A'
        }
        
        # Try to extract date from filename first
        import re
        date_match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', pdf_path.name)
        if date_match:
            month, day, year = date_match.groups()
            meeting_info['date'] = f"{month}.{day}.{year}"
        
        # Try to extract time and location from text
        lines = full_text.split('\n')[:50]  # Check first 50 lines
        for line in lines:
            line = line.strip()
            # Look for time patterns
            time_match = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)', line, re.IGNORECASE)
            if time_match and meeting_info['time'] == 'N/A':
                meeting_info['time'] = time_match.group(1)
            
            # Look for location
            if 'city hall' in line.lower() or 'commission chamber' in line.lower():
                meeting_info['location'] = line[:100]  # Limit length
        
        return meeting_info
    
    def _extract_hyperlinks_pymupdf(self, pdf_path: Path) -> List[Dict[str, any]]:
        """Extract hyperlinks from PDF using PyMuPDF."""
        hyperlinks = []
        
        try:
            # Open PDF with PyMuPDF
            pdf_document = fitz.open(str(pdf_path))
            
            for page_num in range(len(pdf_document)):
                page = pdf_document[page_num]
                
                # Get all links on the page
                links = page.get_links()
                
                for link in links:
                    if link.get('uri'):  # External URL
                        # Get the link text by extracting text from the link rectangle
                        rect = fitz.Rect(link['from'])
                        link_text = page.get_text(clip=rect).strip()
                        
                        # Clean up the link text
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
            
            log.info(f"🔗 Extracted {len(hyperlinks)} hyperlinks from PDF")
            
        except Exception as e:
            log.error(f"Failed to extract hyperlinks with PyMuPDF: {e}")
        
        return hyperlinks
    
    def _associate_urls_with_items(self, items: List[Dict], hyperlinks: List[Dict], full_text: str) -> List[Dict]:
        """Associate extracted URLs with their corresponding agenda items."""
        # Create a mapping of items by their document reference
        items_by_ref = {}
        for item in items:
            if item.get('document_reference'):
                items_by_ref[item['document_reference']] = item
                # Initialize URLs list for each item
                item['urls'] = []
        
        # Try to associate URLs with items based on proximity and context
        for link in hyperlinks:
            # Strategy 1: Check if the link text contains a document reference
            for ref, item in items_by_ref.items():
                if ref in link.get('text', ''):
                    item['urls'].append({
                        'url': link['url'],
                        'text': link['text'],
                        'page': link['page']
                    })
                    log.info(f"🔗 Associated URL with item {item.get('item_code', 'Unknown')}: {link['url'][:50]}...")
                    break
            else:
                # Strategy 2: Check for item codes in the link text
                link_text = link.get('text', '').upper()
                for item in items:
                    item_code = item.get('item_code', '')
                    if item_code and item_code in link_text:
                        item['urls'].append({
                            'url': link['url'],
                            'text': link['text'],
                            'page': link['page']
                        })
                        log.info(f"🔗 Associated URL with item {item_code}: {link['url'][:50]}...")
                        break
        
        # Log summary of URL associations
        items_with_urls = sum(1 for item in items if item.get('urls'))
        total_urls_associated = sum(len(item.get('urls', [])) for item in items)
        log.info(f"📊 Associated {total_urls_associated} URLs with {items_with_urls} agenda items")
        
        return items
    
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
            
            prompt = """Extract ALL agenda items from this city council agenda document. Look for ALL these formats:

- Letter.-Number. Reference (e.g., H.-1. 23-6819)
- Letter-Number Reference (e.g., H-1 23-6819)
- Empty sections marked as "None"

IMPORTANT: 
1. Extract EVERY section even if it says "None"
2. Look for ALL item formats including H.-1., H.-2., etc.
3. Include items without explicit ordinance/resolution text

For EACH section/item found, extract:
1. section_name: The section name (e.g., "CITY MANAGER ITEMS")
2. item_code: The item code (e.g., "H-1") - normalize to Letter-Number format
3. document_reference: The reference number (e.g., "23-6819")
4. title: The full description
5. has_items: true if section has items, false if "None"

Return a JSON array including both sections and items.

Document text:
""" + chunk
            
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are an expert at extracting structured data from city government agenda documents. Return only valid JSON."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,
                    max_tokens=32768
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
                    manual_sections = self._manual_extract_items(chunk)
                    # Flatten sections to items for consistency
                    manual_items = []
                    for section in manual_sections:
                        manual_items.extend(section.get('items', []))
                    all_items.extend(manual_items)
                    log.info(f"Manual fallback extracted {len(manual_items)} items from {len(manual_sections)} sections")
                    
            except Exception as e:
                log.error(f"LLM extraction failed for chunk {i+1}: {e}")
                # Fallback to manual extraction
                manual_sections = self._manual_extract_items(chunk)
                # Flatten sections to items for consistency
                manual_items = []
                for section in manual_sections:
                    manual_items.extend(section.get('items', []))
                all_items.extend(manual_items)
                log.info(f"Manual fallback extracted {len(manual_items)} items from {len(manual_sections)} sections")
        
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
        sections = []
        current_section = None
        
        # Updated section patterns to catch all sections
        section_patterns = [
            (r'^([A-Z])\.\s+(.+)$', 'SECTION'),  # Letter. Section Name
            (r'^(CITY MANAGER ITEMS?)$', 'CITY_MANAGER'),
            (r'^(CITY ATTORNEY ITEMS?)$', 'CITY_ATTORNEY'),
            (r'^(BOARDS?/COMMITTEES? ITEMS?)$', 'BOARDS_COMMITTEES'),
            (r'^(PRESENTATIONS AND PROTOCOL DOCUMENTS)', 'PRESENTATIONS'),
            (r'^(APPROVAL OF MINUTES)', 'MINUTES'),
            (r'^(PUBLIC COMMENTS)', 'PUBLIC_COMMENTS'),
            (r'^(CONSENT AGENDA)', 'CONSENT'),
            (r'^(PUBLIC HEARINGS)', 'PUBLIC_HEARINGS'),
            (r'^(RESOLUTIONS)', 'RESOLUTIONS'),
            (r'^(ORDINANCES.*)', 'ORDINANCES'),
            (r'^(DISCUSSION ITEMS)', 'DISCUSSION'),
            (r'^(BOARDS AND COMMITTEES)', 'BOARDS'),
        ]
        
        # Track if we're in a section that might have "None" as content
        in_section = False
        section_content_lines = []
        
        lines = text.split('\n')
        for i, line in enumerate(lines):
            line_stripped = line.strip()
            
            # Skip empty lines
            if not line_stripped:
                continue
            
            # Check for section headers
            section_found = False
            for pattern, section_type in section_patterns:
                if re.match(pattern, line_stripped, re.IGNORECASE):
                    # Process previous section if it exists
                    if current_section:
                        # Check if section only contains "None"
                        content = ' '.join(section_content_lines).strip()
                        if content.lower() == 'none' or not current_section['items']:
                            current_section['has_items'] = False
                        sections.append(current_section)
                    
                    # Start new section
                    current_section = {
                        'section_name': line_stripped,
                        'section_type': section_type,
                        'items': [],
                        'has_items': True
                    }
                    section_content_lines = []
                    in_section = True
                    section_found = True
                    break
            
            if section_found:
                continue
            
            # Collect section content
            if in_section and current_section:
                section_content_lines.append(line_stripped)
                
            # Updated item patterns to handle multiline items
            if current_section and re.match(r'^[A-Z]\.-\d+\.?\s*$', line_stripped):
                # Item code on its own line
                item_code = line_stripped.strip()
                # Look ahead for document reference
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    doc_ref_match = re.match(r'^(\d{2}-\d{4,5})', next_line)
                    if doc_ref_match:
                        doc_ref = doc_ref_match.group(1)
                        # Get title from remaining text or next lines
                        title_start = i + 1
                        title_lines = []
                        for j in range(title_start, min(i + 5, len(lines))):
                            title_line = lines[j].strip()
                            if title_line and not re.match(r'^[A-Z]\.-\d+\.?', title_line):
                                title_lines.append(title_line)
                        
                        title = ' '.join(title_lines)
                        # Remove the document reference from title
                        title = title.replace(doc_ref, '').strip()
                        
                        current_section['items'].append({
                            'item_code': item_code.rstrip('.'),
                            'document_reference': doc_ref,
                            'title': title,
                            'item_type': self._determine_item_type(title, current_section['section_type'])
                        })
                        log.info(f"Extracted multiline item: {item_code.rstrip('.')} - {doc_ref}")
                        continue
            
            # Original item patterns for single-line items
            item_patterns = [
                r'^([A-Z]\.-\d+\.?)\s+(\d{2}-\d{4,5})\s+(.+)$',  # A.-1. 23-6764
                r'^(\d+\.-\d+\.?)\s+(\d{2}-\d{4,5})\s+(.+)$',    # 1.-1. 23-6797
                r'^([A-Z]-\d+)\s+(\d{2}-\d{4,5})\s+(.+)$',       # E-1 23-6784
            ]
            
            for pattern in item_patterns:
                match = re.match(pattern, line_stripped)
                if match and current_section:
                    item_code = match.group(1)
                    doc_ref = match.group(2)
                    title = match.group(3).strip()
                    
                    # Determine item type based on title or section
                    item_type = self._determine_item_type(title, current_section.get("section_type", ""))
                    
                    item = {
                        "item_code": item_code.rstrip('.'),
                        "document_reference": doc_ref,
                        "title": title[:300],
                        "item_type": item_type
                    }
                    
                    current_section["items"].append(item)
                    log.info(f"Extracted single-line {item_type}: {item_code} - {doc_ref}")
                    break
        
        # Don't forget the last section
        if current_section:
            # Check if section only contains "None"
            content = ' '.join(section_content_lines).strip()
            if content.lower() == 'none' or not current_section['items']:
                current_section['has_items'] = False
            sections.append(current_section)
        
        total_items = sum(len(s['items']) for s in sections)
        log.info(f"Manual extraction complete: {total_items} items in {len(sections)} sections")
        
        return sections
    
    def _determine_item_type(self, title: str, section_type: str) -> str:
        """Determine item type from title and section."""
        title_lower = title.lower()
        
        # Check title first for explicit type
        if 'an ordinance' in title_lower:
            return 'Ordinance'
        elif 'a resolution' in title_lower:
            return 'Resolution'
        elif 'proclamation' in title_lower:
            return 'Proclamation'
        elif 'recognition' in title_lower:
            return 'Recognition'
        elif 'congratulations' in title_lower:
            return 'Recognition'
        elif 'presentation' in title_lower:
            return 'Presentation'
        elif section_type == 'PRESENTATIONS':
            return 'Presentation'
        elif section_type == 'MINUTES':
            return 'Minutes Approval'
        elif section_type == 'CITY_MANAGER':
            return 'City Manager Item'
        elif section_type == 'CITY_ATTORNEY':
            return 'City Attorney Item'
        elif section_type == 'BOARDS_COMMITTEES':
            return 'Board/Committee Item'
        else:
            return 'Agenda Item'  # Generic fallback

    def _build_sections_from_items(self, extracted_data: List[Dict], full_text: str) -> List[Dict[str, str]]:
        """Build sections structure from extracted items or sections."""
        if not extracted_data:
            # If no items found, return the full document as one section
            return [{
                'title': 'Full Document',
                'text': full_text
            }]
        
        # Check if we have sections (from manual extraction) or items (from LLM)
        if extracted_data and isinstance(extracted_data[0], dict) and 'section_name' in extracted_data[0]:
            # We have sections from manual extraction
            sections = []
            for section_data in extracted_data:
                section_text_parts = []
                section_text_parts.append(f"=== {section_data['section_name']} ===\n")
                
                if section_data.get('has_items', True) and section_data.get('items'):
                    for item in section_data['items']:
                        item_text = f"{item['item_code']} - {item['document_reference']}\n{item['title']}\n"
                        section_text_parts.append(item_text)
                else:
                    section_text_parts.append("None\n")
                
                sections.append({
                    'title': section_data['section_name'],
                    'text': '\n'.join(section_text_parts)
                })
            
            return sections
        else:
            # We have items from LLM extraction - group them
            sections = []
            
            # Create agenda items section
            agenda_section_text = []
            for item in extracted_data:
                item_text = f"{item.get('item_code', 'Unknown')} - {item.get('document_reference', 'Unknown')}\n{item.get('title', 'Unknown')}\n"
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

    def _count_items(self, extracted_data: List[Dict]) -> int:
        """Count the number of items in extracted data (items or sections with items)."""
        if not extracted_data:
            return 0
        
        # Check if we have sections or items
        if extracted_data and isinstance(extracted_data[0], dict) and 'section_name' in extracted_data[0]:
            # We have sections - count items within them
            total_items = 0
            for section in extracted_data:
                total_items += len(section.get('items', []))
            return total_items
        else:
            # We have items directly
            return len(extracted_data) 