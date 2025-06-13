"""
Processes verbatim transcript documents and links them to agenda items.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
import re
from datetime import datetime

from .pdf_extractor import PDFExtractor
from ..common.utils import sanitize_filename

log = logging.getLogger(__name__)


class TranscriptLinker:
    """Processes verbatim transcript documents and links them to agenda items."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.pdf_extractor = PDFExtractor()
        
        # Pattern to extract date and item info from filename
        self.filename_pattern = re.compile(
            r'(\d{2})_(\d{2})_(\d{4})\s*-\s*Verbatim Transcripts\s*-\s*(.+)\.pdf',
            re.IGNORECASE
        )

    async def extract_and_save_transcript(self, pdf_path: Path) -> None:
        """Extract and save a verbatim transcript with agenda item linking."""
        log.info(f"🎤 Processing transcript: {pdf_path.name}")
        
        # Parse filename to extract meeting info
        match = self.filename_pattern.match(pdf_path.name)
        if not match:
            log.warning(f"Could not parse transcript filename: {pdf_path.name}")
            # Fall back to generic processing
            await self._process_generic_transcript(pdf_path)
            return
        
        month, day, year = match.groups()[:3]
        item_info = match.group(4).strip()
        meeting_date = f"{month}.{day}.{year}"
        
        # Extract text using PDF extractor
        full_text, pages = self.pdf_extractor.extract_text_from_pdf(pdf_path)
        if not full_text:
            log.warning(f"No text extracted from {pdf_path.name}, skipping.")
            return

        # Parse item codes from filename
        parsed_items = self._parse_item_codes(item_info)
        
        # Determine transcript type
        transcript_type = self._determine_transcript_type(item_info, parsed_items)
        
        # Create transcript data structure
        transcript_data = {
            'source_file': pdf_path.name,
            'doc_id': self._generate_doc_id(pdf_path),
            'full_text': full_text,
            'meeting_date': meeting_date,
            'item_codes': parsed_items['item_codes'],
            'section_codes': parsed_items['section_codes'],
            'transcript_type': transcript_type,
            'item_info_raw': item_info,
            'metadata': {
                'extraction_method': 'docling',
                'num_pages': len(pages),
                'total_chars': len(full_text),
                'extraction_timestamp': datetime.now().isoformat()
            }
        }

        # Save as enriched markdown
        self._save_as_markdown(pdf_path, transcript_data)

    async def _process_generic_transcript(self, pdf_path: Path) -> None:
        """Process transcript with generic filename pattern."""
        # Extract text
        full_text, pages = self.pdf_extractor.extract_text_from_pdf(pdf_path)
        if not full_text:
            log.warning(f"No text extracted from {pdf_path.name}, skipping.")
            return

        # Try to extract date from filename
        date_match = re.search(r'(\d{2})[._](\d{2})[._](\d{4})', pdf_path.name)
        meeting_date = 'unknown'
        if date_match:
            month, day, year = date_match.groups()
            meeting_date = f"{month}.{day}.{year}"

        # Create basic transcript data
        transcript_data = {
            'source_file': pdf_path.name,
            'doc_id': self._generate_doc_id(pdf_path),
            'full_text': full_text,
            'meeting_date': meeting_date,
            'item_codes': [],
            'section_codes': [],
            'transcript_type': 'unknown',
            'item_info_raw': 'parsed from filename',
            'metadata': {
                'extraction_method': 'docling',
                'num_pages': len(pages),
                'total_chars': len(full_text),
                'extraction_timestamp': datetime.now().isoformat()
            }
        }

        # Save as enriched markdown
        self._save_as_markdown(pdf_path, transcript_data)

    def _parse_item_codes(self, item_info: str) -> Dict[str, List[str]]:
        """Parse item codes from the filename item info section."""
        result = {
            'item_codes': [],
            'section_codes': []
        }
        
        # Check for public comment first
        if re.search(r'public\s+comment', item_info, re.IGNORECASE):
            result['section_codes'].append('PUBLIC_COMMENT')
            return result
        
        # Special cases
        if re.search(r'meeting\s+minutes', item_info, re.IGNORECASE):
            result['item_codes'].append('MEETING_MINUTES')
            return result
        
        if re.search(r'public|full\s+meeting', item_info, re.IGNORECASE) and not re.search(r'comment', item_info, re.IGNORECASE):
            result['item_codes'].append('FULL_MEETING')
            return result
        
        # Handle multiple items with "and"
        if ' and ' in item_info.lower():
            parts = re.split(r'\s+and\s+', item_info, flags=re.IGNORECASE)
            for part in parts:
                codes = self._extract_single_item_codes(part.strip())
                result['item_codes'].extend(codes)
        
        # Handle space-separated items (e.g., "E-5 E-6 E-7")
        elif re.match(r'^([A-Z]-?\d+\s*)+$', item_info):
            items = item_info.split()
            for item in items:
                if re.match(r'^[A-Z]-?\d+$', item):
                    normalized = self._normalize_item_code(item)
                    if normalized:
                        result['item_codes'].append(normalized)
        
        # Handle comma-separated items
        elif ',' in item_info:
            parts = item_info.split(',')
            for part in parts:
                codes = self._extract_single_item_codes(part.strip())
                result['item_codes'].extend(codes)
        
        # Single item or other format
        else:
            codes = self._extract_single_item_codes(item_info)
            result['item_codes'].extend(codes)
        
        # Remove duplicates while preserving order
        result['item_codes'] = list(dict.fromkeys(result['item_codes']))
        result['section_codes'] = list(dict.fromkeys(result['section_codes']))
        
        return result

    def _extract_single_item_codes(self, text: str) -> List[str]:
        """Extract item codes from a single text segment."""
        codes = []
        
        # Patterns for item codes
        patterns = [
            r'([A-Z])\.?\-?(\d+)\.?',  # Letter-based items (E-1, E1, E.-1.)
            r'(\d+)\-(\d+)'             # Number-only items (2-1)
        ]
        
        for pattern in patterns:
            for match in re.finditer(pattern, text):
                if pattern.startswith('(\\d'):  # Number-only pattern
                    codes.append(f"{match.group(1)}-{match.group(2)}")
                else:
                    # Letter-number format
                    letter = match.group(1)
                    number = match.group(2)
                    codes.append(f"{letter}-{number}")
        
        return codes

    def _normalize_item_code(self, code: str) -> str:
        """Normalize item code to consistent format (e.g., E-1)."""
        code = code.strip('. ')
        
        # Letter-number pattern
        letter_match = re.match(r'^([A-Z])\.?\-?(\d+)\.?$', code)
        if letter_match:
            letter = letter_match.group(1)
            number = letter_match.group(2)
            return f"{letter}-{number}"
        
        # Number-number pattern
        number_match = re.match(r'^(\d+)\-(\d+)$', code)
        if number_match:
            return code  # Already in correct format
        
        return code

    def _determine_transcript_type(self, item_info: str, parsed_items: Dict) -> str:
        """Determine the type of transcript based on parsed information."""
        if 'PUBLIC_COMMENT' in parsed_items['section_codes']:
            return 'public_comment'
        elif parsed_items['section_codes']:
            return 'section'
        elif len(parsed_items['item_codes']) > 3:
            return 'multi_item'
        elif len(parsed_items['item_codes']) == 1:
            return 'single_item'
        else:
            return 'item_group'

    def _save_as_markdown(self, pdf_path: Path, transcript_data: Dict) -> None:
        """Save transcript as enriched markdown for GraphRAG."""
        # Build comprehensive header
        header = self._build_transcript_header(transcript_data)
        
        # Add questions section
        questions_section = self._build_item_questions(transcript_data['item_codes'])
        
        # Combine with full text
        full_content = header + questions_section + "\n\n# VERBATIM TRANSCRIPT CONTENT\n\n" + transcript_data.get('full_text', '')
        
        # Generate filename
        meeting_date = transcript_data['meeting_date'].replace('.', '_') if transcript_data['meeting_date'] != 'unknown' else 'unknown'
        item_info_clean = re.sub(r'[^a-zA-Z0-9-]', '_', transcript_data['item_info_raw'])
        
        filename = sanitize_filename(f"verbatim_{meeting_date}_{item_info_clean}.md")
        md_path = self.output_dir / filename
        
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(full_content)
        
        log.info(f"📝 Saved transcript markdown to: {md_path}")

    def _build_transcript_header(self, transcript_data: Dict) -> str:
        """Build comprehensive transcript header."""
        items_str = ', '.join(transcript_data['item_codes']) if transcript_data['item_codes'] else 'N/A'
        
        header = f"""---
DOCUMENT METADATA AND CONTEXT
=============================

**DOCUMENT IDENTIFICATION:**
- Document Type: VERBATIM_TRANSCRIPT
- Source File: {transcript_data.get('source_file', 'N/A')}
- Meeting Date: {transcript_data.get('meeting_date', 'N/A')}

**TRANSCRIPT DETAILS:**
- Agenda Items Discussed: {items_str}
- Transcript Type: {transcript_data.get('transcript_type', 'N/A')}
- Page Count: {transcript_data['metadata'].get('num_pages', 'N/A')}

**SEARCHABLE IDENTIFIERS:**
- DOCUMENT_TYPE: VERBATIM_TRANSCRIPT
- MEETING_DATE: {transcript_data.get('meeting_date', 'N/A')}
{self._format_item_identifiers(transcript_data['item_codes'])}

**NATURAL LANGUAGE DESCRIPTION:**
This is the verbatim transcript from the {transcript_data.get('meeting_date', 'unknown')} City Commission meeting covering the discussion of {self._describe_items(transcript_data)}.

**QUERY HELPERS:**
{self._build_transcript_query_helpers(transcript_data)}

---

"""
        return header

    def _format_item_identifiers(self, item_codes: List[str]) -> str:
        """Format agenda items as searchable identifiers."""
        lines = []
        for item in item_codes:
            lines.append(f"- AGENDA_ITEM: {item}")
        return '\n'.join(lines)

    def _describe_items(self, transcript_data: Dict) -> str:
        """Create natural language description of items."""
        if transcript_data['item_codes']:
            if len(transcript_data['item_codes']) == 1:
                return f"agenda item {transcript_data['item_codes'][0]}"
            else:
                return f"agenda items {', '.join(transcript_data['item_codes'])}"
        elif 'PUBLIC_COMMENT' in transcript_data.get('section_codes', []):
            return "public comments section"
        else:
            return "the meeting proceedings"

    def _build_transcript_query_helpers(self, transcript_data: Dict) -> str:
        """Build query helpers for transcripts."""
        helpers = []
        for item in transcript_data.get('item_codes', []):
            helpers.append(f"- To find discussion about {item}, search for 'Item {item}' or '{item} discussion'")
        helpers.append(f"- To find all discussions from this meeting, search for '{transcript_data.get('meeting_date', 'unknown')}'")
        helpers.append("- This transcript contains the exact words spoken during the meeting")
        return '\n'.join(helpers)

    def _build_item_questions(self, item_codes: List[str]) -> str:
        """Build Q&A style entries for items."""
        questions = ["## AGENDA ITEMS COVERED\n"]
        for item in item_codes:
            questions.append(f"### What was discussed about Item {item}?")
            questions.append(f"The discussion of Item {item} is transcribed in this document.\n")
        return '\n'.join(questions)

    def _generate_doc_id(self, pdf_path: Path) -> str:
        """Generate canonical document ID."""
        import hashlib
        return f"DOC_{hashlib.sha1(str(pdf_path.absolute()).encode()).hexdigest()[:12]}" 