"""
Verbatim Transcript Linker
Links verbatim transcript documents to their corresponding agenda items.
Now with full OCR support for all pages.
"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Set
import json
from datetime import datetime
import PyPDF2
import os
import asyncio
import multiprocessing

# Import the PDF extractor for OCR support
from .pdf_extractor import PDFExtractor

log = logging.getLogger('verbatim_transcript_linker')


class VerbatimTranscriptLinker:
    """Links verbatim transcript documents to agenda items in the graph."""
    
    def __init__(self):
        """Initialize the verbatim transcript linker."""
        # Pattern to extract date and item info from filename
        self.filename_pattern = re.compile(
            r'(\d{2})_(\d{2})_(\d{4})\s*-\s*Verbatim Transcripts\s*-\s*(.+)\.pdf',
            re.IGNORECASE
        )
        
        # Debug directory for logging - ensure parent exists
        self.debug_dir = Path("city_clerk_documents/graph_json/debug/verbatim")
        self.debug_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize PDF extractor for OCR
        self.pdf_extractor = PDFExtractor(
            pdf_dir=Path("."),  # We'll use it file by file
            output_dir=Path("city_clerk_documents/extracted_text")
        )
    
    async def link_transcripts_for_meeting(self, 
                                         meeting_date: str,
                                         verbatim_dir: Path) -> Dict[str, List[Dict]]:
        """Find and link all verbatim transcripts for a specific meeting date."""
        log.info(f"🎤 Linking verbatim transcripts for meeting date: {meeting_date}")
        log.info(f"📁 Verbatim directory: {verbatim_dir}")
        
        # Debug logging for troubleshooting
        log.info(f"🔍 Looking for verbatim transcripts in: {verbatim_dir}")
        log.info(f"🔍 Directory exists: {verbatim_dir.exists()}")
        if verbatim_dir.exists():
            all_files = list(verbatim_dir.glob("*.pdf"))
            log.info(f"🔍 Total PDF files in directory: {len(all_files)}")
            if all_files:
                log.info(f"🔍 Sample files: {[f.name for f in all_files[:3]]}")
        
        # Convert meeting date format: "01.09.2024" -> "01_09_2024"
        date_underscore = meeting_date.replace(".", "_")
        
        # Initialize results
        linked_transcripts = {
            "item_transcripts": [],      # Transcripts for specific agenda items
            "public_comments": [],       # Public comment transcripts
            "section_transcripts": []    # Transcripts for entire sections
        }
        
        if not verbatim_dir.exists():
            log.warning(f"⚠️  Verbatim directory not found: {verbatim_dir}")
            return linked_transcripts
        
        # Find all transcript files for this date
        # Try multiple patterns to ensure we catch all files
        patterns = [
            f"{date_underscore}*Verbatim*.pdf",
            f"{date_underscore} - Verbatim*.pdf",
            f"*{date_underscore}*Verbatim*.pdf"
        ]

        transcript_files = []
        for pattern in patterns:
            files = list(verbatim_dir.glob(pattern))
            log.info(f"🔍 Pattern '{pattern}' found {len(files)} files")
            transcript_files.extend(files)

        # Remove duplicates
        transcript_files = list(set(transcript_files))
        
        log.info(f"📄 Found {len(transcript_files)} transcript files")
        
        if transcript_files:
            # Process transcripts in parallel
            max_concurrent = min(multiprocessing.cpu_count(), 8)
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def process_with_semaphore(transcript_path):
                async with semaphore:
                    return await self._process_transcript(transcript_path, meeting_date)
            
            # Process all transcripts concurrently
            results = await asyncio.gather(
                *[process_with_semaphore(path) for path in transcript_files],
                return_exceptions=True
            )
            
            # Categorize results
            for transcript_info, path in zip(results, transcript_files):
                if isinstance(transcript_info, Exception):
                    log.error(f"Error processing transcript {path.name}: {transcript_info}")
                    continue
                if transcript_info:
                    # Categorize based on transcript type
                    if transcript_info['transcript_type'] == 'public_comment':
                        linked_transcripts['public_comments'].append(transcript_info)
                    elif transcript_info['transcript_type'] == 'section':
                        linked_transcripts['section_transcripts'].append(transcript_info)
                    else:
                        linked_transcripts['item_transcripts'].append(transcript_info)
                    
                    # Save extracted text (can also be done in parallel)
                    self._save_extracted_text(path, transcript_info)
        
        # Save linked transcripts info for debugging
        self._save_linking_report(meeting_date, linked_transcripts)
        
        # Log summary
        total_linked = (len(linked_transcripts['item_transcripts']) + 
                       len(linked_transcripts['public_comments']) + 
                       len(linked_transcripts['section_transcripts']))
        
        log.info(f"✅ Verbatim transcript linking complete:")
        log.info(f"   🎤 Item transcripts: {len(linked_transcripts['item_transcripts'])}")
        log.info(f"   🎤 Public comments: {len(linked_transcripts['public_comments'])}")
        log.info(f"   🎤 Section transcripts: {len(linked_transcripts['section_transcripts'])}")
        log.info(f"   📄 Total linked: {total_linked}")
        
        return linked_transcripts
    
    async def _process_transcript(self, transcript_path: Path, meeting_date: str) -> Optional[Dict[str, Any]]:
        """Process a single transcript file with full OCR."""
        try:
            # Parse filename
            match = self.filename_pattern.match(transcript_path.name)
            if not match:
                log.warning(f"Could not parse transcript filename: {transcript_path.name}")
                return None
            
            month, day, year = match.groups()[:3]
            item_info = match.group(4).strip()
            
            # Parse item codes from the item info
            parsed_items = self._parse_item_codes(item_info)
            
            # Extract text from ALL pages using Docling OCR
            log.info(f"🔍 Running OCR on full transcript: {transcript_path.name}...")
            full_text, pages = self.pdf_extractor.extract_text_from_pdf(transcript_path)
            
            if not full_text:
                log.warning(f"No text extracted from {transcript_path.name}")
                return None
            
            log.info(f"✅ OCR extracted {len(full_text)} characters from {len(pages)} pages")
            
            # Determine transcript type and normalize item codes
            transcript_type = self._determine_transcript_type(item_info, parsed_items)
            
            transcript_info = {
                "path": str(transcript_path),
                "filename": transcript_path.name,
                "meeting_date": meeting_date,
                "item_info_raw": item_info,
                "item_codes": parsed_items['item_codes'],
                "section_codes": parsed_items['section_codes'],
                "transcript_type": transcript_type,
                "page_count": len(pages),
                "full_text": full_text,  # Store complete transcript text
                "pages": pages,          # Store page-level data
                "extraction_method": "docling_ocr"
            }
            
            log.info(f"📄 Processed transcript: {transcript_path.name}")
            log.info(f"   Items: {parsed_items['item_codes']}")
            log.info(f"   Type: {transcript_type}")
            
            return transcript_info
            
        except Exception as e:
            log.error(f"Error processing transcript {transcript_path.name}: {e}")
            return None
    
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
        
        # Special case: Meeting Minutes or other general labels
        if re.search(r'meeting\s+minutes', item_info, re.IGNORECASE):
            result['item_codes'].append('MEETING_MINUTES')
            return result
        
        # Special case: Full meeting transcript
        if re.search(r'public|full\s+meeting', item_info, re.IGNORECASE) and not re.search(r'comment', item_info, re.IGNORECASE):
            result['item_codes'].append('FULL_MEETING')
            return result
        
        # Special case: Discussion Items (K section)
        if re.match(r'^K\s*$', item_info.strip()):
            result['section_codes'].append('K')
            return result
        
        # Clean the item info
        item_info = item_info.strip()
        
        # Handle multiple items with "and" or "AND"
        # Examples: "F-7 and F-10", "2-1 AND 2-2"
        if ' and ' in item_info.lower():
            parts = re.split(r'\s+and\s+', item_info, flags=re.IGNORECASE)
            for part in parts:
                codes = self._extract_single_item_codes(part.strip())
                result['item_codes'].extend(codes)
        
        # Handle space-separated items
        # Examples: "E-5 E-6 E-7 E-8 E-9 E-10"
        elif re.match(r'^([A-Z]-?\d+\s*)+$', item_info):
            # Split by spaces and extract each item
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
        
        # Pattern for item codes: letter-number, letter.number, or just number-number
        # Handles: E-1, E1, E.-1., E.1, 2-1, etc.
        patterns = [
            r'([A-Z])\.?\-?(\d+)\.?',  # Letter-based items
            r'(\d+)\-(\d+)'             # Number-only items like 2-1
        ]
        
        for pattern in patterns:
            for match in re.finditer(pattern, text):
                if pattern.startswith('(\\d'):  # Number-only pattern
                    # For number-only, just use as is
                    codes.append(f"{match.group(1)}-{match.group(2)}")
                else:
                    # For letter-number format
                    letter = match.group(1)
                    number = match.group(2)
                    codes.append(f"{letter}-{number}")
        
        return codes
    
    def _normalize_item_code(self, code: str) -> str:
        """Normalize item code to consistent format (e.g., E-1, 2-1)."""
        # Remove dots and ensure dash format
        code = code.strip('. ')
        
        # Pattern: letter followed by optional punctuation and number
        letter_match = re.match(r'^([A-Z])\.?\-?(\d+)\.?$', code)
        if letter_match:
            letter = letter_match.group(1)
            number = letter_match.group(2)
            return f"{letter}-{number}"
        
        # Pattern: number-number format
        number_match = re.match(r'^(\d+)\-(\d+)$', code)
        if number_match:
            return code  # Already in correct format
        
        return code
    
    def _determine_transcript_type(self, item_info: str, parsed_items: Dict) -> str:
        """Determine the type of transcript based on parsed information."""
        if 'PUBLIC_COMMENT' in parsed_items['item_codes']:
            return 'public_comment'
        elif parsed_items['section_codes']:
            return 'section'
        elif len(parsed_items['item_codes']) > 3:
            return 'multi_item'
        elif len(parsed_items['item_codes']) == 1:
            return 'single_item'
        else:
            return 'item_group'
    
    def _save_linking_report(self, meeting_date: str, linked_transcripts: Dict):
        """Save detailed report of transcript linking."""
        report = {
            "meeting_date": meeting_date,
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_transcripts": sum(len(v) for v in linked_transcripts.values()),
                "item_transcripts": len(linked_transcripts["item_transcripts"]),
                "public_comments": len(linked_transcripts["public_comments"]),
                "section_transcripts": len(linked_transcripts["section_transcripts"])
            },
            "transcripts": linked_transcripts
        }
        
        report_filename = f"verbatim_linking_report_{meeting_date.replace('.', '_')}.json"
        report_path = self.debug_dir / report_filename
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        log.info(f"📊 Verbatim linking report saved to: {report_path}")
    
    def _validate_meeting_date(self, meeting_date: str) -> bool:
        """Validate meeting date format MM.DD.YYYY"""
        return bool(re.match(r'^\d{2}\.\d{2}\.\d{4}$', meeting_date))

    def _save_extracted_text(self, transcript_path: Path, transcript_info: Dict[str, Any]):
        """Save extracted transcript text to JSON for GraphRAG processing."""
        output_dir = Path("city_clerk_documents/extracted_text")
        output_dir.mkdir(exist_ok=True)
        
        # Create filename based on transcript info
        meeting_date = transcript_info['meeting_date'].replace('.', '_')
        item_info_clean = re.sub(r'[^a-zA-Z0-9-]', '_', transcript_info['item_info_raw'])
        output_filename = f"verbatim_{meeting_date}_{item_info_clean}_extracted.json"
        output_path = output_dir / output_filename
        
        # Prepare data for saving
        save_data = {
            "document_type": "verbatim_transcript",
            "meeting_date": transcript_info['meeting_date'],
            "item_codes": transcript_info['item_codes'],
            "section_codes": transcript_info['section_codes'],
            "transcript_type": transcript_info['transcript_type'],
            "full_text": transcript_info['full_text'],
            "pages": transcript_info['pages'],
            "metadata": {
                "filename": transcript_info['filename'],
                "item_info_raw": transcript_info['item_info_raw'],
                "page_count": transcript_info['page_count'],
                "extraction_method": "docling_ocr",
                "extracted_at": datetime.now().isoformat()
            }
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, indent=2, ensure_ascii=False)
        
        log.info(f"💾 Saved transcript text to: {output_path}")
        
        # NEW: Also save as markdown
        self._save_transcript_as_markdown(transcript_path, transcript_info, output_dir)

    def _save_transcript_as_markdown(self, transcript_path: Path, transcript_info: Dict[str, Any], output_dir: Path):
        """Save transcript as markdown with metadata header."""
        markdown_dir = output_dir.parent / "extracted_markdown"
        markdown_dir.mkdir(exist_ok=True)
        
        # Build header
        items_str = ', '.join(transcript_info['item_codes']) if transcript_info['item_codes'] else 'N/A'
        
        header = f"""---
DOCUMENT METADATA AND CONTEXT
=============================

**DOCUMENT IDENTIFICATION:**
- Full Path: Verbatim Items/{transcript_info['meeting_date'].split('.')[2]}/{transcript_path.name}
- Document Type: VERBATIM_TRANSCRIPT
- Filename: {transcript_path.name}

**PARSED INFORMATION:**
- Meeting Date: {transcript_info['meeting_date']}
- Agenda Items Discussed: {items_str}
- Transcript Type: {transcript_info['transcript_type']}
- Page Count: {transcript_info['page_count']}

**SEARCHABLE IDENTIFIERS:**
- MEETING_DATE: {transcript_info['meeting_date']}
- DOCUMENT_TYPE: VERBATIM_TRANSCRIPT
{self._format_item_identifiers(transcript_info['item_codes'])}

**NATURAL LANGUAGE DESCRIPTION:**
This is the verbatim transcript from the {transcript_info['meeting_date']} City Commission meeting covering the discussion of {self._describe_items(transcript_info)}.

**QUERY HELPERS:**
{self._build_transcript_query_helpers(transcript_info)}

---

{self._build_item_questions(transcript_info['item_codes'])}

# VERBATIM TRANSCRIPT CONTENT
"""
        
        # Combine with text
        full_content = header + "\n\n" + transcript_info.get('full_text', '')
        
        # Save file
        meeting_date = transcript_info['meeting_date'].replace('.', '_')
        item_info_clean = re.sub(r'[^a-zA-Z0-9-]', '_', transcript_info['item_info_raw'])
        md_filename = f"verbatim_{meeting_date}_{item_info_clean}.md"
        md_path = markdown_dir / md_filename
        
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(full_content)
        
        log.info(f"📝 Saved transcript markdown to: {md_path}")

    def _format_item_identifiers(self, item_codes: List[str]) -> str:
        """Format agenda items as searchable identifiers."""
        lines = []
        for item in item_codes:
            lines.append(f"- AGENDA_ITEM: {item}")
        return '\n'.join(lines)

    def _describe_items(self, transcript_info: Dict) -> str:
        """Create natural language description of items."""
        if transcript_info['item_codes']:
            if len(transcript_info['item_codes']) == 1:
                return f"agenda item {transcript_info['item_codes'][0]}"
            else:
                return f"agenda items {', '.join(transcript_info['item_codes'])}"
        elif 'PUBLIC_COMMENT' in transcript_info.get('section_codes', []):
            return "public comments section"
        else:
            return "the meeting proceedings"

    def _build_transcript_query_helpers(self, transcript_info: Dict) -> str:
        """Build query helpers for transcripts."""
        helpers = []
        for item in transcript_info.get('item_codes', []):
            helpers.append(f"- To find discussion about {item}, search for 'Item {item}' or '{item} discussion'")
        helpers.append(f"- To find all discussions from this meeting, search for '{transcript_info['meeting_date']}'")
        helpers.append("- This transcript contains the exact words spoken during the meeting")
        return '\n'.join(helpers)

    def _build_item_questions(self, item_codes: List[str]) -> str:
        """Build Q&A style entries for items."""
        questions = []
        for item in item_codes:
            questions.append(f"## What was discussed about Item {item}?")
            questions.append(f"The discussion of Item {item} is transcribed in this document.\n")
        return '\n'.join(questions) 