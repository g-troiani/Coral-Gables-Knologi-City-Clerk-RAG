"""
Verbatim Transcript Linker
Links verbatim transcript documents to their corresponding agenda items.
Handles various transcript types including individual items, item ranges, and public comments.
"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Set
import json
from datetime import datetime
import PyPDF2
import os

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
        
        # Process each transcript file
        for transcript_path in transcript_files:
            try:
                transcript_info = await self._process_transcript(transcript_path, meeting_date)
                if transcript_info:
                    # Categorize based on transcript type
                    if transcript_info['transcript_type'] == 'public_comment':
                        linked_transcripts['public_comments'].append(transcript_info)
                    elif transcript_info['transcript_type'] == 'section':
                        linked_transcripts['section_transcripts'].append(transcript_info)
                    else:
                        linked_transcripts['item_transcripts'].append(transcript_info)
                        
            except Exception as e:
                log.error(f"Error processing transcript {transcript_path.name}: {e}")
        
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
        """Process a single transcript file and extract item references."""
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
            
            # Extract text from PDF (first few pages for context)
            text_excerpt = self._extract_pdf_text(transcript_path, max_pages=3)
            
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
                "page_count": self._get_pdf_page_count(transcript_path),
                "text_excerpt": text_excerpt[:500] if text_excerpt else ""
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
        
        # Special case: Meeting Minutes or other general labels
        if re.search(r'meeting\s+minutes', item_info, re.IGNORECASE):
            result['item_codes'].append('MEETING_MINUTES')
            return result
        
        # Special case: Full meeting transcript
        if re.search(r'public|full\s+meeting', item_info, re.IGNORECASE) and not re.search(r'comment', item_info, re.IGNORECASE):
            result['item_codes'].append('FULL_MEETING')
            return result
        
        # Special case: Public Comment
        if re.search(r'public\s+comment', item_info, re.IGNORECASE):
            result['item_codes'].append('PUBLIC_COMMENT')
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
    
    def _extract_pdf_text(self, pdf_path: Path, max_pages: int = 3) -> str:
        """Extract text from first few pages of PDF."""
        try:
            with open(pdf_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                text_parts = []
                
                # Extract text from first few pages
                pages_to_read = min(len(reader.pages), max_pages)
                for i in range(pages_to_read):
                    text = reader.pages[i].extract_text()
                    if text:
                        text_parts.append(text)
                
                return "\n".join(text_parts)
        except Exception as e:
            log.error(f"Failed to extract text from {pdf_path.name}: {e}")
            return ""
    
    def _get_pdf_page_count(self, pdf_path: Path) -> int:
        """Get total page count of PDF."""
        try:
            with open(pdf_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                return len(reader.pages)
        except:
            return 0
    
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