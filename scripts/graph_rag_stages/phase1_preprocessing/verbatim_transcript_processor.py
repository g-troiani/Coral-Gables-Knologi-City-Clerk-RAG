#!/usr/bin/env python3
"""
Verbatim Transcript Processor - Deterministic Filename-Based Extraction

This module implements a hierarchical document structure extraction approach
specifically designed for verbatim meeting transcripts. Unlike LLM-based
approaches, it uses deterministic filename parsing for 100% accuracy.

Key Features:
- Filename pattern matching for item code extraction
- Hierarchical relationship creation (Meeting → Section → Item → Transcript)
- Multiple format support (single items, groups, special cases)
- Rich metadata generation for search optimization
"""

import re
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

from .stage1_pdf_ocr import PDFOCRExtractor

log = logging.getLogger(__name__)


class VerbatimTranscriptProcessor:
    """Processes verbatim transcripts using deterministic filename parsing."""
    
    def __init__(self, output_dir: Path = Path("city_clerk_documents/extracted_json")):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
        self.pdf_extractor = PDFOCRExtractor(output_dir)
        
    async def process_verbatim_transcripts(self, base_dir: Path, meeting_date: str) -> Dict[str, Any]:
        """
        Process all verbatim transcripts for a specific meeting date.
        
        Args:
            base_dir: Base directory containing transcript subdirectories
            meeting_date: Meeting date in format "01.09.2024"
            
        Returns:
            Dictionary containing processed transcript data and relationships
        """
        log.info(f"🎤 Processing verbatim transcripts for meeting: {meeting_date}")
        
        # Discover transcript files
        transcript_files = self._discover_transcript_files(base_dir, meeting_date)
        
        if not transcript_files:
            log.warning(f"No verbatim transcripts found for {meeting_date}")
            return self._empty_result(meeting_date)
        
        log.info(f"📄 Found {len(transcript_files)} transcript files")
        
        # Process each transcript
        processed_transcripts = []
        hierarchical_relationships = []
        
        for transcript_path in transcript_files:
            try:
                result = await self._process_single_transcript(transcript_path, meeting_date)
                if result:
                    processed_transcripts.append(result['transcript_data'])
                    hierarchical_relationships.extend(result['relationships'])
                    
            except Exception as e:
                log.error(f"❌ Failed to process {transcript_path.name}: {e}")
                continue
        
        # Build comprehensive result
        result = {
            "meeting_date": meeting_date,
            "document_type": "verbatim_transcript_collection",
            "transcripts": processed_transcripts,
            "hierarchical_relationships": hierarchical_relationships,
            "summary": self._build_summary(processed_transcripts),
            "metadata": {
                "extraction_method": "filename_parsing_ocr",
                "total_files": len(transcript_files),
                "processed_files": len(processed_transcripts),
                "extracted_at": datetime.now().isoformat()
            }
        }
        
        # Save comprehensive result
        self._save_transcript_collection(result, meeting_date)
        
        log.info(f"✅ Processed {len(processed_transcripts)} verbatim transcripts")
        return result
    
    def _discover_transcript_files(self, base_dir: Path, meeting_date: str) -> List[Path]:
        """Discover transcript files using filename patterns."""
        # Convert date format: "01.09.2024" -> "01_09_2024"
        date_underscore = meeting_date.replace(".", "_")
        
        # Search patterns - include year subdirectories
        search_dirs = [
            base_dir / "Verbating Items",
            base_dir / "Verbatim Items", 
            base_dir / "Transcripts",
            base_dir / "Verbating Items" / "2024",
            base_dir / "Verbatim Items" / "2024"
        ]
        
        patterns = [
            f"{date_underscore}*Verbatim*.pdf",
            f"{date_underscore} - Verbatim*.pdf",
            f"*{date_underscore}*Verbatim*.pdf"
        ]
        
        transcript_files = []
        for search_dir in search_dirs:
            if search_dir.exists():
                for pattern in patterns:
                    matches = list(search_dir.rglob(pattern))
                    transcript_files.extend(matches)
        
        # Remove duplicates and sort
        return sorted(list(set(transcript_files)))
    
    async def _process_single_transcript(self, transcript_path: Path, meeting_date: str) -> Optional[Dict[str, Any]]:
        """Process a single transcript file."""
        log.info(f"📝 Processing: {transcript_path.name}")
        
        # Enhanced date handling with fallback
        if not meeting_date:
            # Try to extract from filename
            date_match = re.search(r'(\d{2})_(\d{2})_(\d{4})', transcript_path.name)
            if date_match:
                month, day, year = date_match.groups()
                meeting_date = f"{month}.{day}.{year}"
                log.warning(f"Missing meeting_date parameter, extracted '{meeting_date}' from filename")
            else:
                log.error(f"Cannot process transcript without meeting_date: {transcript_path.name}")
                return None
        
        # Safe replace operation
        safe_meeting_date = meeting_date.replace('.', '-') if meeting_date else 'unknown'
        
        # Parse filename for item codes
        parsed_info = self._parse_transcript_filename(transcript_path.name, meeting_date)
        if not parsed_info:
            log.warning(f"Could not parse filename: {transcript_path.name}")
            return None
        
        # Extract full text using OCR
        log.info(f"🔍 Running OCR on transcript: {transcript_path.name}")
        ocr_result = self.pdf_extractor.extract_pdf(transcript_path)
        
        # Add check for text
        text = ocr_result.get('full_text')
        if text is None:
            log.error("Skipping transcript with None text")
            return None
        
        if not text:
            log.warning(f"No text extracted from {transcript_path.name}")
            return None
        
        # Build transcript data structure
        transcript_data = {
            "id": f"transcript-{safe_meeting_date}-{self._generate_transcript_id(parsed_info)}",
            "document_type": "verbatim_transcript",
            "source_file": transcript_path.name,
            "file_path": str(transcript_path),
            "meeting_date": meeting_date,
            "item_codes": parsed_info['item_codes'],
            "section_codes": parsed_info['section_codes'],
            "transcript_type": parsed_info['transcript_type'],
            "item_info_raw": parsed_info['item_info_raw'],
            "full_text": text,
            "pages": ocr_result['pages'],
            "agenda_item_ids": parsed_info['item_codes'],  # NEW: List of all parent agenda item IDs
            "primary_agenda_item_id": parsed_info['item_codes'][0] if parsed_info['item_codes'] else None,  # NEW: Primary (first) for single-item compatibility
            "metadata": {
                **ocr_result['metadata'],
                "filename_parsing": parsed_info,
                "extraction_method": "deterministic_filename_ocr",
                "hierarchical_structure": True
            }
        }
        
        # Generate hierarchical relationships
        relationships = self._create_hierarchical_relationships(transcript_data, meeting_date)
        
        # Save individual transcript
        self._save_individual_transcript(transcript_data)
        
        return {
            "transcript_data": transcript_data,
            "relationships": relationships
        }
    
    def _parse_transcript_filename(self, filename: str, meeting_date: str) -> Optional[Dict[str, Any]]:
        """Parse transcript filename to extract item codes and metadata."""
        # Primary filename pattern
        filename_pattern = re.compile(
            r'(\d{2})_(\d{2})_(\d{4})\s*-\s*Verbatim Transcripts?\s*-\s*(.+)\.pdf',
            re.IGNORECASE
        )
        
        match = filename_pattern.match(filename)
        if not match:
            log.warning(f"Filename does not match expected pattern: {filename}")
            return None
        
        month, day, year = match.groups()[:3]
        item_info = match.group(4).strip()
        
        # Verify date matches
        expected_date = f"{month}.{day}.{year}"
        if expected_date != meeting_date:
            log.warning(f"Date mismatch: filename has {expected_date}, expected {meeting_date}")
        
        # Parse item information
        parsed_items = self._parse_item_codes(item_info)
        
        return {
            "item_info_raw": item_info,
            "item_codes": parsed_items['item_codes'],
            "section_codes": parsed_items['section_codes'],
            "transcript_type": self._determine_transcript_type(item_info, parsed_items)
        }
    
    def _parse_item_codes(self, item_info: str) -> Dict[str, List[str]]:
        """Parse item codes from filename item information."""
        result = {"item_codes": [], "section_codes": []}
        
        # Special cases first
        if re.search(r'public\s+comment', item_info, re.IGNORECASE):
            result['section_codes'].append('PUBLIC_COMMENT')
            return result
        
        if re.search(r'meeting\s+minutes', item_info, re.IGNORECASE):
            result['item_codes'].append('MEETING_MINUTES')
            return result
        
        if re.search(r'full\s+meeting', item_info, re.IGNORECASE):
            result['item_codes'].append('FULL_MEETING')
            return result
        
        # Section-only cases (e.g., "K")
        if re.match(r'^[A-Z]\s*$', item_info.strip()):
            result['section_codes'].append(item_info.strip())
            return result
        
        # Multiple item formats
        
        # Format A: "AND" separated (F-7 and F-10)
        if ' and ' in item_info.lower():
            parts = re.split(r'\s+and\s+', item_info, flags=re.IGNORECASE)
            for part in parts:
                codes = self._extract_single_item_codes(part.strip())
                result['item_codes'].extend(codes)
        
        # Format B: Space separated (E-5 E-6 E-7 E-8)
        elif re.match(r'^([A-Z]-?\d+\s*)+$', item_info):
            items = item_info.split()
            for item in items:
                if re.match(r'^[A-Z]-?\d+$', item):
                    normalized = self._normalize_item_code(item)
                    if normalized:
                        result['item_codes'].append(normalized)
        
        # Format C: Comma separated (F-2, F-6, F-10)
        elif ',' in item_info:
            parts = item_info.split(',')
            for part in parts:
                codes = self._extract_single_item_codes(part.strip())
                result['item_codes'].extend(codes)
        
        # Format D: Single item
        else:
            codes = self._extract_single_item_codes(item_info)
            result['item_codes'].extend(codes)
        
        # Remove duplicates and sort
        result['item_codes'] = sorted(list(set(result['item_codes'])))
        result['section_codes'] = sorted(list(set(result['section_codes'])))
        
        return result
    
    def _extract_single_item_codes(self, text: str) -> List[str]:
        """Extract item codes from a single text segment."""
        codes = []
        
        patterns = [
            r'([A-Z])\.?\-?(\d+)\.?',  # Letter-based: E-1, E.1, E1
            r'(\d+)\-(\d+)'             # Number-based: 2-1, 3-2
        ]
        
        for pattern in patterns:
            for match in re.finditer(pattern, text):
                if pattern.startswith('(\\d'):  # Number-only pattern
                    codes.append(f"{match.group(1)}-{match.group(2)}")
                else:  # Letter-number pattern
                    letter = match.group(1)
                    number = match.group(2)
                    codes.append(f"{letter}-{number}")
        
        return codes
    
    def _normalize_item_code(self, code: str) -> str:
        """Normalize item code to consistent format."""
        code = code.strip('. ')
        
        # Letter-number pattern: E.-1. → E-1
        letter_match = re.match(r'^([A-Z])\.?\-?(\d+)\.?$', code)
        if letter_match:
            letter = letter_match.group(1)
            number = letter_match.group(2)
            return f"{letter}-{number}"
        
        # Number-number pattern: 2-1 → 2-1 (unchanged)
        number_match = re.match(r'^(\d+)\-(\d+)$', code)
        if number_match:
            return code
        
        return code
    
    def _determine_transcript_type(self, item_info: str, parsed_items: Dict) -> str:
        """Determine the type of transcript based on parsed information."""
        if 'PUBLIC_COMMENT' in parsed_items.get('section_codes', []):
            return 'public_comment'
        elif parsed_items.get('section_codes'):
            return 'section'
        elif len(parsed_items.get('item_codes', [])) > 3:
            return 'multi_item'
        elif len(parsed_items.get('item_codes', [])) == 1:
            return 'single_item'
        else:
            return 'item_group'
    
    def _generate_transcript_id(self, parsed_info: Dict[str, Any]) -> str:
        """Generate unique transcript ID."""
        if parsed_info['item_codes']:
            return '-'.join(parsed_info['item_codes'])
        elif parsed_info['section_codes']:
            return '-'.join(parsed_info['section_codes'])
        else:
            return 'unknown'
    
    def _create_hierarchical_relationships(self, transcript_data: Dict[str, Any], meeting_date: str) -> List[Dict[str, Any]]:
        """Create hierarchical relationships for the transcript."""
        relationships = []
        meeting_id = f"meeting-{meeting_date.replace('.', '-')}"
        transcript_id = transcript_data['id']
        
        # Create relationships for each item code
        for item_code in transcript_data['item_codes']:
            # Meeting → AgendaItem relationship
            agenda_item_id = f"agenda-item-{meeting_date.replace('.', '-')}-{item_code}"
            
            relationships.append({
                "source": meeting_id,
                "target": agenda_item_id,
                "relationship": "HAS_AGENDA_ITEM",
                "properties": {
                    "item_code": item_code,
                    "meeting_date": meeting_date
                }
            })
            
            # AgendaItem → Transcript relationship
            relationships.append({
                "source": agenda_item_id,
                "target": transcript_id,
                "relationship": "HAS_VERBATIM_TRANSCRIPT",
                "properties": {
                    "transcript_type": transcript_data['transcript_type'],
                    "page_count": len(transcript_data['pages']),
                    "filename": transcript_data['source_file']
                }
            })
        
        # Create relationships for section codes
        for section_code in transcript_data['section_codes']:
            section_id = f"section-{meeting_date.replace('.', '-')}-{section_code}"
            
            # Meeting → Section relationship
            relationships.append({
                "source": meeting_id,
                "target": section_id,
                "relationship": "HAS_SECTION",
                "properties": {
                    "section_code": section_code,
                    "meeting_date": meeting_date
                }
            })
            
            # Section → Transcript relationship
            relationships.append({
                "source": section_id,
                "target": transcript_id,
                "relationship": "HAS_VERBATIM_TRANSCRIPT",
                "properties": {
                    "transcript_type": transcript_data['transcript_type'],
                    "page_count": len(transcript_data['pages']),
                    "filename": transcript_data['source_file']
                }
            })
        
        return relationships
    
    def _build_summary(self, transcripts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build summary statistics for processed transcripts."""
        summary = {
            "total_transcripts": len(transcripts),
            "transcript_types": {},
            "total_item_codes": 0,
            "total_section_codes": 0,
            "total_pages": 0
        }
        
        for transcript in transcripts:
            # Count by type
            t_type = transcript['transcript_type']
            summary['transcript_types'][t_type] = summary['transcript_types'].get(t_type, 0) + 1
            
            # Count codes and pages
            summary['total_item_codes'] += len(transcript['item_codes'])
            summary['total_section_codes'] += len(transcript['section_codes'])
            summary['total_pages'] += len(transcript['pages'])
        
        return summary
    
    def _save_individual_transcript(self, transcript_data: Dict[str, Any]) -> None:
        """Save individual transcript data."""
        filename = f"{transcript_data['source_file'].replace('.pdf', '')}_verbatim_transcript.json"
        verbatim_dir = self.output_dir / "verbatim"
        verbatim_dir.mkdir(parents=True, exist_ok=True)
        output_path = verbatim_dir / filename
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(transcript_data, f, indent=2, ensure_ascii=False)
        
        log.debug(f"💾 Saved transcript: {output_path}")
    
    def _save_transcript_collection(self, result: Dict[str, Any], meeting_date: str) -> None:
        """Save the complete transcript collection."""
        filename = f"{meeting_date.replace('.', '_')}_verbatim_transcript_collection.json"
        verbatim_dir = self.output_dir / "verbatim"
        verbatim_dir.mkdir(parents=True, exist_ok=True)
        output_path = verbatim_dir / filename
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        log.info(f"💾 Saved transcript collection: {output_path}")
    
    def _empty_result(self, meeting_date: str) -> Dict[str, Any]:
        """Return empty result structure."""
        return {
            "meeting_date": meeting_date,
            "document_type": "verbatim_transcript_collection",
            "transcripts": [],
            "hierarchical_relationships": [],
            "summary": {"total_transcripts": 0},
            "metadata": {
                "extraction_method": "filename_parsing_ocr",
                "total_files": 0,
                "processed_files": 0,
                "extracted_at": datetime.now().isoformat()
            }
        } 