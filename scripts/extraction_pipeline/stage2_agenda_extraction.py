#!/usr/bin/env python3
"""
Stage 2: Agenda Item Extraction (LLM)
Intelligent extraction of agenda structure using LLM with regex fallbacks
"""

import logging
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Any
from groq import Groq

log = logging.getLogger(__name__)

class AgendaItemExtractor:
    """Stage 2: Extract agenda structure using LLM with comprehensive fallback patterns."""
    
    def __init__(self, output_dir: Path = Path("extracted_json")):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize Groq client
        self.client = Groq()
        self.model = "llama-3.3-70b-versatile"
        
        # Comprehensive regex patterns for fallback
        self.item_patterns = [
            # Primary patterns
            r'^([A-Z]\.-\d+\.?)\s+(\d{2}-\d{4,5})\s+(.+)$',  # A.-1. 23-6764
            r'^(\d+\.-\d+\.?)\s+(\d{2}-\d{4,5})\s+(.+)$',    # 1.-1. 23-6797
            r'^([A-Z]-\d+)\s+(\d{2}-\d{4,5})\s+(.+)$',       # E-1 23-6784
            # Alternative formats
            r'^([A-Z]\.\d+)\s+(\d{2}-\d{4,5})\s+(.+)$',      # A.1 23-6764
            r'^([A-Z]\d+)\s+(\d{2}-\d{4,5})\s+(.+)$',        # A1 23-6764
            # Without document reference
            r'^([A-Z]\.-\d+\.?)\s+(.+)$',                    # A.-1. Title only
            r'^([A-Z]-\d+)\s+(.+)$',                         # E-1 Title only
        ]
        
        # Section identification patterns
        self.section_patterns = [
            r'^([A-Z]\.)\s*(.+?)(?:\s*\(.*\))?\s*$',         # A. SECTION NAME
            r'^([A-Z])\.\s*(.+)$',                           # A. SECTION NAME
            r'^\d+\.\s*([A-Z]\.)\s*(.+)$',                   # 1. A. SECTION NAME
        ]
    
    def extract_agenda_structure(self, ocr_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract complete agenda structure from OCR data.
        
        Args:
            ocr_data: Output from Stage 1 (PDF OCR extraction)
            
        Returns:
            Structured agenda data with items, sections, and metadata
        """
        log.info(f"📋 Stage 2: Extracting agenda structure from {ocr_data['source_file']}")
        
        full_text = ocr_data["full_text"]
        
        # Extract meeting date
        meeting_date = self._extract_meeting_date(full_text, ocr_data["source_file"])
        
        try:
            # Primary: LLM extraction
            llm_result = self._extract_with_llm(full_text)
            
            # Enhance with hyperlink association
            enhanced_items = self._associate_hyperlinks(llm_result, ocr_data.get("hyperlinks", []))
            
            extraction_result = {
                "source_file": ocr_data["source_file"],
                "doc_id": ocr_data["doc_id"], 
                "meeting_date": meeting_date,
                "full_text": full_text,
                "sections": self._organize_into_sections(enhanced_items),
                "agenda_items": enhanced_items,
                "meeting_info": self._extract_meeting_info(full_text),
                "extraction_method": "llm_primary",
                "metadata": {
                    "stage": 2,
                    "extraction_timestamp": self._get_timestamp(),
                    "item_count": len(enhanced_items),
                    "hyperlink_associations": sum(1 for item in enhanced_items if item.get("urls"))
                }
            }
            
        except Exception as e:
            log.warning(f"⚠️  LLM extraction failed, using regex fallback: {e}")
            
            # Fallback: Regex extraction
            regex_items = self._extract_with_regex(full_text)
            enhanced_items = self._associate_hyperlinks(regex_items, ocr_data.get("hyperlinks", []))
            
            extraction_result = {
                "source_file": ocr_data["source_file"],
                "doc_id": ocr_data["doc_id"],
                "meeting_date": meeting_date, 
                "full_text": full_text,
                "sections": self._organize_into_sections(enhanced_items),
                "agenda_items": enhanced_items,
                "meeting_info": self._extract_meeting_info(full_text),
                "extraction_method": "regex_fallback",
                "metadata": {
                    "stage": 2,
                    "extraction_timestamp": self._get_timestamp(),
                    "item_count": len(enhanced_items),
                    "hyperlink_associations": sum(1 for item in enhanced_items if item.get("urls"))
                }
            }
        
        # Save extraction result
        output_file = self.output_dir / f"{ocr_data['source_file'].replace('.pdf', '')}_stage2_agenda.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(extraction_result, f, indent=2, ensure_ascii=False)
        
        log.info(f"✅ Stage 2 complete: {len(enhanced_items)} agenda items extracted")
        return extraction_result
    
    def _extract_with_llm(self, text: str) -> List[Dict[str, Any]]:
        """Extract agenda items using LLM with specialized prompt."""
        
        # Core extraction prompt - exactly as specified
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
"""
        
        # Process in chunks if text is too long
        max_chunk_size = 30000  # Increased context as specified
        chunks = [text[i:i+max_chunk_size] for i in range(0, len(text), max_chunk_size)]
        
        all_items = []
        
        for i, chunk in enumerate(chunks):
            log.debug(f"🤖 Processing chunk {i+1}/{len(chunks)}")
            
            messages = [
                {
                    "role": "system", 
                    "content": "You are an expert at extracting structured data from city government agenda documents. Return only valid JSON."
                },
                {
                    "role": "user",
                    "content": prompt + chunk
                }
            ]
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0,  # Deterministic
                max_tokens=8192
            )
            
            try:
                result_text = response.choices[0].message.content.strip()
                log.debug(f"🤖 LLM response: {result_text[:200]}...")
                
                # Parse JSON response
                chunk_items = self._parse_llm_response(result_text)
                all_items.extend(chunk_items)
                
            except Exception as e:
                log.error(f"❌ Failed to parse LLM response for chunk {i+1}: {e}")
                continue
        
        # Normalize and deduplicate items
        normalized_items = self._normalize_agenda_items(all_items)
        log.info(f"✅ LLM extracted {len(normalized_items)} agenda items")
        
        return normalized_items
    
    def _parse_llm_response(self, response_text: str) -> List[Dict[str, Any]]:
        """Parse LLM JSON response with robust error handling."""
        
        # Try to extract JSON from code blocks
        if '```json' in response_text:
            response_text = response_text.split('```json')[1].split('```')[0].strip()
        elif '```' in response_text:
            parts = response_text.split('```')
            if len(parts) >= 3:
                response_text = parts[1].strip()
        
        # Remove leading 'json' if present
        if response_text.startswith('json'):
            response_text = response_text[4:].strip()
        
        try:
            parsed_data = json.loads(response_text)
            
            # Handle different response formats
            if isinstance(parsed_data, list):
                return parsed_data
            elif isinstance(parsed_data, dict):
                if 'agenda_items' in parsed_data:
                    return parsed_data['agenda_items']
                elif 'items' in parsed_data:
                    return parsed_data['items']
                else:
                    return [parsed_data]  # Single item
            else:
                log.warning(f"⚠️  Unexpected LLM response format: {type(parsed_data)}")
                return []
                
        except json.JSONDecodeError as e:
            log.error(f"❌ JSON parse error: {e}")
            log.error(f"Raw response: {response_text}")
            return []
    
    def _extract_with_regex(self, text: str) -> List[Dict[str, Any]]:
        """Fallback regex extraction for agenda items."""
        log.info("🔍 Using regex fallback for agenda item extraction")
        
        items = []
        lines = text.split('\n')
        
        for line_num, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            
            # Try each pattern
            for pattern in self.item_patterns:
                match = re.match(pattern, line, re.IGNORECASE)
                if match:
                    groups = match.groups()
                    
                    if len(groups) == 3:  # item_code, doc_ref, title
                        item_code, doc_ref, title = groups
                    elif len(groups) == 2:  # item_code, title (no doc_ref)
                        item_code, title = groups
                        doc_ref = None
                    else:
                        continue
                    
                    # Normalize item code
                    normalized_code = self._normalize_item_code(item_code)
                    
                    items.append({
                        "item_code": normalized_code,
                        "document_reference": doc_ref,
                        "title": title.strip(),
                        "section_name": self._infer_section_name(normalized_code),
                        "extraction_method": "regex",
                        "source_line": line_num + 1
                    })
                    break
        
        log.info(f"✅ Regex extracted {len(items)} agenda items")
        return items
    
    def _normalize_item_code(self, code: str) -> str:
        """Normalize item codes to standard format (e.g., E-1)."""
        if not code:
            return ""
        
        # Remove dots and normalize format
        code = re.sub(r'([A-Z])\.(-)', r'\1\2', code)  # "E.-1" -> "E-1"
        code = re.sub(r'([A-Z])\.(\d)', r'\1-\2', code)  # "E.1" -> "E-1"
        code = re.sub(r'([A-Z])(\d)', r'\1-\2', code)   # "E1" -> "E-1"
        code = code.rstrip('.')  # Remove trailing dots
        
        return code.upper()
    
    def _infer_section_name(self, item_code: str) -> str:
        """Infer section name from item code."""
        if not item_code:
            return "UNKNOWN"
        
        letter = item_code[0] if item_code else "X"
        
        # Common section mappings
        section_map = {
            'A': 'PRESENTATIONS AND PROTOCOL DOCUMENTS',
            'B': 'PUBLIC COMMENT',
            'C': 'COMMITTEE BUSINESS',
            'D': 'CONSENT AGENDA',
            'E': 'ORDINANCES AND RESOLUTIONS',
            'F': 'ORDINANCES AND RESOLUTIONS',
            'G': 'CITY MANAGER ITEMS',
            'H': 'CITY MANAGER ITEMS',
            'I': 'MISCELLANEOUS'
        }
        
        return section_map.get(letter, f"SECTION {letter}")
    
    def _associate_hyperlinks(self, items: List[Dict], hyperlinks: List[Dict]) -> List[Dict]:
        """Associate hyperlinks with agenda items based on proximity and content."""
        
        for item in items:
            item_urls = []
            item_code = item.get("item_code", "")
            doc_ref = item.get("document_reference", "")
            
            for link in hyperlinks:
                link_text = link.get("text", "").strip()
                
                # Match by document reference
                if doc_ref and doc_ref in link_text:
                    item_urls.append(link)
                # Match by item code
                elif item_code and item_code in link_text:
                    item_urls.append(link)
                # Match by proximity (same page as item context)
                # This is simplified - could be enhanced with coordinate matching
                
            if item_urls:
                item["urls"] = item_urls
        
        return items
    
    def _organize_into_sections(self, items: List[Dict]) -> List[Dict]:
        """Organize agenda items into sections."""
        sections = {}
        
        for item in items:
            section_name = item.get("section_name", "UNKNOWN")
            
            if section_name not in sections:
                sections[section_name] = {
                    "section_name": section_name,
                    "items": [],
                    "item_count": 0
                }
            
            sections[section_name]["items"].append(item)
            sections[section_name]["item_count"] += 1
        
        return list(sections.values())
    
    def _extract_meeting_date(self, text: str, filename: str) -> str:
        """Extract meeting date from text or filename."""
        
        # Try filename first
        date_match = re.search(r'(\d{2})\.(\d{1,2})\.(\d{4})', filename)
        if not date_match:
            date_match = re.search(r'(\d{2})_(\d{1,2})_(\d{4})', filename)
        
        if date_match:
            month, day, year = date_match.groups()
            return f"{month.zfill(2)}.{day.zfill(2)}.{year}"
        
        # Try text patterns
        date_patterns = [
            r'(\w+)\s+(\d{1,2}),?\s+(\d{4})',  # January 9, 2024
            r'(\d{1,2})/(\d{1,2})/(\d{4})',    # 1/9/2024
            r'(\d{1,2})-(\d{1,2})-(\d{4})',    # 1-9-2024
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text)
            if match:
                # Convert to standard format
                return self._normalize_date(match.groups())
        
        return "unknown"
    
    def _extract_meeting_info(self, text: str) -> Dict[str, Any]:
        """Extract basic meeting information."""
        
        # Extract meeting time
        time_match = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)', text, re.IGNORECASE)
        meeting_time = time_match.group(1) if time_match else "Unknown"
        
        # Extract location
        location_patterns = [
            r'(?:Commission\s+)?Chambers?',
            r'City\s+Hall',
            r'(?:Meeting\s+)?Room\s+\d+'
        ]
        
        location = "Unknown"
        for pattern in location_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                location = match.group(0)
                break
        
        return {
            "time": meeting_time,
            "location": location,
            "type": "Regular Meeting"  # Could be enhanced with detection
        }
    
    def _normalize_agenda_items(self, items: List[Dict]) -> List[Dict]:
        """Normalize and deduplicate agenda items."""
        seen_codes = set()
        normalized_items = []
        
        for item in items:
            item_code = item.get("item_code", "")
            if not item_code:
                continue
            
            normalized_code = self._normalize_item_code(item_code)
            
            # Skip duplicates
            if normalized_code in seen_codes:
                continue
                
            seen_codes.add(normalized_code)
            
            # Update item with normalized code
            item["item_code"] = normalized_code
            
            # Ensure required fields
            if not item.get("section_name"):
                item["section_name"] = self._infer_section_name(normalized_code)
            
            if not item.get("title"):
                item["title"] = f"Agenda Item {normalized_code}"
            
            normalized_items.append(item)
        
        return normalized_items
    
    def _normalize_date(self, date_parts: tuple) -> str:
        """Normalize date to MM.DD.YYYY format."""
        try:
            if len(date_parts) == 3:
                # Handle different formats
                part1, part2, part3 = date_parts
                
                # If first part is month name
                if part1.isalpha():
                    month_map = {
                        'january': '01', 'february': '02', 'march': '03', 'april': '04',
                        'may': '05', 'june': '06', 'july': '07', 'august': '08',
                        'september': '09', 'october': '10', 'november': '11', 'december': '12'
                    }
                    month = month_map.get(part1.lower(), '01')
                    day = str(part2).zfill(2)
                    year = str(part3)
                else:
                    # Numeric format
                    month = str(part1).zfill(2)
                    day = str(part2).zfill(2)
                    year = str(part3)
                
                return f"{month}.{day}.{year}"
        except:
            pass
        
        return "unknown"
    
    def _get_timestamp(self) -> str:
        """Get current timestamp."""
        from datetime import datetime
        return datetime.now().isoformat()

def main():
    """Example usage of Stage 2 agenda extraction."""
    logging.basicConfig(level=logging.INFO)
    
    extractor = AgendaItemExtractor()
    
    print("🚀 Stage 2: Agenda Item Extractor ready!")
    print("✅ Features:")
    print("  - LLM-powered structure extraction using Groq LLaMA")
    print("  - Comprehensive regex fallback patterns")
    print("  - Intelligent hyperlink association")
    print("  - Section organization and normalization")
    print("  - Robust item code normalization")

if __name__ == "__main__":
    main() 