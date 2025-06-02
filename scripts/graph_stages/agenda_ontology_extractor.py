"""
City Clerk Ontology Extractor - FIXED VERSION
Uses Groq LLM to extract structured data from city agenda documents.
"""

import logging
import json
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import os
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger('ontology_extractor')


class CityClerkOntologyExtractor:
    """Extract structured ontology from city clerk documents using LLM."""
    
    def __init__(self, 
                 groq_api_key: Optional[str] = None,
                 model: str = "qwen-qwq-32b",
                 output_dir: Optional[Path] = None,
                 max_tokens: int = 100000):
        """Initialize the extractor with Groq client."""
        self.api_key = groq_api_key or os.getenv("GROQ_API_KEY")
        if not self.api_key:
            raise ValueError("GROQ_API_KEY not found in environment")
        
        self.client = Groq(api_key=self.api_key)
        self.model = model
        self.max_tokens = max_tokens
        self.output_dir = output_dir or Path("city_clerk_documents/graph_json")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create debug directory
        self.debug_dir = self.output_dir / "debug"
        self.debug_dir.mkdir(exist_ok=True)
    
    def extract(self, pdf_path: Path) -> Dict[str, Any]:
        """Extract complete ontology from agenda PDF."""
        log.info(f"🧠 Extracting ontology from {pdf_path.name}")
        
        # First, load the extracted text
        extracted_path = self.output_dir / f"{pdf_path.stem}_extracted.json"
        if extracted_path.exists():
            with open(extracted_path, 'r') as f:
                extracted_data = json.load(f)
        else:
            raise FileNotFoundError(f"No extracted data found for {pdf_path.name}. Run PDF extraction first.")
        
        # Get full text from sections
        full_text = "\n".join(section.get("text", "") for section in extracted_data.get("sections", []))
        
        # Save full text for debugging
        with open(self.debug_dir / f"{pdf_path.stem}_full_text.txt", 'w', encoding='utf-8') as f:
            f.write(full_text)
        
        # Extract meeting date from filename first (more reliable)
        meeting_date = self._extract_meeting_date_from_filename(pdf_path.stem)
        if not meeting_date:
            meeting_date = self._extract_meeting_date(pdf_path.stem, full_text[:1000])
        
        log.info(f"📅 Extracted meeting date: {meeting_date}")
        
        # Step 1: Extract meeting information
        meeting_info = self._extract_meeting_info(full_text[:4000])
        
        # Step 2: Extract complete agenda structure
        agenda_structure = self._extract_agenda_structure(full_text)
        
        # Step 3: Extract entities
        entities = self._extract_entities(full_text[:15000])
        
        # Step 4: Extract relationships between items
        relationships = self._extract_relationships(agenda_structure)
        
        # Build complete ontology
        ontology = {
            "meeting_date": meeting_date,
            "meeting_info": meeting_info,
            "agenda_structure": agenda_structure,
            "entities": entities,
            "relationships": relationships,
            "hyperlinks": extracted_data.get("hyperlinks", {}),
            "metadata": {
                "source_pdf": str(pdf_path.absolute()),
                "extraction_date": datetime.utcnow().isoformat() + "Z",
                "model": self.model
            }
        }
        
        # Save ontology
        output_path = self.output_dir / f"{pdf_path.stem}_ontology.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(ontology, f, indent=2, ensure_ascii=False)
        
        log.info(f"✅ Ontology extraction complete: {len(agenda_structure)} sections, {sum(len(s.get('items', [])) for s in agenda_structure)} items")
        return ontology
    
    def _extract_meeting_date_from_filename(self, filename: str) -> Optional[str]:
        """Extract meeting date from filename like 'Agenda 01.9.2024'."""
        # Try to extract date from filename
        date_patterns = [
            r'(\d{1,2})\.(\d{1,2})\.(\d{4})',  # 01.9.2024
            r'(\d{1,2})-(\d{1,2})-(\d{4})',    # 01-9-2024
            r'(\d{1,2})_(\d{1,2})_(\d{4})',    # 01_9_2024
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, filename)
            if match:
                month, day, year = match.groups()
                return f"{month.zfill(2)}.{day.zfill(2)}.{year}"
        
        return None
    
    def _extract_meeting_date(self, filename: str, text: str) -> str:
        """Extract meeting date in MM.DD.YYYY format."""
        # Try filename first
        date = self._extract_meeting_date_from_filename(filename)
        if date:
            return date
        
        # Try MM/DD/YYYY format in text
        date_match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', text)
        if date_match:
            month, day, year = date_match.groups()
            return f"{month.zfill(2)}.{day.zfill(2)}.{year}"
        
        # Default fallback
        return "01.09.2024"  # Based on the actual filename
    
    def _clean_json_response(self, json_text: str) -> str:
        """Clean and fix common JSON formatting issues from LLM responses."""
        # Remove markdown code blocks
        json_text = re.sub(r'```json\s*', '', json_text)
        json_text = re.sub(r'\s*```', '', json_text)
        
        # Remove any text before the first { or [
        json_start = json_text.find('{')
        array_start = json_text.find('[')
        
        if json_start == -1 and array_start == -1:
            return json_text
        
        if json_start == -1:
            start_pos = array_start
        elif array_start == -1:
            start_pos = json_start
        else:
            start_pos = min(json_start, array_start)
        
        json_text = json_text[start_pos:]
        
        # Fix common escape issues
        json_text = json_text.replace('\\n', ' ')
        json_text = json_text.replace('\\"', '"')
        json_text = json_text.replace('\\/', '/')
        
        # Fix truncated strings by closing them
        # Count quotes to detect unclosed strings
        in_string = False
        escape_next = False
        cleaned_chars = []
        
        for i, char in enumerate(json_text):
            if escape_next:
                escape_next = False
                cleaned_chars.append(char)
                continue
                
            if char == '\\':
                escape_next = True
                cleaned_chars.append(char)
                continue
                
            if char == '"':
                in_string = not in_string
                
            cleaned_chars.append(char)
        
        # If we end while still in a string, close it
        if in_string:
            cleaned_chars.append('"')
            # Also close any open braces/brackets
            open_braces = cleaned_chars.count('{') - cleaned_chars.count('}')
            open_brackets = cleaned_chars.count('[') - cleaned_chars.count(']')
            
            if open_braces > 0:
                cleaned_chars.append('}' * open_braces)
            if open_brackets > 0:
                cleaned_chars.append(']' * open_brackets)
        
        return ''.join(cleaned_chars)
    
    def _parse_qwen_response(self, response_text: str) -> str:
        """Parse qwen response to extract content outside thinking tags."""
        # Remove thinking tags and their content
        thinking_pattern = r'<thinking>.*?</thinking>'
        cleaned_text = re.sub(thinking_pattern, '', response_text, flags=re.DOTALL)
        
        # Also remove any remaining XML-like tags that qwen might use
        cleaned_text = re.sub(r'<[^>]+>', '', cleaned_text)
        
        return cleaned_text.strip()
    
    def _extract_meeting_info(self, text: str) -> Dict[str, Any]:
        """Extract meeting metadata using LLM."""
        prompt = """Analyze this city commission meeting agenda and extract meeting details.

Text:
{text}

IMPORTANT: Return ONLY the JSON object below. Do not include any other text, markdown formatting, or code blocks.

{{
    "meeting_type": "Regular Meeting or Special Meeting or Workshop",
    "meeting_time": "time if mentioned",
    "location": {{
        "name": "venue name",
        "address": "full address"
    }},
    "officials_present": {{
        "mayor": "name or null",
        "vice_mayor": "name or null",
        "commissioners": ["names"],
        "city_attorney": "name or null",
        "city_manager": "name or null",
        "city_clerk": "name or null"
    }}
}}""".format(text=text)
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a JSON extractor. Return only valid JSON, no markdown or other formatting."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=self.max_tokens  # Use the configurable value
            )
            
            # Save LLM response for debugging
            raw_response = response.choices[0].message.content.strip()
            with open(self.debug_dir / "meeting_info_llm_response.txt", 'w', encoding='utf-8') as f:
                f.write(raw_response)
            
            # Clean and parse JSON
            json_text = self._clean_json_response(raw_response)
            result = json.loads(json_text)
            
            # Save parsed result
            with open(self.debug_dir / "meeting_info_parsed.json", 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2)
            
            return result
            
        except json.JSONDecodeError as e:
            log.error(f"JSON decode error in meeting info: {e}")
            log.error(f"Raw response saved to debug/meeting_info_llm_response.txt")
            return self._default_meeting_info()
        except Exception as e:
            log.error(f"Failed to extract meeting info: {e}")
            return self._default_meeting_info()
    
    def _extract_agenda_structure(self, text: str) -> List[Dict[str, Any]]:
        """Extract complete agenda structure with all items."""
        # Split text into smaller chunks to avoid token limits
        max_chunk_size = 30000  # characters
        
        if len(text) > max_chunk_size:
            # Process in chunks
            chunks = [text[i:i+max_chunk_size] for i in range(0, len(text), max_chunk_size-1000)]
            all_sections = []
            
            for i, chunk in enumerate(chunks):
                log.info(f"Processing chunk {i+1}/{len(chunks)} for agenda structure")
                sections = self._extract_agenda_structure_chunk(chunk, i)
                all_sections.extend(sections)
            
            return all_sections
        else:
            return self._extract_agenda_structure_chunk(text, 0)
    
    def _extract_agenda_structure_chunk(self, text: str, chunk_num: int) -> List[Dict[str, Any]]:
        """Extract agenda structure from a text chunk."""
        prompt = """Extract the agenda structure from this city commission agenda.

CRITICAL: Extract EVERY agenda item without missing any. Each item has a code like E.-1., E.-2., E.-3., etc.
Pay special attention to ensure NO items are skipped in the sequence.

Look for patterns like:
- "E.-1.    23-6784    An Ordinance..."
- "E.-2.    23-6785    An Ordinance..."
- "E.-3.    23-6786    A Resolution..."

Text:
{text}

Return ONLY a JSON array with ALL items. DO NOT SKIP ANY ITEMS IN THE SEQUENCE:
[
    {{
        "section_name": "RESOLUTIONS",
        "section_type": "RESOLUTION",
        "order": 1,
        "items": [
            {{
                "item_code": "E.-1.",
                "document_reference": "23-6784",
                "title": "Full title here",
                "item_type": "Ordinance"
            }},
            {{
                "item_code": "E.-2.",
                "document_reference": "23-6785",
                "title": "Full title here",
                "item_type": "Ordinance"
            }}
        ]
    }}
]""".format(text=text)
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Extract ALL agenda items. Do not skip any items in the sequence. If you see E-1 and E-3, look carefully for E-2."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=self.max_tokens
            )
            
            raw_response = response.choices[0].message.content.strip()
            
            # Save LLM response for debugging
            with open(self.debug_dir / f"agenda_structure_chunk{chunk_num}_llm_response.txt", 'w', encoding='utf-8') as f:
                f.write(raw_response)
            
            # Parse qwen response if using qwen model
            if 'qwen' in self.model.lower():
                json_text = self._parse_qwen_response(raw_response)
            else:
                json_text = self._clean_json_response(raw_response)
            
            # Try to parse
            try:
                agenda_structure = json.loads(json_text)
                
                # Validate for missing items in sequence
                all_items = []
                for section in agenda_structure:
                    all_items.extend(section.get('items', []))
                
                # Check for missing E items
                e_items = sorted([item['item_code'] for item in all_items if item['item_code'].startswith('E')])
                if e_items:
                    log.info(f"Found E-section items: {e_items}")
                    # Check for gaps
                    for i in range(len(e_items) - 1):
                        current = e_items[i]
                        next_item = e_items[i + 1]
                        # Extract numbers
                        current_num = int(re.search(r'\d+', current).group())
                        next_num = int(re.search(r'\d+', next_item).group())
                        if next_num - current_num > 1:
                            log.warning(f"⚠️  Gap detected: {current} -> {next_item}. Missing items in between!")
                
            except json.JSONDecodeError:
                # If parsing fails, try to extract items manually from the text
                log.warning(f"Failed to parse LLM response, extracting items manually")
                agenda_structure = self._extract_items_manually(text)
            
            # Save parsed result
            with open(self.debug_dir / f"agenda_structure_chunk{chunk_num}_parsed.json", 'w', encoding='utf-8') as f:
                json.dump(agenda_structure, f, indent=2)
            
            return agenda_structure
            
        except Exception as e:
            log.error(f"Failed to extract agenda structure chunk {chunk_num}: {e}")
            # Try manual extraction as fallback
            return self._extract_items_manually(text)
    
    def _extract_items_manually(self, text: str) -> List[Dict[str, Any]]:
        """Manually extract agenda items using regex patterns."""
        log.info("Attempting manual extraction of agenda items")
        
        sections = []
        
        # Split text into lines for easier processing
        lines = text.split('\n')
        
        current_section = {
            "section_name": "AGENDA ITEMS",
            "section_type": "GENERAL",
            "order": 1,
            "items": []
        }
        
        # Look for lines that match agenda item patterns
        for i, line in enumerate(lines):
            # Skip empty lines
            if not line.strip():
                continue
                
            # Check for section headers
            if re.match(r'^[A-Z][.\s]+(?:RESOLUTIONS?|ORDINANCES?|CITY COMMISSION ITEMS?)', line):
                # Save current section if it has items
                if current_section["items"]:
                    sections.append(current_section)
                
                # Start new section
                current_section = {
                    "section_name": line.strip(),
                    "section_type": self._determine_section_type(line),
                    "order": len(sections) + 1,
                    "items": []
                }
                continue
            
            # Pattern to match agenda items: "E.-9.    23-6825    Title..."
            # This pattern is flexible to handle variations
            item_match = re.match(
                r'^([A-Z]\.?-?\d+\.?)\s+(\d{2}-\d{4})\s+(.+)$',
                line.strip()
            )
            
            if item_match:
                item_code_raw = item_match.group(1)
                doc_ref = item_match.group(2)
                title = item_match.group(3).strip()
                
                # Normalize the item code to include dots and dashes consistently
                # E.9 -> E.-9.
                # E-9 -> E.-9.
                # E.-9 -> E.-9.
                # E.-9. -> E.-9. (no change)
                item_code = self._normalize_agenda_item_code(item_code_raw)
                
                # Determine item type
                item_type = "Item"
                if "resolution" in title.lower():
                    item_type = "Resolution"
                elif "ordinance" in title.lower():
                    item_type = "Ordinance"
                elif "update" in title.lower() or "discussion" in title.lower():
                    item_type = "Discussion"
                elif "presentation" in title.lower():
                    item_type = "Presentation"
                
                item = {
                    "item_code": item_code,
                    "document_reference": doc_ref,
                    "title": title[:300],  # Longer limit for titles
                    "item_type": item_type
                }
                
                current_section["items"].append(item)
                log.info(f"Extracted item: {item_code} - {doc_ref}")
        
        # Don't forget the last section
        if current_section["items"]:
            sections.append(current_section)
        
        total_items = sum(len(s['items']) for s in sections)
        log.info(f"Manual extraction complete: {total_items} items in {len(sections)} sections")
        
        return sections

    def _normalize_agenda_item_code(self, code: str) -> str:
        """Normalize agenda item code to consistent format for agenda display."""
        # Remove all spaces
        code = code.strip()
        
        # Ensure we have the letter part
        match = re.match(r'([A-Z])\.?-?(\d+)\.?', code)
        if match:
            letter = match.group(1)
            number = match.group(2)
            # Return in consistent format: "E.-9."
            return f"{letter}.-{number}."
        
        # If no match, return as is but ensure it ends with a dot
        if not code.endswith('.'):
            code += '.'
        return code

    def _determine_section_type(self, section_name: str) -> str:
        """Determine section type from section name."""
        section_name_upper = section_name.upper()
        if "RESOLUTION" in section_name_upper:
            return "RESOLUTION"
        elif "ORDINANCE" in section_name_upper:
            return "ORDINANCE"
        elif "COMMISSION" in section_name_upper:
            return "COMMISSION"
        elif "CONSENT" in section_name_upper:
            return "CONSENT"
        else:
            return "GENERAL"
    
    def _extract_entities(self, text: str) -> Dict[str, List[Dict[str, Any]]]:
        """Extract all entities mentioned in the document."""
        prompt = """Extract entities from this city agenda document.

Text:
{text}

IMPORTANT: Return ONLY the JSON object below. No markdown, no code blocks, no other text.

{{
    "people": [
        {{"name": "John Smith", "role": "Mayor", "context": "presiding"}}
    ],
    "organizations": [
        {{"name": "City Commission", "type": "government", "context": "governing body"}}
    ],
    "locations": [
        {{"name": "City Hall", "address": "405 Biltmore Way", "type": "government building"}}
    ],
    "monetary_amounts": [
        {{"amount": "$100,000", "purpose": "budget allocation", "context": "parks improvement"}}
    ],
    "dates": [
        {{"date": "01/23/2024", "event": "meeting date", "type": "meeting"}}
    ],
    "legal_references": [
        {{"type": "Resolution", "number": "2024-01", "title": "Budget Amendment"}}
    ]
}}""".format(text=text[:10000])  # Limit text to avoid token issues
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Extract entities. Return only JSON, no formatting."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=self.max_tokens  # Use the configurable value
            )
            
            raw_response = response.choices[0].message.content.strip()
            
            # Save LLM response for debugging
            with open(self.debug_dir / "entities_llm_response.txt", 'w', encoding='utf-8') as f:
                f.write(raw_response)
            
            # Clean and parse JSON
            json_text = self._clean_json_response(raw_response)
            entities = json.loads(json_text)
            
            # Save parsed result
            with open(self.debug_dir / "entities_parsed.json", 'w', encoding='utf-8') as f:
                json.dump(entities, f, indent=2)
            
            return entities
            
        except json.JSONDecodeError as e:
            log.error(f"JSON decode error in entities: {e}")
            log.error(f"Raw response saved to debug/entities_llm_response.txt")
            return self._default_entities()
        except Exception as e:
            log.error(f"Failed to extract entities: {e}")
            return self._default_entities()
    
    def _extract_relationships(self, agenda_structure: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract relationships between agenda items."""
        relationships = []
        
        # Find related items based on common references
        all_items = []
        for section in agenda_structure:
            for item in section.get("items", []):
                item["section"] = section.get("section_name")
                all_items.append(item)
        
        # Save all items for debugging
        with open(self.debug_dir / "all_agenda_items.json", 'w', encoding='utf-8') as f:
            json.dump(all_items, f, indent=2)
        
        # Look for items that reference each other
        for i, item1 in enumerate(all_items):
            for j, item2 in enumerate(all_items[i+1:], i+1):
                # Check if items share document references
                if (item1.get("document_reference") and 
                    item1.get("document_reference") == item2.get("document_reference")):
                    relationships.append({
                        "from_code": item1.get("item_code"),
                        "to_code": item2.get("item_code"),
                        "relationship_type": "REFERENCES_SAME_DOCUMENT",
                        "description": f"Both reference document {item1.get('document_reference')}"
                    })
        
        return relationships
    
    def _default_meeting_info(self) -> Dict[str, Any]:
        """Return default meeting info structure."""
        return {
            "meeting_type": "Regular Meeting",
            "meeting_time": "9:00 a.m.",
            "location": {
                "name": "City Hall, Commission Chambers",
                "address": "405 Biltmore Way, Coral Gables, FL 33134"
            },
            "officials_present": {
                "mayor": None,
                "vice_mayor": None,
                "commissioners": [],
                "city_attorney": None,
                "city_manager": None,
                "city_clerk": None
            }
        }
    
    def _default_entities(self) -> Dict[str, List]:
        """Return default empty entities structure."""
        return {
            "people": [],
            "organizations": [],
            "locations": [],
            "monetary_amounts": [],
            "dates": [],
            "legal_references": []
        } 