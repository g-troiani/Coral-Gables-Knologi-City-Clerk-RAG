"""
Agenda Ontology Extractor
========================
Extracts city administration ontology from agenda documents using LLM.
"""
import json
import logging
import re
from typing import Dict, List, Any

log = logging.getLogger(__name__)


class CityClerkOntologyExtractor:
    """Extract city administration ontology from agenda documents."""
    
    STANDARD_SECTIONS = [
        "Call to Order",
        "Invocation", 
        "Pledge of Allegiance",
        "Presentations and Protocol Documents",
        "Approval of Minutes",
        "Public Comments",
        "Consent Agenda",
        "Public Hearings",
        "Ordinances on Second Reading",
        "Resolutions",
        "City Commission Items",
        "Board/Committee Items",
        "City Manager Items",
        "City Attorney Items",
        "City Clerk Items",
        "Discussion Items",
        "Adjournment"
    ]
    
    def __init__(self, llm_client):
        self.llm = llm_client
        
    async def extract_agenda_ontology(self, agenda_data: Dict, filename: str) -> Dict:
        """Extract complete ontology from agenda document."""
        
        # Extract meeting date from filename
        meeting_date = self._extract_meeting_date(filename)
        
        # Prepare document text
        full_text = self._prepare_document_text(agenda_data)
        
        ontology = {
            'meeting_date': meeting_date,
            'filename': filename,
            'entities': {},
            'relationships': []
        }
        
        # 1. Extract meeting information
        meeting_info = await self._extract_meeting_info(full_text[:4000])
        ontology['meeting_info'] = meeting_info
        
        # 2. Extract hierarchical agenda structure with ALL items
        agenda_structure = await self._extract_complete_agenda_structure(full_text)
        ontology['agenda_structure'] = agenda_structure
        
        # 3. Extract all entities (people, organizations, locations, etc.)
        entities = await self._extract_entities(full_text)
        ontology['entities'] = entities
        
        # 4. Extract item codes and their metadata
        item_codes = await self._extract_item_codes_and_metadata(full_text)
        ontology['item_codes'] = item_codes
        
        # 5. Extract cross-references and relationships
        relationships = await self._extract_relationships(agenda_structure, item_codes)
        ontology['relationships'] = relationships
        
        return ontology
    
    def _extract_meeting_date(self, filename: str) -> str:
        """Extract date from filename 'Agenda M.DD.YYYY.pdf'"""
        date_match = re.search(r'Agenda\s+(\d{1,2})\.(\d{2})\.(\d{4})', filename)
        if date_match:
            month, day, year = date_match.groups()
            return f"{int(month):02d}.{day}.{year}"
        return "unknown"
    
    def _prepare_document_text(self, agenda_data: Dict) -> str:
        """Prepare clean document text."""
        sections_text = []
        for section in agenda_data.get('sections', []):
            text = section.get('text', '').strip()
            if text and not text.startswith('self_ref='):
                sections_text.append(text)
        return "\n\n".join(sections_text)
    
    def _extract_json_from_response(self, response_content: str) -> Any:
        """Extract JSON from LLM response, handling various formats."""
        if not response_content:
            log.error("Empty response from LLM")
            return None
            
        # Try direct JSON parsing first
        try:
            return json.loads(response_content)
        except json.JSONDecodeError:
            pass
        
        # Try to find JSON in markdown code blocks
        json_match = re.search(r'```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```', response_content, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass
        
        # Try to find raw JSON
        json_match = re.search(r'(\{.*\}|\[.*\])', response_content, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass
        
        log.error(f"Could not parse JSON from response: {response_content[:200]}...")
        return None
    
    async def _extract_meeting_info(self, text: str) -> Dict:
        """Extract detailed meeting information."""
        prompt = f"""Analyze this city commission meeting agenda and extract meeting details.

Text:
{text}

Return a JSON object with these fields:
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
        "commissioners": ["names"] or [],
        "city_attorney": "name or null",
        "city_manager": "name or null",
        "city_clerk": "name or null",
        "other_officials": []
    }},
    "key_topics": ["main topics"],
    "special_presentations": []
}}

Return ONLY the JSON object, no additional text."""

        try:
            response = self.llm.chat.completions.create(
                model=self.llm.deployment_name if hasattr(self.llm, 'deployment_name') else "gpt-4o",
                temperature=0.0,
                messages=[
                    {"role": "system", "content": "You are a municipal document analyzer. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            content = response.choices[0].message.content
            result = self._extract_json_from_response(content)
            return result or {"meeting_type": "unknown"}
            
        except Exception as e:
            log.error(f"Failed to extract meeting info: {e}")
            return {"meeting_type": "unknown"}
    
    async def _extract_complete_agenda_structure(self, text: str) -> List[Dict]:
        """Extract the complete hierarchical structure of the agenda."""
        # Process in smaller chunks to avoid token limits
        chunks = self._chunk_text(text, 8000)
        all_sections = []
        
        for i, chunk in enumerate(chunks):
            prompt = f"""Extract the agenda structure from this text.

Text:
{chunk}

Find ALL sections and items. Look for patterns like:
- Section headers (e.g., "CONSENT AGENDA", "PUBLIC HEARINGS")
- Item codes (E-1, F-12, G-2, etc.)
- Item titles and descriptions

Return a JSON array like this:
[
    {{
        "section_name": "Consent Agenda",
        "section_type": "CONSENT",
        "order": 1,
        "items": [
            {{
                "item_code": "E-1",
                "title": "Resolution approving...",
                "item_type": "Resolution",
                "document_reference": "2024-66",
                "sponsor": "Commissioner Name",
                "department": "Department Name",
                "summary": "Brief summary"
            }}
        ]
    }}
]

Return ONLY the JSON array."""

            try:
                response = self.llm.chat.completions.create(
                    model=self.llm.deployment_name if hasattr(self.llm, 'deployment_name') else "gpt-4o",
                    temperature=0.0,
                    max_tokens=4000,
                    messages=[
                        {"role": "system", "content": "Extract agenda structure. Return only valid JSON."},
                        {"role": "user", "content": prompt}
                    ]
                )
                
                content = response.choices[0].message.content
                sections = self._extract_json_from_response(content)
                if sections and isinstance(sections, list):
                    all_sections.extend(sections)
                    log.info(f"Extracted {len(sections)} sections from chunk {i+1}")
                    
            except Exception as e:
                log.error(f"Failed to parse chunk {i+1}: {e}")
        
        return self._merge_and_clean_sections(all_sections)
    
    async def _extract_entities(self, text: str) -> Dict[str, List[Dict]]:
        """Extract all named entities from the agenda."""
        chunks = self._chunk_text(text, 6000)
        all_entities = {
            'people': [],
            'organizations': [],
            'locations': [],
            'monetary_amounts': [],
            'dates': [],
            'legal_references': []
        }
        
        for chunk in chunks[:3]:  # Limit to first 3 chunks
            prompt = f"""Extract entities from this agenda text.

Text:
{chunk[:4000]}

Return JSON with:
{{
    "people": [{{"name": "John Smith", "role": "Mayor", "context": "presiding"}}],
    "organizations": [{{"name": "City Commission", "type": "government"}}],
    "locations": [{{"name": "City Hall", "address": "405 Biltmore Way"}}],
    "monetary_amounts": [{{"amount": "$100,000", "purpose": "budget"}}],
    "dates": [{{"date": "01/09/2024", "event": "meeting date"}}],
    "legal_references": [{{"type": "Resolution", "number": "2024-01"}}]
}}

Return ONLY the JSON object."""

            try:
                response = self.llm.chat.completions.create(
                    model=self.llm.deployment_name if hasattr(self.llm, 'deployment_name') else "gpt-4o",
                    temperature=0.0,
                    messages=[
                        {"role": "system", "content": "Extract entities. Return only valid JSON."},
                        {"role": "user", "content": prompt}
                    ]
                )
                
                content = response.choices[0].message.content
                entities = self._extract_json_from_response(content)
                if entities and isinstance(entities, dict):
                    # Merge with existing entities
                    for category, items in entities.items():
                        if category in all_entities and isinstance(items, list):
                            all_entities[category].extend(items)
                            
            except Exception as e:
                log.error(f"Failed to extract entities: {e}")
        
        # Deduplicate entities
        for category in all_entities:
            all_entities[category] = self._deduplicate_entities(all_entities[category])
        
        return all_entities
    
    async def _extract_item_codes_and_metadata(self, text: str) -> Dict[str, Dict]:
        """Extract all item codes and their associated metadata."""
        # Use regex to find item codes first
        item_codes = {}
        
        # Common patterns for item codes
        patterns = [
            r'\b([A-Z])-(\d+)\b',  # E-1, F-12
            r'\b([A-Z])(\d+)\b',   # E1, F12
            r'\b(\d+)-(\d+)\b',    # 2-1, 2-2
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                if pattern == r'\b(\d+)-(\d+)\b':
                    code = match.group(0)
                else:
                    code = f"{match.group(1)}-{match.group(2)}"
                
                if code not in item_codes:
                    # Extract context around the code
                    start = max(0, match.start() - 200)
                    end = min(len(text), match.end() + 500)
                    context = text[start:end]
                    
                    # Extract title from context
                    title_match = re.search(rf'{re.escape(code)}[:\s]+([^\n]+)', context)
                    title = title_match.group(1).strip() if title_match else "Unknown"
                    
                    item_codes[code] = {
                        "full_title": title,
                        "type": self._determine_item_type(context),
                        "context": context
                    }
        
        log.info(f"Found {len(item_codes)} item codes via regex")
        return item_codes
    
    def _determine_item_type(self, context: str) -> str:
        """Determine item type from context."""
        context_lower = context.lower()
        if 'resolution' in context_lower:
            return 'Resolution'
        elif 'ordinance' in context_lower:
            return 'Ordinance'
        elif 'contract' in context_lower:
            return 'Contract'
        elif 'proclamation' in context_lower:
            return 'Proclamation'
        elif 'report' in context_lower:
            return 'Report'
        else:
            return 'Item'
    
    async def _extract_relationships(self, agenda_structure: List[Dict], item_codes: Dict) -> List[Dict]:
        """Extract relationships between items."""
        relationships = []
        
        # Create sequential relationships
        all_items = []
        for section in agenda_structure:
            for item in section.get('items', []):
                all_items.append({
                    'code': item['item_code'],
                    'section': section['section_name']
                })
        
        # Sequential relationships within sections
        for i in range(len(all_items) - 1):
            if all_items[i]['section'] == all_items[i+1]['section']:
                relationships.append({
                    'from_code': all_items[i]['code'],
                    'to_code': all_items[i+1]['code'],
                    'relationship_type': 'FOLLOWS',
                    'description': 'Sequential items in same section',
                    'strength': 'strong'
                })
        
        return relationships
    
    def _chunk_text(self, text: str, chunk_size: int) -> List[str]:
        """Split text into overlapping chunks."""
        chunks = []
        overlap = 200
        
        for i in range(0, len(text), chunk_size - overlap):
            chunk = text[i:i + chunk_size]
            chunks.append(chunk)
        
        return chunks
    
    def _merge_and_clean_sections(self, sections: List[Dict]) -> List[Dict]:
        """Merge duplicate sections and ensure all items have codes."""
        merged = {}
        
        for section in sections:
            key = section.get('section_name', 'Unknown')
            
            if key not in merged:
                merged[key] = section
                merged[key]['items'] = section.get('items', [])
            else:
                # Merge items, avoiding duplicates
                existing_codes = {item.get('item_code', '') for item in merged[key].get('items', [])}
                
                for item in section.get('items', []):
                    if item.get('item_code') and item['item_code'] not in existing_codes:
                        merged[key]['items'].append(item)
        
        # Sort by order
        result = list(merged.values())
        result.sort(key=lambda x: x.get('order', 999))
        
        return result
    
    def _deduplicate_entities(self, entities: List[Dict]) -> List[Dict]:
        """Remove duplicate entities."""
        seen = set()
        unique = []
        
        for entity in entities:
            # Create a key based on the entity's main identifier
            if 'name' in entity:
                key = entity['name'].lower().strip()
            elif 'amount' in entity:
                key = entity['amount']
            elif 'date' in entity:
                key = entity['date']
            else:
                continue
            
            if key not in seen:
                seen.add(key)
                unique.append(entity)
        
        return unique 