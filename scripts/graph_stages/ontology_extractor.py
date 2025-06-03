from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json
import logging
import re
from datetime import datetime
from groq import Groq
import os

log = logging.getLogger(__name__)

class OntologyExtractor:
    """Extract rich ontology from agenda data using LLM."""
    
    def __init__(self, output_dir: Optional[Path] = None):
        """Initialize the ontology extractor."""
        self.output_dir = output_dir or Path("city_clerk_documents/graph_json")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Debug directory for LLM responses
        self.debug_dir = Path("debug")
        self.debug_dir.mkdir(exist_ok=True)
        
        # Initialize Groq client
        self.client = Groq(api_key=os.getenv("GROQ_API_KEY"))
        # Use llama3 which is better at following JSON instructions
        self.model = "llama-3.1-70b-versatile"
    
    def extract_ontology(self, agenda_file: Path) -> Dict[str, any]:
        """Extract rich ontology from agenda data."""
        log.info(f"🧠 Extracting ontology from {agenda_file.name}")
        
        # Load extracted data (from docling)
        extracted_file = self.output_dir / f"{agenda_file.stem}_extracted.json"
        if not extracted_file.exists():
            log.error(f"❌ Extracted file not found: {extracted_file}")
            return self._create_empty_ontology(agenda_file.name)
        
        with open(extracted_file, 'r', encoding='utf-8') as f:
            agenda_data = json.load(f)
        
        full_text = agenda_data.get('full_text', '')
        
        # Extract meeting date
        meeting_date = self._extract_meeting_date(agenda_file.name)
        log.info(f"📅 Extracted meeting date: {meeting_date}")
        
        # Extract meeting info
        meeting_info = self._extract_meeting_info(full_text, meeting_date)
        
        # Extract sections and their items
        sections = self._extract_sections_with_items(full_text, agenda_data.get('agenda_items', []))
        
        # Extract entities from the entire document
        entities = self._extract_entities(full_text)
        
        # Extract relationships between entities
        relationships = self._extract_relationships(entities, sections)
        
        # Extract URLs using regex
        urls = self._extract_urls_regex(full_text)
        
        # Enhance agenda items with URLs
        for section in sections:
            for item in section.get('items', []):
                item['urls'] = self._find_urls_for_item(item, urls, full_text)
        
        # Build ontology
        ontology = {
            'source_file': agenda_file.name,
            'meeting_date': meeting_date,
            'meeting_info': meeting_info,
            'sections': sections,
            'entities': entities,
            'relationships': relationships,
            'metadata': {
                'extraction_method': 'llm+regex',
                'num_sections': len(sections),
                'num_items': sum(len(s.get('items', [])) for s in sections),
                'num_entities': len(entities),
                'num_relationships': len(relationships),
                'num_urls': len(urls)
            }
        }
        
        # Save ontology
        output_file = self.output_dir / f"{agenda_file.stem}_ontology.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(ontology, f, indent=2, ensure_ascii=False)
        
        log.info(f"✅ Ontology extraction complete: {len(sections)} sections, {sum(len(s.get('items', [])) for s in sections)} items")
        
        return ontology
    
    def _extract_meeting_date(self, filename: str) -> str:
        """Extract meeting date from filename."""
        # Pattern: Agenda MM.DD.YYYY.pdf or Agenda MM.D.YYYY.pdf
        date_pattern = r'Agenda\s+(\d{1,2})\.(\d{1,2})\.(\d{4})'
        match = re.search(date_pattern, filename)
        if match:
            month = match.group(1).zfill(2)
            day = match.group(2).zfill(2)
            year = match.group(3)
            return f"{month}.{day}.{year}"
        return "01.01.2024"  # Default fallback
    
    def _extract_meeting_info(self, text: str, meeting_date: str) -> Dict[str, any]:
        """Extract detailed meeting information using LLM."""
        prompt = f"""Extract meeting information from this city commission agenda. Find:

1. Meeting type (Regular, Special, Workshop)
2. Meeting time
3. Meeting location/venue
4. Commission members present (if listed)
5. City officials (Mayor, City Manager, City Attorney, City Clerk)

Return as JSON:
{{
  "type": "Regular Meeting",
  "time": "5:30 PM",
  "location": "City Commission Chambers",
  "commissioners": ["Name1", "Name2"],
  "officials": {{
    "mayor": "Mayor Name",
    "city_manager": "Manager Name",
    "city_attorney": "Attorney Name",
    "city_clerk": "Clerk Name"
  }}
}}

Text (first 3000 chars):
{text[:3000]}"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a JSON extraction assistant. You must return ONLY valid JSON with no additional text, explanations, or markdown formatting."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=1000
            )
            
            response_text = response.choices[0].message.content.strip()
            
            # Debug: save raw response
            debug_file = self.debug_dir / "meeting_info_response.txt"
            with open(debug_file, 'w') as f:
                f.write(response_text)
            
            response_text = self._clean_json_response(response_text)
            
            return json.loads(response_text)
            
        except Exception as e:
            log.error(f"Failed to extract meeting info: {e}")
            return {
                "type": "Regular Meeting",
                "time": "5:30 PM",
                "location": "City Commission Chambers",
                "commissioners": [],
                "officials": {}
            }
    
    def _extract_sections_with_items(self, text: str, extracted_items: List[Dict]) -> List[Dict[str, any]]:
        """Extract sections and organize items within them using LLM."""
        
        # First, get section structure from LLM
        prompt = f"""Identify all major sections in this city commission agenda. Common sections include:
- PRESENTATIONS AND PROCLAMATIONS
- CONSENT AGENDA  
- ORDINANCES ON FIRST READING
- ORDINANCES ON SECOND READING
- RESOLUTIONS
- CITY MANAGER REPORTS
- CITY ATTORNEY REPORTS
- GENERAL DISCUSSION

For each section found, provide:
1. section_name: The exact name as it appears
2. section_type: One of [presentations, consent, ordinances_first, ordinances_second, resolutions, reports, discussion, other]
3. description: Brief description of what this section contains

Return as JSON array:
[
  {{
    "section_name": "CONSENT AGENDA",
    "section_type": "consent",
    "description": "Items for routine approval"
  }}
]

Text (first 5000 chars):
{text[:5000]}"""

        sections = []
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Extract agenda sections. Return only valid JSON array."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=2000
            )
            
            response_text = response.choices[0].message.content.strip()
            response_text = self._clean_json_response(response_text)
            
            section_list = json.loads(response_text)
            
            # Now assign items to sections based on their location in text
            for section in section_list:
                section['items'] = []
                
                # Find items that belong to this section
                section_name = section['section_name']
                for item in extracted_items:
                    # Enhanced item with more details
                    enhanced_item = {
                        **item,
                        'section': section_name,
                        'description': '',
                        'sponsors': [],
                        'departments': [],
                        'actions': []
                    }
                    
                    # Try to find item in text and extract context
                    item_code = item.get('item_code', '')
                    if item_code:
                        # Find the item in the text and extract surrounding context
                        pattern = rf'{re.escape(item_code)}.*?(?=(?:[A-Z]\.-\d+\.|$))'
                        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
                        if match:
                            context = match.group(0)[:1000]  # Get context around item
                            
                            # Extract additional details using LLM
                            details = self._extract_item_details(item_code, context)
                            enhanced_item.update(details)
                    
                    # Assign to appropriate section based on item type or position
                    if item.get('item_type') == 'Ordinance':
                        if section['section_type'] in ['ordinances_first', 'ordinances_second']:
                            section['items'].append(enhanced_item)
                    elif item.get('item_type') == 'Resolution' and section['section_type'] == 'resolutions':
                        section['items'].append(enhanced_item)
                    elif section['section_type'] == 'consent' and item_code.startswith('D'):
                        section['items'].append(enhanced_item)
            
            sections = section_list
            
        except Exception as e:
            log.error(f"Failed to extract sections: {e}")
            # Fallback: create basic sections
            sections = self._create_basic_sections(extracted_items)
        
        return sections
    
    def _extract_item_details(self, item_code: str, context: str) -> Dict[str, any]:
        """Extract detailed information about a specific agenda item."""
        prompt = f"""Extract details for agenda item {item_code} from this context:

Find:
1. Full description/summary
2. Sponsoring commissioners or departments
3. Departments involved
4. Recommended actions (approve, deny, discuss, defer, etc.)
5. Key stakeholders mentioned

Return as JSON:
{{
  "description": "Brief description",
  "sponsors": ["Commissioner Name"],
  "departments": ["Planning", "Finance"],
  "actions": ["approve", "authorize"],
  "stakeholders": ["Organization name"]
}}

Context:
{context}"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a JSON extraction assistant. Return ONLY valid JSON with no additional text."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=1000
            )
            
            response_text = response.choices[0].message.content.strip()
            response_text = self._clean_json_response(response_text)
            
            return json.loads(response_text)
            
        except Exception as e:
            log.error(f"Failed to extract item details for {item_code}: {e}")
            return {
                "description": "",
                "sponsors": [],
                "departments": [],
                "actions": [],
                "stakeholders": []
            }
    
    def _extract_entities(self, text: str) -> List[Dict[str, any]]:
        """Extract all entities (people, organizations, departments) from the document."""
        # Process in chunks
        max_chars = 10000
        chunks = [text[i:i+max_chars] for i in range(0, len(text), max_chars)]
        
        all_entities = []
        seen_entities = set()
        
        for i, chunk in enumerate(chunks[:5]):  # Process first 5 chunks
            prompt = f"""Extract all named entities from this government document:

Find:
1. People (commissioners, officials, citizens)
2. Organizations (companies, non-profits, agencies)
3. Departments (city departments, divisions)
4. Locations (addresses, buildings, areas)

For each entity, determine:
- name: Full name
- type: person, organization, department, location
- role: Their role if mentioned (e.g., "Commissioner", "Director", "Applicant")
- context: Brief context where they appear

Return as JSON array:
[
  {{
    "name": "John Smith",
    "type": "person",
    "role": "Commissioner",
    "context": "Sponsoring ordinance E-1"
  }}
]

Text chunk {i+1}:
{chunk}"""

            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are a JSON extraction assistant. You must return ONLY a valid JSON array with no additional text, explanations, or markdown formatting."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,
                    max_tokens=2000
                )
                
                response_text = response.choices[0].message.content.strip()
                
                # Debug: save raw response
                debug_file = self.debug_dir / f"entities_response_chunk_{i}.txt"
                with open(debug_file, 'w') as f:
                    f.write(response_text)
                
                response_text = self._clean_json_response(response_text)
                
                entities = json.loads(response_text)
                if not isinstance(entities, list):
                    log.error(f"Expected list but got {type(entities)} for chunk {i+1}")
                    entities = []
                
                # Deduplicate
                for entity in entities:
                    entity_key = f"{entity.get('type', '')}:{entity.get('name', '').lower()}"
                    if entity_key not in seen_entities:
                        seen_entities.add(entity_key)
                        all_entities.append(entity)
                
            except Exception as e:
                log.error(f"Failed to extract entities from chunk {i+1}: {e}")
                # Try basic regex extraction as fallback
                chunk_entities = self._basic_entity_extraction(chunk)
                all_entities.extend(chunk_entities)
        
        return all_entities
    
    def _basic_entity_extraction(self, text: str) -> List[Dict[str, any]]:
        """Basic entity extraction using patterns as fallback."""
        entities = []
        
        # Extract commissioners/council members
        commissioner_pattern = r'Commissioner\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'
        for match in re.finditer(commissioner_pattern, text):
            entities.append({
                "name": match.group(1),
                "type": "person",
                "role": "Commissioner",
                "context": "City Commissioner"
            })
        
        # Extract Mayor
        mayor_pattern = r'Mayor\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'
        for match in re.finditer(mayor_pattern, text):
            entities.append({
                "name": match.group(1),
                "type": "person",
                "role": "Mayor",
                "context": "City Mayor"
            })
        
        # Extract departments
        dept_pattern = r'(?:Department of|Dept\. of)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'
        for match in re.finditer(dept_pattern, text):
            entities.append({
                "name": f"Department of {match.group(1)}",
                "type": "department",
                "role": "",
                "context": "City Department"
            })
        
        # Extract City Manager, City Attorney, City Clerk
        for role in ["City Manager", "City Attorney", "City Clerk"]:
            pattern = rf'{role}\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'
            for match in re.finditer(pattern, text):
                entities.append({
                    "name": match.group(1),
                    "type": "person",
                    "role": role,
                    "context": f"{role} of the City"
                })
        
        return entities
    
    def _extract_relationships(self, entities: List[Dict], sections: List[Dict]) -> List[Dict[str, any]]:
        """Extract relationships between entities and agenda items."""
        relationships = []
        
        # Extract relationships from agenda items
        for section in sections:
            for item in section.get('items', []):
                item_code = item.get('item_code', '')
                
                # Sponsors relationship
                for sponsor in item.get('sponsors', []):
                    relationships.append({
                        'source': sponsor,
                        'source_type': 'person',
                        'relationship': 'sponsors',
                        'target': item_code,
                        'target_type': 'agenda_item'
                    })
                
                # Department relationships
                for dept in item.get('departments', []):
                    relationships.append({
                        'source': dept,
                        'source_type': 'department',
                        'relationship': 'responsible_for',
                        'target': item_code,
                        'target_type': 'agenda_item'
                    })
                
                # Stakeholder relationships
                for stakeholder in item.get('stakeholders', []):
                    relationships.append({
                        'source': stakeholder,
                        'source_type': 'organization',
                        'relationship': 'involved_in',
                        'target': item_code,
                        'target_type': 'agenda_item'
                    })
        
        return relationships
    
    def _extract_urls_regex(self, text: str) -> List[Dict[str, str]]:
        """Extract all URLs from the text using regex."""
        urls = []
        
        # Comprehensive URL pattern
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+(?:[/?#][^\s<>"{}|\\^`\[\]]*)?'
        
        for match in re.finditer(url_pattern, text):
            url = match.group(0).rstrip('.,;:)')  # Clean trailing punctuation
            
            # Find surrounding context
            start = max(0, match.start() - 100)
            end = min(len(text), match.end() + 100)
            context = text[start:end]
            
            # Try to find associated agenda item
            item_pattern = r'([A-Z])[.-](\d+)'
            item_matches = list(re.finditer(item_pattern, context))
            
            associated_item = None
            if item_matches:
                # Find closest item reference
                closest_match = min(item_matches, 
                                  key=lambda m: abs(m.start() - (match.start() - start)))
                associated_item = f"{closest_match.group(1)}-{closest_match.group(2)}"
            
            urls.append({
                'url': url,
                'context': context.replace('\n', ' ').strip(),
                'associated_item': associated_item
            })
        
        return urls
    
    def _find_urls_for_item(self, item: Dict, all_urls: List[Dict], full_text: str) -> List[str]:
        """Find URLs associated with a specific agenda item."""
        item_code = item.get('item_code', '')
        if not item_code:
            return []
        
        item_urls = []
        
        # Find URLs that mention this item code
        for url_info in all_urls:
            if url_info.get('associated_item') == item_code:
                item_urls.append(url_info['url'])
            elif item_code in url_info.get('context', ''):
                item_urls.append(url_info['url'])
        
        # Also search near the item in the text
        item_pattern = rf'{re.escape(item_code)}[^A-Z]*'
        match = re.search(item_pattern, full_text, re.IGNORECASE)
        if match:
            # Look for URLs within 500 chars of the item
            start = match.start()
            end = min(len(full_text), start + 1000)
            item_context = full_text[start:end]
            
            url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+(?:[/?#][^\s<>"{}|\\^`\[\]]*)?'
            for url_match in re.finditer(url_pattern, item_context):
                url = url_match.group(0).rstrip('.,;:)')
                if url not in item_urls:
                    item_urls.append(url)
        
        return item_urls
    
    def _create_basic_sections(self, items: List[Dict]) -> List[Dict[str, any]]:
        """Create basic sections as fallback."""
        sections = []
        
        # Group by item type
        ordinances = [item for item in items if item.get('item_type') == 'Ordinance']
        resolutions = [item for item in items if item.get('item_type') == 'Resolution']
        others = [item for item in items if item.get('item_type') not in ['Ordinance', 'Resolution']]
        
        if ordinances:
            sections.append({
                'section_name': 'ORDINANCES',
                'section_type': 'ordinances_first',
                'description': 'Ordinances for consideration',
                'items': ordinances
            })
        
        if resolutions:
            sections.append({
                'section_name': 'RESOLUTIONS',
                'section_type': 'resolutions',
                'description': 'Resolutions for consideration',
                'items': resolutions
            })
        
        if others:
            sections.append({
                'section_name': 'OTHER ITEMS',
                'section_type': 'other',
                'description': 'Other agenda items',
                'items': others
            })
        
        return sections
    
    def _clean_json_response(self, response: str) -> str:
        """Clean LLM response to extract valid JSON."""
        # Remove thinking tags if present
        if '<think>' in response:
            # Extract content after </think>
            parts = response.split('</think>')
            if len(parts) > 1:
                response = parts[1].strip()
        
        # Remove markdown code blocks
        if '```json' in response:
            response = response.split('```json')[1].split('```')[0]
        elif '```' in response:
            response = response.split('```')[1].split('```')[0]
        
        # Remove any non-JSON content before/after
        response = response.strip()
        
        # Find JSON array or object
        if '[' in response:
            # Find the first [ and matching ]
            start_idx = response.find('[')
            if start_idx != -1:
                bracket_count = 0
                for i in range(start_idx, len(response)):
                    if response[i] == '[':
                        bracket_count += 1
                    elif response[i] == ']':
                        bracket_count -= 1
                        if bracket_count == 0:
                            return response[start_idx:i+1]
        
        if '{' in response:
            # Find the first { and matching }
            start_idx = response.find('{')
            if start_idx != -1:
                brace_count = 0
                in_string = False
                escape_next = False
                
                for i in range(start_idx, len(response)):
                    char = response[i]
                    
                    if escape_next:
                        escape_next = False
                        continue
                        
                    if char == '\\':
                        escape_next = True
                        continue
                        
                    if char == '"' and not escape_next:
                        in_string = not in_string
                        
                    if not in_string:
                        if char == '{':
                            brace_count += 1
                        elif char == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                return response[start_idx:i+1]
        
        return response
    
    def _create_empty_ontology(self, filename: str) -> Dict[str, any]:
        """Create empty ontology structure."""
        return {
            'source_file': filename,
            'meeting_date': self._extract_meeting_date(filename),
            'meeting_info': {
                'type': 'Regular Meeting',
                'time': '5:30 PM',
                'location': 'City Commission Chambers',
                'commissioners': [],
                'officials': {}
            },
            'sections': [],
            'entities': [],
            'relationships': [],
            'metadata': {
                'extraction_method': 'empty',
                'num_sections': 0,
                'num_items': 0,
                'num_entities': 0,
                'num_relationships': 0
            }
        } 