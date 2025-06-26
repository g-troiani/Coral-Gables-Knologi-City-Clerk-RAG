#!/usr/bin/env python3
"""
Stage 3: Ontology Enhancement (LLM)
Enhanced entity extraction and meeting information processing
"""

import logging
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Any
from groq import Groq

log = logging.getLogger(__name__)

class OntologyEnhancer:
    """Stage 3: Enhance agenda data with deep ontology extraction."""
    
    def __init__(self, output_dir: Path = Path("city_clerk_documents/extracted_json")):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize Groq client
        self.client = Groq()
        self.model = "llama-3.3-70b-versatile"
    
    def enhance_agenda_ontology(self, agenda_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhance agenda data with comprehensive ontology extraction.
        
        Args:
            agenda_data: Output from Stage 2 (agenda extraction)
            
        Returns:
            Enhanced agenda data with detailed meeting info and entities
        """
        log.info(f"🧠 Stage 3: Enhancing ontology for {agenda_data['source_file']}")
        
        full_text = agenda_data["full_text"]
        
        try:
            # Extract detailed meeting information
            enhanced_meeting_info = self._extract_enhanced_meeting_info(full_text)
            
            # Extract entities from agenda items  
            entities = self._extract_entities_from_items(agenda_data["agenda_items"], full_text)
            
            # Create relationships between entities
            relationships = self._create_entity_relationships(agenda_data["agenda_items"], entities)
            
            # Generate canonical IDs and provenance
            provenance_data = self._generate_provenance_data(agenda_data)
            
            # Enhance agenda structure with ontology data
            enhanced_structure = self._enhance_agenda_structure(agenda_data["sections"])
            
            ontology_result = {
                **agenda_data,  # Preserve original data
                "meeting_info": enhanced_meeting_info,
                "sections": enhanced_structure,
                "entities": entities,
                "relationships": relationships,
                "provenance": provenance_data,
                "ontology_metadata": {
                    "stage": 3,
                    "enhancement_timestamp": self._get_timestamp(),
                    "entity_count": len(entities),
                    "relationship_count": len(relationships),
                    "llm_model": self.model
                }
            }
            
        except Exception as e:
            log.error(f"❌ Stage 3 enhancement failed: {e}")
            # Return original data with minimal enhancement
            ontology_result = {
                **agenda_data,
                "entities": [],
                "relationships": [],
                "ontology_metadata": {
                    "stage": 3,
                    "enhancement_failed": True,
                    "error": str(e)
                }
            }
        
        # Save enhanced result
        output_file = self.output_dir / f"{agenda_data['source_file'].replace('.pdf', '')}_stage3_ontology.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(ontology_result, f, indent=2, ensure_ascii=False)
        
        log.info(f"✅ Stage 3 complete: {len(ontology_result.get('entities', []))} entities extracted")
        return ontology_result
    
    def _extract_enhanced_meeting_info(self, text: str) -> Dict[str, Any]:
        """Extract comprehensive meeting information using LLM."""
        
        # Meeting info extraction prompt - exactly as specified
        prompt = f"""Extract meeting information from this city commission agenda. Find:

1. Meeting type (Regular, Special, Workshop)
2. Meeting time
3. Meeting location/venue
4. Commission members present (if listed)
5. City officials (Mayor, City Manager, City Attorney, City Clerk)

Return ONLY the JSON object below, no other text:
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
        
        messages = [
            {
                "role": "system",
                "content": "You are a JSON extraction assistant. Return ONLY valid JSON, no markdown formatting or code blocks."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0
            )
            
            result_text = response.choices[0].message.content.strip()
            log.debug(f"🤖 Meeting info response: {result_text}")
            
            # Parse JSON response
            meeting_info = self._parse_json_response(result_text)
            
            # Validate and enhance with fallback extraction
            enhanced_info = self._validate_and_enhance_meeting_info(meeting_info, text)
            
            return enhanced_info
            
        except Exception as e:
            log.warning(f"⚠️  LLM meeting info extraction failed: {e}")
            return self._fallback_meeting_info_extraction(text)
    
    def _extract_entities_from_items(self, agenda_items: List[Dict], full_text: str) -> List[Dict[str, Any]]:
        """Extract entities from agenda items using LLM."""
        
        entities = []
        
        # Create context from agenda items
        items_text = "\n".join([
            f"{item.get('item_code', '')}: {item.get('title', '')}"
            for item in agenda_items
        ])
        
        entity_prompt = f"""Extract ALL entities from this city council agenda. Find:

1. PEOPLE: Names of commissioners, officials, citizens, business owners
2. ORGANIZATIONS: Companies, nonprofits, government departments, agencies
3. LOCATIONS: Streets, addresses, neighborhoods, parks, buildings
4. PROJECTS: Development projects, infrastructure projects, programs
5. MONEY: Dollar amounts, budgets, fees, costs
6. DOCUMENT_NUMBERS: Ordinance numbers, resolution numbers, permit numbers

For each entity, return:
{{
  "name": "Entity Name",
  "type": "PERSON|ORGANIZATION|LOCATION|PROJECT|MONEY|DOCUMENT_NUMBER",
  "description": "Brief description",
  "origin_doc_id": "source identifier"
}}

Return a JSON array of entities.

Agenda items:
{items_text[:4000]}"""
        
        messages = [
            {
                "role": "system",
                "content": "You are an expert entity extractor. Return only valid JSON arrays."
            },
            {
                "role": "user", 
                "content": entity_prompt
            }
        ]
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0
            )
            
            result_text = response.choices[0].message.content.strip()
            log.debug(f"🤖 Entity extraction response: {result_text[:200]}...")
            
            # Parse entities from response
            extracted_entities = self._parse_entities_response(result_text)
            
            # Add agenda item entities
            for item in agenda_items:
                item_code = item.get("item_code")
                if item_code:
                    entities.append({
                        "name": item_code,
                        "type": "AGENDA_ITEM",
                        "description": f"Agenda item {item_code}",
                        "origin_doc_id": item.get("doc_id", ""),
                        "title": item.get("title", "")
                    })
                
                # Add document reference entities
                doc_ref = item.get("document_reference")
                if doc_ref:
                    entities.append({
                        "name": doc_ref,
                        "type": "DOCUMENT_NUMBER",
                        "description": f"Document reference {doc_ref}",
                        "origin_doc_id": item.get("doc_id", ""),
                        "related_item": item_code
                    })
            
            entities.extend(extracted_entities)
            
        except Exception as e:
            log.warning(f"⚠️  Entity extraction failed: {e}")
            # Return basic entities from agenda items
            for item in agenda_items:
                if item.get("item_code"):
                    entities.append({
                        "name": item["item_code"],
                        "type": "AGENDA_ITEM",
                        "description": f"Agenda item {item['item_code']}",
                        "extraction_method": "fallback"
                    })
        
        log.info(f"✅ Extracted {len(entities)} entities")
        return entities
    
    def _create_entity_relationships(self, agenda_items: List[Dict], entities: List[Dict]) -> List[Dict[str, Any]]:
        """Create relationships between entities."""
        
        relationships = []
        
        # Create item -> document reference relationships
        for item in agenda_items:
            item_code = item.get("item_code")
            doc_ref = item.get("document_reference")
            
            if item_code and doc_ref:
                relationships.append({
                    "source": item_code,
                    "target": doc_ref,
                    "type": "references_document",
                    "description": f"Agenda item {item_code} references document {doc_ref}"
                })
        
        # Create section -> item relationships
        section_items = {}
        for item in agenda_items:
            section = item.get("section_name", "UNKNOWN")
            item_code = item.get("item_code")
            
            if section not in section_items:
                section_items[section] = []
            if item_code:
                section_items[section].append(item_code)
        
        for section, items in section_items.items():
            for item_code in items:
                relationships.append({
                    "source": section,
                    "target": item_code,
                    "type": "contains_item",
                    "description": f"Section {section} contains item {item_code}"
                })
        
        # Create entity co-occurrence relationships
        # This could be enhanced with more sophisticated NLP
        
        log.info(f"✅ Created {len(relationships)} relationships")
        return relationships
    
    def _parse_json_response(self, response_text: str) -> Dict[str, Any]:
        """Parse JSON response with robust error handling."""
        
        # Clean up response text
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
            return json.loads(response_text)
        except json.JSONDecodeError as e:
            log.error(f"❌ JSON parse error: {e}")
            log.error(f"Raw response: {response_text}")
            return {}
    
    def _parse_entities_response(self, response_text: str) -> List[Dict[str, Any]]:
        """Parse entities from LLM response."""
        
        parsed_data = self._parse_json_response(response_text)
        
        if isinstance(parsed_data, list):
            return parsed_data
        elif isinstance(parsed_data, dict):
            if 'entities' in parsed_data:
                return parsed_data['entities']
            else:
                return [parsed_data]  # Single entity
        else:
            return []
    
    def _validate_and_enhance_meeting_info(self, meeting_info: Dict, text: str) -> Dict[str, Any]:
        """Validate LLM-extracted meeting info and enhance with fallbacks."""
        
        enhanced = meeting_info.copy() if meeting_info else {}
        
        # Validate and enhance meeting type
        if not enhanced.get("type"):
            if "special" in text.lower():
                enhanced["type"] = "Special Meeting"
            elif "workshop" in text.lower():
                enhanced["type"] = "Workshop"
            else:
                enhanced["type"] = "Regular Meeting"
        
        # Validate and enhance time
        if not enhanced.get("time"):
            time_match = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)', text, re.IGNORECASE)
            enhanced["time"] = time_match.group(1) if time_match else "Unknown"
        
        # Validate and enhance location
        if not enhanced.get("location"):
            location_patterns = [
                r'Commission\s+Chambers?',
                r'City\s+Hall',
                r'Meeting\s+Room\s+\d+'
            ]
            
            for pattern in location_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    enhanced["location"] = match.group(0)
                    break
            
            if not enhanced.get("location"):
                enhanced["location"] = "City Hall"
        
        # Ensure officials structure exists
        if not enhanced.get("officials"):
            enhanced["officials"] = {}
        
        # Ensure commissioners list exists
        if not enhanced.get("commissioners"):
            enhanced["commissioners"] = []
        
        return enhanced
    
    def _fallback_meeting_info_extraction(self, text: str) -> Dict[str, Any]:
        """Fallback meeting info extraction using regex patterns."""
        
        return {
            "type": "Regular Meeting",
            "time": self._extract_time_regex(text),
            "location": self._extract_location_regex(text),
            "commissioners": [],
            "officials": {},
            "extraction_method": "regex_fallback"
        }
    
    def _extract_time_regex(self, text: str) -> str:
        """Extract meeting time using regex."""
        time_match = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)', text, re.IGNORECASE)
        return time_match.group(1) if time_match else "Unknown"
    
    def _extract_location_regex(self, text: str) -> str:
        """Extract meeting location using regex."""
        location_patterns = [
            r'Commission\s+Chambers?',
            r'City\s+Hall',
            r'Meeting\s+Room\s+\d+'
        ]
        
        for pattern in location_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(0)
        
        return "City Hall"
    
    def _generate_provenance_data(self, agenda_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate provenance and canonical ID data."""
        
        return {
            "source_document": agenda_data["source_file"],
            "document_id": agenda_data["doc_id"],
            "extraction_pipeline": "stages_1_2_3",
            "extraction_timestamp": agenda_data.get("metadata", {}).get("extraction_timestamp"),
            "canonical_ids": {
                "meeting": f"MEETING_{agenda_data['meeting_date'].replace('.', '_')}",
                "agenda_document": agenda_data["doc_id"]
            }
        }
    
    def _enhance_agenda_structure(self, sections: List[Dict]) -> List[Dict[str, Any]]:
        """Enhance agenda structure with additional metadata."""
        
        enhanced_sections = []
        
        for i, section in enumerate(sections):
            enhanced_section = section.copy()
            
            # Add section metadata
            enhanced_section["section_order"] = i + 1
            enhanced_section["section_id"] = f"SECTION_{i+1}"
            
            # Classify section type
            section_name = section.get("section_name", "").upper()
            
            if "CONSENT" in section_name:
                enhanced_section["section_type"] = "CONSENT"
            elif "ORDINANCE" in section_name or "RESOLUTION" in section_name:
                enhanced_section["section_type"] = "ORDINANCES_RESOLUTIONS"
            elif "PUBLIC" in section_name and "COMMENT" in section_name:
                enhanced_section["section_type"] = "PUBLIC_COMMENT"
            elif "PRESENTATION" in section_name:
                enhanced_section["section_type"] = "PRESENTATIONS"
            elif "MANAGER" in section_name:
                enhanced_section["section_type"] = "MANAGER_ITEMS"
            else:
                enhanced_section["section_type"] = "GENERAL"
            
            # Enhance items within section
            enhanced_items = []
            for j, item in enumerate(section.get("items", [])):
                enhanced_item = item.copy()
                enhanced_item["item_order"] = j + 1
                enhanced_item["item_id"] = f"ITEM_{enhanced_item.get('item_code', j+1)}"
                enhanced_items.append(enhanced_item)
            
            enhanced_section["items"] = enhanced_items
            enhanced_sections.append(enhanced_section)
        
        return enhanced_sections
    
    def _get_timestamp(self) -> str:
        """Get current timestamp."""
        from datetime import datetime
        return datetime.now().isoformat()

def main():
    """Example usage of Stage 3 ontology enhancement."""
    logging.basicConfig(level=logging.INFO)
    
    enhancer = OntologyEnhancer()
    
    print("🚀 Stage 3: Ontology Enhancer ready!")
    print("✅ Features:")
    print("  - Enhanced meeting information extraction")
    print("  - Comprehensive entity extraction (people, orgs, locations)")
    print("  - Relationship creation between entities")
    print("  - Canonical ID generation and provenance tracking")
    print("  - Section classification and ordering")

if __name__ == "__main__":
    main() 