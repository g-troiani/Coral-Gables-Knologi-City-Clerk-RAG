"""
Enhanced NER extractor with simplified 3-prompt extraction
"""

from typing import Dict, List, Any
import asyncio
import logging
import os
import json
from pathlib import Path
from .ner_extractor import NERExtractor

log = logging.getLogger(__name__)

class EnhancedNERExtractor(NERExtractor):
    """Enhanced NER extractor with simplified 3-prompt extraction strategy."""
    
    def __init__(self, output_dir, seed_entities=None):
        super().__init__(output_dir)
        self.seed_entities = seed_entities or []
    
    async def _extract_entities_llm(self, chunk_text: str, chunk_metadata: Dict) -> Dict[str, Any]:
        """Extract entities using 3 focused prompts."""
        
        # Prompt 1: Entity Extraction
        entities = await self._extract_entities_only(chunk_text, chunk_metadata)
        
        # Prompt 2: Relationship Extraction (with entity context)
        relationships = await self._extract_relationships_only(chunk_text, entities)
        
        # Prompt 3: Attribute Enhancement
        enhanced_entities = await self._enhance_attributes_only(chunk_text, entities)
        
        return {
            "entities": enhanced_entities,
            "relationships": relationships,
            "extraction_method": "three_prompt_extraction"
        }
    
    async def _extract_entities_only(self, chunk_text: str, metadata: Dict) -> Dict[str, List]:
        """Prompt 1: Extract only entities without relationships."""
        
        # Build entity-focused prompt
        entity_list = "\n".join([
            f"- {etype}: {info['definition']} (e.g., {', '.join(info['examples'][:2])})"
            for etype, info in self.ENTITY_TYPES.items()
        ])
        
        prompt = f"""Extract ALL entities from this City of Coral Gables document chunk.

ENTITY TYPES TO FIND:
{entity_list}

INSTRUCTIONS:
1. Find ALL entities of the types listed above
2. For each entity, provide: name and type
3. Generate unique IDs: type_name_hash (e.g., person_smith_a1b2c3)
4. DO NOT extract relationships or detailed attributes yet

Document Context:
- Type: {metadata.get('document_type', 'unknown')}
- Date: {metadata.get('meeting_date', 'unknown')}

Text to analyze:
{chunk_text[:3000]}

Return JSON format:
{{
  "Person": [{{"personID": "person_smith_a1b2c3", "name": "John Smith"}}],
  "Organization": [{{"orgID": "org_council_b2c3d4", "name": "City Council"}}],
  ... other entity types
}}

Return ONLY valid JSON."""

        response = await self._call_llm(prompt, "entity extractor")
        return self._parse_json_response(response)
    
    async def _extract_relationships_only(self, chunk_text: str, entities: Dict) -> List[Dict]:
        """Prompt 2: Extract only relationships between found entities."""
        
        # Create entity reference list
        entity_refs = []
        entity_lookup = {}  # For validation
        
        for entity_type, entity_list in entities.items():
            for entity in entity_list:
                id_field = f"{entity_type.lower()}ID"
                entity_id = entity.get(id_field) or entity.get('id')
                if entity_id:
                    ref = f"{entity.get('name', 'Unknown')} ({entity_type}, ID: {entity_id})"
                    entity_refs.append(ref)
                    entity_lookup[entity_id] = entity_type
        
        # Build relationship-focused prompt
        rel_list = "\n".join([
            f"- {rtype}: {rdef['source']} → {rdef['target']} (patterns: {', '.join(rdef['patterns'][:2])})"
            for rtype, rdef in self.RELATIONSHIP_DEFINITIONS.items()
        ])
        
        prompt = f"""Extract relationships between these entities found in the text:

ENTITIES FOUND:
{chr(10).join(entity_refs[:30])}  # First 30 for context

RELATIONSHIP TYPES:
{rel_list}

INSTRUCTIONS:
1. Find relationships ONLY between the entities listed above
2. Use entity IDs exactly as shown
3. Include relationship type and direction (source → target)
4. DO NOT create new entities

Text to analyze:
{chunk_text[:2000]}

Return JSON format:
{{
  "relationships": [
    {{
      "type": "isMemberOf",
      "source": "person_smith_a1b2c3",
      "target": "org_council_b2c3d4"
    }}
  ]
}}

Return ONLY valid JSON with relationships array."""

        response = await self._call_llm(prompt, "relationship extractor")
        result = self._parse_json_response(response)
        
        # Validate relationships
        validated = []
        for rel in result.get('relationships', []):
            if self._validate_relationship(rel, entity_lookup):
                validated.append(rel)
        
        return validated
    
    async def _enhance_attributes_only(self, chunk_text: str, entities: Dict) -> Dict[str, List]:
        """Prompt 3: Enhance entities with full attributes."""
        
        enhanced = {}
        
        # Process each entity type
        for entity_type, entity_list in entities.items():
            if not entity_list:
                enhanced[entity_type] = []
                continue
            
            # Get expected attributes for this type
            expected_attrs = self.ENTITY_TYPES[entity_type]['attributes']
            
            # Build attribute-focused prompt
            entities_json = json.dumps(entity_list, indent=2)
            
            prompt = f"""Enhance these {entity_type} entities with full attributes.

ENTITIES TO ENHANCE:
{entities_json}

REQUIRED ATTRIBUTES for {entity_type}:
{', '.join(expected_attrs)}

INSTRUCTIONS:
1. Add all missing attributes from the text
2. Keep existing IDs and names unchanged
3. Use null for attributes not found in text
4. Extract dates, titles, roles, amounts, etc.

Text to analyze:
{chunk_text[:2000]}

Return the enhanced entities with all attributes.

Return ONLY valid JSON array."""

            response = await self._call_llm(prompt, f"{entity_type} attribute enhancer")
            enhanced_list = self._parse_json_response(response)
            
            # Ensure we have a list
            if isinstance(enhanced_list, dict) and 'entities' in enhanced_list:
                enhanced_list = enhanced_list['entities']
            elif not isinstance(enhanced_list, list):
                enhanced_list = [enhanced_list] if enhanced_list else entity_list
            
            # Merge with original to preserve IDs
            final_list = []
            for i, original in enumerate(entity_list):
                if i < len(enhanced_list):
                    # Merge enhanced attributes with original
                    merged = original.copy()
                    merged.update(enhanced_list[i])
                    final_list.append(merged)
                else:
                    final_list.append(original)
            
            enhanced[entity_type] = final_list
        
        return enhanced
    
    async def _call_llm(self, prompt: str, task_name: str) -> str:
        """Make LLM call with error handling."""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": f"You are a {task_name} for city government documents. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            log.error(f"LLM call failed for {task_name}: {e}")
            return "{}"
    
    def _parse_json_response(self, response: str) -> Any:
        """Parse JSON response with markdown handling."""
        if '```json' in response:
            response = response.split('```json')[1].split('```')[0].strip()
        elif '```' in response:
            parts = response.split('```')
            if len(parts) >= 3:
                response = parts[1].strip()
        
        try:
            import json
            return json.loads(response)
        except:
            return {} if response.startswith('{') else [] 