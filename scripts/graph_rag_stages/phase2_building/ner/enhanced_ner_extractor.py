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
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
from scripts.graph_rag_stages.common.entity_factory import EntityFactory
from .extraction_config import EXTRACTION_CONFIGS

log = logging.getLogger(__name__)

class EnhancedNERExtractor(NERExtractor):
    """Enhanced NER extractor with simplified 3-prompt extraction strategy."""
    
    def __init__(self, output_dir, seed_entities=None):
        super().__init__(output_dir)
        self.seed_entities = seed_entities or []
    
    def _detect_document_type(self, chunk_metadata: Dict) -> str:
        """Detect document type from chunk metadata."""
        # Validate input
        if not isinstance(chunk_metadata, dict):
            log.error(f"Expected chunk_metadata to be dict, got {type(chunk_metadata)}")
            return 'verbatim_transcript'  # Default fallback
        
        # Check multiple metadata fields for document type
        # FIX: Use the correct capitalized field names that are actually stored
        doc_type = chunk_metadata.get('Document_Type', '') or chunk_metadata.get('document_type', '')
        source_file = chunk_metadata.get('Source_File_Name', '').lower()
        
        # Also check the document name field for additional context
        document_name = chunk_metadata.get('Document', '').lower()
        
        # Pattern matching for document types
        if 'ordinance' in doc_type.lower() or 'ordinance' in source_file or 'ordinance' in document_name:
            return 'ordinance'
        elif 'resolution' in doc_type.lower() or 'resolution' in source_file or 'resolution' in document_name:
            return 'resolution'
        elif 'verbatim' in doc_type.lower() or 'verbatim' in source_file or 'transcript' in source_file:
            return 'verbatim_transcript'
        elif 'agenda' in doc_type.lower() or 'agenda' in source_file:
            return 'agenda'
        elif 'public_comment' in source_file:
            return 'public_comment'
        else:
            # Log the actual metadata for debugging
            log.warning(f"Unknown document type for {source_file}")
            log.warning(f"Available metadata: {list(chunk_metadata.keys())}")
            log.warning(f"Document_Type: '{doc_type}', Document: '{chunk_metadata.get('Document', '')}'")
            log.warning(f"Defaulting to verbatim_transcript")
            return 'verbatim_transcript'
    
    def _get_document_config(self, doc_type: str) -> Dict:
        """Get document-specific extraction configuration."""
        return EXTRACTION_CONFIGS.get(doc_type, EXTRACTION_CONFIGS.get('verbatim_transcript'))
    
    async def _extract_entities_llm(self, chunk_text: str, chunk_metadata: Dict) -> Dict[str, Any]:
        """Extract entities using 3 focused prompts."""
        
        # CRITICAL FIX: Ensure chunk_metadata is always a dict
        if not isinstance(chunk_metadata, dict):
            log.error(f"🚨 CRITICAL: chunk_metadata is not a dict: {type(chunk_metadata)}")
            log.error(f"   Content: {chunk_metadata}")
            # Create a minimal dict to prevent errors
            chunk_metadata = {
                'Source_File_Name': 'unknown',
                'Document_Type': 'unknown',
                'Meeting_Date': 'unknown'
            }
        
        # Get source file for document entity
        source_file = chunk_metadata.get('Source_File_Name', 'unknown')
        
        # Prompt 1: Entity Extraction
        entities = await self._extract_entities_only(chunk_text, chunk_metadata)
        
        # CRITICAL FIX: Ensure entities is always a dict
        if not isinstance(entities, dict):
            log.error(f"🚨 CRITICAL: entities is not a dict: {type(entities)}")
            entities = {entity_type: [] for entity_type in self.ENTITY_TYPES.keys()}
        
        # Ensure document entity exists
        from scripts.graph_rag_stages.common.document_linker import DocumentLinker
        doc_id = DocumentLinker._generate_document_id(source_file)
        
        # Check if Document entity exists
        doc_entities = entities.get('Document', [])
        doc_exists = any(e.get('documentID') == doc_id for e in doc_entities)
        
        if not doc_exists:
            doc_entity = DocumentLinker._create_document_entity(
                doc_id, source_file, chunk_metadata
            )
            if 'Document' not in entities:
                entities['Document'] = []
            entities['Document'].append(doc_entity)
        
        # Prompt 2: Relationship Extraction (with entity context)
        relationships = await self._extract_relationships_only(chunk_text, entities, chunk_metadata)
        
        # CRITICAL FIX: Ensure relationships is always a list
        if not isinstance(relationships, list):
            log.error(f"🚨 CRITICAL: relationships is not a list: {type(relationships)}")
            relationships = []
        
        # Prompt 3: Attribute Enhancement
        enhanced_entities = await self._enhance_attributes_only(chunk_text, entities, chunk_metadata)
        
        # CRITICAL FIX: Ensure enhanced_entities is always a dict
        if not isinstance(enhanced_entities, dict):
            log.error(f"🚨 CRITICAL: enhanced_entities is not a dict: {type(enhanced_entities)}")
            enhanced_entities = entities  # Fall back to original entities
        
        return {
            "entities": enhanced_entities,
            "relationships": relationships,
            "extraction_method": "three_prompt_extraction"
        }
    
    async def _extract_entities_only(self, chunk_text: str, metadata: Dict) -> Dict[str, List]:
        """Prompt 1: Extract only entities without relationships - now document-aware."""
        
        # Validate metadata is a dict
        if not isinstance(metadata, dict):
            log.error(f"Expected metadata to be dict, got {type(metadata)}")
            metadata = {}
        
        # Detect document type
        doc_type = self._detect_document_type(metadata)
        config = self._get_document_config(doc_type)
        
        # Get focus entities but show ALL entity types to LLM
        focus_entities = config.get('focus_entities', list(self.ENTITY_TYPES.keys()))
        
        # Build COMPLETE entity list with all types
        entity_list = []
        focus_entity_list = []
        
        for etype, info in self.ENTITY_TYPES.items():
            # Use document-specific examples if available
            examples = config.get('entity_patterns', {}).get(etype, info['examples'])
            if isinstance(examples, list):
                example_text = ', '.join(examples[:3])
            else:
                example_text = ', '.join(info['examples'][:2])
            
            entity_description = f"- {etype}: {info['definition']} (e.g., {example_text})"
            entity_list.append(entity_description)
            
            # Separate list for focus entities
            if etype in focus_entities:
                focus_entity_list.append(entity_description)
        
        # Add extraction hints specific to document type
        extraction_hints = "\n".join([f"• {hint}" for hint in config.get('extraction_hints', [])[:5]])
        
        prompt = f"""Extract ALL entities from this City of Coral Gables {doc_type.replace('_', ' ')} document.

DOCUMENT TYPE: {doc_type.replace('_', ' ').title()}

ALL AVAILABLE ENTITY TYPES:
{chr(10).join(entity_list)}

PRIORITY ENTITIES FOR {doc_type.upper()} DOCUMENTS:
{chr(10).join(focus_entity_list)}

DOCUMENT-SPECIFIC EXTRACTION HINTS:
{extraction_hints}

INSTRUCTIONS:
1. Extract entities from ANY of the available entity types above
2. Pay special attention to the PRIORITY entities for {doc_type} documents
3. Use the document-specific patterns and hints for better accuracy
4. Generate unique IDs: type_name_hash (e.g., person_smith_a1b2c3)

Document Context:
- Type: {doc_type.replace('_', ' ').title()}
- Date: {metadata.get('meeting_date', 'unknown')}
- Source: {metadata.get('Source_File_Name', 'unknown')}

Text to analyze:
{chunk_text[:3000]}

Return JSON format with ALL entity types (even if empty):
{{
  {', '.join([f'"{e}": []' for e in self.ENTITY_TYPES.keys()])}
}}

Return ONLY valid JSON."""

        response = await self._call_llm(prompt, f"{doc_type} entity extractor", metadata)
        
        # Check for LLM failure markers
        if response == "LLM_EXTRACTION_FAILED":
            log.error(f"🚨 CRITICAL: LLM extraction failed for {doc_type} - skipping this chunk")
            # Return empty entities structure to prevent further processing
            return {entity_type: [] for entity_type in self.ENTITY_TYPES.keys()}
        
        entities = self._parse_json_response(response)
        
        # Check for JSON parsing failure markers
        if entities == "JSON_PARSING_FAILED":
            log.error(f"🚨 CRITICAL: JSON parsing failed for {doc_type} - skipping this chunk")
            # Return empty entities structure
            return {entity_type: [] for entity_type in self.ENTITY_TYPES.keys()}
        
        # Ensure entities is a dict
        if not isinstance(entities, dict):
            log.warning(f"Expected entities to be dict, got {type(entities)}")
            entities = {}
        
        # Ensure ALL entity types are present in response
        for entity_type in self.ENTITY_TYPES.keys():
            if entity_type not in entities:
                entities[entity_type] = []
            # Also ensure each entity type contains a list
            elif not isinstance(entities[entity_type], list):
                log.warning(f"Expected {entity_type} to be a list, got {type(entities[entity_type])}")
                entities[entity_type] = []
        
        return entities
    
    def _apply_document_patterns(self, chunk_text: str, doc_type: str, config: Dict) -> Dict[str, List[str]]:
        """Apply document-specific regex patterns to find entities."""
        import re
        
        found_entities = {}
        entity_patterns = config.get('entity_patterns', {})
        
        for entity_type, patterns in entity_patterns.items():
            found_entities[entity_type] = []
            
            for pattern in patterns:
                # Convert example patterns to regex
                # Replace [Name], [Number] etc with regex groups
                regex_pattern = pattern
                regex_pattern = regex_pattern.replace('[Name]', r'([A-Z][a-zA-Z\s]+)')
                regex_pattern = regex_pattern.replace('[Number]', r'(\d{2,4}-\d{1,5})')
                regex_pattern = regex_pattern.replace('[Title]', r'(.+?)')
                regex_pattern = regex_pattern.replace('[Code]', r'([A-Z]-?\d+)')
                
                try:
                    matches = re.finditer(regex_pattern, chunk_text, re.IGNORECASE)
                    for match in matches:
                        entity_text = match.group(0)
                        found_entities[entity_type].append(entity_text)
                except re.error:
                    continue
        
        return found_entities
    
    async def _extract_relationships_only(self, chunk_text: str, entities: Dict, chunk_metadata: Dict = None) -> List[Dict]:
        """Prompt 2: Extract only relationships between found entities - now document-aware."""
        
        # Detect document type from the chunk context
        doc_type = self._detect_document_type(chunk_metadata or {'document_type': 'unknown'})
        config = self._get_document_config(doc_type)
        
        # Focus on key relationships for this document type
        key_relationships = config.get('key_relationships', list(self.RELATIONSHIP_DEFINITIONS.keys()))
        
        # Create entity reference list (existing code)
        entity_refs = []
        entity_lookup = {}
        
        for entity_type, entity_list in entities.items():
            for entity in entity_list:
                id_field = EntityIDStandards.get_id_field(entity_type)
                entity_id = entity.get(id_field) or entity.get('id')
                if entity_id:
                    ref = f"{entity.get('name', 'Unknown')} ({entity_type}, ID: {entity_id})"
                    entity_refs.append(ref)
                    entity_lookup[entity_id] = entity_type
        
        # Build relationship list focused on document type
        rel_list = []
        for rtype in key_relationships:
            if rtype in self.RELATIONSHIP_DEFINITIONS:
                rdef = self.RELATIONSHIP_DEFINITIONS[rtype]
                source = rdef['source'] if isinstance(rdef['source'], str) else '/'.join(rdef['source'])
                target = rdef['target'] if isinstance(rdef['target'], str) else '/'.join(rdef['target'])
                patterns = ', '.join(rdef['patterns'][:2])
                rel_list.append(f"- {rtype}: {source} → {target} (patterns: {patterns})")
        
        prompt = f"""Extract relationships from this {doc_type.replace('_', ' ')} document.

DOCUMENT TYPE: {doc_type.replace('_', ' ').title()}
FOCUS ON THESE RELATIONSHIPS: {', '.join(key_relationships[:5])}

ENTITIES FOUND:
{chr(10).join(entity_refs[:30])}

KEY RELATIONSHIPS FOR {doc_type.upper()}:
{chr(10).join(rel_list)}

INSTRUCTIONS:
1. Find relationships ONLY between the entities listed above
2. Focus especially on relationships common in {doc_type} documents
3. Use entity IDs exactly as shown
4. Include relationship type and direction (source → target)

Text to analyze:
{chunk_text[:2000]}

Return JSON format:
{{
  "relationships": [
    {{
      "type": "relationship_type",
      "source": "entity_id",
      "target": "entity_id"
    }}
  ]
}}

Return ONLY valid JSON with relationships array."""

        response = await self._call_llm(prompt, f"{doc_type} relationship extractor", chunk_metadata)
        
        # Check for LLM failure markers
        if response == "LLM_EXTRACTION_FAILED":
            log.error(f"🚨 CRITICAL: LLM extraction failed for {doc_type} relationships - returning empty list")
            return []
        
        result = self._parse_json_response(response)
        
        # Check for JSON parsing failure markers
        if result == "JSON_PARSING_FAILED":
            log.error(f"🚨 CRITICAL: JSON parsing failed for {doc_type} relationships - returning empty list")
            return []
        
        # Validate relationships
        validated = []
        for rel in result.get('relationships', []):
            if self._validate_relationship(rel, entity_lookup):
                validated.append(rel)
        
        return validated
    
    async def _enhance_attributes_only(self, chunk_text: str, entities: Dict, chunk_metadata: Dict = None) -> Dict[str, List]:
        """Prompt 3: Enhance entities with full attributes."""
        
        enhanced = {}
        
        # Process each entity type
        for entity_type, entity_list in entities.items():
            if not entity_list:
                enhanced[entity_type] = []
                continue
            
            # Get expected attributes for this type
            expected_attrs = self.ENTITY_TYPES[entity_type]['attributes']
            id_field = EntityIDStandards.get_id_field(entity_type)
            
            # Build attribute-focused prompt
            entities_json = json.dumps(entity_list, indent=2)
            
            prompt = f"""Enhance these {entity_type} entities with full attributes.

ENTITIES TO ENHANCE:
{entities_json}

REQUIRED ATTRIBUTES for {entity_type}:
{', '.join(expected_attrs)}

CRITICAL JSON FORMAT REQUIREMENTS:
1. Return ONLY a valid JSON array of objects
2. Each object MUST have ALL required field names with colons
3. Do NOT omit field names - always include "fieldName": "value"
4. Use proper JSON syntax with quotes around ALL field names and string values

INSTRUCTIONS:
1. Add all missing attributes from the text
2. Keep existing IDs and names unchanged  
3. Use null for attributes not found in text
4. Extract dates, titles, roles, amounts, etc.

EXAMPLE CORRECT FORMAT:
[
  {{
    "{id_field}": "entity_id_value",
    "name": "Entity Name",
    "attribute1": "value1",
    "attribute2": null
  }}
]

Text to analyze:
{chunk_text[:2000]}

Return the enhanced entities with all attributes in VALID JSON array format.
Ensure ALL field names have colons and proper JSON syntax."""

            response = await self._call_llm(prompt, f"{entity_type} attribute enhancer", chunk_metadata)
            
            # Check for LLM failure markers
            if response == "LLM_EXTRACTION_FAILED":
                log.error(f"🚨 CRITICAL: LLM extraction failed for {entity_type} enhancement - using original entities")
                enhanced[entity_type] = entity_list
                continue
            
            enhanced_list = self._parse_json_response(response)
            
            # Check for JSON parsing failure markers
            if enhanced_list == "JSON_PARSING_FAILED":
                log.error(f"🚨 CRITICAL: JSON parsing failed for {entity_type} enhancement - using original entities")
                enhanced[entity_type] = entity_list
                continue
            
            # Ensure we have a list
            if isinstance(enhanced_list, dict):
                if 'entities' in enhanced_list:
                    enhanced_list = enhanced_list['entities']
                else:
                    log.warning(f"Unexpected dict structure for {entity_type}, using original")
                    enhanced_list = entity_list
            elif not isinstance(enhanced_list, list):
                log.warning(f"Expected list for {entity_type}, got {type(enhanced_list)}, using original")
                enhanced_list = entity_list
            
            # Merge with original to preserve IDs
            final_list = []
            for i, original in enumerate(entity_list):
                if i < len(enhanced_list) and isinstance(enhanced_list[i], dict):
                    # Merge enhanced attributes with original
                    merged = original.copy()
                    merged.update(enhanced_list[i])
                    final_list.append(merged)
                else:
                    final_list.append(original)
            
            enhanced[entity_type] = final_list
        
        return enhanced
    
    def _generate_entity_id(self, entity_type: str, entity_name: str) -> str:
        """Generate unique entity ID - inherited from parent."""
        return super()._generate_entity_id(entity_type, entity_name)

    def _validate_relationship(self, rel: Dict, entity_lookup: Dict) -> bool:
        """Validate relationship has valid source/target."""
        source_id = rel.get('source')
        target_id = rel.get('target')
        rel_type = rel.get('type')
        
        if not all([source_id, target_id, rel_type]):
            return False
        
        # Check entities exist
        if source_id not in entity_lookup or target_id not in entity_lookup:
            return False
        
        # Validate against RELATIONSHIP_DEFINITIONS
        if rel_type in self.RELATIONSHIP_DEFINITIONS:
            rel_def = self.RELATIONSHIP_DEFINITIONS[rel_type]
            source_type = entity_lookup[source_id]
            target_type = entity_lookup[target_id]
            
            # Check source type
            expected_sources = rel_def['source'] if isinstance(rel_def['source'], list) else [rel_def['source']]
            if source_type not in expected_sources:
                return False
            
            # Check target type  
            expected_targets = rel_def['target'] if isinstance(rel_def['target'], list) else [rel_def['target']]
            if target_type not in expected_targets:
                return False
        
        return True
    
    async def _call_llm(self, prompt: str, task_name: str, chunk_metadata: Dict = None) -> str:
        """Make LLM call with error handling, retries, and detailed logging."""
        
        # Log chunk metadata first if available
        if chunk_metadata:
            log.info("\n" + "🏷️  CHUNK METADATA:")
            
            # Extract chunk file name
            chunk_id = chunk_metadata.get('chunk_id', 'unknown')
            document = chunk_metadata.get('document', chunk_metadata.get('Source_File_Name', 'unknown'))
            chunk_file = chunk_metadata.get('chunk_file', f"{chunk_id}_{document}.txt")
            
            log.info(f"📄 Chunk File: {chunk_file}")
            log.info(f"🆔 Chunk ID: {chunk_id}")
            log.info(f"📋 Document: {document}")
            log.info(f"📝 Document Type: {chunk_metadata.get('Document_Type', chunk_metadata.get('document_type', 'unknown'))}")
            log.info(f"📅 Meeting Date: {chunk_metadata.get('meeting_date', chunk_metadata.get('Meeting_Date', 'unknown'))}")
            log.info(f"📂 Source File: {chunk_metadata.get('Source_File_Name', 'unknown')}")
            if 'Index' in chunk_metadata or 'chunk_index' in chunk_metadata:
                index_info = chunk_metadata.get('Index', f"{chunk_metadata.get('chunk_index', 0) + 1}/{chunk_metadata.get('total_chunks', '?')}")
                log.info(f"🔢 Chunk Index: {index_info}")
        
        # Log the LLM call details for debugging
        log.info("\n" + "="*100)
        log.info(f"🤖 LLM CALL: {task_name}")
        log.info("="*100)
        
        log.info(f"📤 PROMPT SENT TO LLM:")
        log.info("-" * 80)
        log.info(prompt)
        log.info("-" * 80)
        
        # Retry logic for LLM calls
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
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
                
                response_content = response.choices[0].message.content.strip()
                
                # Validate response is not empty
                if not response_content or response_content.strip() == "":
                    raise ValueError("LLM returned empty response")
                
                # Log the response
                log.info(f"📥 RESPONSE RECEIVED FROM LLM:")
                log.info("-" * 80)
                log.info(response_content)
                log.info("-" * 80)
                
                # Log usage statistics if available
                if hasattr(response, 'usage') and response.usage:
                    log.info(f"📊 TOKEN USAGE:")
                    log.info(f"  - Prompt tokens: {response.usage.prompt_tokens}")
                    log.info(f"  - Completion tokens: {response.usage.completion_tokens}")
                    log.info(f"  - Total tokens: {response.usage.total_tokens}")
                
                log.info("="*100 + "\n")
                
                return response_content
                
            except Exception as e:
                attempt_msg = f"Attempt {attempt + 1}/{max_retries}"
                log.error(f"❌ LLM CALL FAILED for {task_name} ({attempt_msg}): {e}")
                
                if attempt < max_retries - 1:
                    log.info(f"🔄 Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    # Final attempt failed - this is a CRITICAL error
                    chunk_id = chunk_metadata.get('chunk_id', 'unknown') if chunk_metadata else 'unknown'
                    source_file = chunk_metadata.get('Source_File_Name', 'unknown') if chunk_metadata else 'unknown'
                    
                    log.error("=" * 80)
                    log.error(f"🚨 CRITICAL: All LLM retry attempts failed!")
                    log.error(f"   Task: {task_name}")
                    log.error(f"   Chunk: {chunk_id}")
                    log.error(f"   Source: {source_file}")
                    log.error(f"   Final Error: {e}")
                    log.error(f"   This chunk will be SKIPPED from entity extraction!")
                    log.error("=" * 80)
                    
                    # IMPORTANT: Log critical error but return fallback to avoid pipeline failure
                    # The general exception handler would catch this and make it silent again
                    
                    # Instead, return a special marker that indicates failure
                    return "LLM_EXTRACTION_FAILED"
    
    def _parse_json_response(self, response: str) -> Any:
        """Parse JSON response with markdown handling and robust error recovery."""
        # Clean up markdown formatting
        if '```json' in response:
            response = response.split('```json')[1].split('```')[0].strip()
        elif '```' in response:
            parts = response.split('```')
            if len(parts) >= 3:
                response = parts[1].strip()
        
        # First attempt: standard JSON parsing
        try:
            import json
            result = json.loads(response)
            
            # CRITICAL FIX: Ensure we never return a string that could cause .get() errors
            if isinstance(result, str):
                log.warning(f"LLM returned a string instead of object/array: {result[:100]}")
                # Return empty dict for entity responses, empty list for others
                return {} if any(word in response.lower() for word in ['entit', 'person', 'document']) else []
                
            # CRITICAL FIX: Ensure we always return a proper structure
            if result is None:
                log.warning("LLM returned null, returning empty dict")
                return {}
                
            return result
        
        except json.JSONDecodeError as e:
            log.warning(f"Initial JSON parse failed: {e}")
            log.warning(f"Attempting JSON repair on response: {response[:300]}...")
            
            # Attempt to fix common LLM JSON errors
            try:
                fixed_response = self._repair_json_response(response)
                result = json.loads(fixed_response)
                log.info(f"✅ JSON repair successful!")
                
                # CRITICAL FIX: Ensure repaired result is also not a string
                if isinstance(result, str):
                    log.warning(f"Repaired result is still a string: {result[:100]}")
                    return {}
                    
                return result
            
            except Exception as repair_error:
                log.error(f"❌ CRITICAL: Failed to parse LLM JSON response: {e}")
                log.error(f"❌ JSON repair also failed: {repair_error}")
                log.error(f"   Raw response (first 500 chars): {response[:500]}")
                
                # IMPORTANT: Don't silently return empty data but also don't raise exceptions
                # that get caught by the general exception handler
                log.error("🚨 CRITICAL: JSON parsing failed completely")
                log.error("   Returning failure marker to prevent silent failure")
                return "JSON_PARSING_FAILED"
    
    def _repair_json_response(self, response: str) -> str:
        """Attempt to repair common LLM JSON formatting errors."""
        import re
        
        # Fix 1: Missing field names before values (most common issue)
        # Pattern: "some_id_value", should be "fieldName": "some_id_value",
        
        # Detect if this is an array of objects
        if response.strip().startswith('[') and response.strip().endswith(']'):
            # For entity enhancement responses, try to fix missing field names
            
            # Fix missing topicID field names
            response = re.sub(r'{\s*"(topic_[^"]+)",', r'{\n    "topicID": "\1",', response)
            
            # Fix missing field names for other entity types
            response = re.sub(r'{\s*"(person_[^"]+)",', r'{\n    "personID": "\1",', response)
            response = re.sub(r'{\s*"(organization_[^"]+)",', r'{\n    "orgID": "\1",', response)
            response = re.sub(r'{\s*"(document_[^"]+)",', r'{\n    "documentID": "\1",', response)
            response = re.sub(r'{\s*"(policy_[^"]+)",', r'{\n    "policyID": "\1",', response)
            response = re.sub(r'{\s*"(action_[^"]+)",', r'{\n    "actionID": "\1",', response)
            response = re.sub(r'{\s*"(location_[^"]+)",', r'{\n    "locationID": "\1",', response)
            response = re.sub(r'{\s*"(role_[^"]+)",', r'{\n    "roleID": "\1",', response)
            response = re.sub(r'{\s*"(agendaitem_[^"]+)",', r'{\n    "agendaItemID": "\1",', response)
            response = re.sub(r'{\s*"(meeting_[^"]+)",', r'{\n    "meetingID": "\1",', response)
        
        # Fix 2: Missing closing quotes
        response = re.sub(r':\s*"([^"]*)\n', r': "\1",\n', response)
        
        # Fix 3: Trailing commas before closing braces/brackets  
        response = re.sub(r',(\s*[}\]])', r'\1', response)
        
        # Fix 4: Missing commas between object properties
        response = re.sub(r'"\s*\n\s*"', '",\n    "', response)
        
        log.info(f"🔧 JSON repair attempted. Repaired response (first 300 chars): {response[:300]}...")
        
        return response 