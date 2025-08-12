"""
Enhanced NER extractor with simplified 3-prompt extraction
"""

from typing import Dict, List, Any
import asyncio
import logging
import os
import json
from .ner_extractor import NERExtractor
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
from .extraction_config import EXTRACTION_CONFIGS
from collections import defaultdict

log = logging.getLogger(__name__)

class EnhancedNERExtractor(NERExtractor):
    """Enhanced NER extractor with full ontology context in prompts."""
    
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
        doc_type = chunk_metadata.get('document_type', chunk_metadata.get('Document_Type', ''))
        source_file = chunk_metadata.get('source_file_name', chunk_metadata.get('Source_File_Name', '')).lower()
        
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
        source_file = chunk_metadata.get('source_file_name', chunk_metadata.get('Source_File_Name', 'unknown'))
        
        # Prompt 1: Entity Extraction
        entities = await self._extract_entities_only(chunk_text, chunk_metadata)
        
        # CRITICAL FIX: Ensure entities is always a dict with proper structure
        if not isinstance(entities, dict):
            log.error(f"🚨 CRITICAL: entities is not a dict: {type(entities)}")
            entities = {entity_type: [] for entity_type in self.ENTITY_TYPES.keys()}
        
        # CRITICAL FIX: Ensure each entity list contains only dictionaries
        for entity_type, entity_list in entities.items():
            if not isinstance(entity_list, list):
                log.error(f"🚨 CRITICAL: {entity_type} entity_list is not a list: {type(entity_list)}")
                entities[entity_type] = []
                continue
            
            # Filter out any non-dictionary entities
            valid_entities = []
            for entity in entity_list:
                if isinstance(entity, dict):
                    valid_entities.append(entity)
                else:
                    log.error(f"🚨 CRITICAL: Found non-dict entity in {entity_type}: {type(entity)} - {entity}")
            entities[entity_type] = valid_entities
        
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
    
    def _build_complete_ontology_context(self) -> str:
        """Build comprehensive ontology context for LLM prompts."""
        
        # Build entity definitions with all details
        entity_context = "ENTITY ONTOLOGY:\n" + "="*50 + "\n"
        
        for entity_type, info in self.ENTITY_TYPES.items():
            entity_context += f"\n{entity_type}:\n"
            entity_context += f"  Definition: {info['definition']}\n"
            entity_context += f"  Required ID field: {EntityIDStandards.get_id_field(entity_type)}\n"
            entity_context += f"  Attributes: {', '.join(info['attributes'])}\n"
            entity_context += f"  Examples: {', '.join(info['examples'])}\n"
        
        # Build relationship definitions with source-target mappings
        relationship_context = "\n\nRELATIONSHIP ONTOLOGY:\n" + "="*50 + "\n"
        
        for rel_type, rel_info in self.RELATIONSHIP_DEFINITIONS.items():
            source = rel_info['source'] if isinstance(rel_info['source'], str) else ' OR '.join(rel_info['source'])
            target = rel_info['target'] if isinstance(rel_info['target'], str) else ' OR '.join(rel_info['target'])
            
            relationship_context += f"\n{rel_type}: {source} → {target}\n"
            relationship_context += f"  Attributes: {', '.join(rel_info['attributes'])}\n"
            relationship_context += f"  Patterns: {', '.join(rel_info['patterns'])}\n"
        
        # Build entity-relationship mapping
        mapping_context = "\n\nENTITY-RELATIONSHIP MAPPINGS:\n" + "="*50 + "\n"
        mapping_context += "Which entities can have which relationships:\n\n"
        
        # Group by source entity type
        entity_relationships = defaultdict(list)
        for rel_type, rel_info in self.RELATIONSHIP_DEFINITIONS.items():
            sources = [rel_info['source']] if isinstance(rel_info['source'], str) else rel_info['source']
            targets = [rel_info['target']] if isinstance(rel_info['target'], str) else rel_info['target']
            
            for source in sources:
                for target in targets:
                    entity_relationships[source].append(f"{rel_type} → {target}")
        
        for entity_type, relationships in sorted(entity_relationships.items()):
            mapping_context += f"{entity_type} can have these relationships:\n"
            for rel in relationships:
                mapping_context += f"  - {rel}\n"
            mapping_context += "\n"
        
        return entity_context + relationship_context + mapping_context
    
    async def _extract_entities_only(self, chunk_text: str, metadata: Dict) -> Dict[str, List]:
        """Extract entities with proper ID generation."""
        
        doc_type = self._detect_document_type(metadata)
        config = self._get_document_config(doc_type)
        meeting_date = metadata.get('meeting_date', '').replace('.', '_')
        
        # Get complete ontology context
        ontology_context = self._build_complete_ontology_context()
        
        # Build extraction examples WITH PROPER IDs
        extraction_examples = f"""
EXTRACTION EXAMPLES (with proper IDs for context date: {meeting_date}):
- "Commissioner Smith moved to approve" → 
  Person: {{personID: "person_commissioner_smith", name: "Commissioner Smith", title: "Commissioner"}}
  Action: {{actionID: "action_approve_motion_{meeting_date}", type: "approve", dateTime: "{metadata.get('meeting_date', '')}"}}
  
- "Planning Department is part of Development Services" →
  Organization: {{orgID: "org_planning_department", name: "Planning Department", type: "department"}}
  Organization: {{orgID: "org_development_services", name: "Development Services", type: "division"}}
  
- "Ordinance 2024-01 amends Section 5.1" →
  Policy: {{policyID: "policy_ordinance_2024_01", title: "Ordinance 2024-01", type: "ordinance"}}
  Policy: {{policyID: "policy_section_5_1", title: "Section 5.1", type: "code_section"}}
  
- "Meeting held at City Hall" →
  Location: {{locationID: "location_city_hall", name: "City Hall", address: "405 Biltmore Way"}}
"""
        
        prompt = f"""{ontology_context}

DOCUMENT CONTEXT:
- Type: {doc_type.replace('_', ' ').title()}
- Date: {metadata.get('meeting_date', 'unknown')}
- Source: {metadata.get('source_file_name', metadata.get('Source_File_Name', 'unknown'))}

{extraction_examples}

ID GENERATION RULES:
1. Create deterministic IDs from entity content
2. Format: type_descriptive_name (NO xxx or hash suffixes)
3. Include dates for temporal entities: type_name_YYYY_MM_DD
4. Make IDs predictable and consistent

Text to analyze:
{chunk_text[:3000]}

Return JSON format with ALL entity types and PROPER IDs (no xxx suffixes)."""

        response = await self._call_llm(prompt, f"{doc_type} entity extraction", metadata)
        
        # Parse response...
        entities = self._parse_json_response(response)
        
        # Ensure dict shape
        if not isinstance(entities, dict):
            entities = {}

        # Ensure all categories exist
        for et in self.ENTITY_TYPES.keys():
            entities.setdefault(et, [])

        # NEW: normalize ID fields so relationship step sees the right IDs
        normalized = {}
        for et, lst in entities.items():
            clean_list = []
            for ent in (lst or []):
                if isinstance(ent, dict):
                    ent = EntityIDStandards.normalize_entity_id_fields(ent, et)
                    clean_list.append(ent)
            normalized[et] = clean_list
        entities = normalized
        
        return entities
    
    async def _extract_relationships_only(self, chunk_text: str, entities: Dict, chunk_metadata: Dict = None) -> List[Dict]:
        """Extract relationships with full ontology context and entity mappings."""
        
        doc_type = self._detect_document_type(chunk_metadata or {})
        config = self._get_document_config(doc_type)
        
        # Get complete ontology context
        ontology_context = self._build_complete_ontology_context()
        
        # Create entity reference with types
        entity_refs = []
        entity_lookup = {}
        
        for entity_type, entity_list in entities.items():
            for entity in entity_list:
                if not isinstance(entity, dict):
                    continue
                
                id_field = EntityIDStandards.get_id_field(entity_type)
                entity_id = entity.get(id_field) or entity.get('id')
                if entity_id:
                    entity_name = entity.get('name', 'Unknown')
                    ref = f"{entity_name} (Type: {entity_type}, ID: {entity_id})"
                    entity_refs.append(ref)
                    entity_lookup[entity_id] = entity_type
        
        # Build relationship extraction examples
        relationship_examples = """
RELATIONSHIP EXTRACTION EXAMPLES:

From text: "Commissioner Smith moved to approve the ordinance"
Entities found: person_commissioner_smith (Person), action_approve_motion (Action), policy_ordinance_2024_01 (Policy)
Extract:
- {type: "performsAction", source: "person_commissioner_smith", target: "action_approve_motion"}
- {type: "targetOf", source: "action_approve_motion", target: "policy_ordinance_2024_01"}

From text: "The Planning Department submitted the report"
Entities found: org_planning_department (Organization), document_report (Document)
Extract:
- {type: "authoredBy", source: "document_report", target: "org_planning_department"}

From text: "The meeting was held at City Hall"
Entities found: event_commission_meeting (Event), location_city_hall (Location)
Extract:
- {type: "occursAt", source: "event_commission_meeting", target: "location_city_hall"}
"""
        
        prompt = f"""{ontology_context}

ENTITIES FOUND IN THIS CHUNK:
{chr(10).join(entity_refs[:50])}  # Limit to 50 to avoid token overflow

{relationship_examples}

EXTRACTION INSTRUCTIONS:
1. Find ALL possible relationships between the entities listed above
2. Use ONLY the relationship types defined in the ontology
3. Ensure source and target entity types match the relationship definitions
4. Look for both explicit and implicit relationships in the text
5. Extract relationships even if they're mentioned indirectly
6. One action or event can have multiple relationships

Text to analyze:
{chunk_text[:2500]}

Return JSON format:
{{
  "relationships": [
    {{
      "type": "relationship_type_from_ontology",
      "source": "source_entity_id",
      "target": "target_entity_id",
      "attributes": {{}}
    }}
  ]
}}

IMPORTANT: Extract as many valid relationships as possible. Look for all patterns mentioned in the ontology."""

        response = await self._call_llm(prompt, f"{doc_type} relationship extraction with full ontology", chunk_metadata)
        
        result = self._parse_json_response(response)
        
        # Validate relationships against ontology
        validated = []
        for rel in result.get('relationships', []):
            if self._validate_relationship(rel, entity_lookup):
                validated.append(rel)
        
        return validated
    
    async def _enhance_attributes_only(self, chunk_text: str, entities: Dict, chunk_metadata: Dict = None) -> Dict[str, List]:
        """Enhance entity attributes with ontology context."""
        
        # Get complete ontology context (but shorter for attribute enhancement)
        ontology_summary = "ENTITY ATTRIBUTE REQUIREMENTS:\n"
        for entity_type, info in self.ENTITY_TYPES.items():
            ontology_summary += f"\n{entity_type} must have: {', '.join(info['attributes'])}\n"
        
        enhanced = {}
        
        for entity_type, entity_list in entities.items():
            if not entity_list:
                enhanced[entity_type] = []
                continue
            
            expected_attrs = self.ENTITY_TYPES[entity_type]['attributes']
            id_field = EntityIDStandards.get_id_field(entity_type)
            
            entities_json = json.dumps(entity_list, indent=2)
            
            prompt = f"""{ontology_summary}

ENHANCE THESE {entity_type} ENTITIES:
{entities_json}

REQUIRED ATTRIBUTES for {entity_type}:
{', '.join(expected_attrs)}

INSTRUCTIONS:
1. Add ALL missing attributes from the required list
2. Keep existing IDs unchanged
3. Extract values from the text below
4. Use null for attributes not found in text
5. Follow the ontology definitions precisely

Text to analyze:
{chunk_text[:2000]}

Return enhanced entities as JSON array with ALL required attributes."""

            response = await self._call_llm(prompt, f"{entity_type} attribute enhancement", chunk_metadata)
            
            enhanced_list = self._parse_json_response(response)
            
            # Merge by ID (safer than index)
            id_field = EntityIDStandards.get_id_field(entity_type)
            enhanced_by_id = {e.get(id_field) or e.get('id'): e for e in enhanced_list if isinstance(e, dict)}
            final_list = []
            for original in entity_list:
                oid = original.get(id_field) or original.get('id')
                merged = original.copy()
                if oid and oid in enhanced_by_id:
                    merged.update(enhanced_by_id[oid])
                final_list.append(merged)
            
            enhanced[entity_type] = final_list
        
        return enhanced
    
    def _generate_entity_id(self, entity_type: str, entity_name: str) -> str:
        """Generate unique entity ID - inherited from parent."""
        return super()._generate_entity_id(entity_type, entity_name)

    async def _save_extraction_results(self, chunk_id: str, doc_name: str, 
                                     extraction_result: Dict, chunk_metadata: Dict) -> int:
        """Save extraction results - let Stage 4 handle all dedup/merging."""
        
        # Continue with existing save logic...
        return await super()._save_extraction_results(chunk_id, doc_name, extraction_result, chunk_metadata)

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
            document = chunk_metadata.get('document', chunk_metadata.get('source_file_name', chunk_metadata.get('Source_File_Name', 'unknown')))
            chunk_file = chunk_metadata.get('chunk_file', f"{chunk_id}_{document}.txt")
            
            log.info(f"📄 Chunk File: {chunk_file}")
            log.info(f"🆔 Chunk ID: {chunk_id}")
            log.info(f"📋 Document: {document}")
            log.info(f"📝 Document Type: {chunk_metadata.get('document_type', chunk_metadata.get('Document_Type', 'unknown'))}")
            log.info(f"📅 Meeting Date: {chunk_metadata.get('meeting_date', chunk_metadata.get('Meeting_Date', 'unknown'))}")
            log.info(f"📂 Source File: {chunk_metadata.get('source_file_name', chunk_metadata.get('Source_File_Name', 'unknown'))}")
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
                    source_file = chunk_metadata.get('source_file_name', chunk_metadata.get('Source_File_Name', 'unknown')) if chunk_metadata else 'unknown'
                    
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
        # Strip markdown fences
        if '```json' in response:
            response = response.split('```json')[1].split('```')[0].strip()
        elif '```' in response:
            parts = response.split('```')
            if len(parts) >= 3:
                response = parts[1].strip()

        try:
            import json
            result = json.loads(response)
            # Coerce unexpected primitives to empty object
            if result is None or isinstance(result, (str, int, float, bool)):
                return {}
            return result
        except Exception:
            # Last-resort repair path
            try:
                fixed = self._repair_json_response(response)
                result = json.loads(fixed)
                if result is None or isinstance(result, (str, int, float, bool)):
                    return {}
                return result
            except Exception:
                # Final, safe fallback: empty object (callers add required keys)
                return {}
    
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
            response = re.sub(r'{\s*"(meeting_[^"]+)",', r'{\n    "eventID": "\1",', response)
        
        # Fix 2: Missing closing quotes
        response = re.sub(r':\s*"([^"]*)\n', r': "\1",\n', response)
        
        # Fix 3: Trailing commas before closing braces/brackets  
        response = re.sub(r',(\s*[}\]])', r'\1', response)
        
        # Fix 4: Missing commas between object properties
        response = re.sub(r'"\s*\n\s*"', '",\n    "', response)
        
        log.info(f"🔧 JSON repair attempted. Repaired response (first 300 chars): {response[:300]}...")
        
        return response 