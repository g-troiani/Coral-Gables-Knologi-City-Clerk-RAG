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
from .extractor_util import infer_doc_type
from collections import defaultdict
from scripts.graph_rag_stages.common.ontology_attributes import OntologyAttributesRegistry
import traceback

log = logging.getLogger(__name__)

class EnhancedNERExtractor(NERExtractor):
    """Enhanced NER extractor with full ontology context in prompts."""
    
    def __init__(self, output_dir, seed_entities=None):
        super().__init__(output_dir)
        self.seed_entities = seed_entities or []
        self._validate_ontology()
    
    def _validate_ontology(self):
        missing = []
        for name, rel in (self.RELATIONSHIP_DEFINITIONS or {}).items():
            if not any(k in rel for k in ("attributes", "attribute_schema", "properties", "props")):
                missing.append(name)
        if missing:
            log.warning("Ontology: %d relationships without attributes defined: %s",
                        len(missing), ", ".join(missing[:10]) + ("..." if len(missing) > 10 else ""))
    
    def _detect_document_type(self, chunk_metadata: Dict) -> str:
        """Detect document type from chunk metadata using improved normalization."""
        return infer_doc_type(chunk_metadata)
    
    def _get_document_config(self, doc_type: str) -> Dict:
        """Get document-specific extraction configuration."""
        return EXTRACTION_CONFIGS.get(doc_type, EXTRACTION_CONFIGS.get('verbatim_transcript'))
    
    def _merge_enhancement(self, base_entity: dict, enhanced: dict) -> dict:
        merged = {**base_entity, **(enhanced or {})}

        # Always preserve canonical id
        merged["id"] = base_entity.get("id") or enhanced.get("id")

        # Always keep or infer the type
        merged["type"] = (
            base_entity.get("type")
            or enhanced.get("type")
            or EntityIDStandards.infer_type_from_id(merged.get("id"))
            or "Unknown"
        )

        return merged
    
    def _postprocess_relationships(self, raw_rels: list[dict], entities: list[dict]) -> list[dict]:
        if not raw_rels:
            return []
        entities_by_id = {e["id"]: e for e in entities if e.get("id")}
        known_ids = set(entities_by_id.keys())

        def _allowed(src_t: str, rel_t: str, tgt_t: str) -> bool:
            if rel_t not in self.RELATIONSHIP_DEFINITIONS:
                return False
            rel_def = self.RELATIONSHIP_DEFINITIONS[rel_t]
            expected_sources = rel_def['source'] if isinstance(rel_def['source'], list) else [rel_def['source']]
            expected_targets = rel_def['target'] if isinstance(rel_def['target'], list) else [rel_def['target']]
            return src_t in expected_sources and tgt_t in expected_targets

        out = []
        for r in raw_rels:
            rtype = r.get("type")
            src = r.get("source")
            tgt = r.get("target")
            attrs = r.get("attributes") or {}

            # Canonicalize/repair IDs
            src = EntityIDStandards.canonicalize_id(src, known_ids) or src
            tgt = EntityIDStandards.canonicalize_id(tgt, known_ids) or tgt
            if src not in entities_by_id or tgt not in entities_by_id:
                # Unknown entity id -> drop
                continue

            src_t = entities_by_id[src].get("type") or EntityIDStandards.infer_type_from_id(src) or "Unknown"
            tgt_t = entities_by_id[tgt].get("type") or EntityIDStandards.infer_type_from_id(tgt) or "Unknown"

            # Quick reject of non-ontology types
            if rtype is None:
                continue
            if rtype == "targetOf":
                # Not in your ontology; drop
                continue

            # Normalize common inverse/synonyms
            # (1) Section <-> AgendaItem via containsItem / belongsToSection
            if rtype == "isPartOf":
                if src_t == "AgendaItem" and tgt_t == "Section":
                    rtype = "belongsToSection"
                elif src_t == "Section" and tgt_t == "AgendaItem":
                    # inverse of belongsToSection
                    rtype = "containsItem"
                # otherwise leave as-is and let the ontology validator decide

            # Validate, try inverse if needed
            if not _allowed(src_t, rtype, tgt_t):
                # Try flipping if inverse exists
                INVERSE = {
                    "containsItem": "belongsToSection",
                    "belongsToSection": "containsItem",
                    "authoredBy": None,  # no inverse defined in your ontology
                    "references": None,
                    "addressesTopic": None,
                    "discusses": None,
                    "occursAt": None,
                    "recordedIn": None,
                    "hasTranscript": None,
                    "hasSection": None,
                    "containsItem": "belongsToSection",
                    "precedes": None,
                    "precedesSection": None,
                    "resultsIn": None,
                    "implementedBy": None,
                    "embodies": None,
                    "hasAgenda": None,
                    "belongsToEvent": None,
                    "isMemberOf": None,
                    "isPartOf": None,  # generally, prefer explicit forms above
                }
                inv = INVERSE.get(rtype)
                if inv and _allowed(tgt_t, inv, src_t):
                    src, tgt, rtype = tgt, src, inv
                    src_t, tgt_t = tgt_t, src_t
                else:
                    # Still not allowed → drop
                    continue

            out.append({
                "type": rtype,
                "source": src,
                "target": tgt,
                "attributes": attrs
            })
        return out
    
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
        try:
            from scripts.graph_rag_stages.common.document_linker import DocumentLinker
        except Exception:
            class DocumentLinker:
                @staticmethod
                def _generate_document_id(source_file: str) -> str:
                    import hashlib
                    return "document_" + hashlib.sha1((source_file or "unknown").encode()).hexdigest()[:12]
                @staticmethod
                def _create_document_entity(doc_id: str, source_file: str, meta: Dict) -> Dict:
                    return {
                        "documentID": doc_id,
                        "title": meta.get("Document", source_file) or source_file,
                        "type": "Document",
                        "document_type": meta.get("document_type", meta.get("Document_Type", "document")),
                        "status": "Final",
                        "issueDate": meta.get("meeting_date", meta.get("Meeting_Date")),
                        "sourceURL": meta.get("Source_URL")
                    }
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
        raw_relationships = await self._extract_relationships_only(chunk_text, entities, chunk_metadata)
        
        # CRITICAL FIX: Ensure relationships is always a list
        if not isinstance(raw_relationships, list):
            log.error(f"🚨 CRITICAL: relationships is not a list: {type(raw_relationships)}")
            raw_relationships = []
        
        # Prompt 3: Attribute Enhancement
        enhanced_entities = await self._enhance_attributes_only(chunk_text, entities, chunk_metadata)
        
        # CRITICAL FIX: Ensure enhanced_entities is always a dict
        if not isinstance(enhanced_entities, dict):
            log.error(f"🚨 CRITICAL: enhanced_entities is not a dict: {type(enhanced_entities)}")
            enhanced_entities = entities  # Fall back to original entities
        
        # Postprocess relationships after entity enhancement (when types are stable)
        all_entities = []
        for entity_list in enhanced_entities.values():
            all_entities.extend(entity_list)
        relationships = self._postprocess_relationships(raw_relationships, all_entities)
        
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
            src = rel_info.get("source") or rel_info.get("source_type") or rel_info.get("from") or "unknown"
            tgt = rel_info.get("target") or rel_info.get("target_type") or rel_info.get("to") or "unknown"
            source = src if isinstance(src, str) else ' OR '.join(src) if src else "unknown"
            target = tgt if isinstance(tgt, str) else ' OR '.join(tgt) if tgt else "unknown"
            
            relationship_context += f"\n{rel_type}: {source} → {target}\n"
            # Tolerate missing/variant attribute schema keys and normalize to a list of strings
            attrs = (
                rel_info.get('attributes') or
                rel_info.get('attribute_schema') or
                rel_info.get('properties') or
                rel_info.get('props') or
                []
            )
            if isinstance(attrs, dict):
                # Show typed attributes like "status:bool"
                attrs_fmt = [f"{k}:{v}" for k, v in attrs.items()]
            elif isinstance(attrs, (list, tuple)):
                attrs_fmt = [str(a) for a in attrs]
            elif attrs:
                attrs_fmt = [str(attrs)]
            else:
                attrs_fmt = []
            relationship_context += "  Attributes: " + (", ".join(attrs_fmt) if attrs_fmt else "None") + "\n"
            patterns = (rel_info.get("patterns") or     # current
                        rel_info.get("regex") or        # tolerate renamed keys
                        rel_info.get("rules") or [])
            relationship_context += f"  Patterns: {', '.join(patterns)}\n"
        
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
        
        # Get allowed entity IDs for prompt
        allowed_ids = []
        for entity_type, entity_list in entities.items():
            for entity in entity_list:
                if isinstance(entity, dict):
                    id_field = EntityIDStandards.get_id_field(entity_type)
                    entity_id = entity.get(id_field) or entity.get('id')
                    if entity_id:
                        allowed_ids.append(entity_id)
        
        prompt = f"""{ontology_context}

ENTITIES FOUND IN THIS CHUNK:
{chr(10).join(entity_refs[:50])}  # Limit to 50 to avoid token overflow

ALLOWED_ENTITY_IDS (use ONLY these IDs):
{chr(10).join(allowed_ids)}

{relationship_examples}

EXTRACTION INSTRUCTIONS:
1. Find ALL possible relationships between the entities listed above
2. Use ONLY the relationship types defined in the ontology
3. Use ONLY entity IDs from the ALLOWED_ENTITY_IDS list above
4. Ensure source and target entity types match the relationship definitions
5. Look for both explicit and implicit relationships in the text
6. Extract relationships even if they're mentioned indirectly
7. One action or event can have multiple relationships
8. If a relationship type is not listed in the ontology, do not output it

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

            # Prefer ontology-backed attribute list if available
            expected_attrs = OntologyAttributesRegistry.get_attrs(entity_type) \
                or self.ENTITY_TYPES[entity_type]['attributes']
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

            parsed = self._parse_json_response(response)
            # Normalize to a list of dicts
            if isinstance(parsed, list):
                enhanced_list = [e for e in parsed if isinstance(e, dict)]
            elif isinstance(parsed, dict):
                if "entities" in parsed and isinstance(parsed["entities"], list):
                    enhanced_list = [e for e in parsed["entities"] if isinstance(e, dict)]
                else:
                    enhanced_list = [v for v in parsed.values() if isinstance(v, dict)]
            else:
                enhanced_list = []

            # Merge by ID (safer than index)
            id_field = EntityIDStandards.get_id_field(entity_type)
            enhanced_by_id = {e.get(id_field) or e.get('id'): e for e in enhanced_list if isinstance(e, dict)}
            final_list = []
            for original in entity_list:
                oid = original.get(id_field) or original.get('id')
                enhanced_entity = enhanced_by_id.get(oid) if oid else None
                merged = self._merge_enhancement(original, enhanced_entity)
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

    def _inject_provenance(self, entities: Dict[str, List[Dict]], chunk_metadata: Dict) -> None:
        """Ensure Source_File_Name/Path exist on every entity from this chunk."""
        src_name = (chunk_metadata.get('Source_File_Name') or
                    chunk_metadata.get('source_file_name') or
                    chunk_metadata.get('Document') or
                    chunk_metadata.get('Source') or
                    'unknown')
        src_path = (chunk_metadata.get('Source_File_Path') or
                    chunk_metadata.get('source_file_path') or
                    chunk_metadata.get('Source') or
                    None)
        for et, lst in (entities or {}).items():
            for e in (lst or []):
                if isinstance(e, dict):
                    e.setdefault('Source_File_Name', src_name)
                    if src_path:
                        e.setdefault('Source_File_Path', src_path) 