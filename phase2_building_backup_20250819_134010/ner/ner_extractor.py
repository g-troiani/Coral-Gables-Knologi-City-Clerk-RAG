"""
DEPRECATED: Use phase3_querying.ner.UnifiedQueryEngine instead.
This module remains for backward compatibility and will be removed after migration.
"""

import json
import logging
import asyncio
import traceback
from pathlib import Path
from typing import Dict, List, Any, Optional
from openai import AzureOpenAI
import os
import warnings
import hashlib
import re
from datetime import datetime
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
from scripts.graph_rag_stages.common.document_linker import DocumentLinker
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
from scripts.graph_rag_stages.common.entity_factory import EntityFactory

log = logging.getLogger(__name__)
warnings.filterwarnings("default", category=DeprecationWarning)
warnings.warn(
    "NERExtractor is deprecated; use phase3_querying.ner.UnifiedQueryEngine.",
    DeprecationWarning,
    stacklevel=2,
)


class NERExtractor:
    """Extracts named entities from chunks using LLM based on City Governance Ontology."""
    
    # Entity types and relationships are now sourced from UnifiedOntology in __init__

    
    def __init__(self, output_dir: Path):
        """Initialize the NER extractor."""
        log.warning("NERExtractor is deprecated; prefer UnifiedQueryEngine.")
        self.output_dir = Path(output_dir)
        self.chunks_dir = self.output_dir / "document_chunks"
        
        # Use unified ontology (single source of truth)
        self.ENTITY_TYPES = UnifiedOntology.ENTITY_TYPES
        self.RELATIONSHIP_DEFINITIONS = UnifiedOntology.RELATIONSHIP_DEFINITIONS
        self.RELATIONSHIP_TYPES = UnifiedOntology.RELATIONSHIP_TYPES
        
        # Create directories for entity types
        for entity_type in UnifiedOntology.get_entity_categories():
            (self.output_dir / entity_type).mkdir(parents=True, exist_ok=True)
        
        # Create relationships directory
        (self.output_dir / "relationships").mkdir(parents=True, exist_ok=True)
        
        # Initialize Azure OpenAI client with timeout configuration
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            timeout=60.0  # 60 second timeout to prevent hanging calls
        )
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
        if not self.model:
            raise ValueError("AZURE_OPENAI_DEPLOYMENT_NAME environment variable must be set")
        
        # Rate limiting
        self.max_concurrent = 5
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
    
    async def process_all_chunks(self) -> int:
        """Process all chunks in the chunks directory."""
        chunk_files = list(self.chunks_dir.glob("*.txt"))
        log.info(f"Found {len(chunk_files)} chunks to process for NER")
        
        # Process in batches to avoid overwhelming the API
        batch_size = 10
        total_entities = 0
        
        for i in range(0, len(chunk_files), batch_size):
            batch = chunk_files[i:i + batch_size]
            tasks = [self._process_chunk(chunk_file) for chunk_file in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    log.error(f"Error processing chunk: {result}")
                else:
                    total_entities += result
            
            # Small delay between batches
            if i + batch_size < len(chunk_files):
                await asyncio.sleep(0.5)  # Was 1
        
        return total_entities
    
    async def _process_chunk(self, chunk_file: Path) -> int:
        """Process a single chunk file."""
        async with self.semaphore:
            try:
                # Get chunk ID and document name from filename
                filename_parts = chunk_file.stem.split("_", 1)
                chunk_id = filename_parts[0]
                doc_name = filename_parts[1] if len(filename_parts) > 1 else "unknown"
                
                # Read chunk metadata
                chunk_metadata = self._read_chunk_metadata(chunk_file)
                
                # FIX: Add chunk_id to metadata so logging can find it
                chunk_metadata['chunk_id'] = chunk_id
                chunk_metadata['document'] = doc_name
                chunk_metadata['chunk_file'] = chunk_file.name
                
                # Fix 2: Add fallback for missing metadata
                if 'Source_File_Name' not in chunk_metadata or chunk_metadata['Source_File_Name'] == 'unknown':
                    chunk_metadata['Source_File_Name'] = f"{doc_name}.pdf"
                
                # Read chunk content
                with open(chunk_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # IMPROVED: More robust chunk text extraction
                # Handle multiple --- separators and malformed content
                if "---" in content:
                    parts = content.split("---")
                    
                    # Find the actual content part (skip metadata sections)
                    chunk_text = ""
                    for i, part in enumerate(parts):
                        # Skip first part (before first ---)
                        if i == 0:
                            continue
                        
                        # Check if this part looks like metadata
                        cleaned_part = part.strip()
                        if cleaned_part and not cleaned_part.startswith('- '):
                            # This looks like actual content
                            chunk_text = cleaned_part
                            break
                    
                    # If no clean content found, try last part
                    if not chunk_text and len(parts) > 1:
                        chunk_text = parts[-1].strip()
                else:
                    chunk_text = content
                
                # Skip if no meaningful content
                if not chunk_text or len(chunk_text) < 10:
                    log.warning(f"Skipping chunk {chunk_id} - no meaningful content found")
                    return 0
                
                # Extract entities using LLM
                extraction_result = await self._extract_entities_llm(chunk_text, chunk_metadata)
                
                # Save entity files with metadata
                entity_count = await self._save_extraction_results(chunk_id, doc_name, extraction_result, chunk_metadata)
                
                # Log what we extracted
                log.info(f"Chunk {chunk_id} extracted {entity_count} entities")
                
                return entity_count
                
            except Exception as e:
                log.error(
                    "NER FAILED for chunk=%s  doc=%r  doc_type=%r  meta_keys=%s  error=%s\n%s",
                    chunk_metadata.get("chunk_id") or chunk_metadata.get("chunk") or "<unknown>",
                    chunk_metadata.get("document"),
                    chunk_metadata.get("document_type"),
                    sorted(chunk_metadata.keys()),
                    repr(e),
                    traceback.format_exc(),       # <- full stack in the log
                )
                return 0
    
    async def _extract_entities_llm(self, chunk_text: str, chunk_metadata: Dict) -> Dict[str, Any]:
        """Extract entities using LLM with detailed logging."""
        prompt = self._build_extraction_prompt(chunk_text, chunk_metadata)
        
        # Log chunk metadata first
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
        log.info(f"🤖 LLM CALL: Base NER Entity Extraction")
        log.info("="*100)
        
        log.info(f"📝 CHUNK TEXT (first 500 chars):")
        log.info("-" * 80)
        log.info(chunk_text[:500] + "..." if len(chunk_text) > 500 else chunk_text)
        log.info("-" * 80)
        
        log.info(f"📤 PROMPT SENT TO LLM:")
        log.info("-" * 80)
        log.info(prompt)
        log.info("-" * 80)
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at extracting structured entities from city government documents based on a formal ontology. Return only valid JSON."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0,
                max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Log the response
            log.info(f"📥 RESPONSE RECEIVED FROM LLM:")
            log.info("-" * 80)
            log.info(result_text)
            log.info("-" * 80)
            
            # Log usage statistics if available
            if hasattr(response, 'usage') and response.usage:
                log.info(f"📊 TOKEN USAGE:")
                log.info(f"  - Prompt tokens: {response.usage.prompt_tokens}")
                log.info(f"  - Completion tokens: {response.usage.completion_tokens}")
                log.info(f"  - Total tokens: {response.usage.total_tokens}")
            
            # Parse JSON response
            extraction_result = self._parse_extraction_response(result_text)
            
            log.info(f"✅ PARSED RESULT:")
            log.info(f"  - Entities found: {len(extraction_result.get('entities', {}))}")
            log.info(f"  - Relationships found: {len(extraction_result.get('relationships', []))}")
            log.info("="*100 + "\n")
            
            return extraction_result
            
        except Exception as e:
            log.error(f"❌ LLM EXTRACTION FAILED: {e}")
            log.error("-" * 80)
            log.error("="*100 + "\n")
            return {"entities": {}, "relationships": []}
    
    def _build_extraction_prompt(self, chunk_text: str, chunk_metadata: Dict) -> str:
        """Build the entity extraction prompt with PROPER ID generation."""
        
        # Build entity type descriptions WITH ID requirements
        entity_descriptions = []
        for entity_type, info in self.ENTITY_TYPES.items():
            id_field = EntityIDStandards.get_id_field(entity_type)
            attrs = ", ".join(info['attributes'])
            examples = ", ".join(f'"{ex}"' for ex in info['examples'])
            entity_descriptions.append(
                f"{entity_type}: {info['definition']}\n"
                f"  REQUIRED ID FIELD: {id_field}\n"
                f"  Attributes: {attrs}\n"
                f"  Examples: {examples}"
            )
        
        # Build relationship descriptions
        relationship_descriptions = []
        for rel_type, rel_info in self.RELATIONSHIP_DEFINITIONS.items():
            source = rel_info['source'] if isinstance(rel_info['source'], str) else '/'.join(rel_info['source'])
            target = rel_info['target'] if isinstance(rel_info['target'], str) else '/'.join(rel_info['target'])
            attrs = ", ".join(rel_info['attributes'])
            patterns_list = (rel_info.get("patterns") or     # current
                             rel_info.get("regex") or        # tolerate renamed keys
                             rel_info.get("rules") or [])
            patterns = ", ".join(f'"{p}"' for p in patterns_list[:3])
            relationship_descriptions.append(
                f"{rel_type}: {source} → {target}\n"
                f"  Attributes: {attrs}\n"
                f"  Patterns: {patterns}"
            )
        
        # Get context for better ID generation
        meeting_date = chunk_metadata.get('meeting_date', chunk_metadata.get('Meeting_Date', '')).replace('.', '_')
        doc_type = chunk_metadata.get('document_type', 'doc')
        
        prompt = f"""Extract entities and relationships from this City of Coral Gables government document chunk.

CRITICAL ID GENERATION RULES:
- Person entities MUST use "personID" field
- Organization entities MUST use "orgID" field  
- Document entities MUST use "documentID" field
- AgendaItem entities MUST use "agendaItemID" field
- Event entities MUST use "eventID" field

ID FORMAT RULES:
1. Use deterministic IDs based on entity content
2. Format: type_descriptive_name (NO random suffixes like xxx)
3. For dated entities, include date: type_name_YYYY_MM_DD
4. Make IDs unique but predictable from the entity data
5. Use underscores, no spaces or special characters

ENTITY TYPES TO EXTRACT:
{chr(10).join(entity_descriptions)}

RELATIONSHIP TYPES (with directionality and patterns):
{chr(10).join(relationship_descriptions)}

Document Context:
- Type: {doc_type}
- Date: {meeting_date}
- Source: {chunk_metadata.get('source_file_name', chunk_metadata.get('Source_File_Name', 'unknown'))}

ID GENERATION EXAMPLES:
- "Commissioner Smith" → personID: "person_commissioner_smith"
- "Planning Department" → orgID: "org_planning_department"
- "Ordinance 2024-01" → policyID: "policy_ordinance_2024_01"
- "Agenda Item E-1" → agendaItemID: "agenda_item_e1_{meeting_date}"
- "City Commission Meeting on Jan 9, 2024" → eventID: "event_commission_meeting_2024_01_09"
- "405 Biltmore Way" → locationID: "location_405_biltmore_way"

RELATIONSHIP EXTRACTION EXAMPLES:
- "Commissioner Smith moved to approve" → 
  {{"type": "performsAction", "source": "person_commissioner_smith", "target": "action_approve_motion"}}
- "Planning Department is part of Development Services" → 
  {{"type": "isPartOf", "source": "org_planning_department", "target": "org_development_services"}}

Text to analyze:
{chunk_text[:3000]}

Return format:
{{
  "entities": {{
    "Person": [{{
      "personID": "person_commissioner_smith",  // NO xxx suffix!
      "name": "Commissioner John Smith",
      "title": "Commissioner",
      "affiliation": "City Council"
    }}],
    "Document": [{{
      "documentID": "document_agenda_2024_01_09",  // Clear, deterministic ID
      "title": "City Commission Agenda",
      "type": "agenda",
      "issueDate": "2024-01-09"
    }}]
  }},
  "relationships": [{{
    "type": "isMemberOf",
    "source": "person_commissioner_smith",
    "target": "org_city_council"
  }}]
}}

Return ONLY valid JSON with complete extraction and PROPER IDs."""
        
        return prompt
    
    def _validate_relationship(self, relationship: Dict, entities_in_chunk: Dict[str, str]) -> Optional[Dict]:
        """Validate and enhance a relationship with proper typing."""
        rel_type = relationship.get('type')
        if rel_type not in self.RELATIONSHIP_DEFINITIONS:
            return None
        
        rel_def = self.RELATIONSHIP_DEFINITIONS[rel_type]
        source_id = relationship.get('source')
        target_id = relationship.get('target')
        
        # Check if source and target entities exist
        if source_id not in entities_in_chunk or target_id not in entities_in_chunk:
            return None
        
        # Validate entity types match relationship definition
        source_type = entities_in_chunk[source_id]
        target_type = entities_in_chunk[target_id]
        
        # Check source type
        expected_sources = rel_def['source'] if isinstance(rel_def['source'], list) else [rel_def['source']]
        if source_type not in expected_sources:
            return None
        
        # Check target type
        expected_targets = rel_def['target'] if isinstance(rel_def['target'], list) else [rel_def['target']]
        if target_type not in expected_targets:
            return None
        
        # Ensure attributes are valid
        attributes = relationship.get('attributes', {})
        valid_attributes = {}
        for attr in rel_def['attributes']:
            if attr in attributes:
                valid_attributes[attr] = attributes[attr]
        
        return {
            'type': rel_type,
            'source': source_id,
            'target': target_id,
            'attributes': valid_attributes
        }
    
    def _parse_extraction_response(self, response_text: str) -> Dict[str, Any]:
        """Parse the LLM response with relationship validation and ID normalization."""
        # Clean up response
        if '```json' in response_text:
            response_text = response_text.split('```json')[1].split('```')[0].strip()
        elif '```' in response_text:
            parts = response_text.split('```')
            if len(parts) >= 3:
                response_text = parts[1].strip()
        
        try:
            result = json.loads(response_text)
            
            # Ensure structure is correct
            if "entities" not in result:
                result["entities"] = {}
            if "relationships" not in result:
                result["relationships"] = []
            
            # Build entity ID to type mapping for validation
            entity_id_map = {}
            
            # Validate and normalize entity types
            validated_entities = {}
            for entity_type in self.ENTITY_TYPES:
                if entity_type in result["entities"] and isinstance(result["entities"][entity_type], list):
                    # Normalize each entity's ID fields
                    normalized_entities = []
                    for entity in result["entities"][entity_type]:
                        # Normalize ID fields
                        normalized_entity = EntityIDStandards.normalize_entity_id_fields(entity, entity_type)
                        
                        # Get the standard ID field
                        id_field = EntityIDStandards.get_id_field(entity_type)
                        entity_id = normalized_entity.get(id_field)
                        
                        if entity_id:
                            entity_id_map[entity_id] = entity_type
                            normalized_entities.append(normalized_entity)
                    
                    validated_entities[entity_type] = normalized_entities
                else:
                    validated_entities[entity_type] = []
            
            result["entities"] = validated_entities
            
            # Validate and enhance relationships
            validated_relationships = []
            for rel in result.get("relationships", []):
                validated_rel = self._validate_relationship(rel, entity_id_map)
                if validated_rel:
                    validated_relationships.append(validated_rel)
            
            result["relationships"] = validated_relationships
            
            return result
            
        except json.JSONDecodeError as e:
            log.error(f"Failed to parse entity JSON: {e}")
            return {"entities": {entity_type: [] for entity_type in self.ENTITY_TYPES}, "relationships": []}
    
    async def _save_extraction_results(self, chunk_id: str, doc_name: str, extraction_result: Dict, chunk_metadata: Dict) -> int:
        """Save extraction results with entity validation."""
        total_entities = 0
        
        # Get source file info from chunk metadata
        source_file_name = chunk_metadata.get('source_file_name', chunk_metadata.get('Source_File_Name', doc_name))
        source_file_path = chunk_metadata.get('Source_File_Path', f"unknown/{doc_name}")
        
        # Collect all entities for relationship creation
        all_entities = []
        
        # Determine the canonical document ID based on the chunk's source file.
        # Taxonomy owns/creates the Document vertex; NER will only link to it.
        from scripts.graph_rag_stages.common.document_linker import DocumentLinker
        expected_doc_id = DocumentLinker._generate_document_id(source_file_name)
        
        # Save entities with validation
        for entity_type, entities in extraction_result.get("entities", {}).items():
            if entities:
                # Validate each entity before saving
                validated_entities = []
                for entity in entities:
                    try:
                        if entity_type == "Document":
                            # Do not persist Document entities from NER; taxonomy owns document vertices.
                            continue
                        # Ensure entity has correct ID field
                        validated_entity = EntityFactory.validate_entity({
                            **entity,
                            'type': entity_type
                        })
                        validated_entities.append(validated_entity)
                        
                        # Collect for relationship creation
                        entity_with_type = validated_entity.copy()
                        entity_with_type['type'] = entity_type
                        all_entities.append(entity_with_type)
                    except ValueError as e:
                        log.warning(f"Invalid entity skipped: {e}")
                        continue
                
                filename = f"{chunk_id}_{doc_name}.json"
                filepath = self.output_dir / entity_type / filename
                
                # Save as JSON with metadata
                file_data = {
                    "chunk_id": chunk_id,
                    "document": doc_name,
                    "source_file": source_file_name,
                    "source_path": source_file_path,
                    "entity_type": entity_type,
                    "entities": validated_entities,
                    "_chunk_metadata": chunk_metadata
                }
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(file_data, f, indent=2, ensure_ascii=False)
                
                total_entities += len(validated_entities)
        
        # Create document relationships
        doc_relationships = DocumentLinker.create_document_entity_relationships(
            all_entities, chunk_metadata, chunk_id
        )
        
        # Save relationships including document links
        relationships = extraction_result.get("relationships", [])
        relationships.extend(doc_relationships)
        
        if relationships:
            filename = f"{chunk_id}_{doc_name}.json"
            filepath = self.output_dir / "relationships" / filename
            
            file_data = {
                "chunk_id": chunk_id,
                "document": doc_name,
                "source_file": source_file_name,
                "source_path": source_file_path,
                "relationships": relationships
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(file_data, f, indent=2, ensure_ascii=False)
        
        return total_entities
    
    def _read_chunk_metadata(self, chunk_file: Path) -> Dict[str, Any]:
        """Extract metadata from chunk file header (keys normalized to snake_case)."""
        metadata: Dict[str, Any] = {}
        try:
            content = chunk_file.read_text(encoding="utf-8")
        except Exception:
            return metadata

        if "---" in content:
            header, _ = content.split("---", 1)
            for line in header.strip().split("\n"):
                if line.startswith("#") and ":" in line:
                    key, value = line[1:].strip().split(":", 1)
                    norm_key = key.strip().lower().replace(" ", "_")
                    metadata[norm_key] = value.strip()

        # Back-compat aliases (read-only)
        # e.g. if old headers had "Document Type", callers can use document_type
        if "document_type" not in metadata and "documenttype" in metadata:
            metadata["document_type"] = metadata["documenttype"]
        if "meeting_date" not in metadata and "meetingdate" in metadata:
            metadata["meeting_date"] = metadata["meetingdate"]
        if "source_file_name" not in metadata and "source" in metadata:
            metadata["source_file_name"] = metadata["source"]

        return metadata
    
