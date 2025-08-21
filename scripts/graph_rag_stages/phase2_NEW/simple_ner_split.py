#!/usr/bin/env python3

import os
import json
from pathlib import Path
import sys
from dotenv import load_dotenv

# Ensure project root is on sys.path for package imports
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.graph_rag_stages.common.utils import get_llm_client
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
from scripts.graph_rag_stages.common.entity_factory import EntityFactory
from scripts.graph_rag_stages.common.document_linker import DocumentLinker
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
import hashlib
import re
from datetime import datetime

load_dotenv()

PROMPT_FILE = Path(__file__).parent / "ner_prompt.txt"
ONTOLOGY_FILE = Path(__file__).parent / "ontology_context_camelCase.txt"

# Define entity type groups - split into two halves
ENTITY_TYPE_GROUP_1 = [
    "Person", "Organization", "Document", "Section", "AgendaItem",
    "Policy", "Contract", "Technology"
]

ENTITY_TYPE_GROUP_2 = [
    "VoteOutcome", "Event", "Location", "Asset", "Project", 
    "Role", "Topic", "Action", "Meeting", "Presentation", 
    "PublicComment", "Board", "Appointment", "LegalReference"
]


def load_prompts_from_file() -> tuple[str, str, str, str]:
    text = PROMPT_FILE.read_text(encoding='utf-8')
    parts = text.split("=== PROMPT 1 — ENTITIES ONLY ===")
    system_part = parts[0].replace("SYSTEM TEMPLATE", "").strip()
    user_p1 = parts[1].split("=== PROMPT 2", 1)[0].strip()
    user_p2 = ""
    user_p3 = ""
    if "=== PROMPT 2 — RELATIONSHIPS ONLY ===" in text:
        rel_part = text.split("=== PROMPT 2 — RELATIONSHIPS ONLY ===", 1)[1]
        user_p2 = rel_part.split("=== PROMPT 3", 1)[0].strip()
    if "=== PROMPT 3 — ATTRIBUTE ENHANCEMENT" in text:
        attr_part = text.split("=== PROMPT 3 — ATTRIBUTE ENHANCEMENT", 1)[1]
        user_p3 = attr_part.strip()
    return system_part, user_p1, user_p2 if user_p2 else "", user_p3 if user_p3 else ""


def parse_chunk_file(chunk_file: str):
    text = Path(chunk_file).read_text(encoding='utf-8')
    meta = {}
    header_parts = text.split("---")
    header_text = "\n".join(header_parts[:2]) if len(header_parts) >= 2 else header_parts[0]
    for line in header_text.splitlines():
        if line.startswith('#') and ':' in line:
            key, val = line[1:].split(':', 1)
            meta[key.strip()] = val.strip()
    chunk_id = meta.get('Chunk', meta.get('Chunk ID', 'unknown'))
    document = meta.get('Document', meta.get('Source', 'unknown'))
    document_type = meta.get('Document_Type', meta.get('Document Type', 'unknown')).lower()
    meeting_date = meta.get('Meeting_Date', meta.get('Meeting Date', 'unknown'))
    source_file_name = meta.get('sourceFileName', meta.get('Source_File_Name', Path(chunk_file).name))
    source_file_path = meta.get('sourceFilePath', meta.get('Source_File_Path', 'unknown'))
    body_text = header_parts[-1].strip() if header_parts else text
    return {
        'chunkId': chunk_id or 'unknown',
        'document': document or 'unknown',
        'documentType': document_type or 'unknown',
        'meetingDate': meeting_date or 'unknown',
        'sourceFileName': source_file_name,
        'sourceFilePath': source_file_path,
        'chunkFile': Path(chunk_file).name,
    }, body_text


def _normalize_slug(entity_type: str, raw_name: str) -> str:
    s = (raw_name or "").lower().strip()
    if entity_type == "Person":
        for t in ("commissioner", "mayor", "vice mayor", "mr", "ms", "mrs", "dr"):
            s = re.sub(rf"\b{re.escape(t)}\b", "", s).strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s.replace(" ", "_")[:60] or entity_type.lower()


def _type_prefix(entity_type: str) -> str:
    return {
        "AgendaItem": "agenda_item",
        "AgendaDocument": "agenda_doc"
    }.get(entity_type, entity_type.lower())


def _ensure_id(entity: dict, entity_type: str) -> dict:
    normalized = EntityIDStandards.normalize_entity_id_fields(entity, entity_type)
    id_field = EntityIDStandards.get_id_field(entity_type)
    if not normalized.get(id_field):
        if entity_type == "Policy":
            preferred = EntityIDStandards.preferred_policy_id(normalized)
            if preferred:
                normalized[id_field] = preferred
                normalized['id'] = preferred
                return normalized
        if entity_type == "AgendaItem":
            preferred = EntityIDStandards.preferred_agendaitem_id(normalized)
            if preferred:
                normalized[id_field] = preferred
                normalized['id'] = preferred
                return normalized
        base_name = normalized.get("name") or normalized.get("title") or "unknown"
        slug = _normalize_slug(entity_type, base_name)
        hash6 = hashlib.sha256(f"{entity_type}|{slug}".encode("utf-8")).hexdigest()[:6]
        new_id = f"{_type_prefix(entity_type)}_{slug}_{hash6}"
        normalized[id_field] = new_id
        normalized['id'] = new_id
    return normalized


def _persist_phase2_new(meta: dict, parsed: dict, raw_text: str):
    # Track all entities from raw output and their persistence status
    persistence_log = {
        "raw_entities_count": 0,
        "persisted_entities_count": 0,
        "missing_entities": [],
        "failure_reasons": {
            "json_parse_failure": 0,
            "not_dict_root": 0,
            "invalid_bucket_type": 0,
            "bucket_not_list": 0,
            "entity_not_dict": 0,
            "validation_failed": 0,
            "empty_bucket": 0
        },
        "buckets_summary": {}  # Track all buckets and their entity counts
    }
    
    # First, try to count entities in raw text and track all buckets
    try:
        raw_parsed = json.loads(raw_text)
        if isinstance(raw_parsed, dict):
            raw_entities = raw_parsed.get('entities', raw_parsed)
            if isinstance(raw_entities, dict):
                for bucket, items in raw_entities.items():
                    if isinstance(items, list):
                        persistence_log["raw_entities_count"] += len(items)
                        persistence_log["buckets_summary"][bucket] = {
                            "raw_count": len(items),
                            "persisted_count": 0,
                            "status": "found"
                        }
                    else:
                        persistence_log["buckets_summary"][bucket] = {
                            "raw_count": 0,
                            "persisted_count": 0,
                            "status": f"invalid_type: {type(items).__name__}"
                        }
    except:
        # If raw parsing fails, we'll note it in the log
        persistence_log["failure_reasons"]["json_parse_failure"] = 1
    
    entities_root = parsed.get('entities') if isinstance(parsed, dict) and isinstance(parsed.get('entities'), dict) else parsed
    if not isinstance(entities_root, dict):
        persistence_log["failure_reasons"]["not_dict_root"] = 1
        return [], persistence_log

    out_root = Path(__file__).parent / "output"
    ents_root = out_root / "entities"
    rels_root = out_root / "relationships"
    ents_root.mkdir(parents=True, exist_ok=True)
    rels_root.mkdir(parents=True, exist_ok=True)
    
    # Create folders for all entity types from ontology
    for entity_type in UnifiedOntology.ENTITY_TYPES.keys():
        entity_folder = ents_root / entity_type
        entity_folder.mkdir(parents=True, exist_ok=True)

    chunk_id = meta.get('chunkId', 'unknown')
    doc_name = meta.get('document', 'unknown')
    source_file = meta.get('sourceFileName', 'unknown')
    source_path = meta.get('sourceFilePath', 'unknown')

    all_entities = []
    known_entity_types = set(UnifiedOntology.ENTITY_TYPES.keys())

    for etype, ents in entities_root.items():
        # Check if bucket type is valid
        if etype not in known_entity_types:
            persistence_log["failure_reasons"]["invalid_bucket_type"] += 1
            if isinstance(ents, list):
                for e in ents:
                    persistence_log["missing_entities"].append({
                        "entity": e,
                        "bucket": etype,
                        "reason": f"Invalid bucket type '{etype}' not in ontology"
                    })
            continue
            
        if not isinstance(ents, list):
            persistence_log["failure_reasons"]["bucket_not_list"] += 1
            persistence_log["missing_entities"].append({
                "bucket": etype,
                "reason": f"Bucket value is {type(ents).__name__}, not list"
            })
            continue
            
        if not ents:
            persistence_log["failure_reasons"]["empty_bucket"] += 1
            if etype in persistence_log["buckets_summary"]:
                persistence_log["buckets_summary"][etype]["status"] = "empty"
            continue
            
        validated = []
        for idx, e in enumerate(ents):
            if not isinstance(e, dict):
                persistence_log["failure_reasons"]["entity_not_dict"] += 1
                persistence_log["missing_entities"].append({
                    "entity": e,
                    "bucket": etype,
                    "index": idx,
                    "reason": f"Entity is {type(e).__name__}, not dict"
                })
                continue
                
            e = _ensure_id(e, etype)
            try:
                validated_entity = EntityFactory.validate_entity({**e, 'type': etype})
                
                # Add extraction metadata for compatibility with main pipeline
                validated_entity['extraction_chunk_id'] = chunk_id
                validated_entity['extraction_source_file'] = source_file
                validated_entity['entity_type'] = etype
                validated_entity['extracted_at'] = datetime.now().isoformat()
                
                validated.append(validated_entity)
                all_entities.append({**validated_entity, 'type': etype})
                persistence_log["persisted_entities_count"] += 1
                if etype in persistence_log["buckets_summary"]:
                    persistence_log["buckets_summary"][etype]["persisted_count"] += 1
            except Exception as ex:
                persistence_log["failure_reasons"]["validation_failed"] += 1
                persistence_log["missing_entities"].append({
                    "entity": e,
                    "bucket": etype,
                    "index": idx,
                    "reason": f"Validation failed: {str(ex)}"
                })
                continue
                
        if validated:
            payload = {
                "chunkId": chunk_id,
                "document": doc_name,
                "sourceFile": source_file,
                "sourcePath": source_path,
                "entityType": etype,
                "entities": validated,
                "_chunkMetadata": meta,
                "extraction_chunk_id": chunk_id,
                "extraction_source_file": source_file,
                "extracted_at": datetime.now().isoformat()
            }
            out_file = ents_root / etype / f"{chunk_id}_{doc_name}.json"
            out_file.parent.mkdir(parents=True, exist_ok=True)
            out_file.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8')

    # Provenance edges (+ ensure document entity exists via DocumentLinker)
    doc_edges = DocumentLinker.create_document_entity_relationships(all_entities, meta, chunk_id)
    if doc_edges:
        rel_file = rels_root / f"{chunk_id}_{doc_name}.json"
        rel_file.write_text(json.dumps({"relationships": doc_edges}, indent=2, ensure_ascii=False), encoding='utf-8')

    return all_entities, persistence_log


def _group_by_type(flat_entities: list[dict]) -> dict:
    by_type = {}
    for e in flat_entities:
        et = e.get('type')
        if not et:
            continue
        by_type.setdefault(et, []).append(e)
    return by_type


def _build_attr_summary() -> str:
    lines = []
    for et, info in UnifiedOntology.ENTITY_TYPES.items():
        attrs = ", ".join(info.get('attributes', []))
        lines.append(f"{et} must have: {attrs}")
    return "\n".join(lines)


def _persist_relationships(rel_parsed: dict, doc_edges: list[dict], all_entities: list[dict], meta: dict, rel_text: str):
    """Persist relationships with detailed logging of what was kept/dropped"""
    import re  # Ensure re is available in this function scope
    
    relationship_log = {
        "raw_relationships_count": 0,
        "persisted_relationships_count": 0,
        "provenance_edges_count": len(doc_edges),
        "missing_relationships": [],
        "failure_reasons": {
            "json_parse_failure": 0,
            "not_list": 0,
            "not_dict": 0,
            "missing_source": 0,
            "missing_target": 0,
            "source_entity_not_found": 0,
            "target_entity_not_found": 0,
            "invalid_relationship_type": 0,
            "validation_failed": 0
        }
    }
    
    # Build entity lookup
    entity_lookup = {}
    for e in all_entities:
        etype = e.get('type')
        if etype:
            id_field = EntityIDStandards.get_id_field(etype)
            eid = e.get(id_field) or e.get('id')
            if eid:
                entity_lookup[eid] = etype
    
    # Try to count raw relationships
    try:
        raw_parsed = json.loads(rel_text)
        if isinstance(raw_parsed, dict) and 'relationships' in raw_parsed:
            raw_rels = raw_parsed['relationships']
            if isinstance(raw_rels, list):
                relationship_log["raw_relationships_count"] = len(raw_rels)
    except:
        relationship_log["failure_reasons"]["json_parse_failure"] = 1
    
    # Get relationships from parsed result
    relationships = rel_parsed.get('relationships', [])
    if not isinstance(relationships, list):
        relationship_log["failure_reasons"]["not_list"] = 1
        relationships = []
    
    validated_relationships = []
    known_rel_types = set(UnifiedOntology.RELATIONSHIP_TYPES)
    
    for idx, rel in enumerate(relationships):
        if not isinstance(rel, dict):
            relationship_log["failure_reasons"]["not_dict"] += 1
            relationship_log["missing_relationships"].append({
                "index": idx,
                "relationship": rel,
                "reason": f"Relationship is {type(rel).__name__}, not dict"
            })
            continue
            
        source = rel.get('source')
        target = rel.get('target')
        rel_type = rel.get('relationship') or rel.get('type')
        
        if not source:
            relationship_log["failure_reasons"]["missing_source"] += 1
            relationship_log["missing_relationships"].append({
                "index": idx,
                "relationship": rel,
                "reason": "Missing source field"
            })
            continue
            
        if not target:
            relationship_log["failure_reasons"]["missing_target"] += 1
            relationship_log["missing_relationships"].append({
                "index": idx,
                "relationship": rel,
                "reason": "Missing target field"
            })
            continue
            
        # Normalize source/target IDs that may have type prefixes
        normalized_source = source
        normalized_target = target
        
        # Handle common pattern where LLM adds type prefix to existing IDs
        # e.g., "agendaItem_E-1_abcdef" -> "E-1_abcdef"
        if source not in entity_lookup:
            # Try removing common type prefixes
            for prefix in ['agendaItem_', 'agenda_item_', 'person_', 'org_', 'document_', 'policy_', 'section_', 'event_', 'location_', 'role_', 'action_']:
                if source.startswith(prefix):
                    potential_id = source[len(prefix):]
                    if potential_id in entity_lookup:
                        normalized_source = potential_id
                        break
                    # Also try with hyphen instead of underscore for agenda items
                    if prefix in ['agendaItem_', 'agenda_item_'] and '_' in potential_id:
                        hyphen_id = potential_id.replace('_', '-', 1).upper()
                        if hyphen_id in entity_lookup:
                            normalized_source = hyphen_id
                            break
                    # Try converting e1 -> E-1 format
                    if prefix in ['agendaItem_', 'agenda_item_'] and potential_id.lower().startswith('e'):
                        # e1_a1b2c3 -> E-1_a1b2c3
                        match = re.match(r'e(\d+)(.*)', potential_id, re.IGNORECASE)
                        if match:
                            formatted_id = f"E-{match.group(1)}{match.group(2)}"
                            if formatted_id in entity_lookup:
                                normalized_source = formatted_id
                                break
        
        if target not in entity_lookup:
            # Try removing common type prefixes
            for prefix in ['agendaItem_', 'agenda_item_', 'person_', 'org_', 'document_', 'policy_', 'section_', 'event_', 'location_', 'role_', 'action_']:
                if target.startswith(prefix):
                    potential_id = target[len(prefix):]
                    if potential_id in entity_lookup:
                        normalized_target = potential_id
                        break
                    # Also try with hyphen instead of underscore for agenda items
                    if prefix in ['agendaItem_', 'agenda_item_'] and '_' in potential_id:
                        hyphen_id = potential_id.replace('_', '-', 1).upper()
                        if hyphen_id in entity_lookup:
                            normalized_target = hyphen_id
                            break
                    # Try converting e1 -> E-1 format
                    if prefix in ['agendaItem_', 'agenda_item_'] and potential_id.lower().startswith('e'):
                        # e1_a1b2c3 -> E-1_a1b2c3
                        match = re.match(r'e(\d+)(.*)', potential_id, re.IGNORECASE)
                        if match:
                            formatted_id = f"E-{match.group(1)}{match.group(2)}"
                            if formatted_id in entity_lookup:
                                normalized_target = formatted_id
                                break
        
        # Check if normalized source/target exist in our entities
        if normalized_source not in entity_lookup:
            # Try fuzzy matching: check if any entity ID starts with the given ID
            fuzzy_match_source = None
            for entity_id in entity_lookup:
                if entity_id.startswith(normalized_source) or entity_id.startswith(source):
                    fuzzy_match_source = entity_id
                    break
            
            if fuzzy_match_source:
                normalized_source = fuzzy_match_source
            else:
                relationship_log["failure_reasons"]["source_entity_not_found"] += 1
                relationship_log["missing_relationships"].append({
                    "index": idx,
                    "relationship": rel,
                    "reason": f"Source entity '{source}' not found in extracted entities (tried normalizing to '{normalized_source}')"
                })
                continue
            
        if normalized_target not in entity_lookup:
            # Try fuzzy matching: check if any entity ID starts with the given ID
            fuzzy_match_target = None
            for entity_id in entity_lookup:
                if entity_id.startswith(normalized_target) or entity_id.startswith(target):
                    fuzzy_match_target = entity_id
                    break
            
            if fuzzy_match_target:
                normalized_target = fuzzy_match_target
            else:
                relationship_log["failure_reasons"]["target_entity_not_found"] += 1
                relationship_log["missing_relationships"].append({
                    "index": idx,
                    "relationship": rel,
                    "reason": f"Target entity '{target}' not found in extracted entities (tried normalizing to '{normalized_target}')"
                })
                continue
            
        # Normalize relationship type
        normalized_type = rel_type
        if rel_type and rel_type not in known_rel_types:
            # Try lowercase version
            lowercase_type = rel_type.lower()
            # Check if it's an alias
            if lowercase_type in UnifiedOntology.RELATIONSHIP_DEFINITIONS:
                rel_def = UnifiedOntology.RELATIONSHIP_DEFINITIONS[lowercase_type]
                if 'aliasOf' in rel_def:
                    normalized_type = rel_def['aliasOf']
                else:
                    normalized_type = lowercase_type
            else:
                # Check if any known type matches case-insensitively
                for known_type in known_rel_types:
                    if known_type.lower() == lowercase_type:
                        normalized_type = known_type
                        break
        
        if not normalized_type or normalized_type not in known_rel_types:
            relationship_log["failure_reasons"]["invalid_relationship_type"] += 1
            relationship_log["missing_relationships"].append({
                "index": idx,
                "relationship": rel,
                "reason": f"Invalid relationship type '{rel_type}' (normalized attempt: '{normalized_type}')"
            })
            continue
            
        # Build validated relationship with normalized IDs
        validated_rel = {
            "source": normalized_source,
            "target": normalized_target,
            "relationship": normalized_type,
            "source_type": entity_lookup[normalized_source],
            "target_type": entity_lookup[normalized_target]
        }
        
        # Add valid attributes based on relationship schema
        rel_schema = UnifiedOntology.RELATIONSHIP_DEFINITIONS.get(normalized_type, {})
        allowed_attrs = rel_schema.get('attributes', [])
        for attr in allowed_attrs:
            if attr in rel:
                validated_rel[attr] = rel[attr]
        
        validated_relationships.append(validated_rel)
        relationship_log["persisted_relationships_count"] += 1
    
    # Merge with provenance edges
    all_relationships = doc_edges + validated_relationships
    
    # Persist to file
    chunk_id = meta.get('chunkId', 'unknown')
    doc_name = meta.get('document', 'unknown')
    rel_file = Path(__file__).parent / "output" / "relationships" / f"{chunk_id}_{doc_name}.json"
    rel_file.parent.mkdir(parents=True, exist_ok=True)
    rel_file.write_text(json.dumps({"relationships": all_relationships}, indent=2, ensure_ascii=False), encoding='utf-8')
    
    return relationship_log


def _merge_attributes(original: list[dict], patches: list[dict], entity_type: str) -> list[dict]:
    id_field = EntityIDStandards.get_id_field(entity_type)
    lookup = { (e.get(id_field) or e.get('id')): e for e in original if isinstance(e, dict) }
    for p in (patches or []):
        if not isinstance(p, dict):
            continue
        pid = p.get(id_field) or p.get('id')
        if pid and pid in lookup:
            # Merge only non-empty values; keep IDs
            for k, v in p.items():
                if k in (id_field, 'id'):
                    continue
                if v is not None and v != "":
                    lookup[pid][k] = v
    return list(lookup.values())


def extract_entities_split(chunk_text: str, document_type: str, meeting_date: str, source_file: str):
    """
    Extract entities using two separate API calls:
    - First call extracts entity types from ENTITY_TYPE_GROUP_1
    - Second call extracts entity types from ENTITY_TYPE_GROUP_2
    - Results are merged before returning
    """
    import logging
    log = logging.getLogger(__name__)
    
    log.info("🔍 [EXTRACT_ENTITIES_SPLIT] Starting split entity extraction with phase2_NEW")
    log.info(f"   📄 Document type: {document_type}")
    log.info(f"   📅 Meeting date: {meeting_date}")
    log.info(f"   📁 Source file: {source_file}")
    log.info(f"   📝 Chunk text length: {len(chunk_text)} characters")
    log.info(f"   📝 Chunk preview: {chunk_text[:150]}...")
    
    client = get_llm_client()
    model = (os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME") or "").split('"')[0].strip()
    if not model:
        log.error("❌ [EXTRACT_ENTITIES_SPLIT] AZURE_OPENAI_DEPLOYMENT_NAME environment variable not set")
        raise ValueError("AZURE_OPENAI_DEPLOYMENT_NAME environment variable must be set")
    
    log.info(f"🤖 [EXTRACT_ENTITIES_SPLIT] Using LLM model: {model}")

    log.info("📄 [EXTRACT_ENTITIES_SPLIT] Loading prompts and ontology context")
    system_prompt, user_p1, user_p2, user_p3 = load_prompts_from_file()
    ontology_context = ONTOLOGY_FILE.read_text(encoding='utf-8')
    
    log.info(f"   📋 System prompt length: {len(system_prompt)} characters")
    log.info(f"   📋 User prompt 1 length: {len(user_p1)} characters")
    log.info(f"   📋 Relationships template available: {bool(user_p2)}")
    log.info(f"   📋 Attributes template available: {bool(user_p3)}")
    log.info(f"   📋 Ontology context length: {len(ontology_context)} characters")

    # Initialize merged results
    merged_entities = {}
    
    # Process Group 1
    log.info("🔄 [EXTRACT_ENTITIES_SPLIT] Processing Group 1 entity types")
    log.info(f"   🏷️ Group 1 types: {ENTITY_TYPE_GROUP_1}")
    
    user_prompt_1 = (user_p1
        .replace("{DOC_TYPE_TITLE}", str(document_type).replace('_', ' ').title())
        .replace("{MEETING_DATE}", str(meeting_date))
        .replace("{SOURCE_FILE_NAME}", str(source_file))
        .replace("{CHUNK_TEXT_3000}", str(chunk_text[:3000]))
    )
    
    if "{ALL_ENTITY_BUCKETS_JSON_TEMPLATE}" in user_prompt_1:
        buckets_1 = []
        for t in ENTITY_TYPE_GROUP_1:
            buckets_1.append(f'"{t}": []')
        user_prompt_1 = user_prompt_1.replace("{ALL_ENTITY_BUCKETS_JSON_TEMPLATE}", ", ".join(buckets_1))
        log.info(f"   🏷️ Group 1 entity types configured: {len(ENTITY_TYPE_GROUP_1)}")

    # Create a modified prompt instruction for Group 1
    instruction_addon_1 = f"""
IMPORTANT: For this extraction, focus ONLY on these entity types:
{', '.join(ENTITY_TYPE_GROUP_1)}

Ignore all other entity types for now - they will be extracted separately.
"""
    
    user_prompt_full_1 = f"{ontology_context}\n\n{instruction_addon_1}\n\n{user_prompt_1}"
    log.info(f"📋 [EXTRACT_ENTITIES_SPLIT] Group 1 prompt length: {len(user_prompt_full_1)} characters")

    system_prompt_entities_1 = system_prompt.replace("{TASK_NAME}", f"entity extraction for group 1 types: {', '.join(ENTITY_TYPE_GROUP_1)}")
    
    log.info("🚀 [EXTRACT_ENTITIES_SPLIT] Sending Group 1 request to LLM")
    log.info(f"   🤖 Model: {model}")
    log.info(f"   🌡️ Temperature: 0")
    log.info(f"   📏 Max tokens: {os.getenv('MAX_TOKENS', '16384')}")

    response_1 = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt_entities_1},
            {"role": "user", "content": user_prompt_full_1}
        ],
        temperature=0,
        max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
    )
    
    result_text_1 = (response_1.choices[0].message.content or '').strip()
    log.info(f"✅ [EXTRACT_ENTITIES_SPLIT] Received Group 1 response: {len(result_text_1)} characters")
    log.info(f"   📝 Response preview: {result_text_1[:200]}...")
    
    # Parse Group 1 results
    try:
        parsed_1 = json.loads(result_text_1)
        log.info("✅ [EXTRACT_ENTITIES_SPLIT] Group 1 JSON parsing successful")
        
        if isinstance(parsed_1, dict):
            entities_dict_1 = parsed_1.get('entities', parsed_1) if 'entities' in parsed_1 else parsed_1
            
            # Add Group 1 entities to merged results
            for entity_type in ENTITY_TYPE_GROUP_1:
                if entity_type in entities_dict_1:
                    merged_entities[entity_type] = entities_dict_1[entity_type]
                    log.info(f"   📊 {entity_type}: {len(entities_dict_1[entity_type])} entities")
                else:
                    merged_entities[entity_type] = []
                    
    except json.JSONDecodeError as e:
        log.error(f"❌ [EXTRACT_ENTITIES_SPLIT] Group 1 JSON parsing failed: {e}")
        for entity_type in ENTITY_TYPE_GROUP_1:
            merged_entities[entity_type] = []
    
    # Process Group 2
    log.info("🔄 [EXTRACT_ENTITIES_SPLIT] Processing Group 2 entity types")
    log.info(f"   🏷️ Group 2 types: {ENTITY_TYPE_GROUP_2}")
    
    user_prompt_2 = (user_p1
        .replace("{DOC_TYPE_TITLE}", str(document_type).replace('_', ' ').title())
        .replace("{MEETING_DATE}", str(meeting_date))
        .replace("{SOURCE_FILE_NAME}", str(source_file))
        .replace("{CHUNK_TEXT_3000}", str(chunk_text[:3000]))
    )
    
    if "{ALL_ENTITY_BUCKETS_JSON_TEMPLATE}" in user_prompt_2:
        buckets_2 = []
        for t in ENTITY_TYPE_GROUP_2:
            buckets_2.append(f'"{t}": []')
        user_prompt_2 = user_prompt_2.replace("{ALL_ENTITY_BUCKETS_JSON_TEMPLATE}", ", ".join(buckets_2))
        log.info(f"   🏷️ Group 2 entity types configured: {len(ENTITY_TYPE_GROUP_2)}")

    # Create a modified prompt instruction for Group 2
    instruction_addon_2 = f"""
IMPORTANT: For this extraction, focus ONLY on these entity types:
{', '.join(ENTITY_TYPE_GROUP_2)}

Ignore all other entity types for now - they will be extracted separately.
"""
    
    user_prompt_full_2 = f"{ontology_context}\n\n{instruction_addon_2}\n\n{user_prompt_2}"
    log.info(f"📋 [EXTRACT_ENTITIES_SPLIT] Group 2 prompt length: {len(user_prompt_full_2)} characters")

    system_prompt_entities_2 = system_prompt.replace("{TASK_NAME}", f"entity extraction for group 2 types: {', '.join(ENTITY_TYPE_GROUP_2)}")
    
    log.info("🚀 [EXTRACT_ENTITIES_SPLIT] Sending Group 2 request to LLM")

    response_2 = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt_entities_2},
            {"role": "user", "content": user_prompt_full_2}
        ],
        temperature=0,
        max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
    )
    
    result_text_2 = (response_2.choices[0].message.content or '').strip()
    log.info(f"✅ [EXTRACT_ENTITIES_SPLIT] Received Group 2 response: {len(result_text_2)} characters")
    log.info(f"   📝 Response preview: {result_text_2[:200]}...")
    
    # Parse Group 2 results
    try:
        parsed_2 = json.loads(result_text_2)
        log.info("✅ [EXTRACT_ENTITIES_SPLIT] Group 2 JSON parsing successful")
        
        if isinstance(parsed_2, dict):
            entities_dict_2 = parsed_2.get('entities', parsed_2) if 'entities' in parsed_2 else parsed_2
            
            # Add Group 2 entities to merged results
            for entity_type in ENTITY_TYPE_GROUP_2:
                if entity_type in entities_dict_2:
                    merged_entities[entity_type] = entities_dict_2[entity_type]
                    log.info(f"   📊 {entity_type}: {len(entities_dict_2[entity_type])} entities")
                else:
                    merged_entities[entity_type] = []
                    
    except json.JSONDecodeError as e:
        log.error(f"❌ [EXTRACT_ENTITIES_SPLIT] Group 2 JSON parsing failed: {e}")
        for entity_type in ENTITY_TYPE_GROUP_2:
            merged_entities[entity_type] = []
    
    # Handle AgendaDocument entities by merging them into Document
    if 'AgendaDocument' in merged_entities:
        agenda_docs = merged_entities.pop('AgendaDocument', [])
        if isinstance(agenda_docs, list):
            # Convert AgendaDocument entities to Document entities
            doc_list = merged_entities.get('Document', [])
            if not isinstance(doc_list, list):
                doc_list = []
            
            for agenda_doc in agenda_docs:
                if isinstance(agenda_doc, dict):
                    # Convert agendaDocID to documentID
                    if 'agendaDocID' in agenda_doc:
                        agenda_doc['documentID'] = agenda_doc.pop('agendaDocID')
                    # Ensure it's marked as Document type
                    agenda_doc['type'] = 'agenda'
                    agenda_doc['entity_type'] = 'Document'
                    doc_list.append(agenda_doc)
            
            merged_entities['Document'] = doc_list
            log.info(f"   📋 Converted {len(agenda_docs)} AgendaDocument entities to Document type")
    
    # Log final summary
    entities_summary = {}
    total_entities = 0
    for entity_type, entities in merged_entities.items():
        count = len(entities) if isinstance(entities, list) else 0
        entities_summary[entity_type] = count
        total_entities += count
    
    log.info(f"📊 [EXTRACT_ENTITIES_SPLIT] Final merged entities by type: {entities_summary}")
    log.info(f"   📈 Total entities extracted: {total_entities}")
    log.info("✅ [EXTRACT_ENTITIES_SPLIT] Split entity extraction completed")
    
    # Create a combined raw text for logging purposes
    combined_raw_text = json.dumps({"entities": merged_entities}, indent=2, ensure_ascii=False)
    
    return {"entities": merged_entities}, combined_raw_text, user_p2, user_p3, system_prompt


def extract_relationships(chunk_text: str, user_p2: str, system_prompt_base: str, all_entities: list[dict]):
    import logging
    log = logging.getLogger(__name__)
    
    log.info("🔗 [EXTRACT_RELATIONSHIPS] Starting relationship extraction with phase2_NEW")
    log.info(f"   📊 Available entities: {len(all_entities)}")
    log.info(f"   📝 Chunk text length: {len(chunk_text)} characters")
    log.info(f"   📋 Relationship template available: {bool(user_p2)}")
    
    # Build entity references for top 50 entities
    refs = []
    for e in all_entities[:50]:
        etype = e.get('type', 'Unknown')
        name = e.get('name') or e.get('title') or e.get('id')
        id_field = EntityIDStandards.get_id_field(etype)
        eid = e.get(id_field) or e.get('id')
        if eid:
            refs.append(f"{name} (Type: {etype}, ID: {eid})")
    entity_refs = "\n".join(refs)
    log.info(f"   📋 Entity references built: {len(refs)} entities (top 50)")
    
    # Group entities by type for clearer presentation
    entities_by_type = {}
    for e in all_entities:
        etype = e.get('type')
        if etype:
            entities_by_type.setdefault(etype, []).append(e)
    
    entity_type_summary = {etype: len(entities) for etype, entities in entities_by_type.items()}
    log.info(f"   📊 Entities by type: {entity_type_summary}")
    
    # Format entities as JSON for the prompt
    entities_json = json.dumps(entities_by_type, indent=2, ensure_ascii=False)
    log.info(f"   📋 Entities JSON length: {len(entities_json)} characters")

    log.info("🔄 [EXTRACT_RELATIONSHIPS] Building relationship extraction prompt")
    user_rel = (user_p2
        .replace("{ENTITY_REFS_TOP50}", entity_refs)
        .replace("{CHUNK_TEXT_2500}", str(chunk_text[:2500]))
    )
    
    # Add the full entity list to help with ID consistency
    user_rel = f"""EXTRACTED ENTITIES FROM THIS CHUNK (USE THESE EXACT IDs):
{entities_json}

{user_rel}"""

    ontology_context = ONTOLOGY_FILE.read_text(encoding='utf-8')
    user_rel_full = f"{ontology_context}\n\n{user_rel}"
    log.info(f"📋 [EXTRACT_RELATIONSHIPS] Final prompt length: {len(user_rel_full)} characters")

    system_prompt_rel = system_prompt_base.replace("{TASK_NAME}", "relationship extraction with full ontology")

    client = get_llm_client()
    model = (os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME") or "").split('"')[0].strip()
    
    log.info("🚀 [EXTRACT_RELATIONSHIPS] Sending request to LLM")
    log.info(f"   🤖 Model: {model}")
    log.info(f"   🌡️ Temperature: 0")
    log.info(f"   📏 Max tokens: {os.getenv('MAX_TOKENS', '16384')}")

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt_rel},
            {"role": "user", "content": user_rel_full}
        ],
        temperature=0,
        max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
    )
    
    rel_text = (response.choices[0].message.content or '').strip()
    log.info(f"✅ [EXTRACT_RELATIONSHIPS] Received LLM response: {len(rel_text)} characters")
    log.info(f"   📝 Response preview: {rel_text[:200]}...")
    
    log.info("🔍 [EXTRACT_RELATIONSHIPS] Parsing JSON response")
    try:
        rel_parsed = json.loads(rel_text)
        log.info("✅ [EXTRACT_RELATIONSHIPS] JSON parsing successful")
        
        if isinstance(rel_parsed, dict) and 'relationships' in rel_parsed:
            relationships_count = len(rel_parsed['relationships'])
            log.info(f"   🔗 Relationships extracted: {relationships_count}")
            
            # Log relationship types summary
            rel_types = {}
            for rel in rel_parsed['relationships']:
                if isinstance(rel, dict):
                    rel_type = rel.get('relationship') or rel.get('type', 'unknown')
                    rel_types[rel_type] = rel_types.get(rel_type, 0) + 1
            log.info(f"   📊 Relationship types: {rel_types}")
        else:
            log.warning(f"⚠️ [EXTRACT_RELATIONSHIPS] Unexpected response structure: {list(rel_parsed.keys()) if isinstance(rel_parsed, dict) else type(rel_parsed)}")
            
    except json.JSONDecodeError as e:
        log.error(f"❌ [EXTRACT_RELATIONSHIPS] JSON parsing failed: {e}")
        log.error(f"   📝 Raw response: {rel_text[:500]}...")
        rel_parsed = {"relationships": []}
    
    log.info("✅ [EXTRACT_RELATIONSHIPS] Relationship extraction completed")
    return rel_parsed, rel_text


def extract_attributes(chunk_text: str, user_p3: str, system_prompt_base: str, by_type_entities: dict) -> tuple[dict, str]:
    import logging
    log = logging.getLogger(__name__)
    
    log.info("🏷️ [EXTRACT_ATTRIBUTES] Starting attribute extraction with phase2_NEW")
    
    if not user_p3:
        log.info("ℹ️ [EXTRACT_ATTRIBUTES] No attribute template provided, skipping attribute extraction")
        return {}, ""
    
    log.info(f"   📊 Entity types to enhance: {len(by_type_entities)}")
    total_entities = sum(len(entities) for entities in by_type_entities.values())
    log.info(f"   📈 Total entities to enhance: {total_entities}")
    log.info(f"   📝 Chunk text length: {len(chunk_text)} characters")
    
    client = get_llm_client()
    model = (os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME") or "").split('"')[0].strip()
    ontology_context = ONTOLOGY_FILE.read_text(encoding='utf-8')
    attr_summary = _build_attr_summary()
    
    log.info(f"🤖 [EXTRACT_ATTRIBUTES] Using LLM model: {model}")
    log.info(f"📋 [EXTRACT_ATTRIBUTES] Attribute summary length: {len(attr_summary)} characters")

    enhanced: dict = {}
    raw_blocks = []
    processed_types = 0
    total_enhanced = 0

    for etype, ents in by_type_entities.items():
        if not ents:
            log.info(f"⏭️ [EXTRACT_ATTRIBUTES] Skipping {etype}: no entities")
            continue
        
        processed_types += 1
        log.info(f"🔄 [EXTRACT_ATTRIBUTES] Processing {etype}: {len(ents)} entities ({processed_types}/{len(by_type_entities)})")
        
        expected_attrs = UnifiedOntology.ENTITY_TYPES.get(etype, {}).get('attributes', [])
        log.info(f"   🏷️ Expected attributes for {etype}: {expected_attrs}")
        
        user_attr = (user_p3
            .replace("{ENTITY_ATTRIBUTE_SUMMARY}", attr_summary)
            .replace("{ENTITY_TYPE}", etype)
            .replace("{ENTITY_LIST_JSON}", json.dumps(ents, ensure_ascii=False, indent=2))
            .replace("{EXPECTED_ATTRS_LIST}", ", ".join(expected_attrs))
            .replace("{CHUNK_TEXT_2000}", str(chunk_text[:2000]))
        )
        user_attr_full = f"{ontology_context}\n\n{user_attr}"
        log.info(f"   📋 Attribute prompt length: {len(user_attr_full)} characters")
        
        system_prompt_attr = system_prompt_base.replace("{TASK_NAME}", f"attribute enhancement for {etype}")
        
        log.info(f"🚀 [EXTRACT_ATTRIBUTES] Sending {etype} request to LLM")
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt_attr},
                {"role": "user", "content": user_attr_full}
            ],
            temperature=0,
            max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
        )
        
        txt = (resp.choices[0].message.content or '').strip()
        log.info(f"✅ [EXTRACT_ATTRIBUTES] Received {etype} response: {len(txt)} characters")
        
        raw_blocks.append(f"=== {etype} ===\n{txt}\n")
        
        log.info(f"🔍 [EXTRACT_ATTRIBUTES] Parsing {etype} JSON response")
        try:
            patches = json.loads(txt)
            log.info(f"✅ [EXTRACT_ATTRIBUTES] {etype} JSON parsing successful")
            patches_count = len(patches) if isinstance(patches, list) else 0
            log.info(f"   🏷️ Attribute patches received: {patches_count}")
        except json.JSONDecodeError as e:
            log.error(f"❌ [EXTRACT_ATTRIBUTES] {etype} JSON parsing failed: {e}")
            patches = []
        
        merged = _merge_attributes(ents, patches if isinstance(patches, list) else [], etype)
        enhanced[etype] = merged
        total_enhanced += len(merged)
        
        log.info(f"✅ [EXTRACT_ATTRIBUTES] {etype} attribute enhancement completed: {len(merged)} enhanced entities")

    log.info(f"📊 [EXTRACT_ATTRIBUTES] Attribute extraction summary:")
    log.info(f"   📂 Entity types processed: {processed_types}")
    log.info(f"   📈 Total entities enhanced: {total_enhanced}")
    log.info(f"   📄 Raw response blocks: {len(raw_blocks)}")
    log.info("✅ [EXTRACT_ATTRIBUTES] Attribute extraction completed")

    return enhanced, "\n".join(raw_blocks)


# Alias for backward compatibility - the main pipeline can still use extract_entities
# but it will use the split implementation
extract_entities = extract_entities_split

# Export the persist function for use by the adapter
__all__ = [
    'extract_entities',
    'extract_entities_split',
    'extract_relationships',
    'extract_attributes',
    'parse_chunk_file',
    '_group_by_type',
    '_persist_phase2_new'
]


if __name__ == "__main__":
    # Process all chunks from the main pipeline
    chunks_dir = Path(__file__).parents[3] / "simple_ner_graph/document_chunks"
    
    if not chunks_dir.exists():
        print(f"Error: Chunks directory not found: {chunks_dir}")
        exit(1)
    
    chunk_files = list(chunks_dir.glob("*.txt"))
    if not chunk_files:
        print(f"Error: No chunk files found in {chunks_dir}")
        exit(1)
    
    print(f"Found {len(chunk_files)} chunks to process")
    
    # Initialize cumulative logs
    cumulative_entity_log = []
    cumulative_relationship_log = []
    total_raw_entities = 0
    total_persisted_entities = 0
    total_raw_relationships = 0
    total_persisted_relationships = 0
    
    # Process each chunk
    for i, chunk_path in enumerate(chunk_files, 1):
        print(f"\n{'='*60}")
        print(f"Processing chunk {i}/{len(chunk_files)}: {chunk_path.name}")
        print(f"{'='*60}")
        
        try:
            meta, text = parse_chunk_file(str(chunk_path))
            
            # Entities call (now using split extraction)
            result, raw_text, rel_template, attr_template, sys_prompt = extract_entities_split(
                text,
                document_type=meta.get('documentType', 'unknown'),
                meeting_date=meta.get('meetingDate', 'unknown'),
                source_file=meta.get('sourceFileName', 'unknown'),
            )
            
            # Persist entities/provenance and get normalized flat list for relationships prompt
            norm_flat, persistence_log = _persist_phase2_new(meta, result, raw_text)
            
            # Update cumulative counts
            total_raw_entities += persistence_log['raw_entities_count']
            total_persisted_entities += persistence_log['persisted_entities_count']
            
            # Log chunk results
            cumulative_entity_log.append(f"\n=== CHUNK: {chunk_path.name} ===")
            cumulative_entity_log.append(f"Raw entities: {persistence_log['raw_entities_count']}")
            cumulative_entity_log.append(f"Persisted: {persistence_log['persisted_entities_count']}")
            
            # Relationships call (if template available)
            if rel_template and norm_flat:
                rel_parsed, rel_text = extract_relationships(text, rel_template, sys_prompt, norm_flat)
                
                # Get provenance edges that were already persisted
                chunk_id = meta.get('chunkId', 'unknown')
                doc_name = meta.get('document', 'unknown')
                rel_file = Path(__file__).parent / "output" / "relationships" / f"{chunk_id}_{doc_name}.json"
                existing_doc_edges = []
                if rel_file.exists():
                    try:
                        existing_data = json.loads(rel_file.read_text(encoding='utf-8'))
                        existing_doc_edges = existing_data.get('relationships', [])
                    except:
                        pass
                
                # Persist relationships with logging
                rel_log = _persist_relationships(rel_parsed, existing_doc_edges, norm_flat, meta, rel_text)
                
                # Update cumulative counts
                total_raw_relationships += rel_log['raw_relationships_count']
                total_persisted_relationships += rel_log['persisted_relationships_count']
                
                cumulative_relationship_log.append(f"\n=== CHUNK: {chunk_path.name} ===")
                cumulative_relationship_log.append(f"Raw relationships: {rel_log['raw_relationships_count']}")
                cumulative_relationship_log.append(f"Persisted: {rel_log['persisted_relationships_count']}")
            
            # Attributes/enrichment call (if template available)
            by_type = _group_by_type(norm_flat)
            if attr_template and by_type:
                enhanced_by_type, attr_raw = extract_attributes(text, attr_template, sys_prompt, by_type)
                
                # Re-persist enhanced entities if we got any enhancements
                if enhanced_by_type:
                    enhanced_count = 0
                    for etype, enhanced_entities in enhanced_by_type.items():
                        if enhanced_entities:
                            # Update the envelope with enhanced entities
                            out_file = Path(__file__).parent / "output" / "entities" / etype / f"{chunk_id}_{doc_name}.json"
                            if out_file.exists():
                                try:
                                    existing_data = json.loads(out_file.read_text(encoding='utf-8'))
                                    existing_data["entities"] = enhanced_entities
                                    existing_data["_enhanced"] = True
                                    out_file.write_text(json.dumps(existing_data, indent=2, ensure_ascii=False), encoding='utf-8')
                                    enhanced_count += len(enhanced_entities)
                                except Exception as e:
                                    print(f"Failed to update {etype} entities: {e}")
            
            print(f"Chunk {i} completed: {persistence_log['persisted_entities_count']} entities persisted")
            
        except Exception as e:
            print(f"Error processing chunk {i} ({chunk_path.name}): {e}")
            cumulative_entity_log.append(f"\n=== CHUNK: {chunk_path.name} ===")
            cumulative_entity_log.append(f"ERROR: {str(e)}")
    
    # Save cumulative logs
    final_entity_log = [
        "=== CUMULATIVE ENTITY PERSISTENCE LOG ===",
        f"Total chunks processed: {len(chunk_files)}",
        f"Total raw entities: {total_raw_entities}",
        f"Total persisted entities: {total_persisted_entities}",
        f"Total missing entities: {total_raw_entities - total_persisted_entities}",
        "",
        "=== PER-CHUNK BREAKDOWN ==="
    ] + cumulative_entity_log
    
    (Path(__file__).parent / "entity_persistence_log.txt").write_text(
        "\n".join(final_entity_log), 
        encoding='utf-8'
    )
    
    final_relationship_log = [
        "=== CUMULATIVE RELATIONSHIP PERSISTENCE LOG ===",
        f"Total chunks processed: {len(chunk_files)}",
        f"Total raw relationships: {total_raw_relationships}",
        f"Total persisted relationships: {total_persisted_relationships}",
        f"Total missing relationships: {total_raw_relationships - total_persisted_relationships}",
        "",
        "=== PER-CHUNK BREAKDOWN ==="
    ] + cumulative_relationship_log
    
    (Path(__file__).parent / "relationship_persistence_log.txt").write_text(
        "\n".join(final_relationship_log), 
        encoding='utf-8'
    )
    
    print(f"\n{'='*60}")
    print(f"=== FINAL SUMMARY ===")
    print(f"{'='*60}")
    print(f"Total chunks processed: {len(chunk_files)}")
    print(f"Total entities persisted: {total_persisted_entities}")
    print(f"Total relationships persisted: {total_persisted_relationships}")
    print(f"\nDetailed logs saved to:")
    print(f"  - entity_persistence_log.txt")
    print(f"  - relationship_persistence_log.txt")
