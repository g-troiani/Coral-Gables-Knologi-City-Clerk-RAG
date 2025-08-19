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


def load_prompts_from_file() -> tuple[str, str, str]:
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
                        import re
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


def extract_entities(chunk_text: str, document_type: str, meeting_date: str, source_file: str):
    client = get_llm_client()
    model = (os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME") or "").split('"')[0].strip()
    if not model:
        raise ValueError("AZURE_OPENAI_DEPLOYMENT_NAME environment variable must be set")

    system_prompt, user_p1, user_p2, user_p3 = load_prompts_from_file()
    ontology_context = ONTOLOGY_FILE.read_text(encoding='utf-8')

    # Fill Prompt 1
    user_prompt = (user_p1
        .replace("{DOC_TYPE_TITLE}", str(document_type).replace('_', ' ').title())
        .replace("{MEETING_DATE}", str(meeting_date))
        .replace("{SOURCE_FILE_NAME}", str(source_file))
        .replace("{CHUNK_TEXT_3000}", str(chunk_text[:3000]))
    )
    if "{ALL_ENTITY_BUCKETS_JSON_TEMPLATE}" in user_prompt:
        buckets = []
        buckets_types = [
            "Person","Organization","Document","AgendaDocument","Section","AgendaItem",
            "Policy","Contract","Technology","VoteOutcome","Event","Location","Asset","Project","Role","Topic","Action"
        ]
        for t in buckets_types:
            buckets.append(f'"{t}": []')
        user_prompt = user_prompt.replace("{ALL_ENTITY_BUCKETS_JSON_TEMPLATE}", ", ".join(buckets))

    user_prompt_full = f"{ontology_context}\n\n{user_prompt}"

    system_prompt_entities = system_prompt.replace("{TASK_NAME}", "entity extraction with full ontology")

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt_entities},
            {"role": "user", "content": user_prompt_full}
        ],
        temperature=0,
        max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
    )
    result_text = (response.choices[0].message.content or '').strip()
    try:
        parsed = json.loads(result_text)
    except json.JSONDecodeError:
        parsed = {"entities": {}, "relationships": []}
    return parsed, result_text, user_p2, user_p3, system_prompt


def extract_relationships(chunk_text: str, user_p2: str, system_prompt_base: str, all_entities: list[dict]):
    refs = []
    for e in all_entities[:50]:
        etype = e.get('type', 'Unknown')
        name = e.get('name') or e.get('title') or e.get('id')
        id_field = EntityIDStandards.get_id_field(etype)
        eid = e.get(id_field) or e.get('id')
        if eid:
            refs.append(f"{name} (Type: {etype}, ID: {eid})")
    entity_refs = "\n".join(refs)
    
    # Group entities by type for clearer presentation
    entities_by_type = {}
    for e in all_entities:
        etype = e.get('type')
        if etype:
            entities_by_type.setdefault(etype, []).append(e)
    
    # Format entities as JSON for the prompt
    entities_json = json.dumps(entities_by_type, indent=2, ensure_ascii=False)

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

    system_prompt_rel = system_prompt_base.replace("{TASK_NAME}", "relationship extraction with full ontology")

    client = get_llm_client()
    model = (os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME") or "").split('"')[0].strip()

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
    try:
        rel_parsed = json.loads(rel_text)
    except json.JSONDecodeError:
        rel_parsed = {"relationships": []}
    return rel_parsed, rel_text


def extract_attributes(chunk_text: str, user_p3: str, system_prompt_base: str, by_type_entities: dict) -> tuple[dict, str]:
    if not user_p3:
        return {}, ""
    client = get_llm_client()
    model = (os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME") or "").split('"')[0].strip()
    ontology_context = ONTOLOGY_FILE.read_text(encoding='utf-8')
    attr_summary = _build_attr_summary()

    enhanced: dict = {}
    raw_blocks = []

    for etype, ents in by_type_entities.items():
        if not ents:
            continue
        expected_attrs = UnifiedOntology.ENTITY_TYPES.get(etype, {}).get('attributes', [])
        user_attr = (user_p3
            .replace("{ENTITY_ATTRIBUTE_SUMMARY}", attr_summary)
            .replace("{ENTITY_TYPE}", etype)
            .replace("{ENTITY_LIST_JSON}", json.dumps(ents, ensure_ascii=False, indent=2))
            .replace("{EXPECTED_ATTRS_LIST}", ", ".join(expected_attrs))
            .replace("{CHUNK_TEXT_2000}", str(chunk_text[:2000]))
        )
        user_attr_full = f"{ontology_context}\n\n{user_attr}"
        system_prompt_attr = system_prompt_base.replace("{TASK_NAME}", f"attribute enhancement for {etype}")
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
        raw_blocks.append(f"=== {etype} ===\n{txt}\n")
        try:
            patches = json.loads(txt)
        except json.JSONDecodeError:
            patches = []
        merged = _merge_attributes(ents, patches if isinstance(patches, list) else [], etype)
        enhanced[etype] = merged

    return enhanced, "\n".join(raw_blocks)


if __name__ == "__main__":
    chunk_path = Path(__file__).parents[3] / "simple_ner_graph/document_chunks/461881bb58f6_agenda_01_09_2024.txt"
    meta, text = parse_chunk_file(str(chunk_path))

    # Entities call
    result, raw_text, rel_template, attr_template, sys_prompt = extract_entities(
        text,
        document_type=meta.get('documentType', 'unknown'),
        meeting_date=meta.get('meetingDate', 'unknown'),
        source_file=meta.get('sourceFileName', 'unknown'),
    )
    (Path(__file__).parent / "llm_entity_extraction_output.txt").write_text(raw_text, encoding='utf-8')

    # Persist entities/provenance and get normalized flat list for relationships prompt
    norm_flat, persistence_log = _persist_phase2_new(meta, result, raw_text)
    
    # Save persistence log
    log_content = [
        "=== ENTITY PERSISTENCE LOG ===",
        f"Raw entities count: {persistence_log['raw_entities_count']}",
        f"Persisted entities count: {persistence_log['persisted_entities_count']}",
        f"Missing entities: {persistence_log['raw_entities_count'] - persistence_log['persisted_entities_count']}",
        "",
        "=== FAILURE REASONS SUMMARY ===",
        ""
    ]
    
    for reason, count in persistence_log['failure_reasons'].items():
        if count > 0:
            log_content.append(f"{reason}: {count}")
    
    log_content.extend([
        "",
        "=== ALL BUCKETS SUMMARY ===",
        ""
    ])
    
    # Sort buckets alphabetically for consistent output
    for bucket in sorted(persistence_log['buckets_summary'].keys()):
        info = persistence_log['buckets_summary'][bucket]
        log_content.append(f"{bucket}: {info['raw_count']} raw, {info['persisted_count']} persisted (status: {info['status']})")
    
    log_content.extend([
        "",
        "=== DETAILED MISSING ENTITIES ===",
        ""
    ])
    
    for missing in persistence_log['missing_entities']:
        log_content.append(f"Bucket: {missing.get('bucket', 'unknown')}")
        if 'index' in missing:
            log_content.append(f"  Index: {missing['index']}")
        if 'entity' in missing:
            log_content.append(f"  Entity: {json.dumps(missing['entity'], indent=4, ensure_ascii=False)}")
        log_content.append(f"  Reason: {missing['reason']}")
        log_content.append("")
    
    (Path(__file__).parent / "entity_persistence_log.txt").write_text(
        "\n".join(log_content), 
        encoding='utf-8'
    )

    # Relationships call (if template available)
    if rel_template:
        rel_parsed, rel_text = extract_relationships(text, rel_template, sys_prompt, norm_flat)
        (Path(__file__).parent / "lll_relationship_extraction_output.txt").write_text(rel_text, encoding='utf-8')
        
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
        
        # Save relationship persistence log
        rel_log_content = [
            "=== RELATIONSHIP PERSISTENCE LOG ===",
            f"Raw relationships count: {rel_log['raw_relationships_count']}",
            f"Persisted domain relationships: {rel_log['persisted_relationships_count']}",
            f"Provenance edges added: {rel_log['provenance_edges_count']}",
            f"Total relationships persisted: {rel_log['persisted_relationships_count'] + rel_log['provenance_edges_count']}",
            f"Missing relationships: {rel_log['raw_relationships_count'] - rel_log['persisted_relationships_count']}",
            "",
            "=== FAILURE REASONS SUMMARY ===",
            ""
        ]
        
        for reason, count in rel_log['failure_reasons'].items():
            if count > 0:
                rel_log_content.append(f"{reason}: {count}")
        
        rel_log_content.extend([
            "",
            "=== DETAILED MISSING RELATIONSHIPS ===",
            ""
        ])
        
        for missing in rel_log['missing_relationships']:
            rel_log_content.append(f"Index: {missing.get('index', 'unknown')}")
            if 'relationship' in missing:
                rel_log_content.append(f"  Relationship: {json.dumps(missing['relationship'], indent=4, ensure_ascii=False)}")
            rel_log_content.append(f"  Reason: {missing['reason']}")
            rel_log_content.append("")
        
        (Path(__file__).parent / "relationship_persistence_log.txt").write_text(
            "\n".join(rel_log_content), 
            encoding='utf-8'
        )

    # Attributes/enrichment call (if template available)
    by_type = _group_by_type(norm_flat)
    if attr_template:
        enhanced_by_type, attr_raw = extract_attributes(text, attr_template, sys_prompt, by_type)
        (Path(__file__).parent / "lll_attribute_extraction_output.txt").write_text(attr_raw, encoding='utf-8')
        
        # Re-persist enhanced entities if we got any enhancements
        if enhanced_by_type:
            print("\n=== RE-PERSISTING ENHANCED ENTITIES ===")
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
            print(f"Enhanced {enhanced_count} entities with additional attributes")

    print("\n=== LLM RAW RESULT ===\n" + json.dumps(result, indent=2, ensure_ascii=False))
    print(f"\n=== PERSISTENCE SUMMARY ===")
    print(f"Raw entities: {persistence_log['raw_entities_count']}")
    print(f"Persisted: {persistence_log['persisted_entities_count']}")
    print(f"Missing: {persistence_log['raw_entities_count'] - persistence_log['persisted_entities_count']}")
    print(f"\nDetailed log saved to: entity_persistence_log.txt")
