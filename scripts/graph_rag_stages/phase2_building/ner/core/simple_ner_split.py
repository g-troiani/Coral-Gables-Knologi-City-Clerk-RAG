#!/usr/bin/env python3

import os
import json
from pathlib import Path
import sys
from dotenv import load_dotenv
import concurrent.futures
import asyncio
import time
import threading
import psutil
import gc
import logging

# Ensure project root is on sys.path for package imports
_PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.graph_rag_stages.common.utils import get_llm_client
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
from scripts.graph_rag_stages.common.entity_factory import EntityFactory
from scripts.graph_rag_stages.common.document_linker import DocumentLinker
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
from scripts.graph_rag_stages.phase2_building.ner.core.simple_ner_consolidated import PerformanceMonitor
import hashlib
import re
from datetime import datetime

load_dotenv()

# Rate limiting configuration - OPTIMIZED FOR PERFORMANCE
MAX_CONCURRENT_CALLS = 30  # Increased for better parallel processing
_rate_limit_semaphore = None
_last_call_times = []
MIN_CALL_INTERVAL = 0.01  # Reduced from 0.1s to 10ms for faster processing

def _calculate_max_chunk_size() -> int:
    """
    Calculate maximum chunk size based on MAX_TOKENS environment variable.
    Accounts for prompt overhead, response tokens, and safety margin.
    """
    max_tokens = int(os.getenv('MAX_TOKENS', '16384'))
    chars_per_token = 3.5  # Conservative estimate
    prompt_overhead = 2000  # Tokens for prompt, ontology, instructions
    response_tokens = 1500  # Tokens needed for LLM response
    safety_margin = 500    # Safety buffer
    
    available_tokens = max_tokens - prompt_overhead - response_tokens - safety_margin
    max_chunk_chars = int(available_tokens * chars_per_token)
    
    # Ensure we have a reasonable minimum
    return max(max_chunk_chars, 8000)

def _get_rate_limiter():
    """Get or create the global rate limiting semaphore"""
    global _rate_limit_semaphore
    if _rate_limit_semaphore is None:
        _rate_limit_semaphore = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_CALLS)
    return _rate_limit_semaphore

def _apply_rate_limit():
    """Apply optimized rate limiting between API calls with performance monitoring"""
    global _last_call_times
    rate_limit_start = time.time()
    current_time = time.time()
    log = logging.getLogger(__name__)
    
    # Remove old timestamps (older than 60 seconds)
    old_count = len(_last_call_times)
    _last_call_times = [t for t in _last_call_times if current_time - t < 60]
    cleaned_count = old_count - len(_last_call_times)
    
    if cleaned_count > 0:
        log.debug(f"🧹 [RATE_LIMIT] Cleaned {cleaned_count} old timestamps")
    
    # If we have recent calls, apply minimum interval
    if _last_call_times:
        time_since_last = current_time - _last_call_times[-1]
        if time_since_last < MIN_CALL_INTERVAL:
            sleep_time = MIN_CALL_INTERVAL - time_since_last
            
            # Log rate limit wait if significant
            if sleep_time > 0.05:  # Only log waits > 50ms
                PerformanceMonitor.log_rate_limit_wait(log, sleep_time)
                log.info(f"   📊 Recent calls: {len(_last_call_times)}, Time since last: {time_since_last:.3f}s")
                
            time.sleep(sleep_time)  # Minimal sleep time for performance
            
            # Log actual wait time
            actual_wait_time = time.time() - rate_limit_start
            if actual_wait_time > 0.05:
                log.info(f"   🛑 Rate limit wait completed: {actual_wait_time:.3f}s")
        else:
            log.debug(f"🚀 [RATE_LIMIT] No wait needed, {time_since_last:.3f}s since last call")
    
    # Record this call
    _last_call_times.append(time.time())
    log.debug(f"📊 [RATE_LIMIT] Total tracked calls: {len(_last_call_times)}")

PROMPT_FILE = Path(__file__).parent / "ner_prompt.txt"
ONTOLOGY_FILE = Path(__file__).parent / "ontology_context_camelCase.txt"

# Define entity type groups - split into three balanced groups
ENTITY_TYPE_GROUP_1 = [
    "Person", "Organization", "Role", "Meeting", "Event", "Action", "VoteOutcome"
]

ENTITY_TYPE_GROUP_2 = [
    "Document", "Section", "AgendaItem", "Policy", "Contract", 
    "Presentation", "PublicComment", "LegalReference"
]

ENTITY_TYPE_GROUP_3 = [
    "Location", "Asset", "Project", "Topic", "Technology", "Board", "Appointment"
]


def get_relationships_for_group(entity_group: list[str]) -> set[str]:
    """Get relationships where source or target is in the entity group."""
    relevant_relationships = set()
    
    for rel_name, rel_def in UnifiedOntology.RELATIONSHIP_DEFINITIONS.items():
        source_types = rel_def.get('source', [])
        target_types = rel_def.get('target', [])
        
        # Normalize to lists
        if isinstance(source_types, str):
            source_types = [source_types]
        if isinstance(target_types, str):
            target_types = [target_types]
        
        # Check if any source or target is in our group
        if any(s in entity_group for s in source_types) or \
           any(t in entity_group for t in target_types):
            relevant_relationships.add(rel_name)
    
    return relevant_relationships


def build_focused_ontology_context(entity_group: list[str], group_name: str) -> str:
    """Build ontology context focused on specific entity types and their relationships."""
    lines = [f"FOCUSED ONTOLOGY FOR {group_name}",
             "="*50,
             "",
             "ENTITY TYPES:",
             ""]
    
    # Add entity definitions for this group
    for entity_type in sorted(entity_group):
        if entity_type in UnifiedOntology.ENTITY_TYPES:
            entity_def = UnifiedOntology.ENTITY_TYPES[entity_type]
            lines.append(f"## {entity_type}")
            lines.append(f"Definition: {entity_def['definition']}")
            lines.append(f"Attributes: {', '.join(entity_def['attributes'])}")
            lines.append(f"Examples: {', '.join(entity_def.get('examples', []))}")
            lines.append("")
    
    # Add relevant relationships
    lines.extend(["", "RELATIONSHIPS:", ""])
    relevant_rels = get_relationships_for_group(entity_group)
    
    for rel_name in sorted(relevant_rels):
        if rel_name in UnifiedOntology.RELATIONSHIP_DEFINITIONS:
            rel_def = UnifiedOntology.RELATIONSHIP_DEFINITIONS[rel_name]
            source = rel_def.get('source', 'Unknown')
            target = rel_def.get('target', 'Unknown')
            attrs = rel_def.get('attributes', [])
            lines.append(f"## {rel_name}")
            lines.append(f"From: {source} → To: {target}")
            if attrs:
                lines.append(f"Attributes: {', '.join(attrs)}")
            lines.append("")
    
    return "\n".join(lines)


def merge_entities_advanced(entities_dict_1: dict, entities_dict_2: dict, entities_dict_3: dict) -> dict:
    """Advanced merge logic with deduplication for 3 groups."""
    merged_entities = {}
    
    # Merge entities from all three groups
    all_entity_dicts = [entities_dict_1, entities_dict_2, entities_dict_3]
    
    # Get all entity types from all groups
    all_entity_types = set()
    for entity_dict in all_entity_dicts:
        all_entity_types.update(entity_dict.keys())
    
    # Merge each entity type
    for entity_type in all_entity_types:
        merged_list = []
        seen_ids = set()
        
        # Collect entities from all groups for this type
        for entity_dict in all_entity_dicts:
            entities = entity_dict.get(entity_type, [])
            if isinstance(entities, list):
                for entity in entities:
                    if isinstance(entity, dict):
                        # Get entity ID for deduplication
                        entity_id = entity.get('id') or entity.get(EntityIDStandards.get_id_field(entity_type))
                        if entity_id and entity_id not in seen_ids:
                            seen_ids.add(entity_id)
                            merged_list.append(entity)
                        elif not entity_id:
                            # If no ID, include anyway (will get ID assigned later)
                            merged_list.append(entity)
        
        merged_entities[entity_type] = merged_list
    
    return merged_entities


def load_prompts_from_file() -> tuple[str, str, str, str]:
    text = PROMPT_FILE.read_text(encoding='utf-8')
    
    # Check if this is the new triple format
    if "=== TRIPLE EXTRACTION PROMPT ===" in text:
        # New triple format - extract system and user parts
        parts = text.split("=== TRIPLE EXTRACTION PROMPT ===")
        system_part = parts[0].replace("SYSTEM TEMPLATE", "").strip()
        user_part = parts[1].strip() if len(parts) > 1 else ""
        # Return triple format as single prompt (no separate relationship/attribute prompts)
        return system_part, user_part, "", ""
    else:
        # Legacy three-phase format
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
    
    # Special handling for AgendaItem
    if entity_type == "AgendaItem":
        # Standardize agenda item formats: E-4, E.4, E 4 → e4
        s = re.sub(r'\b([a-z])\s*[-.\s]\s*(\d+)\b', r'\1\2', s)
        # Also handle itemNumber patterns like "item e-4" → "item e4"
        s = re.sub(r'\bitem\s+([a-z])\s*[-.\s]\s*(\d+)\b', r'item_\1\2', s)
    
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


def _format_date_suffix(meeting_date: str) -> str:
    """Format meeting date as MM_DD_YYYY suffix for AgendaItems."""
    if not meeting_date:
        return "unknown_date"
    
    # Handle various date formats and convert to MM_DD_YYYY
    date_str = str(meeting_date).strip()
    
    # Common patterns: 01.09.2024, 2024-01-09, 01/09/2024, etc.
    import re
    
    # Pattern 1: MM.DD.YYYY (e.g., "01.09.2024") - US format with dots
    match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
    if match:
        month, day, year = match.groups()
        return f"{year}_{month.zfill(2)}_{day.zfill(2)}"
    
    # Pattern 2: YYYY-MM-DD (e.g., "2024-01-09")  
    match = re.match(r'(\d{4})-(\d{1,2})-(\d{1,2})', date_str)
    if match:
        year, month, day = match.groups()
        return f"{year}_{month.zfill(2)}_{day.zfill(2)}"
    
    # Pattern 3: MM/DD/YYYY (e.g., "01/09/2024")
    match = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
    if match:
        month, day, year = match.groups()
        return f"{year}_{month.zfill(2)}_{day.zfill(2)}"
    
    # Pattern 4: Already in YYYY_MM_DD format
    if re.match(r'\d{4}_\d{2}_\d{2}', date_str):
        return date_str
    
    # Pattern 5: Handle old MM_DD_YYYY format and convert
    if re.match(r'\d{2}_\d{2}_\d{4}', date_str):
        parts = date_str.split('_')
        return f"{parts[2]}_{parts[0]}_{parts[1]}"
    
    # Fallback: try to extract year and use generic format
    year_match = re.search(r'(20\d{2})', date_str)
    if year_match:
        year = year_match.group(1)
        return f"{year}_01_09"  # Default to Jan 9 if can't parse month/day
    
    return "unknown_date"


def _ensure_id(entity: dict, entity_type: str) -> dict:
    normalized = EntityIDStandards.normalize_entity_id_fields(entity, entity_type)
    id_field = EntityIDStandards.get_id_field(entity_type)
    if not normalized.get(id_field):
        if entity_type == "Policy":
            preferred = EntityIDStandards.preferred_policy_id(normalized)
            if preferred:
                normalized[id_field] = preferred.lower()  # Ensure lowercase
                normalized['id'] = preferred.lower()
                return normalized
        if entity_type == "AgendaItem":
            preferred = EntityIDStandards.preferred_agendaitem_id(normalized)
            if preferred:
                normalized[id_field] = preferred.lower()  # Ensure lowercase
                normalized['id'] = preferred.lower()
                return normalized
        
        # For AgendaItem, prioritize itemNumber or itemID fields
        if entity_type == "AgendaItem":
            base_name = (normalized.get("itemNumber") or 
                        normalized.get("itemID") or 
                        normalized.get("name") or 
                        normalized.get("title") or 
                        "unknown")
        else:
            base_name = normalized.get("name") or normalized.get("title") or "unknown"
        
        slug = _normalize_slug(entity_type, base_name)
        
        # AgendaItem and Event get date suffixes for proper temporal identification
        if entity_type in ["AgendaItem", "Event"]:
            # Extract date from entity context and format as MM_DD_YYYY
            meeting_date = normalized.get('meetingDate', '') or normalized.get('dateTime', '') or normalized.get('date', '') or ''
            date_suffix = _format_date_suffix(meeting_date)
            new_id = f"{_type_prefix(entity_type)}_{slug}_{date_suffix}"
        else:
            # All other entities: no hash, no date suffix
            new_id = f"{_type_prefix(entity_type)}_{slug}"
        
        # Always ensure lowercase
        normalized[id_field] = new_id.lower()
        normalized['id'] = new_id.lower()
    else:
        # If ID already exists, ensure it's lowercase
        existing_id = normalized.get(id_field)
        if existing_id:
            normalized[id_field] = str(existing_id).lower()
            normalized['id'] = str(existing_id).lower()
    
    return normalized


def _persist_phase2_new(meta: dict, parsed: dict, raw_text: str, output_root: Path = None):
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

    # Use provided output_root or default to original location for backward compatibility
    if output_root is not None:
        out_root = Path(output_root)
    else:
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


def _persist_relationships(rel_parsed: dict, doc_edges: list[dict], all_entities: list[dict], meta: dict, rel_text: str, output_root: Path = None):
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
            "type": normalized_type,  # Use "type" for consistency with graph format
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
    if output_root is not None:
        rel_file = Path(output_root) / "relationships" / f"{chunk_id}_{doc_name}.json"
    else:
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
    Extract entities using three separate API calls:
    - First call extracts entity types from ENTITY_TYPE_GROUP_1 (Governance & People)
    - Second call extracts entity types from ENTITY_TYPE_GROUP_2 (Documents & Content)
    - Third call extracts entity types from ENTITY_TYPE_GROUP_3 (Infrastructure & Resources)
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

    log.info("📄 [EXTRACT_ENTITIES_SPLIT] Loading prompts and building focused ontology contexts")
    system_prompt, user_p1, user_p2, user_p3 = load_prompts_from_file()
    
    # Build focused ontology contexts for each group
    ontology_context_1 = build_focused_ontology_context(ENTITY_TYPE_GROUP_1, "GROUP 1: GOVERNANCE & PEOPLE")
    ontology_context_2 = build_focused_ontology_context(ENTITY_TYPE_GROUP_2, "GROUP 2: DOCUMENTS & CONTENT")  
    ontology_context_3 = build_focused_ontology_context(ENTITY_TYPE_GROUP_3, "GROUP 3: INFRASTRUCTURE & RESOURCES")
    
    # Fallback to full ontology for relationships/attributes phases
    full_ontology_context = ONTOLOGY_FILE.read_text(encoding='utf-8')
    
    # Check if we're using the new triple format
    is_triple_format = not user_p2 and not user_p3 and "triples" in user_p1.lower()
    
    log.info(f"   📋 System prompt length: {len(system_prompt)} characters")
    log.info(f"   📋 User prompt 1 length: {len(user_p1)} characters")
    log.info(f"   📋 Format: {'Triple extraction' if is_triple_format else 'Legacy three-phase'}")
    log.info(f"   📋 Relationships template available: {bool(user_p2)}")
    log.info(f"   📋 Attributes template available: {bool(user_p3)}")
    log.info(f"   📋 Focused ontology contexts: Group1={len(ontology_context_1)}, Group2={len(ontology_context_2)}, Group3={len(ontology_context_3)} chars")
    
    # If triple format, use consolidated extraction
    if is_triple_format:
        log.info("🔄 [EXTRACT_ENTITIES_SPLIT] Using triple extraction format")
        from scripts.graph_rag_stages.phase2_building.ner.core.simple_ner_consolidated import extract_triples, convert_triples_to_entities_relationships
        
        # Extract triples
        triples_data, raw_response = extract_triples(
            chunk_text, document_type, meeting_date, source_file,
            {'chunkId': 'split_test', 'document': 'split_test', 'documentType': document_type, 
             'meetingDate': meeting_date, 'sourceFileName': source_file}
        )
        
        # Convert to entities format
        entities_by_type, relationships = convert_triples_to_entities_relationships(triples_data)
        
        # Format as expected by the adapter
        result = {"entities": entities_by_type, "_extracted_relationships": relationships}
        
        log.info(f"   🔗 Converted {len(relationships)} relationships from triples")
        
        # Create dummy templates for compatibility
        rel_template = "dummy_rel_template"
        attr_template = "dummy_attr_template"
        
        return result, raw_response, rel_template, attr_template, system_prompt

    # Initialize merged results
    merged_entities = {}
    
    # Process Group 1
    log.info("🔄 [EXTRACT_ENTITIES_SPLIT] Processing Group 1 entity types")
    log.info(f"   🏷️ Group 1 types: {ENTITY_TYPE_GROUP_1}")
    
    # Calculate optimal chunk size based on available tokens
    max_chunk_size = _calculate_max_chunk_size()
    chunk_text_optimized = str(chunk_text[:max_chunk_size])
    
    log.info(f"📏 [EXTRACT_ENTITIES_SPLIT] Chunk size optimization:")
    log.info(f"   📊 Original chunk length: {len(chunk_text):,} characters")
    log.info(f"   📊 MAX_TOKENS capacity: {os.getenv('MAX_TOKENS', '16384')}")
    log.info(f"   📊 Calculated max chunk size: {max_chunk_size:,} characters")
    log.info(f"   📊 Optimized chunk length: {len(chunk_text_optimized):,} characters")
    if len(chunk_text) <= max_chunk_size:
        log.info(f"   ✅ No truncation needed - processing 100% of content")
    else:
        coverage = (len(chunk_text_optimized) / len(chunk_text)) * 100
        log.info(f"   ⚠️ Chunk truncated to {coverage:.1f}% of original content")
    
    user_prompt_1 = (user_p1
        .replace("{DOC_TYPE_TITLE}", str(document_type).replace('_', ' ').title())
        .replace("{MEETING_DATE}", str(meeting_date))
        .replace("{SOURCE_FILE_NAME}", str(source_file))
        .replace("{CHUNK_TEXT_3000}", chunk_text_optimized)
        .replace("{CHUNK_TEXT}", chunk_text_optimized)
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
    
    user_prompt_full_1 = f"{ontology_context_1}\n\n{instruction_addon_1}\n\n{user_prompt_1}"
    log.info(f"📋 [EXTRACT_ENTITIES_SPLIT] Group 1 prompt length: {len(user_prompt_full_1)} characters")

    system_prompt_entities_1 = system_prompt.replace("{TASK_NAME}", f"entity extraction for group 1 types: {', '.join(ENTITY_TYPE_GROUP_1)}")
    
    log.info("🚀 [EXTRACT_ENTITIES_SPLIT] Preparing parallel requests to LLM")
    log.info(f"   🤖 Model: {model}")
    log.info(f"   🌡️ Temperature: 0")
    log.info(f"   📏 Max tokens: {os.getenv('MAX_TOKENS', '16384')}")

    # Prepare Group 2 prompt (reuse optimized chunk text)
    user_prompt_2 = (user_p1
        .replace("{DOC_TYPE_TITLE}", str(document_type).replace('_', ' ').title())
        .replace("{MEETING_DATE}", str(meeting_date))
        .replace("{SOURCE_FILE_NAME}", str(source_file))
        .replace("{CHUNK_TEXT_3000}", chunk_text_optimized)
        .replace("{CHUNK_TEXT}", chunk_text_optimized)
    )
    
    if "{ALL_ENTITY_BUCKETS_JSON_TEMPLATE}" in user_prompt_2:
        buckets_2 = []
        for t in ENTITY_TYPE_GROUP_2:
            buckets_2.append(f'"{t}": []')
        user_prompt_2 = user_prompt_2.replace("{ALL_ENTITY_BUCKETS_JSON_TEMPLATE}", ", ".join(buckets_2))

    instruction_addon_2 = f"""
IMPORTANT: For this extraction, focus ONLY on these entity types:
{', '.join(ENTITY_TYPE_GROUP_2)}

Ignore all other entity types for now - they will be extracted separately.
"""
    
    user_prompt_full_2 = f"{ontology_context_2}\n\n{instruction_addon_2}\n\n{user_prompt_2}"
    system_prompt_entities_2 = system_prompt.replace("{TASK_NAME}", f"entity extraction for group 2 types: {', '.join(ENTITY_TYPE_GROUP_2)}")

    # Prepare Group 3 prompt (reuse optimized chunk text)
    user_prompt_3 = (user_p1
        .replace("{DOC_TYPE_TITLE}", str(document_type).replace('_', ' ').title())
        .replace("{MEETING_DATE}", str(meeting_date))
        .replace("{SOURCE_FILE_NAME}", str(source_file))
        .replace("{CHUNK_TEXT_3000}", chunk_text_optimized)
        .replace("{CHUNK_TEXT}", chunk_text_optimized)
    )
    
    if "{ALL_ENTITY_BUCKETS_JSON_TEMPLATE}" in user_prompt_3:
        buckets_3 = []
        for t in ENTITY_TYPE_GROUP_3:
            buckets_3.append(f'"{t}": []')
        user_prompt_3 = user_prompt_3.replace("{ALL_ENTITY_BUCKETS_JSON_TEMPLATE}", ", ".join(buckets_3))

    instruction_addon_3 = f"""
IMPORTANT: For this extraction, focus ONLY on these entity types:
{', '.join(ENTITY_TYPE_GROUP_3)}

Ignore all other entity types for now - they will be extracted separately.
"""
    
    user_prompt_full_3 = f"{ontology_context_3}\n\n{instruction_addon_3}\n\n{user_prompt_3}"
    system_prompt_entities_3 = system_prompt.replace("{TASK_NAME}", f"entity extraction for group 3 types: {', '.join(ENTITY_TYPE_GROUP_3)}")

    # Execute all three API calls in parallel with rate limiting
    def call_group_1():
        _apply_rate_limit()  # Apply rate limiting before API call
        payload_size = len(system_prompt_entities_1) + len(user_prompt_full_1)
        thread_id = threading.current_thread().name
        operation_name = f"Group 1 Entities - {document_type} - {thread_id}"
        
        api_start_time = PerformanceMonitor.log_api_call_start(log, operation_name, payload_size, thread_id)
        
        try:
            log.info("🚀 [EXTRACT_ENTITIES_SPLIT] Sending Group 1 request to LLM")
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt_entities_1},
                    {"role": "user", "content": user_prompt_full_1}
                ],
                temperature=0,
                max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
            )
            
            response_size = len(response.choices[0].message.content or '')
            PerformanceMonitor.log_api_call_end(log, operation_name, api_start_time, response_size, success=True)
            return response
            
        except Exception as e:
            PerformanceMonitor.log_api_call_end(log, operation_name, api_start_time, 0, success=False)
            raise
    
    def call_group_2():
        _apply_rate_limit()  # Apply rate limiting before API call
        payload_size = len(system_prompt_entities_2) + len(user_prompt_full_2)
        thread_id = threading.current_thread().name
        operation_name = f"Group 2 Entities - {document_type} - {thread_id}"
        
        api_start_time = PerformanceMonitor.log_api_call_start(log, operation_name, payload_size, thread_id)
        
        try:
            log.info("🚀 [EXTRACT_ENTITIES_SPLIT] Sending Group 2 request to LLM")
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt_entities_2},
                    {"role": "user", "content": user_prompt_full_2}
                ],
                temperature=0,
                max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
            )
            
            response_size = len(response.choices[0].message.content or '')
            PerformanceMonitor.log_api_call_end(log, operation_name, api_start_time, response_size, success=True)
            return response
            
        except Exception as e:
            PerformanceMonitor.log_api_call_end(log, operation_name, api_start_time, 0, success=False)
            raise
    
    def call_group_3():
        _apply_rate_limit()  # Apply rate limiting before API call
        payload_size = len(system_prompt_entities_3) + len(user_prompt_full_3)
        thread_id = threading.current_thread().name
        operation_name = f"Group 3 Entities - {document_type} - {thread_id}"
        
        api_start_time = PerformanceMonitor.log_api_call_start(log, operation_name, payload_size, thread_id)
        
        try:
            log.info("🚀 [EXTRACT_ENTITIES_SPLIT] Sending Group 3 request to LLM")
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt_entities_3},
                    {"role": "user", "content": user_prompt_full_3}
                ],
                temperature=0,
                max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
            )
            
            response_size = len(response.choices[0].message.content or '')
            PerformanceMonitor.log_api_call_end(log, operation_name, api_start_time, response_size, success=True)
            return response
            
        except Exception as e:
            PerformanceMonitor.log_api_call_end(log, operation_name, api_start_time, 0, success=False)
            raise
    
    # Enhanced parallel API execution with performance monitoring
    log.info("🔥 [EXTRACT_ENTITIES_SPLIT] Starting 3 parallel API calls with performance monitoring")
    parallel_start_time = PerformanceMonitor.log_parallel_execution_start(log, 3)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Submit calls with timing
        submit_time = time.time()
        future_1 = executor.submit(call_group_1)
        future_2 = executor.submit(call_group_2)
        future_3 = executor.submit(call_group_3)
        
        log.info(f"   🚀 All futures submitted in {time.time() - submit_time:.3f}s")
        
        # Get results with individual timing
        log.info("⏳ [EXTRACT_ENTITIES_SPLIT] Waiting for parallel responses...")
        
        response_1_start = time.time()
        response_1 = future_1.result()
        response_1_time = time.time() - response_1_start
        
        response_2_start = time.time()
        response_2 = future_2.result()
        response_2_time = time.time() - response_2_start
        
        response_3_start = time.time()
        response_3 = future_3.result()
        response_3_time = time.time() - response_3_start
        
        log.info("📊 [EXTRACT_ENTITIES_SPLIT] Individual response times:")
        log.info(f"   ⏱️  Group 1: {response_1_time:.2f}s")
        log.info(f"   ⏱️  Group 2: {response_2_time:.2f}s")
        log.info(f"   ⏱️  Group 3: {response_3_time:.2f}s")
    
    result_text_1 = (response_1.choices[0].message.content or '').strip()
    result_text_2 = (response_2.choices[0].message.content or '').strip()
    result_text_3 = (response_3.choices[0].message.content or '').strip()
    
    # Log parallel execution summary
    results_info = {
        'group_1_chars': len(result_text_1),
        'group_2_chars': len(result_text_2), 
        'group_3_chars': len(result_text_3),
        'total_response_chars': len(result_text_1) + len(result_text_2) + len(result_text_3)
    }
    
    PerformanceMonitor.log_parallel_execution_end(log, parallel_start_time, 3, results_info)
    
    log.info(f"✅ [EXTRACT_ENTITIES_SPLIT] Received all three responses in parallel:")
    log.info(f"   📝 Group 1 response: {len(result_text_1)} characters")
    log.info(f"   📝 Group 2 response: {len(result_text_2)} characters")
    log.info(f"   📝 Group 3 response: {len(result_text_3)} characters")
    
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
    
    # Parse Group 3 results
    try:
        parsed_3 = json.loads(result_text_3)
        log.info("✅ [EXTRACT_ENTITIES_SPLIT] Group 3 JSON parsing successful")
        
        if isinstance(parsed_3, dict):
            entities_dict_3 = parsed_3.get('entities', parsed_3) if 'entities' in parsed_3 else parsed_3
            
            # Add Group 3 entities to merged results
            for entity_type in ENTITY_TYPE_GROUP_3:
                if entity_type in entities_dict_3:
                    merged_entities[entity_type] = entities_dict_3[entity_type]
                    log.info(f"   📊 {entity_type}: {len(entities_dict_3[entity_type])} entities")
                else:
                    merged_entities[entity_type] = []
                    
    except json.JSONDecodeError as e:
        log.error(f"❌ [EXTRACT_ENTITIES_SPLIT] Group 3 JSON parsing failed: {e}")
        for entity_type in ENTITY_TYPE_GROUP_3:
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
    max_chunk_size = _calculate_max_chunk_size()
    relationship_chunk = str(chunk_text[:max_chunk_size])
    log.info(f"   📏 Using {len(relationship_chunk):,} chars for relationship extraction (vs old limit: 2,500)")
    
    user_rel = (user_p2
        .replace("{ENTITY_REFS_TOP50}", entity_refs)
        .replace("{CHUNK_TEXT_2500}", relationship_chunk)
        .replace("{CHUNK_TEXT}", relationship_chunk)
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

    # Filter out empty entity types
    valid_entity_types = {etype: ents for etype, ents in by_type_entities.items() if ents}
    
    if not valid_entity_types:
        log.info("⏭️ [EXTRACT_ATTRIBUTES] No valid entity types with entities, skipping")
        return {}, ""
    
    log.info(f"🚀 [EXTRACT_ATTRIBUTES] Starting PARALLEL attribute extraction for {len(valid_entity_types)} entity types")
    
    def extract_single_entity_type_attributes(etype_and_ents):
        """Process a single entity type for attributes - designed for parallel execution."""
        etype, ents = etype_and_ents
        
        log.info(f"🔄 [EXTRACT_ATTRIBUTES] Processing {etype}: {len(ents)} entities [PARALLEL]")
        
        expected_attrs = UnifiedOntology.ENTITY_TYPES.get(etype, {}).get('attributes', [])
        log.info(f"   🏷️ Expected attributes for {etype}: {expected_attrs}")
        
        max_chunk_size = _calculate_max_chunk_size()
        attribute_chunk = str(chunk_text[:max_chunk_size])
        log.info(f"   📏 Using {len(attribute_chunk):,} chars for {etype} attribute extraction (vs old limit: 2,000)")
        
        user_attr = (user_p3
            .replace("{ENTITY_ATTRIBUTE_SUMMARY}", attr_summary)
            .replace("{ENTITY_TYPE}", etype)
            .replace("{ENTITY_LIST_JSON}", json.dumps(ents, ensure_ascii=False, indent=2))
            .replace("{EXPECTED_ATTRS_LIST}", ", ".join(expected_attrs))
            .replace("{CHUNK_TEXT_2000}", attribute_chunk)
            .replace("{CHUNK_TEXT}", attribute_chunk)
        )
        user_attr_full = f"{ontology_context}\n\n{user_attr}"
        log.info(f"   📋 Attribute prompt length for {etype}: {len(user_attr_full)} characters")
        
        system_prompt_attr = system_prompt_base.replace("{TASK_NAME}", f"attribute enhancement for {etype}")
        
        # Apply rate limiting before API call
        _apply_rate_limit()
        
        log.info(f"🚀 [EXTRACT_ATTRIBUTES] Sending {etype} request to LLM [PARALLEL]")
        try:
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
            log.info(f"✅ [EXTRACT_ATTRIBUTES] Received {etype} response: {len(txt)} characters [PARALLEL]")
            
            raw_block = f"=== {etype} ===\n{txt}\n"
            
            log.info(f"🔍 [EXTRACT_ATTRIBUTES] Parsing {etype} JSON response [PARALLEL]")
            try:
                patches = json.loads(txt)
                log.info(f"✅ [EXTRACT_ATTRIBUTES] {etype} JSON parsing successful [PARALLEL]")
                patches_count = len(patches) if isinstance(patches, list) else 0
                log.info(f"   🏷️ Attribute patches received for {etype}: {patches_count}")
            except json.JSONDecodeError as e:
                log.error(f"❌ [EXTRACT_ATTRIBUTES] {etype} JSON parsing failed: {e}")
                patches = []
                
            if not patches:
                log.warning(f"   ⚠️ No valid attribute patches for {etype}")
                return etype, [], raw_block, 0
                
            # Merge the attributes
            merged = _merge_attributes(ents, patches if isinstance(patches, list) else [], etype)
            entities_enhanced = len(merged) if merged else 0
            log.info(f"   📊 {etype}: enhanced {entities_enhanced} entities with attributes [PARALLEL]")
            
            return etype, merged, raw_block, entities_enhanced
            
        except Exception as e:
            log.error(f"❌ [EXTRACT_ATTRIBUTES] API call failed for {etype}: {e}")
            return etype, [], f"=== {etype} (FAILED) ===\nError: {str(e)}\n", 0
    
    # Execute all entity type attribute extractions in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(valid_entity_types), MAX_CONCURRENT_CALLS)) as executor:
        # Submit all tasks
        future_to_etype = {
            executor.submit(extract_single_entity_type_attributes, (etype, ents)): etype 
            for etype, ents in valid_entity_types.items()
        }
        
        log.info(f"🔥 [EXTRACT_ATTRIBUTES] Submitted {len(future_to_etype)} parallel attribute extraction tasks")
        
        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_etype):
            original_etype = future_to_etype[future]
            try:
                etype, merged_entities, raw_block, entities_enhanced = future.result()
                
                # Store results
                if merged_entities:
                    enhanced[etype] = merged_entities
                    
                raw_blocks.append(raw_block)
                total_enhanced += entities_enhanced
                
                log.info(f"✅ [EXTRACT_ATTRIBUTES] Completed {etype}: {entities_enhanced} entities enhanced [PARALLEL]")
                
            except Exception as e:
                log.error(f"❌ [EXTRACT_ATTRIBUTES] Task failed for {original_etype}: {e}")
                raw_blocks.append(f"=== {original_etype} (ERROR) ===\nException: {str(e)}\n")
    
    processed_types = len(valid_entity_types)
    log.info(f"🎉 [EXTRACT_ATTRIBUTES] PARALLEL attribute extraction completed")

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
    chunks_dir = Path(__file__).parents[4] / "simple_ner_graph/document_chunks"
    
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