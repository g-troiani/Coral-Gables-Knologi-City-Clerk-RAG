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

load_dotenv()

PROMPT_FILE = Path(__file__).parent / "ner_prompt.txt"
ONTOLOGY_FILE = Path(__file__).parent / "ontology_context.txt"


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
    source_file_name = meta.get('Source_File_Name', Path(chunk_file).name)
    source_file_path = meta.get('Source_File_Path', 'unknown')
    body_text = header_parts[-1].strip() if header_parts else text
    return {
        'chunk_id': chunk_id or 'unknown',
        'document': document or 'unknown',
        'document_type': document_type or 'unknown',
        'meeting_date': meeting_date or 'unknown',
        'Source_File_Name': source_file_name,
        'Source_File_Path': source_file_path,
        'chunk_file': Path(chunk_file).name,
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

    out_root = Path("scripts/graph_rag_stages/phase2_NEW/output")
    ents_root = out_root / "entities"
    rels_root = out_root / "relationships"
    ents_root.mkdir(parents=True, exist_ok=True)
    rels_root.mkdir(parents=True, exist_ok=True)

    chunk_id = meta.get('chunk_id', 'unknown')
    doc_name = meta.get('document', 'unknown')
    source_file = meta.get('Source_File_Name', 'unknown')
    source_path = meta.get('Source_File_Path', 'unknown')

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
                "chunk_id": chunk_id,
                "document": doc_name,
                "source_file": source_file,
                "source_path": source_path,
                "entity_type": etype,
                "entities": validated,
                "_chunk_metadata": meta,
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

    user_rel = (user_p2
        .replace("{ENTITY_REFS_TOP50}", entity_refs)
        .replace("{CHUNK_TEXT_2500}", str(chunk_text[:2500]))
    )

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
        document_type=meta.get('document_type', 'unknown'),
        meeting_date=meta.get('meeting_date', 'unknown'),
        source_file=meta.get('Source_File_Name', 'unknown'),
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

    # Attributes/enrichment call (if template available)
    by_type = _group_by_type(norm_flat)
    if attr_template:
        enhanced_by_type, attr_raw = extract_attributes(text, attr_template, sys_prompt, by_type)
        (Path(__file__).parent / "lll_attribute_extraction_output.txt").write_text(attr_raw, encoding='utf-8')
        # No re-persist for now; we keep output as raw file to inspect

    print("\n=== LLM RAW RESULT ===\n" + json.dumps(result, indent=2, ensure_ascii=False))
    print(f"\n=== PERSISTENCE SUMMARY ===")
    print(f"Raw entities: {persistence_log['raw_entities_count']}")
    print(f"Persisted: {persistence_log['persisted_entities_count']}")
    print(f"Missing: {persistence_log['raw_entities_count'] - persistence_log['persisted_entities_count']}")
    print(f"\nDetailed log saved to: entity_persistence_log.txt")
