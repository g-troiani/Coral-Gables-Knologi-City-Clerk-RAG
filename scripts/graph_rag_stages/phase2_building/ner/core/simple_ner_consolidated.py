#!/usr/bin/env python3
"""
Consolidated NER extraction using triple-based approach.
Preserves all existing deduplication, naming conventions, and merging rules.
"""

import os
import json
from pathlib import Path
import sys
from dotenv import load_dotenv
import hashlib
import re
from datetime import datetime
import logging
import time
import threading
import psutil
import gc

# Ensure project root is on sys.path for package imports
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.graph_rag_stages.common.utils import get_llm_client
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
from scripts.graph_rag_stages.common.entity_factory import EntityFactory
from scripts.graph_rag_stages.common.document_linker import DocumentLinker
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

PROMPT_FILE = Path(__file__).parent / "ner_prompt.txt"
ONTOLOGY_FILE = Path(__file__).parent / "ontology_context_camelCase.txt"

# Performance monitoring utilities
class PerformanceMonitor:
    """Utility class for comprehensive performance monitoring."""
    
    @staticmethod
    def get_memory_info():
        """Get current memory usage info."""
        process = psutil.Process()
        mem_info = process.memory_info()
        return {
            'rss_mb': mem_info.rss / 1024 / 1024,  # Resident set size in MB
            'vms_mb': mem_info.vms / 1024 / 1024,  # Virtual memory size in MB
            'percent': process.memory_percent()
        }
    
    @staticmethod
    def log_api_call_start(log, operation: str, payload_size: int, thread_id: str = None):
        """Log detailed info about API call start."""
        mem_info = PerformanceMonitor.get_memory_info()
        thread_id = thread_id or threading.current_thread().name
        
        log.info(f"🚀 [API_CALL_START] {operation}")
        log.info(f"   📏 Request payload size: {payload_size:,} characters ({payload_size/1024:.2f}KB)")
        log.info(f"   💾 Memory usage: RSS={mem_info['rss_mb']:.1f}MB, Virtual={mem_info['vms_mb']:.1f}MB, {mem_info['percent']:.1f}%")
        log.info(f"   🧵 Thread: {thread_id}")
        log.info(f"   ⏰ Start time: {datetime.now().isoformat()}")
        return time.time()
    
    @staticmethod
    def log_api_call_end(log, operation: str, start_time: float, response_size: int, success: bool = True):
        """Log detailed info about API call completion."""
        end_time = time.time()
        duration = end_time - start_time
        mem_info = PerformanceMonitor.get_memory_info()
        
        status = "✅ SUCCESS" if success else "❌ FAILED"
        log.info(f"{status} [API_CALL_END] {operation}")
        log.info(f"   ⏱️  Total duration: {duration:.2f}s")
        log.info(f"   📊 Response size: {response_size:,} characters ({response_size/1024:.2f}KB)")
        log.info(f"   🚄 Throughput: {response_size/duration:.0f} chars/sec")
        log.info(f"   💾 Memory after: RSS={mem_info['rss_mb']:.1f}MB, Virtual={mem_info['vms_mb']:.1f}MB, {mem_info['percent']:.1f}%")
        
        # Bottleneck warnings
        if duration > 30:
            log.warning(f"⚠️  SLOW API CALL: {operation} took {duration:.2f}s (>30s threshold)")
        if response_size/1024 > 50:  # >50KB response
            log.warning(f"⚠️  LARGE RESPONSE: {operation} returned {response_size/1024:.2f}KB")
        if mem_info['percent'] > 80:
            log.warning(f"⚠️  HIGH MEMORY: Process using {mem_info['percent']:.1f}% memory")
            
        return duration
    
    @staticmethod
    def log_rate_limit_wait(log, wait_time: float):
        """Log rate limiting wait."""
        log.info(f"⏸️  [RATE_LIMIT] Waiting {wait_time:.2f}s before next API call")
        
    @staticmethod
    def log_parallel_execution_start(log, num_calls: int):
        """Log start of parallel API execution."""
        mem_info = PerformanceMonitor.get_memory_info()
        log.info(f"🔥 [PARALLEL_START] Launching {num_calls} parallel API calls")
        log.info(f"   💾 Pre-parallel memory: RSS={mem_info['rss_mb']:.1f}MB, {mem_info['percent']:.1f}%")
        return time.time()
        
    @staticmethod
    def log_parallel_execution_end(log, start_time: float, num_calls: int, results_info: dict):
        """Log end of parallel API execution."""
        duration = time.time() - start_time
        mem_info = PerformanceMonitor.get_memory_info()
        
        log.info(f"✅ [PARALLEL_END] Completed {num_calls} parallel calls in {duration:.2f}s")
        log.info(f"   ⚡ Average time per call: {duration/num_calls:.2f}s")
        log.info(f"   📊 Results summary: {results_info}")
        log.info(f"   💾 Post-parallel memory: RSS={mem_info['rss_mb']:.1f}MB, {mem_info['percent']:.1f}%")
        
        if duration/num_calls > 20:  # Average >20s per call
            log.warning(f"⚠️  SLOW PARALLEL EXECUTION: Average {duration/num_calls:.2f}s per call")


def load_triple_prompt() -> str:
    """Load the triple extraction prompt from ner_prompt.txt."""
    text = PROMPT_FILE.read_text(encoding='utf-8')
    # The prompt file now contains the triple extraction prompt
    return text


def parse_chunk_file(chunk_file: str):
    """Parse chunk file to extract metadata and body text."""
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
    """Normalize entity name to create ID slug - preserving exact logic from original."""
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
    """Get the ID prefix for an entity type."""
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
    
    # Pattern 1: DD.MM.YYYY (e.g., "01.09.2024")
    match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
    if match:
        day, month, year = match.groups()
        return f"{month.zfill(2)}_{day.zfill(2)}_{year}"
    
    # Pattern 2: YYYY-MM-DD (e.g., "2024-01-09")  
    match = re.match(r'(\d{4})-(\d{1,2})-(\d{1,2})', date_str)
    if match:
        year, month, day = match.groups()
        return f"{month.zfill(2)}_{day.zfill(2)}_{year}"
    
    # Pattern 3: MM/DD/YYYY (e.g., "01/09/2024")
    match = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
    if match:
        month, day, year = match.groups()
        return f"{month.zfill(2)}_{day.zfill(2)}_{year}"
    
    # Pattern 4: Already in MM_DD_YYYY format
    if re.match(r'\d{2}_\d{2}_\d{4}', date_str):
        return date_str
    
    # Fallback: try to extract year and use generic format
    year_match = re.search(r'(20\d{2})', date_str)
    if year_match:
        year = year_match.group(1)
        return f"01_09_{year}"  # Default to 01/09 if can't parse month/day
    
    return "unknown_date"


def _ensure_id(entity: dict, entity_type: str) -> dict:
    """Ensure entity has a properly formatted ID - preserving exact logic from original."""
    normalized = EntityIDStandards.normalize_entity_id_fields(entity, entity_type)
    id_field = EntityIDStandards.get_id_field(entity_type)
    if not normalized.get(id_field):
        if entity_type == "Policy":
            preferred = EntityIDStandards.preferred_policy_id(normalized)
            if preferred:
                normalized[id_field] = preferred.lower()
                normalized['id'] = preferred.lower()
                return normalized
        if entity_type == "AgendaItem":
            preferred = EntityIDStandards.preferred_agendaitem_id(normalized)
            if preferred:
                normalized[id_field] = preferred.lower()
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
        
        # Only AgendaItem gets a date suffix (no more hashes for other entities)
        if entity_type == "AgendaItem":
            # Extract date from entity context and format as MM_DD_YYYY
            meeting_date = normalized.get('meetingDate', '') or ''
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


def extract_triples(chunk_text: str, document_type: str, meeting_date: str, source_file: str, chunk_meta: dict, ontology_override: str = None):
    """Extract entities and relationships as triples in a single LLM call.
    
    Args:
        ontology_override: Optional focused ontology context to use instead of full ontology.
    """
    #NEVER USE IT, IF YOU USE THIS THERE WILL BE HUGE CONSEQUENCES
    extraction_start_time = time.time()
    
    log.info("🔍 [EXTRACT_TRIPLES] Starting triple extraction")
    log.info(f"   📄 Document type: {document_type}")
    log.info(f"   📅 Meeting date: {meeting_date}")
    log.info(f"   📁 Source file: {source_file}")
    log.info(f"   📝 Chunk text length: {len(chunk_text)} characters")
    
    # Log initial memory state
    mem_info = PerformanceMonitor.get_memory_info()
    log.info(f"   💾 Initial memory: RSS={mem_info['rss_mb']:.1f}MB, {mem_info['percent']:.1f}%")
    
    # Client setup with timing
    client_setup_start = time.time()
    client = get_llm_client()
    model = (os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME") or "").split('"')[0].strip()
    if not model:
        log.error("❌ [EXTRACT_TRIPLES] AZURE_OPENAI_DEPLOYMENT_NAME environment variable not set")
        raise ValueError("AZURE_OPENAI_DEPLOYMENT_NAME environment variable must be set")
    
    client_setup_time = time.time() - client_setup_start
    log.info(f"   🔧 Client setup time: {client_setup_time:.3f}s")
    
    # Load prompt template
    prompt_template = load_triple_prompt()
    
    # Extract system and user parts
    parts = prompt_template.split("=== TRIPLE EXTRACTION PROMPT ===")
    system_prompt = parts[0].replace("SYSTEM TEMPLATE", "").strip()
    user_prompt_template = parts[1].strip() if len(parts) > 1 else prompt_template
    
    # Load ontology context (use override if provided for focused extraction)
    if ontology_override:
        ontology_context = ontology_override
        log.info("🎯 [EXTRACT_TRIPLES] Using focused ontology context")
    else:
        ontology_context = ONTOLOGY_FILE.read_text(encoding='utf-8')
        log.info("📚 [EXTRACT_TRIPLES] Using full ontology context")
    
    # Fill in placeholders
    user_prompt = (user_prompt_template
        .replace("{ONTOLOGY_CONTEXT}", ontology_context)
        .replace("{DOC_TYPE_TITLE}", str(document_type).replace('_', ' ').title())
        .replace("{MEETING_DATE}", str(meeting_date))
        .replace("{SOURCE_FILE_NAME}", str(source_file))
        .replace("{CHUNK_TEXT_3000}", str(chunk_text))
        .replace("{CHUNK_TEXT}", str(chunk_text))
        .replace("{CHUNK_ID}", chunk_meta.get('chunkId', 'unknown'))
        .replace("{DOC_ID}", chunk_meta.get('document', 'unknown'))
    )
    
    # Set task name based on whether we're using focused or full ontology
    task_name = "focused triple extraction" if ontology_override else "triple extraction with full ontology"
    system_prompt = system_prompt.replace("{TASK_NAME}", task_name)
    
    # Calculate total payload size for performance monitoring
    total_payload_size = len(system_prompt) + len(user_prompt)
    log.info(f"📋 [EXTRACT_TRIPLES] Final prompt length: {len(user_prompt)} characters")
    log.info(f"   📊 Total payload size: {total_payload_size:,} characters ({total_payload_size/1024:.2f}KB)")
    
    # Print the actual prompts being sent to LLM
    print("\n" + "="*100)
    print("🤖 SYSTEM PROMPT SENT TO LLM:")
    print("="*100)
    print(system_prompt)
    print("\n" + "="*100)
    print("👤 USER PROMPT SENT TO LLM:")
    print("="*100)
    print(user_prompt)
    print("="*100 + "\n")
    
    # Enhanced API call with comprehensive performance monitoring
    operation_name = f"Triple extraction - {document_type} - {len(chunk_text)} chars"
    api_start_time = PerformanceMonitor.log_api_call_start(log, operation_name, total_payload_size)
    
    try:
        log.info("🚀 [EXTRACT_TRIPLES] Sending request to LLM")
        log.info(f"   🤖 Model: {model}")
        log.info(f"   🌡️ Temperature: 0")
        log.info(f"   📏 Max tokens: {os.getenv('MAX_TOKENS', '16384')}")
        
        # Trigger garbage collection before API call to free memory
        gc.collect()
        
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0,
            max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
        )
        
        result_text = (response.choices[0].message.content or '').strip()
        
        # Log successful API call completion
        api_duration = PerformanceMonitor.log_api_call_end(
            log, operation_name, api_start_time, len(result_text), success=True
        )
        
        log.info(f"✅ [EXTRACT_TRIPLES] Received LLM response: {len(result_text)} characters")
        
        # Log usage info if available
        if hasattr(response, 'usage') and response.usage:
            usage = response.usage
            log.info(f"   📊 Token usage: {usage.prompt_tokens} prompt + {usage.completion_tokens} completion = {usage.total_tokens} total")
            log.info(f"   💰 Efficiency: {usage.completion_tokens / api_duration:.1f} completion tokens/sec")
        
    except Exception as e:
        # Log failed API call
        api_duration = PerformanceMonitor.log_api_call_end(
            log, operation_name, api_start_time, 0, success=False
        )
        log.error(f"❌ [EXTRACT_TRIPLES] API call failed after {api_duration:.2f}s: {e}")
        raise
    
    try:
        parsing_start = time.time()
        parsed = json.loads(result_text)
        parsing_time = time.time() - parsing_start
        
        log.info("✅ [EXTRACT_TRIPLES] JSON parsing successful")
        log.info(f"   ⏱️  JSON parsing time: {parsing_time:.3f}s")
        
        if isinstance(parsed, dict) and 'triples' in parsed:
            triples_count = len(parsed['triples'])
            log.info(f"   🔗 Triples extracted: {triples_count}")
        else:
            log.warning("⚠️ [EXTRACT_TRIPLES] Response missing 'triples' key")
            parsed = {"triples": [], "documentContext": {}}
            triples_count = 0
            
    except json.JSONDecodeError as e:
        parsing_time = 0
        triples_count = 0
        log.error(f"❌ [EXTRACT_TRIPLES] JSON parsing failed: {e}")
        log.error(f"   📝 Raw response preview: {result_text[:500]}...")
        parsed = {"triples": [], "documentContext": {}}
    
    # Log total extraction performance summary  
    total_extraction_time = time.time() - extraction_start_time
    log.info(f"📊 [EXTRACT_TRIPLES] Total extraction summary:")
    log.info(f"   ⏱️  Total time: {total_extraction_time:.2f}s")
    log.info(f"   📏 Input size: {len(chunk_text):,} chars")
    log.info(f"   📊 Output size: {len(result_text):,} chars") 
    log.info(f"   🔗 Triples found: {triples_count}")
    if total_extraction_time > 0:
        log.info(f"   🚄 Overall throughput: {len(chunk_text)/total_extraction_time:.0f} input chars/sec")
    
    # Performance warnings
    if total_extraction_time > 60:
        log.warning(f"⚠️  VERY SLOW EXTRACTION: Total time {total_extraction_time:.2f}s (>60s)")
    if triples_count == 0:
        log.warning("⚠️  NO TRIPLES EXTRACTED: Check prompt or input quality")
    
    return parsed, result_text


def convert_triples_to_entities_relationships(triples_data: dict) -> tuple[dict, list]:
    """Convert triple format to entities and relationships format with deduplication."""
    entities_by_type = {}
    relationships = []
    entity_registry = {}  # Track unique entities by ID
    
    for triple in triples_data.get('triples', []):
        subject = triple.get('subject', {})
        predicate = triple.get('predicate') or triple.get('relationship')  # Handle both field names
        obj = triple.get('object', {})
        
        # Process subject entity
        if subject and 'type' in subject:
            subject_type = subject['type']
            subject_attrs = subject.get('attributes', {})
            
            # Ensure ID using original logic
            subject_attrs['type'] = subject_type
            subject_normalized = _ensure_id(subject_attrs, subject_type)
            subject_id = subject_normalized.get('id')
            
            # Deduplicate and merge attributes
            if subject_id not in entity_registry:
                entity_registry[subject_id] = subject_normalized
                
                # Add to type bucket
                if subject_type not in entities_by_type:
                    entities_by_type[subject_type] = []
                entities_by_type[subject_type].append(subject_normalized)
            else:
                # Merge attributes - keep non-null values
                existing = entity_registry[subject_id]
                for k, v in subject_normalized.items():
                    if v is not None and (k not in existing or existing[k] is None):
                        existing[k] = v
        
        # Process object entity
        if obj and 'type' in obj:
            obj_type = obj['type']
            obj_attrs = obj.get('attributes', {})
            
            # Ensure ID using original logic
            obj_attrs['type'] = obj_type
            obj_normalized = _ensure_id(obj_attrs, obj_type)
            obj_id = obj_normalized.get('id')
            
            # Deduplicate and merge attributes
            if obj_id not in entity_registry:
                entity_registry[obj_id] = obj_normalized
                
                # Add to type bucket
                if obj_type not in entities_by_type:
                    entities_by_type[obj_type] = []
                entities_by_type[obj_type].append(obj_normalized)
            else:
                # Merge attributes - keep non-null values
                existing = entity_registry[obj_id]
                for k, v in obj_normalized.items():
                    if v is not None and (k not in existing or existing[k] is None):
                        existing[k] = v
        
        # Create relationship with normalized IDs
        if predicate and subject_id and obj_id:
            rel = {
                "type": predicate,
                "source": subject_id,
                "target": obj_id,
                "source_type": subject_type,
                "target_type": obj_type
            }
            
            # Add any relationship attributes from the triple
            if 'attributes' in triple and isinstance(triple['attributes'], dict):
                for k, v in triple['attributes'].items():
                    if k not in ['type', 'source', 'target', 'source_type', 'target_type']:
                        rel[k] = v
            
            relationships.append(rel)
    
    return entities_by_type, relationships


def _persist_entities_and_relationships(meta: dict, entities_by_type: dict, relationships: list, raw_text: str, output_dir: Path = None):
    """Persist entities and relationships using the original persistence logic."""
    # Track persistence status
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
        "buckets_summary": {}
    }
    
    # Count entities
    for entity_type, entities in entities_by_type.items():
        persistence_log["raw_entities_count"] += len(entities)
        persistence_log["buckets_summary"][entity_type] = {
            "raw_count": len(entities),
            "persisted_count": 0,
            "status": "found"
        }
    
    # Use provided output directory or default to phase2_NEW/output
    if output_dir:
        out_root = output_dir
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

    # Persist entities by type
    for etype, ents in entities_by_type.items():
        # Check if bucket type is valid
        if etype not in known_entity_types:
            persistence_log["failure_reasons"]["invalid_bucket_type"] += 1
            continue
            
        validated = []
        for idx, e in enumerate(ents):
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

    # Create provenance edges
    doc_edges = DocumentLinker.create_document_entity_relationships(all_entities, meta, chunk_id)
    
    # Combine with extracted relationships
    all_relationships = doc_edges + relationships
    
    # Persist relationships
    if all_relationships:
        rel_file = rels_root / f"{chunk_id}_{doc_name}.json"
        rel_file.write_text(json.dumps({"relationships": all_relationships}, indent=2, ensure_ascii=False), encoding='utf-8')
    
    # Create relationship log
    relationship_log = {
        "raw_relationships_count": len(relationships),
        "persisted_relationships_count": len(relationships),
        "provenance_edges_count": len(doc_edges),
        "total_relationships": len(all_relationships)
    }
    
    return all_entities, persistence_log, relationship_log


def process_chunk(chunk_file: str) -> dict:
    """Process a single chunk file using triple extraction."""
    log.info(f"📄 [PROCESS_CHUNK] Processing: {chunk_file}")
    
    # Parse chunk file
    meta, body_text = parse_chunk_file(chunk_file)
    if not body_text or len(body_text.strip()) < 50:
        log.warning(f"⚠️ [PROCESS_CHUNK] Skipping chunk with insufficient text")
        return {
            "chunk": chunk_file,
            "entities_extracted": 0,
            "relationships_extracted": 0,
            "error": "Insufficient text"
        }
    
    try:
        # Extract triples
        triples_data, raw_response = extract_triples(
            body_text,
            meta['documentType'],
            meta['meetingDate'],
            meta['sourceFileName'],
            meta
        )
        
        # Convert to entities and relationships
        entities_by_type, relationships = convert_triples_to_entities_relationships(triples_data)
        
        # Persist with original logic
        all_entities, entity_log, rel_log = _persist_entities_and_relationships(
            meta, entities_by_type, relationships, raw_response
        )
        
        # Write raw outputs for debugging
        debug_dir = Path(__file__).parent / "debug"
        debug_dir.mkdir(exist_ok=True)
        
        # Save raw LLM response
        (debug_dir / f"{meta['chunkId']}_llm_response.txt").write_text(
            raw_response, encoding='utf-8'
        )
        
        # Save extracted triples
        (debug_dir / f"{meta['chunkId']}_triples.json").write_text(
            json.dumps(triples_data, indent=2, ensure_ascii=False), encoding='utf-8'
        )
        
        return {
            "chunk": chunk_file,
            "entities_extracted": entity_log['persisted_entities_count'],
            "relationships_extracted": rel_log['persisted_relationships_count'],
            "triples_extracted": len(triples_data.get('triples', [])),
            "entity_log": entity_log,
            "relationship_log": rel_log
        }
        
    except Exception as e:
        log.error(f"❌ [PROCESS_CHUNK] Error processing chunk: {e}")
        import traceback
        traceback.print_exc()
        return {
            "chunk": chunk_file,
            "entities_extracted": 0,
            "relationships_extracted": 0,
            "error": str(e)
        }


def main():
    """Main entry point for processing chunks."""
    import argparse
    
    parser = argparse.ArgumentParser(description='NER extraction using triple-based approach')
    parser.add_argument('--chunk-dir', type=str, 
                       default='simple_ner_graph/document_chunks',
                       help='Directory containing chunk files')
    parser.add_argument('--chunk-file', type=str,
                       help='Process a single chunk file')
    
    args = parser.parse_args()
    
    if args.chunk_file:
        # Process single chunk
        result = process_chunk(args.chunk_file)
        print(f"Results: {json.dumps(result, indent=2)}")
    else:
        # Process all chunks in directory
        chunks_dir = Path(__file__).parents[3] / args.chunk_dir
        
        if not chunks_dir.exists():
            print(f"Error: Chunks directory not found: {chunks_dir}")
            exit(1)
        
        chunk_files = list(chunks_dir.glob("*.txt"))
        if not chunk_files:
            print(f"Error: No chunk files found in {chunks_dir}")
            exit(1)
        
        print(f"Found {len(chunk_files)} chunks to process")
        
        # Initialize cumulative stats
        total_entities = 0
        total_relationships = 0
        total_triples = 0
        errors = []
        
        # Process each chunk
        for i, chunk_path in enumerate(chunk_files, 1):
            print(f"\n{'='*60}")
            print(f"Processing chunk {i}/{len(chunk_files)}: {chunk_path.name}")
            print(f"{'='*60}")
            
            result = process_chunk(str(chunk_path))
            
            if 'error' in result:
                errors.append(f"{chunk_path.name}: {result['error']}")
                print(f"❌ Error: {result['error']}")
            else:
                total_entities += result['entities_extracted']
                total_relationships += result['relationships_extracted']
                total_triples += result.get('triples_extracted', 0)
                print(f"✅ Extracted: {result['entities_extracted']} entities, {result['relationships_extracted']} relationships from {result.get('triples_extracted', 0)} triples")
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"=== FINAL SUMMARY ===")
        print(f"{'='*60}")
        print(f"Total chunks processed: {len(chunk_files)}")
        print(f"Total entities extracted: {total_entities}")
        print(f"Total relationships extracted: {total_relationships}")
        print(f"Total triples processed: {total_triples}")
        
        if errors:
            print(f"\nErrors encountered: {len(errors)}")
            for error in errors[:5]:
                print(f"  - {error}")
            if len(errors) > 5:
                print(f"  ... and {len(errors) - 5} more errors")


if __name__ == "__main__":
    main()
