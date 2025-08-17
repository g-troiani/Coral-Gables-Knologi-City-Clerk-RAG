import hashlib
import re
from typing import Optional
from .ontology_attributes import OntologyAttributesRegistry as _OAR
from .entity_id_standards import EntityIDStandards

def _hash8(s: str) -> str:
    return hashlib.sha1((s or '').encode('utf-8')).hexdigest()[:8]

def ensure_min_document_props(entity: dict) -> dict:
    # NEW: also ensure ontology-specified Document attributes exist
    try:
        ensure_min_entity_props(entity, "Document")
    except Exception:
        pass
    # Your required minimum prop set
    entity['Source_File_Name'] = entity.get('Source_File_Name') or entity.get('name')
    entity['Source_File_Path'] = entity.get('Source_File_Path') or f"/{entity.get('name','')}"
    entity['sourceURL'] = entity.get('sourceURL') or ''
    # Keep both "document_type" and "type" for compatibility
    if entity.get('document_type') and not entity.get('type'):
        entity['type'] = entity['document_type']
    if not entity.get('entity_type'):
        entity['entity_type'] = 'Document'
    return entity

def build_document(document_id: str, *, doc_type: str, source_file_name: str, title: str,
                   issue_date: str, metadata: dict, meeting_date: str = None, **kw) -> dict:
    doc = {
        'documentID': document_id,
        'name': source_file_name,
        'title': title,
        'document_type': doc_type,
        'type': doc_type,

        'issueDate': issue_date,
        'meeting_date': meeting_date or (metadata or {}).get('meeting_date') or issue_date,

        'summary': kw.get('summary',''),
        'page_count': kw.get('page_count') or (metadata or {}).get('page_count') or 0,
        'status': kw.get('status','final'),
        'version': kw.get('version','1.0'),

        'Source_File_Name': source_file_name,
        'Source_File_Path': kw.get('Source_File_Path') or (metadata or {}).get('Source_File_Path') or f'/{source_file_name}',
        'sourceURL': kw.get('sourceURL') or (metadata or {}).get('sourceURL') or '',

        'document_classification': {'agenda':'agenda','ordinance':'legal','resolution':'legal','transcript':'verbatim'}.get(doc_type,'general'),
        'is_proclamation': 'proclamation' in (title or '').lower(),

        'parent_meeting_id': kw.get('parent_meeting_id'),

        'partitionKey': kw.get('partitionKey','cgGraph'),
        'entity_type': 'Document',
        '_sources': kw.get('_sources',[]),
    }
    return ensure_min_document_props(doc)

# --- Policy builders ---

def make_policy_id(policy_type: str, year: str, num: str, source_file_name: str) -> str:
    # Delegate to centralized implementation
    return EntityIDStandards.make_policy_id(policy_type, year, num, source_file_name)

def build_policy(policy_id: str, *, policy_type: str, source_file_name: str, title: str,
                 ordinance_year: str, ordinance_number: str, issue_date: str, meeting_date: str,
                 metadata: dict, **kw) -> dict:
    policy = {
        'policyID': policy_id,
        'name': f"{policy_type.capitalize()} {ordinance_year}-{ordinance_number}",
        'title': title,
        'policy_type': policy_type,

        'effective_date': kw.get('effective_date') or issue_date,
        'issueDate': issue_date,
        'meeting_date': meeting_date,
        'status': kw.get('status','enacted'),
        'version': kw.get('version','1.0'),

        # Back-compat + your minimum set
        'document_type': policy_type,
        'type': policy_type,
        'Source_File_Name': source_file_name,
        'Source_File_Path': kw.get('Source_File_Path') or (metadata or {}).get('Source_File_Path') or f'/{source_file_name}',
        'sourceURL': kw.get('sourceURL') or (metadata or {}).get('sourceURL') or '',
        'entity_type': 'Document',

        'ordinance_year': ordinance_year,
        'ordinance_number': ordinance_number,

        'parent_meeting_id': kw.get('parent_meeting_id'),
        'partitionKey': kw.get('partitionKey','cgGraph'),
        '_sources': kw.get('_sources', []),
    }
    return ensure_min_document_props(policy)

# NEW: generic ontology-backed filler
def ensure_min_entity_props(entity: dict, entity_type: Optional[str] = None) -> dict:
    """
    Ensure that all attributes defined in ontology_model_final.txt for this entity type exist.
    Missing attributes are added with value None. No-op if the ontology isn't available.
    """
    if not isinstance(entity, dict):
        return entity
    et = (
        entity_type
        or entity.get("type")
        or _infer_entity_type_from_ids(entity)
        or "Unknown"
    )
    try:
        _OAR.ensure_defaults(et, entity)
    except Exception:
        # Never fail the pipeline on attribute padding
        pass
    return entity

def _infer_entity_type_from_ids(entity: dict) -> Optional[str]:
    """
    Lightweight inference: look for known ID fields via EntityIDStandards.
    """
    try:
        for et in [
            "Person","Organization","Document","Policy","AgendaItem","Event","Location",
            "Role","Topic","VoteOutcome","Action","Board","Appointment","Contract",
            "Asset","Project","Technology","Section","Presentation","PublicComment","LegalReference"
        ]:
            id_field = EntityIDStandards.get_id_field(et)
            if id_field in entity:
                return et
    except Exception:
        return None
    return None
