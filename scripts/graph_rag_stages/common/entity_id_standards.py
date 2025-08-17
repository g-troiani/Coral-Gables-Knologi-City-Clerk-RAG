"""
Centralized entity ID field standards to ensure consistency across the pipeline.
"""

from typing import Any, Dict, Optional, Tuple
import re
import hashlib

class EntityIDStandards:
    """Single source of truth for entity ID field names."""
    
    # Standard ID field for each entity type
    ID_FIELD_MAP = {
        'Person': 'personID',
        'Organization': 'orgID', 
        'Location': 'locationID',
        'Event': 'eventID',
        'Document': 'documentID',  # NOT docID
        'AgendaItem': 'agendaItemID',  # NOT agendaID
        'Policy': 'policyID',
        'Asset': 'assetID',
        'Contract': 'contractID',
        'Project': 'projectID',
        'Role': 'roleID',
        'Action': 'actionID',
        'Topic': 'topicID',
        'Section': 'sectionID',
        'Technology': 'technologyID',
        'VoteOutcome': 'voteOutcomeID',
        'Transcript': 'transcriptID',
        'Presentation': 'presentationID',
        'PublicComment': 'publicCommentID',
        'Issue': 'issueID',
        'Recommendation': 'recommendationID',
        'HistoricalReference': 'historicalReferenceID',
        'Appointment': 'appointmentID',
        'Board': 'boardID',
        'LegalReference': 'legalReferenceID',
        'AgendaDocument': 'agendaDocID'
    }
    
    @classmethod
    def get_id_field(cls, entity_type: str) -> str:
        """Get the standard ID field name for an entity type."""
        return cls.ID_FIELD_MAP.get(entity_type, f"{entity_type.lower()}ID")
    
    @classmethod
    def normalize_entity_id_fields(cls, entity: dict, entity_type: str) -> dict:
        """Normalize entity to use standard ID field."""
        normalized = entity.copy()
        id_field = cls.get_id_field(entity_type)
        
        # Handle various ID field variations
        possible_id_fields = [
            'id', 'ID', id_field,
            f"{entity_type.lower()}ID",
            f"{entity_type}ID",
            'docID' if entity_type == 'Document' else None,
            'agendaID' if entity_type == 'AgendaItem' else None
        ]
        
        # Find the ID value from possible fields
        id_value = None
        for field in possible_id_fields:
            if field and field in entity:
                id_value = entity[field]
                break
        
        if id_value:
            # Set the standard ID field
            normalized[id_field] = id_value
            # Also keep 'id' for compatibility
            normalized['id'] = id_value
            
            # Remove non-standard fields
            for field in possible_id_fields:
                if field and field != id_field and field != 'id' and field in normalized:
                    del normalized[field]
        
        return normalized 
    
    # Prefix to type mapping for ID inference
    PREFIX_TYPE_MAP = {
        "person_": "Person",
        "org_": "Organization",
        "document_": "Document",
        "policy_": "Policy",
        "agendaItem_": "AgendaItem",
        "agendaDoc_": "AgendaDocument",
        "section_": "Section",
        "event_": "Event",
        "action_": "Action",
        "location_": "Location",
        "topic_": "Topic",
        "asset_": "Asset",
        "contract_": "Contract",
        "technology_": "Technology",
        "vote": "VoteOutcome",      # e.g., "vote_outcome_..."
        "project_": "Project",
        "meeting_": "Meeting",
    }

    @staticmethod
    def infer_type_from_id(entity_id: str) -> Optional[str]:
        if not entity_id:
            return None
        for prefix, t in EntityIDStandards.PREFIX_TYPE_MAP.items():
            if entity_id.startswith(prefix):
                return t
        return None

    @staticmethod
    def canonicalize_id(entity_id: str, known_ids: set) -> Optional[str]:
        if not entity_id:
            return None
        if entity_id in known_ids:
            return entity_id
        candidates = {
            entity_id.replace("-", "_"),
            entity_id.replace("_", "-"),
            entity_id.lower(),
        }
        for c in list(candidates):
            if c in known_ids:
                return c
            # try case-insensitive match
            for k in known_ids:
                if k.lower() == c:
                    return k
        return None

    # ---------- Shared helpers (centralized) ----------
    @staticmethod
    def _hash8(s: str) -> str:
        return hashlib.sha1((s or '').encode('utf-8')).hexdigest()[:8]

    @staticmethod
    def make_policy_id(policy_type: str, year: str, num: str, source_file_name: str) -> str:
        # EXACT convention requested:
        # document_ordinance_2024_02_14051e20
        norm = (policy_type or '').lower().strip()
        year = re.search(r'(20\d{2})', year or '') and re.search(r'(20\d{2})', year).group(1) or ''
        # keep number as extracted; if empty digits, default to '00' for shape parity
        num_raw = str(num or '')
        num_match = re.search(r'(\d{1,3})', num_raw)
        num_keep = num_match.group(1) if num_match else '00'
        return f"document_{norm}_{year}_{num_keep}_{EntityIDStandards._hash8(source_file_name)}"

    @staticmethod
    def normalize_date_yyyymmdd(s: Optional[str]) -> str:
        if not s:
            return ""
        s = str(s).strip().replace("/", "-").replace(".", "-").replace("_", "-")
        m1 = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
        if m1:
            y, m, d = m1.groups()
            return f"{y}{m.zfill(2)}{d.zfill(2)}"
        m2 = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{4})$", s)
        if m2:
            m, d, y = m2.groups()
            return f"{y}{m.zfill(2)}{d.zfill(2)}"
        m3 = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{2})$", s)
        if m3:
            m, d, y2 = m3.groups()
            return f"20{y2}{m.zfill(2)}{d.zfill(2)}"
        return s

    @staticmethod
    def _clean_code(code: Optional[str]) -> str:
        if not code:
            return ""
        return re.sub(r"[^A-Z0-9]", "", str(code).upper())

    @staticmethod
    def _extract_e_code(entity: Dict) -> Optional[str]:
        # Try explicit fields first
        for k in ("code", "itemCode", "agendaCode"):
            val = entity.get(k)
            if val:
                m = re.search(r"\b([A-Z]-?\d+)\b", str(val))
                if m:
                    return m.group(1)
        # Try title/name fallbacks
        text = f"{entity.get('title','')} {entity.get('name','')}"
        m = re.search(r"\b([A-Z]-?\d+)\b", text)
        return m.group(1) if m else None

    @staticmethod
    def _extract_ordres_number(entity: Dict) -> Tuple[Optional[str], Optional[str]]:
        """
        Returns (kind, number) where kind in {'ordinance','resolution'} and number like '2024-03'.
        """
        title = f"{entity.get('title','')} {entity.get('name','')}"
        if entity.get("ordinanceNumber"):
            return ("ordinance", str(entity["ordinanceNumber"]).strip())
        if entity.get("resolutionNumber"):
            return ("resolution", str(entity["resolutionNumber"]).strip())
        m = re.search(r"\bOrdinance\s+(\d{4}-\d+)\b", title, re.I)
        if m:
            return ("ordinance", m.group(1))
        m = re.search(r"\bResolution\s+(\d{4}-\d+)\b", title, re.I)
        if m:
            return ("resolution", m.group(1))
        return (None, None)

    # ---------- Canonical ID builders (source of truth) ----------
    @staticmethod
    def preferred_policy_id(entity: Dict) -> Optional[str]:
        kind, num = EntityIDStandards._extract_ordres_number(entity)  # 'ordinance'/'resolution', '2024-03'
        if not (kind and num):
            return None
        parts = str(num).split('-', 1)
        if len(parts) != 2 or not parts[0].isdigit():
            return None
        year, ordinal = parts[0], parts[1]
        seed = (
            entity.get("Source_File_Name")
            or entity.get("documentID")
            or entity.get("policyID")
            or num
        )
        return EntityIDStandards.make_policy_id(kind, year, ordinal, seed)

    @staticmethod
    def preferred_agendaitem_id(entity: Dict) -> Optional[str]:
        code = EntityIDStandards._extract_e_code(entity)
        if not code:
            return None
        code_clean = EntityIDStandards._clean_code(code)  # E-4 -> E4
        date_norm = EntityIDStandards.normalize_date_yyyymmdd(
            entity.get("meetingDate") or entity.get("meeting_date") or entity.get("date") or ""
        )
        if not date_norm:
            return None
        date_canon = f"{date_norm[0:4]}_{date_norm[4:6]}_{date_norm[6:8]}"
        return f"agenda_item_{code_clean}_{date_canon}"