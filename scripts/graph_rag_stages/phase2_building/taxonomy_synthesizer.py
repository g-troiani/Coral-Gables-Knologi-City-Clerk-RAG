"""
Synthesizes taxonomy entities from JSON extraction output into NER-compatible format.
Writes to simple_ner_graph/registry/ directory maintaining exact same structure as NER.
"""

import json
import logging
import re
import hashlib
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import asyncio

from scripts.graph_rag_stages.common.graph_entity_toolkit import GraphEntityToolkit
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
from scripts.graph_rag_stages.common.standards import (
    build_document, build_policy, make_policy_id, ensure_min_document_props, ensure_min_entity_props
)
try:
    from scripts.graph_rag_stages.common.relationship_labels import normalize_rel_label
except Exception:
    def normalize_rel_label(x: str) -> str:
        return (x or "").strip()

log = logging.getLogger(__name__)

def _hash8(s: str) -> str:
    return EntityIDStandards._hash8(s)

def _clean_agenda_code(code: str) -> str:
    return EntityIDStandards.clean_agenda_code(code)

def _policy_id_from_ordinance(ordinance_number: str, stable_seed: str) -> str:
    # Delegate to centralized standard
    parts = str(ordinance_number or '').split('-', 1)
    if len(parts) == 2 and parts[0].isdigit():
        year, ordinal = parts[0], parts[1]
        return EntityIDStandards.make_policy_id('ordinance', year, ordinal, stable_seed)
    # Fallback for non-standard format - no more hashes
    num = (ordinance_number or '').strip().replace('-', '_')
    return f"policy_ordinance_{num}"

# Import debug flags from main pipeline
try:
    from scripts.graph_rag_stages.main_pipeline import DEBUG_DOCUMENT_FLOW, DEBUG_FILE_DISCOVERY
except ImportError:
    # Fallback if main_pipeline is not available
    DEBUG_DOCUMENT_FLOW = False
    DEBUG_FILE_DISCOVERY = False

class TaxonomySynthesizer:
    """Synthesizes taxonomy entities from JSON into NER-compatible format."""
    
    def __init__(self, output_dir: Path, toolkit: GraphEntityToolkit = None):
        """
        Initialize synthesizer.
        
        Args:
            output_dir: Base directory (e.g., simple_ner_graph)
            toolkit: GraphEntityToolkit instance
        """
        self.output_dir = Path(output_dir)
        self.registry_dir = self.output_dir / "registry"
        self.toolkit = toolkit or GraphEntityToolkit()
        
        # Create registry directories for each entity type
        for entity_type in UnifiedOntology.get_entity_categories():
            (self.registry_dir / entity_type).mkdir(parents=True, exist_ok=True)
        
        # Create relationships directory
        (self.registry_dir / "relationships").mkdir(parents=True, exist_ok=True)
        
        # Track what we've created to avoid duplicates
        self.created_entities = {}
        self.created_relationships = []
        
        # Track current file being processed for source metadata
        self.current_source_file = None

    # --- helper: ensure a minimal stub exists for reused IDs ---
    def _ensure_entity_stub(self, entity_type: str, entity_id: str, attrs: Optional[Dict[str, Any]] = None) -> None:
        """Register a minimal entity so relationship validation knows its type."""
        if not entity_type or not entity_id:
            return
        bucket = self.created_entities.setdefault(entity_type, {})
        if entity_id in bucket:
            return
        entity = dict(attrs or {})
        id_key = EntityIDStandards.get_id_field(entity_type)
        entity[id_key] = entity_id
        entity["type"] = entity_type
        bucket[entity_id] = entity

    # --- NEW: create-correct-at-origin validators ---
    def _type_of(self, entity_id: str) -> Optional[str]:
        """Return ontology type for an ID we've created or that exists in merged entities."""
        # Strategy 1: Check taxonomy created entities (fast)
        for et, bucket in self.created_entities.items():
            if entity_id in bucket:
                return et
        
        # Strategy 2: Check merged entities for cross-source types (slower but comprehensive)
        return self._get_type_from_merged_entities(entity_id)
    
    def _get_type_from_merged_entities(self, entity_id: str) -> Optional[str]:
        """Get entity type from merged entities (cross-source) or infer from ID pattern."""
        try:
            # Check common entity types that might contain cross-source entities
            entity_types = ['AgendaItem', 'Document', 'Policy', 'Person', 'Organization', 'Event', 'Section']
            
            for entity_type in entity_types:
                merged_file = self.output_dir / 'merged' / 'entities' / f'{entity_type}.json'
                if merged_file.exists():
                    import json
                    with open(merged_file, 'r') as f:
                        data = json.load(f)
                    
                    entities = data.get('entities', [])
                    for entity in entities:
                        # Check multiple ID fields
                        entity_ids = [
                            entity.get('id'),
                            entity.get(f'{entity_type.lower()}ID'),
                            entity.get('agendaItemID'),  # Specific for AgendaItem
                            entity.get('documentID'),    # Specific for Document
                            entity.get('policyID'),      # Specific for Policy
                            entity.get('sectionID'),     # Specific for Section
                        ]
                        
                        if entity_id in entity_ids:
                            return entity_type
                            
        except Exception as e:
            log.debug(f"Error checking merged entities for type of {entity_id}: {e}")
        
        # Fallback: Infer type from ID pattern (for newly created entities)
        if entity_id:
            id_lower = entity_id.lower()
            if id_lower.startswith('agenda_item_') or id_lower.startswith('agendaitem_'):
                return 'AgendaItem'
            elif id_lower.startswith('document_'):
                return 'Document'
            elif id_lower.startswith('policy_'):
                return 'Policy'
            elif id_lower.startswith('event_'):
                return 'Event'
            elif id_lower.startswith('person_'):
                return 'Person'
            elif id_lower.startswith('org_'):
                return 'Organization'
            elif id_lower.startswith('section_'):
                return 'Section'
            
        return None

    def _rel_allowed(self, rel_type: str, src_type: str, tgt_type: str) -> bool:
        """Minimal, explicit mapping for relationships used by this synthesizer."""
        RULES = {
            # Legacy relationships (for backward compatibility)
            'hasDocument':      ({'Event','Policy'}, {'Document'}),
            'isAbout':          ({'Document','Policy'}, {'AgendaItem'}),
            'adoptedAt':        ({'Policy'}, {'Event'}),
            'enactsPolicy':     ({'Document'}, {'Policy'}),
            'isRecordOf':       ({'Document'}, {'AgendaItem'}),
            
            # Standard ontology relationships
            'hasSection':       ({'Document'}, {'Section'}),
            'hasAgendaItem':    ({'Section'}, {'AgendaItem'}),
            'hasAgenda':        ({'Event'}, {'Document'}),
            'hasTranscript':    ({'Event', 'AgendaItem', 'Section'}, {'Document'}),
            'discusses':        ({'Event'}, {'AgendaItem', 'Policy', 'Document'}),
            'discussedIn':      ({'Document', 'Policy', 'AgendaItem'}, {'Event'}),
            'references':       ({'Document'}, {'Document', 'Policy'}),
            'isPartOf':         ({'AgendaItem'}, {'Document'}),
            'implements':       ({'AgendaItem'}, {'Policy', 'Document'}),
            'implementedBy':    ({'Policy'}, {'Document'}),
            'embodies':         ({'Document'}, {'Policy'}),
            'mentionedIn':      ({'Policy', 'Person', 'Organization', 'VoteOutcome'}, {'Document'}),
            'sponsors':         ({'Person'}, {'Policy'}),
            'votedOn':          ({'VoteOutcome'}, {'Policy'}),
            'resultsIn':        ({'AgendaItem'}, {'VoteOutcome'}),  # Added for VoteOutcome connections
            'isLocatedAt':      ({'Organization'}, {'Location'}),
        }
        if rel_type not in RULES:
            return False
        sources, targets = RULES[rel_type]
        return (src_type in sources) and (tgt_type in targets)

    def _provenance_for_file(self, file_path: Optional[Path], extra: Optional[Dict] = None) -> Dict[str, Any]:
        """Build a minimal provenance dict for the originating file."""
        base = {
            "Source_File_Name": (file_path.name if isinstance(file_path, Path) else None),
            "Source_File_Path": (str(file_path) if isinstance(file_path, Path) else None),
        }
        if extra:
            for k, v in extra.items():
                if v is not None and k not in base:
                    base[k] = v
        return base
    
    def _cleanup_redundant_date_fields(self, entity: Dict[str, Any], entity_type: str) -> Dict[str, Any]:
        """Remove redundant date fields while preserving canonical ones."""
        
        # Define canonical date fields for each entity type based on ontology and phase3_querying
        canonical_fields = {
            'Document': ['meetingDate', 'issueDate'],  # Keep both - different purposes
            'Event': ['dateTime'],  # Remove meetingDate, date - keep dateTime (used by phase3)
            'Policy': ['meetingDate', 'expirationDate'],  # Removed effectiveDate - rarely used
            'AgendaItem': ['meetingDate'],  # Standard field
            'Action': ['dateTime'],  # Standard field
            'AgendaDocument': ['meetingDate'],  # Remove date - keep meetingDate
        }
        
        # Get canonical fields for this entity type
        keep_fields = canonical_fields.get(entity_type, [])
        if not keep_fields:
            return entity  # No cleanup needed for this type
        
        # List of all possible date fields
        all_date_fields = [
            'meetingDate', 'meeting_date', 'Meeting_Date',
            'issueDate', 'issue_date', 'Issue_Date', 
            'dateTime', 'date_time', 'Date_Time',
            'effectiveDate', 'effective_date', 'Effective_Date',
            'expirationDate', 'expiration_date', 'Expiration_Date',
            'date', 'Date'
        ]
        
        # Remove redundant date fields
        cleaned_entity = entity.copy()
        removed_fields = []
        
        for field in all_date_fields:
            if field in cleaned_entity and field not in keep_fields:
                removed_fields.append(field)
                del cleaned_entity[field]
        
        if removed_fields:
            log.debug(f"Cleaned {entity_type} {entity.get('id', 'unknown')}: removed {removed_fields}")
        
        return cleaned_entity

    def _date_to_yyyy_mm_dd(self, s: str) -> str:
        """Normalize dates like '01.09.2024' or '1-9-2024' to '2024_01_09'."""
        if not s:
            return "unknown"
        import re
        t = s.strip().replace("/", "-").replace(".", "-").replace("_", "-")
        m1 = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", t)
        if m1:
            y, m, d = m1.groups()
            return f"{y}_{m.zfill(2)}_{d.zfill(2)}"
        m2 = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{4})$", t)
        if m2:
            m, d, y = m2.groups()
            return f"{y}_{m.zfill(2)}_{d.zfill(2)}"
        return s.replace("-", "_")

    def _preferred_policy_id(self, entity: Dict) -> Optional[str]:
        """Generate preferred policy ID using centralized standards."""
        return EntityIDStandards.preferred_policy_id(entity)
    
    async def synthesize_from_json(self, json_dir: Path) -> Dict[str, int]:
        """
        Read ontology JSON files and synthesize entities/relationships.
        
        Args:
            json_dir: Directory containing extracted JSON files
            
        Returns:
            Statistics of created entities by type
        """
        log.info(f"🔄 Synthesizing taxonomy from {json_dir}")
        
        # Find agenda files
        agenda_files = []
        agenda_dir = json_dir / "agenda"
        if agenda_dir.exists():
            agenda_files = list(agenda_dir.glob("agenda_*.json"))
        else:
            # Fallback to flat structure
            agenda_files = list(json_dir.glob("*agenda*.json"))
        
        # Find legal files
        legal_files = []
        legal_dir = json_dir / "legal"
        if legal_dir.exists():
            legal_files.extend(list(legal_dir.glob("*enhanced*.json")))
        else:
            # Fallback to flat structure
            legal_files.extend(list(json_dir.glob("*enhanced*.json")))
        
        # Find verbatim files
        verbatim_files = []
        verbatim_dir = json_dir / "verbatim"
        if verbatim_dir.exists():
            verbatim_files = list(verbatim_dir.glob("*_verbatim_transcript*.json"))
        else:
            # Fallback to flat structure
            verbatim_files = list(json_dir.glob("*verbatim*.json"))
        
        log.info(f"Found {len(agenda_files)} agenda files")
        log.info(f"Found {len(legal_files)} legal documents")
        log.info(f"Found {len(verbatim_files)} verbatim transcripts")
        
        # Process agenda files
        for agenda_file in agenda_files:
            await self._process_agenda_file(agenda_file)
        
        # Process legal files
        for legal_file in legal_files:
            await self._process_legal_file(legal_file)
        
        # Process verbatim files
        for verbatim_file in verbatim_files:
            await self._process_verbatim_file(verbatim_file)
        
        # Save all entities
        await self._save_all_entities()
        
        # Return statistics
        stats = {entity_type: len(entities) for entity_type, entities in self.created_entities.items()}
        log.info(f"✅ Synthesized: {stats}")
        return stats

    async def synthesize_meeting(self, meeting_dir: Path, strict=True):
        """
        Gated policy creation:
            - require AgendaItem(E-*) AND its Section.
            - if item exists but has no section, create an ad-hoc Section node and wire it.
            - if item doesn't exist at all and strict=True -> raise.
        """
        out_vertices, out_edges = [], []

        def _slug(s: str) -> str:
            return re.sub(r'[^a-z0-9_]+', '-', (s or '').strip().lower())

        def _section_vertex(section_name: str, meeting_date: str, partitionKey='cgGraph'):
            sid = f"section_{_slug(section_name)}_{meeting_date.replace('.','_')}"
            return {
                'sectionID': sid,
                'name': section_name,
                'meeting_date': meeting_date,
                'partitionKey': partitionKey,
                'entity_type': 'Section'
            }

        def _event_id(meeting_title: str, meeting_date: str):
            # Remove hash, use descriptive ID based on title and date
            title_clean = re.sub(r'[^a-z0-9\s]', '', (meeting_title or 'city_commission_meeting').lower())
            title_clean = re.sub(r'\s+', '_', title_clean.strip())
            date_clean = meeting_date.replace('.', '_').replace('-', '_').replace('/', '_')
            return f"event_{title_clean}_{date_clean}"

        def _extract_code_from_filename_or_text(name: str, title: str, text: str) -> str:
            for source in (name or '', title or '', text or ''):
                m = re.search(r'\b([A-H]|[1-9])\s*[-\.]?\s*(\d+)\b', source)
                if m:
                    return f"{m.group(1)}-{m.group(2)}"
            return ''

        def _extract_ord_year_num(name: str, text: str):
            # e.g., "2024-02 - 01_09_2024.pdf" or within full text "Ordinance No. 2024-02"
            for source in (name or '', text or ''):
                m = re.search(r'\b(20\d{2})\s*[-/]\s*(0*\d{1,3})\b', source)
                if m:
                    return m.group(1), m.group(2).lstrip('0') or '0'
                m2 = re.search(r'\bOrdinance\s+No\.?\s*(20\d{2})\s*[-/]\s*(\d{1,3})\b', source, re.I)
                if m2:
                    return m2.group(1), m2.group(2)
            return '', ''

        # --- Load agenda JSON (single meeting)
        agenda_files = sorted((meeting_dir / "agenda").glob("agenda_*.json"))
        if not agenda_files:
            return out_vertices, out_edges

        agenda = json.loads(agenda_files[0].read_text(encoding='utf-8'))
        meeting_info = agenda.get('meeting_info', {})
        meeting_date = meeting_info.get('date') or agenda.get('meeting_date') or 'unknown'
        meeting_title = "City Commission Meeting"
        event_id = agenda.get('meeting_info', {}).get('eventID') \
                   or agenda.get('eventID') \
                   or _event_id(meeting_title, meeting_date)
        event_vertex = {
            'eventID': event_id,
            'name': meeting_title,
            'meeting_date': meeting_date,
            'partitionKey': 'cgGraph',
            'entity_type': 'Event'
        }
        out_vertices.append(event_vertex)

        # --- Build Section vertices from extracted headers
        section_by_name = {}
        for s in agenda.get('sections', []):
            sec_name = s.get('section_name') or s.get('name') or s.get('title')
            if not sec_name:
                continue
            sec_id = s.get('section_id')
            if sec_id:
                v = {
                    'sectionID': sec_id,
                    'name': sec_name,
                    'meeting_date': meeting_date,
                    'partitionKey': 'cgGraph',
                    'entity_type': 'Section'
                }
            else:
                v = _section_vertex(sec_name, meeting_date)
            section_by_name[sec_name] = v
            out_vertices.append(v)

        # --- Build AgendaItem vertices and inSection edges
        items = agenda.get('agenda_items', [])
        item_by_code = {}
        for it in items:
            code = it.get('item_code')
            if not code:
                continue
            aid = f"agenda_item_{_slug(code)}_{meeting_date.replace('.','_')}"
            v = {
                'agendaItemID': aid,
                'name': f"{code} - {it.get('title','')[:80]}",
                'code': code,
                'title': it.get('title',''),
                'relative_section_id': it.get('relative_section_id'),
                'meeting_date': meeting_date,
                'partitionKey': 'cgGraph',
                'entity_type': 'AgendaItem'
            }
            item_by_code[code] = v
            out_vertices.append(v)

            # inSection
            sec_name = it.get('section_name')
            if sec_name and sec_name in section_by_name:
                out_edges.append({
                    'from': v['agendaItemID'], 'to': section_by_name[sec_name]['sectionID'], 'label': 'inSection'
                })
            elif it.get('relative_section_id'):
                out_edges.append({
                    'from': v['agendaItemID'], 'to': it['relative_section_id'], 'label': 'inSection'
                })
            else:
                # create ad-hoc section if missing
                ad_hoc = _section_vertex("Unspecified Section", meeting_date)
                sid = ad_hoc['sectionID']
                if sid not in {x.get('sectionID') for x in out_vertices if x.get('sectionID')}:
                    out_vertices.append(ad_hoc)
                out_edges.append({'from': v['agendaItemID'], 'to': sid, 'label': 'inSection'})
                v['relative_section_id'] = sid

        # --- Documents (agenda, transcripts, legal) -> Event hasDocument
        # Agenda document
        agenda_doc = build_document(
            document_id=f"document_agenda_{meeting_date.replace('.','_')}",
            doc_type='agenda',
            source_file_name=agenda.get('source_file'),
            title=f"Agenda {meeting_date}",
            issue_date=meeting_date,
            metadata=agenda.get('metadata', {}),
            meeting_date=meeting_date,
            parent_meeting_id=event_id
        )
        out_vertices.append(agenda_doc)
        out_edges.append({'from': event_id, 'to': agenda_doc['documentID'], 'label': 'hasDocument'})

        # Transcripts
        verb_dir = meeting_dir / "verbatim"
        if verb_dir.exists():
            for jf in sorted(verb_dir.glob("*_verbatim_transcript*.json")):
                j = json.loads(jf.read_text(encoding='utf-8'))
                src = j.get('Source_File_Name') or j.get('source_file') or jf.name
                doc = build_document(
                    document_id=f"document_transcript_{_slug(src)}_{meeting_date.replace('.','_')}",
                    doc_type='transcript',
                    source_file_name=src,
                    title=f"Transcript {meeting_date}",
                    issue_date=meeting_date,
                    metadata=j.get('metadata', {}),
                    meeting_date=meeting_date,
                    parent_meeting_id=event_id
                )
                out_vertices.append(doc)
                out_edges.append({'from': event_id, 'to': doc['documentID'], 'label': 'hasDocument'})

                # If filename contains an item code, wire isRecordOf
                code = _extract_code_from_filename_or_text(src, '', j.get('full_text',''))
                if code and code in item_by_code:
                    out_edges.append({'from': doc['documentID'], 'to': item_by_code[code]['agendaItemID'], 'label': 'isRecordOf'})

        # Legal docs (ordinances/resolutions)
        legal_dir = meeting_dir / "legal"
        if legal_dir.exists():
            for jf in sorted(legal_dir.glob("*enhanced*.json")):
                j = json.loads(jf.read_text(encoding='utf-8'))
                src = j.get('Source_File_Name') or j.get('name') or jf.name
                doc_type = (j.get('document_type') or '').lower() or ('ordinance' if 'ordinance' in src.lower() else 'resolution')

                full_text = j.get('full_text','')
                title = j.get('title') or src
                issue_date = meeting_date

                # Build Document vertex (ensure required props)
                doc_id = f"document_{doc_type}_{_slug(src)}_{meeting_date.replace('.','_')}"
                doc_v = build_document(
                    document_id=doc_id,
                    doc_type=doc_type,
                    source_file_name=src,
                    title=title,
                    issue_date=issue_date,
                    metadata=j.get('metadata', {}),
                    meeting_date=meeting_date,
                    parent_meeting_id=event_id
                )
                out_vertices.append(doc_v)
                out_edges.append({'from': event_id, 'to': doc_v['documentID'], 'label': 'hasDocument'})

                # Map to AgendaItem by code
                code = j.get('agenda_item_code') or _extract_code_from_filename_or_text(src, title, full_text)
                if not code or code not in item_by_code:
                    if strict:
                        raise RuntimeError(f"Strict mode: missing AgendaItem for legal doc {src} (code={code!r}).")
                    else:
                        continue  # skip policy creation if not matched

                ai = item_by_code[code]

                # Document -> AgendaItem
                out_edges.append({'from': doc_v['documentID'], 'to': ai['agendaItemID'], 'label': 'isAbout'})

                # Ensure AgendaItem has a section; if not, create ad-hoc
                rel_sec = ai.get('relative_section_id')
                if not rel_sec:
                    ad_hoc = _section_vertex("Unspecified Section", meeting_date)
                    sid = ad_hoc['sectionID']
                    if sid not in {x.get('sectionID') for x in out_vertices if x.get('sectionID')}:
                        out_vertices.append(ad_hoc)
                    out_edges.append({'from': ai['agendaItemID'], 'to': sid, 'label': 'inSection'})
                    ai['relative_section_id'] = sid

                # --- GATED POLICY CREATION ---
                ord_year, ord_num = _extract_ord_year_num(src, full_text)
                pol_id = make_policy_id(doc_type, ord_year or meeting_date[-4:], ord_num or '0', src)
                policy_v = build_policy(
                    policy_id=pol_id,
                    policy_type=doc_type,
                    source_file_name=src,
                    title=title,
                    ordinance_year=ord_year or meeting_date[-4:],  # fallback
                    ordinance_number=ord_num or '0',
                    issue_date=issue_date,
                    meeting_date=meeting_date,
                    metadata=j.get('metadata', {}),
                    parent_meeting_id=event_id
                )
                out_vertices.append(policy_v)

                # Edges for policy
                out_edges.append({'from': doc_v['documentID'], 'to': policy_v['policyID'], 'label': 'enactsPolicy'})
                out_edges.append({'from': policy_v['policyID'], 'to': ai['agendaItemID'], 'label': 'isAbout'})
                out_edges.append({'from': event_id, 'to': policy_v['policyID'], 'label': 'adoptedAt'})

                # --- VOTE OUTCOME CREATION ---
                # Extract and create VoteOutcome entities from legal_metadata
                legal_metadata = j.get('legal_metadata', {})
                vote_details = legal_metadata.get('vote_details', {})
                
                # Create VoteOutcome only if meaningful voting data exists
                if vote_details and (vote_details.get('yeas') or vote_details.get('nays')):
                    vote_outcome_v, vote_edges = _create_vote_outcome_entities(
                        legal_metadata, ai['agendaItemID'], doc_v['documentID'], 
                        policy_v['policyID'], meeting_date
                    )
                    if vote_outcome_v:
                        out_vertices.append(vote_outcome_v)
                        out_edges.extend(vote_edges)

        # Final pass: enforce minimum props on anything that is a Document
        for v in out_vertices:
            if v.get('entity_type') == 'Document' or v.get('documentID'):
                ensure_min_document_props(v)

        return out_vertices, out_edges
    
    async def _process_agenda_file(self, agenda_file: Path) -> None:
        """Process an agenda JSON file to extract taxonomy entities."""
        try:
            with open(agenda_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract original PDF metadata from JSON data
            original_pdf_name = data.get('Source_File_Name', agenda_file.name.replace('.json', '.pdf'))
            original_pdf_path = data.get('Source_File_Path', str(agenda_file))
            
            # Set current source to None to use explicit metadata
            self.current_source_file = None
            
            # Log the actual structure
            log.info(f"📋 Agenda JSON keys: {list(data.keys())}")
            
            # ADD DEBUG LOGGING
            log.info(f"📋 Processing agenda with meeting_date: {data.get('meeting_date')}")
            log.info(f"   Sections found: {len(data.get('sections', []))}")
            
            # Check if sections exist and are not empty
            sections = data.get('sections', [])
            if not sections:
                log.warning(f"⚠️ No sections found in {agenda_file.name} — creating Event + Agenda Document only")
                log.info(f"   Available keys: {list(data.keys())}")
            
            meeting_date = data.get('meeting_date', 'unknown')
            doc_id = data.get('doc_id', agenda_file.stem)
            source_file = original_pdf_name
            
            # Create Meeting entity
            meeting_id = self._create_entity(
                'Event',
                {
                    'name': f"City Commission Meeting {meeting_date}",
                    'type': 'Regular Meeting',
                    'dateTime': meeting_date,
                    'status': 'Completed',
                    'outcome': 'Adjourned',
                    'Source_File_Name': original_pdf_name,
                    'Source_File_Path': original_pdf_path
                },
                source=f"taxonomy_{agenda_file.stem}"
            )
            log.info(f"   Created Event: {meeting_id}")
            
            # Decide agenda Document ID first (reuse if found)
            normalized_date = self._date_to_yyyy_mm_dd(meeting_date)
            doc_entity_id = (
                self._find_existing_document_id(meeting_date, 'agenda')
                or f"document_agenda_{normalized_date}"
            )

            # Ensure the Document exists in this run (create or stub)
            if doc_entity_id not in (self.created_entities.get('Document') or {}):
                # Prefer creating the real Document entity
                created_id = self._create_entity(
                    'Document',
                    {
                        'documentID': doc_entity_id,
                        'title': f"City Commission Agenda {meeting_date}",
                        'documentType': 'agenda',
                        'status': 'Final',
                        'issueDate': meeting_date,
                        'meetingDate': meeting_date,
                        'sourceURL': (
                            data.get('hyperlinks', [{}])[0].get('url', '')
                            if data.get('hyperlinks') else None
                        ),
                        **self._provenance_for_file(agenda_file)
                    },
                    source=f"taxonomy_{agenda_file.stem}"
                )
                # If for any reason creation returned a different ID,
                # still guarantee we have a stub under doc_entity_id.
                if created_id != doc_entity_id:
                    self._ensure_entity_stub(
                        'Document', doc_entity_id,
                        {'title': f"City Commission Agenda {meeting_date}", 'documentType': 'agenda',
                         'issueDate': meeting_date, 'meetingDate': meeting_date}
                    )
            else:
                # Document exists elsewhere – register a stub so relationships can validate
                self._ensure_entity_stub('Document', doc_entity_id, {
                    'documentType': 'agenda'
                })
            
            # Make the Event own the agenda doc using ontology-compliant relationship
            self._create_relationship(
                'hasAgenda',
                meeting_id,
                doc_entity_id,
                {'role': 'agenda'}
            )
            
            # Process sections (only if they exist)
            if sections:
                for section in sections:
                    section_name = section.get('section_name', '')
                    log.info(f"   Processing section: {section_name}")
                    
                    # Create Section entity (canonical)
                    sec_slug = re.sub(r'[^a-z0-9]+', '_', (section_name or '').strip().lower()).strip('_')
                    section_id = f"section_{normalized_date}_{sec_slug}" if sec_slug else f"section_{normalized_date}"
                    section_entity_id = self._create_entity(
                        'Section',
                        {
                            'sectionID': section_id,
                            'name': section_name,
                            'meetingDate': meeting_date,
                            'order': section.get('section_order', 0),
                            **self._provenance_for_file(agenda_file)
                        },
                        source=f"taxonomy_{agenda_file.stem}"
                    )
                    # Link Document → Section
                    self._create_relationship(
                        'hasSection',
                        doc_entity_id,
                        section_entity_id,
                        {'section_order': section.get('section_order', 0)}
                    )
                    
                    # Process items in section
                    items = section.get('items', [])
                    log.info(f"      Found {len(items)} items in section")
                    
                    for item in items:
                        item_code = item.get('item_code', '')
                        log.info(f"      Processing agenda item: {item_code}")
                        
                        # Create AgendaItem entity with canonical ID
                        item_title = item.get('title', '')
                        code_clean = _clean_agenda_code(item_code)  # "E4"
                        # canonical upfront: agenda_item_<CODE>_<YYYY_MM_DD>
                        ymd = self._date_to_yyyy_mm_dd(meeting_date)  # '2024_01_09'
                        agenda_item_id = f"agenda_item_{code_clean}_{ymd}"
                        
                        # Extract URLs and document reference
                        urls = item.get('urls', [])
                        document_reference = item.get('document_reference', '')
                        primary_url = urls[0].get('url') if urls and len(urls) > 0 else None
                        
                        # Create entity manually with our custom ID
                        agenda_entity = {
                            'type': 'AgendaItem',
                            'id': agenda_item_id,
                            'agendaItemID': agenda_item_id,        # make standards happy downstream
                            'itemID': item_code,
                            'code': item_code,                     # keep original "E-4" for display
                            'title': item_title,
                            'documentReference': document_reference,
                            'url': primary_url,
                            'meetingDate': meeting_date,          # helps dedup & linking
                            'subtype': item.get('type', ''),
                            'presenter': item.get('presenter'),
                            'estimatedDuration': item.get('estimatedDuration'),
                            'Source_File_Name': agenda_file.name,
                            'Source_File_Path': str(agenda_file),
                            '_source': f"taxonomy_{agenda_file.stem}",
                            '_created_at': datetime.now().isoformat()
                        }
                        
                        # Store entity directly
                        if 'AgendaItem' not in self.created_entities:
                            self.created_entities['AgendaItem'] = {}
                        self.created_entities['AgendaItem'][agenda_item_id] = agenda_entity
                        log.info(f"      Created AgendaItem: {agenda_item_id}")
                        
                        # Link agenda item to its agenda document (AgendaItem -> Document)
                        self._create_relationship(
                            'isPartOf',
                            agenda_item_id,
                            doc_entity_id,
                            {}
                        )

                        # Link AgendaItem ↔ Section (canonical)
                        # 'inSection' is not defined in the ontology; emit only the canonical edge:
                        self._create_relationship('hasAgendaItem', section_entity_id, agenda_item_id, {})
                        
                        # Link event to agenda item
                        self._create_relationship(
                            'discusses',
                            meeting_id,
                            agenda_item_id,
                            {'order': item.get('item_order', 0)}
                        )
            
            # Process entities (ordinances, resolutions, etc.)
            for entity in data.get('entities', []):
                entity_type = entity.get('type', '')
                
                if entity_type in ['ORDINANCE', 'RESOLUTION']:
                    # Create Policy entity
                    policy_id = self._create_entity(
                        'Policy',
                        {
                            'title': entity.get('name', ''),
                            'status': 'Proposed',
                            'effectiveDate': meeting_date,
                            'meetingDate': meeting_date,
                            'legalReferences': []
                        },
                        source=f"taxonomy_{agenda_file.stem}"
                    )
                    
                    # Connect Policy to the Meeting Event
                    self._create_relationship('discusses', meeting_id, policy_id, {
                        'context': f'{entity_type.lower()} discussed in meeting'
                    })
                    self._create_relationship('discussedIn', policy_id, meeting_id, {
                        'meeting_date': meeting_date
                    })
                    
                    # Process vote details
                    vote_details = entity.get('vote_details', {})
                    if vote_details:
                        # Create VoteOutcome entity
                        outcome_id = self._create_entity(
                            'VoteOutcome',
                            {
                                'agendaItemID': entity.get('related_item', ''),
                                'status': 'passed' if vote_details.get('yeas', 0) > vote_details.get('nays', 0) else 'failed',
                                'yesVotes': vote_details.get('yeas', 0),
                                'noVotes': vote_details.get('nays', 0),
                                'abstentions': 0,
                                'voteDetails': []
                            },
                            source=f"taxonomy_{agenda_file.stem}"
                        )
                        
                        # Link outcome to policy
                        self._create_relationship(
                            'votedOn',
                            outcome_id,
                            policy_id,
                            {}
                        )
                    
                    # Process motion details
                    motion = entity.get('motion', {})
                    if motion.get('moved_by'):
                        # Create Person entity for mover
                        person_id = self._create_entity(
                            'Person',
                            {
                                'name': motion['moved_by'],
                                'title': 'Commissioner',
                                'affiliation': 'City Council',
                                'contactInfo': None
                            },
                            source=f"taxonomy_{agenda_file.stem}"
                        )
                        
                        # Create sponsors relationship
                        self._create_relationship(
                            'sponsors',
                            person_id,
                            policy_id,
                            {'sponsorshipType': 'primary'}
                        )
                    
                    if motion.get('seconded_by'):
                        # Create Person entity for seconder
                        person_id = self._create_entity(
                            'Person',
                            {
                                'name': motion['seconded_by'],
                                'title': 'Commissioner',
                                'affiliation': 'City Council',
                                'contactInfo': None
                            },
                            source=f"taxonomy_{agenda_file.stem}"
                        )
                        
                        # Create sponsors relationship
                        self._create_relationship(
                            'sponsors',
                            person_id,
                            policy_id,
                            {'sponsorshipType': 'secondary'}
                        )
            
        except Exception as e:
            log.error(f"Error processing agenda file {agenda_file}: {e}")
    
    async def _process_legal_file(self, legal_file: Path) -> None:
        """Process an enhanced legal document JSON file."""
        try:
            with open(legal_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract original PDF metadata from JSON data
            original_pdf_name = data.get('Source_File_Name', legal_file.name.replace('_enhanced_ordinance.json', '.pdf').replace('_enhanced_resolution.json', '.pdf'))
            original_pdf_path = data.get('Source_File_Path', str(legal_file))
            
            # Set current source to the original PDF info for provenance tracking
            self.current_source_file = None  # Clear to use explicit metadata
            
            doc_type = data.get('document_type', 'ordinance')
            doc_number = data.get('document_number', legal_file.stem)
            
            # Determine if this is ordinance or resolution and extract number
            title_text = data.get('full_title', '') or data.get('title', '') or legal_file.name
            file_path_str = str(legal_file).lower()
            doc_kind = None
            doc_number_extracted = None
            
            # Try to extract ordinance or resolution number
            # Check file path first (most reliable indicator) - look for folder names
            if '/resolutions/' in file_path_str or '\\resolutions\\' in file_path_str:
                doc_kind = 'resolution'
            elif '/ordinances/' in file_path_str or '\\ordinances\\' in file_path_str:
                doc_kind = 'ordinance'
            # Then check explicit document_type field
            elif doc_type.lower() == 'resolution' or 'resolution' in title_text.lower():
                doc_kind = 'resolution'
            elif doc_type.lower() == 'ordinance' or 'ordinance' in title_text.lower():
                doc_kind = 'ordinance'
            else:
                # Default to ordinance if unclear
                doc_kind = 'ordinance'
                
            # Extract document number
            doc_number_extracted = doc_number
            if not doc_number_extracted:
                match = re.search(r'\b(\d{4}-\d{1,3})\b', title_text)
                if match:
                    doc_number_extracted = match.group(1)
                else:
                    doc_number_extracted = legal_file.stem  # fallback
            
            # Create Document entity first
            # For ordinances/resolutions, create proper Document ID (not Policy ID)
            if doc_kind in ('ordinance', 'resolution') and doc_number_extracted and '-' in doc_number_extracted:
                year, ordinal = (doc_number_extracted.split('-', 1) + ['0'])[:2]
                # Create Document ID with "document_" prefix (different from Policy ID)
                doc_id_override = f"document_{doc_kind}_{year}_{ordinal.zfill(2)}"
                doc_attrs = {
                    'documentID': doc_id_override,  # Explicitly set the ID
                    'title': data.get('full_title') or f"{doc_type} {doc_number}",
                    'documentType': doc_type,
                    'status': 'Final',
                    'issueDate': data.get('adoption_date'),
                    'meetingDate': data.get('adoption_date') or data.get('meeting_date'),
                    'sourceURL': None
                }
            else:
                doc_attrs = {
                    'title': data.get('full_title') or f"{doc_type} {doc_number}",
                    'documentType': doc_type,
                    'status': 'Final',
                    'issueDate': data.get('adoption_date'),
                    'meetingDate': data.get('adoption_date') or data.get('meeting_date'),
                    'sourceURL': None
                }
            
            # Add original PDF metadata to document attributes
            doc_attrs['Source_File_Name'] = original_pdf_name
            doc_attrs['Source_File_Path'] = original_pdf_path
            
            doc_id = self._create_entity(
                'Document',
                doc_attrs,
                source=f"taxonomy_{legal_file.stem}"
            )
            
            # Create Policy entity using unified ID generator
            if doc_kind in ('ordinance','resolution'):
                year, ordinal = (doc_number_extracted.split('-', 1) + ['0'])[:2]
                policy_id = make_policy_id(doc_kind, year, ordinal, legal_file.name)
                policy_title = f"{doc_kind.capitalize()} {doc_number_extracted}"
                status = data.get('status', 'enacted' if doc_kind=='ordinance' else 'adopted')
            else:
                # Fallback for unclear document types
                policy_id = make_policy_id('ordinance', '0000', '0', legal_file.name)
                policy_title = f"Ordinance {doc_number_extracted}"
                status = data.get('status', 'enacted')
            
            policy_entity = {
                'type': 'Policy',
                'id': policy_id,
                'policyID': policy_id,
                'title': policy_title,                               # human-friendly label
                'status': status,                                    # enacted for ordinances, adopted for resolutions
                'policyType': doc_kind,
                'meetingDate': data.get('adoption_date') or data.get('meeting_date'),
                'effectiveDate': data.get('effective_date'),
                'expirationDate': data.get('expiration_date'),
                'legalReferences': data.get('references', []),
                'Source_File_Name': original_pdf_name,
                'Source_File_Path': original_pdf_path,
                '_sources': [f"taxonomy_{legal_file.stem}"],
                '_created_at': datetime.now().isoformat()
            }
            
            # Add ordinanceNumber or resolutionNumber field
            if doc_kind == 'ordinance':
                policy_entity['ordinanceNumber'] = doc_number_extracted
            else:
                policy_entity['resolutionNumber'] = doc_number_extracted
            
            # Store entity directly
            if 'Policy' not in self.created_entities:
                self.created_entities['Policy'] = {}
            self.created_entities['Policy'][policy_id] = policy_entity
            
            # Wire Policy to its Document (text) using correct ontology relationship
            self._create_relationship(
                'implementedBy',   # Policy is implementedBy its Document representation
                policy_id,
                doc_id,
                {}
            )
            
            # Link to meeting by adoption date
            meeting_date = data.get('adoption_date') or data.get('meeting_date')
            event_id = self._find_event_by_date(meeting_date)
            if event_id:
                # Use ontology-compliant relationships
                self._create_relationship('discusses', event_id, policy_id, {'context': f'{doc_type} discussed in meeting'})
                self._create_relationship('discussedIn', policy_id, event_id, {'meeting_date': meeting_date})
                log.info(f"   🏛️ Connected {doc_type} {policy_id} to Event {event_id}")
            else:
                log.warning(f"   ⚠️ Could not find Event for date {meeting_date} to link {doc_type} {policy_id}")
            
            # Link to agenda item if we have the item code
            item_code = (data.get('agenda_item_code') or data.get('related_item') or
                        data.get('agenda_item') or data.get('related_item_code'))
            
            if item_code and meeting_date:
                agenda_item_id = self._find_agenda_item_id(item_code, meeting_date)
                if agenda_item_id:
                    # Use correct ontology relationships
                    self._create_relationship('mentionedIn', policy_id, doc_id, {})
                    
                    # Wire AgendaItem implements Policy (correct direction per ontology)
                    self._create_relationship(
                        'implements',
                        agenda_item_id,
                        policy_id,
                        {'agendaCode': item_code}
                    )
            
            # Wire Policy directly to the Event (one-hop meeting link) - already handled above
            
            # Process sponsors
            for sponsor in data.get('sponsors', []):
                person_id = self._create_entity(
                    'Person',
                    {
                        'name': sponsor.get('name', ''),
                        'title': sponsor.get('title', 'Commissioner'),
                        'affiliation': 'City Council',
                        'contactInfo': None,
                        'Source_File_Name': original_pdf_name,
                        'Source_File_Path': original_pdf_path
                    },
                    source=f"taxonomy_{legal_file.stem}"
                )
                
                self._create_relationship(
                    'sponsors',
                    person_id,
                    policy_id,
                    {'sponsorshipType': 'primary'}
                )
            
            # --- VOTE OUTCOME CREATION ---
            # Extract and create VoteOutcome entities from legal_metadata
            legal_metadata = data.get('legal_metadata', {})
            vote_details = legal_metadata.get('vote_details', {})
            

            
            # Create VoteOutcome only if meaningful voting data exists
            if vote_details and (vote_details.get('yeas') or vote_details.get('nays')):
                meeting_date_for_vote = meeting_date or data.get('meeting_date', '')
                
                # Use agenda_item_id if available, otherwise create a fallback ID
                target_agenda_item_id = None
                if item_code and meeting_date:
                    target_agenda_item_id = self._find_agenda_item_id(item_code, meeting_date)
                    if not target_agenda_item_id:
                        # Create fallback agenda item ID if not found
                        target_agenda_item_id = f"agendaitem_{item_code}_{meeting_date.replace('.', '_')}"
                else:
                    # Create generic agenda item ID if no item code available
                    target_agenda_item_id = f"agendaitem_unknown_{meeting_date_for_vote.replace('.', '_')}"
                

                
                vote_outcome_v, vote_edges = _create_vote_outcome_entities(
                    legal_metadata, target_agenda_item_id, doc_id, policy_id, meeting_date_for_vote
                )
                if vote_outcome_v:
                    outcome_id = vote_outcome_v['outcomeID']
                    
                    # Store the VoteOutcome entity
                    if 'VoteOutcome' not in self.created_entities:
                        self.created_entities['VoteOutcome'] = {}
                    
                    self.created_entities['VoteOutcome'][outcome_id] = vote_outcome_v
                    
                    # Create the relationships
                    for edge in vote_edges:
                        self._create_relationship(edge['label'], edge['from'], edge['to'], {})
                    
                    log.info(f"   ✅ Created VoteOutcome: {outcome_id} ({vote_outcome_v['status']})")
                else:
                    log.warning(f"      ❌ vote_outcome_v is None")
            else:
                log.warning(f"      ❌ No valid vote_details found")
                if not vote_details:
                    log.warning(f"         vote_details is empty")
                else:
                    log.warning(f"         yeas: '{vote_details.get('yeas', '')}', nays: '{vote_details.get('nays', '')}'")
            
        except Exception as e:
            log.error(f"Error processing legal file {legal_file}: {e}")
    
    def _create_entity(self, entity_type: str, attributes: Dict[str, Any], source: str) -> str:
        """
        Validate + assign canonical ID, then persist into registry.
        """
        # Ensure canonical Policy IDs ("policy_*"), not "document_*"
        if entity_type == "Policy":
            preferred = self._preferred_policy_id(attributes)
            if preferred:
                attributes["policyID"] = preferred
                attributes["id"] = preferred
        # --- NEW: protect domain "type" and normalize to canonical attribute names ---
        attrs = dict(attributes)  # avoid mutating caller
        if 'type' in attrs and attrs['type'] and attrs['type'] != entity_type:
            if entity_type == 'Document':
                attrs.setdefault('documentType', attrs['type'])
            else:
                attrs.setdefault('subtype', attrs['type'])
            del attrs['type']
        # Back-compat aliases → canonical
        if entity_type == 'Document':
            if 'document_type' in attrs and 'documentType' not in attrs:
                attrs['documentType'] = attrs.pop('document_type')
            if 'Document_Type' in attrs and 'documentType' not in attrs:
                attrs['documentType'] = attrs.pop('Document_Type')
            if 'meeting_date' in attrs and 'meetingDate' not in attrs:
                attrs['meetingDate'] = attrs.pop('meeting_date')
        else:
            if 'meeting_date' in attrs and 'meetingDate' not in attrs:
                attrs['meetingDate'] = attrs.pop('meeting_date')

        # --- NEW: ensure minimal provenance exists even if caller didn't pass it ---
        # Prioritize original PDF metadata if it exists in the attributes, otherwise use current file
        if not attrs.get('Source_File_Name') or not attrs.get('Source_File_Path'):
            if self.current_source_file and isinstance(self.current_source_file, Path):
                attrs.setdefault('Source_File_Name', self.current_source_file.name)
                attrs.setdefault('Source_File_Path', str(self.current_source_file))
            else:
                attrs.setdefault('Source_File_Name', source if isinstance(source, str) else None)
                attrs.setdefault('Source_File_Path', f"taxonomy://{source}" if isinstance(source, str) else None)

        # Create entity
        entity = self.toolkit.create_entity(entity_type, attrs, source)
        entity = EntityIDStandards.normalize_entity_id_fields(dict(entity), entity_type)
        # Keep ontology class without clobbering domain "type"
        
        # Clean up redundant date fields
        entity = self._cleanup_redundant_date_fields(entity, entity_type)
        
        # Add extraction metadata for consistency with NER entities
        if 'extraction_chunk_id' not in entity:
            entity['extraction_chunk_id'] = 'taxonomy'  # Consistent metadata
        if 'extracted_at' not in entity:
            entity['extracted_at'] = datetime.now().isoformat()

        # --- NEW: pad attributes from ontology ---
        try:
            ensure_min_entity_props(entity, entity_type)
        except Exception:
            pass
        
        # Get canonical ID field from standards
        id_key = EntityIDStandards.get_id_field(entity_type)
        id_field = entity.get(id_key) or entity.get('id')
        
        if not id_field:
            # Generate ID if missing
            entity_id = self.toolkit.generate_entity_id(entity_type, attributes)
            entity[id_key] = entity_id
        else:
            entity_id = id_field
        
        # Store entity
        if entity_type not in self.created_entities:
            self.created_entities[entity_type] = {}
        
        # Merge if entity already exists
        if entity_id in self.created_entities[entity_type]:
            existing = self.created_entities[entity_type][entity_id]
            entity = self.toolkit.merge_entities(existing, entity)
        
        self.created_entities[entity_type][entity_id] = entity
        
        return entity_id
    
    def _create_relationship(self, rel_type: str, source_id: str, 
                           target_id: str, attributes: Dict) -> None:
        """Create a relationship using the toolkit, only if ontology-valid."""
        # Normalize label
        canonical = normalize_rel_label((rel_type or "").strip())
        if not canonical:
            log.warning("Skipping relationship with empty type between %s -> %s", source_id, target_id)
            return
        # Validate endpoints against entities created so far
        src_t = self._type_of(source_id)
        tgt_t = self._type_of(target_id)
        if not src_t or not tgt_t:
            log.warning("Skipping relationship %s: unknown endpoint types src=%s(%s) tgt=%s(%s)",
                        canonical, source_id, src_t, target_id, tgt_t)
            return
        if not self._rel_allowed(canonical, src_t, tgt_t):
            log.warning("Skipping non-ontology relationship %s: %s → %s", canonical, src_t, tgt_t)
            return
        rel = self.toolkit.create_relationship(canonical, source_id, target_id, attributes, source="taxonomy")
        self.created_relationships.append(rel)
    
    def _find_existing_document_id(self, meeting_date: str, doc_type: str) -> Optional[str]:
        """
        Find existing document ID for reuse instead of creating duplicate.
        
        Args:
            meeting_date: Meeting date (e.g., "01.09.2024")
            doc_type: Document type (e.g., "agenda")
            
        Returns:
            Existing document ID or None if not found
        """
        def _canon_date(s: str) -> str:
            if not s: return ""
            s = s.strip()
            fmts = ("%m.%d.%Y", "%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%B %d, %Y", "%b %d, %Y")
            for fmt in fmts:
                try:
                    from datetime import datetime
                    return datetime.strptime(s, fmt).strftime("%Y%m%d")
                except Exception:
                    pass
            # last resort: digits only
            return re.sub(r"\D", "", s)

        target = _canon_date(meeting_date)
        # First, check if merged documents exist (post-deduplication)
        merged_docs_file = self.output_dir / "merged" / "entities" / "Document.json"
        if merged_docs_file.exists():
            try:
                with open(merged_docs_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                for entity in data.get('entities', []):
                    title = entity.get('title', '').lower()
                    name = entity.get('name', '').lower()
                    entity_type = (entity.get('documentType') or entity.get('document_type') or '').lower()
                    
                    # Look for date patterns in title/name
                    date_normalized = target
                    
                    if (doc_type.lower() in title or doc_type.lower() in name or 
                        doc_type.lower() == entity_type):
                        # Check if date matches
                        t_norm = re.sub(r"\D", "", title)
                        n_norm = re.sub(r"\D", "", name)
                        if (date_normalized and (date_normalized in t_norm or date_normalized in n_norm)):
                            document_id = entity.get('documentID')
                            if document_id:
                                log.info(f"   Found existing merged document ID: {document_id}")
                                return document_id
                                
            except Exception as e:
                log.warning(f"Error reading merged documents file: {e}")
        
        # Fallback: Look in the raw NER extracted documents
        doc_dir = self.output_dir / "Document"
        if not doc_dir.exists():
            return None
        
        for doc_file in doc_dir.glob("*.json"):
            try:
                with open(doc_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                for entity in data.get('entities', []):
                    title = entity.get('title', '').lower()
                    name = entity.get('name', '').lower()
                    entity_type = (entity.get('documentType') or entity.get('document_type') or '').lower()
                    
                    date_normalized = target
                    
                    if (doc_type.lower() in title or doc_type.lower() in name or 
                        doc_type.lower() == entity_type):
                        t_norm = re.sub(r"\D", "", title)
                        n_norm = re.sub(r"\D", "", name)
                        if (date_normalized and (date_normalized in t_norm or date_normalized in n_norm)):
                            document_id = entity.get('documentID')
                            if document_id:
                                log.info(f"   Found existing NER document ID: {document_id}")
                                return document_id
                                
            except Exception as e:
                log.warning(f"Error reading document file {doc_file}: {e}")
                continue
        
        return None
    
    async def _save_all_entities(self) -> None:
        """Save all created entities and relationships to files."""
        # Save entities by type
        for entity_type, entities in self.created_entities.items():
            if not entities:
                continue
            
            # Convert to list format matching NER output
            entity_list = list(entities.values())
            
            # Save to file in NER format with timestamp for incremental runs
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"taxonomy_synthesis_{timestamp}.json"
            filepath = self.registry_dir / entity_type / filename
            
            file_data = {
                "chunk_id": "taxonomy",
                "document": "taxonomy_synthesis",
                "source_file": "multiple",
                "entity_type": entity_type,
                "entities": entity_list,
                "_metadata": {
                    "synthesis_timestamp": datetime.now().isoformat(),
                    "entity_count": len(entity_list)
                }
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(file_data, f, indent=2, ensure_ascii=False)
        
        # Save relationships
        if self.created_relationships:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"taxonomy_synthesis_{timestamp}.json"
            filepath = self.registry_dir / "relationships" / filename
            
            file_data = {
                "chunk_id": "taxonomy",
                "document": "taxonomy_synthesis",
                "source_file": "multiple",
                "relationships": self.created_relationships,
                "_metadata": {
                    "synthesis_timestamp": datetime.now().isoformat(),
                    "relationship_count": len(self.created_relationships)
                }
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(file_data, f, indent=2, ensure_ascii=False)
    
    async def create_seed_entities(self) -> None:
        """
        Create minimal seed entities to ensure taxonomy exists.
        This guarantees core entities even if extraction is empty.
        """
        log.info("🌱 Creating seed entities")
        
        # Core organization
        city_id = self._create_entity(
            'Organization',
            {
                'name': 'City of Coral Gables',
                'type': 'Municipality',
                'jurisdiction': 'Coral Gables',
                'address': '405 Biltmore Way, Coral Gables, FL 33134'
            },
            source="seed"
        )
        
        # Core roles
        for role_title in ['Mayor', 'Vice Mayor', 'Commissioner', 'City Manager', 'City Attorney', 'City Clerk']:
            role_id = self._create_entity(
                'Role',
                {
                    'title': role_title,
                    'startDate': None,
                    'endDate': None
                },
                source="seed"
            )
        
        # Core location
        city_hall_id = self._create_entity(
            'Location',
            {
                'name': 'City Hall',
                'type': 'Building',
                'address': '405 Biltmore Way, Coral Gables, FL 33134',
                'coordinates': None
            },
            source="seed"
        )
        
        # Link organization to location
        self._create_relationship(
            'isLocatedAt',
            city_id,
            city_hall_id,
            {}
        )
        
        # Core topics
        for topic_name in ['Budget', 'Zoning', 'Public Safety', 'Infrastructure', 'Parks and Recreation']:
            topic_id = self._create_entity(
                'Topic',
                {
                    'name': topic_name,
                    'category': 'Governance',
                    'description': f"City governance topic: {topic_name}"
                },
                source="seed"
            )
        
        # Save seed entities
        await self._save_all_entities()
        
        log.info("✅ Seed entities created")
    
    async def _process_verbatim_file(self, verbatim_file: Path) -> None:
        """Process a verbatim transcript JSON file."""
        try:
            with open(verbatim_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract original PDF metadata from JSON data
            original_pdf_name = data.get('Source_File_Name', verbatim_file.name.replace('.json', '.pdf'))
            original_pdf_path = data.get('Source_File_Path', str(verbatim_file))
            
            # Set current source to None to use explicit metadata
            self.current_source_file = None
            
            meeting_date = data.get('meeting_date', '')
            doc_type = data.get('document_type', 'verbatim_transcript')
            
            # Extract item codes for linking
            source_hint = original_pdf_name
            codes = (
                data.get('item_codes') or data.get('agenda_item_codes')
                or [c for c in self._extract_item_codes_from_text(source_hint)]
            )
            
            title_suffix = f" - {', '.join(codes)}" if codes else ''
            
            # Create Document entity
            doc_id = self._create_entity(
                'Document',
                {
                    'title': f"Verbatim Transcript {meeting_date}{title_suffix}",
                    'documentType': doc_type,
                    'status': 'Final',
                    'issueDate': meeting_date,
                    'meetingDate': meeting_date,
                    'sourceURL': None,
                    'Source_File_Name': original_pdf_name,
                    'Source_File_Path': original_pdf_path
                },
                source=f"taxonomy_{verbatim_file.stem}"
            )
            
            # Link to meeting using ontology-compliant relationships
            event_id = self._find_event_by_date(meeting_date)
            if event_id:
                self._create_relationship('hasTranscript', event_id, doc_id, {'kind': 'verbatim'})
                self._create_relationship('discussedIn', doc_id, event_id, {'meeting_date': meeting_date})
            
            # Handle different types of verbatim transcripts
            section_codes = data.get('section_codes', [])
            transcript_type = data.get('transcript_type', '')
            
            # Check if this is a public comment verbatim transcript
            is_public_comment_verbatim = (
                'PUBLIC_COMMENT' in section_codes or 
                transcript_type == 'public_comment' or
                (self._is_public_comment_verbatim_by_name(original_pdf_name) and not codes)
            )
            
            if is_public_comment_verbatim:
                # Link to public comment section
                section_id = self._find_public_comment_section_id(meeting_date)
                if section_id:
                    self._create_relationship('hasTranscript', section_id, doc_id, {'kind': 'verbatim'})
            else:
                # Link to agenda items if we have codes using ontology-compliant relationships
                for code in codes:
                    agenda_item_id = self._find_agenda_item_id(code, meeting_date)
                    if agenda_item_id:
                        self._create_relationship('hasTranscript', agenda_item_id, doc_id, {'kind': 'verbatim'})
            
            # Link to agenda document for the same meeting date
            agenda_doc_id = self._find_existing_document_id(meeting_date, 'agenda')
            if agenda_doc_id:
                self._create_relationship('references', agenda_doc_id, doc_id, {'context': 'verbatim transcript of agenda items'})
            
        except Exception as e:
            log.error(f"Error processing verbatim file {verbatim_file}: {e}")
    
    def _digits_date(self, s: str) -> str:
        """Extract only digits from a date string."""
        if not s: 
            return ""
        return ''.join(ch for ch in s if ch.isdigit())

    def _canon_yyyymmdd(self, s: str) -> str:
        """Normalize many common date formats to YYYYMMDD."""
        if not s:
            return ""
        t = self._date_to_yyyy_mm_dd(s)  # e.g. '2024_01_09'
        return (t or "").replace("_", "")
    
    def _normalize_item_code(self, code: str) -> str:
        """Normalize item code by removing separators and converting to uppercase."""
        # "E-1" -> "E1", ignore case
        return (code or '').replace('-', '').replace('_', '').strip().upper()
    
    def _find_event_by_date(self, date_str: str) -> Optional[str]:
        """Find an Event entity by matching date."""
        if not date_str:
            return None
        target = self._canon_yyyymmdd(date_str)
        bucket = self.created_entities.get('Event', {})
        
        log.debug(f"   🔍 Looking for Event with date {date_str} (normalized: {target})")
        log.debug(f"   📋 Available Events: {list(bucket.keys())}")
        
        for eid, e in bucket.items():
            d = e.get('dateTime') or e.get('meetingDate') or e.get('meeting_date') or e.get('issueDate')
            if d:
                d_norm = self._canon_yyyymmdd(d)
                log.debug(f"   📅 Event {eid}: date={d} -> normalized={d_norm}")
                if d_norm == target:
                    log.info(f"   ✅ Found matching Event: {eid}")
                    return eid
        
        log.warning(f"   ❌ No Event found for date {date_str} (target: {target})")
        return None
    
    def _find_agenda_item_id(self, item_code: str, meeting_date: str) -> Optional[str]:
        """Find an AgendaItem entity by code and meeting date with cross-source search."""
        if not item_code or not meeting_date: 
            return None
            
        # Strategy 1: Search in taxonomy created entities (current logic)
        result = self._find_agenda_item_in_created_entities(item_code, meeting_date)
        if result:
            return result
            
        # Strategy 2: Search in merged entities (cross-source)
        result = self._find_agenda_item_in_merged_entities(item_code, meeting_date)
        if result:
            return result
            
        return None
    
    def _find_agenda_item_in_created_entities(self, item_code: str, meeting_date: str) -> Optional[str]:
        """Find agenda item in taxonomy created entities (original logic)."""
        code = self._normalize_item_code(item_code)
        date = self._canon_yyyymmdd(meeting_date)
        bucket = self.created_entities.get('AgendaItem', {})
        for aid, a in bucket.items():
            a_code = self._normalize_item_code(a.get('itemID', ''))
            a_date = self._canon_yyyymmdd(a.get('meetingDate', '') or a.get('meeting_date', '') or a.get('date', ''))
            if a_code == code and (not date or a_date == date):
                return aid
        return None
    
    def _find_agenda_item_in_merged_entities(self, item_code: str, meeting_date: str) -> Optional[str]:
        """Find agenda item in merged entities from all sources (NER + taxonomy)."""
        try:
            merged_entities_file = self.output_dir / 'merged' / 'entities' / 'AgendaItem.json'
            if not merged_entities_file.exists():
                return None
                
            import json
            with open(merged_entities_file, 'r') as f:
                data = json.load(f)
            
            entities = data.get('entities', [])
            code = self._normalize_item_code(item_code)
            date = self._canon_yyyymmdd(meeting_date)
            
            for entity in entities:
                # Try multiple ID field variations
                entity_code = (entity.get('itemID') or entity.get('code') or 
                             entity.get('agendaItemCode') or '')
                entity_date = (entity.get('meetingDate') or entity.get('meeting_date') or 
                             entity.get('date') or '')
                
                a_code = self._normalize_item_code(entity_code)
                a_date = self._canon_yyyymmdd(entity_date)
                
                if a_code == code and (not date or a_date == date):
                    return entity.get('agendaItemID') or entity.get('id')
                    
        except Exception as e:
            log.warning(f"Error searching merged entities for agenda item {item_code}: {e}")
            
        return None
    
    def _is_public_comment_verbatim_by_name(self, filename: str) -> bool:
        """Check if filename indicates a public comment verbatim transcript."""
        if not filename:
            return False
        
        filename_lower = filename.lower()
        
        # Check for verbatim + public comment patterns
        has_verbatim = 'verbatim' in filename_lower or 'transcript' in filename_lower
        has_public_comment = 'public' in filename_lower and 'comment' in filename_lower
        
        return has_verbatim and has_public_comment
    
    def _find_public_comment_section_id(self, meeting_date: str) -> Optional[str]:
        """Find public comment section for given meeting date using multi-strategy lookup."""
        # Strategy 1: Search in taxonomy created entities (fast)
        section_id = self._find_public_comment_section_in_created_entities(meeting_date)
        if section_id:
            return section_id
        
        # Strategy 2: Search in merged entities manifest (cross-source)
        return self._find_public_comment_section_in_merged_entities(meeting_date)
    
    def _find_public_comment_section_in_created_entities(self, meeting_date: str) -> Optional[str]:
        """Find public comment section in taxonomy-created entities."""
        norm_date = self._canon_yyyymmdd(meeting_date)
        
        for entity_id, entity in self.created_entities.items():
            if entity.get('type') == 'Section':
                entity_date = entity.get('meetingDate', '')
                section_type = entity.get('sectionType', '')
                section_name = entity.get('name', '') or ''
                
                if (self._canon_yyyymmdd(entity_date) == norm_date and 
                    (section_type == 'PUBLIC_COMMENT' or 
                     section_type == 'public_comments' or
                     ('public' in section_name.lower() and 'comment' in section_name.lower()))):
                    return entity_id
        
        return None
    
    def _find_public_comment_section_in_merged_entities(self, meeting_date: str) -> Optional[str]:
        """Find public comment section in merged entities manifest."""
        try:
            merged_entities_dir = Path(self.output_dir) / 'merged' / 'entities'
            section_file = merged_entities_dir / 'Section.json'
            
            if section_file.exists():
                with open(section_file, 'r') as f:
                    data = json.load(f)
                
                entities = data.get('entities', [])
                norm_date = self._canon_yyyymmdd(meeting_date)
                
                for entity in entities:
                    entity_date = entity.get('meetingDate', '')
                    section_type = entity.get('sectionType', '')
                    section_name = entity.get('name', '') or ''
                    entity_id = entity.get('sectionID') or entity.get('id')
                    
                    if (self._canon_yyyymmdd(entity_date) == norm_date and 
                        entity_id and
                        (section_type == 'PUBLIC_COMMENT' or 
                         section_type == 'public_comments' or
                         ('public' in section_name.lower() and 'comment' in section_name.lower()))):
                        return entity_id
            
            return None
            
        except Exception as e:
            log.error(f"Error searching merged entities for public comment section {meeting_date}: {e}")
            return None
    
    def _extract_item_codes_from_text(self, text: str) -> List[str]:
        """Extract item codes like E-1, F-2, etc. from text."""
        import re
        return re.findall(r'[A-Z]-?\d+', text or '')
    
    def _create_relationship(self, rel_type: str, source_id: str, 
                           target_id: str, attributes: Dict) -> None:
        """Create a relationship between two entities with validation and auto-stubbing."""
        if not rel_type or not source_id or not target_id:
            return
        
        src_t = self._type_of(source_id)
        tgt_t = self._type_of(target_id)
        # Auto-stub well-known ID prefixes to avoid drops in cross-run linking
        def _guess_and_stub(eid: str, etype: Optional[str]) -> Optional[str]:
            if etype:
                return etype
            prefix = (eid or "").split("_", 1)[0]
            guess = {
                "document": "Document",
                "policy": "Policy", 
                "agendaitem": "AgendaItem",  # More specific to avoid conflicts
                "agenda_item": "AgendaItem",  # Handle both formats
                "event": "Event",
                "person": "Person",
                "org": "Organization",
                "location": "Location",
                "section": "Section",
                "outcome": "VoteOutcome",  # Add VoteOutcome support
            }.get(prefix)
            if guess:
                self._ensure_entity_stub(guess, eid)
            return guess
        src_t = _guess_and_stub(source_id, src_t)
        tgt_t = _guess_and_stub(target_id, tgt_t)
        
        # Validate relationship
        if not self._rel_allowed(rel_type, src_t, tgt_t):
            log.warning(f"Skipping relationship {rel_type}: {src_t} → {tgt_t} (unknown endpoint types or not allowed)")
            return
        
        # Create the relationship
        # Use proper edge ID generation from GraphEntityToolkit to avoid collisions
        edge_id = self.toolkit.generate_edge_id(source_id, rel_type, target_id, attributes)
        
        relationship = {
            'type': rel_type,
            'source': source_id,
            'target': target_id,
            'attributes': attributes or {},
            '_source': 'taxonomy',
            '_created_at': datetime.now().isoformat(),
            '_edge_id': edge_id
        }
        
        self.created_relationships.append(relationship)


def _create_vote_outcome_entities(legal_metadata, agenda_item_id, document_id, policy_id, meeting_date):
    """Create VoteOutcome entities from legal_metadata without hardcoding values."""
    
    vote_details = legal_metadata.get('vote_details', {})
    if not vote_details:
        return None, []
    
    # Generate unique vote outcome ID using document ID to avoid collisions
    # Use document_id to ensure uniqueness since each document should have one VoteOutcome
    if document_id.startswith('document_'):
        # Extract meaningful part from document_id like "document_ordinance_2024_01" 
        doc_suffix = document_id.replace('document_', '')
        outcome_id = f"outcome_{doc_suffix}"
    else:
        # Fallback with agenda item info
        if '_' in agenda_item_id:
            id_parts = agenda_item_id.split('_')
            agenda_code = id_parts[1] if len(id_parts) > 1 else 'unknown'
        else:
            agenda_code = 'unknown'
        
        date_formatted = meeting_date.replace('.', '-')
        outcome_id = f"outcome_{agenda_code}_{date_formatted}"
    
    # Extract vote counts dynamically
    yeas_list = vote_details.get('yeas', '')
    nays_list = vote_details.get('nays', '')
    abstentions_list = vote_details.get('abstentions', '')
    
    # Count votes dynamically with special handling for unanimous descriptions
    if yeas_list and ('unanimous' in yeas_list.lower() or 'voice vote' in yeas_list.lower()):
        # Handle "Unanimous Voice Vote" or "unanimous" descriptions
        yes_count = 5  # Standard number of commissioners in Coral Gables
    elif vote_details.get('unanimous') and yeas_list:
        # Handle cases where unanimous=true but yeas only contains mover/seconder
        name_count = len([name.strip() for name in yeas_list.split(',') if name.strip()])
        if name_count < 5:  # If less than full commission, it's likely unanimous with only mover/seconder listed
            yes_count = 5  # Set to full commission for unanimous votes
        else:
            yes_count = name_count
    else:
        # Count individual commissioner names
        yes_count = len([name.strip() for name in yeas_list.split(',') if name.strip()]) if yeas_list else 0
    
    no_count = len([name.strip() for name in nays_list.split(',') if name.strip()]) if nays_list else 0
    abstention_count = len([name.strip() for name in abstentions_list.split(',') if name.strip()]) if abstentions_list else 0
    
    # Determine status dynamically
    status = legal_metadata.get('outcome_status', 'unknown')
    if 'passed' in status.lower() or vote_details.get('unanimous'):
        result_status = 'passed'
    elif 'failed' in status.lower():
        result_status = 'failed'
    else:
        result_status = 'passed' if yes_count > no_count else 'failed'
    
    # Build VoteOutcome vertex using existing ontology
    vote_vertex = {
        'outcomeID': outcome_id,
        'agendaItemID': agenda_item_id,
        'status': result_status,
        'result': result_status,
        'yesVotes': yes_count,
        'noVotes': no_count,
        'abstentions': abstention_count,
        'voteDetails': vote_details.get('yeas', '') + '; ' + vote_details.get('nays', ''),
        'unanimous': vote_details.get('unanimous', False),
        'entity_type': 'VoteOutcome',
        'partitionKey': 'cgGraph',
        'meeting_date': meeting_date,
        '_sources': [document_id]
    }
    
    # Create relationships using correct ontology semantics
    edges = [
        {'from': outcome_id, 'to': policy_id, 'label': 'votedOn'},  # VoteOutcome votedOn Policy (correct semantic: vote decided policy fate)
        {'from': agenda_item_id, 'to': outcome_id, 'label': 'resultsIn'}  # AgendaItem resultsIn VoteOutcome (reverse direction per ontology)
    ]
    # Note: Removed mentionedIn → Document as it's semantically incorrect
    # VoteOutcome should connect to Policy (what was voted on) not Document (text container)
    
    return vote_vertex, edges
