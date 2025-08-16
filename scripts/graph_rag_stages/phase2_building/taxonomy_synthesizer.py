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
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
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
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:8]

def _clean_agenda_code(code: str) -> str:
    # "E-4" -> "E4"
    return re.sub(r'[^A-Z0-9]', '', (code or '').upper())

def _policy_id_from_ordinance(ordinance_number: str, stable_seed: str) -> str:
    # ordinance_number: "2024-01" -> "2024_01"
    num = (ordinance_number or '').strip().replace('-', '_')
    return f"policy_ordinance_{num}_{_hash8(stable_seed)}"

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
        """Return ontology type for an ID we've created in this synthesizer run."""
        for et, bucket in self.created_entities.items():
            if entity_id in bucket:
                return et
        return None

    def _rel_allowed(self, rel_type: str, src_type: str, tgt_type: str) -> bool:
        """Minimal, explicit mapping for relationships used by this synthesizer."""
        RULES = {
            'hasDocument':      ({'Event','Policy'}, {'Document'}),
            'isAbout':          ({'Document','Policy'}, {'AgendaItem'}),
            'hasSection':       ({'Document'}, {'Section'}),
            'hasAgendaItem':    ({'Section'}, {'AgendaItem'}),
            'discusses':        ({'Event'}, {'AgendaItem'}),
            'adoptedAt':        ({'Policy'}, {'Event'}),
            'enactsPolicy':     ({'Document'}, {'Policy'}),
            'isRecordOf':       ({'Document'}, {'AgendaItem'}),
            # Add relationships that are actually created in the code
            'isPartOf':         ({'AgendaItem'}, {'Document'}),
            'sponsors':         ({'Person'}, {'Policy'}),
            'votedOn':          ({'VoteOutcome'}, {'Policy'}),
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
    
    async def synthesize_from_json(self, json_dir: Path) -> Dict[str, int]:
        """
        Read ontology JSON files and synthesize entities/relationships.
        
        Args:
            json_dir: Directory containing extracted JSON files
            
        Returns:
            Statistics of created entities by type
        """
        if DEBUG_DOCUMENT_FLOW:
            log.info("🔍 DEBUG [TAXONOMY] Starting taxonomy synthesis")
            log.info(f"🔍 DEBUG [TAXONOMY] JSON directory: {json_dir}")
        
        log.info(f"🔄 Synthesizing taxonomy from {json_dir}")
        
        stats = {}
        
        if DEBUG_FILE_DISCOVERY:
            log.info("🔍 DEBUG [TAXONOMY] Beginning file discovery for taxonomy synthesis")
            log.info(f"🔍 DEBUG [TAXONOMY] Directory exists: {json_dir.exists()}")
            if json_dir.exists():
                subdirs = [d.name for d in json_dir.iterdir() if d.is_dir()]
                log.info(f"🔍 DEBUG [TAXONOMY] Available subdirectories: {subdirs}")
        
        # Process agenda files
        agenda_dir = json_dir / "agenda"
        if DEBUG_FILE_DISCOVERY:
            log.info(f"🔍 DEBUG [TAXONOMY] Checking agenda directory: {agenda_dir}")
            log.info(f"🔍 DEBUG [TAXONOMY] Agenda directory exists: {agenda_dir.exists()}")
        
        if agenda_dir.exists():
            agenda_files = list(agenda_dir.glob("agenda_*.json"))
            if DEBUG_DOCUMENT_FLOW:
                log.info(f"🔍 DEBUG [TAXONOMY] Found {len(agenda_files)} agenda files")
                for agenda_file in agenda_files:
                    log.info(f"🔍 DEBUG [TAXONOMY]   Agenda file: {agenda_file.name}")
            log.info(f"Found {len(agenda_files)} agenda files")
            
            for agenda_file in agenda_files:
                await self._process_agenda_file(agenda_file)
        
        # Process legal documents
        legal_dir = json_dir / "legal"
        if DEBUG_FILE_DISCOVERY:
            log.info(f"🔍 DEBUG [TAXONOMY] Checking legal directory: {legal_dir}")
            log.info(f"🔍 DEBUG [TAXONOMY] Legal directory exists: {legal_dir.exists()}")
        
        if legal_dir.exists():
            if DEBUG_FILE_DISCOVERY:
                all_files_in_legal = list(legal_dir.glob("*"))
                log.info(f"🔍 DEBUG [TAXONOMY] All files in legal/: {[f.name for f in all_files_in_legal]}")
            
            legal_files = list(legal_dir.glob("*_enhanced_*.json"))
            if DEBUG_DOCUMENT_FLOW:
                log.info(f"🔍 DEBUG [TAXONOMY] Found {len(legal_files)} legal documents matching pattern '*_enhanced_*.json'")
                for legal_file in legal_files:
                    log.info(f"🔍 DEBUG [TAXONOMY]   Legal file: {legal_file.name}")
            
            # CRITICAL DEBUG: This is where the second major loss occurs
            if len(legal_files) < 5:  # Expected based on the log analysis
                log.warning(f"🚨 CRITICAL: TAXONOMY LEGAL DISCOVERY ISSUE!")
                log.warning(f"🚨   Expected more legal documents but only found: {len(legal_files)}")
                log.warning(f"🚨   Pattern used: '*_enhanced_*.json'")
                log.warning(f"🚨   Directory: {legal_dir}")
                
                if DEBUG_FILE_DISCOVERY:
                    # Try alternative patterns to see what's actually there
                    alt_patterns = ["*.json", "*enhanced*", "*ordinance*", "*resolution*"]
                    for pattern in alt_patterns:
                        alt_files = list(legal_dir.glob(pattern))
                        log.info(f"🔍 DEBUG [TAXONOMY]   Pattern '{pattern}': {len(alt_files)} files")
                        if alt_files:
                            for f in alt_files[:3]:  # Show first 3 examples
                                log.info(f"🔍 DEBUG [TAXONOMY]     Example: {f.name}")
            
            log.info(f"Found {len(legal_files)} legal documents")
            
            for legal_file in legal_files:
                if DEBUG_DOCUMENT_FLOW:
                    log.info(f"🔍 DEBUG [TAXONOMY] Processing legal file: {legal_file.name}")
                await self._process_legal_file(legal_file)
        else:
            if DEBUG_DOCUMENT_FLOW:
                log.warning(f"🔍 DEBUG [TAXONOMY] ❌ Legal directory does not exist: {legal_dir}")
                # Check if files might be in other locations
                for subdir in json_dir.iterdir():
                    if subdir.is_dir():
                        enhanced_files = list(subdir.glob("*enhanced*.json"))
                        if enhanced_files:
                            log.info(f"🔍 DEBUG [TAXONOMY] Found enhanced files in {subdir.name}/: {len(enhanced_files)}")
                            for f in enhanced_files[:2]:
                                log.info(f"🔍 DEBUG [TAXONOMY]   {f.name}")
        
        # In synthesize_from_json(), after line 150:
        verbatim_dir = json_dir / "verbatim"
        if verbatim_dir.exists():
            verbatim_files = list(verbatim_dir.glob("*_verbatim_transcript.json"))
            for verbatim_file in verbatim_files:
                await self._process_verbatim_file(verbatim_file)
        
        # Save all entities and relationships
        await self._save_all_entities()
        
        # Calculate statistics
        for entity_type, entities in self.created_entities.items():
            stats[entity_type] = len(entities)
        stats['relationships'] = len(self.created_relationships)
        
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
            base = f"{meeting_title or 'City-Commission-Meeting'}_{meeting_date}"
            return f"event_{hashlib.sha1(base.encode()).hexdigest()[:8]}"

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
            source_file = data.get('Source_File_Name', agenda_file.name)
            
            # Create Meeting entity
            meeting_id = self._create_entity(
                'Event',
                {
                    'name': f"City Commission Meeting {meeting_date}",
                    'type': 'Regular Meeting',
                    'dateTime': meeting_date,
                    'status': 'Completed',
                    'outcome': 'Adjourned',
                    **self._provenance_for_file(agenda_file)
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
            
            # Make the Event own the agenda doc
            self._create_relationship(
                'hasDocument',
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
                        
                        # Create entity manually with our custom ID
                        agenda_entity = {
                            'type': 'AgendaItem',
                            'id': agenda_item_id,
                            'agendaItemID': agenda_item_id,        # make standards happy downstream
                            'itemID': item_code,
                            'code': item_code,                     # keep original "E-4" for display
                            'title': item_title,
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
                            'legalReferences': []
                        },
                        source=f"taxonomy_{agenda_file.stem}"
                    )
                    
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
            
            doc_type = data.get('document_type', 'ordinance')
            doc_number = data.get('document_number', legal_file.stem)
            
            # Create Document entity first
            doc_id = self._create_entity(
                'Document',
                {
                    'title': data.get('full_title') or f"{doc_type} {doc_number}",
                    'documentType': doc_type,
                    'status': 'Final',
                    'issueDate': data.get('adoption_date'),
                    'meetingDate': data.get('adoption_date') or data.get('meeting_date'),
                    'sourceURL': None
                },
                source=f"taxonomy_{legal_file.stem}"
            )
            
            # Determine if this is ordinance or resolution and extract number
            title_text = data.get('full_title', '') or data.get('title', '') or legal_file.name
            doc_kind = None
            doc_number_extracted = None
            
            # Try to extract ordinance or resolution number
            if doc_type.lower() == 'ordinance' or 'ordinance' in title_text.lower():
                doc_kind = 'ordinance'
                # Try explicit field first, then regex
                doc_number_extracted = doc_number
                if not doc_number_extracted:
                    match = re.search(r'\b(\d{4}-\d{1,3})\b', title_text)
                    if match:
                        doc_number_extracted = match.group(1)
                    else:
                        doc_number_extracted = legal_file.stem  # fallback
            elif doc_type.lower() == 'resolution' or 'resolution' in title_text.lower():
                doc_kind = 'resolution'
                # Try explicit field first, then regex
                doc_number_extracted = doc_number
                if not doc_number_extracted:
                    match = re.search(r'\b(\d{4}-\d{1,3})\b', title_text)
                    if match:
                        doc_number_extracted = match.group(1)
                    else:
                        doc_number_extracted = legal_file.stem  # fallback
            else:
                # Default to ordinance if unclear
                doc_kind = 'ordinance'
                doc_number_extracted = doc_number or legal_file.stem
            
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
                'Source_File_Name': legal_file.name,
                'Source_File_Path': str(legal_file),
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
            
            # Wire Policy to its Document (text)
            self._create_relationship(
                'hasDocument',   # choose a canonical name; consistent across the project
                policy_id,
                doc_id,
                {}
            )
            
            # Link to meeting by adoption date
            meeting_date = data.get('adoption_date') or data.get('meeting_date')
            event_id = self._find_event_by_date(meeting_date)
            if event_id:
                self._create_relationship('hasDocument', event_id, doc_id, {'role': doc_type})
            
            # Link to agenda item if we have the item code
            item_code = (data.get('agenda_item_code') or data.get('related_item') or
                        data.get('agenda_item') or data.get('related_item_code'))
            
            if item_code and meeting_date:
                agenda_item_id = self._find_agenda_item_id(item_code, meeting_date)
                if agenda_item_id:
                    self._create_relationship('isAbout', doc_id, agenda_item_id, {})
                    
                    # Wire Policy to the AgendaItem
                    self._create_relationship(
                        'isAbout',
                        policy_id,
                        agenda_item_id,
                        {'agendaCode': item_code}
                    )
            
            # Wire Policy directly to the Event (one-hop meeting link)
            if event_id:
                self._create_relationship('adoptedAt', policy_id, event_id, {'meetingDate': meeting_date})
            
            # Process sponsors
            for sponsor in data.get('sponsors', []):
                person_id = self._create_entity(
                    'Person',
                    {
                        'name': sponsor.get('name', ''),
                        'title': sponsor.get('title', 'Commissioner'),
                        'affiliation': 'City Council',
                        'contactInfo': None,
                        **self._provenance_for_file(legal_file)
                    },
                    source=f"taxonomy_{legal_file.stem}"
                )
                
                self._create_relationship(
                    'sponsors',
                    person_id,
                    policy_id,
                    {'sponsorshipType': 'primary'}
                )
            
        except Exception as e:
            log.error(f"Error processing legal file {legal_file}: {e}")
    
    def _create_entity(self, entity_type: str, attributes: Dict, source: str) -> str:
        """
        Create an entity using the toolkit.
        
        Returns:
            Entity ID
        """
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
        attrs.setdefault('Source_File_Name', source if isinstance(source, str) else None)
        attrs.setdefault('Source_File_Path', f"taxonomy://{source}" if isinstance(source, str) else None)

        # Create entity
        entity = self.toolkit.create_entity(entity_type, attrs, source)
        entity = EntityIDStandards.normalize_entity_id_fields(dict(entity), entity_type)
        # Keep ontology class without clobbering domain "type"
        entity['entity_type'] = entity_type

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
            
            meeting_date = data.get('meeting_date', '')
            doc_type = data.get('document_type', 'verbatim_transcript')
            
            # Extract item codes for linking
            source_hint = data.get('source_file') or data.get('Source_File_Name') or verbatim_file.stem
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
                    'sourceURL': None
                },
                source=f"taxonomy_{verbatim_file.stem}"
            )
            
            # Link to meeting
            event_id = self._find_event_by_date(meeting_date)
            if event_id:
                self._create_relationship('hasDocument', event_id, doc_id, {'role': 'transcript'})
            
            # Link to agenda items if we have codes
            for code in codes:
                agenda_item_id = self._find_agenda_item_id(code, meeting_date)
                if agenda_item_id:
                    self._create_relationship('isAbout', doc_id, agenda_item_id, {})
            
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
        for eid, e in bucket.items():
            d = e.get('dateTime') or e.get('meetingDate') or e.get('meeting_date') or e.get('issueDate')
            if d and self._canon_yyyymmdd(d) == target:
                return eid
        return None
    
    def _find_agenda_item_id(self, item_code: str, meeting_date: str) -> Optional[str]:
        """Find an AgendaItem entity by code and meeting date."""
        if not item_code or not meeting_date: 
            return None
        code = self._normalize_item_code(item_code)
        date = self._canon_yyyymmdd(meeting_date)
        bucket = self.created_entities.get('AgendaItem', {})
        for aid, a in bucket.items():
            a_code = self._normalize_item_code(a.get('itemID', ''))
            a_date = self._canon_yyyymmdd(a.get('meetingDate', '') or a.get('meeting_date', '') or a.get('date', ''))
            if a_code == code and (not date or a_date == date):
                return aid
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
                "agenda": "AgendaItem",
                "event": "Event",
                "person": "Person",
                "org": "Organization",
                "location": "Location",
                "section": "Section",
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
        relationship = {
            'type': rel_type,
            'source': source_id,
            'target': target_id,
            'attributes': attributes or {},
            '_source': 'taxonomy',
            '_created_at': datetime.now().isoformat(),
            '_edge_id': hashlib.md5(f"{source_id}_{rel_type}_{target_id}".encode()).hexdigest()[:12]
        }
        
        self.created_relationships.append(relationship)
