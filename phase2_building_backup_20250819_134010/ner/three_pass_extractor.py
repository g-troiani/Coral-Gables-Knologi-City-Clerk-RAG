import os, re, json, asyncio, logging, traceback
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from openai import AzureOpenAI

from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
from scripts.graph_rag_stages.common.entity_factory import EntityFactory
from scripts.graph_rag_stages.common.document_linker import DocumentLinker
from scripts.graph_rag_stages.common.relationship_labels import normalize_rel_label

log = logging.getLogger(__name__)

# -------------------------
# System + Prompt templates
# -------------------------

SYSTEM_MSG = (
  "You are extracting structured knowledge for a City Governance Ontology. "
  "Follow the ontology and ID standards exactly. Prefer reuse of provided IDs. "
  "Be conservative: if unsure, omit. Return ONLY strict JSON (no prose, no code fences). "
  "For every item you output, include _evidence with a short quote and character offsets "
  "into the supplied chunk text; include an optional confidence ∈ [0,1]."
)

PROMPT_ENTITIES = """CHUNK ENTITY EXTRACTION — ALL NODE TYPES

Context
- documentType: {document_type}
- meetingDate: {meeting_date}
- sourceFile: {source_file}
- chunkId: {chunk_id}

KNOWN IDS (reuse these if they match; do NOT duplicate)
{context_entities_json}

Types & REQUIRED ID fields
Person(personID), Organization(orgID), Document(documentID), AgendaDocument(agendaDocID),
Section(sectionID), AgendaItem(agendaItemID), Policy(policyID), Contract(contractID),
Technology(technologyID), VoteOutcome(outcomeID), Event(eventID), Location(locationID),
Asset(assetID), Project(projectID), Role(roleID), Topic(topicID),
Action(actionID), Presentation(presentationID), PublicComment(publicCommentID),
Board(boardID), Appointment(appointmentID), LegalReference(legalReferenceID)

ID rules (critical)
- Deterministic; snake_case; no random suffixes; include dates where they disambiguate.
- Examples:
  * "Commissioner John Smith" → person_commissioner_john_smith
  * "Planning Department" → org_planning_department
  * Agenda item “E‑4” on {meeting_date} → agenda_item_E4_{meeting_date_underscored}
  * Ordinance 2024‑01 → policy_ordinance_2024_01
  * City Commission Meeting → event_city_commission_meeting_{meeting_date_underscored}
  * Verbatim Transcript → document_verbatim_transcript_{meeting_date_underscored}

Output (JSON ONLY)
{{
  "entities": {{
    "<Type>": [
      {{
        "<idField>": "...",
        "type": "<Type>",
        "name"?: "...", "title"?: "...", "itemID"?: "E-4", "policyType"?: "ordinance",
        "meetingDate"?: "YYYY-MM-DD",
        "_evidence": [{{"quote":"...", "char_start": n, "char_end": n}}],
        "confidence"?: 0.0_to_1.0,
        "sameAs"?: ["<known_id_if_reused>"]
      }}
    ],
    ...
  }},
  "_meta": {{"chunkId":"{chunk_id}","sourceFile":"{source_file}"}}
}}

Text to analyze (use indices on this exact string)
<<<
{chunk_text}
>>>

Strictness
- Output only entities explicitly supported by this chunk.
- Every entity needs at least one _evidence span with offsets.
"""

PROMPT_RELATIONSHIPS = """CHUNK RELATIONSHIP EXTRACTION — ALL REL TYPES

Context
- chunkId: {chunk_id}
- meetingDate: {meeting_date}
- sourceFile: {source_file}

KNOWN IDS (reuse; do not invent new IDs unless deterministic from text)
{context_entities_json}

Allowed labels & directions (normalize to these)
isMemberOf (Person→Organization, Person→Board)
isPartOf (Organization→Organization, AgendaItem→Document/AgendaDocument)
holdsRole (Person→Role)
participatesIn (Person/Organization→Event)
authoredBy (Document/Policy/AgendaDocument→Person/Organization)
sponsors (Person/Organization→Policy/Project)
performsAction (Person/Organization→Action)
targetOf (Action→Document/Policy/Project/Asset)
recordedIn (Action→Document, Event→Document)
isLocatedAt (Organization/Project→Location)
occursAt (Event→Location)
references (Document/Policy/AgendaDocument→Document/Policy/Topic/LegalReference)
amends (Policy→Policy)
repeals (Policy→Policy)
owns (Person/Organization→Asset)
funds (Asset→Project/Organization)
addressesTopic (Document/Event/Section/Project→Topic)
discusses (Event→AgendaItem/Policy/Document/Topic)
hasAgenda (Event→AgendaDocument)
hasSection (AgendaDocument→Section)
hasAgendaItem (Section→AgendaItem)
precedes (AgendaItem→AgendaItem)
precedesSection (Section→Section)
resultsIn (AgendaItem→VoteOutcome)
governedBy (Contract→Policy)
uses (Organization→Technology)
votedOn (VoteOutcome→Policy/Contract/Project)
presents (Person→AgendaItem)
awards (Organization→Contract)
awardedTo (Contract→Organization)
implementedBy (Policy→Document)
embodies (Document→Policy)
implements (AgendaItem→Policy/Document)
hasTranscript (AgendaItem/Event→Document)
discussedIn (Document/Policy/AgendaItem→Event)
mentionedIn (ANY→Document)
containsItem (Document/Section→AgendaItem)

Output (JSON ONLY)
{{
  "relationships": [
    {{
      "type": "<normalized_label>",
      "source": "<entity_id>",
      "target": "<entity_id>",
      "attributes"?: {{
        "vote"?: "yes|no|abstain",
        "result"?: "passed|failed|tabled|deferred",
        "yesVotes"?: n, "noVotes"?: n, "abstentions"?: n,
        "startDate"?: "YYYY-MM-DD", "endDate"?: "YYYY-MM-DD",
        "amount"?: number
      }},
      "_evidence": [{{"quote":"...", "char_start": n, "char_end": n}}],
      "confidence"?: 0.0_to_1.0
    }}
  ],
  "_meta": {{"chunkId":"{chunk_id}","sourceFile":"{source_file}"}}
}}

Text to analyze
<<<
{chunk_text}
>>>

Rules
- Discard an edge if either endpoint ID cannot be resolved to Prompt‑1 output or KNOWN IDS.
- Put _evidence at the top level (NOT inside attributes).
"""

PROMPT_ATTRIBUTES = """CHUNK ATTRIBUTE MINING — PATCHES BY ENTITY

Context
- chunkId: {chunk_id}
- meetingDate: {meeting_date}
- sourceFile: {source_file}

Entities to fill (IDs only; use exactly these IDs)
{entity_ids_json}

Attribute targets (hints; include only when explicitly present)
- Person: name, title, affiliation, contactInfo?, speakerType?, votePosition?
- Organization: name, type, jurisdiction?, address?
- Document: title, documentType, issueDate?, meetingDate?, status?, sourceURL?, hyperlinks[]
- AgendaDocument: title, documentType="agenda", issueDate?, meetingDate?, sourceURL?
- Section: name, code?, order?, sectionType?, meetingDate?, parentAgendaDocID?
- AgendaItem: itemID(code), title, meetingDate?, documentType?, documentClassification?, order?, presenter?, parentSectionID?
- Policy: ordinanceNumber?, resolutionNumber?, title?, status?, effectiveDate?, expirationDate?, legalReferences[], meetingDate?
- Event: name, type?, dateTime?, status?, outcome?
- Action: actionType (move|second|approve|amend|defer...), timestamp?, outcome?, details?
- VoteOutcome: status/result, yesVotes?, noVotes?, abstentions?, voteDetails[]
- Location: name, type?, address?, coordinates?
- Asset: name, type, value?, currency?, fiscalYear?
- Project: name, description?, status?, startDate?, endDate?
- Role: title, startDate?, endDate?
- Topic: name, category?, description?
- Contract: title, vendor?, amount?, startDate?, endDate?, status?
- Technology: name, vendor?, purpose?, licenseType?
- Presentation: presenter?, topic?, agendaItem?
- PublicComment: speaker?, topic?, duration?, position?
- Board: name, purpose?, termStructure?
- Appointment: termStart?, termEnd?, boardName?, appointeeStatus?, nominatedBy?
- LegalReference: citation, codeName?, jurisdiction?, url?

Output (JSON ONLY)
{{
  "attributes": {{
    "<entity_id>": {{
      "<attr>": {{
        "value": <string|number|enum|array>,
        "_evidence": [{{"quote":"...", "char_start": n, "char_end": n}}],
        "confidence"?: 0.0_to_1.0
      }},
      ...
    }},
    ...
  }},
  "_meta": {{"chunkId":"{chunk_id}","sourceFile":"{source_file}"}}
}}

Text to analyze
<<<
{chunk_text}
>>>

Rules
- Only include attributes explicitly supported by the text (with evidence).
- Normalize: dates → YYYY-MM-DD; numbers → numeric; enums → prescribed strings.
"""

# -------------------------
# Runner
# -------------------------

class ThreePassExtractor:
    """
    Runs the 3-pass extraction (entities → relationships → attributes) per chunk and
    writes files into simple_ner_graph layout so Stage 4 (dedup) and Stage 5 (Cosmos push) just work.
    """

    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.chunks_dir = self.output_dir / "document_chunks"
        (self.output_dir / "relationships").mkdir(parents=True, exist_ok=True)
        (self.output_dir / "entities").mkdir(parents=True, exist_ok=True)
        # Ensure per-type entity dirs exist
        for t in UnifiedOntology.get_entity_categories():
            (self.output_dir / "entities" / t).mkdir(parents=True, exist_ok=True)

        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            timeout=60.0
        )
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
        if not self.model:
            raise ValueError("AZURE_OPENAI_DEPLOYMENT_NAME environment variable must be set")

        self.max_concurrent = int(os.getenv("NER_MAX_CONCURRENT", "5"))
        self.semaphore = asyncio.Semaphore(self.max_concurrent)

        # Toggle: should NER persist Document vertices (taxonomy already does)?
        self.keep_documents = os.getenv("NER_INCLUDE_DOCUMENTS", "false").lower() in ("1","true","yes")

    # ---------- Public API ----------

    async def run_all(self, phase1_entities: List[Dict]) -> int:
        """
        Process all chunk .txt files in document_chunks and write entities/relationships.
        Returns total entities written.
        """
        chunk_files = list(self.chunks_dir.glob("*.txt"))
        total_entities = 0
        tasks = [self._process_one_chunk(cf, phase1_entities) for cf in chunk_files]
        for fut in asyncio.as_completed(tasks):
            total_entities += await fut
        return total_entities

    # ---------- Internals ----------

    async def _process_one_chunk(self, chunk_file: Path, phase1_entities: List[Dict]) -> int:
        async with self.semaphore:
            try:
                meta = self._read_chunk_metadata(chunk_file)
                meta.setdefault("chunk_id", chunk_file.stem.split("_", 1)[0])
                meta.setdefault("document", chunk_file.stem.split("_", 1)[-1])
                source_file = meta.get("source_file_name") or meta.get("Source_File_Name") or f"{meta.get('document')}.pdf"
                meeting_date = meta.get("meeting_date") or meta.get("Meeting_Date") or ""
                doc_type = meta.get("document_type") or meta.get("Document_Type") or "doc"
                text = self._extract_chunk_text(chunk_file)

                # Build context IDs from stage-1 entities and any prior NER files for the same meeting
                context_entities = self._build_context_entities(phase1_entities, meeting_date)

                # -------- Pass 1: Entities
                ent_payload = await self._call_llm(
                    PROMPT_ENTITIES.format(
                        document_type=doc_type,
                        meeting_date=meeting_date,
                        meeting_date_underscored=meeting_date.replace("-", "_").replace(".", "_"),
                        source_file=source_file,
                        chunk_id=meta["chunk_id"],
                        context_entities_json=json.dumps(context_entities, ensure_ascii=False, indent=2),
                        chunk_text=text,
                    )
                )
                ent_result = self._parse_json(ent_payload, default={"entities": {}, "_meta": {}})
                entities_by_type = self._normalize_entities(ent_result.get("entities", {}))

                # Collect the IDs we just created for Pass 2/3
                local_ids = self._flatten_entity_ids(entities_by_type)

                # -------- Pass 2: Relationships
                rel_payload = await self._call_llm(
                    PROMPT_RELATIONSHIPS.format(
                        chunk_id=meta["chunk_id"],
                        meeting_date=meeting_date,
                        source_file=source_file,
                        context_entities_json=json.dumps(self._merge_context_ids(context_entities, local_ids), ensure_ascii=False, indent=2),
                        chunk_text=text,
                    )
                )
                rel_result = self._parse_json(rel_payload, default={"relationships": [], "_meta": {}})
                relationships = self._normalize_relationships(rel_result.get("relationships", []))

                # -------- Pass 3: Attributes
                attrs_payload = await self._call_llm(
                    PROMPT_ATTRIBUTES.format(
                        chunk_id=meta["chunk_id"],
                        meeting_date=meeting_date,
                        source_file=source_file,
                        entity_ids_json=json.dumps({"ids": list(self._merge_context_ids({}, local_ids).keys())}, ensure_ascii=False),
                        chunk_text=text,
                    )
                )
                attrs_result = self._parse_json(attrs_payload, default={"attributes": {}, "_meta": {}})

                # Merge attributes into entity dicts
                entities_by_type = self._apply_attribute_patches(entities_by_type, attrs_result.get("attributes", {}))

                # Persist entities (per type) and relationships
                written = self._persist_entities_and_relationships(
                    chunk_file, meta, entities_by_type, relationships
                )
                return written
            except Exception as e:
                log.error("3-pass extraction failed for %s: %s\n%s", chunk_file.name, e, traceback.format_exc())
                return 0

    # ---------- Helpers ----------

    async def _call_llm(self, user_prompt: str) -> str:
        # Single place to call Azure OpenAI (Chat Completions)
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "system", "content": SYSTEM_MSG},
                      {"role": "user", "content": user_prompt}],
            temperature=0,
            max_tokens=int(os.getenv("MAX_TOKENS", "8192"))
        )
        return (resp.choices[0].message.content or "").strip()

    def _parse_json(self, text: str, default: Any) -> Any:
        # Tolerate fenced code blocks
        if "```json" in text:
            text = text.split("```json", 1)[1].split("```", 1)[0].strip()
        elif "```" in text:
            text = text.split("```", 1)[1].split("```", 1)[0].strip()
        try:
            return json.loads(text)
        except Exception:
            log.warning("Failed to parse JSON; returning default. Text (first 400 chars): %r", text[:400])
            return default

    def _read_chunk_metadata(self, chunk_file: Path) -> Dict[str, Any]:
        md: Dict[str, Any] = {}
        try:
            content = chunk_file.read_text(encoding="utf-8")
        except Exception:
            return md
        if "---" in content:
            header, _ = content.split("---", 1)
            for line in header.strip().split("\n"):
                if line.startswith("#") and ":" in line:
                    key, value = line[1:].strip().split(":", 1)
                    norm_key = key.strip().lower().replace(" ", "_")
                    md[norm_key] = value.strip()
        # aliases
        if "document_type" not in md and "documenttype" in md:
            md["document_type"] = md["documenttype"]
        if "meeting_date" not in md and "meetingdate" in md:
            md["meeting_date"] = md["meetingdate"]
        if "source_file_name" not in md and "source" in md:
            md["source_file_name"] = md["source"]
        return md

    def _extract_chunk_text(self, chunk_file: Path) -> str:
        content = chunk_file.read_text(encoding="utf-8")
        if "---" not in content:
            return content
        # keep the first non-metadata part as text
        parts = content.split("---")
        for i, part in enumerate(parts):
            if i == 0:  # header
                continue
            cleaned = part.strip()
            if cleaned and not cleaned.startswith("- "):
                return cleaned
        return parts[-1].strip()

    def _build_context_entities(self, phase1_entities: List[Dict], meeting_date: str) -> Dict[str, List[Dict]]:
        """
        Phase-1 entities are already available in your pipeline. We map them into the
        {Type: [{idField:..., ...}]} shape to help the LLM reuse IDs.
        """
        by_type: Dict[str, List[Dict]] = {}
        for e in phase1_entities or []:
            t = e.get("type")
            if not t:
                continue
            t = str(t)
            id_field = EntityIDStandards.get_id_field(t)
            # Only include if there is an id or a strongly identifying field
            if e.get(id_field) or e.get("itemID") or e.get("title") or e.get("name"):
                by_type.setdefault(t, []).append(e)
        return by_type

    def _flatten_entity_ids(self, entities_by_type: Dict[str, List[Dict]]) -> Dict[str, str]:
        """
        Returns {entity_id: type}
        """
        out: Dict[str, str] = {}
        for t, ents in entities_by_type.items():
            id_field = EntityIDStandards.get_id_field(t)
            for e in ents:
                eid = e.get("id") or e.get(id_field)
                if eid:
                    out[eid] = t
        return out

    def _merge_context_ids(self, ctx: Dict[str, List[Dict]], id_type_map: Dict[str, str]) -> Dict[str, Dict]:
        """
        Produces a flat {id: {type:..., ...minimal fields...}} map used by prompts 2/3.
        """
        flat: Dict[str, Dict] = {}
        for t, items in (ctx or {}).items():
            id_field = EntityIDStandards.get_id_field(t)
            for it in items:
                eid = it.get("id") or it.get(id_field)
                if not eid:
                    continue
                flat[eid] = {"type": t}
        for eid, t in (id_type_map or {}).items():
            flat.setdefault(eid, {"type": t})
        return flat

    def _normalize_entities(self, raw_by_type: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
        """
        Entity-level normalization:
        - ensure canonical type labels present
        - normalize ID fields with EntityIDStandards
        """
        out: Dict[str, List[Dict]] = {}
        for t, ents in (raw_by_type or {}).items():
            if not isinstance(ents, list):
                continue
            id_field = EntityIDStandards.get_id_field(t)
            buf: List[Dict] = []
            for e in ents:
                try:
                    e["type"] = t
                    e = EntityIDStandards.normalize_entity_id_fields(e, t)
                    # Back-compat: copy canonical id into 'id' for convenience
                    eid = e.get("id") or e.get(id_field)
                    if eid:
                        e["id"] = eid
                    EntityFactory.validate_entity({**e, "type": t})  # raises on bad shapes
                    buf.append(e)
                except Exception as ex:
                    log.warning("Skipping invalid %s entity: %s", t, ex)
            if buf:
                out[t] = buf
        return out

    def _normalize_relationships(self, rels: List[Dict]) -> List[Dict]:
        out: List[Dict] = []
        for r in rels or []:
            if not isinstance(r, dict):
                continue
            rtype = normalize_rel_label(str(r.get("type") or "").strip())
            src = r.get("source")
            tgt = r.get("target")
            attrs = r.get("attributes") or {}
            if not isinstance(attrs, dict):
                attrs = {}
            # Strip volatile attrs
            for k in list(attrs.keys()):
                if k.startswith("Source_") or k.startswith("_") or k in {"created_at","_created_at","timestamp"}:
                    attrs.pop(k, None)
            if rtype and src and tgt:
                out.append({
                    "type": rtype, "source": src, "target": tgt,
                    "attributes": attrs,
                    "_evidence": r.get("_evidence", []),
                    "confidence": r.get("confidence")
                })
        return out

    def _apply_attribute_patches(self, entities_by_type: Dict[str, List[Dict]], patches: Dict[str, Dict]) -> Dict[str, List[Dict]]:
        """
        patches: {"<entity_id>": {"attr":{"value":..., "_evidence":[...]}, ...}}
        We shallow-merge attribute values into the entity dict with normalized keys.
        """
        if not patches:
            return entities_by_type
        # Build an index by entity id
        index: Dict[str, Dict] = {}
        for t, ents in entities_by_type.items():
            id_field = EntityIDStandards.get_id_field(t)
            for e in ents:
                eid = e.get("id") or e.get(id_field)
                if eid:
                    index[eid] = e
        # Apply patches
        for eid, attrs in patches.items():
            target = index.get(eid)
            if not target:
                continue
            for k, payload in attrs.items():
                if not isinstance(payload, dict) or "value" not in payload:
                    continue
                target[k] = payload["value"]
                # keep evidence on a side-channel if desired
                if "_evidence" in payload:
                    target.setdefault("_attr_evidence", {})[k] = payload["_evidence"]
        return entities_by_type

    def _persist_entities_and_relationships(self, chunk_file: Path, meta: Dict[str, Any],
                                            entities_by_type: Dict[str, List[Dict]],
                                            relationships: List[Dict]) -> int:
        chunk_id = meta.get("chunk_id") or chunk_file.stem.split("_", 1)[0]
        doc_name = meta.get("document") or chunk_file.stem.split("_", 1)[-1]
        source_file = meta.get("source_file_name") or meta.get("Source_File_Name") or f"{doc_name}.pdf"
        source_path = meta.get("source_file_path") or meta.get("Source_File_Path") or "unknown"
        meeting_date = meta.get("meeting_date") or meta.get("Meeting_Date")

        # Flatten for document-linker
        flat_ents: List[Dict] = []
        total = 0
        taxonomy_owned = {"Document"}  # AgendaDocument/Section/AgendaItem okay to persist; de-dup will merge

        for t, ents in (entities_by_type or {}).items():
            if not ents:
                continue
            if (not self.keep_documents) and (t in taxonomy_owned):
                continue
            # Envelope
            payload = {
                "chunk_id": chunk_id,
                "document": doc_name,
                "source_file": source_file,
                "source_path": source_path,
                "entity_type": t,
                "entities": ents,
                "_chunk_metadata": meta
            }
            out_file = self.output_dir / "entities" / t / f"{chunk_id}_{doc_name}.json"
            out_file.parent.mkdir(parents=True, exist_ok=True)
            out_file.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            total += len(ents)
            flat_ents.extend([{**e, "type": t} for e in ents])

        # Document provenance edges
        doc_edges = DocumentLinker.create_document_entity_relationships(flat_ents, meta, chunk_id)
        relationships = (relationships or []) + (doc_edges or [])

        if relationships:
            rel_file = self.output_dir / "relationships" / f"{chunk_id}_{doc_name}.json"
            rel_file.write_text(json.dumps({"relationships": relationships}, indent=2, ensure_ascii=False), encoding="utf-8")

        return total

