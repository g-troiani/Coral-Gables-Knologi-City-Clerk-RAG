# io_writer.py
import json, logging
from pathlib import Path

log = logging.getLogger(__name__)

class SimpleNERWriter:
    def __init__(self, out_root: Path):
        self.out_root = Path(out_root)
        (self.out_root / "document_chunks").mkdir(parents=True, exist_ok=True)
        (self.out_root / "entities").mkdir(parents=True, exist_ok=True)
        (self.out_root / "relationships").mkdir(parents=True, exist_ok=True)

    def write_chunks(self, chunks) -> int:
        """
        Persist chunk texts to disk so downstream stages (indices, vector DB)
        can read them from simple_ner_graph/document_chunks/.
        Header format is what VectorDatabasePusher._read_chunk_file() expects.
        """
        from pathlib import Path

        out_dir = Path(self.out_root) / "document_chunks"
        out_dir.mkdir(parents=True, exist_ok=True)

        def _get(d: dict, *keys, default=""):
            for k in keys:
                if k in d and d[k] is not None:
                    return d[k]
            return default

        written = 0
        for ch in (chunks or []):
            try:
                chunk_id = str(_get(ch, "chunk_id", "id", "Chunk ID")).strip()
                if not chunk_id:
                    # Keep patch minimal; skip if no stable key is provided
                    continue

                doc_name  = _get(ch, "document", "Document", "Source_File_Name", "source_file_name", default="unknown")
                doc_type  = _get(ch, "document_type", "Document Type", "documentType", default="unknown")
                meet_date = _get(ch, "meeting_date", "Meeting Date", "meetingDate", default="")
                idx = _get(ch, "Index")
                if not idx:
                    ci = _get(ch, "chunk_index", default=0)
                    tc = _get(ch, "total_chunks", default=1)
                    try:
                        idx = f"{int(ci)+1}/{int(tc)}"
                    except Exception:
                        idx = "1/1"
                src = _get(ch, "source_file_path", "Source_File_Path", "source", default="")
                content = _get(ch, "content", "text", default="")

                header_lines = [
                    f"# Chunk ID: {chunk_id}",
                    f"# Document: {doc_name}",
                    f"# Document Type: {doc_type}",
                    f"# Meeting Date: {meet_date}",
                    f"# Index: {idx}",
                ]
                if src:
                    header_lines.append(f"# Source: {src}")

                (out_dir / f"{chunk_id}.txt").write_text(
                    "\n".join(header_lines) + "\n---\n" + content,
                    encoding="utf-8"
                )
                written += 1
            except Exception:
                # Fail soft per chunk; keep pipeline moving
                continue

        return written

    def write_entities(self, entities):
        # entities: iterable of dict-like with 'type' and an id field (personID/orgID/etc.)
        from collections import defaultdict
        buckets = defaultdict(list)
        for e in entities:
            d = e.to_dict() if hasattr(e, "to_dict") else dict(e)
            buckets[d.get("type", "Unknown")].append(d)
        for etype, items in buckets.items():
            etype_dir = self.out_root / "entities" / etype
            etype_dir.mkdir(parents=True, exist_ok=True)
            for d in items:
                # pick the first *_id-like key as file name
                eid = (
                    d.get("personID") or d.get("orgID") or d.get("locationID") or
                    d.get("documentID") or d.get("agendaItemID") or d.get("meetingID") or
                    d.get("policyID") or d.get("id") or d.get("name")
                )
                if not eid:
                    eid = f"{etype}_{abs(hash(d.get('name', 'unknown'))) & 0xfffffff}"
                (etype_dir / f"{eid}.json").write_text(json.dumps(d, ensure_ascii=False), encoding="utf-8")

    def write_relationships(self, relationships):
        # relationships: iterable of dict-like with 'type', 'source', 'target'
        from collections import defaultdict
        rel_dir = self.out_root / "relationships"
        rel_dir.mkdir(parents=True, exist_ok=True)
        
        # Group by chunk_id if available, otherwise use a default
        chunks = defaultdict(list)
        for r in relationships:
            d = r.to_dict() if hasattr(r, "to_dict") else dict(r)
            chunk_id = d.get("chunk_id", "relationships")
            chunks[chunk_id].append(d)
        
        for chunk_id, rels in chunks.items():
            # Filter valid relationships
            valid_rels = [r for r in rels 
                         if isinstance(r, dict) and r.get("type") and r.get("source") and r.get("target")]
            if valid_rels:
                path = rel_dir / f"{chunk_id}_relationships.json"
                path.write_text(json.dumps(valid_rels, indent=2, ensure_ascii=False), encoding="utf-8")