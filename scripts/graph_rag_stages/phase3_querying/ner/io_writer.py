# scripts/graph_rag_stages/phase3_querying/ner/io_writer.py
from pathlib import Path
from collections import defaultdict
import json

class SimpleNERWriter:
    def __init__(self, root: Path):
        self.root = Path(root)
        (self.root / "document_chunks").mkdir(parents=True, exist_ok=True)
        (self.root / "entities").mkdir(parents=True, exist_ok=True)
        (self.root / "relationships").mkdir(parents=True, exist_ok=True)

    def write_chunks(self, chunks):
        # chunks: iterable of objects with .id and .to_dict() or dicts
        out_dir = self.root / "document_chunks"
        for ch in chunks:
            data = ch.to_dict() if hasattr(ch, "to_dict") else dict(ch)
            cid = data.get("id") or data.get("chunkID") or data.get("chunk_id")
            if not cid:
                # fallback: deterministic file name
                cid = f"chunk_{abs(hash(json.dumps(data, sort_keys=True))) & 0xfffffff}"
            (out_dir / f"{cid}.json").write_text(json.dumps(data, ensure_ascii=False))

    def write_entities(self, entities):
        # entities: iterable of dict-like with 'type' and an id field (personID/orgID/etc.)
        buckets = defaultdict(list)
        for e in entities:
            d = e.to_dict() if hasattr(e, "to_dict") else dict(e)
            buckets[d.get("type", "Unknown")].append(d)
        for etype, items in buckets.items():
            etype_dir = self.root / "entities" / etype
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
                (etype_dir / f"{eid}.json").write_text(json.dumps(d, ensure_ascii=False))

    def write_relationships(self, relationships):
        rel_file = self.root / "relationships" / "relationships.jsonl"
        with open(rel_file, "w") as f:
            for r in relationships or []:
                d = r.to_dict() if hasattr(r, "to_dict") else dict(r)
                f.write(json.dumps(d, ensure_ascii=False) + "\n")
