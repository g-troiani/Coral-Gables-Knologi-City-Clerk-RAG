# scripts/graph_rag_stages/helpers/db_upsert.py
"""
Stage 6 — Supabase batch upsert (revendored; legacy pipeline unchanged)
"""
from __future__ import annotations

import json, logging, os, pathlib, threading
from datetime import datetime
from typing import Any, Dict, List, Sequence

from dotenv import load_dotenv
from supabase import create_client
from tqdm import tqdm

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

META = [
    "document_type",
    "title",
    "date",
    "year",
    "month",
    "day",
    "mayor",
    "vice_mayor",
    "commissioners",
    "city_attorney",
    "city_manager",
    "city_clerk",
    "public_works_director",
    "agenda",
    "keywords",
]

log = logging.getLogger(__name__)


# ── thread-safe client pool ───────────────────────────────────────
class _Pool:
    def __init__(self, size: int = 10):
        self.size = size
        self._cli: list[Any] = []
        self._lock = threading.Lock()
        self._init = False

    def _lazy(self):
        for _ in range(self.size):
            self._cli.append(create_client(SUPABASE_URL, SUPABASE_KEY))
        self._init = True

    def get(self):
        with self._lock:
            if not self._init:
                self._lazy()
            import random

            return self._cli[random.randint(0, self.size - 1)]


_SB_POOL = _Pool()


# ── helpers ───────────────────────────────────────────────────────
def _scrub(obj: Any) -> Any:
    if isinstance(obj, str):
        return obj.replace("\x00", "")
    if isinstance(obj, list):
        return [_scrub(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _scrub(v) for k, v in obj.items()}
    return obj


def _upsert_doc(sb, meta: Dict[str, Any]) -> str:
    meta = _scrub(meta)
    res = sb.table("city_clerk_documents").insert(meta).execute()
    if getattr(res, "error", None):
        raise RuntimeError(res.error)
    return res.data[0]["id"]


def _insert_chunks(
    sb, doc_id: str, chunks: Sequence[Dict[str, Any]], src: pathlib.Path, batch: int = 500
) -> None:
    ts = datetime.utcnow().isoformat()
    rows = [
        {
            "document_id": doc_id,
            "chunk_index": i,
            "token_start": c["token_start"],
            "token_end": c["token_end"],
            "page_start": c["page_start"],
            "page_end": c["page_end"],
            "text": _scrub(c["text"]),
            "metadata": _scrub(c.get("metadata", {})),
            "chunking_strategy": "token_window",
            "source_file": str(src),
            "created_at": ts,
        }
        for i, c in enumerate(chunks)
    ]
    for i in range(0, len(rows), batch):
        part = rows[i : i + batch]
        res = sb.table("documents_chunks").insert(part).execute()
        if getattr(res, "error", None):
            log.error("batch insert error → %s", res.error)


# ── public API ────────────────────────────────────────────────────
def upsert(
    json_doc: pathlib.Path,
    chunks: List[Dict[str, Any]] | None,
    *,
    do_embed: bool = False,
) -> None:
    sb = _SB_POOL.get()
    data = json.loads(json_doc.read_text())
    meta = {k: data.get(k) for k in META} | {"source_pdf": data.get("source_pdf")}
    meta["commissioners"] = meta.get("commissioners") or []
    meta["keywords"] = meta.get("keywords") or []
    doc_id = _upsert_doc(sb, meta)
    if chunks:
        _insert_chunks(sb, doc_id, chunks, json_doc)
    log.info("↑ %s → %s", json_doc.name, doc_id)

    if do_embed:
        from .embed_vectors import main_async as _ev_async

        import asyncio

        asyncio.run(_ev_async(200, 10, 3))


# CLI helper
if __name__ == "__main__":
    import argparse, logging as _lg

    _lg.basicConfig(level=_lg.INFO, format="%(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("json", type=pathlib.Path)
    p.add_argument("--chunks", type=pathlib.Path)
    p.add_argument("--embed", action="store_true")
    a = p.parse_args()
    ch = json.loads(a.chunks.read_text()) if a.chunks and a.chunks.exists() else None
    upsert(a.json, ch, do_embed=a.embed) 