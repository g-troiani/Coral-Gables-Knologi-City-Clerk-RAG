#!/usr/bin/env python3
"""
Stage 6 — Supabase upsert optimiser (vendored copy).

Thread-safe client-pool, batch inserts with automatic fallback, and robust
type-sanitisation.
"""
from __future__ import annotations

import json, logging, os, pathlib, sys, threading
from datetime import datetime
from typing import Any, Dict, Sequence, List

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

META_FIELDS = [
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

# ── pool ────────────────────────────────────────────────────────────
class _Pool:
    def __init__(self, size: int = 10):
        self._size = size
        self._clients: List = []
        self._lock = threading.Lock()
        self._init = False

    def get(self):
        with self._lock:
            if not self._init:
                for _ in range(self._size):
                    self._clients.append(_init_sb())
                self._init = True
            import random

            return self._clients[random.randint(0, self._size - 1)]


_pool = _Pool()


def _init_sb():
    if not (SUPABASE_URL and SUPABASE_KEY):
        sys.exit("SUPABASE creds missing")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _scrub(obj: Any) -> Any:
    if isinstance(obj, str):
        return obj.replace("\x00", "")
    if isinstance(obj, list):
        return [_scrub(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _scrub(v) for k, v in obj.items()}
    return obj


# ── upsert helpers ─────────────────────────────────────────────────
def _upsert_document(sb, meta: Dict[str, Any]) -> str:
    meta = _scrub(meta)
    doc_type, date, title = meta.get("document_type"), meta.get("date"), meta.get("title")
    if doc_type and date and title:
        existing = (
            sb.table("city_clerk_documents")
            .select("id")
            .eq("document_type", doc_type)
            .eq("date", date)
            .eq("title", title)
            .limit(1)
            .execute()
            .data
        )
        if existing:
            doc_id = existing[0]["id"]
            sb.table("city_clerk_documents").update(meta).eq("id", doc_id).execute()
            return doc_id
    res = sb.table("city_clerk_documents").insert(meta).execute()
    if getattr(res, "error", None):
        raise RuntimeError(f"insert failed: {res.error}")
    return res.data[0]["id"]


def _insert_chunks(
    sb,
    doc_id: str,
    chunks: Sequence[Dict[str, Any]],
    src_json: pathlib.Path,
    batch_size: int = 500,
) -> int:
    ts = datetime.utcnow().isoformat()
    rows = [
        {
            "document_id": doc_id,
            "chunk_index": ch["chunk_index"],
            "token_start": ch["token_start"],
            "token_end": ch["token_end"],
            "page_start": ch["page_start"],
            "page_end": ch["page_end"],
            "text": _scrub(ch["text"]),
            "metadata": _scrub(ch.get("metadata", {})),
            "chunking_strategy": "token_window",
            "source_file": str(src_json),
            "created_at": ts,
        }
        for ch in chunks
    ]

    inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        try:
            res = sb.table("documents_chunks").insert(batch).execute()
            if getattr(res, "error", None):
                raise RuntimeError(res.error)
            inserted += len(batch)
        except Exception as exc:
            log.warning("batch insert fallback → %s", exc)
            # fall back to sub-batches
            for j in range(0, len(batch), 100):
                sub = batch[j : j + 100]
                try:
                    res = sb.table("documents_chunks").insert(sub).execute()
                    if getattr(res, "data", None):
                        inserted += len(sub)
                except Exception:
                    pass
    return inserted


# ── public façade ─────────────────────────────────────────────────
def upsert(
    json_doc: pathlib.Path,
    chunks: List[Dict[str, Any]] | None,
    *,
    do_embed: bool = False,
):
    sb = _pool.get()
    data = json.loads(json_doc.read_text())

    row = {k: data.get(k) for k in META_FIELDS} | {
        "source_pdf": data.get("source_pdf", str(json_doc))
    }

    # sanitise list fields
    if isinstance(row.get("commissioners"), str):
        row["commissioners"] = [row["commissioners"]]
    row["commissioners"] = row.get("commissioners") or []
    row["keywords"] = row.get("keywords") or []

    # agenda list → text
    if isinstance(row.get("agenda"), list):
        row["agenda"] = "; ".join(map(str, row["agenda"]))

    # numeric fields
    for fld in ("year", "month", "day"):
        if row.get(fld) is not None:
            try:
                row[fld] = int(row[fld])
            except Exception:
                row[fld] = None

    doc_id = _upsert_document(sb, row)

    if chunks:
        ins = _insert_chunks(sb, doc_id, chunks, json_doc)
        log.info("↑ inserted %d chunks for %s", ins, json_doc.stem)

    if do_embed and chunks:
        from . import embed_vectors  # local vendored import

        embed_vectors._cli()  # run with defaults


# CLI ----------------------------------------------------------------
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("json", type=pathlib.Path)
    p.add_argument("--chunks", type=pathlib.Path)
    p.add_argument("--embed", action="store_true")
    args = p.parse_args()

    ch = None
    if args.chunks and args.chunks.exists():
        ch = json.loads(args.chunks.read_text())
    upsert(args.json, ch, do_embed=args.embed) 