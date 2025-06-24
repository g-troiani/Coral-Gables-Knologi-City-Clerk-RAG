#!/usr/bin/env python3
"""
Vendored Stage-7 — Optimised embedding with strict rate limiting,
deduplication, and conservative batching.

This is a **stand-alone** module; it should *not* import from the legacy
RAG_stages tree so that Graph-RAG evolves independently.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Set

import aiohttp
from dotenv import load_dotenv
from openai import OpenAI
from supabase import create_client
from tqdm.asyncio import tqdm as async_tqdm

# ── env & constants ────────────────────────────────────────────────
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

EMBEDDING_MODEL = "text-embedding-ada-002"
MODEL_TOKEN_LIMIT = 8192
TOKEN_GUARD = 200

# dynamic token budget
MAX_BATCH_TOKENS = 7692
MAX_CHUNK_TOKENS = 6000
MIN_BATCH_TOKENS = 100

DEFAULT_BATCH_ROWS = 200
DEFAULT_COMMIT_ROWS = 10
DEFAULT_MAX_CONCURRENT = 3

MAX_RETRIES = 5
RETRY_DELAY = 2
RATE_LIMIT_DELAY = 0.3
MAX_CALLS_PER_MINUTE = 150

# optional tiktoken accuracy
try:
    import tiktoken

    _ENCODER = tiktoken.encoding_for_model(EMBEDDING_MODEL)
except Exception:
    _ENCODER = None

# ── logging ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

# ── supabase helper ────────────────────────────────────────────────
def _init_supabase():
    if not (SUPABASE_URL and SUPABASE_KEY):
        log.error("SUPABASE env vars missing")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ── async embedder ─────────────────────────────────────────────────
class AsyncEmbedder:
    """Async OpenAI embedding client with hard rate-limit & token guard."""

    def __init__(self, api_key: str, max_concurrent: int = DEFAULT_MAX_CONCURRENT):
        self._api_key = api_key
        self._sem = asyncio.Semaphore(max_concurrent)
        self._session: aiohttp.ClientSession | None = None
        self._call_timestamps: List[float] = []
        self._total_tokens = 0
        self.call_count = 0

    # token counting
    def _count(self, text: str) -> int:
        if _ENCODER:
            return len(_ENCODER.encode(text))
        # fallback heuristic
        return int(len(text.split()) * 0.75) + 50

    # aio context
    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {self._api_key}"},
            timeout=aiohttp.ClientTimeout(total=300),
            connector=aiohttp.TCPConnector(limit=20, limit_per_host=10),
        )
        return self

    async def __aexit__(self, *exc):
        if self._session:
            await self._session.close()

    # rate-limit guard
    async def _rate_limit(self):
        now = time.time()
        self._call_timestamps[:] = [t for t in self._call_timestamps if now - t < 60]
        if len(self._call_timestamps) >= MAX_CALLS_PER_MINUTE - 5:
            sleep = 60 - (now - self._call_timestamps[0])
            log.info("🛑 60-sec rate-limit guard: sleeping %.1fs", sleep)
            await asyncio.sleep(sleep)
        await asyncio.sleep(RATE_LIMIT_DELAY)
        self._call_timestamps.append(time.time())

    # public embed method
    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        async with self._sem:
            await self._rate_limit()

            batch_tokens = sum(self._count(t) for t in texts)
            if batch_tokens > MAX_BATCH_TOKENS:
                raise RuntimeError(
                    f"batch token budget exceeded: {batch_tokens} > {MAX_BATCH_TOKENS}"
                )

            # actual call
            for attempt in range(MAX_RETRIES):
                try:
                    async with self._session.post(
                        "https://api.openai.com/v1/embeddings",
                        json={"model": EMBEDDING_MODEL, "input": texts},
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            self.call_count += 1
                            self._total_tokens += batch_tokens
                            return [item["embedding"] for item in data["data"]]
                        if resp.status == 429:  # rate-limit
                            sleep = (2 ** attempt) * 2
                            log.warning("rate-limit: backoff %.0fs", sleep)
                            await asyncio.sleep(sleep)
                        else:
                            log.warning("OpenAI error %d: %s", resp.status, await resp.text())
                except Exception as exc:
                    log.warning("OpenAI attempt %d failed: %s", attempt + 1, exc)
                await asyncio.sleep(RETRY_DELAY)
            raise RuntimeError("failed to embed after retries")


# ── chunk housekeeping helpers ────────────────────────────────────
def _deduplicate_chunks(chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    uniq = []
    for ch in chunks:
        txt = (ch.get("text") or "").strip()
        if not txt:
            continue
        h = hashlib.md5(txt.encode()).hexdigest()
        if h not in seen:
            seen.add(h)
            uniq.append(ch)
    removed = len(chunks) - len(uniq)
    if removed:
        log.info("deduplication removed %d identical chunks", removed)
    return uniq


def _count_tokens(text: str) -> int:
    if _ENCODER:
        return len(_ENCODER.encode(text))
    return int(len(text.split()) * 0.75) + 50


def _dynamic_batch_slices(rows: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    batches: List[List[Dict[str, Any]]] = []
    cur: List[Dict[str, Any]] = []
    cur_tok = 0

    for row in rows:
        txt = (row["text"] or "").replace("\x00", "").strip()
        if not txt:
            continue
        tks = _count_tokens(txt)
        if tks > MAX_CHUNK_TOKENS:
            log.warning("skipping oversize chunk (%d tok)", tks)
            continue
        if cur_tok + tks > MAX_BATCH_TOKENS and cur:
            batches.append(cur)
            cur = []
            cur_tok = 0
        cur.append(row)
        cur_tok += tks
    if cur_tok >= MIN_BATCH_TOKENS:
        batches.append(cur)
    elif cur:
        log.warning("dropping tiny tail batch (%d tok)", cur_tok)
    log.info("dynamic batching → %d batches (≤ %d tok each)", len(batches), MAX_BATCH_TOKENS)
    return batches


# ── DB helpers (unchanged behaviour, copied here so module is self-contained)
def _count_processed(sb) -> int:
    res = (
        sb.table("documents_chunks")
        .select("id", count="exact")
        .not_.is_("embedding", "null")
        .execute()
    )
    return res.count or 0


def _fetch_unprocessed(sb, *, limit: int) -> List[Dict[str, Any]]:
    res = (
        sb.table("documents_chunks")
        .select("id,text,token_start,token_end")
        .eq("chunking_strategy", "token_window")
        .is_("embedding", "null")
        .limit(limit)
        .execute()
    )
    return res.data or []


# ── main async driver ──────────────────────────────────────────────
async def _process_batch(
    sb, rows: List[Dict[str, Any]], embedder: AsyncEmbedder, commit_size: int
) -> int:
    uniq = _deduplicate_chunks(rows)
    slices = _dynamic_batch_slices(uniq)
    total = 0

    for slice_rows in slices:
        texts = [r["text"] for r in slice_rows]
        embeds = await embedder.embed_batch(texts)

        # batched DB update
        for row, emb in zip(slice_rows, embeds):
            sb.table("documents_chunks").update({"embedding": emb}).eq("id", row["id"]).execute()
        total += len(slice_rows)
    return total


async def _main_async(batch_size: int, commit_size: int, max_concurrent: int):
    if not OPENAI_API_KEY:
        log.error("OPENAI_API_KEY missing")
        sys.exit(1)

    sb = _init_supabase()
    existing = _count_processed(sb)
    log.info("rows already embedded: %d", existing)

    async with AsyncEmbedder(OPENAI_API_KEY, max_concurrent) as embedder:
        loop_no = 0
        while True:
            loop_no += 1
            rows = _fetch_unprocessed(sb, limit=batch_size)
            if not rows:
                log.info("✨  done – no more chunks")
                break
            embedded = await _process_batch(sb, rows, embedder, commit_size)
            log.info("loop %d → embedded %d chunks", loop_no, embedded)

    # write summary report
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    report = {
        "timestamp": ts,
        "batch_size": batch_size,
        "commit_size": commit_size,
        "total_rows_with_embedding": _count_processed(sb),
    }
    os.makedirs("reports/embedding", exist_ok=True)
    fname = f"reports/embedding/report_{ts}.json"
    with open(fname, "w") as fh:
        json.dump(report, fh, indent=2)
    log.info("report written → %s", fname)


# ── CLI entry-point ────────────────────────────────────────────────
def _cli():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_ROWS)
    ap.add_argument("--commit", type=int, default=DEFAULT_COMMIT_ROWS)
    ap.add_argument("--max-concurrent", type=int, default=DEFAULT_MAX_CONCURRENT)
    args = ap.parse_args()
    asyncio.run(_main_async(args.batch_size, args.commit, args.max_concurrent))


if __name__ == "__main__":  # pragma: no cover
    _cli() 