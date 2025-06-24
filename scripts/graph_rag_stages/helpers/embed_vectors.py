# scripts/graph_rag_stages/helpers/embed_vectors.py
"""
Stage 7 — async embedding with strict rate-limit & dynamic-batch safety
(Re-vendored helper; legacy RAG_stages version left untouched.)
"""
from __future__ import annotations

import argparse, asyncio, hashlib, json, logging, os, sys, time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import aiohttp
from dotenv import load_dotenv
from openai import OpenAI
from supabase import create_client
from tqdm.asyncio import tqdm as async_tqdm

# optional accurate token counter
try:
    import tiktoken

    _TOK = tiktoken.encoding_for_model("text-embedding-ada-002")
except ImportError:
    _TOK = None

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

EMBED_MODEL = "text-embedding-ada-002"
MODEL_MAX_TOK = 8192
SAFE_MARGIN = 500
MAX_BATCH_TOK = MODEL_MAX_TOK - SAFE_MARGIN
MAX_CHUNK_TOK = 6000
MIN_BATCH_TOK = 100

MAX_CONCURRENT_DEFAULT = 3
MAX_CALLS_PER_MIN = 150
RATE_DELAY = 0.3
MAX_RETRIES = 5
RETRY_DELAY = 2

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)


# ── tiktoken helpers ──────────────────────────────────────────────
def _count_tok(text: str) -> int:
    if _TOK:
        return len(_TOK.encode(text))
    # conservative fallback ~0.75*words + 50
    return int(len(text.split()) * 0.75) + 50


# ── async embedder ────────────────────────────────────────────────
class AsyncEmbedder:
    def __init__(self, api_key: str, max_concurrent: int = MAX_CONCURRENT_DEFAULT):
        self.api_key = api_key
        self.sem = asyncio.Semaphore(max_concurrent)
        self.session: aiohttp.ClientSession | None = None
        self.call_times: List[float] = []
        self.total_tok = 0
        self.calls = 0

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=aiohttp.ClientTimeout(total=300),
            connector=aiohttp.TCPConnector(limit=20, limit_per_host=10),
        )
        return self

    async def __aexit__(self, *exc) -> None:
        if self.session:
            await self.session.close()

    async def _rate_limit(self) -> None:
        now = time.time()
        self.call_times = [t for t in self.call_times if now - t < 60]
        if len(self.call_times) >= MAX_CALLS_PER_MIN - 5:
            await asyncio.sleep(60 - (now - self.call_times[0]))
        await asyncio.sleep(RATE_DELAY)
        self.call_times.append(time.time())

    async def embed_batch_async(self, texts: List[str]) -> List[List[float]]:
        async with self.sem:
            await self._rate_limit()
            batch_tok = sum(_count_tok(t) for t in texts)
            if batch_tok > MAX_BATCH_TOK:
                raise RuntimeError(
                    f"batch too large: {batch_tok} tok (limit {MAX_BATCH_TOK})"
                )

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    assert self.session
                    async with self.session.post(
                        "https://api.openai.com/v1/embeddings",
                        json={"model": EMBED_MODEL, "input": texts},
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            self.total_tok += batch_tok
                            self.calls += 1
                            return [item["embedding"] for item in data["data"]]
                        if resp.status == 429:
                            await asyncio.sleep((2**attempt) * 2)
                        else:
                            log.warning("unexpected %s: %s", resp.status, await resp.text())
                except Exception as exc:  # network hiccup etc.
                    log.warning("attempt %d failed: %s", attempt, exc)
                    await asyncio.sleep(RETRY_DELAY * attempt)
            raise RuntimeError("failed to embed after retries")


# ── batching helpers ──────────────────────────────────────────────
def deduplicate_chunks(chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    uniq: List[Dict[str, Any]] = []
    dup = 0
    for ch in chunks:
        text = (ch.get("text") or "").strip()
        if not text:
            continue
        h = hashlib.md5(text.encode()).hexdigest()
        if h not in seen:
            seen.add(h)
            uniq.append(ch)
        else:
            dup += 1
    if dup:
        log.info("dedup removed %d duplicate chunks", dup)
    return uniq


def dynamic_batch_slices(rows: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    batches: List[List[Dict[str, Any]]] = []
    cur: List[Dict[str, Any]] = []
    cur_tok = 0
    skip = 0

    for row in rows:
        text = (row.get("text") or "").replace("\x00", "").strip()
        if not text:
            continue
        tok = _count_tok(text)
        if tok > MAX_CHUNK_TOK:
            skip += 1
            continue

        if cur_tok + tok > MAX_BATCH_TOK and cur:
            batches.append(cur)
            cur, cur_tok = [], 0
        cur.append(row)
        cur_tok += tok
    if cur_tok >= MIN_BATCH_TOK:
        batches.append(cur)

    log.info(
        "batch slicing: %d batches, %d skipped, max_batch_tok=%d",
        len(batches),
        skip,
        max((sum(_count_tok(x["text"]) for x in b) for b in batches), default=0),
    )
    return batches


# ── Supabase helpers (unchanged API) ──────────────────────────────
def _init_sb():
    if not (SUPABASE_URL and SUPABASE_KEY):
        sys.exit("⛔ SUPABASE creds missing")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _count_done(sb) -> int:
    return (
        sb.table("documents_chunks")
        .select("id", count="exact")
        .not_
        .is_("embedding", "null")
        .execute()
        .count
        or 0
    )


def _fetch_pending(sb, *, limit: int) -> List[Dict[str, Any]]:
    res = (
        sb.table("documents_chunks")
        .select("id,text,token_start,token_end")
        .eq("chunking_strategy", "token_window")
        .is_("embedding", "null")
        .limit(limit)
        .execute()
    )
    return res.data or []


# ── main driver ───────────────────────────────────────────────────
async def process_chunks_async(
    sb, rows: List[Dict[str, Any]], embedder: AsyncEmbedder
) -> int:
    uniq = deduplicate_chunks(rows)
    total_ok = 0
    for slc in dynamic_batch_slices(uniq):
        txts = [r["text"] for r in slc]
        embeds = await embedder.embed_batch_async(txts)
        tasks = []
        loop = asyncio.get_event_loop()

        def _update(r, e):
            return (
                sb.table("documents_chunks")
                .update({"embedding": e})
                .eq("id", r["id"])
                .execute()
            )

        for row, emb in zip(slc, embeds):
            tasks.append(loop.run_in_executor(None, _update, row, emb))
        res = await asyncio.gather(*tasks, return_exceptions=True)
        total_ok += sum(
            1
            for r in res
            if not isinstance(r, Exception) and not getattr(r, "error", None)
        )
    return total_ok


async def main_async(batch_size: int, commit: int, max_conc: int):
    sb = _init_sb()
    done0 = _count_done(sb)
    log.info("already embedded: %d rows", done0)

    async with AsyncEmbedder(OPENAI_API_KEY, max_conc) as embedder:
        while True:
            rows = _fetch_pending(sb, limit=batch_size)
            if not rows:
                break
            ok = await process_chunks_async(sb, rows, embedder)
            log.info("✓ embedded %d rows (total calls %d)", ok, embedder.calls)

    # summary JSON
    rep = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "total_embeddings": _count_done(sb),
    }
    out_dir = Path("reports/embedding")
    out_dir.mkdir(parents=True, exist_ok=True)
    fp = out_dir / f"report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    fp.write_text(json.dumps(rep, indent=2))
    log.info("report saved → %s", fp)


def _cli():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--commit", type=int, default=10)
    ap.add_argument("--max-concurrent", type=int, default=MAX_CONCURRENT_DEFAULT)
    args = ap.parse_args()
    asyncio.run(main_async(args.batch_size, args.commit, args.max_concurrent))


if __name__ == "__main__":
    _cli() 