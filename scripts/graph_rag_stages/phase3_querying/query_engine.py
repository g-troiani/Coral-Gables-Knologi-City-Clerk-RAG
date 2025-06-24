# scripts/graph_rag_stages/phase3_querying/query_engine.py
"""
High-level engine that runs GraphRAG queries via subprocess and
post-processes answers (citations, source tracking, etc.).
"""

from __future__ import annotations

import asyncio, json, logging, subprocess, sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .smart_query_router import SmartQueryRouter
from .response_enhancer import ResponseEnhancer
from .source_tracker import SourceTracker

log = logging.getLogger(__name__)


class QueryEngine:
    def __init__(self, graphrag_root: Path):
        self.root = graphrag_root
        self.out_dir = graphrag_root / "output"
        dedup = self.out_dir / "deduplicated"
        self.data_root = dedup if dedup.exists() else self.out_dir

        self.router = SmartQueryRouter()
        self.enhancer = ResponseEnhancer()
        self.sources = SourceTracker()

    # ── main public API ──────────────────────────────────────────
    async def answer(self, q: str, method: str = "auto") -> Dict[str, Any]:
        log.info("🔎 query: %s", q[:120])

        self.sources.reset()
        if method == "auto":
            routing = self.router.determine_query_method(q)
            method = routing["method"]
            log.info("📍 routed → %s", method)

        raw = await self._run_graphrag(q, method)
        if not raw:
            return self._err("GraphRAG execution failed")

        enhanced = await self.enhancer.enhance_response(q, raw)
        enhanced["sources"] = self.sources.get_summary()
        enhanced["query_metadata"] = {
            "method_used": method,
            "root": str(self.root),
            "data_source": self.data_root.name,
        }
        return enhanced

    # ── subprocess exec ─────────────────────────────────────────
    async def _run_graphrag(self, q: str, method: str) -> Optional[Dict[str, Any]]:
        cmd = [
            sys.executable,
            "-m",
            "graphrag.query",
            "--root",
            str(self.root),
            "--method",
            method,
            q,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=300)
        except asyncio.TimeoutError:
            proc.kill()
            log.error("GraphRAG timed-out")
            return None

        if proc.returncode != 0:
            log.error("GraphRAG error → %s", err.decode()[:500])
            return None

        response_line = self._pick_response(out.decode().splitlines())
        return {"answer": response_line, "raw_output": out.decode(), "success": True}

    @staticmethod
    def _pick_response(lines: List[str]) -> str:
        for ln in reversed(lines):
            if ln.strip() and not ln.startswith(("INFO", "[")):
                return ln.strip()
        return lines[-1] if lines else ""

    # ── misc helpers ────────────────────────────────────────────
    @staticmethod
    def _err(msg: str) -> Dict[str, Any]:
        return {"success": False, "answer": f"Error: {msg}", "sources": {}}

    # convenience util
    def stats(self) -> Dict[str, Any]:
        ents = self.data_root / "create_final_entities.parquet"
        rels = self.data_root / "create_final_relationships.parquet"
        comm = self.data_root / "create_final_communities.parquet"

        get_len = lambda p: (pd.read_parquet(p).shape[0] if p.exists() else 0)
        return {
            "root": str(self.root),
            "entities": get_len(ents),
            "relationships": get_len(rels),
            "communities": get_len(comm),
        }

 