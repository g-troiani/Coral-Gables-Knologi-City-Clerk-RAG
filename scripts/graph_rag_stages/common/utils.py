# scripts/graph_rag_stages/common/utils.py
"""
Shared helper utilities for the GraphRAG pipeline.
These helpers are *re-vendored* here so that the legacy RAG_stages tree
remains frozen and untouched.

Functions exported here **must** be re-exported in
`graph_rag_stages.common.__init__`.
"""

from __future__ import annotations

import json, logging, os, re
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List

import yaml
from dotenv import load_dotenv
from groq import Groq

# ──────────────────────────────────────────────────────────────
# env / LLM client
# ──────────────────────────────────────────────────────────────
load_dotenv()
_OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
_GROQ_CLIENT: Groq | None = None
log = logging.getLogger(__name__)


def get_llm_client() -> Groq:
    """Return a *singleton* Groq client (used across extractor stages)."""
    global _GROQ_CLIENT
    if _GROQ_CLIENT is None:
        _GROQ_CLIENT = Groq()
    return _GROQ_CLIENT


# ──────────────────────────────────────────────────────────────
# file / path helpers
# ──────────────────────────────────────────────────────────────
def ensure_directory_exists(p: Path | str) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)


def sanitize_filename(name: str) -> str:
    return re.sub(r"[^\w\-.]+", "_", name)


# ──────────────────────────────────────────────────────────────
# YAML-front-matter metadata extractor (for enriched markdown)
# ──────────────────────────────────────────────────────────────
_META_HEADER_RE = re.compile(r"^---\s*(.+?)\n---", re.S)


def extract_metadata_from_header(md: str) -> Dict[str, Any]:
    """
    Parse the top "DOCUMENT METADATA AND CONTEXT" header inserted by the
    agenda / document extractors and return a simple dict.
    """
    m = _META_HEADER_RE.search(md)
    if not m:
        return {}

    raw = m.group(1)
    # safe-load YAML in case the header is valid YAML
    try:
        meta = yaml.safe_load(raw)
        if isinstance(meta, dict):
            return _flatten_meta(meta)
    except Exception:
        pass  # fall back to heuristic parsing

    return _heuristic_meta_parse(raw)


def _flatten_meta(d: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten 2-level YAML mapping to a single dict with snake_case keys."""
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, dict):
            for k2, v2 in v.items():
                out[f"{k}_{k2}".lower()] = v2
        else:
            out[k.lower()] = v
    return out


def _heuristic_meta_parse(txt: str) -> Dict[str, Any]:
    """
    Ultra-simple "key: value" extractor for the header in case YAML load fails.
    """
    out: Dict[str, Any] = {}
    for line in txt.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            # Clean up the key - remove leading dashes, asterisks, and extra whitespace
            k_clean = k.strip().lstrip('- *').strip().lower().replace(' ', '_')
            v_clean = v.strip()
            if k_clean and v_clean:  # Only add if both key and value are non-empty
                out[k_clean] = v_clean
    return out


# ──────────────────────────────────────────────────────────────
# LLM JSON cleaning helpers
# ──────────────────────────────────────────────────────────────
_JSON_RE = re.compile(r"{[\s\S]+}", re.MULTILINE)


def clean_json_response(raw: str) -> Any:
    """
    Extract JSON object/array from an LLM string response and load it —
    returns *None* if no JSON payload found or parsing fails.
    """
    if m := _JSON_RE.search(raw):
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError as exc:
            log.warning("⚠️  JSON decode failed → %s", exc)
    return None


async def call_llm_with_retry(
    cli: Groq,
    messages: List[Dict[str, str]],
    *,
    model: str,
    temperature: float = 0.0,
    max_tokens: int = 8192,
    retries: int = 3,
) -> str:
    """Async wrapper calling Groq with exponential-backoff retries."""
    import asyncio, random

    for attempt in range(1, retries + 1):
        try:
            rsp = await cli.chat.completions.create(
                model=model,
                temperature=temperature,
                max_completion_tokens=max_tokens,
                top_p=1,
                stream=False,
                stop=None,
                messages=messages,
            )
            return rsp.choices[0].message.content
        except Exception as exc:
            if attempt == retries:
                raise
            delay = 2 ** attempt + random.random()
            log.warning("LLM call failed (%d/%d) → %s – retry in %.1fs", attempt, retries, exc, delay)
            await asyncio.sleep(delay)


async def extract_json_with_llm(cli: Groq, text: str, model: str) -> Dict[str, Any]:
    """
    Helper for the extractors: run one LLM prompt that returns a JSON block
    with all metadata. Cleans & returns a dict (may be empty).
    """
    prompt = dedent(
        """
        Read the following city-clerk document text and output ONE single
        JSON object with *all* metadata fields you can detect:
          - document_type, title, date, year, month, day
          - mayor, vice_mayor, commissioners[]
          - city_attorney, city_manager, city_clerk, public_works_director
          - agenda_items[] {item_code, title}
          - keywords[]
        """
    )

    messages = [
        {"role": "system", "content": "Structured metadata extractor"},
        {"role": "user", "content": prompt + "\n\n" + text[:12_000]},
    ]
    raw = await call_llm_with_retry(cli, messages, model=model, temperature=0.0)
    cleaned = clean_json_response(raw)
    return cleaned if isinstance(cleaned, dict) else {}


# ──────────────────────────────────────────────────────────────
# misc
# ──────────────────────────────────────────────────────────────
__all__ = [
    "get_llm_client",
    "ensure_directory_exists",
    "sanitize_filename",
    "extract_metadata_from_header",
    "clean_json_response",
    "call_llm_with_retry",
    "extract_json_with_llm",
] 