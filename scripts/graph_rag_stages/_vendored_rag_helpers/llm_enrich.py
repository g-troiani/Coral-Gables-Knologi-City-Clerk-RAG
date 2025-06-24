#!/usr/bin/env python3
"""
Stage 4 — LLM metadata enrichment (vendored copy).

Adds async rate-limited OpenAI calls so bulk enrichment can be parallelised.
"""
from __future__ import annotations

import asyncio, json, logging, pathlib, re, os
from textwrap import dedent
from typing import Any, Dict, List

from dotenv import load_dotenv
from groq import Groq

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = "gpt-4.1-mini-2025-04-14"
log = logging.getLogger(__name__)

_META_FIELDS = [
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
_DEF_META = {k: None for k in _META_FIELDS}


def _first_words(t: str, n: int = 3000) -> str:
    return " ".join(t.split()[:n])


def _gpt(text: str) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        return {}
    cli = Groq()
    sys_prompt = dedent(
        """
        Extract all metadata fields from this city-clerk document.
        Return ONE JSON object with keys:
          document_type, title, date, year, month, day,
          mayor, vice_mayor, commissioners[], city_attorney,
          city_manager, city_clerk, public_works_director,
          agenda, keywords[]
        """
    )
    rsp = cli.chat.completions.create(
        model="meta-llama/llama-4-maverick-17b-128e-instruct",
        temperature=0,
        max_completion_tokens=8192,
        top_p=1,
        messages=[{"role": "system", "content": sys_prompt},
                  {"role": "user", "content": _first_words(text)}],
    )
    raw = rsp.choices[0].message.content
    m = re.search(r"{[\s\S]*}", raw)
    return json.loads(m.group(0) if m else "{}")


# ── async wrapper with semaphore rate-limit ────────────────────────
async def _gpt_async(text: str, sem: asyncio.Semaphore) -> Dict[str, Any]:
    async with sem:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _gpt, text)


async def enrich_async(json_path: pathlib.Path, sem: asyncio.Semaphore) -> None:
    data = json.loads(json_path.read_text())
    body = " ".join(
        el.get("text", "")
        for sec in data.get("sections", [])
        for el in sec.get("elements", [])
    )
    new_meta = await _gpt_async(body, sem)
    data.update(new_meta)
    json_path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    log.info("✓ enriched %s", json_path.name)


async def enrich_batch_async(
    paths: List[pathlib.Path],
    *,
    max_concurrent: int = 10,
) -> None:
    sem = asyncio.Semaphore(max_concurrent)
    await asyncio.gather(*(enrich_async(p, sem) for p in paths))


def enrich(json_path: pathlib.Path) -> None:
    asyncio.run(enrich_batch_async([json_path]))


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("json", type=pathlib.Path)
    args = ap.parse_args()
    enrich(args.json) 