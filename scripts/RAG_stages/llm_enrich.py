#!/usr/bin/env python3
"""
Stage 4 — LLM metadata enrichment with concurrent API calls.
"""
from __future__ import annotations
import json, logging, pathlib, re, os
from textwrap import dedent
from typing import Any, Dict, List
import asyncio
from concurrent.futures import ThreadPoolExecutor
import aiofiles
from groq import Groq

# ─── minimal shared helpers ────────────────────────────────────────
def _authors(val:Any)->List[str]:
    if val is None: return []
    if isinstance(val,list): return [str(a).strip() for a in val if a]
    return re.split(r"\s*,\s*|\s+and\s+", str(val).strip())

META_FIELDS_CORE = [
    "document_type", "title", "date", "year", "month", "day",
    "mayor", "vice_mayor", "commissioners",
    "city_attorney", "city_manager", "city_clerk", "public_works_director",
    "agenda", "keywords"
]
EXTRA_MD_FIELDS = ["peer_reviewed","open_access","license","open_access_status"]
META_FIELDS     = META_FIELDS_CORE+EXTRA_MD_FIELDS
_DEF_META_TEMPLATE = {**{k:None for k in META_FIELDS_CORE},
                      "doc_type":"scientific paper",
                      "authors":[], "keywords":[], "research_topics":[],
                      "peer_reviewed":None,"open_access":None,
                      "license":None,"open_access_status":None}

def merge_meta(*sources:Dict[str,Any])->Dict[str,Any]:
    merged=_DEF_META_TEMPLATE.copy()
    for src in sources:
        for k in META_FIELDS:
            v=src.get(k)
            if v not in (None,"",[],{}): merged[k]=v
    merged["authors"]=_authors(merged["authors"])
    merged["keywords"]=merged["keywords"] or []
    merged["research_topics"]=merged["research_topics"] or []
    return merged
# ───────────────────────────────────────────────────────────────────

from dotenv import load_dotenv
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL          = "gpt-4.1-mini-2025-04-14"
log            = logging.getLogger(__name__)

def _first_words(txt:str,n:int=3000)->str: return " ".join(txt.split()[:n])

def _gpt(text:str)->Dict[str,Any]:
    if not OPENAI_API_KEY: return text
    cli=Groq()
    sys_prompt = dedent(f"""
        Extract all metadata fields from this city clerk document. Return ONE JSON object with these fields:
        - document_type: must be one of [Resolution, Ordinance, Proclamation, Contract, Meeting Minutes, Agenda]
        - title: the document title
        - date: full date string as found in document
        - year: numeric year (YYYY)
        - month: numeric month (1-12)
        - day: numeric day of month
        - mayor: name only (e.g., "John Smith") - single person
        - vice_mayor: name only (e.g., "Jane Doe") - single person
        - commissioners: array of commissioner names only (e.g., ["Robert Brown", "Sarah Johnson", "Michael Davis"])
        - city_attorney: name only (e.g., "Emily Wilson")
        - city_manager: name only
        - city_clerk: name only
        - public_works_director: name only
        - agenda: agenda items or meeting topics if present
        - keywords: array of relevant keywords or topics (e.g., ["budget", "zoning", "infrastructure"])
    """)
    rsp=cli.chat.completions.create(
        model="meta-llama/llama-4-maverick-17b-128e-instruct",
        temperature=0,
        max_completion_tokens=8192,
        top_p=1,
        stream=False,
        stop=None,
        messages=[{"role":"system","content":sys_prompt},
                  {"role":"user","content":text}])
    raw=rsp.choices[0].message.content
    m=re.search(r"{[\s\S]*}",raw)
    return json.loads(m.group(0) if m else "{}")

# Async version of GPT call
async def _gpt_async(text: str, semaphore: asyncio.Semaphore) -> Dict[str, Any]:
    """Async GPT call with rate limiting."""
    if not OPENAI_API_KEY:
        return {}
    
    async with semaphore:  # Rate limiting
        loop = asyncio.get_event_loop()
        # Run synchronous OpenAI call in thread pool
        return await loop.run_in_executor(None, _gpt, text)

async def enrich_async(json_path: pathlib.Path, semaphore: asyncio.Semaphore) -> None:
    """Async version of enrich."""
    # Read file asynchronously
    async with aiofiles.open(json_path, 'r') as f:
        content = await f.read()
        data = json.loads(content)
    
    # Reconstruct body text
    full = " ".join(
        el.get("text", "") for sec in data["sections"] 
        for el in sec.get("elements", [])
    )
    
    # Make async GPT call
    new_meta = await _gpt_async(_first_words(full), semaphore)
    
    # Merge metadata
    data.update(new_meta)
    
    # Write back asynchronously
    async with aiofiles.open(json_path, 'w') as f:
        await f.write(json.dumps(data, indent=2, ensure_ascii=False))
    
    log.info("✓ metadata enriched → %s", json_path.name)

async def enrich_batch_async(
    json_paths: List[pathlib.Path],
    max_concurrent: int = 10
) -> None:
    """Enrich multiple documents concurrently with rate limiting."""
    semaphore = asyncio.Semaphore(max_concurrent)
    
    tasks = [enrich_async(path, semaphore) for path in json_paths]
    
    from tqdm.asyncio import tqdm_asyncio
    await tqdm_asyncio.gather(*tasks, desc="Enriching metadata")

# Keep original interface for compatibility
def enrich(json_path: pathlib.Path) -> None:
    """Original synchronous interface."""
    data = json.loads(json_path.read_text())
    full = " ".join(
        el.get("text", "") for sec in data["sections"] 
        for el in sec.get("elements", [])
    )
    new_meta = _gpt(_first_words(full))
    data.update(new_meta)
    json_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), 'utf-8')
    log.info("✓ metadata enriched → %s", json_path.name)

if __name__ == "__main__":
    import argparse, logging
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    p=argparse.ArgumentParser(); p.add_argument("json",type=pathlib.Path)
    enrich(p.parse_args().json) 