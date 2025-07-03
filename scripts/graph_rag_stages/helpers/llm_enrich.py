# scripts/graph_rag_stages/helpers/llm_enrich.py
"""
Stage 4 — async metadata enrichment (revendored helper)

Usage:
    python -m scripts.graph_rag_stages.helpers.llm_enrich <json_file>
"""
from __future__ import annotations

import asyncio, json, logging, os, pathlib, re
from textwrap import dedent
from typing import Any, Dict, List

from dotenv import load_dotenv
from openai import AzureOpenAI

from .acceleration_utils import hardware

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = "gpt-4.1-mini-2025-04-14"
log = logging.getLogger(__name__)


def _first(txt: str, n: int = 3000) -> str:
    return " ".join(txt.split()[:n])


def _sync_gpt(text: str) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        return {}
    cli = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "").split(" #")[0].strip().strip('"')
    )
    sys_prompt = dedent(
        """
        Extract city-clerk metadata in ONE JSON with:
          document_type, title, date, year, month, day,
          mayor, vice_mayor, commissioners[], city_attorney,
          city_manager, city_clerk, public_works_director,
          agenda, keywords[]
        """
    )
    
    # Get Azure deployment name, clean it
    deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4").split('"')[0].strip()
    
    rsp = cli.chat.completions.create(
        model=deployment_name,
        temperature=0,
        max_tokens=32768,
        messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": text}],
    )
    m = re.search(r"{[\s\S]*}", rsp.choices[0].message.content)
    return json.loads(m.group(0) if m else "{}")


async def _gpt_async(text: str, sem: asyncio.Semaphore) -> Dict[str, Any]:
    async with sem:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_gpt, text)


async def enrich_async(path: pathlib.Path, sem: asyncio.Semaphore) -> None:
    data = json.loads(path.read_text())
    body = " ".join(sec.get("text", "") for sec in data.get("sections", []))
    new_meta = await _gpt_async(_first(body), sem)
    data.update(new_meta)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    log.info("✓ enriched %s", path.name)


async def enrich_batch_async(files: List[pathlib.Path], max_conc: int = 10) -> None:
    sem = asyncio.Semaphore(max_conc)
    await asyncio.gather(*(enrich_async(f, sem) for f in files))


def _cli():
    import argparse, logging as _lg

    _lg.basicConfig(level=_lg.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("json", type=pathlib.Path, nargs="+")
    ap.add_argument("--concurrent", type=int, default=10)
    args = ap.parse_args()
    asyncio.run(enrich_batch_async(args.json, args.concurrent))


if __name__ == "__main__":
    _cli() 