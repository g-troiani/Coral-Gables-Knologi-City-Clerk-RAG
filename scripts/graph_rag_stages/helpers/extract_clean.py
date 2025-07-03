# scripts/graph_rag_stages/helpers/extract_clean.py
"""
Stages 1-2 — PDF → clean text → logical sections (with concurrency)

• Thread-safe Docling converter pool
• OCR fallback
• Single GPT call for metadata
• Writes <city_clerk_documents/{json,txt}> artefacts
"""
from __future__ import annotations

import asyncio, json, logging, os, pathlib, re
from collections import Counter
from datetime import datetime
from textwrap import dedent
from typing import Any, Dict, List, Optional, Sequence

from dotenv import load_dotenv
from openai import AzureOpenAI
from tqdm import tqdm

from .acceleration_utils import hardware

# heavy deps (raise early if missing)
try:
    import PyPDF2
    from unstructured.partition.pdf import partition_pdf
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption
except ImportError as exc:  # pragma: no cover
    raise SystemExit(f"missing dependency → {exc}")

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

REPO = pathlib.Path(__file__).resolve().parents[3]
TXT_DIR = REPO / "city_clerk_documents" / "txt"
JSON_DIR = REPO / "city_clerk_documents" / "json"
TXT_DIR.mkdir(parents=True, exist_ok=True)
JSON_DIR.mkdir(parents=True, exist_ok=True)

GPT_META_MODEL = "gpt-4.1-mini-2025-04-14"
log = logging.getLogger(__name__)

# ── minimal helpers (latin-1 scrub, ws-norm etc.) ─────────────────
_LATIN1_MAP = {0x91: "'", 0x92: "'", 0x93: '"', 0x94: '"', 0x96: "–", 0x97: "—"}
_TRANSLATE = str.maketrans(_LATIN1_MAP)
_WS_RE = re.compile(r"[ \t]+\n")


def latin1(text: str) -> str:
    return text.translate(_TRANSLATE)


def norm_ws(txt: str) -> str:
    txt = _WS_RE.sub("\n", txt)
    return re.sub(r"[ \t]{2,}", " ", txt).strip()


def needs_ocr(txt: str) -> bool:
    return not txt.strip() or ("\x00" in txt)


# ── converter pool ────────────────────────────────────────────────
def _new_converter() -> DocumentConverter:
    opts = PdfPipelineOptions(do_ocr=True)
    return DocumentConverter({InputFormat.PDF: PdfFormatOption(pipeline_options=opts)})


class ConverterPool:
    def __init__(self, size: int | None = None):
        self.size = size or os.cpu_count() or 4
        self._conv: list[DocumentConverter] = []
        self._init = False
        import multiprocessing as mp

        self._lock = mp.Lock()

    def _lazy_init(self):
        self._conv = [_new_converter() for _ in range(self.size)]
        self._init = True

    def get(self) -> DocumentConverter:
        with self._lock:
            if not self._init:
                self._lazy_init()
            # naive round-robin
            idx = os.getpid() % self.size
            return self._conv[idx]


_CONV_POOL = ConverterPool()

# ── GPT metadata helper ───────────────────────────────────────────
def _gpt_meta(text: str) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        return {}
    
    # Clean environment variables (remove embedded comments/quotes)
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "").split(" #")[0].strip().strip('"')
    deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4").split('"')[0].strip()
    
    cli = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
        azure_endpoint=endpoint
    )
    
    prompt = dedent(
        """
        Extract metadata from this city clerk document. Return ONE JSON object with:
          document_type, title, date, year, month, day,
          mayor, vice_mayor, commissioners[], city_attorney,
          city_manager, city_clerk, public_works_director,
          agenda (array or text), keywords[]
        Text:
        """
    )
    rsp = cli.chat.completions.create(
        model=deployment_name,
        temperature=0,
        max_tokens=int(os.getenv("MAX_TOKENS", "16384")),
        messages=[
            {"role": "system", "content": "metadata extractor"},
            {"role": "user", "content": prompt + text[:15000]},
        ],
    )
    m = re.search(r"{[\s\S]*}", rsp.choices[0].message.content)
    return json.loads(m.group(0) if m else "{}")


# ── extraction core ───────────────────────────────────────────────
def _unstructured_elements(pdf: pathlib.Path) -> List[Dict[str, Any]]:
    els = partition_pdf(str(pdf), strategy="hi_res")
    pages: Dict[int, List[str]] = {}
    for el in els:
        pn = getattr(el.metadata, "page_number", 1)
        pages.setdefault(pn, []).append(norm_ws(str(el)))
    out = []
    for pn, txts in pages.items():
        out.append({"section": f"Page {pn}", "page_number": pn, "text": "\n".join(txts)})
    return out


def _pypdf_extract(pdf: pathlib.Path) -> List[Dict[str, Any]]:
    out = []
    with pdf.open("rb") as fh:
        for pn, pg in enumerate(PyPDF2.PdfReader(fh).pages, 1):
            txt = norm_ws(pg.extract_text() or "")
            out.append({"section": f"Page {pn}", "page_number": pn, "text": txt})
    return out


def extract_single(
    pdf: pathlib.Path,
    *,
    overwrite: bool = False,
    enrich_llm: bool = True,
    stats: Counter | None = None,
) -> pathlib.Path:
    txt_path = TXT_DIR / f"{pdf.stem}.txt"
    json_path = JSON_DIR / f"{pdf.stem}.json"
    if not overwrite and txt_path.exists() and json_path.exists():
        return json_path

    conv = _CONV_POOL.get()
    try:
        bundle = conv.convert(str(pdf))
        sections = [
            {
                "section": f"Page {pn}",
                "page_number": pn,
                "text": norm_ws(pg.text) if hasattr(pg, "text") else "",
            }
            for pn, pg in enumerate(bundle.document.pages, 1)
        ]
    except Exception as exc:
        log.warning("Docling failed on %s → %s", pdf.name, exc)
        sections = []

    if not sections:
        try:
            sections = _unstructured_elements(pdf)
        except Exception:
            sections = _pypdf_extract(pdf)

    # OCR repair
    if '{ocr}' in pdf.name.lower() or any(needs_ocr(s["text"]) for s in sections):
        log.info("OCR fallback on %s", pdf.name)
        # could plug pytesseract here if needed

    for s in sections:
        s["text"] = latin1(s["text"])

    body_text = " ".join(s["text"] for s in sections)
    meta = _gpt_meta(body_text) if enrich_llm else {}

    payload = {
        **meta,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "source_pdf": str(pdf),
        "sections": sections,
    }
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    txt_path.write_text("\n\n".join(f"# {s['section']}\n{s['text']}" for s in sections))

    if stats is not None:
        stats["processed"] += 1
    return json_path


async def extract_batch_async(
    pdfs: List[pathlib.Path],
    *,
    overwrite: bool = False,
    enrich_llm: bool = True,
) -> List[pathlib.Path]:
    stats = Counter()
    loop = asyncio.get_event_loop()

    async def _one(pdf):
        return await loop.run_in_executor(
            None, extract_single, pdf, overwrite, enrich_llm, stats
        )

    futs = [_one(p) for p in pdfs]
    from tqdm.asyncio import tqdm_asyncio

    results = await tqdm_asyncio.gather(*futs, desc="extract PDFs")
    log.info("Extraction complete – %d processed", stats["processed"])
    return results


if __name__ == "__main__":
    import argparse, logging as _lg

    _lg.basicConfig(level=_lg.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf", type=pathlib.Path)
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()
    print(extract_single(args.pdf, overwrite=args.overwrite)) 