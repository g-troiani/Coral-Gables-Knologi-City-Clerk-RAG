#!/usr/bin/env python3
"""
Stage 1-2 — *Extract PDF → clean text → logical sections*  
Optimised version with concurrent processing.

This copy is vendored inside `graph_rag_stages._vendored_rag_helpers`
so the legacy `RAG_stages` tree can remain frozen.
"""
from __future__ import annotations

import json, logging, os, pathlib, re, sys
from collections import Counter
from datetime import datetime
from textwrap import dedent
from typing import Any, Dict, List, Sequence, Optional
import asyncio
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

# ─── helpers (inline, no external deps) ─────────────────────────────
_LATIN1_REPLACEMENTS = {
    0x82: "‚", 0x83: "ƒ", 0x84: "„", 0x85: "…", 0x86: "†", 0x87: "‡",
    0x88: "ˆ", 0x89: "‰", 0x8A: "Š", 0x8B: "‹", 0x8C: "Œ", 0x8E: "Ž",
    0x91: "'", 0x92: "'", 0x93: '"', 0x94: '"', 0x95: "•", 0x96: "–",
    0x97: "—", 0x98: "˜", 0x99: "™", 0x9A: "š", 0x9B: "›", 0x9C: "œ",
    0x9E: "ž", 0x9F: "Ÿ",
}
_TRANSLATE_LAT1 = str.maketrans(_LATIN1_REPLACEMENTS)
def latin1_scrub(txt: str) -> str: return txt.translate(_TRANSLATE_LAT1)

_WS_RE = re.compile(r"[ \t]+\n")
def normalize_ws(txt: str) -> str:
    return re.sub(r"[ \t]{2,}", " ", _WS_RE.sub("\n", txt)).strip()

def pct_ascii_letters(txt: str) -> float:
    letters = sum(ch.isascii() and ch.isalpha() for ch in txt)
    return letters / max(1, len(txt))

def needs_ocr(txt: str) -> bool:
    return (not txt.strip()) or ("\x00" in txt) or (pct_ascii_letters(txt) < 0.15)

def scrub_nuls(obj: Any) -> Any:
    if isinstance(obj, str):
        return obj.replace("\x00", "")
    if isinstance(obj, list):
        return [scrub_nuls(x) for x in obj]
    if isinstance(obj, dict):
        return {k: scrub_nuls(v) for k, v in obj.items()}
    return obj
# ────────────────────────────────────────────────────────────────────

from dotenv import load_dotenv
from groq import Groq
from supabase import create_client                      # option to push rows
from tqdm import tqdm

# heavy deps guarded
try:
    import PyPDF2
    from unstructured.partition.pdf import partition_pdf
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption
except ImportError as exc:  # pragma: no cover
    sys.exit(
        f"Missing dependency → {exc}.  "
        "Run `pip install -r requirements.txt`."
    )

# ─── env / paths ────────────────────────────────────────────────────
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
TXT_DIR = REPO_ROOT / "city_clerk_documents" / "txt"
JSON_DIR = REPO_ROOT / "city_clerk_documents" / "json"
TXT_DIR.mkdir(parents=True, exist_ok=True)
JSON_DIR.mkdir(parents=True, exist_ok=True)

GPT_META_MODEL = "gpt-4.1-mini-2025-04-14"
log = logging.getLogger(__name__)

# ╔════════ metadata helpers ════════════════════════════════════════╗
_HEADING_TYPES = {"title", "heading", "header", "subtitle", "subheading"}
_DOI_RE = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.I)
_JOURNAL_RE = re.compile(
    r"(Journal|Proceedings|Annals|Psychiatry|Psychology|Nature|Science)[^\n]{0,120}",
    re.I,
)
_ABSTRACT_RE = re.compile(
    r"(?<=\bAbstract\b[:\s])(.{50,2000}?)(?:\n[A-Z][^\n]{0,60}\n|\Z)", re.S
)
_KEYWORDS_RE = re.compile(r"\bKey\s*words?\b[:\s]*(.+)", re.I)

_DEF_META = {
    "document_type": None,
    "title": None,
    "date": None,
    "year": None,
    "month": None,
    "day": None,
    "mayor": None,
    "vice_mayor": None,
    "commissioners": [],
    "city_attorney": None,
    "city_manager": None,
    "city_clerk": None,
    "public_works_director": None,
    "agenda": None,
    "keywords": [],
}


def _authors(val) -> List[str]:
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x).strip() for x in val if x]
    return re.split(r"\s*,\s*|\s+and\s+", str(val).strip())


def merge_meta(*sources: Dict[str, Any]) -> Dict[str, Any]:
    out = _DEF_META.copy()
    for src in sources:
        for k, v in src.items():
            if v not in (None, "", [], {}):
                out[k] = v
    out["commissioners"] = out.get("commissioners") or []
    out["keywords"] = out.get("keywords") or []
    return out


def bib_from_filename(pdf: pathlib.Path) -> Dict[str, Any]:
    s = pdf.stem
    m = re.search(r"\b(19|20)\d{2}\b", s)
    yr = int(m.group(0)) if m else None
    title = s[m.end() :].strip(" -_") if m else s
    return {"year": yr, "title": title}


def bib_from_header(txt: str) -> Dict[str, Any]:
    md = {}
    if (m := _DOI_RE.search(txt)):
        md["doi"] = m.group(0)
    if (m := _JOURNAL_RE.search(txt)):
        md["journal"] = " ".join(m.group(0).split())
    if (m := _ABSTRACT_RE.search(txt)):
        md["abstract"] = " ".join(m.group(1).split())
    if (m := _KEYWORDS_RE.search(txt)):
        kws = [
            k.strip(" ;.,") for k in re.split(r"[;,]", m.group(1)) if k.strip()
        ]
        md["keywords"] = kws
    return md


def _first_words(txt: str, n: int = 3000) -> str:
    return " ".join(txt.split()[:n])


# ─── GPT enrichment ────────────────────────────────────────────────
def gpt_metadata(text: str) -> Dict[str, Any]:
    """Single-call metadata extractor."""
    if not OPENAI_API_KEY:
        return {}
    cli = Groq()
    prompt = dedent(
        """
        Extract metadata from this city clerk document and return a JSON object
        with these fields:
          - document_type: one of [Resolution, Ordinance, Proclamation,
                                  Contract, Meeting Minutes, Agenda]
          - title, date, year, month, day
          - mayor, vice_mayor, commissioners[], city_attorney,
            city_manager, city_clerk, public_works_director
          - agenda: agenda items or summary if present
          - keywords[]
        Text:
        """
    ) + text
    rsp = cli.chat.completions.create(
        model="meta-llama/llama-4-maverick-17b-128e-instruct",
        temperature=0,
        max_completion_tokens=8192,
        top_p=1,
        messages=[{"role": "system", "content": "Structured metadata extractor"},
                  {"role": "user", "content": prompt}],
    )
    txt = rsp.choices[0].message.content
    j = re.search(r"{[\s\S]*}", txt)
    return json.loads(j.group(0) if j else "{}")


# ╔══════════════════════════════════════════════════════════════════╗
# ║                 core extraction logic (docling / fallback)       ║
# ╚══════════════════════════════════════════════════════════════════╝
def _make_converter() -> DocumentConverter:
    opts = PdfPipelineOptions()
    opts.do_ocr = True
    return DocumentConverter(
        {InputFormat.PDF: PdfFormatOption(pipeline_options=opts)}
    )


def _docling_elements(doc) -> List[Dict[str, Any]]:
    pages: Dict[int, List[Dict[str, str]]] = {}
    for el, _ in doc.iterate_items():
        pn = getattr(el.prov[0], "page_no", 1)
        lbl = (getattr(el, "label", "") or "").upper()
        typ = (
            "heading"
            if lbl in ("TITLE", "SECTION_HEADER", "HEADER")
            else "list_item"
            if lbl == "LIST_ITEM"
            else "table"
            if lbl == "TABLE"
            else "paragraph"
        )
        pages.setdefault(pn, []).append({"type": typ, "text": str(el).strip()})
    out = []
    for pn in sorted(pages):
        out.append(
            {
                "section": f"Page {pn}",
                "page_number": pn,
                "text": "\n".join(el["text"] for el in pages[pn]),
                "elements": pages[pn],
            }
        )
    return out


def _unstructured_elements(pdf: pathlib.Path) -> List[Dict[str, Any]]:
    els = partition_pdf(str(pdf), strategy="hi_res")
    pages: Dict[int, List[Dict[str, str]]] = {}
    for el in els:
        pn = getattr(el.metadata, "page_number", 1)
        pages.setdefault(pn, []).append(
            {"type": el.category or "paragraph", "text": normalize_ws(str(el))}
        )
    return [
        {
            "section": f"Page {pn}",
            "page_number": pn,
            "text": "\n".join(e["text"] for e in it),
            "elements": it,
        }
        for pn, it in sorted(pages.items())
    ]


def _pypdf_elements(pdf: pathlib.Path) -> List[Dict[str, Any]]:
    out = []
    with open(pdf, "rb") as fh:
        for pn, pg in enumerate(PyPDF2.PdfReader(fh).pages, 1):
            raw = normalize_ws(pg.extract_text() or "")
            paras = [p for p in re.split(r"\n{2,}", raw) if p.strip()]
            out.append(
                {
                    "section": f"Page {pn}",
                    "page_number": pn,
                    "text": "\n".join(paras),
                    "elements": [{"type": "paragraph", "text": p} for p in paras],
                }
            )
    return out


def _group_by_headings(
    page_secs: Sequence[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    out, cur, last = [], None, 1
    for s in page_secs:
        pn = s.get("page_number", last)
        last = pn
        for el in s.get("elements", []):
            kind = (el.get("type") or "").lower()
            txt = el.get("text", "").strip()
            if kind in _HEADING_TYPES and txt:
                if cur:
                    out.append(cur)
                cur = {
                    "section": txt,
                    "page_start": pn,
                    "page_end": pn,
                    "elements": [el.copy()],
                }
            else:
                if not cur:
                    cur = {
                        "section": "(untitled)",
                        "page_start": pn,
                        "page_end": pn,
                        "elements": [],
                    }
                cur["elements"].append(el.copy())
                cur["page_end"] = pn
    if cur and not out:
        out.append(cur)
    return out


def _sections_md(sections: List[Dict[str, Any]]) -> str:
    md: List[str] = []
    for s in sections:
        md.append(f"# {s.get('section','(Untitled)')}")
        for el in s.get("elements", []):
            md.append(el.get("text", ""))
        md.append("")
    return "\n".join(md).strip()


# ╔══════════════════════════════════════════════════════════════════╗
# ║                     extraction entry-point                      ║
# ╚══════════════════════════════════════════════════════════════════╝
def extract_pdf(
    pdf: pathlib.Path,
    txt_dir: pathlib.Path,
    json_dir: pathlib.Path,
    conv: DocumentConverter,
    *,
    overwrite: bool = False,
    ocr_lang: str = "eng",
    keep_markup: bool = True,
    docling_only: bool = False,
    stats: Counter | None = None,
    enrich_llm: bool = True,
) -> pathlib.Path:
    """Return path to JSON payload ready for downstream stages."""
    txt_path = txt_dir / f"{pdf.stem}.txt"
    json_path = json_dir / f"{pdf.stem}.json"
    if (
        not overwrite
        and txt_path.exists()
        and json_path.exists()
    ):
        return json_path

    # 1️⃣ Docling
    page_secs: List[Dict[str, Any]] = []
    bundle = None
    try:
        bundle = conv.convert(str(pdf))
        if keep_markup:
            page_secs = _docling_elements(bundle.document)
        else:
            full = bundle.document.export_to_text(page_break_marker="\f")
            page_secs = [
                {
                    "section": "Full document",
                    "page_number": 1,
                    "text": full,
                    "elements": [{"type": "paragraph", "text": full}],
                }
            ]
    except Exception as exc:
        log.warning("Docling failed on %s → %s", pdf.name, exc)

    # 2️⃣ Unstructured fallback
    if not page_secs and not docling_only:
        try:
            page_secs = _unstructured_elements(pdf)
        except Exception as exc:
            log.warning("unstructured failed on %s → %s", pdf.name, exc)

    # 3️⃣ PyPDF fallback
    if not page_secs and not docling_only:
        log.info("PyPDF fallback on %s", pdf.name)
        page_secs = _pypdf_elements(pdf)

    if not page_secs:
        raise RuntimeError("No text extracted from PDF")

    # Latin-1 scrub + per-section OCR repair
    for sec in page_secs:
        sec["text"] = latin1_scrub(sec.get("text", ""))
        for el in sec.get("elements", []):
            el["text"] = latin1_scrub(el.get("text", ""))
        pn = sec.get("page_number")
        if pn and needs_ocr(sec["text"]):
            try:
                from pdfplumber import open as pdfopen
                import pytesseract

                with pdfopen(str(pdf)) as doc:
                    pil = doc.pages[pn - 1].to_image(resolution=300).original
                ocr_txt = normalize_ws(pytesseract.image_to_string(pil, lang=ocr_lang))
                if ocr_txt:
                    sec["text"] = ocr_txt
                    sec["elements"] = [
                        {"type": "paragraph", "text": p}
                        for p in re.split(r"\n{2,}", ocr_txt)
                        if p.strip()
                    ]
                    if stats is not None:
                        stats["ocr_pages"] += 1
            except Exception:
                pass

    logical_secs = _group_by_headings(page_secs) if keep_markup else page_secs

    # force each section to have a "text" field
    for sec in logical_secs:
        if "elements" in sec:
            sec["text"] = "\n".join(el.get("text", "") for el in sec["elements"])
        sec["text"] = sec.get("text", "")

    header_txt = " ".join(s["text"] for s in page_secs[:2])[:8000]
    heuristic_meta = merge_meta(
        bundle.document.metadata.model_dump()
        if bundle and hasattr(bundle.document, "metadata")
        else {},
        bib_from_filename(pdf),
        bib_from_header(header_txt),
    )

    llm_meta: Dict[str, Any] = {}
    if enrich_llm and OPENAI_API_KEY:
        try:
            llm_meta = gpt_metadata(
                _first_words(" ".join(s["text"] for s in logical_secs))
            )
        except Exception as exc:
            log.warning("LLM metadata extraction failed on %s → %s", pdf.name, exc)

    meta = merge_meta(heuristic_meta, llm_meta)
    payload = {
        **meta,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "source_pdf": str(pdf.resolve()),
        "sections": logical_secs,
    }

    json_path.parent.mkdir(parents=True, exist_ok=True)
    txt_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), "utf-8")

    md_text = (
        _sections_md(logical_secs)
        if keep_markup
        else "\n".join(
            "# " + s.get("section", "(No title)") + "\n\n" + s.get("text", "")
            for s in logical_secs
        )
    )
    txt_path.write_text(md_text, "utf-8")

    if stats is not None:
        stats["processed"] += 1

    return json_path


# ─── thread-safe converter pool ────────────────────────────────────
class _ConverterPool:
    def __init__(self, size: int | None = None):
        self.size = size or mp.cpu_count()
        self._convs: List[DocumentConverter] = []
        self._lock = mp.Lock()
        self._init = False

    def get(self) -> DocumentConverter:
        with self._lock:
            if not self._init:
                self._convs = [_make_converter() for _ in range(self.size)]
                self._init = True
            return self._convs[mp.current_process()._identity[0] % self.size]


_converter_pool = _ConverterPool()


def extract_pdf_concurrent(
    pdf: pathlib.Path,
    txt_dir: pathlib.Path,
    json_dir: pathlib.Path,
    *,
    overwrite: bool = False,
    ocr_lang: str = "eng",
    keep_markup: bool = True,
    docling_only: bool = False,
    stats: Counter | None = None,
    enrich_llm: bool = True,
) -> pathlib.Path:
    conv = _converter_pool.get()
    return extract_pdf(
        pdf,
        txt_dir,
        json_dir,
        conv,
        overwrite=overwrite,
        ocr_lang=ocr_lang,
        keep_markup=keep_markup,
        docling_only=docling_only,
        stats=stats,
        enrich_llm=enrich_llm,
    )


# async batch driver ------------------------------------------------
async def extract_batch_async(
    pdfs: List[pathlib.Path],
    *,
    overwrite: bool = False,
    keep_markup: bool = True,
    ocr_lang: str = "eng",
    enrich_llm: bool = True,
    max_workers: Optional[int] = None,
) -> List[pathlib.Path]:
    from .acceleration_utils import hardware  # local vendored import

    stats = Counter()
    results: List[pathlib.Path] = []

    with hardware.get_process_pool(max_workers) as executor:
        fut2pdf = {
            executor.submit(
                extract_pdf_concurrent,
                pdf,
                TXT_DIR,
                JSON_DIR,
                overwrite=overwrite,
                ocr_lang=ocr_lang,
                keep_markup=keep_markup,
                enrich_llm=enrich_llm,
                stats=stats,
            ): pdf
            for pdf in pdfs
        }
        for fut in tqdm(
            as_completed(fut2pdf), total=len(fut2pdf), desc="Extracting PDFs"
        ):
            pdf = fut2pdf[fut]
            try:
                results.append(fut.result())
            except Exception as exc:
                log.error("extract failed for %s → %s", pdf.name, exc)

    log.info(
        "Extraction complete: %d processed, %d OCR pages",
        stats["processed"],
        stats["ocr_pages"],
    )
    return results


# one-off CLI -------------------------------------------------------
def run_one(pdf: pathlib.Path, *, overwrite: bool = False) -> pathlib.Path:
    conv = _make_converter()
    return extract_pdf(
        pdf,
        TXT_DIR,
        JSON_DIR,
        conv,
        overwrite=overwrite,
        enrich_llm=True,
    )


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("pdf", type=pathlib.Path)
    p.add_argument("--overwrite", action="store_true")
    args = p.parse_args()
    out = run_one(args.pdf, overwrite=args.overwrite)
    print("→", out) 