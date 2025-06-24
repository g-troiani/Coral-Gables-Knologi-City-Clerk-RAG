"""
LLM-assisted agenda extractor.

• One Groq call for ALL metadata / items.
• Extracts hyperlinks with PyMuPDF and associates them with items.
• Caches by file-hash; emits enriched Markdown + JSON sidecar.
"""
from __future__ import annotations

import asyncio, hashlib, json, logging, re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import fitz  # PyMuPDF

from ..common.utils import (
    get_llm_client,
    clean_json_response,
    extract_json_with_llm,
    sanitize_filename,
)
from ..helpers.pdf_extractor import PDFExtractor

log = logging.getLogger(__name__)


class AgendaExtractor:
    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.pdf_extractor = PDFExtractor()
        self.llm = get_llm_client()
        self.model = "llama-3.3-70b-versatile"
        self._cache: dict[str, Dict] = {}

    # ── orchestrator ──────────────────────────────────────────────
    async def extract_and_save(self, pdf: Path) -> None:
        h = self._hash(pdf)
        if h in self._cache:
            data = self._cache[h]
        else:
            data = await self._process_pdf(pdf)
            self._cache[h] = data

        self._save_markdown(pdf, data)
        self._save_json(pdf, data["metadata"])

    # ── internals ────────────────────────────────────────────────
    async def _process_pdf(self, pdf: Path) -> Dict:
        full, _ = self.pdf_extractor.extract_text_from_pdf(pdf)
        meta_json = await extract_json_with_llm(self.llm, full, self.model)

        items = await self._extract_items_llm(full)
        links = self._links(pdf)
        self._link_items(items, links)
        meeting = self._meeting_info(pdf, full)

        return {
            "source_file": pdf.name,
            "doc_id": self._doc_id(pdf),
            "full_text": full,
            "agenda_items": items,
            "hyperlinks": links,
            "meeting_info": meeting,
            "metadata": {
                **meta_json,
                "extraction_method": "docling+llm+pymupdf",
                "num_items": len(items),
                "num_links": len(links),
                "extracted": datetime.utcnow().isoformat() + "Z",
            },
        }

    # ---------- LLM helpers --------------------------------------
    async def _extract_items_llm(self, text: str) -> List[Dict]:
        prompt = (
            "Extract ALL agenda items (code, title, section_name, "
            "document_reference, item_type) as JSON array."
        )
        messages = [
            {"role": "system", "content": prompt},
            {"role": "user", "content": text[:15000]},
        ]
        rsp = await clean_json_response(self.llm, messages, model=self.model)
        if not isinstance(rsp, list):
            rsp = [rsp] if rsp else []
        doc_hash = hashlib.sha1(text[:1000].encode()).hexdigest()[:12]
        for idx, it in enumerate(rsp):
            it["id"] = f"ITEM_{doc_hash}_{idx:03d}"
            it.setdefault("urls", [])
        return rsp

    # ---------- link extraction ----------------------------------
    @staticmethod
    def _links(pdf: Path) -> List[Dict]:
        out: List[Dict] = []
        doc = fitz.open(str(pdf))
        for pn in range(len(doc)):
            for ln in doc[pn].get_links():
                if ln.get("uri"):
                    rect = fitz.Rect(ln["from"])
                    txt = doc[pn].get_text(clip=rect).strip()
                    out.append(
                        {
                            "url": ln["uri"],
                            "text": txt or "link",
                            "page": pn + 1,
                        }
                    )
        return out

    @staticmethod
    def _link_items(items: List[Dict], links: List[Dict]) -> None:
        for lk in links:
            t = lk["text"].upper()
            for it in items:
                if (code := it.get("item_code")) and code in t:
                    it["urls"].append(lk)
                    break

    # ---------- misc helpers -------------------------------------
    @staticmethod
    def _meeting_info(pdf: Path, text: str) -> Dict:
        info = {"date": "N/A", "time": "N/A", "location": "N/A"}
        if m := re.search(r"(\d{2})\.(\d{2})\.(\d{4})", pdf.name):
            info["date"] = ".".join(m.groups())
        for ln in text.split("\n")[:50]:
            ln_low = ln.lower()
            if "city hall" in ln_low or "commission chamber" in ln_low:
                info["location"] = ln.strip()[:100]
            if m := re.search(r"(\d{1,2}:\d{2}\s*[AP]M)", ln, re.I):
                info["time"] = m.group(1)
        return info

    @staticmethod
    def _doc_id(pdf: Path) -> str:
        return "DOC_" + hashlib.sha1(str(pdf).encode()).hexdigest()[:12]

    @staticmethod
    def _hash(pdf: Path) -> str:
        return hashlib.md5(pdf.read_bytes()).hexdigest()

    # ---------- output writers -----------------------------------
    def _save_markdown(self, pdf: Path, data: Dict) -> None:
        md = self._header(data) + self._items_md(data) + "\n\n# FULL TEXT\n\n" + data["full_text"]
        fn = f"agenda_{data['meeting_info']['date'].replace('.','_')}.md"
        (self.out_dir / fn).write_text(md, "utf-8")
        log.info("📝 markdown → %s", fn)

    def _save_json(self, pdf: Path, meta: Dict) -> None:
        fn = f"agenda_{meta.get('date', 'unknown').replace('.','_')}.json"
        out = self.out_dir.parent / "json" / fn
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(meta, indent=2, ensure_ascii=False))
        log.info("💾 json → %s", fn)

    # ---------- markdown building -------------------------------
    @staticmethod
    def _header(data: Dict) -> str:
        mi = data["meeting_info"]
        codes = [it["item_code"] for it in data["agenda_items"] if it.get("item_code")]
        return f"""---
DOCUMENT METADATA
=================
- Document Type: AGENDA
- Meeting Date: {mi['date']}
- Meeting Time: {mi['time']}
- Location: {mi['location']}

**Agenda items present:** {', '.join(codes[:15])}{' …' if len(codes) > 15 else ''}
---

"""

    @staticmethod
    def _items_md(data: Dict) -> str:
        md = ["## AGENDA ITEMS\n"]
        for it in data["agenda_items"]:
            code = it.get("item_code", "UNKNOWN")
            md.append(f"### Item {code}\n{it.get('title', 'N/A')}\n")
            for url in it.get("urls", []):
                md.append(f"- [{url['text']}]({url['url']})  (p.{url['page']})")
            md.append("")
        return "\n".join(md) 