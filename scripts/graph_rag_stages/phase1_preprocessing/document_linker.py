"""
Generic ordinance / resolution linker.

• Extracts full text via PDFExtractor.
• One LLM JSON call for metadata.
• Writes enriched Markdown plus JSON companion.
"""
from __future__ import annotations

import asyncio, hashlib, json, logging, re
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from ..common.utils import (
    get_llm_client,
    extract_json_with_llm,
    sanitize_filename,
)
from ..helpers.pdf_extractor import PDFExtractor

log = logging.getLogger(__name__)


class DocumentLinker:
    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.pdf_extractor = PDFExtractor()
        self.llm = get_llm_client()
        self.model = "llama-3.3-70b-versatile"

    # ── orchestrator ──────────────────────────────────────────────
    async def extract_and_save(self, pdf: Path) -> None:
        full, pages = self.pdf_extractor.extract_text_from_pdf(pdf)
        meta_json = await extract_json_with_llm(self.llm, full, self.model)

        doc_num = (
            re.search(r"(\d{4}-\d{2,})", pdf.name).group(1)
            if re.search(r"(\d{4}-\d{2,})", pdf.name)
            else pdf.stem
        )

        data = {
            "source_file": pdf.name,
            "doc_id": self._doc_id(pdf),
            "full_text": full,
            "metadata": {
                **meta_json,
                "document_number": doc_num,
                "num_pages": len(pages),
                "chars": len(full),
                "extracted": datetime.utcnow().isoformat() + "Z",
            },
        }

        self._save_markdown(pdf, data)
        self._save_json(pdf, data["metadata"])

    # ── utils -----------------------------------------------------
    @staticmethod
    def _doc_id(pdf: Path) -> str:
        return "DOC_" + hashlib.sha1(str(pdf).encode()).hexdigest()[:12]

    # ── writers ---------------------------------------------------
    def _save_markdown(self, pdf: Path, data: Dict) -> None:
        md = self._header(data) + "\n\n# CONTENT\n\n" + data["full_text"]
        fn = sanitize_filename(f"{data['metadata']['document_type']}_{data['metadata']['document_number']}.md")
        (self.out_dir / fn).write_text(md, "utf-8")
        log.info("📝 markdown → %s", fn)

    def _save_json(self, pdf: Path, meta: Dict) -> None:
        jdir = self.out_dir.parent / "json"
        jdir.mkdir(parents=True, exist_ok=True)
        fn = sanitize_filename(f"{meta['document_type']}_{meta['document_number']}.json")
        (jdir / fn).write_text(json.dumps(meta, indent=2, ensure_ascii=False))
        log.info("💾 json → %s", fn)

    @staticmethod
    def _header(data: Dict) -> str:
        m = data["metadata"]
        return f"""---
DOCUMENT METADATA
=================
- Type: {m.get('document_type', 'N/A')}
- Title: {m.get('title', data['source_file'])}
- Number: {m.get('document_number')}
- Date: {m.get('date', 'N/A')}
- Linked Agenda Items: {', '.join(x.get('item_code','') for x in m.get('agenda_items',[])) or 'N/A'}
---
""" 