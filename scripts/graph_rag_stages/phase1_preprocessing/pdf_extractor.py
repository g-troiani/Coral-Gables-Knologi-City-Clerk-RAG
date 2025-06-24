# scripts/graph_rag_stages/phase1_preprocessing/pdf_extractor.py
"""
Thin wrapper around Docling PDF converter providing robust text extraction
for GraphRAG ingest.  Guarantees every page returns *some* text, writes a
tiny JSON debug stub for later provenance checks.
"""

from __future__ import annotations

import json, logging
from pathlib import Path
from typing import Dict, List, Tuple

from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption

log = logging.getLogger(__name__)


class PDFExtractor:
    def __init__(self, out_dir: Path | None = None) -> None:
        self.out_dir = out_dir or Path.cwd() / "temp_extraction_output"
        self.out_dir.mkdir(parents=True, exist_ok=True)

        pipeline = PdfPipelineOptions(do_ocr=True, do_table_structure=True)
        self.converter = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline)}
        )

    # ──────────────────────────────────────────────────────────
    def extract_text_from_pdf(self, pdf: Path) -> Tuple[str, List[Dict[str, any]]]:
        """Return full text + per-page list[{'text', 'page_num'}]."""
        log.info("📑 Docling extracting %s", pdf.name)

        try:
            bundle = self.converter.convert(str(pdf))
            doc = bundle.document
        except Exception as exc:
            log.error("❌ Docling failed %s → %s", pdf.name, exc)
            return "", []

        full = doc.export_to_markdown() or ""
        pages: List[Dict[str, any]] = []

        if getattr(doc, "pages", None):
            for idx, pg in enumerate(doc.pages, 1):
                txt = getattr(pg, "text", "") or getattr(pg, "get_text", lambda: "")()
                if not txt and getattr(pg, "elements", None):
                    txt = "\n".join(e.text for e in pg.elements if getattr(e, "text", ""))
                if not txt:
                    txt = "(blank page)"  # ensure something
                pages.append({"text": txt, "page_num": idx})

        if not pages and full:
            pages = [{"text": full, "page_num": 1}]

        self._debug_write(pdf, len(pages), len(full))
        log.info("✅ extracted %d pages (%d chars total)", len(pages), len(full))
        return full, pages

    # ──────────────────────────────────────────────────────────
    def _debug_write(self, pdf: Path, n_pages: int, n_chars: int) -> None:
        dbg = {
            "file": pdf.name,
            "total_pages": n_pages,
            "total_characters": n_chars,
        }
        dbg_path = self.out_dir / f"{pdf.stem}_extract_debug.json"
        dbg_path.write_text(json.dumps(dbg, indent=2), "utf-8") 