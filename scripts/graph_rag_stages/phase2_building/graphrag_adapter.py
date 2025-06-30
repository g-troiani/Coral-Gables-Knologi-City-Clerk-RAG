# scripts/graph_rag_stages/phase2_building/graphrag_adapter.py
"""
Builds GraphRAG-ready CSV + settings.yaml from enriched markdown
produced by agenda/document extractors.

Key features
------------
• auto-discovers *.md in <markdown_dir>
• extracts rich header metadata (title, document_type, meeting_date, …)
• emits input/city_clerk_documents.csv in <output_dir>/input/
• deep-merge override support for settings.yaml
• validation helpers with length / missing-text diagnostics
"""

from __future__ import annotations

import json, logging
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml

from scripts.graph_rag_stages.common.utils import (
    extract_metadata_from_header,
    ensure_directory_exists,
)

log = logging.getLogger(__name__)


class GraphRAGAdapter:
    # ────────────────────────────────────────────────────────────
    # public API
    # ────────────────────────────────────────────────────────────
    def create_graphrag_input_csv(self, md_dir: Path, out_dir: Path) -> Path:
        """
        Scan *md_dir* for enriched markdown → build city_clerk_documents.csv
        
        Returns Path to CSV (…/input/city_clerk_documents.csv)
        """
        ensure_directory_exists(out_dir)
        in_dir = out_dir / "input"
        ensure_directory_exists(in_dir)

        docs: List[Dict[str, Any]] = []
        md_files = sorted(md_dir.glob("*.md"))
        log.info("📄 %d markdown files discovered in %s", len(md_files), md_dir)
        
        # Track processing statistics
        processed = 0
        skipped = 0
        errors = []

        for md in md_files:
            try:
                content = md.read_text("utf-8")
                
                # Skip empty files
                if not content.strip():
                    log.warning("⚠️ Skipping empty file: %s", md.name)
                    skipped += 1
                    continue
                    
                meta = extract_metadata_from_header(content)
                
                # Ensure we have minimum required fields
                doc_id = md.stem
                if not doc_id:
                    log.warning("⚠️ Skipping file with no stem: %s", md.name)
                    skipped += 1
                    continue

                docs.append({
                    "id": doc_id,
                    "text": content,
                    "title": meta.get("title", md.stem.replace("_", " ").title()),
                    "document_type": self._doc_type(md, meta),
                    "meeting_date": meta.get("meeting_date", meta.get("date", "")),
                    "source_file": md.name,
                } | ({"agenda_item": meta["agenda_item"]} if "agenda_item" in meta else {}))
                
                processed += 1
                
            except Exception as exc:
                error_msg = f"❌ Failed to parse {md.name}: {exc}"
                log.error(error_msg)
                errors.append(error_msg)
                skipped += 1

        # Log processing summary
        log.info("📊 Processing Summary:")
        log.info("  - Total files found: %d", len(md_files))
        log.info("  - Successfully processed: %d", processed)
        log.info("  - Skipped/failed: %d", skipped)
        
        if errors:
            log.error("❌ Errors encountered:")
            for err in errors[:10]:  # Show first 10 errors
                log.error("  %s", err)
            if len(errors) > 10:
                log.error("  ... and %d more errors", len(errors) - 10)

        if not docs:
            raise RuntimeError("No markdown documents found – adapter can't proceed.")

        df = pd.DataFrame(docs)
        csv_path = in_dir / "city_clerk_documents.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8")
        log.info("✅ CSV written → %s  (%d rows)", csv_path, len(df))

        # Quick stats
        for k, v in df["document_type"].value_counts().items():
            log.info("   • %s: %d", k, v)

        return csv_path

    def create_graphrag_settings(
        self, out_dir: Path, custom: Dict[str, Any] | None = None
    ) -> Path:
        """
        Emit GraphRAG settings.yaml in *out_dir* (deep-merge defaults+custom).
        """
        base = _DEFAULT_SETTINGS.copy()
        if custom:
            base = self._merge(base, custom)

        yaml_path = out_dir / "settings.yaml"
        yaml_path.write_text(yaml.dump(base, indent=2), "utf-8")
        log.info("⚙️  settings.yaml written → %s", yaml_path)
        return yaml_path

    def validate_input_data(self, csv: Path) -> bool:
        """Basic sanity checks for generated CSV."""
        try:
            df = pd.read_csv(csv)
        except Exception as exc:
            log.error("CSV read failed → %s", exc)
            return False

        missing_cols = [c for c in ("id", "text") if c not in df.columns]
        if missing_cols:
            log.error("Missing required cols: %s", missing_cols)
            return False

        log.info(
            "📊 length stats → avg:%d  med:%d  min:%d  max:%d",
            df["text"].str.len().mean(),
            df["text"].str.len().median(),
            df["text"].str.len().min(),
            df["text"].str.len().max(),
        )
        if (df["text"].str.len() < 100).any():
            log.warning("⚠️ very short documents detected (<100 chars)")

        return True

    # ────────────────────────────────────────────────────────────
    # internal helpers
    # ────────────────────────────────────────────────────────────
    @staticmethod
    def _doc_type(md: Path, meta: Dict[str, Any]) -> str:
        if "document_type" in meta:
            return str(meta["document_type"]).lower()
        fn = md.name.lower()
        for tag in ("agenda", "minutes", "ordinance", "resolution", "transcript"):
            if tag in fn:
                return tag
        return "document"

    @staticmethod
    def _merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
        out = a.copy()
        for k, v in b.items():
            out[k] = GraphRAGAdapter._merge(out[k], v) if k in out and isinstance(out[k], dict) else v
        return out


# ──────────────────────────────────────────────────────────────
# default settings template
# ──────────────────────────────────────────────────────────────
_DEFAULT_SETTINGS: Dict[str, Any] = {
    "llm": {
        "api_key": "${OPENAI_API_KEY}",
        "type": "openai_chat",
        "model": "gpt-4",
        "temperature": 0.0,
        "max_tokens": 4000,
    },
    "encoding_model": "cl100k_base",
    "async_mode": "threaded",
    "parallelization": {"stagger": 0.3, "num_threads": 4},
    "chunks": {"size": 1200, "overlap": 100, "group_by_columns": ["source_file"]},
    "input": {
        "type": "file",
        "file_type": "csv",
        "base_dir": "input",
        "source_column": "text",
        "timestamp_column": "meeting_date",
        "timestamp_format": "%m.%d.%Y",
        "text_column": "text",
        "title_column": "title",
    },
    "cache": {"type": "file", "base_dir": "cache"},
    "storage": {"type": "file", "base_dir": "output"},
    "reporting": {"type": "file", "base_dir": "output/reports"},
    "snapshots": {"embeddings": False, "transient": False},
} 