# scripts/graph_rag_stages/phase2_building/ner/extractor_util.py
from pathlib import Path

_ALIASES = {
    "legal_document": "ordinance",  # or split into ordinance/resolution based on filename
    "legal": "ordinance",
    "ordinance": "ordinance",
    "resolution": "resolution",
    "agenda": "agenda",
    "transcript": "verbatim_transcript",
    "verbatim_transcript": "verbatim_transcript",
}

def canonical_doc_type(meta: dict) -> str:
    raw = (meta.get("document_type") or "").lower().strip()
    if raw in _ALIASES:
        return _ALIASES[raw]
    name_blob = " ".join([
        str(meta.get("document") or ""),
        str(meta.get("source_file_name") or meta.get("Source_File_Name") or ""),
        str(meta.get("source_file_path") or meta.get("Source_File_Path") or "")
    ]).lower()
    if "resolution" in name_blob: return "resolution"
    if "ordinance"  in name_blob: return "ordinance"
    if "agenda"     in name_blob: return "agenda"
    if "verbatim" in name_blob or "transcript" in name_blob:
        return "verbatim_transcript"
    return "verbatim_transcript"

def infer_doc_type(meta: dict) -> str:
    """
    Returns a canonical doc_type for the extractor config.
    Looks at multiple fields to be resilient to empty/incorrect metadata.
    """
    return canonical_doc_type(meta)
