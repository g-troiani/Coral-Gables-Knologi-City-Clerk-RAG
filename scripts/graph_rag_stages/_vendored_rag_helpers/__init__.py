"""
Vendored copies of helper stages that originally lived in the top-level
`RAG_stages` package.  They are re-namespaced here so the legacy tree can
stay frozen while Graph-RAG evolves independently.

Public surface:

    from graph_rag_stages._vendored_rag_helpers import (
        extract_clean,
        embed_vectors,
        acceleration_utils,
        llm_enrich,
        db_upsert,
    )
"""
from importlib import import_module

__all__ = [
    "extract_clean",
    "embed_vectors",
    "acceleration_utils",
    "llm_enrich",
    "db_upsert",
]

# Lazy-import sub-modules to keep import time low.
for _name in __all__:
    globals()[_name] = import_module(f"{__name__}.{_name}") 