# scripts/graph_rag_stages/phase2_building/ner/simple_graph_builder.py
"""
Compat shim: the old NER 'simple_graph_builder' is deprecated.
Phase 5 (CustomGraphBuilder.push_from_merged_manifests) is now the only
place we build/push the graph. This shim is a no-op to satisfy imports.
"""
import logging
log = logging.getLogger(__name__)

class SimpleGraphBuilder:
    def __init__(self, *args, **kwargs):
        log.warning(
            "Deprecated SimpleGraphBuilder used by NER. This is a no-op shim. "
            "Graph building happens in Stage 5 via CustomGraphBuilder."
        )

    async def build(self, *args, **kwargs):
        log.info("SimpleGraphBuilder shim: .build() skipped (no-op).")
        return {"built": False, "note": "shim"}

    async def build_complete_graph(self, *args, **kwargs):
        log.info("SimpleGraphBuilder shim: .build_complete_graph() skipped (no-op).")
        return {"built": False, "note": "shim"}
