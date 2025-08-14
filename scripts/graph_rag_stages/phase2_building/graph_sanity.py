# scripts/graph_rag_stages/phase2_building/graph_sanity.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Set, List
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards

def sanity_check(merged_dir: Path) -> Dict[str, int]:
    """
    Quick checks before graph push:
      - every entity has its required ID field
      - relationship types are known to the ontology
      - relationship endpoints exist in the merged entity set
    Returns simple counts you can log.
    """
    merged_dir = Path(merged_dir)
    ent_root = merged_dir / "entities"
    rel_path = merged_dir / "relationships.json"

    # 1) Load all canonical entity IDs (by entity type)
    ids_by_type: Dict[str, Set[str]] = {}
    total_entities = 0
    missing_id_field = 0

    for f in (ent_root.glob("*.json") if ent_root.exists() else []):
        entity_type = f.stem
        id_field = EntityIDStandards.get_id_field(entity_type)
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        for e in data.get("entities", []):
            total_entities += 1
            eid = e.get(id_field) or e.get("id")
            if not eid:
                missing_id_field += 1
                continue
            ids_by_type.setdefault(entity_type, set()).add(eid)

    # flatten canonical set (we only check membership)
    all_ids: Set[str] = set().union(*ids_by_type.values()) if ids_by_type else set()

    # 2) Load relationships and validate basic rules
    total_relationships = 0
    unknown_rel_type = 0
    missing_endpoints = 0

    if rel_path.exists():
        data = json.loads(rel_path.read_text(encoding="utf-8"))
        for r in data.get("relationships", []):
            total_relationships += 1
            rtype = r.get("type")
            if rtype not in UnifiedOntology.RELATIONSHIP_DEFINITIONS:
                unknown_rel_type += 1
            src = r.get("source")
            tgt = r.get("target")
            if src not in all_ids or tgt not in all_ids:
                missing_endpoints += 1

    return {
        "entities": total_entities,
        "relationships": total_relationships,
        "entity_missing_id": missing_id_field,
        "relationships_unknown_type": unknown_rel_type,
        "relationships_missing_endpoints": missing_endpoints,
    }