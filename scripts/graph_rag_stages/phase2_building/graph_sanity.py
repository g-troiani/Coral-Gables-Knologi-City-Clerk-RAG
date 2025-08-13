# scripts/graph_rag_stages/phase2_building/graph_sanity.py
import json
from pathlib import Path
from collections import defaultdict

def _load_entities(merged_dir: Path):
    by_type = defaultdict(dict)
    ent_dir = Path(merged_dir) / "entities"
    for f in ent_dir.glob("*.json"):
        data = json.loads(f.read_text(encoding="utf-8"))
        et = data.get("entity_type") or f.stem
        id_field = {
            'Person':'personID','Organization':'orgID','Document':'documentID','Policy':'policyID',
            'Event':'eventID','Location':'locationID','AgendaItem':'agendaItemID','Section':'sectionID',
            'Role':'roleID','Topic':'topicID','Contract':'contractID','Technology':'technologyID',
            'VoteOutcome':'outcomeID','Asset':'assetID','Project':'projectID'
        }.get(et, 'id')
        for e in data.get("entities", []):
            eid = e.get(id_field) or e.get('id')
            if eid:
                by_type[et][eid] = e
    return by_type

def sanity_check(merged_dir: Path) -> dict:
    merged_dir = Path(merged_dir)
    ents = _load_entities(merged_dir)
    rels = json.loads((merged_dir / "relationships.json").read_text(encoding="utf-8"))
    rels = rels.get("relationships", [])

    present = {et: set(ents[et].keys()) for et in ents}
    violations = {"policy_no_doc": 0, "policy_no_event": 0, "item_no_section_link": 0}

    # quick sets
    hasDocument = set((r["source"], r["target"]) for r in rels if r.get("type")=="hasDocument")
    adoptedAt   = set((r["source"], r["target"]) for r in rels if r.get("type")=="adoptedAt")
    inSection   = set((r["source"], r["target"]) for r in rels if r.get("type")=="inSection")
    hasAgenda   = set((r["source"], r["target"]) for r in rels if r.get("type")=="hasAgendaItem")

    # Policies: hasDocument + adoptedAt
    for pid in present.get("Policy", []):
        if not any(src==pid for (src,_tgt) in hasDocument):
            violations["policy_no_doc"] += 1
        if not any(src==pid for (src,_tgt) in adoptedAt):
            violations["policy_no_event"] += 1

    # AgendaItems: either inSection OR (Section->hasAgendaItem)
    for aid in present.get("AgendaItem", []):
        linked = any(src==aid for (src,_tgt) in inSection) or any(tgt==aid for (src,tgt) in hasAgenda)
        if not linked:
            violations["item_no_section_link"] += 1

    return violations
