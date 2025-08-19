"""
Extended EntityDeduplicator with multi-source support.
This extends the existing deduplicator to handle both NER and taxonomy sources.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any, Optional
from collections import defaultdict
import hashlib
import re
import os

from scripts.graph_rag_stages.common.graph_entity_toolkit import GraphEntityToolkit
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
from scripts.graph_rag_stages.common.standards import ensure_min_document_props, ensure_min_entity_props
from scripts.graph_rag_stages.common.standards import make_policy_id

log = logging.getLogger(__name__)

# Import debug flags from main pipeline
try:
    from scripts.graph_rag_stages.main_pipeline import DEBUG_ENTITY_DEDUPLICATION, DEBUG_RELATIONSHIP_LINKING
except ImportError:
    # Fallback if main_pipeline is not available
    DEBUG_ENTITY_DEDUPLICATION = False
    DEBUG_RELATIONSHIP_LINKING = False

class EntityDeduplicatorExtended:
    """Extended deduplicator that handles multiple sources."""
        _TYPE_MAP = {t.lower(): t for t in UnifiedOntology.get_entity_categories()}

    def _canon_type(self, t: Optional[str]) -> Optional[str]:
        if not t:
            return t
        return self._TYPE_MAP.get(str(t).lower(), t)

    def _preferred_policy_id(self, entity: Dict) -> Optional[str]:
        # Single source of truth
        return EntityIDStandards.preferred_policy_id(entity)

    def _preferred_agendaitem_id(self, entity: Dict) -> Optional[str]:
        # Single source of truth
        return EntityIDStandards.preferred_agendaitem_id(entity)

    def __init__(self, similarity_threshold: float = 0.85):
        """
        Initialize deduplicator.

        Args:
            similarity_threshold: Minimum similarity for merging (0-1)
        """
        self.similarity_threshold = similarity_threshold
        self.toolkit = GraphEntityToolkit()
        self.merge_map = {}  # old_id -> canonical_id
        self.entity_groups = defaultdict(list)  # canonical_id -> [entities]

    async def deduplicate_multi_source(self,
                                      ner_dir: Path,
                                      registry_dir: Path) -> Dict[str, str]:
        """
        Deduplicate across NER and taxonomy sources.

        Args:
            ner_dir: Directory with NER extracted entities
            registry_dir: Directory with taxonomy entities

        Returns:
            Merge map: {old_id: canonical_id}
        """
log.info("🔄 Starting multi-source deduplication")
log.info(f"Deduplicating {len(entities)} {entity_type} entities")
log.info(f"✅ Created merge map with {len(self.merge_map)} mappings")

                    # Create-correct-at-origin ethos: do NOT rewrite types here.
                    # If upstream emitted non-canonical types, warn so we can fix at origin.
                    if etype == 'Meeting':
                        log.warning("Create-at-origin policy: encountered type 'Meeting' in %s; upstream should emit 'Event'. Leaving unchanged.", json_file.name)
                    if etype == 'Topic' and str(entity.get('category','')).lower() in {'meeting section','agenda_section'}:
                        log.warning("Create-at-origin policy: encountered Topic{category=agenda_section} in %s; upstream should emit 'Section'. Leaving unchanged.", json_file.name)

                    # Normalize ID fields AFTER final etype decision (no in-method retagging)
                    entity = EntityIDStandards.normalize_entity_id_fields(entity, etype)
                    # Ensure entity has the right ID field present
                    id_field = EntityIDStandards.get_id_field(etype)
                    if id_field not in entity and 'id' in entity:
                        entity[id_field] = entity['id']

                    entities_by_type[etype].append(entity)

            except Exception as e:
                log.error(f"Error loading {json_file}: {e}")

        # Helper: infer entity_type from an aggregated file (e.g., Person.json)
        def _infer_entity_type_from_file(json_file: Path) -> Optional[str]:
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                et = data.get("entity_type") or data.get("type")
                if isinstance(et, str) and et.strip():
                    return et.strip()
            except Exception:
                pass
            stem = json_file.stem
            return stem[:1].upper() + stem[1:] if stem else None

        # Iterate through entity type directories **and** support aggregated files in base_dir
        for entity_dir in base_dir.iterdir():
            if not entity_dir.is_dir():
                # Support aggregated per-type files directly under base_dir (e.g., Person.json)
                if entity_dir.suffix.lower() == ".json":
                    inferred_type = _infer_entity_type_from_file(entity_dir)
                    if inferred_type:
                        _process_entity_file(entity_dir, inferred_type)
                    else:
                        log.warning("Skipping aggregated entity file with unknown type: %s", entity_dir.name)
                continue
            # skip non-entity buckets in root
            if entity_dir.name in {"relationships", "registry", "merged", "document_chunks", "indices"}:
                continue

            if entity_dir.name == "entities":
                # Walk one more level: entities/<Type>/*.json **and** aggregated files (entities/Person.json)
                for typed_dir in entity_dir.iterdir():
                    # Aggregated per-type file under 'entities' (e.g., entities/Person.json)
                    if typed_dir.is_file() and typed_dir.suffix.lower() == ".json":
                        inferred_type = _infer_entity_type_from_file(typed_dir)
                        if inferred_type:
                            _process_entity_file(typed_dir, inferred_type)
                        else:
                            log.warning("Skipping aggregated entity file with unknown type: %s", typed_dir.name)
                        continue
                    if not typed_dir.is_dir():
                        continue
                    # Skip potential indices or other non-entity subdirs
                    if typed_dir.name in {"indices", "merged"}:
                        continue
                    entity_type = typed_dir.name
                    for json_file in typed_dir.glob("*.json"):
                        _process_entity_file(json_file, entity_type)
                # Done with ./entities container; continue to next top-level dir
                continue

            entity_type = entity_dir.name

            # Load all JSON files in this entity directory
            for json_file in entity_dir.glob("*.json"):
                _process_entity_file(json_file, entity_type)

        # Optional compact debug summary (counts + a few IDs per type)
        try:
            if DEBUG_ENTITY_DEDUPLICATION :
                debug_dir = base_dir / "debug"
                debug_dir.mkdir(parents=True, exist_ok=True)
                summary = {"source": source_label, "root": str(base_dir), "by_type": {}}
                total = 0
                for etype, ents in entities_by_type.items():
                    id_field = EntityIDStandards.get_id_field(etype)
                    ids = []
                    for e in ents[:10]:
                        ids.append(e.get("id") or e.get(id_field))
                    # AgendaItem completeness (helps spot ID/date/code issues fast)
                    ag_stats = None
                    if etype == "AgendaItem":
                        have_both = sum(1 for e in ents if (e.get("itemID") or e.get("code")) and (e.get("meetingDate") or e.get("meeting_date")))
                        ag_stats = {"count": len(ents), "have_item_code_and_meeting_date": have_both}
                    summary["by_type"][etype] = {
                        "count": len(ents),
                        "sample_ids": [i for i in ids if i],
                        **({"agenda_item_stats": ag_stats} if ag_stats else {})
                    }
                    total += len(ents)
                summary["total_loaded"] = total
                with open(debug_dir / f"load_{source_label}.json", "w", encoding="utf-8") as f:
                    json.dump(summary, f, indent=2)
        except Exception:
            pass

        return dict(entities_by_type)

    async

        def entity_score(entity):
            score = 0

            # Source priority
            sources = entity.get('_sources', [])
            if any('taxonomy' in s for s in sources):
                score += 1000
            elif any('seed' in s for s in sources):
                score += 500

            # Prefer new naming patterns for canonical IDs
            eid = str(entity.get('id') or '')
            if re.match(r'^policy_(ordinance|resolution)_\d{4}_\d+_[0-9a-f]{8}$', eid):
                score += 200
            # Strongly prefer date-based AgendaItem IDs (agenda_item_E4_2024_01_09)
            if re.match(r'^agenda_item_[A-Z]\d+_\d{4}_\d{2}_\d{2}$', eid):
                score += 220
            # Still give some credit to legacy hash-based IDs to avoid regressions
            if re.match(r'^agendaitem_[A-Z]\d+_[0-9a-f]{8}$', eid):
                score += 120

            # Completeness (non-null attributes)
            for key, value in entity.items():
                if not key.startswith('_') and value is not None:
                    score += 1

            return score

        return max(group, key=entity_score)

    def _apply_id_naming_upgrades(self, entities_by_type: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
        out: Dict[str, List[Dict]] = {}
        for etype, ents in entities_by_type.items():
            etype_canon = self._canon_type(etype)
            id_field = EntityIDStandards.get_id_field(etype_canon)
            bucket: Dict[str, Dict] = {}
            for e in ents:
                # normalize type on the entity itself
                e['type'] = self._canon_type(e.get('type') or etype_canon)
                cur_id = e.get('id') or e.get(id_field)
                new_id = None
                if etype_canon == 'Policy':
                    new_id = self._preferred_policy_id(e)
                elif etype_canon == 'AgendaItem':
                    new_id = self._preferred_agendaitem_id(e)
                # If we can compute a preferred new id and it differs, map & rewrite
                target_id = new_id if (new_id and new_id != cur_id) else cur_id
                if not target_id:
                    # skip entities without any usable ID
                    continue
                if target_id != cur_id:
                    self.merge_map[cur_id] = target_id
                    e['id'] = target_id
                    e[id_field] = target_id
                # Collapse duplicates under the same target_id
                if target_id in bucket:
                    bucket[target_id] = self.toolkit.merge_entities(bucket[target_id], e)
                else:
                    bucket[target_id] = e
            out[etype_canon] = list(bucket.values())
        return out

    async def generate_merge_manifest(self, output_dir: Path) -> None:
        """
        Generate merged entity and relationship manifests.

        Args:
            output_dir: Directory to write merged manifests
        """
        merged_dir = Path(output_dir) / "merged"
        entities_dir = merged_dir / "entities"
        entities_dir.mkdir(parents=True, exist_ok=True)

        log.info("📝 Generating merged manifests")

        # Process entities by type (pre-merge groups formed earlier)
        entities_by_type = defaultdict(list)

        for canonical_id, group in self.entity_groups.items():
            if not group:
                continue

            # Merge all entities in group
            merged = group[0].copy()
            for entity in group[1:]:
                merged = self.toolkit.merge_entities(merged, entity)

            # Determine entity type (canonicalized)
            entity_type = self._canon_type(merged.get('type'))
            if not entity_type:
                # Try to infer from ID field
                for etype in ['Person', 'Organization', 'Document', 'Policy',
                            'Event', 'Location', 'AgendaItem', 'Asset',
                            'Project', 'Role', 'Topic', 'Contract',
                            'Technology', 'VoteOutcome']:
                    id_field = EntityIDStandards.get_id_field(etype)
                    if id_field in merged:
                        entity_type = self._canon_type(etype)
                        break
            if not entity_type:
                # Fallback: infer from any present ID value
                any_id = (
                    merged.get('id') or merged.get('documentID') or merged.get('policyID') or
                    merged.get('agendaItemID') or merged.get('personID') or merged.get('orgID') or
                    merged.get('eventID') or merged.get('locationID') or merged.get('sectionID')
                )
                if any_id:
                    try:
                        entity_type = EntityIDStandards.infer_type_from_id(str(any_id))
                    except Exception:
                        entity_type = None

            # Last-mile guard: if it has a documentID, treat it as a Document
            if not entity_type and (merged.get('documentID') or merged.get('document_id')):
                entity_type = 'Document'

            if entity_type:
                # normalize the in-entity type as well
                merged['type'] = entity_type
                # NEW: Pad ontology attributes for all entity types
                try:
                    ensure_min_entity_props(merged, entity_type)
                except Exception:
                    pass
                # Keep the existing Document minimums logic
                if entity_type == 'Document' or merged.get('documentID'):
                    ensure_min_document_props(merged)
                entities_by_type[entity_type].append(merged)

        # --- New: Upgrade IDs to the preferred naming and collapse duplicates ---
        entities_by_type = self._apply_id_naming_upgrades(entities_by_type)

        # Save merged entities by type
        for entity_type, entities in entities_by_type.items():
            filepath = entities_dir / f"{entity_type}.json"

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump({
                    "entity_type": entity_type,
                    "count": len(entities),
                    "entities": entities,
                    "_metadata": {
                        "merge_timestamp": self._get_timestamp(),
                        "source_counts": self._count_sources(entities)
                    }
                }, f, indent=2, ensure_ascii=False)

            log.info(f"  Saved {len(entities)} {entity_type} entities")

        # Process relationships (uses merge_map including our renames)
        await self._merge_relationships(output_dir, merged_dir)

        # Save merge map
        merge_map_file = merged_dir / "merge_map.json"
        with open(merge_map_file, 'w', encoding='utf-8') as f:
            json.dump({
                "mappings": self.merge_map,
                "statistics": {
                    "total_mappings": len(self.merge_map),
                    "canonical_entities": len(self.entity_groups)
                },
                "timestamp": self._get_timestamp()
            }, f, indent=2, ensure_ascii=False)

        log.info(f"✅ Merged manifests saved to {merged_dir}")

    async def _merge_relationships(self, source_dir: Path, merged_dir: Path) -> None:
        """
        Merge relationships and update IDs based on merge map.

        Args:
            source_dir: Source directory with NER/taxonomy data
            merged_dir: Output directory for merged data
        """
        all_relationships = []

        # Load relationships from NER
        ner_rel_dir = source_dir / "relationships"

                    cleaned = []
                    skipped = 0
                    for rel in relationships:
                        if not isinstance(rel, dict):
                            skipped += 1
                            continue

                        if '_source' not in rel:
                            rel['_source'] = f"ner_{rel_file.stem}"

                        # align older payloads that might use 'properties'
                        if 'attributes' not in rel and 'properties' in rel:
                            rel['attributes'] = rel.pop('properties')
                        # normalize relationship type to canonical
                        rel['type'] = normalize_rel_label((rel.get('type') or "").strip())

                        # ensure attributes is a dict (prevents crashes later)
                        if rel.get('attributes') is not None and not isinstance(rel.get('attributes'), dict):
                            rel['attributes'] = {}

                        cleaned.append(rel)

                    if skipped:
                        log.warning("Skipped %d malformed relationship entries in %s", skipped, rel_file.name)

                    all_relationships.extend(cleaned)

                    if isinstance(data, dict):
                        relationships = data.get('relationships', [])
                    elif isinstance(data, list):
                        relationships = data
                    else:
                        log.warning("Skipping %s: unexpected JSON type %s", rel_file.name, type(data).__name__)
                        relationships = []

                    cleaned = []
                    skipped = 0
                    for rel in relationships:
                        if not isinstance(rel, dict):
                            skipped += 1
                            continue

                        if '_source' not in rel:
                            rel['_source'] = f"taxonomy_{rel_file.stem}"

                        if 'attributes' not in rel and 'properties' in rel:
                            rel['attributes'] = rel.pop('properties')

                        rel_type_str = str(rel.get('type') or "").strip()
                        rel['type'] = normalize_rel_label(rel_type_str)

                        if rel.get('attributes') is not None and not isinstance(rel.get('attributes'), dict):
                            rel['attributes'] = {}

                        cleaned.append(rel)

                    if skipped:
                        log.warning("Skipped %d malformed relationship entries in %s", skipped, rel_file.name)

                    all_relationships.extend(cleaned)
                except Exception as e:
                    log.error(f"Error loading relationships from {rel_file}: {e}")

        # Update relationship IDs based on merge map
        updated_relationships = []
        seen_edges = set()
        # canonical set from the merged entities just written to disk
        canonical_ids = set()
        entities_dir = merged_dir / "entities"
        if entities_dir.exists():
            for f in entities_dir.glob("*.json"):
                try:
                    with open(f, "r", encoding="utf-8") as fh:
                        data = json.load(fh)
                    etype = f.stem
                    id_field = EntityIDStandards.get_id_field(etype)
                    for ent in data.get("entities", []):
                        eid = ent.get("id") or ent.get(id_field)
                        if eid:
                            canonical_ids.add(eid)
                except Exception:
                    pass
        rewired_edges = 0
        by_type = defaultdict(int)
        unresolved_by_type = defaultdict(int)
        attrs_nonempty_by_type = defaultdict(int)
        attrs_keys_sum_by_type = defaultdict(int)
        unresolved_edges = []
        unresolved_seen = set()

        def _resolve_canonical(eid):
            """Follow merge chain A->B->C until it stabilizes; protect against loops."""
            if not eid:
                return None
            seen = set()
            cur = eid
            while cur in self.merge_map and cur not in seen:
                seen.add(cur)
                cur = self.merge_map[cur]
            return cur

        # allow toggling unresolved-edge retention (defaults to keep)
        keep_unresolved = os.getenv("MERGE_KEEP_UNRESOLVED_EDGES", "true").lower() in ("1", "true", "yes")

        for rel in all_relationships:
            # Rewire endpoints using transitive merge resolution
            source_id_original = rel.get('source')
            target_id_original = rel.get('target')
            source_id = _resolve_canonical(source_id_original)
            target_id = _resolve_canonical(target_id_original)
            if source_id != source_id_original or target_id != target_id_original:
                rewired_edges += 1
            rel['source'] = source_id
            rel['target'] = target_id

            # If an endpoint isn't present in the canonical set, keep the edge,
            # but mark it as unresolved so downstream can decide how to handle it.
            unresolved = []
            if source_id not in canonical_ids:
                unresolved.append(source_id)
            if target_id not in canonical_ids:
                unresolved.append(target_id)
            if unresolved:
                rel.setdefault('_notes', {})
                rel['_notes']['unresolved_endpoints'] = unresolved
                key = (source_id, rel.get('type'), target_id)
                if key not in unresolved_seen:
                    unresolved_seen.add(key)
                    unresolved_edges.append({
                        "source": source_id,
                        "type": rel.get('type'),
                        "target": target_id,
                        "_source": rel.get('_source')
                    })
                # optionally drop unresolved edges so the graph push doesn't choke
                if not keep_unresolved:
                    continue

            # Strip volatile attrs before edge ID
            attrs = rel.get('attributes') or {}
            if not isinstance(attrs, dict):
                attrs = {}
            for k in list(attrs.keys()):
                if k.startswith('Source_') or k.startswith('_') or k in {'created_at','_created_at','timestamp'}:
                    attrs.pop(k, None)
            rel['attributes'] = attrs

            # Generate edge ID for deduplication (post-rewire)
            edge_id = self.toolkit.generate_edge_id(
                rel['source'],
                rel['type'],
                rel['target'],
                rel.get('attributes', {})
            )

            # Skip duplicate edges
            if edge_id in seen_edges:
                continue

            seen_edges.add(edge_id)
            rel['_edge_id'] = edge_id
            updated_relationships.append(rel)
            # stats
            rtype = rel.get('type') or 'UNKNOWN'
            by_type[rtype] += 1
            if rel.get('_notes', {}).get('unresolved_endpoints'):
                unresolved_by_type[rtype] += 1
            ak = len(rel.get('attributes') or {})
            if ak > 0:
                attrs_nonempty_by_type[rtype] += 1
            attrs_keys_sum_by_type[rtype] += ak

        # Save merged relationships
        rel_file = merged_dir / "relationships.json"
        with open(rel_file, 'w', encoding='utf-8') as f:
            json.dump({
                "count": len(updated_relationships),
                "relationships": updated_relationships,
                "_metadata": {
                    "merge_timestamp": self._get_timestamp(),
                    "duplicate_edges_removed": len(all_relationships) - len(updated_relationships),
                    "rewired_edges": rewired_edges,
                    "unresolved_edges": len(unresolved_edges),
                    "by_type": by_type,
                    "unresolved_by_type": unresolved_by_type,
                    "attributes_nonempty_by_type": attrs_nonempty_by_type,
                    "avg_attribute_keys_by_type": {
                        k: (attrs_keys_sum_by_type[k] / by_type[k]) if by_type[k] else 0.0
                        for k in by_type
                    }
                }
            }, f, indent=2, ensure_ascii=False)

        # Sidecar report with unresolved endpoints for follow-up (optional placeholder creation)
        if unresolved_edges:
            sidecar = merged_dir / "relationships_unresolved.json"
            with open(sidecar, 'w', encoding='utf-8') as f:
                json.dump({
                    "count": len(unresolved_edges),
                    "edges": unresolved_edges,
                    "_metadata": {
                        "note": "These edges reference IDs not present in the canonical entity set. Consider creating placeholders or improving extraction for these IDs."
                    }
                }, f, indent=2, ensure_ascii=False)

        log.info(f"  Saved {len(updated_relationships)} relationships "
                 f"(removed {len(all_relationships) - len(updated_relationships)} duplicates, "
                 f"rewired {rewired_edges}, unresolved {len(unresolved_edges)})")

    def _count_sources(self, entities: List[Dict]) -> Dict[str, int]:
        """Count entities by source."""
        source_counts = defaultdict(int)
        for entity in entities:
            sources = entity.get('_sources', [])
            for source in sources:
                if 'taxonomy' in source:
                    source_counts['taxonomy'] += 1
                elif 'ner' in source:
                    source_counts['ner'] += 1
                elif 'seed' in source:
                    source_counts['seed'] += 1
                else:
                    source_counts['other'] += 1
        return dict(source_counts)

    def _get_timestamp(self) -> str:
        """Get current timestamp."""
        from datetime import datetime
        return datetime.now().isoformat()
