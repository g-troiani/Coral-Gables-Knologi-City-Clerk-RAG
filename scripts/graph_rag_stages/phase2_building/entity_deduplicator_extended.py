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

from scripts.graph_rag_stages.common.graph_entity_toolkit import GraphEntityToolkit
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards

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
    
    def _normalize_date_yyyymmdd(self, s: Optional[str]) -> str:
        if not s:
            return ""
        import re
        s = s.strip().replace("/", "-").replace(".", "-").replace("_", "-")
        m1 = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
        if m1:
            y,m,d = m1.groups()
            return f"{y}{m.zfill(2)}{d.zfill(2)}"
        m2 = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{4})$", s)
        if m2:
            m,d,y = m2.groups()
            return f"{y}{m.zfill(2)}{d.zfill(2)}"
        m3 = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{2})$", s)  # last-resort
        if m3:
            m,d,y2 = m3.groups()
            return f"20{y2}{m.zfill(2)}{d.zfill(2)}"
        return s
    
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
        if DEBUG_ENTITY_DEDUPLICATION:
            log.info("🧹 DEBUG [DEDUPLICATION] Starting multi-source deduplication")
            log.info(f"🧹 DEBUG [DEDUPLICATION] NER directory: {ner_dir}")
            log.info(f"🧹 DEBUG [DEDUPLICATION] Registry directory: {registry_dir}")
        
        log.info("🔄 Starting multi-source deduplication")
        
        # Load all entities from both sources
        all_entities = {}
        
        # Load NER entities
        ner_entities = await self._load_entities_from_dir(ner_dir, "ner")
        if DEBUG_ENTITY_DEDUPLICATION:
            ner_count = sum(len(entities) for entities in ner_entities.values())
            log.info(f"🧹 DEBUG [DEDUPLICATION] Loaded {ner_count} NER entities from {len(ner_entities)} types")
            for entity_type, entities in ner_entities.items():
                log.info(f"🧹 DEBUG [DEDUPLICATION]   {entity_type}: {len(entities)} entities")
        
        for entity_type, entities in ner_entities.items():
            if entity_type not in all_entities:
                all_entities[entity_type] = []
            all_entities[entity_type].extend(entities)
        
        # Load taxonomy entities
        taxonomy_entities = await self._load_entities_from_dir(registry_dir, "taxonomy")
        if DEBUG_ENTITY_DEDUPLICATION:
            taxonomy_count = sum(len(entities) for entities in taxonomy_entities.values())
            log.info(f"🧹 DEBUG [DEDUPLICATION] Loaded {taxonomy_count} taxonomy entities from {len(taxonomy_entities)} types")
            for entity_type, entities in taxonomy_entities.items():
                log.info(f"🧹 DEBUG [DEDUPLICATION]   {entity_type}: {len(entities)} entities")
        
        for entity_type, entities in taxonomy_entities.items():
            if entity_type not in all_entities:
                all_entities[entity_type] = []
            all_entities[entity_type].extend(entities)
        
        # Deduplicate each entity type
        total_before = sum(len(entities) for entities in all_entities.values())
        
        for entity_type, entities in all_entities.items():
            before_count = len(entities)
            if DEBUG_ENTITY_DEDUPLICATION:
                log.info(f"🧹 DEBUG [DEDUPLICATION] Processing {entity_type}: {before_count} entities before deduplication")
            
            log.info(f"Deduplicating {len(entities)} {entity_type} entities")
            await self._deduplicate_entity_type(entity_type, entities)
        
        log.info(f"✅ Created merge map with {len(self.merge_map)} mappings")
        return self.merge_map
    
    async def _load_entities_from_dir(self, base_dir: Path, 
                                     source_label: str) -> Dict[str, List[Dict]]:
        """
        Load all entities from a directory.
        
        Args:
            base_dir: Base directory containing entity subdirectories
            source_label: Label for source tracking
            
        Returns:
            Dict of entity_type -> list of entities
        """
        entities_by_type = defaultdict(list)
        
        if not base_dir.exists():
            log.warning(f"Directory not found: {base_dir}")
            return entities_by_type
        
        # Iterate through entity type directories
        for entity_dir in base_dir.iterdir():
            if not entity_dir.is_dir():
                continue
            # skip non-entity buckets in root
            if entity_dir.name in {"relationships", "registry", "merged", "document_chunks"}:
                continue
            
            entity_type = entity_dir.name
            
            # Load all JSON files in this entity directory
            for json_file in entity_dir.glob("*.json"):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Extract entities from file
                    file_entities = data.get('entities', [])
                    
                    # Add source tracking
                    for entity in file_entities:
                        if '_sources' not in entity:
                            entity['_sources'] = []
                        entity['_sources'].append(f"{source_label}_{json_file.stem}")
                        # keep a stable .type for downstream rules if missing
                        entity.setdefault('type', entity_type)
                        
                        # Ensure entity has the right ID field
                        id_field = EntityIDStandards.get_id_field(entity_type)
                        if id_field not in entity and 'id' in entity:
                            entity[id_field] = entity['id']
                        
                        entities_by_type[entity_type].append(entity)
                        
                except Exception as e:
                    log.error(f"Error loading {json_file}: {e}")
        
        return dict(entities_by_type)
    
    async def _deduplicate_entity_type(self, entity_type: str, 
                                      entities: List[Dict]) -> None:
        """Deduplicate entities of a specific type."""
        if not entities:
            return
        
        # Get ID field for this entity type
        id_field = EntityIDStandards.get_id_field(entity_type)
        
        # First pass: Group by normalized name
        name_groups = defaultdict(list)
        
        for entity in entities:
            entity_id = entity.get(id_field) or entity.get('id')
            if not entity_id:
                continue
            
            norm_key = self._get_normalization_key(entity, entity_type)
            name_groups[norm_key].append(entity)
        
        # Second pass: Check for XXX duplicates across groups
        xxx_merge_candidates = self._find_xxx_duplicates(entities, entity_type, id_field)
        
        # Merge XXX duplicates into existing groups
        for xxx_id, canonical_id in xxx_merge_candidates.items():
            # Find which group contains the xxx entity
            xxx_entity = None
            canonical_entity = None
            
            for entity in entities:
                eid = entity.get(id_field) or entity.get('id')
                if eid == xxx_id:
                    xxx_entity = entity
                elif eid == canonical_id:
                    canonical_entity = entity
            
            if xxx_entity and canonical_entity:
                # Add to merge map
                self.merge_map[xxx_id] = canonical_id
                
                # Add xxx entity to canonical's group
                canonical_key = self._get_normalization_key(canonical_entity, entity_type)
                if xxx_entity not in name_groups[canonical_key]:
                    name_groups[canonical_key].append(xxx_entity)
        
        # Continue with existing group processing...
        for norm_key, group in name_groups.items():
            if len(group) == 1:
                entity = group[0]
                entity_id = entity.get(id_field) or entity.get('id')
                self.entity_groups[entity_id] = [entity]
                continue
            
            canonical = self._select_canonical_entity(group)
            canonical_id = canonical.get(id_field) or canonical.get('id')
            
            for entity in group:
                entity_id = entity.get(id_field) or entity.get('id')
                if entity_id != canonical_id:
                    self.merge_map[entity_id] = canonical_id
            
            self.entity_groups[canonical_id] = group
    
    def _get_normalization_key(self, entity: Dict, entity_type: str) -> str:
        """
        Get normalized key for entity grouping.
        
        Args:
            entity: Entity dict
            entity_type: Entity type
            
        Returns:
            Normalized key string
        """
        # Enhanced Document normalization
        if entity_type == 'Document':
            return self._get_document_normalization_key(entity)
        elif entity_type == 'AgendaItem':
            code = (entity.get('itemID') or entity.get('agendaItemID') or "").lower().replace("-", "").replace("_", "")
            date_norm = self._normalize_date_yyyymmdd(entity.get('meeting_date') or entity.get('date') or "")
            return f"{code}|{date_norm}" if code or date_norm else entity.get(EntityIDStandards.get_id_field(entity_type), 'unknown')
        
        # Priority fields for normalization
        key_fields = {
            'Person': ['name'],
            'Organization': ['name'],
            'Document': ['title', 'documentID'],
            'Policy': ['title', 'policyID'],
            'AgendaItem': ['itemID', 'title'],
            'Event': ['name', 'dateTime'],
            'Location': ['name', 'address'],
            'Asset': ['name', 'assetID'],
            'Project': ['name', 'projectID'],
            'Role': ['title'],
            'Topic': ['name'],
            'Contract': ['contractID', 'title'],
            'Technology': ['name', 'vendor'],
            'VoteOutcome': ['agendaItemID', 'outcomeID']
        }
        
        fields = key_fields.get(entity_type, ['name'])
        
        # Build key from available fields
        key_parts = []
        for field in fields:
            if field in entity and entity[field]:
                value = str(entity[field]).lower().strip()
                # Normalize common variations
                value = value.replace(',', '').replace('.', '').replace('-', ' ')
                key_parts.append(value)
        
        if key_parts:
            return '|'.join(key_parts)
        
        # Fallback to entity ID
        id_field = EntityIDStandards.get_id_field(entity_type)
        return entity.get(id_field, 'unknown')
    
    def _get_document_normalization_key(self, entity: Dict) -> str:
        """Just extract the date and type, ignore everything else."""
        import re
        
        # Get any field that might have the info
        text = str(entity.get('documentID', '')) + str(entity.get('name', '')) + str(entity.get('title', ''))
        
        # Find a date
        date_match = re.search(r'(\d{1,2})[._\-](\d{1,2})[._\-](\d{4})', text)
        if date_match:
            m, d, y = date_match.groups()
            date_key = f"{y}{m.zfill(2)}{d.zfill(2)}"
        else:
            # also try YYYY_MM_DD
            date_match2 = re.search(r'(\d{4})[._\-](\d{1,2})[._\-](\d{1,2})', text)
            if date_match2:
                y, m, d = date_match2.groups()
                date_key = f"{y}{m.zfill(2)}{d.zfill(2)}"
            else:
                date_key = "unknown"
        
        # Find type
        if 'agenda' in text.lower():
            return f"agenda_{date_key}"
        elif 'ordinance' in text.lower():
            return f"ordinance_{date_key}"
        # etc...
        
        return f"doc_{date_key}"
    
    def _select_canonical_entity(self, group: List[Dict]) -> Dict:
        """
        Select the canonical entity from a group.
        Priority: taxonomy > ner, then most complete.
        
        Args:
            group: List of duplicate entities
            
        Returns:
            Selected canonical entity
        """
        # Sort by source priority and completeness
        def entity_score(entity):
            score = 0
            
            # Source priority
            sources = entity.get('_sources', [])
            if any('taxonomy' in s for s in sources):
                score += 1000
            elif any('seed' in s for s in sources):
                score += 500
            
            # Completeness (non-null attributes)
            for key, value in entity.items():
                if not key.startswith('_') and value is not None:
                    score += 1
            
            return score
        
        return max(group, key=entity_score)
    
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
        
        # Process entities by type
        entities_by_type = defaultdict(list)
        
        for canonical_id, group in self.entity_groups.items():
            if not group:
                continue
            
            # Merge all entities in group
            merged = group[0].copy()
            for entity in group[1:]:
                merged = self.toolkit.merge_entities(merged, entity)
            
            # Determine entity type
            entity_type = merged.get('type')
            if not entity_type:
                # Try to infer from ID field
                for etype in ['Person', 'Organization', 'Document', 'Policy', 
                            'Event', 'Location', 'AgendaItem', 'Asset', 
                            'Project', 'Role', 'Topic', 'Contract', 
                            'Technology', 'VoteOutcome']:
                    id_field = EntityIDStandards.get_id_field(etype)
                    if id_field in merged:
                        entity_type = etype
                        break
            
            if entity_type:
                entities_by_type[entity_type].append(merged)
        
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
        
        # Process relationships
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
        
        if DEBUG_RELATIONSHIP_LINKING:
            log.info("🔗 DEBUG [RELATIONSHIPS] Starting relationship merging")
            log.info(f"🔗 DEBUG [RELATIONSHIPS] Source directory: {source_dir}")
            log.info(f"🔗 DEBUG [RELATIONSHIPS] Merged directory: {merged_dir}")
        
        # Load relationships from NER
        ner_rel_dir = source_dir / "relationships"
        if DEBUG_RELATIONSHIP_LINKING:
            log.info(f"🔗 DEBUG [RELATIONSHIPS] Checking NER relationships: {ner_rel_dir}")
            log.info(f"🔗 DEBUG [RELATIONSHIPS] NER rel dir exists: {ner_rel_dir.exists()}")
        
        if ner_rel_dir.exists():
            ner_rel_files = list(ner_rel_dir.glob("*.json"))
            if DEBUG_RELATIONSHIP_LINKING:
                log.info(f"🔗 DEBUG [RELATIONSHIPS] Found {len(ner_rel_files)} NER relationship files")
                
            for rel_file in ner_rel_files:
                try:
                    with open(rel_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    relationships = data.get('relationships', [])
                    
                    if DEBUG_RELATIONSHIP_LINKING:
                        log.info(f"🔗 DEBUG [RELATIONSHIPS] {rel_file.name}: {len(relationships)} relationships")
                    
                    # Add source tracking
                    for rel in relationships:
                        if '_source' not in rel:
                            rel['_source'] = f"ner_{rel_file.stem}"
                    
                    all_relationships.extend(relationships)
                except Exception as e:
                    log.error(f"Error loading relationships from {rel_file}: {e}")
                    if DEBUG_RELATIONSHIP_LINKING:
                        log.error(f"🔗 DEBUG [RELATIONSHIPS] ❌ Failed to load {rel_file.name}: {e}")
        elif DEBUG_RELATIONSHIP_LINKING:
            log.warning(f"🔗 DEBUG [RELATIONSHIPS] ❌ NER relationships directory does not exist")
        
        # Load relationships from taxonomy
        tax_rel_dir = source_dir / "registry" / "relationships"
        if tax_rel_dir.exists():
            for rel_file in tax_rel_dir.glob("*.json"):
                try:
                    with open(rel_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    relationships = data.get('relationships', [])
                    
                    # Add source tracking
                    for rel in relationships:
                        if '_source' not in rel:
                            rel['_source'] = f"taxonomy_{rel_file.stem}"
                    
                    all_relationships.extend(relationships)
                except Exception as e:
                    log.error(f"Error loading relationships from {rel_file}: {e}")
        
        # Update relationship IDs based on merge map
        updated_relationships = []
        seen_edges = set()
        # canonical set includes singletons chosen as-is
        canonical_ids = set(self.entity_groups.keys())
        rewired_edges = 0
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
                    "unresolved_edges": len(unresolved_edges)
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
    
    def _find_xxx_duplicates(self, entities: List[Dict], entity_type: str, 
                             id_field: str) -> Dict[str, str]:
        """
        Find entities that are duplicates except for 'xxx' suffix.
        Returns mapping of xxx_id -> canonical_id
        """
        xxx_mappings = {}
        
        # Build lookup by ID
        entities_by_id = {}
        for entity in entities:
            eid = entity.get(id_field) or entity.get('id')
            if eid:
                entities_by_id[eid] = entity
        
        # Check each entity with 'xxx' in its ID
        for entity_id, entity in entities_by_id.items():
            if 'xxx' not in entity_id.lower():
                continue
            
            # Extract base ID without xxx
            base_id = self._extract_base_id(entity_id)
            if not base_id:
                continue
            
            # Look for matching entity without xxx
            for other_id, other_entity in entities_by_id.items():
                if other_id == entity_id or 'xxx' in other_id.lower():
                    continue
                
                # Check if this could be a match
                if self._is_xxx_duplicate(entity, other_entity, entity_type, base_id, other_id):
                    xxx_mappings[entity_id] = other_id
                    log.info(f"Found XXX duplicate: {entity_id} -> {other_id}")
                    break
        
        return xxx_mappings

    def _extract_base_id(self, entity_id: str) -> str:
        """
        Extract base ID without xxx suffix.
        Examples:
            'person_smith_xxx' -> 'person_smith'
            'agenda_item_e1_xxx' -> 'agenda_item_e1'
            'document_agenda_xxx_2024' -> 'document_agenda'
        """
        import re
        
        # Remove various xxx patterns
        patterns = [
            r'_xxx\d*$',  # _xxx or _xxx123 at end
            r'_xxx_',      # _xxx_ in middle
            r'xxx\d*$',    # xxx or xxx123 at end without underscore
        ]
        
        base_id = entity_id
        for pattern in patterns:
            base_id = re.sub(pattern, '', base_id)
        
        # Also try removing hash-like suffixes (6-8 alphanumeric chars)
        base_id = re.sub(r'_[a-f0-9]{6,8}$', '', base_id)
        
        return base_id if base_id != entity_id else None

    def _is_xxx_duplicate(self, xxx_entity: Dict, other_entity: Dict, 
                          entity_type: str, xxx_base_id: str, other_id: str) -> bool:
        """
        Check if xxx_entity is a duplicate of other_entity.
        Requires at least 2 matching fields for Documents, 1 for others.
        """
        matches = 0
        
        # Special handling for Documents - need type AND date match
        if entity_type == 'Document':
            # Check document type
            xxx_type = (xxx_entity.get('document_type') or 
                       xxx_entity.get('type') or '').lower()
            other_type = (other_entity.get('document_type') or 
                         other_entity.get('type') or '').lower()
            
            if xxx_type and other_type:
                # Both must be agenda, or both ordinance, etc.
                if xxx_type == other_type:
                    matches += 1
                elif 'agenda' in xxx_type and 'agenda' in other_type:
                    matches += 1
                elif 'ordinance' in xxx_type and 'ordinance' in other_type:
                    matches += 1
                elif 'resolution' in xxx_type and 'resolution' in other_type:
                    matches += 1
                elif 'transcript' in xxx_type and 'transcript' in other_type:
                    matches += 1
            
            # Check date match
            xxx_date = self._extract_date_from_entity(xxx_entity)
            other_date = self._extract_date_from_entity(other_entity)
            
            if xxx_date and other_date and xxx_date == other_date:
                matches += 1
            
            # For documents, require both type AND date (2 matches)
            return matches >= 2
        
        # For AgendaItems - check item code and meeting date
        elif entity_type == 'AgendaItem':
            # Check item code
            xxx_code = xxx_entity.get('itemID', '').lower().replace('-', '').replace('_', '')
            other_code = other_entity.get('itemID', '').lower().replace('-', '').replace('_', '')
            
            if xxx_code and other_code and xxx_code == other_code:
                matches += 1
            
            # Check meeting date
            xxx_date = xxx_entity.get('meeting_date', '')
            other_date = other_entity.get('meeting_date', '')
            
            if xxx_date and other_date:
                # Normalize dates for comparison
                xxx_date_norm = xxx_date.replace('.', '').replace('-', '').replace('_', '')
                other_date_norm = other_date.replace('.', '').replace('-', '').replace('_', '')
                if xxx_date_norm == other_date_norm:
                    matches += 1
            
            return matches >= 2
        
        # For Person/Organization - check name similarity
        elif entity_type in ['Person', 'Organization']:
            xxx_name = (xxx_entity.get('name', '') or '').lower().strip()
            other_name = (other_entity.get('name', '') or '').lower().strip()
            
            if xxx_name and other_name:
                # Remove common titles for comparison
                for title in ['commissioner', 'mayor', 'vice', 'mr', 'ms', 'mrs', 'dr']:
                    xxx_name = xxx_name.replace(title, '').strip()
                    other_name = other_name.replace(title, '').strip()
                
                # Check if names are similar enough
                if xxx_name == other_name:
                    return True
                
                # Check if one is substring of other (e.g., "smith" in "john smith")
                if xxx_name in other_name or other_name in xxx_name:
                    return True
        
        # For other entity types, check if base ID matches part of other ID
        else:
            # Generic check - does the base ID appear in the other ID?
            if xxx_base_id:
                xxx_base_clean = xxx_base_id.replace('_', '').lower()
                other_clean = other_id.replace('_', '').lower()
                
                if xxx_base_clean in other_clean or other_clean in xxx_base_clean:
                    # At least one other field should match
                    for field in ['name', 'title', 'type', 'status']:
                        if field in xxx_entity and field in other_entity:
                            if str(xxx_entity[field]).lower() == str(other_entity[field]).lower():
                                return True
        
        return False

    def _extract_date_from_entity(self, entity: Dict) -> Optional[str]:
        """Extract and normalize date from entity fields."""
        import re
        
        # Check various date fields
        date_fields = ['meeting_date', 'issueDate', 'dateTime', 'date', 'Date']
        
        for field in date_fields:
            if field in entity and entity[field]:
                date_str = str(entity[field])
                # Normalize to YYYYMMDD for comparison
                match = re.search(r'(\d{1,2})[._\-](\d{1,2})[._\-](\d{4})', date_str)
                if match:
                    m, d, y = match.groups()
                    return f"{y}{m.zfill(2)}{d.zfill(2)}"
                
                match = re.search(r'(\d{4})[._\-](\d{1,2})[._\-](\d{1,2})', date_str)
                if match:
                    y, m, d = match.groups()
                    return f"{y}{m.zfill(2)}{d.zfill(2)}"
        
        # Check in title/name
        text = str(entity.get('title', '')) + str(entity.get('name', ''))
        match = re.search(r'(\d{1,2})[._\-](\d{1,2})[._\-](\d{4})', text)
        if match:
            m, d, y = match.groups()
            return f"{y}{m.zfill(2)}{d.zfill(2)}"
        
        return None
    
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
