"""
Extended EntityDeduplicator with multi-source support.
This extends the existing deduplicator to handle both NER and taxonomy sources.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any
from collections import defaultdict
import hashlib

from scripts.graph_rag_stages.common.graph_entity_toolkit import GraphEntityToolkit
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards

log = logging.getLogger(__name__)


class EntityDeduplicatorExtended:
    """Extended deduplicator that handles multiple sources."""
    
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
        
        # Load all entities from both sources
        all_entities = {}
        
        # Load NER entities
        ner_entities = await self._load_entities_from_dir(ner_dir, "ner")
        for entity_type, entities in ner_entities.items():
            if entity_type not in all_entities:
                all_entities[entity_type] = []
            all_entities[entity_type].extend(entities)
        
        # Load taxonomy entities
        taxonomy_entities = await self._load_entities_from_dir(registry_dir, "taxonomy")
        for entity_type, entities in taxonomy_entities.items():
            if entity_type not in all_entities:
                all_entities[entity_type] = []
            all_entities[entity_type].extend(entities)
        
        # Deduplicate each entity type
        for entity_type, entities in all_entities.items():
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
            if not entity_dir.is_dir() or entity_dir.name == "relationships":
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
        """
        Deduplicate entities of a specific type.
        
        Args:
            entity_type: Type of entities
            entities: List of entities to deduplicate
        """
        if not entities:
            return
        
        # Get ID field for this entity type
        id_field = EntityIDStandards.get_id_field(entity_type)
        
        # Group entities by normalized name for initial clustering
        name_groups = defaultdict(list)
        
        for entity in entities:
            # Get entity ID
            entity_id = entity.get(id_field) or entity.get('id')
            if not entity_id:
                continue
            
            # Get normalized key for grouping
            norm_key = self._get_normalization_key(entity, entity_type)
            name_groups[norm_key].append(entity)
        
        # Process each group
        for norm_key, group in name_groups.items():
            if len(group) == 1:
                # No duplicates
                entity = group[0]
                entity_id = entity.get(id_field) or entity.get('id')
                self.entity_groups[entity_id] = [entity]
                continue
            
            # Find canonical entity (prefer taxonomy source)
            canonical = self._select_canonical_entity(group)
            canonical_id = canonical.get(id_field) or canonical.get('id')
            
            # Create merge mappings
            for entity in group:
                entity_id = entity.get(id_field) or entity.get('id')
                if entity_id != canonical_id:
                    self.merge_map[entity_id] = canonical_id
            
            # Store group
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
        """
        Enhanced normalization for Document entities to better match agenda documents.
        
        Args:
            entity: Document entity
            
        Returns:
            Normalized key for grouping
        """
        import re
        
        # Get document name/title
        name = entity.get('name') or entity.get('title', '')
        doc_type = entity.get('document_type') or entity.get('type', '')
        
        if not name:
            return 'unknown_document'
        
        # Normalize document name
        normalized = name.lower().strip()
        
        # Remove common file extensions
        normalized = normalized.replace('.pdf', '').replace('.doc', '').replace('.docx', '')
        
        # Extract date pattern (01.09.2024, 01_09_2024, 01-09-2024)
        date_match = re.search(r'(\d{1,2})[._-](\d{1,2})[._-](\d{4})', normalized)
        date_part = ''
        if date_match:
            # Standardize date format
            day, month, year = date_match.groups()
            date_part = f"{day.zfill(2)}{month.zfill(2)}{year}"
        
        # Remove punctuation and standardize separators
        normalized = re.sub(r'[._-]+', ' ', normalized)
        normalized = re.sub(r'[^\w\s]', '', normalized)
        normalized = ' '.join(normalized.split())  # Normalize whitespace
        
        # Build normalized key: type + date + core_name
        key_parts = []
        
        # Add document type if available (normalize agenda/document types)
        if doc_type:
            # Normalize document type - treat "document" and "agenda" as equivalent for agenda docs
            normalized_type = doc_type.lower()
            if normalized_type == 'document' and 'agenda' in normalized.lower():
                normalized_type = 'agenda'
            elif normalized_type in ['agenda', 'document']:
                normalized_type = 'agenda'  # Standardize to 'agenda' for agenda documents
            key_parts.append(normalized_type)
        
        # Add standardized date
        if date_part:
            key_parts.append(date_part)
        
        # Add core document name (without date)
        if date_match:
            # Remove the original date from name
            core_name = re.sub(r'\d{1,2}[._-]\d{1,2}[._-]\d{4}', '', normalized).strip()
        else:
            core_name = normalized
        
        # Further normalize core name for agenda documents
        if core_name and 'agenda' in core_name:
            # Remove redundant words and standardize
            core_name = re.sub(r'\b(city|commission|meeting|final)\b', '', core_name).strip()
            core_name = re.sub(r'\s+', ' ', core_name).strip()  # Normalize whitespace
            if not core_name or core_name == 'agenda':
                core_name = 'agenda'
        
        if core_name:
            key_parts.append(core_name)
        
        return '|'.join(key_parts) if key_parts else 'unknown_document'
    
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
        
        # Load relationships from NER
        ner_rel_dir = source_dir / "relationships"
        if ner_rel_dir.exists():
            for rel_file in ner_rel_dir.glob("*.json"):
                try:
                    with open(rel_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    relationships = data.get('relationships', [])
                    
                    # Add source tracking
                    for rel in relationships:
                        if '_source' not in rel:
                            rel['_source'] = f"ner_{rel_file.stem}"
                    
                    all_relationships.extend(relationships)
                except Exception as e:
                    log.error(f"Error loading relationships from {rel_file}: {e}")
        
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
        
        for rel in all_relationships:
            # Update source and target IDs
            source_id = rel.get('source')
            target_id = rel.get('target')
            
            # Apply merge map
            if source_id in self.merge_map:
                rel['source'] = self.merge_map[source_id]
            if target_id in self.merge_map:
                rel['target'] = self.merge_map[target_id]
            
            # Generate edge ID for deduplication
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
                    "duplicate_edges_removed": len(all_relationships) - len(updated_relationships)
                }
            }, f, indent=2, ensure_ascii=False)
        
        log.info(f"  Saved {len(updated_relationships)} relationships (removed {len(all_relationships) - len(updated_relationships)} duplicates)")
    
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
