"""
Entity deduplication module for merging duplicate nodes
"""
import logging
import hashlib
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict
from difflib import SequenceMatcher
import re
from pathlib import Path
import json

log = logging.getLogger(__name__)


class EntityDeduplicator:
    """Handles entity deduplication across chunks and documents."""
    
    def __init__(self, similarity_threshold: float = 0.85):
        self.similarity_threshold = similarity_threshold
        self.entity_registry = defaultdict(dict)  # {entity_type: {normalized_key: entity_data}}
        self.merge_mappings = {}  # {old_id: new_id}
        
    def normalize_entity_name(self, name: str, entity_type: str) -> str:
        """Normalize entity name for comparison."""
        if not name:
            return ""
            
        # Lowercase and strip
        normalized = name.lower().strip()
        
        # Remove titles for Person entities
        if entity_type == "Person":
            titles = [
                'commissioner', 'mayor', 'vice mayor', 'city manager', 
                'city attorney', 'city clerk', 'mr.', 'ms.', 'mrs.', 'dr.',
                'councilmember', 'council member', 'honorable', 'hon.'
            ]
            for title in titles:
                normalized = normalized.replace(title, '').strip()
                
        # Remove common organizational suffixes
        elif entity_type == "Organization":
            suffixes = ['inc.', 'inc', 'llc', 'corp', 'corporation', 'department', 'dept']
            for suffix in suffixes:
                normalized = re.sub(f'\\b{suffix}\\b', '', normalized).strip()
                
        # Remove punctuation
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        
        # Collapse multiple spaces
        normalized = ' '.join(normalized.split())
        
        return normalized
    
    def calculate_similarity(self, name1: str, name2: str, entity_type: str) -> float:
        """Calculate similarity between two entity names."""
        norm1 = self.normalize_entity_name(name1, entity_type)
        norm2 = self.normalize_entity_name(name2, entity_type)
        
        # Direct match after normalization
        if norm1 == norm2:
            return 1.0
            
        # Token-based matching for names
        if entity_type == "Person":
            tokens1 = set(norm1.split())
            tokens2 = set(norm2.split())
            
            # If all tokens from shorter name are in longer name
            if tokens1.issubset(tokens2) or tokens2.issubset(tokens1):
                return 0.9
                
        # Sequence matching
        return SequenceMatcher(None, norm1, norm2).ratio()
    
    def find_duplicate_candidates(self, entity: Dict, entity_type: str) -> List[Tuple[str, float]]:
        """Find potential duplicate entities."""
        candidates = []
        entity_name = entity.get('name', '')
        
        if not entity_name:
            return candidates
            
        # Look through existing entities of same type
        for existing_id, existing_data in self.entity_registry[entity_type].items():
            existing_name = existing_data.get('name', '')
            
            similarity = self.calculate_similarity(entity_name, existing_name, entity_type)
            
            if similarity >= self.similarity_threshold:
                candidates.append((existing_id, similarity))
                
        # Sort by similarity score
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates
    
    def merge_entity_properties(self, primary: Dict, secondary: Dict) -> Dict:
        """Merge properties from secondary entity into primary."""
        merged = primary.copy()
        
        # Merge simple properties (prefer non-null, non-empty values)
        for key, value in secondary.items():
            if key not in merged or not merged[key]:
                merged[key] = value
            elif key == 'chunk_ids' and isinstance(value, list):
                # Merge chunk IDs
                existing = set(merged.get(key, []))
                existing.update(value)
                merged[key] = list(existing)
                
        return merged
    
    def _get_entity_id_field(self, entity_type: str) -> str:
        """Get the appropriate ID field for an entity type."""
        # Map entity types to their ID fields
        id_mapping = {
            'Person': 'personID',
            'Organization': 'orgID',
            'Document': 'documentID',
            'Policy': 'policyID',
            'Event': 'eventID',
            'Action': 'actionID',
            'Asset': 'assetID',
            'Project': 'projectID',
            'Location': 'locationID',
            'Role': 'roleID',
            'Topic': 'topicID',
            'AgendaItem': 'agendaItemID',
            'Contract': 'contractID',
            'Technology': 'techID',
            'VoteOutcome': 'outcomeID'
        }
        return id_mapping.get(entity_type, 'id')
    
    async def deduplicate_extracted_entities(self, extraction_dir: Path) -> Dict[str, int]:
        """Process extracted entities and create deduplication mappings."""
        stats = {'total_entities': 0, 'duplicates_found': 0, 'entities_merged': 0}
        
        # Load all entities first
        log.info("Loading entities for deduplication...")
        
        for entity_type_dir in extraction_dir.iterdir():
            if not entity_type_dir.is_dir() or entity_type_dir.name in ['document_chunks', 'relationships']:
                continue
                
            entity_type = entity_type_dir.name
            
            for entity_file in entity_type_dir.glob("*.json"):
                try:
                    with open(entity_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Handle both dictionary and list formats
                    entities = []
                    
                    if isinstance(data, dict):
                        # Expected format: {"entities": [...], "chunk_id": ...}
                        entities = data.get('entities', [])
                    elif isinstance(data, list):
                        # Alternative format: direct list of entities
                        entities = data
                    else:
                        log.warning(f"Unexpected data format in {entity_file}: {type(data)}")
                        continue
                    
                    # Process each entity
                    for entity in entities:
                        if not isinstance(entity, dict):
                            log.warning(f"Skipping non-dict entity in {entity_file}")
                            continue
                            
                        stats['total_entities'] += 1
                        
                        # Get entity ID using appropriate field
                        id_field = self._get_entity_id_field(entity_type)
                        entity_id = entity.get(id_field) or entity.get('id')
                        
                        if not entity_id:
                            log.warning(f"Entity without ID in {entity_file}: {entity}")
                            continue
                        
                        # Check for duplicates
                        candidates = self.find_duplicate_candidates(entity, entity_type)
                        
                        if candidates:
                            # Merge with best match
                            best_match_id, similarity = candidates[0]
                            existing = self.entity_registry[entity_type][best_match_id]
                            
                            # Merge properties
                            merged = self.merge_entity_properties(existing, entity)
                            self.entity_registry[entity_type][best_match_id] = merged
                            
                            # Track mapping
                            if entity_id != best_match_id:
                                self.merge_mappings[entity_id] = best_match_id
                                stats['duplicates_found'] += 1
                                
                            log.debug(f"Merged {entity_type} '{entity.get('name')}' -> '{existing.get('name')}' (similarity: {similarity:.2f})")
                        else:
                            # New unique entity
                            self.entity_registry[entity_type][entity_id] = entity
                            
                except json.JSONDecodeError as e:
                    log.error(f"Failed to parse JSON file {entity_file}: {e}")
                except Exception as e:
                    log.error(f"Error processing {entity_file}: {e}")
                    
        log.info(f"Deduplication complete: {stats['duplicates_found']} duplicates found out of {stats['total_entities']} entities")
        return stats
    
    def update_relationships_with_mappings(self, relationships: List[Dict]) -> List[Dict]:
        """Update relationship source/target IDs based on merge mappings."""
        updated_relationships = []
        
        for rel in relationships:
            updated_rel = rel.copy()
            
            # Update source if it was merged
            if rel.get('source') in self.merge_mappings:
                updated_rel['source'] = self.merge_mappings[rel['source']]
                
            # Update target if it was merged
            if rel.get('target') in self.merge_mappings:
                updated_rel['target'] = self.merge_mappings[rel['target']]
                
            updated_relationships.append(updated_rel)
            
        return updated_relationships
    
    async def apply_deduplication_to_ner_output(self, extraction_dir: Path) -> None:
        """Apply deduplication mappings back to the NER output files."""
        log.info("Applying deduplication mappings to NER output...")
        
        # Update entity files
        for entity_type_dir in extraction_dir.iterdir():
            if not entity_type_dir.is_dir() or entity_type_dir.name in ['document_chunks', 'relationships']:
                continue
                
            entity_type = entity_type_dir.name
            
            for entity_file in entity_type_dir.glob("*.json"):
                updated = False
                
                with open(entity_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Handle both formats
                if isinstance(data, dict) and 'entities' in data:
                    entities = data['entities']
                    
                    # Update entity IDs
                    for entity in entities:
                        id_field = self._get_entity_id_field(entity_type)
                        entity_id = entity.get(id_field) or entity.get('id')
                        
                        if entity_id in self.merge_mappings:
                            new_id = self.merge_mappings[entity_id]
                            if id_field in entity:
                                entity[id_field] = new_id
                            if 'id' in entity:
                                entity['id'] = new_id
                            updated = True
                    
                    if updated:
                        with open(entity_file, 'w', encoding='utf-8') as f:
                            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Update relationship files
        rel_dir = extraction_dir / "relationships"
        if rel_dir.exists():
            for rel_file in rel_dir.glob("*.json"):
                with open(rel_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if isinstance(data, dict) and 'relationships' in data:
                    updated_rels = self.update_relationships_with_mappings(data['relationships'])
                    
                    if updated_rels != data['relationships']:
                        data['relationships'] = updated_rels
                        with open(rel_file, 'w', encoding='utf-8') as f:
                            json.dump(data, f, indent=2, ensure_ascii=False)
        
        log.info("Deduplication mappings applied to NER output files") 