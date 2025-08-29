"""
Incremental entity merger that handles conflicts between existing and new entities.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any, Optional
from collections import defaultdict
from datetime import datetime

from .entity_deduplicator_extended import EntityDeduplicatorExtended
from ..common.entity_versioning import EntityVersionTracker
from ..common.cosmos_client import CosmosGraphClient

log = logging.getLogger(__name__)


class IncrementalEntityMerger(EntityDeduplicatorExtended):
    """
    Enhanced entity merger with incremental processing support.
    
    Extends EntityDeduplicatorExtended to add:
    - Smart property merging (union instead of replace)
    - Version tracking
    - Conflict resolution
    - Relationship preservation
    """
    
    def __init__(self, similarity_threshold: float = 0.95, cosmos_client: Optional[CosmosGraphClient] = None):
        """
        Initialize incremental merger with higher threshold for safety.
        
        Args:
            similarity_threshold: Higher threshold (0.95) for incremental merging
            cosmos_client: Optional Cosmos client for checking existing entities
        """
        super().__init__(similarity_threshold)
        self.cosmos_client = cosmos_client
        self.version_tracker = None
        self.merge_strategy = "conservative"  # conservative, aggressive, or custom
        self.conflict_log = []
    
    def set_version_tracker(self, version_dir: Path):
        """Set the version tracker for entity versioning."""
        self.version_tracker = EntityVersionTracker(version_dir)
    
    async def merge_with_existing(self, 
                                 new_entities: Dict[str, List[Dict]], 
                                 existing_entities: Dict[str, List[Dict]],
                                 processing_run_id: str) -> Tuple[Dict[str, List[Dict]], List[Dict]]:
        """
        Merge new entities with existing ones using smart conflict resolution.
        
        Args:
            new_entities: New entities from current processing run
            existing_entities: Existing entities from Cosmos/previous runs
            processing_run_id: ID of current processing run
            
        Returns:
            Tuple of (merged_entities, conflict_log)
        """
        merged = defaultdict(list)
        self.conflict_log = []
        
        # Process each entity type
        all_types = set(new_entities.keys()) | set(existing_entities.keys())
        
        for entity_type in all_types:
            log.info(f"🔄 Merging {entity_type} entities...")
            
            new_list = new_entities.get(entity_type, [])
            existing_list = existing_entities.get(entity_type, [])
            
            # Create lookup for existing entities by ID
            existing_by_id = {}
            for entity in existing_list:
                entity_id = self._get_entity_id(entity)
                if entity_id:
                    existing_by_id[entity_id] = entity
            
            # Process new entities
            for new_entity in new_list:
                new_id = self._get_entity_id(new_entity)
                if not new_id:
                    # No ID, just add as new
                    if self.version_tracker:
                        new_entity = self.version_tracker.add_entity_metadata(new_entity, processing_run_id)
                    merged[entity_type].append(new_entity)
                    continue
                
                if new_id in existing_by_id:
                    # Entity exists - merge
                    existing_entity = existing_by_id[new_id]
                    merged_entity = await self._merge_entity_pair(
                        existing_entity, new_entity, processing_run_id
                    )
                    merged[entity_type].append(merged_entity)
                    
                    # Remove from existing to track what's processed
                    del existing_by_id[new_id]
                else:
                    # New entity
                    if self.version_tracker:
                        new_entity = self.version_tracker.add_entity_metadata(new_entity, processing_run_id)
                        self.version_tracker.track_entity_change(None, new_entity, processing_run_id, "create")
                    merged[entity_type].append(new_entity)
            
            # Add remaining existing entities (not updated)
            for entity_id, entity in existing_by_id.items():
                merged[entity_type].append(entity)
        
        return dict(merged), self.conflict_log
    
    async def _merge_entity_pair(self, 
                               existing: Dict, 
                               new: Dict,
                               processing_run_id: str) -> Dict:
        """
        Merge a pair of entities with conflict resolution.
        
        Strategy:
        - Keep existing ID and core fields
        - Union list properties (e.g., roles, affiliations)
        - Update scalar properties if different
        - Preserve all relationships
        - Track version history
        """
        merged = existing.copy()
        
        # Track what changed
        changes = []
        
        # Handle list properties (union)
        list_props = ['roles', 'affiliations', 'agenda_items', 'speakers', 
                     'voting_records', 'related_documents', '_sources']
        
        for prop in list_props:
            if prop in new and prop in existing:
                # Union of lists, removing duplicates
                existing_vals = existing.get(prop, [])
                new_vals = new.get(prop, [])
                
                if isinstance(existing_vals, list) and isinstance(new_vals, list):
                    # Convert to sets for comparison (handle dicts by converting to strings)
                    existing_set = {json.dumps(v, sort_keys=True) if isinstance(v, dict) else v 
                                  for v in existing_vals}
                    new_set = {json.dumps(v, sort_keys=True) if isinstance(v, dict) else v 
                             for v in new_vals}
                    
                    if new_set - existing_set:
                        # New values to add
                        all_vals = existing_vals + [v for v in new_vals 
                                                   if (json.dumps(v, sort_keys=True) if isinstance(v, dict) else v) 
                                                   not in existing_set]
                        merged[prop] = all_vals
                        changes.append(f"Added {len(new_set - existing_set)} items to {prop}")
        
        # Handle scalar properties (update if different)
        scalar_props = ['name', 'title', 'description', 'summary', 'status', 
                       'department', 'address', 'email', 'phone']
        
        for prop in scalar_props:
            if prop in new and new[prop] != existing.get(prop):
                old_val = existing.get(prop)
                new_val = new[prop]
                
                # Apply merge strategy
                if self.merge_strategy == "conservative":
                    # Only update if existing is empty/null
                    if not old_val:
                        merged[prop] = new_val
                        changes.append(f"Set {prop} to '{new_val}'")
                elif self.merge_strategy == "aggressive":
                    # Always take new value
                    merged[prop] = new_val
                    changes.append(f"Updated {prop} from '{old_val}' to '{new_val}'")
                else:
                    # Custom logic - log conflict
                    self.conflict_log.append({
                        "entity_id": self._get_entity_id(existing),
                        "entity_type": existing.get('type'),
                        "property": prop,
                        "existing_value": old_val,
                        "new_value": new_val,
                        "resolution": "kept_existing"
                    })
        
        # Update metadata
        if self.version_tracker:
            merged = self.version_tracker.add_entity_metadata(merged, processing_run_id)
            self.version_tracker.track_entity_change(existing, merged, processing_run_id, "merge")
        
        # Log merge
        if changes:
            log.info(f"Merged {existing.get('type')} {self._get_entity_id(existing)}: {', '.join(changes)}")
        
        return merged
    
    def _get_entity_id(self, entity: Dict) -> Optional[str]:
        """Extract entity ID using parent method."""
        # Use the parent class method which handles all ID field variations
        return self.toolkit._get_entity_id(entity)
    
    async def load_existing_from_cosmos(self) -> Dict[str, List[Dict]]:
        """
        Load existing entities from Cosmos DB for comparison.
        
        Returns:
            Dictionary of entities by type
        """
        if not self.cosmos_client:
            log.warning("No Cosmos client available, cannot load existing entities")
            return {}
        
        existing = defaultdict(list)
        
        try:
            # Query all vertices grouped by label
            query = "g.V().group().by(label).by(valueMap(true).fold())"
            result = await self.cosmos_client._execute_query(query)
            
            if result and isinstance(result, list) and result[0]:
                groups = result[0]
                
                for label, entities in groups.items():
                    # Map Cosmos labels to entity types
                    entity_type = self._map_label_to_type(label)
                    
                    for entity_props in entities:
                        # Convert Cosmos properties to entity dict
                        entity = self._cosmos_props_to_entity(entity_props, entity_type)
                        existing[entity_type].append(entity)
            
            log.info(f"Loaded {sum(len(v) for v in existing.values())} existing entities from Cosmos")
            
        except Exception as e:
            log.error(f"Error loading existing entities from Cosmos: {e}")
        
        return dict(existing)
    
    def _map_label_to_type(self, label: str) -> str:
        """Map Cosmos vertex label to entity type."""
        # This should match the mapping in CosmosGraphOptimizer
        label_to_type = {
            'person': 'Person',
            'organization': 'Organization',
            'document': 'Document',
            'policy': 'Policy',
            'event': 'Event',
            'action': 'Action',
            'asset': 'Asset',
            'project': 'Project',
            'location': 'Location',
            'role': 'Role',
            'topic': 'Topic',
            'agendaitem': 'AgendaItem',
            'contract': 'Contract',
            'technology': 'Technology',
            'voteoutcome': 'VoteOutcome',
            'section': 'Section'
        }
        return label_to_type.get(label.lower(), label.title())
    
    def _cosmos_props_to_entity(self, props: Dict, entity_type: str) -> Dict:
        """Convert Cosmos property map to entity dictionary."""
        entity = {'type': entity_type}
        
        for key, values in props.items():
            if key == 'id' and values:
                # ID is special - single value
                entity['id'] = values[0] if isinstance(values, list) else values
            elif isinstance(values, list) and values:
                # Cosmos returns properties as lists
                if len(values) == 1:
                    # Single value - unwrap
                    try:
                        # Try to parse JSON strings
                        if isinstance(values[0], str) and values[0].startswith('['):
                            entity[key] = json.loads(values[0])
                        else:
                            entity[key] = values[0]
                    except:
                        entity[key] = values[0]
                else:
                    # Multiple values - keep as list
                    entity[key] = values
        
        return entity
    
    def get_merge_summary(self) -> Dict:
        """Get summary of merge operations."""
        return {
            "total_conflicts": len(self.conflict_log),
            "merge_strategy": self.merge_strategy,
            "conflicts_by_type": self._group_conflicts_by_type(),
            "conflicts_by_property": self._group_conflicts_by_property()
        }
    
    def _group_conflicts_by_type(self) -> Dict[str, int]:
        """Group conflicts by entity type."""
        by_type = defaultdict(int)
        for conflict in self.conflict_log:
            by_type[conflict.get('entity_type', 'unknown')] += 1
        return dict(by_type)
    
    def _group_conflicts_by_property(self) -> Dict[str, int]:
        """Group conflicts by property name."""
        by_prop = defaultdict(int)
        for conflict in self.conflict_log:
            by_prop[conflict.get('property', 'unknown')] += 1
        return dict(by_prop)
