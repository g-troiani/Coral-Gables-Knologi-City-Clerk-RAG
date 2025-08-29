"""
Entity versioning system for tracking entity changes over time.
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set, Any
from collections import defaultdict

log = logging.getLogger(__name__)


class EntityVersionTracker:
    """
    Tracks entity versions and changes for incremental processing.
    
    Maintains version history for entities to support:
    - Conflict detection
    - Change tracking
    - Rollback capabilities
    - Audit trails
    """
    
    def __init__(self, version_dir: Path):
        """Initialize the entity version tracker."""
        self.version_dir = Path(version_dir)
        self.version_dir.mkdir(parents=True, exist_ok=True)
        self.version_file = self.version_dir / "entity_versions.json"
        self.history_file = self.version_dir / "entity_history.json"
        self._ensure_files_exist()
    
    def _ensure_files_exist(self):
        """Create version tracking files if they don't exist."""
        if not self.version_file.exists():
            self._save_versions({
                "version": "1.0",
                "created_at": datetime.now().isoformat(),
                "entities": {},
                "last_update": None
            })
        
        if not self.history_file.exists():
            self._save_history({
                "version": "1.0",
                "created_at": datetime.now().isoformat(),
                "changes": []
            })
    
    def _load_versions(self) -> Dict:
        """Load entity version data."""
        try:
            with open(self.version_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            log.warning("Version file corrupted or missing, creating new one")
            self._ensure_files_exist()
            with open(self.version_file, 'r') as f:
                return json.load(f)
    
    def _save_versions(self, data: Dict):
        """Save entity version data."""
        with open(self.version_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def _load_history(self) -> Dict:
        """Load entity change history."""
        try:
            with open(self.history_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            log.warning("History file corrupted or missing, creating new one")
            self._ensure_files_exist()
            with open(self.history_file, 'r') as f:
                return json.load(f)
    
    def _save_history(self, data: Dict):
        """Save entity change history."""
        with open(self.history_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def add_entity_metadata(self, entity: Dict, processing_run_id: str) -> Dict:
        """
        Add version metadata to an entity.
        
        Args:
            entity: Entity dictionary
            processing_run_id: ID of the current processing run
            
        Returns:
            Entity with added version metadata
        """
        entity_id = self._get_entity_id(entity)
        if not entity_id:
            return entity
        
        versions = self._load_versions()
        entity_key = f"{entity.get('type', 'unknown')}:{entity_id}"
        
        # Check if entity exists
        if entity_key in versions["entities"]:
            # Existing entity - increment version
            current_version = versions["entities"][entity_key]
            new_version = current_version["version"] + 1
            
            # Add version metadata
            entity["_version"] = new_version
            entity["_created_at"] = current_version["created_at"]
            entity["_updated_at"] = datetime.now().isoformat()
            entity["_processing_runs"] = current_version.get("processing_runs", []) + [processing_run_id]
        else:
            # New entity
            entity["_version"] = 1
            entity["_created_at"] = datetime.now().isoformat()
            entity["_updated_at"] = datetime.now().isoformat()
            entity["_processing_runs"] = [processing_run_id]
        
        return entity
    
    def track_entity_change(self, 
                          old_entity: Optional[Dict], 
                          new_entity: Dict,
                          processing_run_id: str,
                          change_type: str = "update"):
        """
        Track an entity change in version history.
        
        Args:
            old_entity: Previous version of entity (None for new entities)
            new_entity: New version of entity
            processing_run_id: ID of the current processing run
            change_type: Type of change (create, update, merge)
        """
        entity_id = self._get_entity_id(new_entity)
        if not entity_id:
            return
        
        entity_type = new_entity.get('type', 'unknown')
        entity_key = f"{entity_type}:{entity_id}"
        
        # Update version tracking
        versions = self._load_versions()
        
        if entity_key not in versions["entities"]:
            versions["entities"][entity_key] = {
                "id": entity_id,
                "type": entity_type,
                "version": 1,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
                "processing_runs": [processing_run_id]
            }
        else:
            entity_info = versions["entities"][entity_key]
            entity_info["version"] += 1
            entity_info["updated_at"] = datetime.now().isoformat()
            entity_info["processing_runs"].append(processing_run_id)
        
        versions["last_update"] = datetime.now().isoformat()
        self._save_versions(versions)
        
        # Record change in history
        history = self._load_history()
        change_record = {
            "timestamp": datetime.now().isoformat(),
            "processing_run_id": processing_run_id,
            "entity_id": entity_id,
            "entity_type": entity_type,
            "change_type": change_type,
            "version": versions["entities"][entity_key]["version"],
            "changes": self._compute_changes(old_entity, new_entity) if old_entity else {}
        }
        
        history["changes"].append(change_record)
        
        # Keep only last 1000 changes
        if len(history["changes"]) > 1000:
            history["changes"] = history["changes"][-1000:]
        
        self._save_history(history)
    
    def _get_entity_id(self, entity: Dict) -> Optional[str]:
        """Extract entity ID from entity dictionary."""
        # Try various ID field names
        id_fields = ['id', 'entityID', 'documentID', 'personID', 'organizationID', 
                    'policyID', 'eventID', 'agendaItemID', 'locationID']
        
        for field in id_fields:
            if field in entity and entity[field]:
                return str(entity[field])
        
        # Try type-specific ID
        entity_type = entity.get('type', '').lower()
        type_id_field = f"{entity_type}ID"
        if type_id_field in entity and entity[type_id_field]:
            return str(entity[type_id_field])
        
        return None
    
    def _compute_changes(self, old_entity: Dict, new_entity: Dict) -> Dict[str, Any]:
        """Compute what changed between two entity versions."""
        changes = {
            "added_fields": [],
            "removed_fields": [],
            "modified_fields": {}
        }
        
        old_keys = set(old_entity.keys())
        new_keys = set(new_entity.keys())
        
        # Skip version metadata fields
        skip_fields = {'_version', '_created_at', '_updated_at', '_processing_runs'}
        
        # Added fields
        for key in new_keys - old_keys:
            if key not in skip_fields:
                changes["added_fields"].append(key)
        
        # Removed fields
        for key in old_keys - new_keys:
            if key not in skip_fields:
                changes["removed_fields"].append(key)
        
        # Modified fields
        for key in old_keys & new_keys:
            if key not in skip_fields and old_entity[key] != new_entity[key]:
                changes["modified_fields"][key] = {
                    "old": old_entity[key],
                    "new": new_entity[key]
                }
        
        return changes
    
    def get_entity_version(self, entity_type: str, entity_id: str) -> Optional[Dict]:
        """Get version info for a specific entity."""
        versions = self._load_versions()
        entity_key = f"{entity_type}:{entity_id}"
        return versions["entities"].get(entity_key)
    
    def get_entity_history(self, entity_type: str, entity_id: str) -> List[Dict]:
        """Get change history for a specific entity."""
        history = self._load_history()
        entity_changes = []
        
        for change in history["changes"]:
            if (change["entity_type"] == entity_type and 
                change["entity_id"] == entity_id):
                entity_changes.append(change)
        
        return entity_changes
    
    def get_processing_run_changes(self, processing_run_id: str) -> List[Dict]:
        """Get all changes from a specific processing run."""
        history = self._load_history()
        run_changes = []
        
        for change in history["changes"]:
            if change["processing_run_id"] == processing_run_id:
                run_changes.append(change)
        
        return run_changes
    
    def rollback_processing_run(self, processing_run_id: str) -> Dict[str, int]:
        """
        Mark entities from a processing run for rollback.
        
        Note: This doesn't actually rollback data, just marks what would need
        to be rolled back. Actual rollback should be implemented by the caller.
        
        Returns:
            Dictionary with counts of entities to rollback by type
        """
        versions = self._load_versions()
        rollback_entities = defaultdict(list)
        
        for entity_key, entity_info in versions["entities"].items():
            if processing_run_id in entity_info["processing_runs"]:
                entity_type = entity_info["type"]
                rollback_entities[entity_type].append(entity_info["id"])
        
        return {
            entity_type: len(entities) 
            for entity_type, entities in rollback_entities.items()
        }
