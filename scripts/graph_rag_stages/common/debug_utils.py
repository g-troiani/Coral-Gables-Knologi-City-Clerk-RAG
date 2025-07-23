"""Debug utilities for entity ID tracking."""

import logging
from typing import Dict, Any, List

log = logging.getLogger(__name__)

class EntityIDDebugger:
    """Helper to debug entity ID issues."""
    
    @staticmethod
    def log_entity_ids(entities: List[Dict[str, Any]], context: str = ""):
        """Log all entity IDs for debugging."""
        log.debug(f"\n{'='*60}")
        log.debug(f"Entity ID Debug - {context}")
        log.debug(f"{'='*60}")
        
        for entity in entities:
            entity_type = entity.get('type', 'Unknown')
            
            # Find all ID-like fields
            id_fields = {k: v for k, v in entity.items() 
                        if 'id' in k.lower() or k == 'id'}
            
            log.debug(f"\nEntity Type: {entity_type}")
            log.debug(f"ID Fields Found: {id_fields}")
            log.debug(f"Name: {entity.get('name', 'No name')}")
        
        log.debug(f"{'='*60}\n") 