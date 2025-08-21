"""
Partition key optimization for Cosmos DB to avoid hot partitions.
Implements a strategy for better partition key distribution.
"""

import hashlib
from typing import Dict, Any, Optional
from datetime import datetime
import logging

log = logging.getLogger(__name__)


class PartitionKeyOptimizer:
    """
    Optimizes partition key distribution to avoid hot partitions in Cosmos DB.
    
    Instead of using a single partition value like "demo", this distributes
    entities across multiple logical partitions based on entity characteristics.
    """
    
    def __init__(self, num_partitions: int = 20):
        """
        Initialize the optimizer.
        
        Args:
            num_partitions: Number of logical partitions to distribute across
        """
        self.num_partitions = num_partitions
        self._partition_stats = {}  # Track distribution for monitoring
    
    def get_partition_key(self, entity_type: str, entity_id: str, 
                         properties: Optional[Dict[str, Any]] = None) -> str:
        """
        Generate an optimized partition key for an entity.
        
        Strategy:
        1. For high-cardinality entities (Person, Organization), use hash-based distribution
        2. For date-based entities (Meeting, Event), include year/month in partition
        3. For reference data (Policy, Role), use entity type as partition
        
        Args:
            entity_type: Type of the entity (e.g., "Person", "Meeting")
            entity_id: Unique ID of the entity
            properties: Entity properties (optional, used for date-based partitioning)
            
        Returns:
            Optimized partition key value
        """
        
        # High-cardinality entities - distribute by hash
        if entity_type.lower() in ['person', 'organization', 'document', 'location']:
            # Hash the entity ID to get consistent distribution
            hash_val = int(hashlib.md5(entity_id.encode()).hexdigest()[:8], 16)
            partition_num = hash_val % self.num_partitions
            partition_key = f"{entity_type.lower()}_{partition_num}"
            
        # Date-based entities - partition by time period
        elif entity_type.lower() in ['meeting', 'event', 'agendaitem']:
            # Try to extract date from properties
            date_str = None
            if properties:
                date_str = properties.get('meetingDate') or properties.get('date') or properties.get('eventDate')
            
            if date_str:
                try:
                    # Parse date and create year-month partition
                    if len(date_str) >= 7:  # At least YYYY-MM
                        year_month = date_str[:7].replace('-', '_')
                        partition_key = f"{entity_type.lower()}_{year_month}"
                    else:
                        # Fallback to hash-based
                        hash_val = int(hashlib.md5(entity_id.encode()).hexdigest()[:8], 16)
                        partition_num = hash_val % 12  # Monthly buckets
                        partition_key = f"{entity_type.lower()}_month{partition_num}"
                except:
                    # Fallback to entity type
                    partition_key = f"{entity_type.lower()}_default"
            else:
                # No date available - use hash
                hash_val = int(hashlib.md5(entity_id.encode()).hexdigest()[:8], 16)
                partition_num = hash_val % 12
                partition_key = f"{entity_type.lower()}_bucket{partition_num}"
        
        # Low-cardinality reference data - partition by type
        elif entity_type.lower() in ['policy', 'role', 'topic', 'voteoutcome']:
            partition_key = f"reference_{entity_type.lower()}"
        
        # Default fallback - distribute by hash
        else:
            hash_val = int(hashlib.md5(f"{entity_type}_{entity_id}".encode()).hexdigest()[:8], 16)
            partition_num = hash_val % self.num_partitions
            partition_key = f"general_{partition_num}"
        
        # Track distribution statistics
        self._partition_stats[partition_key] = self._partition_stats.get(partition_key, 0) + 1
        
        # Log distribution periodically
        total_entities = sum(self._partition_stats.values())
        if total_entities % 1000 == 0:
            self._log_distribution_stats()
        
        return partition_key
    
    def _log_distribution_stats(self):
        """Log partition distribution statistics."""
        total = sum(self._partition_stats.values())
        if total == 0:
            return
            
        log.info("📊 Partition Key Distribution:")
        
        # Sort by count descending
        sorted_stats = sorted(self._partition_stats.items(), key=lambda x: x[1], reverse=True)
        
        # Show top 10 partitions
        for partition, count in sorted_stats[:10]:
            percentage = (count / total) * 100
            log.info(f"  {partition}: {count} entities ({percentage:.1f}%)")
        
        # Check for hot partitions (>10% of data)
        hot_partitions = [(p, c) for p, c in sorted_stats if (c / total) > 0.1]
        if hot_partitions:
            log.warning(f"⚠️  Hot partitions detected: {hot_partitions}")
    
    def get_partition_stats(self) -> Dict[str, Any]:
        """Get current partition distribution statistics."""
        total = sum(self._partition_stats.values())
        if total == 0:
            return {"total_entities": 0, "partition_count": 0}
        
        sorted_stats = sorted(self._partition_stats.items(), key=lambda x: x[1], reverse=True)
        
        return {
            "total_entities": total,
            "partition_count": len(self._partition_stats),
            "top_partitions": sorted_stats[:5],
            "distribution": {
                p: {"count": c, "percentage": (c/total)*100} 
                for p, c in sorted_stats
            },
            "hot_partitions": [
                (p, c, (c/total)*100) 
                for p, c in sorted_stats 
                if (c/total) > 0.1
            ]
        }


# Example usage function
def create_optimized_partition_config() -> Dict[str, Any]:
    """
    Create an optimized Cosmos configuration with better partition key strategy.
    
    Returns:
        Configuration dict with partition optimizer
    """
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Initialize optimizer
    optimizer = PartitionKeyOptimizer(num_partitions=20)
    
    return {
        'cosmos_endpoint': os.getenv("COSMOS_ENDPOINT"),
        'cosmos_key': os.getenv("COSMOS_KEY"),
        'cosmos_database': os.getenv("COSMOS_DATABASE", "cgGraph"),
        'cosmos_container': os.getenv("COSMOS_CONTAINER", "cityClerk"),
        'partition_optimizer': optimizer,
        # Remove the static partition value
        # 'partitionValue': "demo"  # DON'T USE THIS
    }
