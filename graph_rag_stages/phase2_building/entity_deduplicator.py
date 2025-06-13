"""
Entity deduplication for GraphRAG output to improve graph quality.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import asyncio
from difflib import SequenceMatcher

log = logging.getLogger(__name__)

# Configuration presets
DEDUP_CONFIGS = {
    'aggressive': {
        'similarity_threshold': 0.75,
        'preserve_agenda_items': True,
        'min_combined_score': 0.75,
        'enable_partial_name_matching': True,
        'enable_token_matching': True,
        'enable_semantic_matching': True,
        'enable_graph_structure_matching': True,
        'enable_abbreviation_matching': True,
        'enable_role_based_matching': True,
    },
    'conservative': {
        'similarity_threshold': 0.9,
        'preserve_agenda_items': True,
        'min_combined_score': 0.85,
        'enable_partial_name_matching': True,
        'enable_token_matching': True,
        'enable_semantic_matching': True,
        'enable_graph_structure_matching': True,
        'enable_abbreviation_matching': True,
        'enable_role_based_matching': False,
    },
    'name_focused': {
        'similarity_threshold': 0.8,
        'preserve_agenda_items': True,
        'min_combined_score': 0.8,
        'enable_partial_name_matching': True,
        'enable_token_matching': True,
        'enable_semantic_matching': True,
        'enable_graph_structure_matching': True,
        'enable_abbreviation_matching': True,
        'enable_role_based_matching': True,
    }
}


class EntityDeduplicator:
    """Post-processes GraphRAG entities to remove duplicates and improve quality."""
    
    def __init__(self, output_dir: Path, config_name: str = 'conservative', custom_config: Dict = None):
        """
        Initialize the deduplicator.

        Args:
            output_dir: Path to GraphRAG output directory.
            config_name: The name of the configuration preset to use ('conservative', 'aggressive', 'name_focused').
            custom_config: A dictionary to override default settings.
        """
        self.output_dir = Path(output_dir)
        self.graphrag_root = output_dir  # For compatibility
        self.dedup_dir = self.output_dir / "deduplicated"
        
        # CORRECTED: Look up config dict from name, allow overrides
        self.config = DEDUP_CONFIGS.get(config_name, DEDUP_CONFIGS['conservative']).copy()
        if custom_config:
            self.config.update(custom_config)
        
        log.info(f"EntityDeduplicator initialized with config '{config_name}' (threshold: {self.config['similarity_threshold']})")

    async def run_deduplication(self, config_name: str = None) -> None:
        """Run entity deduplication with the specified configuration."""
        # Use the provided config_name or fall back to the one from constructor
        if config_name and config_name in DEDUP_CONFIGS:
            self.config = DEDUP_CONFIGS[config_name].copy()
        
        log.info(f"🔄 Starting entity deduplication with config: {config_name or 'constructor default'}")
        
        # Ensure output directory exists
        self.dedup_dir.mkdir(exist_ok=True)
        
        # Load GraphRAG entities
        entities_df = self._load_entities()
        if entities_df is None or len(entities_df) == 0:
            log.warning("No entities found to deduplicate")
            return
        
        log.info(f"📊 Found {len(entities_df)} entities to process")
        
        # Perform deduplication
        deduplicated_df = await self._deduplicate_entities(entities_df, self.config)
        
        # Save deduplicated results
        self._save_deduplicated_entities(deduplicated_df)
        
        # Generate deduplication report
        self._generate_deduplication_report(entities_df, deduplicated_df, config_name or 'default')
        
        log.info(f"✅ Entity deduplication completed")
        log.info(f"📈 Reduced {len(entities_df)} entities to {len(deduplicated_df)} entities")

    def _load_entities(self) -> Optional[pd.DataFrame]:
        """Load entities from GraphRAG output."""
        entities_file = self.output_dir / "create_final_entities.parquet"
        
        if not entities_file.exists():
            log.error(f"Entities file not found: {entities_file}")
            return None
        
        try:
            df = pd.read_parquet(entities_file)
            log.info(f"Loaded entities with columns: {list(df.columns)}")
            return df
        except Exception as e:
            log.error(f"Error loading entities file: {e}")
            return None

    async def _deduplicate_entities(self, entities_df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Perform entity deduplication based on configuration."""
        log.info("🔍 Analyzing entities for duplicates...")
        
        # Create a copy to work with
        df = entities_df.copy()
        
        # Find duplicate groups
        duplicate_groups = self._find_duplicate_groups(df, config)
        
        log.info(f"Found {len(duplicate_groups)} duplicate groups")
        
        # Merge duplicates within each group
        entities_to_remove = set()
        for group in duplicate_groups:
            if len(group) > 1:
                # Keep the first entity as the canonical one
                canonical_entity = group[0]
                duplicates = group[1:]
                
                # Mark duplicates for removal
                entities_to_remove.update(duplicates)
        
        # Remove duplicate entities
        df_deduplicated = df[~df.index.isin(entities_to_remove)].copy()
        
        return df_deduplicated

    def _find_duplicate_groups(self, df: pd.DataFrame, config: Dict) -> List[List[int]]:
        """Find groups of duplicate entities."""
        duplicate_groups = []
        processed_indices = set()
        
        for idx in df.index:
            if idx in processed_indices:
                continue
            
            # Find all entities similar to this one
            similar_indices = self._find_similar_entities(df, idx, config)
            
            if len(similar_indices) > 1:
                duplicate_groups.append(similar_indices)
                processed_indices.update(similar_indices)
            else:
                processed_indices.add(idx)
        
        return duplicate_groups

    def _find_similar_entities(self, df: pd.DataFrame, target_idx: int, config: Dict) -> List[int]:
        """Find entities similar to the target entity."""
        target_entity = df.loc[target_idx]
        similar_indices = [target_idx]
        
        target_name = str(target_entity.get('title', '')).strip().lower()
        
        for idx in df.index:
            if idx == target_idx:
                continue
            
            entity = df.loc[idx]
            
            # Calculate similarity
            similarity = self._calculate_entity_similarity(target_entity, entity)
            
            if similarity >= config['similarity_threshold']:
                similar_indices.append(idx)
        
        return similar_indices

    def _calculate_entity_similarity(self, entity1: pd.Series, entity2: pd.Series) -> float:
        """Calculate similarity between two entities."""
        name1 = str(entity1.get('title', '')).strip().lower()
        name2 = str(entity2.get('title', '')).strip().lower()
        
        # Name similarity (most important)
        name_sim = SequenceMatcher(None, name1, name2).ratio()
        
        return name_sim

    def _save_deduplicated_entities(self, deduplicated_df: pd.DataFrame) -> None:
        """Save deduplicated entities."""
        output_path = self.dedup_dir / "create_final_entities.parquet"
        deduplicated_df.to_parquet(output_path)
        
        log.info(f"✅ Deduplicated entities saved to: {output_path}")

    def _generate_deduplication_report(self, original_df: pd.DataFrame, 
                                     deduplicated_df: pd.DataFrame, config_name: str) -> None:
        """Generate a report on the deduplication process."""
        report = {
            'config_used': config_name,
            'original_entity_count': len(original_df),
            'deduplicated_entity_count': len(deduplicated_df),
            'entities_removed': len(original_df) - len(deduplicated_df),
            'reduction_percentage': ((len(original_df) - len(deduplicated_df)) / len(original_df)) * 100
        }
        
        # Save report
        import json
        report_path = self.dedup_dir / "deduplication_report.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        log.info(f"📊 Deduplication report saved to: {report_path}")
        log.info(f"📈 Reduction: {report['reduction_percentage']:.1f}% ({report['entities_removed']} entities removed)") 