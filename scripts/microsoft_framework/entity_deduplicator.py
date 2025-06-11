#!/usr/bin/env python3
"""
Advanced Entity Deduplication for GraphRAG output.

This module provides sophisticated entity deduplication capabilities that go beyond
simple string matching by considering:
- String similarity using multiple algorithms
- Graph-based clustering coefficients
- Entity relationship patterns
- Configurable merging strategies
"""

import pandas as pd
import networkx as nx
from pathlib import Path
import difflib
from typing import Dict, List, Tuple, Set, Any, Optional
import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class AdvancedEntityDeduplicator:
    """
    Advanced entity deduplication using multiple similarity metrics and graph analysis.
    """
    
    def __init__(
        self,
        output_dir: Path,
        similarity_threshold: float = 0.95,
        clustering_tolerance: float = 0.1,
        merge_strategy: str = "keep_most_connected"
    ):
        """
        Initialize the entity deduplicator.
        
        Args:
            output_dir: Path to GraphRAG output directory
            similarity_threshold: Minimum string similarity for merging (0-1)
            clustering_tolerance: Maximum difference in clustering coefficients
            merge_strategy: Strategy for which entity to keep ("keep_most_connected" or "keep_first")
        """
        self.output_dir = Path(output_dir)
        self.similarity_threshold = similarity_threshold
        self.clustering_tolerance = clustering_tolerance
        self.merge_strategy = merge_strategy
        
        # Initialize data containers
        self.entities_df = None
        self.relationships_df = None
        self.graph = None
        self.merge_groups = []
        self.merge_report = {
            'timestamp': datetime.now().isoformat(),
            'parameters': {
                'similarity_threshold': similarity_threshold,
                'clustering_tolerance': clustering_tolerance,
                'merge_strategy': merge_strategy
            },
            'merges': []
        }
        
        logger.info(f"Initialized deduplicator for {output_dir}")
        logger.info(f"Settings: threshold={similarity_threshold}, tolerance={clustering_tolerance}")
    
    def deduplicate_entities(self) -> Dict[str, Any]:
        """
        Main deduplication process.
        
        Returns:
            Dictionary with deduplication statistics
        """
        logger.info("Starting entity deduplication process")
        
        # Load data
        self._load_data()
        original_entity_count = len(self.entities_df)
        
        # Build graph for analysis
        self._build_graph()
        
        # Calculate clustering coefficients
        clustering_coeffs = self._calculate_clustering_coefficients()
        
        # Find similar entities
        similar_groups = self._find_similar_entities(clustering_coeffs)
        
        # Merge entities
        merged_entities_df = self._merge_entities(similar_groups)
        
        # Save results
        self._save_deduplicated_data(merged_entities_df)
        self._save_merge_report()
        
        # Calculate statistics
        final_entity_count = len(merged_entities_df)
        merged_count = original_entity_count - final_entity_count
        
        stats = {
            'original_entities': original_entity_count,
            'merged_entities': final_entity_count,
            'merged_count': merged_count,
            'merge_groups': len(similar_groups),
            'output_dir': str(self.output_dir / "deduplicated")
        }
        
        logger.info(f"Deduplication complete: {original_entity_count} -> {final_entity_count} entities")
        return stats
    
    def _load_data(self):
        """Load GraphRAG entities and relationships data."""
        entities_path = self.output_dir / "entities.parquet"
        relationships_path = self.output_dir / "relationships.parquet"
        
        if not entities_path.exists():
            raise FileNotFoundError(f"Entities file not found: {entities_path}")
        if not relationships_path.exists():
            raise FileNotFoundError(f"Relationships file not found: {relationships_path}")
        
        self.entities_df = pd.read_parquet(entities_path)
        self.relationships_df = pd.read_parquet(relationships_path)
        
        logger.info(f"Loaded {len(self.entities_df)} entities and {len(self.relationships_df)} relationships")
    
    def _build_graph(self):
        """Build NetworkX graph from relationships for analysis."""
        self.graph = nx.Graph()
        
        # Add entities as nodes
        for _, entity in self.entities_df.iterrows():
            self.graph.add_node(entity['title'], **entity.to_dict())
        
        # Add relationships as edges
        for _, rel in self.relationships_df.iterrows():
            if 'source' in rel and 'target' in rel:
                self.graph.add_edge(rel['source'], rel['target'], **rel.to_dict())
        
        logger.info(f"Built graph with {self.graph.number_of_nodes()} nodes and {self.graph.number_of_edges()} edges")
    
    def _calculate_clustering_coefficients(self) -> Dict[str, float]:
        """
        Calculate clustering coefficients for all entities.
        
        Returns:
            Dictionary mapping entity names to clustering coefficients
        """
        clustering_coeffs = {}
        
        for entity_name in self.entities_df['title']:
            if entity_name in self.graph:
                clustering_coeffs[entity_name] = nx.clustering(self.graph, entity_name)
            else:
                clustering_coeffs[entity_name] = 0.0
        
        logger.info(f"Calculated clustering coefficients for {len(clustering_coeffs)} entities")
        return clustering_coeffs
    
    def _find_similar_entities(self, clustering_coeffs: Dict[str, float]) -> List[List[str]]:
        """
        Find groups of similar entities based on string similarity and clustering coefficients.
        
        Args:
            clustering_coeffs: Dictionary of clustering coefficients
            
        Returns:
            List of groups, where each group is a list of similar entity names
        """
        similar_groups = []
        processed_entities = set()
        
        entity_names = self.entities_df['title'].tolist()
        
        for i, entity1 in enumerate(entity_names):
            if entity1 in processed_entities:
                continue
            
            current_group = [entity1]
            
            for j, entity2 in enumerate(entity_names[i+1:], i+1):
                if entity2 in processed_entities:
                    continue
                
                # Check string similarity
                similarity = self._string_similarity(entity1, entity2)
                
                if similarity >= self.similarity_threshold:
                    # Check clustering coefficient similarity
                    coeff1 = clustering_coeffs.get(entity1, 0.0)
                    coeff2 = clustering_coeffs.get(entity2, 0.0)
                    coeff_diff = abs(coeff1 - coeff2)
                    
                    if coeff_diff <= self.clustering_tolerance:
                        current_group.append(entity2)
                        processed_entities.add(entity2)
            
            if len(current_group) > 1:
                similar_groups.append(current_group)
                for entity in current_group:
                    processed_entities.add(entity)
            else:
                processed_entities.add(entity1)
        
        logger.info(f"Found {len(similar_groups)} groups of similar entities")
        for i, group in enumerate(similar_groups):
            logger.debug(f"Group {i+1}: {group}")
        
        return similar_groups
    
    def _string_similarity(self, str1: str, str2: str) -> float:
        """
        Calculate string similarity using SequenceMatcher.
        
        Args:
            str1: First string
            str2: Second string
            
        Returns:
            Similarity score between 0 and 1
        """
        # Normalize strings for comparison
        str1_norm = str1.lower().strip()
        str2_norm = str2.lower().strip()
        
        # Use SequenceMatcher for similarity
        similarity = difflib.SequenceMatcher(None, str1_norm, str2_norm).ratio()
        
        return similarity
    
    def _merge_entities(self, similar_groups: List[List[str]]) -> pd.DataFrame:
        """
        Merge similar entities based on the configured strategy.
        
        Args:
            similar_groups: List of groups of similar entity names
            
        Returns:
            DataFrame with merged entities
        """
        merged_df = self.entities_df.copy()
        entities_to_remove = set()
        
        for group in similar_groups:
            if len(group) <= 1:
                continue
            
            # Determine which entity to keep
            if self.merge_strategy == "keep_most_connected":
                # Keep entity with highest degree centrality
                degrees = {entity: self.graph.degree(entity) if entity in self.graph else 0 
                          for entity in group}
                primary_entity = max(degrees, key=degrees.get)
            else:  # keep_first
                primary_entity = group[0]
            
            # Get entities to merge
            entities_to_merge = [e for e in group if e != primary_entity]
            
            # Update primary entity description to include merged information
            primary_row = merged_df[merged_df['title'] == primary_entity].iloc[0]
            merged_descriptions = [primary_row.get('description', '')]
            
            for entity in entities_to_merge:
                entity_row = merged_df[merged_df['title'] == entity].iloc[0]
                entity_desc = entity_row.get('description', '')
                if entity_desc and entity_desc not in merged_descriptions:
                    merged_descriptions.append(entity_desc)
                
                # Mark for removal
                entities_to_remove.add(entity)
            
            # Update primary entity description
            if len(merged_descriptions) > 1:
                merged_desc = f"{merged_descriptions[0]} [MERGED: Contains information from {', '.join(entities_to_merge)}]"
                merged_df.loc[merged_df['title'] == primary_entity, 'description'] = merged_desc
            
            # Record merge for report
            self.merge_report['merges'].append({
                'primary_entity': primary_entity,
                'merged_entities': entities_to_merge,
                'merge_reason': f"String similarity >= {self.similarity_threshold}",
                'strategy': self.merge_strategy
            })
        
        # Remove merged entities
        merged_df = merged_df[~merged_df['title'].isin(entities_to_remove)]
        
        logger.info(f"Merged {len(entities_to_remove)} entities into {len(similar_groups)} primary entities")
        return merged_df
    
    def _save_deduplicated_data(self, merged_entities_df: pd.DataFrame):
        """
        Save deduplicated data to output directory.
        
        Args:
            merged_entities_df: DataFrame with merged entities
        """
        output_subdir = self.output_dir / "deduplicated"
        output_subdir.mkdir(exist_ok=True)
        
        # Save merged entities
        entities_output = output_subdir / "entities.parquet"
        merged_entities_df.to_parquet(entities_output, index=False)
        
        # Copy other files unchanged
        other_files = [
            "relationships.parquet",
            "communities.parquet", 
            "community_reports.parquet"
        ]
        
        for filename in other_files:
            source_path = self.output_dir / filename
            target_path = output_subdir / filename
            
            if source_path.exists():
                import shutil
                shutil.copy2(source_path, target_path)
        
        logger.info(f"Saved deduplicated data to {output_subdir}")
    
    def _save_merge_report(self):
        """Save detailed merge report."""
        report_path = self.output_dir / "deduplicated" / "merge_report.json"
        
        with open(report_path, 'w') as f:
            json.dump(self.merge_report, f, indent=2)
        
        logger.info(f"Saved merge report to {report_path}")


# Main execution for standalone testing
if __name__ == "__main__":
    import argparse
    import sys
    from pathlib import Path
    
    # Add project root to path
    project_root = Path(__file__).parent.parent.parent
    sys.path.append(str(project_root))
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(description='Run advanced entity deduplication')
    parser.add_argument('--output-dir', '-o', type=str, default='graphrag_data/output',
                       help='Path to GraphRAG output directory')
    parser.add_argument('--threshold', '-t', type=float, default=0.95,
                       help='String similarity threshold (0-1)')
    parser.add_argument('--tolerance', type=float, default=0.1,
                       help='Clustering coefficient tolerance')
    parser.add_argument('--strategy', choices=['keep_most_connected', 'keep_first'],
                       default='keep_most_connected', help='Merge strategy')
    
    args = parser.parse_args()
    
    output_dir = project_root / args.output_dir
    
    if not output_dir.exists():
        print(f"❌ Output directory not found: {output_dir}")
        sys.exit(1)
    
    print(f"🔍 Running entity deduplication on {output_dir}")
    print(f"   Similarity threshold: {args.threshold}")
    print(f"   Clustering tolerance: {args.tolerance}")
    print(f"   Merge strategy: {args.strategy}")
    
    try:
        deduplicator = AdvancedEntityDeduplicator(
            output_dir,
            similarity_threshold=args.threshold,
            clustering_tolerance=args.tolerance,
            merge_strategy=args.strategy
        )
        
        stats = deduplicator.deduplicate_entities()
        
        print(f"\n✅ Deduplication complete!")
        print(f"   Original entities: {stats['original_entities']}")
        print(f"   After deduplication: {stats['merged_entities']}")
        print(f"   Entities merged: {stats['merged_count']}")
        print(f"   Merge groups: {stats['merge_groups']}")
        print(f"   Output saved to: {stats['output_dir']}")
        
    except Exception as e:
        print(f"❌ Deduplication failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1) 