from pathlib import Path
import pandas as pd
import json
from typing import Dict, List, Any

class GraphRAGOutputProcessor:
    """Process and load GraphRAG output artifacts."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        
    def load_graphrag_artifacts(self) -> Dict[str, Any]:
        """Load all GraphRAG output artifacts."""
        artifacts = {}
        
        # Load entities
        entities_path = self.output_dir / "entities.parquet"
        if entities_path.exists():
            artifacts['entities'] = pd.read_parquet(entities_path)
        
        # Load relationships
        relationships_path = self.output_dir / "relationships.parquet"
        if relationships_path.exists():
            artifacts['relationships'] = pd.read_parquet(relationships_path)
        
        # Load communities
        communities_path = self.output_dir / "communities.parquet"
        if communities_path.exists():
            artifacts['communities'] = pd.read_parquet(communities_path)
        
        # Load community reports
        reports_path = self.output_dir / "community_reports.parquet"
        if reports_path.exists():
            artifacts['community_reports'] = pd.read_parquet(reports_path)
        
        return artifacts
    
    def get_entity_summary(self) -> Dict[str, int]:
        """Get summary statistics of extracted entities."""
        entities_path = self.output_dir / "entities.parquet"
        if not entities_path.exists():
            return {}
        
        entities_df = pd.read_parquet(entities_path)
        
        summary = {
            'total_entities': len(entities_df),
            'entity_types': entities_df['type'].value_counts().to_dict()
        }
        
        return summary
    
    def get_relationship_summary(self) -> Dict[str, int]:
        """Get summary statistics of extracted relationships."""
        relationships_path = self.output_dir / "relationships.parquet"
        if not relationships_path.exists():
            return {}
        
        relationships_df = pd.read_parquet(relationships_path)
        
        summary = {
            'total_relationships': len(relationships_df),
            'relationship_types': relationships_df['type'].value_counts().to_dict()
        }
        
        return summary 