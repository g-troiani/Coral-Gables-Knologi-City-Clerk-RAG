from pathlib import Path
import pandas as pd
import json
from typing import Dict, List, Any
import re
import ast
import logging

log = logging.getLogger(__name__)

class GraphRAGOutputProcessor:
    """Process and load GraphRAG output artifacts."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        
    def parse_config_string(self, config_str: str) -> Dict[str, Any]:
        """Parse configuration strings from GraphRAG output."""
        try:
            # Handle strings like: "default_vector_store": { "type": "lancedb", ... }
            # First try to parse as JSON-like string
            if '"default_vector_store"' in config_str or "'default_vector_store'" in config_str:
                # Extract the dictionary part
                match = re.search(r'"default_vector_store"\s*:\s*({[^}]+})', config_str)
                if not match:
                    match = re.search(r"'default_vector_store'\s*:\s*({[^}]+})", config_str)
                
                if match:
                    dict_str = match.group(1)
                    # Replace single quotes with double quotes for JSON parsing
                    dict_str = dict_str.replace("'", '"')
                    # Handle None/null values
                    dict_str = dict_str.replace('None', 'null')
                    dict_str = dict_str.replace('True', 'true')
                    dict_str = dict_str.replace('False', 'false')
                    
                    return json.loads(dict_str)
            
            # Try to parse as Python literal
            return ast.literal_eval(config_str)
        except Exception as e:
            log.error(f"Failed to parse config string: {e}")
            return {}
    
    def extract_vector_store_config(self, artifacts: Dict[str, Any]) -> Dict[str, Any]:
        """Extract vector store configuration from artifacts."""
        vector_configs = {}
        
        # Check entities for embedded config
        if 'entities' in artifacts:
            entities_df = artifacts['entities']
            # Look for configuration in entity descriptions or metadata
            for _, entity in entities_df.iterrows():
                if 'description' in entity and 'default_vector_store' in str(entity['description']):
                    config = self.parse_config_string(str(entity['description']))
                    if config:
                        vector_configs['from_entities'] = config
        
        # Check community reports
        if 'community_reports' in artifacts:
            reports_df = artifacts['community_reports']
            for _, report in reports_df.iterrows():
                if 'summary' in report and 'default_vector_store' in str(report['summary']):
                    config = self.parse_config_string(str(report['summary']))
                    if config:
                        vector_configs['from_reports'] = config
        
        return vector_configs
    
    def load_artifacts(self) -> Dict[str, Any]:
        """Load all GraphRAG artifacts."""
        artifacts = {}
        
        # Load entities
        entities_path = self.output_dir / "entities.parquet"
        if entities_path.exists():
            try:
                import pandas as pd
                artifacts['entities'] = pd.read_parquet(entities_path)
            except Exception as e:
                log.error(f"Failed to load entities: {e}")
        
        # Load relationships
        relationships_path = self.output_dir / "relationships.parquet"
        if relationships_path.exists():
            try:
                import pandas as pd
                artifacts['relationships'] = pd.read_parquet(relationships_path)
            except Exception as e:
                log.error(f"Failed to load relationships: {e}")
        
        # Load community reports
        reports_path = self.output_dir / "community_reports.parquet"
        if reports_path.exists():
            try:
                import pandas as pd
                artifacts['community_reports'] = pd.read_parquet(reports_path)
            except Exception as e:
                log.error(f"Failed to load community reports: {e}")
        
        return artifacts
    
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
    
    def get_entity_summary(self) -> Dict[str, Any]:
        """Get summary of entities."""
        entities_path = self.output_dir / "entities.parquet"
        if not entities_path.exists():
            return {}
        
        try:
            entities_df = pd.read_parquet(entities_path)
            
            summary = {
                'total_entities': len(entities_df),
                'entity_types': entities_df['type'].value_counts().to_dict() if 'type' in entities_df.columns else {}
            }
            return summary
        except Exception as e:
            log.error(f"Failed to load entities: {e}")
            return {}
    
    def get_relationship_summary(self) -> Dict[str, Any]:
        """Get summary of relationships."""
        relationships_path = self.output_dir / "relationships.parquet"
        if not relationships_path.exists():
            return {}
        
        try:
            relationships_df = pd.read_parquet(relationships_path)
            
            summary = {
                'total_relationships': len(relationships_df),
                'relationship_types': relationships_df['type'].value_counts().to_dict() if 'type' in relationships_df.columns else {}
            }
            return summary
        except Exception as e:
            log.error(f"Failed to load relationships: {e}")
            return {}
    
    def get_community_summary(self) -> Dict[str, Any]:
        """Get summary statistics of extracted communities."""
        communities_path = self.output_dir / "communities.parquet"
        if communities_path.exists():
            communities_df = pd.read_parquet(communities_path)
            
            summary = {
                'total_communities': len(communities_df),
                'community_types': communities_df['type'].value_counts().to_dict()
            }
            return summary
        else:
            return {}
    
    def parse_vector_store_config(self, text: str) -> dict:
        """Parse vector store configuration from text."""
        import re
        import json
        
        pattern = r'"default_vector_store"\s*:\s*(\{[^}]+\})'
        match = re.search(pattern, text)
        
        if match:
            try:
                config_str = match.group(1)
                config_str = config_str.replace("null", "null")
                config_str = config_str.replace("true", "true")
                config_str = config_str.replace("false", "false")
                return json.loads(config_str)
            except:
                pass
        return {} 