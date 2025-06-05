import os
from pathlib import Path
import yaml
import subprocess
from typing import Dict, Any

class GraphRAGInitializer:
    """Initialize and configure Microsoft GraphRAG for city clerk documents."""
    
    def __init__(self, project_root: Path):
        self.project_root = Path(project_root)
        self.graphrag_root = self.project_root / "graphrag_data"
        
    def setup_environment(self):
        """Setup GraphRAG environment and configuration."""
        # Create directory structure
        self.graphrag_root.mkdir(exist_ok=True)
        
        # Run GraphRAG init
        subprocess.run([
            "graphrag", "init", 
            "--root", str(self.graphrag_root),
            "--force"
        ])
        
        # Configure settings
        self._configure_settings()
        self._configure_prompts()
        
    def _configure_settings(self):
        """Configure GraphRAG settings for city clerk documents."""
        settings = {
            "llm": {
                "api_type": "openai",
                "model": "gpt-4.1-mini-2025-04-14",
                "api_key": "${OPENAI_API_KEY}",
                "max_tokens": 32768,
                "temperature": 0
            },
            "chunks": {
                "size": 1200,
                "overlap": 200,
                "group_by_columns": ["document_type", "meeting_date", "item_code"]
            },
            "entity_extraction": {
                "prompt": "prompts/city_clerk_entity_extraction.txt",
                "entity_types": [
                    "person",
                    "organization", 
                    "location",
                    "document",
                    "meeting",
                    "money",
                    "project",
                    "agenda_item",
                    "ordinance",
                    "resolution",
                    "contract"
                ],
                "max_gleanings": 2
            },
            "claim_extraction": {
                "enabled": True,
                "prompt": "prompts/city_clerk_claims.txt",
                "description": "Extract voting records, motions, and decisions"
            },
            "community_reports": {
                "prompt": "prompts/city_clerk_community_report.txt",
                "max_length": 2000,
                "max_input_length": 32768
            },
            "embeddings": {
                "model": "text-embedding-3-small",
                "api_key": "${OPENAI_API_KEY}",
                "batch_size": 16,
                "batch_max_tokens": 2048
            },
            "cluster_graph": {
                "max_cluster_size": 10
            },
            "storage": {
                "type": "file",
                "base_dir": "./output/artifacts"
            },
            "query": {
                "global_search": {
                    "community_level": 2,
                    "max_tokens": 32768,
                    "temperature": 0.0,
                    "top_p": 1.0,
                    "n": 1,
                    "use_dynamic_community_selection": True,
                    "relevance_score_threshold": 0.7,
                    "rate_relevancy_model": "gpt-3.5-turbo"
                },
                "local_search": {
                    "text_unit_prop": 0.5,
                    "community_prop": 0.1,
                    "conversation_history_max_turns": 5,
                    "top_k_entities": 10,
                    "top_k_relationships": 10,
                    "max_tokens": 32768,
                    "temperature": 0.0
                },
                "drift_search": {
                    "initial_community_level": 2,
                    "max_iterations": 5,
                    "follow_up_expansion": 3,
                    "relevance_threshold": 0.7,
                    "max_tokens": 32768,
                    "temperature": 0.0,
                    "primer_queries": 3,
                    "follow_up_depth": 5,
                    "similarity_threshold": 0.8,
                    "termination_strategy": "convergence",
                    "include_global_context": True
                }
            }
        }
        
        settings_path = self.graphrag_root / "settings.yaml"
        with open(settings_path, 'w') as f:
            yaml.dump(settings, f)
    
    def _configure_prompts(self):
        """Setup prompt configuration placeholders."""
        # This method will be called after auto-tuning
        pass 