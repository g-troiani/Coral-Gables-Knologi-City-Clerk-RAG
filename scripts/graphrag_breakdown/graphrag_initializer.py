import os
from pathlib import Path
import yaml
import subprocess
import sys
from typing import Dict, Any

class GraphRAGInitializer:
    """Initialize and configure Microsoft GraphRAG for city clerk documents."""
    
    def __init__(self, project_root: Path):
        self.project_root = Path(project_root)
        self.graphrag_root = self.project_root / "graphrag_data"
        
    def setup_environment(self):
        """Setup GraphRAG environment and configuration."""
        # Get the correct Python executable
        if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
            # We're in a virtualenv
            python_exe = sys.executable
        else:
            # Try to find venv Python
            venv_python = os.path.join(os.path.dirname(sys.executable), '..', 'venv', 'bin', 'python3')
            if os.path.exists(venv_python):
                python_exe = venv_python
            else:
                python_exe = sys.executable
        
        print(f"🐍 Using Python: {python_exe}")
        
        # Create directory structure
        self.graphrag_root.mkdir(exist_ok=True)
        
        # Run GraphRAG init
        subprocess.run([
            python_exe,  # Use the correct Python
            "-m", "graphrag", "init", 
            "--root", str(self.graphrag_root),
            "--force"
        ])
        
        # Configure settings
        self._configure_settings()
        self._configure_prompts()
        
    def _configure_settings(self):
        """Configure GraphRAG settings for city clerk documents using the modern format."""
        settings = {
            "encoding_model": "cl100k_base",
            "skip_workflows": [],
            "models": {
                "default_chat_model": {
                    "api_key": "${OPENAI_API_KEY}",
                    "type": "openai_chat",
                    "model": "gpt-4.1-mini-2025-04-14",
                    "encoding_model": "cl100k_base",
                    "max_tokens": 32768,
                    "temperature": 0,
                    "api_type": "openai"
                },
                "default_embedding_model": {
                    "api_key": "${OPENAI_API_KEY}",
                    "type": "openai_embedding",
                    "model": "text-embedding-3-small",
                    "encoding_model": "cl100k_base",
                    "batch_size": 16,
                    "batch_max_tokens": 2048
                }
            },
            "input": {
                "type": "file",
                "file_type": "csv",
                "base_dir": ".",
                "source_column": "text",
                "text_column": "text",
                "title_column": "title"
            },
            "chunks": {
                "group_by_columns": [
                    "document_type",
                    "meeting_date",
                    "item_code"
                ],
                "overlap": 200,
                "size": 1200
            },
            "extract_graph": {
                "model_id": "default_chat_model",
                "prompt": "prompts/entity_extraction.txt",  # Use custom prompt
                "entity_types": [
                    "agenda_item",
                    "ordinance", 
                    "resolution",
                    "person",
                    "organization",
                    "meeting",
                    "money",
                    "project"
                ],
                "max_gleanings": 2,
                "pattern_examples": {
                    "agenda_item": ["E-1", "F-10", "H-3", "Item E-2"],
                    "ordinance": ["2024-01", "Ordinance 2024-15"],
                    "resolution": ["2024-123", "Resolution 2024-45"]
                }
            },
            "entity_extraction": {
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
            "community_reports": {
                "max_input_length": 32768,
                "max_length": 2000
            },
            "claim_extraction": {
                "description": "Extract voting records, motions, and decisions",
                "enabled": True
            },
            "cluster_graph": {
                "max_cluster_size": 10
            },
            "storage": {
                "base_dir": "./output/artifacts",
                "type": "file"
            }
        }
        
        settings_path = self.graphrag_root / "settings.yaml"
        with open(settings_path, 'w') as f:
            yaml.dump(settings, f, sort_keys=False)
    
    def _configure_prompts(self):
        """Setup prompt configuration placeholders."""
        # This method will be called after auto-tuning
        pass 