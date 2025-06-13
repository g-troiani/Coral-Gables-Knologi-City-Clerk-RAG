"""
Configuration management for the unified GraphRAG pipeline.
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional
import yaml
from dataclasses import dataclass
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

log = logging.getLogger(__name__)

@dataclass
class Config:
    """Configuration class for the unified pipeline."""
    
    # Environment settings
    openai_api_key: str
    openai_model: str = "gpt-4"
    
    # Cosmos DB settings (for custom graph pipeline)
    cosmos_endpoint: Optional[str] = None
    cosmos_key: Optional[str] = None
    cosmos_database: str = "cgGraph"
    cosmos_container: str = "cityClerk"
    
    # File paths
    project_root: Path = None
    source_documents_dir: Path = None
    markdown_output_dir: Path = None
    graphrag_input_dir: Path = None
    
    # Pipeline settings
    chunk_size: int = 4000
    chunk_overlap: int = 200
    max_concurrent_requests: int = 5
    
    # GraphRAG specific settings
    graphrag_verbose: bool = True
    force_reindex: bool = False
    enable_entity_deduplication: bool = True
    deduplication_config: str = "conservative"
    
    def __post_init__(self):
        """Initialize paths after dataclass creation."""
        if self.project_root is None:
            self.project_root = Path(__file__).parent.parent.parent
        
        if self.source_documents_dir is None:
            self.source_documents_dir = self.project_root / "city_clerk_documents/global/City Comissions 2024"
        
        if self.markdown_output_dir is None:
            self.markdown_output_dir = self.project_root / "city_clerk_documents/extracted_markdown"
        
        if self.graphrag_input_dir is None:
            self.graphrag_input_dir = self.project_root / "graphrag_data"


def get_config(config_file: Optional[Path] = None) -> Config:
    """
    Load configuration from environment variables and optional YAML file.
    
    Args:
        config_file: Optional YAML configuration file path
        
    Returns:
        Config object with all settings
    """
    # Start with environment variables
    config_data = {
        "openai_api_key": os.getenv("OPENAI_API_KEY"),
        "openai_model": os.getenv("OPENAI_MODEL", "gpt-4"),
        "cosmos_endpoint": os.getenv("COSMOS_ENDPOINT"),
        "cosmos_key": os.getenv("COSMOS_KEY"),
        "cosmos_database": os.getenv("COSMOS_DATABASE", "cgGraph"),
        "cosmos_container": os.getenv("COSMOS_CONTAINER", "cityClerk"),
    }
    
    # Load from YAML if provided
    if config_file and config_file.exists():
        try:
            with open(config_file, 'r') as f:
                yaml_config = yaml.safe_load(f)
                config_data.update(yaml_config)
            log.info(f"Loaded configuration from {config_file}")
        except Exception as e:
            log.warning(f"Failed to load config file {config_file}: {e}")
    
    # Check for settings.yaml in project root
    project_root = Path(__file__).parent.parent.parent
    settings_file = project_root / "settings.yaml"
    if settings_file.exists() and config_file != settings_file:
        try:
            with open(settings_file, 'r') as f:
                yaml_config = yaml.safe_load(f)
                # Only update if not already set
                for key, value in yaml_config.items():
                    if key not in config_data or config_data[key] is None:
                        config_data[key] = value
            log.info(f"Loaded additional configuration from {settings_file}")
        except Exception as e:
            log.warning(f"Failed to load settings file {settings_file}: {e}")
    
    # Validate required settings
    if not config_data.get("openai_api_key"):
        raise ValueError("OPENAI_API_KEY is required but not found in environment or config file")
    
    return Config(**{k: v for k, v in config_data.items() if v is not None})


def save_config(config: Config, output_file: Path) -> None:
    """
    Save configuration to a YAML file.
    
    Args:
        config: Configuration object
        output_file: Path to output YAML file
    """
    # Convert config to dict, handling Path objects
    config_dict = {}
    for key, value in config.__dict__.items():
        if isinstance(value, Path):
            config_dict[key] = str(value)
        else:
            config_dict[key] = value
    
    try:
        with open(output_file, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False, indent=2)
        log.info(f"Configuration saved to {output_file}")
    except Exception as e:
        log.error(f"Failed to save configuration to {output_file}: {e}")
        raise 