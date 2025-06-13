"""
Adapts the enriched markdown files into a format suitable for GraphRAG ingestion.
"""

import pandas as pd
from pathlib import Path
import logging
import json
from typing import Dict, Any, List
from ..common.utils import extract_metadata_from_header, ensure_directory_exists

log = logging.getLogger(__name__)


class GraphRAGAdapter:
    """Prepares structured data for the GraphRAG indexing pipeline."""

    def create_graphrag_input_csv(self, markdown_dir: Path, output_dir: Path) -> Path:
        """
        Scans a directory of enriched markdown files and creates the final CSV
        that will be fed into the GraphRAG index command.
        
        Args:
            markdown_dir: Directory containing enriched markdown files
            output_dir: GraphRAG working directory
            
        Returns:
            Path to the created CSV file
        """
        log.info(f"📋 Creating GraphRAG input CSV from markdown files in: {markdown_dir}")
        
        # Ensure directories exist
        ensure_directory_exists(output_dir)
        input_dir = output_dir / "input"
        ensure_directory_exists(input_dir)
        
        documents = []
        
        # Process all markdown files
        markdown_files = list(markdown_dir.glob("*.md"))
        log.info(f"Found {len(markdown_files)} markdown files to process")
        
        for md_file in markdown_files:
            try:
                with open(md_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Extract metadata from the enriched header
                metadata = extract_metadata_from_header(content)
                
                # Create document record
                doc_record = {
                    'id': md_file.stem,
                    'text': content,  # Full content including rich metadata header
                    'title': metadata.get('title', md_file.stem.replace('_', ' ').title()),
                    'document_type': self._determine_document_type(md_file, metadata),
                    'meeting_date': metadata.get('meeting_date', ''),
                    'source_file': md_file.name
                }
                
                # Add agenda item information if available
                if 'agenda_item' in metadata:
                    doc_record['agenda_item'] = metadata['agenda_item']
                
                documents.append(doc_record)
                
            except Exception as e:
                log.error(f"Error processing {md_file.name}: {e}")
                continue

        if not documents:
            log.warning("No documents found to adapt for GraphRAG. The pipeline might fail.")
            return None

        # Create DataFrame and save as CSV
        df = pd.DataFrame(documents)
        output_csv_path = input_dir / "city_clerk_documents.csv"
        df.to_csv(output_csv_path, index=False, encoding='utf-8')
        
        log.info(f"✅ Successfully created GraphRAG input file at: {output_csv_path}")
        log.info(f"📊 Processed {len(documents)} documents:")
        
        # Log summary by document type
        type_counts = df['document_type'].value_counts()
        for doc_type, count in type_counts.items():
            log.info(f"   - {doc_type}: {count}")
        
        return output_csv_path

    def _determine_document_type(self, md_file: Path, metadata: Dict[str, Any]) -> str:
        """
        Determine document type from filename and metadata.
        
        Args:
            md_file: Path to markdown file
            metadata: Extracted metadata
            
        Returns:
            Document type string
        """
        # Check metadata first
        if 'document_type' in metadata:
            return metadata['document_type'].lower()
        
        # Check filename patterns
        filename_lower = md_file.name.lower()
        
        if 'agenda' in filename_lower:
            return 'agenda'
        elif 'verbatim' in filename_lower:
            return 'transcript'
        elif 'ordinance' in filename_lower:
            return 'ordinance'
        elif 'resolution' in filename_lower:
            return 'resolution'
        elif 'minutes' in filename_lower:
            return 'minutes'
        else:
            return 'document'

    def create_graphrag_settings(self, output_dir: Path, custom_settings: Dict[str, Any] = None) -> Path:
        """
        Create GraphRAG settings file with city clerk specific configuration.
        
        Args:
            output_dir: GraphRAG working directory
            custom_settings: Optional custom settings to override defaults
            
        Returns:
            Path to the created settings file
        """
        log.info("⚙️ Creating GraphRAG settings file")
        
        # Default settings optimized for city clerk documents
        default_settings = {
            "llm": {
                "api_key": "${OPENAI_API_KEY}",
                "type": "openai_chat",
                "model": "gpt-4",
                "max_tokens": 4000,
                "temperature": 0.0
            },
            "parallelization": {
                "stagger": 0.3,
                "num_threads": 4
            },
            "async_mode": "threaded",
            "encoding_model": "cl100k_base",
            "skip_workflows": [],
            "entity_extraction": {
                "prompt": "Given a text document about city government proceedings, identify all entities. Extract entities that represent people, organizations, locations, agenda items, ordinances, resolutions, and key concepts discussed in city meetings.",
                "entity_types": [
                    "PERSON", "ORGANIZATION", "LOCATION", 
                    "AGENDA_ITEM", "ORDINANCE", "RESOLUTION",
                    "MEETING", "COMMITTEE", "DEPARTMENT"
                ],
                "max_gleanings": 1
            },
            "summarize_descriptions": {
                "prompt": "Given one or more entities that have been identified from city government documents, provide a comprehensive description that captures their role in municipal governance.",
                "max_length": 500
            },
            "community_reports": {
                "prompt": "Write a comprehensive report about the community and its entities, focusing on municipal governance, city operations, and citizen services.",
                "max_length": 2000,
                "max_input_length": 8000
            },
            "claim_extraction": {
                "prompt": "Given a text document about city government proceedings, extract all factual claims made during meetings, including voting records, policy decisions, and citizen concerns.",
                "description": "Any claim or assertion made in city government documents",
                "max_gleanings": 1
            },
            "chunks": {
                "size": 1200,
                "overlap": 100,
                "group_by_columns": ["source_file"]
            },
            "input": {
                "type": "file",
                "file_type": "csv",
                "base_dir": "input",
                "source_column": "text",
                "timestamp_column": "meeting_date",
                "timestamp_format": "%m.%d.%Y",
                "text_column": "text",
                "title_column": "title"
            },
            "cache": {
                "type": "file",
                "base_dir": "cache"
            },
            "storage": {
                "type": "file",
                "base_dir": "output"
            },
            "reporting": {
                "type": "file",
                "base_dir": "output/reports"
            },
            "snapshots": {
                "embeddings": False,
                "transient": False
            }
        }
        
        # Merge with custom settings if provided
        if custom_settings:
            settings = self._deep_merge_dicts(default_settings, custom_settings)
        else:
            settings = default_settings
        
        # Save settings file
        settings_path = output_dir / "settings.yaml"
        
        import yaml
        with open(settings_path, 'w') as f:
            yaml.dump(settings, f, default_flow_style=False, indent=2)
        
        log.info(f"✅ GraphRAG settings saved to: {settings_path}")
        return settings_path

    def _deep_merge_dicts(self, dict1: Dict, dict2: Dict) -> Dict:
        """Deep merge two dictionaries."""
        result = dict1.copy()
        for key, value in dict2.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge_dicts(result[key], value)
            else:
                result[key] = value
        return result

    def validate_input_data(self, csv_path: Path) -> bool:
        """
        Validate the prepared CSV data for GraphRAG ingestion.
        
        Args:
            csv_path: Path to the CSV file
            
        Returns:
            True if validation passes, False otherwise
        """
        log.info(f"🔍 Validating GraphRAG input data: {csv_path}")
        
        try:
            df = pd.read_csv(csv_path)
            
            # Check required columns
            required_columns = ['id', 'text']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                log.error(f"Missing required columns: {missing_columns}")
                return False
            
            # Check for empty content
            empty_texts = df['text'].isna().sum()
            if empty_texts > 0:
                log.warning(f"Found {empty_texts} documents with empty text")
            
            # Check text length distribution
            text_lengths = df['text'].str.len()
            log.info(f"📊 Text length statistics:")
            log.info(f"   - Average: {text_lengths.mean():.0f} characters")
            log.info(f"   - Median: {text_lengths.median():.0f} characters")
            log.info(f"   - Min: {text_lengths.min():.0f} characters")
            log.info(f"   - Max: {text_lengths.max():.0f} characters")
            
            # Warn about very short documents
            short_docs = (text_lengths < 100).sum()
            if short_docs > 0:
                log.warning(f"Found {short_docs} documents with less than 100 characters")
            
            log.info(f"✅ Validation completed for {len(df)} documents")
            return True
            
        except Exception as e:
            log.error(f"❌ Validation failed: {e}")
            return False 