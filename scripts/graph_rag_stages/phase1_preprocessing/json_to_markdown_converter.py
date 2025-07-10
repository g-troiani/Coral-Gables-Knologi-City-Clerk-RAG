"""
JSON to Markdown Converter for GraphRAG Pipeline

Converts the extracted JSON files to markdown format expected by GraphRAG indexing.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any
import re
from scripts.graph_rag_stages.common.metadata_standards import MetadataStandards

log = logging.getLogger(__name__)

class JSONToMarkdownConverter:
    """Converts extracted JSON files to GraphRAG-compatible markdown."""
    
    def __init__(self, json_dir: Path, markdown_dir: Path):
        self.json_dir = Path(json_dir)
        self.markdown_dir = Path(markdown_dir)
        self.markdown_dir.mkdir(parents=True, exist_ok=True)
    
    def convert_all_json_files(self) -> List[Path]:
        """Convert all stage3 JSON files to markdown."""
        # Look for stage3 files in organized structure first
        stage3_dir = self.json_dir / "stage3"
        if stage3_dir.exists():
            stage3_files = list(stage3_dir.glob("*_stage3_ontology.json"))
        else:
            # Fallback to flat structure for backward compatibility
            stage3_files = list(self.json_dir.glob("*_stage3_ontology.json"))
        
        log.info(f"Found {len(stage3_files)} stage3 JSON files to convert")
        
        converted_files = []
        for json_file in stage3_files:
            try:
                markdown_file = self.convert_json_file(json_file)
                if markdown_file:
                    converted_files.append(markdown_file)
            except Exception as e:
                log.error(f"Failed to convert {json_file.name}: {e}")
        
        log.info(f"Successfully converted {len(converted_files)} files to markdown")
        return converted_files
    
    def convert_json_file(self, json_file: Path) -> Path:
        """Convert a single JSON file to markdown."""
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Generate markdown filename
        base_name = json_file.stem.replace('_stage3_ontology', '')
        markdown_file = self.markdown_dir / f"{base_name}.md"
        
        # Convert to markdown
        markdown_content = self._json_to_markdown(data)
        
        # Write markdown file
        with open(markdown_file, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        log.info(f"Converted {json_file.name} → {markdown_file.name}")
        return markdown_file
    
    def _json_to_markdown(self, data: Dict[str, Any]) -> str:
        """Convert JSON data to GraphRAG markdown format."""
        
        # Extract and validate metadata
        metadata = {
            'meeting_date': data.get('meeting_date', 'N/A'),
            'document_type': self._determine_doc_type(data.get('source_file', '')),
            'source_file': data.get('source_file', 'Unknown')
        }
        
        # Validate metadata
        metadata = MetadataStandards.validate_metadata(metadata)
        
        # Build YAML header with consistent format
        markdown_parts = [
            "---",
            f"- Meeting Date: {metadata['meeting_date']}",
            f"- Document Type: {metadata['document_type'].upper()}",
            f"- Source File: {metadata['source_file']}",
            "---",
            "",
            "**DOCUMENT METADATA:**",
            "=" * 30,
            ""
        ]
        
        # Add document metadata
        if data.get('title'):
            markdown_parts.append(f"**Title:** {data['title']}")
            markdown_parts.append("")
        
        if data.get('document_number'):
            markdown_parts.append(f"**Document Number:** {data['document_number']}")
            markdown_parts.append("")
        
        if data.get('agenda_item_code'):
            markdown_parts.append(f"**Agenda Item:** {data['agenda_item_code']}")
            markdown_parts.append("")
        
        # Add full text if available
        if data.get('full_text'):
            markdown_parts.extend([
                "",
                "**DOCUMENT CONTENT:**",
                "=" * 30,
                "",
                data['full_text'].strip()
            ])
        
        return "\n".join(markdown_parts)
    
    def _determine_doc_type(self, source_file: str) -> str:
        """Determine document type from source filename."""
        return MetadataStandards.classify_document(source_file)
    
    def _extract_entities(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract entities from the JSON data."""
        entities = []
        
        # Extract from ontology section
        ontology = data.get('ontology', {})
        
        # Get entities from different ontology sections
        for category, items in ontology.items():
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        entities.append({
                            'type': category.upper(),
                            'name': item.get('name', item.get('id', str(item))),
                            'id': item.get('id', ''),
                            'description': item.get('description', '')
                        })
        
        # Also extract from sections for agenda items
        sections = data.get('sections', [])
        for section in sections:
            items = section.get('items', [])
            for item in items:
                item_code = item.get('item_code', '')
                if item_code:
                    entities.append({
                        'type': 'AGENDA_ITEM',
                        'name': item_code,
                        'id': item.get('item_id', ''),
                        'description': item.get('title', '')
                    })
        
        return entities


def convert_json_to_markdown(json_dir: Path, markdown_dir: Path) -> List[Path]:
    """
    Convert all JSON files to markdown format for GraphRAG.
    
    Args:
        json_dir: Directory containing JSON files
        markdown_dir: Directory to write markdown files
        
    Returns:
        List of created markdown files
    """
    converter = JSONToMarkdownConverter(json_dir, markdown_dir)
    return converter.convert_all_json_files()


if __name__ == "__main__":
    # Test conversion
    import sys
    if len(sys.argv) > 2:
        json_dir = Path(sys.argv[1])
        markdown_dir = Path(sys.argv[2])
        converted = convert_json_to_markdown(json_dir, markdown_dir)
        print(f"Converted {len(converted)} files")
    else:
        print("Usage: python json_to_markdown_converter.py <json_dir> <markdown_dir>") 