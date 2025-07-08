"""
JSON to Markdown Converter for GraphRAG Pipeline

Converts the extracted JSON files to markdown format expected by GraphRAG indexing.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any
import re

log = logging.getLogger(__name__)

class JSONToMarkdownConverter:
    """Converts extracted JSON files to GraphRAG-compatible markdown."""
    
    def __init__(self, json_dir: Path, markdown_dir: Path):
        self.json_dir = Path(json_dir)
        self.markdown_dir = Path(markdown_dir)
        self.markdown_dir.mkdir(parents=True, exist_ok=True)
    
    def convert_all_json_files(self) -> List[Path]:
        """Convert all stage3 JSON files to markdown."""
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
        
        # Extract metadata
        source_file = data.get('source_file', 'Unknown')
        meeting_date = data.get('meeting_date', 'N/A')
        doc_type = self._determine_doc_type(source_file)
        
        # Extract entities from ontology
        entities = self._extract_entities(data)
        
        # Build PROPER YAML header that markdown_chunker.py can read
        markdown_parts = [
            "---",
            f"- Meeting Date: {meeting_date}",
            f"- Document Type: {doc_type.upper()}",
            f"- Source File: {source_file}",
            "---",
            "",
            "DOCUMENT METADATA AND CONTEXT",
            "=============================",
            "",
            "**DOCUMENT IDENTIFICATION:**",
            f"- Document Type: {doc_type.upper()}",
            f"- Meeting Date: {meeting_date}",
            f"- Source File: {source_file}",
            ""
        ]
        
        # Add entities section
        if entities:
            markdown_parts.extend([
                "**ENTITIES IN THIS DOCUMENT:**"
            ])
            
            # Group entities by type
            entity_groups = {}
            for entity in entities:
                entity_type = entity.get('type', 'UNKNOWN')
                if entity_type not in entity_groups:
                    entity_groups[entity_type] = []
                entity_groups[entity_type].append(entity)
            
            # Add entities by type
            for entity_type, type_entities in sorted(entity_groups.items()):
                for entity in type_entities[:20]:  # Limit to first 20 per type
                    name = entity.get('name', entity.get('id', 'Unknown'))
                    markdown_parts.append(f"- {entity_type}: {name}")
            
            markdown_parts.append("")
        
        # Add document content
        markdown_parts.extend([
            "**DOCUMENT CONTENT:**",
            "=" * 20,
            ""
        ])
        
        # Add the full text content
        full_text = data.get('full_text', '')
        if not full_text or "CONVERTED" in full_text or "SKIPPED" in full_text:
            pages = data.get('pages', [])
            if pages:
                full_text = "\n\n".join(page.get('text', '') for page in pages)
            else:
                full_text = "No content available."

        # Clean up the text slightly
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)  # Reduce excessive newlines
        full_text = full_text.strip()
        markdown_parts.append(full_text)
        
        # Add agenda sections if available
        sections = data.get('sections', [])
        if sections:
            markdown_parts.extend([
                "",
                "",
                "**STRUCTURED AGENDA ITEMS:**",
                "=" * 30,
                ""
            ])
            
            for section in sections:
                section_name = section.get('section_name', 'Unknown Section')
                markdown_parts.append(f"## {section_name}")
                
                items = section.get('items', [])
                for item in items:
                    item_code = item.get('item_code', '')
                    title = item.get('title', 'No title')
                    doc_ref = item.get('document_reference', '')
                    
                    if item_code:
                        markdown_parts.append(f"### {item_code} - {doc_ref}")
                    markdown_parts.append(f"{title}")
                    markdown_parts.append("")
        
        return "\n".join(markdown_parts)
    
    def _determine_doc_type(self, source_file: str) -> str:
        """Determine document type from source filename."""
        source_lower = source_file.lower()
        
        if 'agenda' in source_lower:
            return 'agenda'
        elif 'ordinance' in source_lower:
            return 'ordinance'
        elif 'resolution' in source_lower:
            return 'resolution'
        elif 'verbatim' in source_lower or 'transcript' in source_lower:
            return 'verbatim_transcript'
        elif 'minutes' in source_lower:
            return 'minutes'
        else:
            return 'document'
    
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