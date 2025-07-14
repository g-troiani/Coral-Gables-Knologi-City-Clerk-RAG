"""
JSON to Markdown Converter for GraphRAG Pipeline

Converts the extracted JSON files to markdown format expected by GraphRAG indexing.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
import re
from collections import defaultdict
from scripts.graph_rag_stages.common.metadata_standards import MetadataStandards

log = logging.getLogger(__name__)

class JSONToMarkdownConverter:
    """Converts extracted JSON files to GraphRAG-compatible markdown."""
    
    def __init__(self, json_dir: Path, markdown_dir: Path):
        self.json_dir = Path(json_dir)
        self.markdown_dir = Path(markdown_dir)
        self.markdown_dir.mkdir(parents=True, exist_ok=True)
    
    def convert_all_json_files(self) -> List[Path]:
        """Convert all JSON files to markdown, including stage1 fallbacks (Fix for Issues 1,3,4)."""
        files_by_pdf = self._discover_all_json_files()
        converted_files = []
        
        for pdf_id, json_files in files_by_pdf.items():
            # Sort by priority: stage3 > enhanced/legal/verbatim > stage2 > stage1
            prioritized = self._prioritize_json_files(json_files)
            
            # Convert using highest priority file
            converted = False
            for json_file in prioritized:
                result = self.convert_json_file(json_file)
                if result:
                    converted_files.append(result)
                    converted = True
                    break  # One markdown per PDF
            
            if not converted:
                log.warning(f"No suitable JSON found for PDF ID: {pdf_id}")
        
        log.info(f"Successfully converted {len(converted_files)} files to markdown")
        return converted_files
    
    def _discover_all_json_files(self) -> Dict[str, List[Path]]:
        """Discover all JSON files grouped by PDF source (Fix for Issue 3)."""
        files_by_pdf = defaultdict(list)
        
        # Check all possible locations
        dirs_to_check = [
            (self.json_dir / "stage3", "*_stage3_ontology.json"),
            (self.json_dir / "stage2", "*_stage2_agenda.json"),
            (self.json_dir / "stage1", "*_stage1_ocr.json"),
            (self.json_dir / "legal", "*_enhanced_*.json"),
            (self.json_dir / "verbatim", "*_verbatim_transcript.json"),
        ]
        
        for dir_path, pattern in dirs_to_check:
            if dir_path.exists():
                for json_file in dir_path.glob(pattern):
                    pdf_id = self._extract_pdf_identifier(json_file)
                    files_by_pdf[pdf_id].append(json_file)
        
        # Fallback flat search
        flat_patterns = ["*_stage3_ontology.json", "*_stage2_agenda.json", "*_stage1_ocr.json"]
        for pattern in flat_patterns:
            for json_file in self.json_dir.glob(pattern):
                pdf_id = self._extract_pdf_identifier(json_file)
                if json_file not in files_by_pdf[pdf_id]:  # Avoid duplicates
                    files_by_pdf[pdf_id].append(json_file)
        
        return dict(files_by_pdf)
    
    def _prioritize_json_files(self, json_files: List[Path]) -> List[Path]:
        """Sort files by priority: stage3 > enhanced/legal/verbatim > stage2 > stage1."""
        priority_order = {
            '_stage3_ontology.json': 0,
            '_enhanced_ordinance.json': 1,
            '_enhanced_resolution.json': 1,
            '_verbatim_transcript.json': 1,
            '_stage2_agenda.json': 2,
            '_stage1_ocr.json': 3
        }
        
        def get_priority(file: Path) -> int:
            for suffix, prio in priority_order.items():
                if file.name.endswith(suffix):
                    return prio
            return 99  # Lowest priority for unknowns
        
        return sorted(json_files, key=get_priority)
    
    def _extract_pdf_identifier(self, json_file: Path) -> str:
        """Extract unique PDF identifier from JSON filename (Fix for Issue 3 collisions)."""
        stem = json_file.stem
        # Remove common suffixes
        for suffix in ['_stage3_ontology', '_stage2_agenda', '_stage1_ocr', '_enhanced_ordinance', '_enhanced_resolution', '_verbatim_transcript']:
            stem = stem.replace(suffix, '')
        # Clean extra parts like dates or " - "
        stem = re.sub(r'\s*-\s*\d{2}_\d{2}_\d{4}', '', stem)
        stem = re.sub(r'^\d{2}_\d{2}_\d{4}\s*-\s*', '', stem)
        return stem.strip()
    
    def _get_document_type_from_stage1(self, json_file: Path, data: Dict) -> str:
        """Determine document type from stage1 file (Fix for Issue 2)."""
        filename = json_file.name.lower()
        
        # Check filename patterns
        if any(prefix in filename for prefix in ['soe', 'cg', 'eo', 'cao', 'ordinance', 'ord']):
            return 'ordinance'
        elif 'resolution' in filename or 'res' in filename:
            return 'resolution'
        elif 'verbatim' in filename or 'transcript' in filename:
            return 'verbatim_transcript'
        elif 'agenda' in filename:
            return 'agenda'
        elif 'minutes' in filename:
            return 'minutes'
        
        # Check source file path in data
        source_file = data.get('source_file', '').lower()
        if 'ordinances' in source_file:
            return 'ordinance'
        elif 'resolutions' in source_file:
            return 'resolution'
        elif 'verbatim' in source_file or 'transcripts' in source_file:
            return 'verbatim_transcript'
        elif 'agenda' in source_file:
            return 'agenda'
        
        return 'document'
    
    def _process_file(self, json_file: Path) -> List[Path]:
        """Process single file, skip if markdown exists."""
        markdown_file = self._get_markdown_path(json_file)
        if markdown_file.exists():
            log.debug(f"Skipping {json_file.name} - markdown already exists")
            return []
        
        return [self.convert_json_file(json_file)] if self.convert_json_file(json_file) else []
    
    def _get_markdown_path(self, json_file: Path) -> Path:
        """Generate markdown path based on JSON type."""
        base_name = json_file.stem.replace('_stage3_ontology', '').replace('_enhanced_ordinance', '').replace('_enhanced_resolution', '').replace('_verbatim_transcript', '').replace('_stage2_agenda', '').replace('_stage1_ocr', '')
        return self.markdown_dir / f"{base_name}.md"
    
    def convert_json_file(self, json_file: Path) -> Optional[Path]:
        """Convert single JSON, dispatch by type (extended for stage1)."""
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Generate markdown filename
        markdown_file = self._get_markdown_path(json_file)
        
        # Dispatch by type
        if "_stage3_ontology" in json_file.name:
            markdown_content = self._create_stage3_markdown(data, json_file.parent.name)
        elif "_enhanced_ordinance" in json_file.name or "_enhanced_resolution" in json_file.name:
            markdown_content = self._create_legal_markdown(data)
        elif "_verbatim_transcript" in json_file.name:
            markdown_content = self._create_verbatim_markdown(data)
        elif "_stage2_agenda" in json_file.name:
            markdown_content = self._create_stage2_markdown(data)  # Existing or add simple
        elif "_stage1_ocr" in json_file.name:
            doc_type = self._get_document_type_from_stage1(json_file, data)
            markdown_content = self._create_stage1_markdown(data, doc_type)
        else:
            log.warning(f"Unknown JSON type: {json_file.name} - skipping")
            return None
        
        # Write markdown file
        with open(markdown_file, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        log.info(f"Converted {json_file.name} → {markdown_file.name}")
        return markdown_file
    
    def _create_stage3_markdown(self, data: Dict[str, Any], subdir: str) -> str:
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
    
    def _create_legal_markdown(self, data: Dict[str, Any]) -> str:
        """Create markdown for ordinances/resolutions (new, tailored)."""
        metadata = data.get('metadata', {})
        legal_meta = data.get('legal_metadata', {})
        
        parts = [
            "---",
            f"- Meeting Date: {data.get('meeting_date', 'N/A')}",
            f"- Document Type: {data.get('document_type', 'legal').upper()}",
            f"- Document Number: {data.get('document_number', 'N/A')}",
            f"- Agenda Item: {data.get('agenda_item_code', 'N/A')}",
            f"- Source File: {data.get('source_file', 'Unknown')}",
            "---",
            "",
            "**DOCUMENT METADATA:**",
            f"- Title: {data.get('title', 'N/A')}",
            f"- Passed First Reading: {legal_meta.get('passed_first_reading', False)}",
            f"- Passed Second Reading: {legal_meta.get('passed_second_reading', False)}",
            f"- Outcome: {legal_meta.get('outcome_status', 'Pending')}",
            f"- Vote: Yeas {legal_meta.get('vote_details', {}).get('yeas', 'N/A')}, Nays {legal_meta.get('vote_details', {}).get('nays', 'N/A')}",
            "",
            "**FULL TEXT:**",
            data.get('full_text', 'No content available.')
        ]
        return "\n".join(parts)
    
    def _create_verbatim_markdown(self, data: Dict[str, Any]) -> str:
        """Create markdown for verbatims (new, with timestamps if available)."""
        parts = [
            "---",
            f"- Meeting Date: {data.get('meeting_date', 'N/A')}",
            f"- Document Type: VERBATIM_TRANSCRIPT",
            f"- Agenda Items: {', '.join(data.get('item_codes', [])) or 'N/A'}",
            f"- Source File: {data.get('source_file', 'Unknown')}",
            "---",
            "",
            "**TRANSCRIPT METADATA:**",
            f"- Type: {data.get('transcript_type', 'N/A')}",
            f"- Sections: {', '.join(data.get('section_codes', [])) or 'N/A'}",
            "",
            "**FULL TRANSCRIPT:**",
            data.get('full_text', 'No content available.')
        ]
        return "\n".join(parts)
    
    def _create_stage2_markdown(self, data: Dict[str, Any]) -> str:
        """Create markdown from stage2 agenda data (simple structure)."""
        parts = [
            "---",
            f"- Meeting Date: {data.get('meeting_date', 'N/A')}",
            f"- Document Type: AGENDA",
            f"- Source File: {data.get('source_file', 'Unknown')}",
            f"- Processed Stage: Stage 2 (Agenda Extraction)",
            "---",
            "",
            "**DOCUMENT METADATA:**",
            f"- Type: AGENDA",
            "",
            "**FULL TEXT:**",
            data.get('full_text', 'No content available.')
        ]
        return "\n".join(parts)
    
    def _create_stage1_markdown(self, data: Dict[str, Any], doc_type: str) -> str:
        """Create markdown from stage1 OCR data (new, basic structure)."""
        parts = [
            "---",
            f"- Meeting Date: {data.get('meeting_date', 'N/A')}",
            f"- Document Type: {doc_type.upper()}",
            f"- Source File: {data.get('source_file', 'Unknown')}",
            f"- Processed Stage: Stage 1 (OCR Extraction)",
            "---",
            "",
            "**DOCUMENT METADATA:**",
            f"- Type: {doc_type.upper()}",
            "",
            "**FULL TEXT:**",
            data.get('full_text', 'No content available.')
        ]
        return "\n".join(parts)
    
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