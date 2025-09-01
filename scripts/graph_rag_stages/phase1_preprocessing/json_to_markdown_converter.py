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

# Import debug flags from main pipeline
try:
    from scripts.graph_rag_stages.main_pipeline import DEBUG_DOCUMENT_FLOW, DEBUG_FILE_DISCOVERY
except ImportError:
    # Fallback if main_pipeline is not available
    DEBUG_DOCUMENT_FLOW = False
    DEBUG_FILE_DISCOVERY = False

class JSONToMarkdownConverter:
    """Converts extracted JSON files to GraphRAG-compatible markdown."""
    
    def __init__(self, json_dir: Path, markdown_dir: Path):
        self.json_dir = Path(json_dir)
        self.markdown_dir = Path(markdown_dir)
        self.markdown_dir.mkdir(parents=True, exist_ok=True)
    
    def convert_all_json_files(self) -> List[Path]:
        """Convert all JSON files to markdown, including stage1 fallbacks (Fix for Issues 1,3,4)."""
        if DEBUG_DOCUMENT_FLOW:
            log.info("🔍 DEBUG [JSON-TO-MD] Starting JSON to Markdown conversion")
            log.info(f"🔍 DEBUG [JSON-TO-MD] Source directory: {self.json_dir}")
            log.info(f"🔍 DEBUG [JSON-TO-MD] Target directory: {self.markdown_dir}")
        
        files_by_pdf = self._discover_all_json_files()
        converted_files = []
        
        if DEBUG_DOCUMENT_FLOW:
            log.info(f"🔍 DEBUG [JSON-TO-MD] Discovered {len(files_by_pdf)} PDF groups")
            total_json_files = sum(len(files) for files in files_by_pdf.values())
            log.info(f"🔍 DEBUG [JSON-TO-MD] Total JSON files found: {total_json_files}")
        
        for pdf_id, json_files in files_by_pdf.items():
            if DEBUG_DOCUMENT_FLOW:
                log.info(f"🔍 DEBUG [JSON-TO-MD] Processing PDF group '{pdf_id}' with {len(json_files)} files")
                for json_file in json_files:
                    log.info(f"🔍 DEBUG [JSON-TO-MD]   File: {json_file.name}")
            
            # Sort by priority: stage3 > enhanced/legal/verbatim > stage2 > stage1
            prioritized = self._prioritize_json_files(json_files)
            
            # Check if we have enhanced files - if so, convert them individually instead of grouping
            enhanced_files = [f for f in json_files if ('_enhanced_ordinance' in f.name or '_enhanced_resolution' in f.name)]
            if enhanced_files:
                if DEBUG_DOCUMENT_FLOW:
                    log.info(f"🔍 DEBUG [JSON-TO-MD] Found {len(enhanced_files)} enhanced files for individual conversion")
                log.info(f"Converting {len(enhanced_files)} enhanced files individually for '{pdf_id}'")
                for enhanced_file in enhanced_files:
                    if DEBUG_DOCUMENT_FLOW:
                        log.info(f"🔍 DEBUG [JSON-TO-MD] Converting enhanced file: {enhanced_file.name}")
                    result = self.convert_json_file(enhanced_file)
                    if result:
                        converted_files.append(result)
                        if DEBUG_DOCUMENT_FLOW:
                            log.info(f"🔍 DEBUG [JSON-TO-MD] ✅ Successfully converted: {result.name}")
                    else:
                        if DEBUG_DOCUMENT_FLOW:
                            log.warning(f"🔍 DEBUG [JSON-TO-MD] ❌ Failed to convert: {enhanced_file.name}")
                continue  # Skip grouped conversion for enhanced files
            
            # Convert using highest priority file (for non-enhanced files only)
            converted = False
            for json_file in prioritized:
                if DEBUG_DOCUMENT_FLOW:
                    log.info(f"🔍 DEBUG [JSON-TO-MD] Attempting conversion of: {json_file.name}")
                result = self.convert_json_file(json_file)
                if result:
                    converted_files.append(result)
                    converted = True
                    if DEBUG_DOCUMENT_FLOW:
                        log.info(f"🔍 DEBUG [JSON-TO-MD] ✅ Successfully converted: {result.name}")
                    break  # Stop after first successful conversion
                else:
                    if DEBUG_DOCUMENT_FLOW:
                        log.warning(f"🔍 DEBUG [JSON-TO-MD] ❌ Failed to convert: {json_file.name}")
            
            if not converted and DEBUG_DOCUMENT_FLOW:
                log.warning(f"🔍 DEBUG [JSON-TO-MD] ❌ NO FILES CONVERTED for PDF group '{pdf_id}'")
            
            if not converted:
                log.warning(f"No suitable JSON found for PDF ID: {pdf_id}")
        
        log.info(f"Successfully converted {len(converted_files)} files to markdown")
        return converted_files
    
    def _discover_all_json_files(self) -> Dict[str, List[Path]]:
        """Discover all JSON files grouped by PDF source (Fix for Issue 3)."""
        files_by_pdf = defaultdict(list)
        
        if DEBUG_FILE_DISCOVERY:
            log.info("🔍 DEBUG [FILE-DISCOVERY] Starting JSON file discovery")
            log.info(f"🔍 DEBUG [FILE-DISCOVERY] Searching in: {self.json_dir}")
        
        # Check all possible locations
        dirs_to_check = [
            (self.json_dir / "stage3", "*_stage3_ontology.json"),
            (self.json_dir / "stage2", "*_stage2_agenda.json"),
            (self.json_dir / "stage1", "*_stage1_ocr.json"),
            (self.json_dir / "legal", "*_enhanced_*.json"),
            (self.json_dir / "verbatim", "*_verbatim_transcript.json"),
        ]
        
        for dir_path, pattern in dirs_to_check:
            if DEBUG_FILE_DISCOVERY:
                log.info(f"🔍 DEBUG [FILE-DISCOVERY] Checking {dir_path.name}/ with pattern {pattern}")
                log.info(f"🔍 DEBUG [FILE-DISCOVERY]   Directory exists: {dir_path.exists()}")
            
            if dir_path.exists():
                json_files = list(dir_path.glob(pattern))
                if DEBUG_FILE_DISCOVERY:
                    log.info(f"🔍 DEBUG [FILE-DISCOVERY]   Found {len(json_files)} files matching pattern")
                
                for json_file in json_files:
                    if DEBUG_FILE_DISCOVERY:
                        log.info(f"🔍 DEBUG [FILE-DISCOVERY]     Processing: {json_file.name}")
                    
                    pdf_id = self._extract_pdf_identifier(json_file)
                    files_by_pdf[pdf_id].append(json_file)
                    
                    if DEBUG_FILE_DISCOVERY:
                        log.info(f"🔍 DEBUG [FILE-DISCOVERY]     ✅ Mapped to PDF ID: {pdf_id}")
            elif DEBUG_FILE_DISCOVERY:
                log.info(f"🔍 DEBUG [FILE-DISCOVERY]   ❌ Directory does not exist")
        
        if DEBUG_FILE_DISCOVERY:
            log.info("🔍 DEBUG [FILE-DISCOVERY] Starting fallback flat search")
        
        # Fallback flat search
        flat_patterns = ["*_stage3_ontology.json", "*_stage2_agenda.json", "*_stage1_ocr.json"]
        for pattern in flat_patterns:
            if DEBUG_FILE_DISCOVERY:
                log.info(f"🔍 DEBUG [FILE-DISCOVERY] Flat search with pattern: {pattern}")
            
            flat_files = list(self.json_dir.glob(pattern))
            if DEBUG_FILE_DISCOVERY:
                log.info(f"🔍 DEBUG [FILE-DISCOVERY]   Found {len(flat_files)} files in flat search")
            
            for json_file in flat_files:
                pdf_id = self._extract_pdf_identifier(json_file)
                if json_file not in files_by_pdf[pdf_id]:  # Avoid duplicates
                    files_by_pdf[pdf_id].append(json_file)
                    if DEBUG_FILE_DISCOVERY:
                        log.info(f"🔍 DEBUG [FILE-DISCOVERY]   ✅ Added from flat search: {json_file.name}")
        
        if DEBUG_FILE_DISCOVERY:
            log.info(f"🔍 DEBUG [FILE-DISCOVERY] DISCOVERY COMPLETE")
            log.info(f"🔍 DEBUG [FILE-DISCOVERY] Total PDF groups: {len(files_by_pdf)}")
            total_files = sum(len(files) for files in files_by_pdf.values())
            log.info(f"🔍 DEBUG [FILE-DISCOVERY] Total JSON files: {total_files}")
            for pdf_id, files in files_by_pdf.items():
                log.info(f"🔍 DEBUG [FILE-DISCOVERY]   PDF ID '{pdf_id}': {len(files)} files")
        
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
        original_stem = json_file.stem
        stem = json_file.stem
        # Remove common suffixes
        for suffix in ['_stage3_ontology', '_stage2_agenda', '_stage1_ocr', '_enhanced_ordinance', '_enhanced_resolution', '_verbatim_transcript']:
            stem = stem.replace(suffix, '')
        # Clean extra parts like dates or " - "
        stem = re.sub(r'\s*-\s*\d{2}_\d{2}_\d{4}', '', stem)
        stem = re.sub(r'^\d{2}_\d{2}_\d{4}\s*-\s*', '', stem)
        result = stem.strip()
        return result
    
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
        source_file = data.get('Source_File_Name', data.get('source_file', '')).lower()
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
        original_stem = json_file.stem
        base_name = json_file.stem
        
        # For enhanced files, preserve the enhanced suffix in the markdown filename
        if '_enhanced_ordinance' in original_stem or '_enhanced_resolution' in original_stem:
            # Keep the enhanced suffix
            base_name = original_stem
        else:
            # For non-enhanced files, strip all suffixes as before
            base_name = original_stem.replace('_stage3_ontology', '').replace('_enhanced_ordinance', '').replace('_enhanced_resolution', '').replace('_verbatim_transcript', '').replace('_stage2_agenda', '').replace('_stage1_ocr', '')
        
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
        elif "_enhanced_legal_documents" in json_file.name:
            markdown_content = self._create_legal_collection_markdown(data)
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
        
        # Extract metadata - DON'T reassign the tuple to metadata
        source_file_name = data.get('Source_File_Name', data.get('source_file', ''))
        source_file_path = data.get('Source_File_Path', data.get('file_path', ''))
        metadata = {
            'meeting_date': data.get('meeting_date', data.get('Meeting_Date', 'N/A')),
            'document_type': self._determine_doc_type(source_file_name, source_file_path),
            'source_file': source_file_name
        }
        
        # Validate metadata (but don't reassign)
        is_valid, missing_fields = MetadataStandards.validate_metadata(metadata)
        if not is_valid:
            log.warning(f"Missing metadata fields: {missing_fields}")
        
        # Standardize the metadata
        metadata = MetadataStandards.standardize_metadata(data)
        
        # Get values with backward compatibility
        meeting_date = metadata.get('Meeting_Date', metadata.get('meeting_date', 'N/A'))
        document_type = metadata.get('Document_Type', metadata.get('document_type', 'document')).upper()
        source_file_name = metadata.get('Source_File_Name', metadata.get('source_file', 'Unknown'))
        source_file_path = metadata.get('Source_File_Path', metadata.get('file_path', 'Unknown'))
        
        # Build YAML header with consistent format
        markdown_parts = [
            "---",
            f"- Meeting_Date: {meeting_date}",
            f"- Document_Type: {document_type}",
            f"- Source_File_Name: {source_file_name}",
            f"- Source_File_Path: {source_file_path}",
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
        metadata_dict = data.get('metadata', {})
        legal_meta = data.get('legal_metadata', {})
        
        # Standardize metadata
        metadata = MetadataStandards.standardize_metadata(data)
        
        # Get standardized metadata with backward compatibility
        meeting_date = metadata.get('Meeting_Date', metadata.get('meeting_date', 'N/A'))
        document_type = metadata.get('Document_Type', metadata.get('document_type', 'legal')).upper()
        source_file_name = metadata.get('Source_File_Name', metadata.get('source_file', 'Unknown'))
        source_file_path = metadata.get('Source_File_Path', metadata.get('file_path', 'Unknown'))
        
        parts = [
            "---",
            f"- Meeting_Date: {meeting_date}",
            f"- Document_Type: {document_type}",
            f"- Document_Number: {data.get('document_number', 'N/A')}",
            f"- Agenda_Item: {data.get('agenda_item_code', 'N/A')}",
            f"- Source_File_Name: {source_file_name}",
            f"- Source_File_Path: {source_file_path}",
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
    
    def _create_legal_collection_markdown(self, data: Dict[str, Any]) -> str:
        """Create markdown for legal collection files."""
        # Standardize metadata
        metadata = MetadataStandards.standardize_metadata(data)
        
        # Get standardized metadata with backward compatibility
        meeting_date = metadata.get('Meeting_Date', metadata.get('meeting_date', 'N/A'))
        source_file_name = metadata.get('Source_File_Name', metadata.get('source_file', 'Unknown'))
        source_file_path = metadata.get('Source_File_Path', metadata.get('file_path', 'Unknown'))
        
        parts = [
            "---",
            f"- Meeting_Date: {meeting_date}",
            f"- Document_Type: LEGAL_COLLECTION",
            f"- Source_File_Name: {source_file_name}",
            f"- Source_File_Path: {source_file_path}",
            "---",
            "",
            "**FULL CONTENT:**",
            json.dumps(data.get('documents', []), indent=2)  # Simple dump of documents
        ]
        return "\n".join(parts)
    
    def _create_verbatim_markdown(self, data: Dict[str, Any]) -> str:
        """Create markdown for verbatims (new, with timestamps if available)."""
        # Standardize metadata
        metadata = MetadataStandards.standardize_metadata(data)
        
        # Get standardized metadata with backward compatibility
        meeting_date = metadata.get('Meeting_Date', metadata.get('meeting_date', 'N/A'))
        source_file_name = metadata.get('Source_File_Name', metadata.get('source_file', 'Unknown'))
        source_file_path = metadata.get('Source_File_Path', metadata.get('file_path', 'Unknown'))
        
        parts = [
            "---",
            f"- Meeting_Date: {meeting_date}",
            f"- Document_Type: VERBATIM_TRANSCRIPT",
            f"- Agenda_Items: {', '.join(data.get('item_codes', [])) or 'N/A'}",
            f"- Source_File_Name: {source_file_name}",
            f"- Source_File_Path: {source_file_path}",
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
        # Standardize metadata
        metadata = MetadataStandards.standardize_metadata(data)
        
        # Get standardized metadata with backward compatibility
        meeting_date = metadata.get('Meeting_Date', metadata.get('meeting_date', 'N/A'))
        source_file_name = metadata.get('Source_File_Name', metadata.get('source_file', 'Unknown'))
        source_file_path = metadata.get('Source_File_Path', metadata.get('file_path', 'Unknown'))
        
        parts = [
            "---",
            f"- Meeting_Date: {meeting_date}",
            f"- Document_Type: AGENDA",
            f"- Source_File_Name: {source_file_name}",
            f"- Source_File_Path: {source_file_path}",
            f"- Processed_Stage: Stage 2 (Agenda Extraction)",
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
        # Standardize metadata
        metadata = MetadataStandards.standardize_metadata(data)
        
        # Get standardized metadata with backward compatibility
        meeting_date = metadata.get('Meeting_Date', metadata.get('meeting_date', 'N/A'))
        source_file_name = metadata.get('Source_File_Name', metadata.get('source_file', 'Unknown'))
        source_file_path = metadata.get('Source_File_Path', metadata.get('file_path', 'Unknown'))
        
        parts = [
            "---",
            f"- Meeting_Date: {meeting_date}",
            f"- Document_Type: {doc_type.upper()}",
            f"- Source_File_Name: {source_file_name}",
            f"- Source_File_Path: {source_file_path}",
            f"- Processed_Stage: Stage 1 (OCR Extraction)",
            "---",
            "",
            "**DOCUMENT METADATA:**",
            f"- Type: {doc_type.upper()}",
            "",
            "**FULL TEXT:**",
            data.get('full_text', 'No content available.')
        ]
        return "\n".join(parts)
    
    def _determine_doc_type(self, source_file: str, file_path: str = "") -> str:
        """Determine document type from source filename and path."""
        return MetadataStandards.classify_document(source_file, "", file_path)
    
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


def convert_json_to_markdown(json_source_dir: Path, markdown_output_dir: Path) -> List[Path]:
    """Convert JSON extraction output to markdown files for NER processing."""
    markdown_output_dir.mkdir(parents=True, exist_ok=True)
    converted_files = []
    
    # MODIFIED: Process type-based directories with fallback
    type_dirs = {
        'agenda': 'AGENDA',
        'legal': 'LEGAL_DOCUMENT',
        'verbatim': 'TRANSCRIPT'
    }
    
    # Also check stage directories for backward compatibility
    fallback_dirs = {
        'stage3': 'AGENDA',
        'stage2': 'AGENDA',
        'stage1': 'DOCUMENT'
    }
    
    # Helper to generate markdown from JSON
    def _generate_markdown_from_json(data: Dict[str, Any], doc_type_label: str) -> str:
        # Minimal, consistent structure used by downstream NER
        from scripts.graph_rag_stages.common.metadata_standards import MetadataStandards
        metadata = MetadataStandards.standardize_metadata(data)
        meeting_date = metadata.get('Meeting_Date', metadata.get('meeting_date', 'N/A'))
        source_file_name = metadata.get('Source_File_Name', metadata.get('source_file', 'Unknown'))
        source_file_path = metadata.get('Source_File_Path', metadata.get('file_path', 'Unknown'))
        parts = [
            '---',
            f"- Meeting_Date: {meeting_date}",
            f"- Document_Type: {doc_type_label}",
            f"- Source_File_Name: {source_file_name}",
            f"- Source_File_Path: {source_file_path}",
            '---',
            '',
            '**FULL TEXT:**',
            data.get('full_text', 'No content available.')
        ]
        return "\n".join(parts)
    
    # Process type-based directories first
    for dir_name, doc_type_label in type_dirs.items():
        source_dir = json_source_dir / dir_name
        if source_dir.exists():
            for json_file in source_dir.glob('*.json'):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Generate markdown with proper metadata
                    markdown_content = _generate_markdown_from_json(data, doc_type_label)
                    
                    # Save markdown file
                    output_file = markdown_output_dir / f"{json_file.stem}.md"
                    with open(output_file, 'w', encoding='utf-8') as f:
                        f.write(markdown_content)
                    
                    converted_files.append(output_file)
                    log.debug(f"Converted {json_file.name} to markdown")
                    
                except Exception as e:
                    log.error(f"Failed to convert {json_file.name}: {e}")
    
    # Process fallback stage directories if type dirs don't exist
    if not converted_files:
        for dir_name, doc_type_label in fallback_dirs.items():
            source_dir = json_source_dir / dir_name
            if source_dir.exists():
                for json_file in source_dir.glob('*.json'):
                    try:
                        with open(json_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        markdown_content = _generate_markdown_from_json(data, doc_type_label)
                        output_file = markdown_output_dir / f"{json_file.stem}.md"
                        
                        with open(output_file, 'w', encoding='utf-8') as f:
                            f.write(markdown_content)
                        
                        converted_files.append(output_file)
                        log.debug(f"Converted {json_file.name} to markdown (from {dir_name})")
                        
                    except Exception as e:
                        log.error(f"Failed to convert {json_file.name}: {e}")
    
    log.info(f"Converted {len(converted_files)} JSON files to markdown")
    return converted_files


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