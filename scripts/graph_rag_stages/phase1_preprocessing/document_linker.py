"""
Generic document processor that extracts text and links documents to agenda items.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
import re
from datetime import datetime

from .pdf_extractor import PDFExtractor
from ..common.utils import get_llm_client, sanitize_filename, extract_json_with_llm

log = logging.getLogger(__name__)


class DocumentLinker:
    """Links ordinance and resolution documents to agenda items."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.pdf_extractor = PDFExtractor()
        self.llm_client = get_llm_client()
        self.model = "llama-3.3-70b-versatile"

    async def extract_and_save_document(self, pdf_path: Path) -> None:
        """Extract and save a generic document with agenda item linking."""
        log.info(f"📄 Processing document: {pdf_path.name}")
        
        # Extract text using base PDF extractor
        full_text, pages = self.pdf_extractor.extract_text_from_pdf(pdf_path)
        if not full_text:
            log.warning(f"No text extracted from {pdf_path.name}, skipping.")
            return

        # Use the new centralized function to get all metadata in one shot
        json_metadata = await extract_json_with_llm(self.llm_client, full_text, self.model)
        
        # Extract document number from filename if present
        doc_number_match = re.search(r'(\d{4}-\d{2,})', pdf_path.name)
        if doc_number_match:
            document_number = doc_number_match.group(1)
        else:
            document_number = pdf_path.stem
        
        # Create document data structure from the extracted JSON
        document_data = {
            'source_file': pdf_path.name,
            'doc_id': self._generate_doc_id(pdf_path),
            'full_text': full_text,
            'document_type': json_metadata.get('document_type', 'document'),
            'title': json_metadata.get('title', pdf_path.stem),
            # Extract the first agenda item code if available, for linking
            'agenda_item_code': (json_metadata.get('agenda_items', [{}])[0].get('item_code') if json_metadata.get('agenda_items') else None),
            'metadata': {
                'extraction_method': 'docling+llm_json_extract',
                'num_pages': len(pages),
                'total_chars': len(full_text),
                'extraction_timestamp': datetime.now().isoformat(),
                'document_number': document_number,  # Add document number to metadata
                **json_metadata # Embed the entire extracted JSON object here
            }
        }

        # Save as enriched markdown
        self._save_as_markdown(pdf_path, document_data)
        
        # Save the raw JSON output - pass the complete metadata
        self._save_as_json(pdf_path, document_data['metadata'])

    def _save_as_json(self, pdf_path: Path, json_metadata: Dict) -> None:
        """Save the extracted JSON metadata to the json directory."""
        import json
        
        # Create json output directory in the correct location
        # Go up to project root and then into city_clerk_documents/json
        project_root = Path(__file__).resolve().parent.parent.parent.parent
        json_output_dir = project_root / "city_clerk_documents" / "json"
        json_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Use the same filename generation logic as markdown
        doc_type = json_metadata.get('document_type', 'document')
        doc_number = json_metadata.get('document_number', pdf_path.stem)
        
        json_filename = sanitize_filename(f"{doc_type}_{doc_number}.json")
        json_path = json_output_dir / json_filename
        
        # Save the JSON metadata
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_metadata, f, indent=2, ensure_ascii=False)
        
        log.info(f"💾 Saved JSON metadata to: {json_path}")

    def _save_as_markdown(self, pdf_path: Path, document_data: Dict) -> None:
        """Save document as enriched markdown for GraphRAG."""
        # Build header
        header = self._build_document_header(document_data)
        
        # Combine with full text
        full_content = header + "\n\n# DOCUMENT CONTENT\n\n" + document_data.get('full_text', '')
        
        # Generate filename
        doc_type = document_data['metadata'].get('document_type', 'document')
        doc_number = document_data['metadata'].get('document_number', pdf_path.stem)
        
        filename = sanitize_filename(f"{doc_type}_{doc_number}.md")
        md_path = self.output_dir / filename
        
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(full_content)
        
        log.info(f"📝 Saved document markdown to: {md_path}")

    def _build_document_header(self, document_data: Dict) -> str:
        """Build document header with rich metadata from JSON."""
        metadata = document_data.get('metadata', {})
        
        # Create a clean string for agenda items list
        agenda_items_list = metadata.get('agenda_items', [])
        items_str = ', '.join([item.get('item_code', '') for item in agenda_items_list if item.get('item_code')]) or 'N/A'
        
        header = f"""---
DOCUMENT METADATA AND CONTEXT
=============================

**DOCUMENT IDENTIFICATION:**
- Document Type: {document_data.get('document_type', 'DOCUMENT').upper()}
- Title: {document_data.get('title', 'N/A')}
- Source File: {document_data.get('source_file', 'N/A')}

**AGENDA LINKAGE:**
- Linked Agenda Items: {items_str}

**DOCUMENT DETAILS:**
- Document Date: {metadata.get('date', 'N/A')}
- Mayor: {metadata.get('mayor', 'N/A')}
- Vice Mayor: {metadata.get('vice_mayor', 'N/A')}
- Commissioners: {', '.join(metadata.get('commissioners', []))}

**SEARCHABLE IDENTIFIERS:**
- DOCUMENT_TYPE: {document_data.get('document_type', 'DOCUMENT').upper()}
- AGENDA_ITEM: {document_data.get('agenda_item_code', 'N/A')} # Primary linked item

---
"""
        return header

    def _generate_doc_id(self, pdf_path: Path) -> str:
        """Generate canonical document ID."""
        import hashlib
        return f"DOC_{hashlib.sha1(str(pdf_path.absolute()).encode()).hexdigest()[:12]}" 