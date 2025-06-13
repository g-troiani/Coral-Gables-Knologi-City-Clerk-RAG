"""
Generic document processor that extracts text and links documents to agenda items.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
import re
from datetime import datetime

from .pdf_extractor import PDFExtractor
from ..common.utils import get_llm_client, call_llm_with_retry, sanitize_filename

log = logging.getLogger(__name__)


class DocumentLinker:
    """Links ordinance and resolution documents to agenda items."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.pdf_extractor = PDFExtractor()
        self.llm_client = get_llm_client()
        self.model = "gpt-4"

    async def extract_and_save_document(self, pdf_path: Path) -> None:
        """Extract and save a generic document with agenda item linking."""
        log.info(f"📄 Processing document: {pdf_path.name}")
        
        # Extract text using base PDF extractor
        full_text, pages = self.pdf_extractor.extract_text_from_pdf(pdf_path)
        if not full_text:
            log.warning(f"No text extracted from {pdf_path.name}, skipping.")
            return

        # Try to extract document metadata
        doc_metadata = await self._extract_document_metadata(full_text, pdf_path)
        
        # Try to find linked agenda item
        agenda_item_code = await self._extract_agenda_item_code(full_text, pdf_path.stem)
        
        # Create document data structure
        document_data = {
            'source_file': pdf_path.name,
            'doc_id': self._generate_doc_id(pdf_path),
            'full_text': full_text,
            'document_type': doc_metadata.get('document_type', 'document'),
            'title': doc_metadata.get('title', pdf_path.stem),
            'agenda_item_code': agenda_item_code,
            'metadata': {
                'extraction_method': 'docling',
                'num_pages': len(pages),
                'total_chars': len(full_text),
                'extraction_timestamp': datetime.now().isoformat(),
                **doc_metadata
            }
        }

        # Save as enriched markdown
        self._save_as_markdown(pdf_path, document_data)

    async def _extract_document_metadata(self, text: str, pdf_path: Path) -> Dict:
        """Extract document metadata using pattern matching and LLM assistance."""
        metadata = {}
        
        # Determine document type from filename and content
        filename_lower = pdf_path.name.lower()
        text_sample = text[:2000].lower()
        
        if 'ordinance' in filename_lower or 'ordinance' in text_sample:
            metadata['document_type'] = 'ordinance'
        elif 'resolution' in filename_lower or 'resolution' in text_sample:
            metadata['document_type'] = 'resolution'
        elif 'minutes' in filename_lower or 'minutes' in text_sample:
            metadata['document_type'] = 'minutes'
        elif 'transcript' in filename_lower or 'transcript' in text_sample:
            metadata['document_type'] = 'transcript'
        else:
            metadata['document_type'] = 'document'
        
        # Extract title
        metadata['title'] = self._extract_title(text)
        
        # Extract date information
        date_info = self._extract_date_info(text, pdf_path)
        metadata.update(date_info)
        
        # Extract document number from filename if present
        doc_number_match = re.search(r'(\d{4}-\d{2,})', pdf_path.name)
        if doc_number_match:
            metadata['document_number'] = doc_number_match.group(1)
        
        return metadata

    async def _extract_agenda_item_code(self, text: str, document_id: str) -> Optional[str]:
        """Extract agenda item code from document text using LLM."""
        log.info(f"🧠 Extracting agenda item code for {document_id}")
        
        # Prepare focused prompt for agenda item extraction
        messages = [
            {
                "role": "system",
                "content": """You are an expert at finding agenda item references in city council documents.
Look for agenda item codes that typically appear in formats like:
- (Agenda Item: E-1)
- Agenda Item E-3
- Item H-3
- H.-3. or E.-2.
- E-2 (simple format)

Search the ENTIRE document carefully. The agenda item can appear anywhere.
Respond with just the code (e.g., "E-2") or "NOT_FOUND" if none exists."""
            },
            {
                "role": "user",
                "content": f"Find the agenda item code in this document:\n\n{text[:8000]}"  # Limit length
            }
        ]
        
        try:
            response = await call_llm_with_retry(
                self.llm_client,
                messages,
                model=self.model,
                temperature=0.1
            )
            
            # Clean and normalize the response
            code = response.strip()
            if code and code != "NOT_FOUND":
                normalized_code = self._normalize_item_code(code)
                log.info(f"✅ Found agenda item code: {normalized_code}")
                return normalized_code
            else:
                log.info(f"❌ No agenda item code found")
                return None
                
        except Exception as e:
            log.error(f"Failed to extract agenda item code: {e}")
            return None

    def _normalize_item_code(self, code: str) -> str:
        """Normalize item code to consistent format (e.g., E-1)."""
        if not code:
            return code
        
        # Remove trailing dots and spaces
        code = code.rstrip('. ')
        
        # Remove dots between letter and dash: "E.-1" -> "E-1"
        code = re.sub(r'([A-Z])\.(-)', r'\1\2', code)
        
        # Handle cases without dash: "E.1" -> "E-1"
        code = re.sub(r'([A-Z])\.(\d)', r'\1-\2', code)
        
        # Remove any remaining dots
        code = code.replace('.', '')
        
        return code

    def _extract_title(self, text: str) -> str:
        """Extract document title from text."""
        # Look for common title patterns
        title_patterns = [
            r'(AN?\s+(ORDINANCE|RESOLUTION)[^.]+\.)',
            r'(ORDINANCE\s+NO\.\s*\d+[^.]+\.)',
            r'(RESOLUTION\s+NO\.\s*\d+[^.]+\.)'
        ]
        
        for pattern in title_patterns:
            title_match = re.search(pattern, text[:2000], re.IGNORECASE)
            if title_match:
                return title_match.group(1).strip()
        
        # Fallback to first substantive line
        lines = text.split('\n')
        for line in lines[:20]:
            line = line.strip()
            if len(line) > 20 and not line.isdigit() and not line.isupper():
                return line[:200]  # Limit length
        
        return "Untitled Document"

    def _extract_date_info(self, text: str, pdf_path: Path) -> Dict:
        """Extract date information from text and filename."""
        date_info = {}
        
        # Try to extract from filename first
        date_match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', pdf_path.name)
        if date_match:
            month, day, year = date_match.groups()
            date_info['meeting_date'] = f"{month}.{day}.{year}"
        
        # Look for "day of Month, Year" pattern in text
        date_pattern = r'day\s+of\s+(\w+),?\s+(\d{4})'
        date_match = re.search(date_pattern, text[:2000])
        if date_match:
            date_info['adoption_date'] = date_match.group(0)
        
        return date_info

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
        """Build document header with metadata."""
        metadata = document_data['metadata']
        
        header = f"""---
DOCUMENT METADATA AND CONTEXT
=============================

**DOCUMENT IDENTIFICATION:**
- Document Type: {metadata.get('document_type', 'DOCUMENT').upper()}
- Title: {document_data.get('title', 'N/A')}
- Source File: {document_data.get('source_file', 'N/A')}

**AGENDA LINKAGE:**
- Linked Agenda Item: {document_data.get('agenda_item_code', 'N/A')}

**DOCUMENT DETAILS:**
- Document Number: {metadata.get('document_number', 'N/A')}
- Meeting Date: {metadata.get('meeting_date', 'N/A')}
- Adoption Date: {metadata.get('adoption_date', 'N/A')}

**SEARCHABLE IDENTIFIERS:**
- DOCUMENT_TYPE: {metadata.get('document_type', 'DOCUMENT').upper()}
- AGENDA_ITEM: {document_data.get('agenda_item_code', 'N/A')}

---

"""
        return header

    def _generate_doc_id(self, pdf_path: Path) -> str:
        """Generate canonical document ID."""
        import hashlib
        return f"DOC_{hashlib.sha1(str(pdf_path.absolute()).encode()).hexdigest()[:12]}" 