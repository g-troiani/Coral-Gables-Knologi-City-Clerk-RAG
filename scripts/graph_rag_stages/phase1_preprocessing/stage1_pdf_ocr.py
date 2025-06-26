#!/usr/bin/env python3
"""
Stage 1: PDF Extraction & OCR
Transforms raw PDF documents into structured text with hyperlink extraction
"""

import logging
import json
import hashlib
import re
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import fitz  # PyMuPDF
from datetime import datetime
try:
    from docling.document_converter import DocumentConverter
    from docling.datamodel.base_models import InputFormat
    DOCLING_AVAILABLE = True
except ImportError:
    DOCLING_AVAILABLE = False
    print("⚠️  Docling not available, using PyMuPDF fallback only")

log = logging.getLogger(__name__)

class PDFOCRExtractor:
    """Stage 1: Extract text and structure from PDF documents using Docling + PyMuPDF."""
    
    def __init__(self, output_dir: Path = Path("city_clerk_documents/extracted_json")):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Add comprehensive tracking
        self.processing_stats = {
            'total_discovered': 0,
            'already_processed': 0,
            'needs_processing': 0,
            'missing_both': 0,
            'missing_markdown_only': 0,
            'missing_json_only': 0,
            'processed_this_run': 0,
            'skipped_this_run': 0
        }
        
        # Configure Docling if available
        if DOCLING_AVAILABLE:
            try:
                self.converter = DocumentConverter()
            except Exception as e:
                log.warning(f"⚠️  Failed to initialize Docling converter: {e}")
                self.converter = None
        else:
            self.converter = None
    
    def _get_markdown_path(self, pdf_path: Path, markdown_dir: Path) -> Optional[Path]:
        """Get the correct markdown path for a PDF file using proper naming patterns."""
        pdf_name = pdf_path.name
        
        # Pattern 1: Ordinances (2024-03 - 01_09_2024.pdf -> Ordinance_2024-03.md)
        ordinance_match = re.match(r'^(\d{4}-\d+)\s*-', pdf_name)
        if ordinance_match and 'Ordinances' in str(pdf_path):
            doc_number = ordinance_match.group(1)
            return markdown_dir / f"Ordinance_{doc_number}.md"
        
        # Pattern 2: Resolutions (2024-05 - 02_13_2024.pdf -> Resolution_2024-05.md)
        resolution_match = re.match(r'^(\d{4}-\d+)\s*-', pdf_name)
        if resolution_match and 'Resolutions' in str(pdf_path):
            doc_number = resolution_match.group(1)
            return markdown_dir / f"Resolution_{doc_number}.md"
        
        # Pattern 3: Verbatim Transcripts (01_09_2024 - Verbatim Transcripts - E-4.pdf -> verbatim_01_09_2024_E-4.md)
        verbatim_match = re.match(r'^(\d{2}_\d{2}_\d{4})\s*-\s*Verbatim Transcripts\s*-\s*(.+)\.pdf$', pdf_name)
        if verbatim_match:
            date_part = verbatim_match.group(1)
            item_part = verbatim_match.group(2)
            return markdown_dir / f"verbatim_{date_part}_{item_part}.md"
        
        # Pattern 4: Agendas (Agenda 01.23.2024.pdf -> Agenda 01.23.2024.md)
        if pdf_name.startswith('Agenda '):
            return markdown_dir / pdf_name.replace('.pdf', '.md')
        
        # Pattern 5: Meeting Minutes (08_26_2014 - Meeting Minutes.pdf -> 08_26_2014 - Meeting Minutes.md)
        if 'Meeting Minutes' in pdf_name:
            return markdown_dir / pdf_name.replace('.pdf', '.md')
        
        return None

    def _check_processing_status(self, pdf_path: Path) -> Tuple[str, Dict]:
        """Check the processing status of a PDF file."""
        pdf_stem = pdf_path.stem
        
        # Check for markdown and JSON files
        markdown_dir = Path('city_clerk_documents/extracted_markdown')
        json_dir = Path('city_clerk_documents/extracted_json')
        
        markdown_path = self._get_markdown_path(pdf_path, markdown_dir)
        markdown_exists = markdown_path and markdown_path.exists()
        
        # Check for ANY stage of JSON processing (stage1, stage2, or stage3)
        json_candidates = [
            json_dir / f"{pdf_stem}_stage3_ontology.json",  # Best: has entities and structure
            json_dir / f"{pdf_stem}_stage2_agenda.json",    # Good: has agenda structure  
            json_dir / f"{pdf_stem}_stage1_ocr.json"       # Basic: just OCR text
        ]
        
        best_json_path = None
        for candidate in json_candidates:
            if candidate.exists():
                best_json_path = candidate
                break
        
        json_exists = best_json_path is not None
        json_path_str = str(best_json_path) if best_json_path else str(json_candidates[0])
        
        status_info = {
            'pdf': pdf_path.name,
            'markdown_path': str(markdown_path) if markdown_path else None,
            'markdown_exists': markdown_exists,
            'json_path': json_path_str,
            'json_exists': json_exists
        }
        
        if markdown_exists and json_exists:
            return 'FULLY_PROCESSED', status_info
        elif not markdown_exists and json_exists:
            return 'MISSING_MARKDOWN_ONLY', status_info
        elif markdown_exists and not json_exists:
            return 'MISSING_JSON_ONLY', status_info
        else:  # both missing
            return 'MISSING_BOTH', status_info

    def _should_skip_processing(self, pdf_path: Path) -> Tuple[bool, str, Dict]:
        """Determine if a PDF should be skipped and why."""
        status, status_info = self._check_processing_status(pdf_path)
        
        if status == 'FULLY_PROCESSED':
            return True, f"✅ SKIP: Already fully processed (has both MD + JSON)", status_info
        elif status == 'MISSING_MARKDOWN_ONLY':
            # JSON exists but markdown missing - convert JSON to markdown instead of OCR
            return True, f"🔄 CONVERT: JSON exists, will convert to markdown (no OCR needed)", status_info
        else:
            reason_map = {
                'MISSING_BOTH': f"🔴 PROCESS: Missing both markdown and JSON files",
                'MISSING_JSON_ONLY': f"🟢 PROCESS: Has markdown, missing JSON file"
            }
            return False, reason_map[status], status_info

    def extract_pdf(self, pdf_path: Path) -> Dict[str, Any]:
        """
        Extract text, structure, and hyperlinks from PDF.
        
        Returns:
            Complete extraction data with OCR text, pages, and hyperlinks
        """
        # Check if we should skip this file
        should_skip, reason, status_info = self._should_skip_processing(pdf_path)
        
        # Update statistics  
        self.processing_stats['total_discovered'] += 1
        
        if should_skip:
            self.processing_stats['already_processed'] += 1
            self.processing_stats['skipped_this_run'] += 1
            
            log.info(f"{reason}")
            log.info(f"  📄 PDF: {pdf_path.name}")
            if status_info['markdown_path']:
                log.info(f"  📝 MD: {status_info['markdown_path']} {'✅' if status_info['markdown_exists'] else '❌'}")
            log.info(f"  📊 JSON: {status_info['json_path']} {'✅' if status_info['json_exists'] else '❌'}")
            
            # Check if this is a conversion case
            if "CONVERT:" in reason:
                log.info(f"  🔄 CONVERTING JSON→MD instead of re-running OCR...")
                success = self._convert_json_to_markdown(pdf_path, status_info)
                if success:
                    log.info(f"  ✅ CONVERTED: JSON→MD conversion completed")
                else:
                    log.info(f"  ❌ FAILED: JSON→MD conversion failed")
            else:
                log.info(f"  ⏭️  SKIPPING - Already processed")
            
            # Return a minimal result structure for compatibility
            return {
                "source_file": pdf_path.name,
                "file_path": str(pdf_path),
                "doc_id": self._generate_document_id(pdf_path),
                "full_text": "SKIPPED - Already processed" if "SKIP:" in reason else "CONVERTED - JSON to markdown",
                "pages": [],
                "hyperlinks": [],
                "metadata": {
                    "extraction_method": "skipped_already_processed" if "SKIP:" in reason else "converted_json_to_markdown",
                    "skip_reason": reason,
                    "markdown_path": str(status_info['markdown_path']) if status_info['markdown_path'] else None,
                    "json_path": str(status_info['json_path'])
                }
            }
        
        # File needs processing
        self.processing_stats['needs_processing'] += 1
        
        # Categorize what's missing
        status, _ = self._check_processing_status(pdf_path)
        if status == 'MISSING_BOTH':
            self.processing_stats['missing_both'] += 1
        elif status == 'MISSING_MARKDOWN_ONLY':
            self.processing_stats['missing_markdown_only'] += 1
        elif status == 'MISSING_JSON_ONLY':
            self.processing_stats['missing_json_only'] += 1
        
        log.info(f"{reason}")
        log.info(f"  📄 PDF: {pdf_path.name}")
        if status_info['markdown_path']:
            md_status = "✅" if status_info['markdown_exists'] else "❌"
            log.info(f"  📝 MD: {status_info['markdown_path']} {md_status}")
        json_status = "✅" if status_info['json_exists'] else "❌"
        log.info(f"  📊 JSON: {status_info['json_path']} {json_status}")
        log.info(f"  🔄 PROCESSING...")

        # Proceed with actual processing
        self.processing_stats['processed_this_run'] += 1
        
        log.info(f"📄 Extracting PDF: {pdf_path.name}")
        
        try:
            # Stage 1A: Docling OCR extraction (if available)
            if self.converter:
                docling_result = self._extract_with_docling(pdf_path)
            else:
                docling_result = self._fallback_text_extraction(pdf_path)
            
            # Stage 1B: PyMuPDF hyperlink extraction  
            hyperlinks = self._extract_hyperlinks(pdf_path)
             
            # Stage 1C: Combine results
            extraction_result = {
                "source_file": pdf_path.name,
                "file_path": str(pdf_path),
                "doc_id": self._generate_document_id(pdf_path),
                "full_text": docling_result["full_text"],
                "pages": docling_result["pages"],
                "hyperlinks": hyperlinks,
                "metadata": {
                    "extraction_method": "docling_ocr_pymupdf",
                    "num_pages": len(docling_result["pages"]),
                    "total_chars": len(docling_result["full_text"]),
                    "hyperlink_count": len(hyperlinks),
                    "extraction_timestamp": self._get_timestamp()
                }
            }
            
            # Save extraction result
            output_file = self.output_dir / f"{pdf_path.stem}_stage1_ocr.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extraction_result, f, indent=2, ensure_ascii=False)
            
            log.info(f"✅ Stage 1 complete: {len(docling_result['pages'])} pages, {len(hyperlinks)} hyperlinks")
            return extraction_result
            
        except Exception as e:
            log.error(f"❌ Stage 1 extraction failed for {pdf_path.name}: {e}")
            raise

    def _convert_json_to_markdown(self, pdf_path: Path, status_info: Dict) -> bool:
        """Convert existing JSON to markdown using the sophisticated converter."""
        try:
            # Look for existing JSON files for this PDF
            pdf_stem = pdf_path.stem
            json_dir = Path('city_clerk_documents/extracted_json')
            markdown_dir = Path('city_clerk_documents/extracted_markdown')
            markdown_dir.mkdir(parents=True, exist_ok=True)
            
            # Try different JSON file types in order of preference
            json_candidates = [
                json_dir / f"{pdf_stem}_stage3_ontology.json",  # Best: has entities and structure
                json_dir / f"{pdf_stem}_stage2_agenda.json",    # Good: has agenda structure  
                json_dir / f"{pdf_stem}_stage1_ocr.json"       # Basic: just OCR text
            ]
            
            source_json = None
            for candidate in json_candidates:
                if candidate.exists():
                    source_json = candidate
                    break
            
            if not source_json:
                log.warning(f"No JSON file found for {pdf_path.name}")
                return False
            
            # Load JSON data
            with open(source_json, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # Create markdown file path
            markdown_path = Path(status_info['markdown_path'])
            
            # Use sophisticated conversion for stage3 files
            if "_stage3_ontology" in source_json.name:
                # Use the sophisticated converter logic directly
                markdown_content = self._create_stage3_markdown(json_data, pdf_path, source_json)
                
            # Use enhanced conversion for stage2 files  
            elif "_stage2_agenda" in source_json.name:
                markdown_content = self._create_stage2_markdown(json_data, pdf_path, source_json)
                
            # Use basic but proper conversion for stage1 files
            else:
                markdown_content = self._create_stage1_markdown(json_data, pdf_path, source_json)
            
            # Save markdown file
            with open(markdown_path, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            
            log.debug(f"✅ Converted {source_json.name} → {markdown_path.name}")
            return True
            
        except Exception as e:
            log.error(f"❌ JSON→MD conversion failed for {pdf_path.name}: {e}")
            return False

    def _create_stage3_markdown(self, json_data: Dict, pdf_path: Path, source_json: Path) -> str:
        """Create sophisticated markdown from stage3 ontology data (matches normal pipeline)."""
        # Extract metadata
        source_file = json_data.get('source_file', pdf_path.name)
        meeting_date = json_data.get('meeting_date', 'N/A')
        doc_type = self._determine_doc_type(source_file)
        
        # Extract entities from ontology
        entities = self._extract_entities_from_json(json_data)
        
        # Build markdown content
        markdown_parts = [
            "---",
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
        full_text = json_data.get('full_text', '')
        if full_text:
            # Clean up the text slightly
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)  # Reduce excessive newlines
            full_text = full_text.strip()
            markdown_parts.append(full_text)
        else:
            markdown_parts.append("No content available.")
        
        # Add agenda sections if available
        sections = json_data.get('sections', [])
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

    def _extract_entities_from_json(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract entities from JSON data (same as sophisticated converter)."""
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

    def _create_stage2_markdown(self, json_data: Dict, pdf_path: Path, source_json: Path) -> str:
        """Create enhanced markdown from stage2 agenda data."""
        source_file = json_data.get('source_file', pdf_path.name)
        meeting_date = json_data.get('meeting_date', 'N/A')
        doc_type = self._determine_doc_type(source_file)
        
        # Extract agenda items for entity hints
        agenda_items = json_data.get('agenda_items', [])
        sections = json_data.get('sections', [])
        
        markdown_parts = [
            "---",
            "DOCUMENT METADATA AND CONTEXT",
            "=============================",
            "",
            "**DOCUMENT IDENTIFICATION:**",
            f"- Document Type: {doc_type.upper()}",
            f"- Meeting Date: {meeting_date}",
            f"- Source File: {source_file}",
            f"- Processed Stage: Stage 2 (Agenda Structure)",
            ""
        ]
        
        # Add agenda items as entities
        if agenda_items:
            markdown_parts.extend([
                "**ENTITIES IN THIS DOCUMENT:**"
            ])
            for item in agenda_items[:20]:  # Limit to first 20
                item_code = item.get('item_code', '')
                if item_code:
                    markdown_parts.append(f"- AGENDA_ITEM: {item_code}")
            markdown_parts.append("")
        
        # Add document content
        full_text = json_data.get('full_text', '')
        if full_text:
            markdown_parts.extend([
                "**DOCUMENT CONTENT:**",
                "=" * 20,
                "",
                full_text
            ])
        
        # Add structured agenda items
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

    def _create_stage1_markdown(self, json_data: Dict, pdf_path: Path, source_json: Path) -> str:
        """Create basic but properly formatted markdown from stage1 OCR data."""
        source_file = json_data.get('source_file', pdf_path.name)
        doc_type = self._determine_doc_type(source_file)
        
        # Extract text content
        full_text = ""
        if 'full_text' in json_data:
            full_text = json_data['full_text']
        elif 'extracted_text' in json_data:
            full_text = json_data['extracted_text']
        elif 'pages' in json_data:
            # Reconstruct from pages
            for page in json_data['pages']:
                if isinstance(page, dict) and 'text' in page:
                    full_text += page['text'] + "\n\n"
                elif isinstance(page, str):
                    full_text += page + "\n\n"
        
        # Create proper markdown structure
        markdown_parts = [
            "---",
            "DOCUMENT METADATA AND CONTEXT",
            "=============================",
            "",
            "**DOCUMENT IDENTIFICATION:**",
            f"- Document Type: {doc_type.upper()}",
            f"- Source File: {source_file}",
            f"- Processed Stage: Stage 1 (OCR Extraction)",
            f"- Conversion Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "**DOCUMENT CONTENT:**",
            "=" * 20,
            ""
        ]
        
        if full_text.strip():
            # Clean up the text
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)
            full_text = full_text.strip()
            markdown_parts.append(full_text)
        else:
            markdown_parts.append("No content available.")
        
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

    def print_processing_summary(self):
        """Print a comprehensive summary of processing statistics."""
        stats = self.processing_stats
        
        log.info("\n" + "="*80)
        log.info("📊 COMPREHENSIVE PROCESSING SUMMARY")
        log.info("="*80)
        
        log.info(f"📂 Total PDFs discovered: {stats['total_discovered']}")
        log.info(f"✅ Already fully processed: {stats['already_processed']}")
        log.info(f"🔄 Needed processing: {stats['needs_processing']}")
        
        if stats['needs_processing'] > 0:
            log.info(f"\n📋 BREAKDOWN OF FILES THAT NEEDED PROCESSING:")
            log.info(f"  🔴 Missing both MD + JSON: {stats['missing_both']}")
            log.info(f"  🟡 Missing only markdown: {stats['missing_markdown_only']}")
            log.info(f"  🟢 Missing only JSON: {stats['missing_json_only']}")
        
        log.info(f"\n⚡ THIS RUN RESULTS:")
        log.info(f"  🔄 Files processed: {stats['processed_this_run']}")
        log.info(f"  ⏭️  Files skipped: {stats['skipped_this_run']}")
        
        # Calculate processing efficiency
        if stats['total_discovered'] > 0:
            skip_rate = (stats['skipped_this_run'] / stats['total_discovered']) * 100
            log.info(f"  📈 Skip efficiency: {skip_rate:.1f}% (higher is better)")
            
            # Show conversion efficiency
            converted_count = stats['missing_markdown_only']  # These get converted
            if converted_count > 0:
                log.info(f"  🔄 JSON→MD conversions: {converted_count} (avoided expensive OCR)")
        
        log.info("="*80)

    def _extract_with_docling(self, pdf_path: Path) -> Dict[str, Any]:
        """Extract text and structure using Docling OCR."""
        try:
            log.debug(f"🔍 Running Docling OCR on {pdf_path.name}")
            
            # Convert with structure preservation
            result = self.converter.convert(str(pdf_path))
            
            # Try to get markdown format
            try:
                full_markdown = result.document.export_to_markdown()
            except:
                # Fallback to text format
                full_markdown = str(result.document)
            
            # Extract page-by-page content
            pages = []
            
            # Try to split by pages (various possible separators)
            page_separators = ['\n\n---\n\n', '\n---\n', '\f', '\n\n\n']
            page_texts = [full_markdown]
            
            for separator in page_separators:
                if separator in full_markdown:
                    page_texts = full_markdown.split(separator)
                    break
            
            for i, page_text in enumerate(page_texts, 1):
                if page_text.strip():
                    pages.append({
                        "page_number": i,
                        "text": page_text.strip(),
                        "char_count": len(page_text)
                    })
            
            return {
                "full_text": full_markdown,
                "pages": pages
            }
            
        except Exception as e:
            log.error(f"❌ Docling extraction failed: {e}")
            # Fallback to basic text extraction
            return self._fallback_text_extraction(pdf_path)
    
    def _extract_hyperlinks(self, pdf_path: Path) -> List[Dict[str, Any]]:
        """Extract hyperlinks with coordinates using PyMuPDF."""
        hyperlinks = []
        
        try:
            log.debug(f"🔗 Extracting hyperlinks from {pdf_path.name}")
            
            with fitz.open(str(pdf_path)) as doc:
                for page_num in range(len(doc)):
                    page = doc[page_num]
                    links = page.get_links()
                    
                    for link in links:
                        if link.get('uri'):  # External URL
                            # Get text content in link area
                            rect = fitz.Rect(link['from'])
                            link_text = page.get_textbox(rect).strip()
                            
                            hyperlinks.append({
                                "url": link['uri'],
                                "text": link_text,
                                "page": page_num + 1,
                                "coordinates": {
                                    "x0": rect.x0,
                                    "y0": rect.y0, 
                                    "x1": rect.x1,
                                    "y1": rect.y1
                                }
                            })
            
            log.debug(f"🔗 Found {len(hyperlinks)} hyperlinks")
            return hyperlinks
            
        except Exception as e:
            log.error(f"❌ Hyperlink extraction failed: {e}")
            return []
    
    def _fallback_text_extraction(self, pdf_path: Path) -> Dict[str, Any]:
        """Fallback text extraction using PyMuPDF only."""
        log.warning(f"⚠️  Using fallback extraction for {pdf_path.name}")
        
        pages = []
        full_text = ""
        
        try:
            with fitz.open(str(pdf_path)) as doc:
                for page_num in range(len(doc)):
                    page = doc[page_num]
                    page_text = page.get_text()
                    
                    pages.append({
                        "page_number": page_num + 1,
                        "text": page_text,
                        "char_count": len(page_text)
                    })
                    
                    full_text += page_text + "\n\n"
            
            return {
                "full_text": full_text.strip(),
                "pages": pages
            }
            
        except Exception as e:
            log.error(f"❌ Fallback extraction failed: {e}")
            return {"full_text": "", "pages": []}
    
    def _generate_document_id(self, pdf_path: Path) -> str:
        """Generate canonical document ID based on filename."""
        # Use filename + size for consistent ID generation
        file_stats = pdf_path.stat()
        id_string = f"{pdf_path.name}_{file_stats.st_size}_{file_stats.st_mtime}"
        return f"DOC_{hashlib.sha1(id_string.encode()).hexdigest()[:12]}"
    
    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        return datetime.now().isoformat()
    
    def process_directory(self, pdf_dir: Path) -> List[Dict[str, Any]]:
        """Process all PDF files in a directory."""
        log.info(f"🚀 Stage 1: Processing PDF directory: {pdf_dir}")
        
        pdf_files = list(pdf_dir.glob("*.pdf"))
        log.info(f"Found {len(pdf_files)} PDF files")
        
        results = []
        for pdf_file in pdf_files:
            try:
                result = self.extract_pdf(pdf_file)
                results.append(result)
            except Exception as e:
                log.error(f"❌ Failed to process {pdf_file.name}: {e}")
                continue
        
        log.info(f"✅ Stage 1 completed: {len(results)}/{len(pdf_files)} files processed")
        return results

def main():
    """Example usage of Stage 1 PDF extraction."""
    logging.basicConfig(level=logging.INFO)
    
    extractor = PDFOCRExtractor()
    
    # Example: Process a directory of PDFs
    # pdf_directory = Path("city_clerk_documents/pdfs")
    # results = extractor.process_directory(pdf_directory)
    
    print("🚀 Stage 1: PDF OCR Extractor ready!")
    print("✅ Features:")
    print("  - Docling OCR with table structure detection")
    print("  - PyMuPDF hyperlink extraction with coordinates") 
    print("  - Robust fallback mechanisms")
    print("  - Page-by-page content preservation")
    print("  - Canonical document ID generation")
    print("  - Comprehensive skip logic for already processed files")

if __name__ == "__main__":
    main() 