from pathlib import Path
import json
import pandas as pd
import csv
import re
from typing import List, Dict, Any
from datetime import datetime
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

class CityClerkDocumentAdapter:
    """Adapt city clerk documents for GraphRAG processing."""
    
    def __init__(self, extracted_text_dir: Path):
        self.extracted_text_dir = Path(extracted_text_dir)
        
    def prepare_documents_for_graphrag(self, output_dir: Path) -> pd.DataFrame:
        """Prepare documents with enhanced source tracking."""
        
        all_documents = []
        
        for json_file in self.extracted_text_dir.glob("*_extracted.json"):
            with open(json_file, 'r') as f:
                doc_data = json.load(f)
            
            # Extract metadata
            doc_type = doc_data.get('document_type', 'unknown')
            meeting_date = doc_data.get('meeting_date', '')
            
            # For agenda items, create separate documents
            if 'items' in doc_data:
                for item in doc_data['items']:
                    doc_dict = {
                        'id': f"{json_file.stem}_{item['item_code']}",  # GraphRAG expects an 'id' column
                        'text': self._format_agenda_item(item),
                        'title': f"Agenda Item {item['item_code']} - {meeting_date}",
                        'document_type': 'agenda_item',
                        'meeting_date': meeting_date,
                        'item_code': item['item_code'],
                        'source_file': json_file.name,
                        'source_id': f"{json_file.stem}_{item['item_code']}",  # Unique source ID
                        'metadata': json.dumps({
                            'original_file': json_file.name,
                            'extraction_method': doc_data.get('metadata', {}).get('extraction_method', 'unknown'),
                            'item_type': item.get('type', 'unknown')
                        }),
                        # Add canonical IDs for WP-3
                        'doc_id': item.get('doc_id', doc_data.get('doc_id', '')),
                        'section_id': item.get('section_id', ''),
                        'chunk_id': item.get('chunk_id', f"{json_file.stem}_{item['item_code']}")
                    }
                    all_documents.append(doc_dict)
            
            # For other documents
            else:
                doc_dict = {
                    'id': json_file.stem,  # GraphRAG expects an 'id' column
                    'text': doc_data.get('full_text', ''),
                    'title': self._generate_title(doc_data),
                    'document_type': doc_type,
                    'meeting_date': meeting_date,
                    'item_code': doc_data.get('item_code', ''),
                    'source_file': json_file.name,
                    'source_id': json_file.stem,  # Unique source ID
                    'metadata': json.dumps({
                        'original_file': json_file.name,
                        'document_number': doc_data.get('document_number', ''),
                        'extraction_method': doc_data.get('metadata', {}).get('extraction_method', 'unknown')
                    }),
                    # Add canonical IDs for WP-3 (with fallback for backwards compatibility)
                    'doc_id': doc_data.get('doc_id', f"DOC_{json_file.stem}"),
                    'section_id': doc_data.get('section_id', ''),
                    'chunk_id': doc_data.get('chunk_id', json_file.stem)  # Use file stem as default chunk_id
                }
                all_documents.append(doc_dict)
        
        # Create DataFrame with source tracking columns
        df = pd.DataFrame(all_documents)
        
        # Ensure required columns exist (including canonical IDs for WP-3)
        required_columns = ['id', 'text', 'title', 'source_id', 'source_file', 'metadata', 'doc_id', 'section_id', 'chunk_id']
        for col in required_columns:
            if col not in df.columns:
                df[col] = ''
        
        # Save with source tracking preserved
        csv_path = output_dir / "city_clerk_documents.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8')
        
        return df
    
    def _format_agenda_item(self, item: Dict) -> str:
        """Format agenda item text."""
        parts = []
        
        if item.get('item_code'):
            parts.append(f"Item Code: {item['item_code']}")
        
        if item.get('title'):
            parts.append(f"Title: {item['title']}")
        
        if item.get('description'):
            parts.append(f"Description: {item['description']}")
        
        if item.get('sponsors'):
            sponsors = ', '.join(item['sponsors'])
            parts.append(f"Sponsors: {sponsors}")
        
        return "\n\n".join(parts)
    
    def _generate_title(self, doc_data: Dict) -> str:
        """Generate title for document."""
        doc_type = doc_data.get('document_type', 'Document')
        doc_number = doc_data.get('document_number', '')
        
        if doc_number:
            return f"{doc_type.title()} {doc_number}"
        else:
            return doc_type.title()
    
    def _process_json_file(self, json_file: Path) -> List[Dict]:
        """Process a single JSON file and return its documents."""
        documents = []
        
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        doc_type = data.get('document_type', self._determine_document_type(json_file.name))
        meeting_date = data.get('meeting_date', self._extract_meeting_date(json_file.name))
        
        # CRITICAL: Process agenda items as completely separate documents
        if doc_type == 'agenda' and 'agenda_items' in data:
            # Each agenda item becomes its own document with NO references to other items
            for item in data['agenda_items']:
                # Create a unique, isolated document for this specific item
                doc_record = {
                    'id': f"agenda_item_{meeting_date}_{item['item_code']}",
                    'title': f"Agenda Item {item['item_code']} ONLY",
                    'text': self._prepare_isolated_item_text(item, meeting_date),
                    'document_type': 'agenda_item',
                    'meeting_date': meeting_date,
                    'item_code': item['item_code'],
                    'source_file': json_file.name,
                    'isolation_flag': True,  # Flag to indicate this must be treated in isolation
                    'urls': json.dumps(item.get('urls', [])),
                    # Add canonical IDs for WP-3
                    'doc_id': item.get('doc_id', data.get('doc_id', '')),
                    'section_id': item.get('section_id', ''),
                    'chunk_id': item.get('chunk_id', '')
                }
                documents.append(doc_record)
        
        elif doc_type in ['ordinance', 'resolution']:
            # Process ordinances and resolutions with explicit item code isolation
            doc_record = {
                'id': f"{doc_type}_{data.get('document_number', json_file.stem)}",
                'title': f"{doc_type.title()} {data.get('document_number', '')}",
                'text': self._prepare_isolated_document_text(data),
                'document_type': doc_type,
                'meeting_date': meeting_date,
                'item_code': data.get('item_code', ''),
                'document_number': data.get('document_number', ''),
                'source_file': json_file.name,
                'isolation_flag': True,
                'urls': json.dumps([])
            }
            documents.append(doc_record)
        
        elif doc_type == 'verbatim_transcript':
            # Process transcripts with item code isolation
            item_codes = data.get('item_codes', [])
            # Create separate document for each item code mentioned
            if item_codes:
                for item_code in item_codes:
                    doc_record = {
                        'id': f"transcript_{json_file.stem}_{item_code}",
                        'title': f"Transcript for Item {item_code} ONLY",
                        'text': self._extract_item_specific_transcript(data, item_code),
                        'document_type': 'verbatim_transcript',
                        'meeting_date': meeting_date,
                        'item_code': item_code,
                        'transcript_type': data.get('transcript_type', ''),
                        'source_file': json_file.name,
                        'isolation_flag': True,
                        'urls': json.dumps([])
                    }
                    documents.append(doc_record)
            else:
                # General transcript without specific items
                doc_record = {
                    'id': f"transcript_{json_file.stem}",
                    'title': f"Transcript: {data.get('transcript_type', 'Meeting')}",
                    'text': data.get('full_text', ''),
                    'document_type': 'verbatim_transcript',
                    'meeting_date': meeting_date,
                    'item_code': '',
                    'transcript_type': data.get('transcript_type', ''),
                    'source_file': json_file.name,
                    'urls': json.dumps([])
                }
                documents.append(doc_record)
        
        return documents
    
    def _prepare_isolated_item_text(self, item: Dict, meeting_date: str) -> str:
        """Prepare agenda item text with STRICT isolation - no references to other items."""
        parts = []
        
        # Add isolation header
        parts.append(f"=== ISOLATED ENTITY: AGENDA ITEM {item['item_code']} ===")
        parts.append(f"THIS DOCUMENT CONTAINS INFORMATION ABOUT {item['item_code']} ONLY.")
        parts.append(f"DO NOT CONFUSE WITH OTHER AGENDA ITEMS.\n")
        
        # Add item-specific information
        parts.append(f"Agenda Item Code: {item['item_code']}")
        parts.append(f"Meeting Date: {meeting_date}")
        
        # Add title
        if item.get('title'):
            parts.append(f"Title: {item['title']}")
        
        # Add sponsors
        if item.get('sponsors'):
            sponsors = ', '.join(item['sponsors'])
            parts.append(f"Sponsors: {sponsors}")
        
        # Add description
        if item.get('description'):
            parts.append(f"Description: {item['description']}")
        
        # Add URLs as context
        if item.get('urls'):
            parts.append("\nReferenced Documents:")
            for url in item['urls']:
                parts.append(f"- {url.get('text', 'Link')}: {url.get('url', '')}")
        
        # Add isolation footer
        parts.append(f"\n=== END OF ISOLATED ENTITY {item['item_code']} ===")
        
        return "\n\n".join(parts)
    
    def _prepare_isolated_document_text(self, data: Dict) -> str:
        """Prepare document text with isolation markers."""
        parts = []
        
        doc_number = data.get('document_number', '')
        item_code = data.get('item_code', '')
        
        # Add isolation header
        if item_code:
            parts.append(f"=== ISOLATED ENTITY: {data.get('document_type', '').upper()} FOR ITEM {item_code} ===")
            parts.append(f"THIS DOCUMENT IS SPECIFICALLY ABOUT AGENDA ITEM {item_code}.")
        else:
            parts.append(f"=== ISOLATED ENTITY: {data.get('document_type', '').upper()} {doc_number} ===")
        
        # Add the full text
        parts.append(data.get('full_text', ''))
        
        # Add isolation footer
        parts.append(f"\n=== END OF ISOLATED ENTITY ===")
        
        return "\n\n".join(parts)
    
    def _extract_item_specific_transcript(self, data: Dict, item_code: str) -> str:
        """Extract only the transcript portions relevant to a specific item code."""
        full_text = data.get('full_text', '')
        
        # Try to extract sections mentioning this specific item
        lines = full_text.split('\n')
        relevant_sections = []
        in_relevant_section = False
        context_buffer = []
        
        for i, line in enumerate(lines):
            # Check if this line mentions the specific item code
            if item_code in line:
                in_relevant_section = True
                # Add some context before
                start_idx = max(0, i - 5)
                context_buffer = lines[start_idx:i]
                relevant_sections.extend(context_buffer)
                relevant_sections.append(line)
            elif in_relevant_section:
                # Continue adding lines until we hit another item code or section break
                if re.search(r'[A-Z]-\d+', line) and item_code not in line:
                    # Hit another item code, stop
                    in_relevant_section = False
                else:
                    relevant_sections.append(line)
        
        if relevant_sections:
            isolated_text = f"=== TRANSCRIPT EXCERPT FOR ITEM {item_code} ONLY ===\n\n"
            isolated_text += "\n".join(relevant_sections)
            isolated_text += f"\n\n=== END OF ITEM {item_code} TRANSCRIPT ==="
            return isolated_text
        else:
            return f"No specific transcript content found for item {item_code}"

    def _determine_document_type(self, filename: str) -> str:
        """Determine document type from filename."""
        filename_lower = filename.lower()
        if 'agenda' in filename_lower:
            return 'agenda'
        elif 'ordinance' in filename_lower:
            return 'ordinance'
        elif 'resolution' in filename_lower:
            return 'resolution'
        elif 'verbatim' in filename_lower:
            return 'verbatim_transcript'
        elif 'minutes' in filename_lower:
            return 'minutes'
        else:
            return 'document'
    
    def _extract_meeting_date(self, filename: str) -> str:
        """Extract meeting date from filename."""
        import re
        # Try different date patterns
        patterns = [
            r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
            r'(\d{2}_\d{2}_\d{4})',  # MM_DD_YYYY
            r'(\d{2}\.\d{2}\.\d{4})'  # MM.DD.YYYY
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                date_str = match.group(1)
                # Normalize to MM.DD.YYYY format
                if '-' in date_str:
                    parts = date_str.split('-')
                    return f"{parts[1]}.{parts[2]}.{parts[0]}"
                elif '_' in date_str:
                    parts = date_str.split('_')
                    return f"{parts[0]}.{parts[1]}.{parts[2]}"
                else:
                    return date_str
        
        return ""

    def prepare_documents_from_markdown(self, output_dir: Path) -> pd.DataFrame:
        """Convert markdown files to GraphRAG CSV format with enhanced metadata."""
        
        markdown_dir = Path("city_clerk_documents/extracted_markdown")
        
        if not markdown_dir.exists():
            raise ValueError(f"Markdown directory not found: {markdown_dir}")
        
        documents = []
        
        # Process all markdown files
        for md_file in markdown_dir.glob("*.md"):
            try:
                with open(md_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Extract metadata from the header
                metadata = self._extract_enhanced_metadata(content)
                
                # Build enriched text that includes all identifiers
                enriched_text = self._build_enriched_text(content, metadata)
                
                # Clean content for CSV
                enriched_text = self._clean_text_for_graphrag(enriched_text)
                
                # Ensure content is not empty
                if not enriched_text.strip():
                    print(f"⚠️  Skipping empty file: {md_file.name}")
                    continue
                
                # Parse document type from filename
                filename = md_file.stem
                if filename.startswith('agenda_'):
                    doc_type = 'agenda'
                elif filename.startswith('ordinance_'):
                    doc_type = 'ordinance'
                elif filename.startswith('resolution_'):
                    doc_type = 'resolution'
                elif filename.startswith('verbatim_'):
                    doc_type = 'verbatim_transcript'
                else:
                    doc_type = 'document'
                
                # Extract meeting date and item code from filename or content
                meeting_date = self._extract_meeting_date_from_markdown(filename, content)
                item_code = self._extract_item_code_from_markdown(filename, content)
                
                doc_record = {
                    'id': filename,
                    'title': self._build_comprehensive_title(metadata),
                    'text': enriched_text,
                    'document_type': doc_type,
                    'meeting_date': meeting_date,
                    'item_code': metadata.get('item_code', item_code),
                    'document_number': metadata.get('document_number', ''),
                    'related_items': json.dumps(metadata.get('related_items', [])),
                    'source_file': md_file.name,
                    # Add canonical IDs for WP-3
                    'doc_id': metadata.get('doc_id', ''),
                    'section_id': metadata.get('section_id', ''),
                    'chunk_id': metadata.get('chunk_id', '')
                }
                documents.append(doc_record)
                
            except Exception as e:
                print(f"❌ Error processing {md_file.name}: {e}")
                continue
        
        if not documents:
            raise ValueError("No documents were successfully processed!")
        
        # Convert to DataFrame
        df = pd.DataFrame(documents)
        
        # Ensure no null values in required columns
        df['text'] = df['text'].fillna('')
        df['title'] = df['title'].fillna('Untitled')
        df['meeting_date'] = df['meeting_date'].fillna('')
        df['item_code'] = df['item_code'].fillna('')
        df['document_number'] = df['document_number'].fillna('')
        df['related_items'] = df['related_items'].fillna('[]')
        
        # Ensure text column has content
        empty_texts = df[df['text'].str.strip() == '']
        if len(empty_texts) > 0:
            print(f"⚠️  Warning: {len(empty_texts)} documents have empty text")
            df = df[df['text'].str.strip() != '']
        
        # Log summary
        print(f"📊 Prepared {len(df)} documents from markdown:")
        for doc_type in df['document_type'].unique():
            count = len(df[df['document_type'] == doc_type])
            print(f"   - {doc_type}: {count}")
        
        # Save as CSV with proper escaping
        output_file = output_dir / "city_clerk_documents.csv"
        
        # Use pandas to_csv with specific parameters to handle special characters
        df.to_csv(
            output_file, 
            index=False,
            encoding='utf-8',
            escapechar='\\',
            doublequote=True,
            quoting=csv.QUOTE_MINIMAL
        )
        
        print(f"💾 Saved CSV to: {output_file}")
        
        # Verify the CSV can be read back
        try:
            test_df = pd.read_csv(output_file)
            print(f"✅ CSV verification: {len(test_df)} rows can be read back")
        except Exception as e:
            print(f"❌ CSV verification failed: {e}")
        
        return df

    def _clean_text_for_graphrag(self, text: str) -> str:
        """Clean markdown text for GraphRAG processing."""
        # Remove metadata header if present
        if text.startswith('---'):
            parts = text.split('---', 2)
            if len(parts) >= 3:
                # Keep only the main content after metadata
                text = parts[2].strip()
                
                # Also remove the "ORIGINAL DOCUMENT CONTENT" marker if present
                if "ORIGINAL DOCUMENT CONTENT" in text:
                    text = text.split("ORIGINAL DOCUMENT CONTENT", 1)[1].strip()
        
        # Remove excessive markdown formatting
        text = re.sub(r'^#+\s+', '', text, flags=re.MULTILINE)  # Remove headers
        text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)  # Remove bold
        text = re.sub(r'\n{3,}', '\n\n', text)  # Reduce multiple newlines
        
        # DO NOT TRUNCATE - GraphRAG will handle chunking itself
        # Just ensure the text is clean and complete
        return text.strip()

    def _clean_text_for_csv(self, text: str) -> str:
        """Clean text to be CSV-safe."""
        # Remove markdown metadata header if present
        if text.startswith('---'):
            parts = text.split('---', 2)
            if len(parts) >= 3:
                # Keep only the main content
                text = parts[2]
        
        # Remove problematic characters
        text = text.replace('\x00', '')  # Null bytes
        text = text.replace('\r\n', '\n')  # Windows line endings
        
        # Replace multiple newlines with double newline
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # Remove or escape quotes that might break CSV
        text = text.replace('"', '""')  # Escape quotes for CSV
        
        # Ensure text is not too long (GraphRAG might have limits)
        max_length = 50000  # Adjust as needed
        if len(text) > max_length:
            text = text[:max_length] + "... [truncated]"
        
        return text.strip()

    def _extract_meeting_date_from_markdown(self, filename: str, content: str) -> str:
        """Extract meeting date from filename or markdown content."""
        # First try from filename
        date_from_filename = self._extract_meeting_date(filename)
        if date_from_filename:
            return date_from_filename
        
        # Try from content metadata
        meeting_date_match = re.search(r'- Meeting Date: (.+)', content)
        if meeting_date_match:
            return meeting_date_match.group(1).strip()
        
        return ""

    def _extract_item_code_from_markdown(self, filename: str, content: str) -> str:
        """Extract item code from filename or markdown content."""
        # Try from filename
        item_match = re.search(r'([A-Z]-\d+)', filename)
        if item_match:
            return item_match.group(1)
        
        # Try from content
        item_match = re.search(r'- Agenda Items Discussed: (.+)', content)
        if item_match:
            return item_match.group(1).strip()
        
        return ""

    def _extract_title_from_markdown(self, content: str, filename: str) -> str:
        """Extract title from markdown content."""
        # Look for title in metadata section
        import re
        title_match = re.search(r'\*\*PARSED INFORMATION:\*\*.*?- Title: (.+)', content, re.DOTALL)
        if title_match:
            return title_match.group(1).strip()
        
        # Fallback to filename
        return filename.replace('_', ' ').title()

    def _parse_custom_metadata(self, metadata_text: str) -> Dict:
        """Parse our custom metadata format from the enhanced PDF extractor."""
        frontmatter = {}
        
        # Extract document type from the metadata
        import re
        
        # Look for document type
        doc_type_match = re.search(r'Document Type:\s*(.+)', metadata_text)
        if doc_type_match:
            frontmatter['document_type'] = doc_type_match.group(1).strip()
        
        # Look for filename
        filename_match = re.search(r'Filename:\s*(.+)', metadata_text)
        if filename_match:
            frontmatter['filename'] = filename_match.group(1).strip()
        
        # Look for document number
        doc_num_match = re.search(r'Document Number:\s*(.+)', metadata_text)
        if doc_num_match and doc_num_match.group(1).strip() != 'N/A':
            frontmatter['document_number'] = doc_num_match.group(1).strip()
        
        # Look for meeting date
        date_match = re.search(r'Meeting Date:\s*(.+)', metadata_text)
        if date_match and date_match.group(1).strip() != 'N/A':
            frontmatter['meeting_date'] = date_match.group(1).strip()
        
        # Look for agenda items
        items_match = re.search(r'Related Agenda Items:\s*(.+)', metadata_text)
        if items_match and items_match.group(1).strip() != 'N/A':
            frontmatter['agenda_items'] = items_match.group(1).strip()
        
        return frontmatter 

    def _extract_enhanced_metadata(self, content: str) -> Dict:
        """Extract all metadata including cross-references."""
        metadata = {}
        
        # Extract all document numbers
        doc_nums = re.findall(r'(?:Ordinance|Resolution)\s*(?:No\.\s*)?(\d{4}-\d+|\d+)', content)
        metadata['all_document_numbers'] = list(set(doc_nums))
        
        # Extract all agenda items
        agenda_items = re.findall(r'(?:Item|Agenda Item)\s*:?\s*([A-Z]-?\d+)', content)
        metadata['all_agenda_items'] = list(set(agenda_items))
        
        # Extract relationships
        metadata['related_items'] = self._extract_relationships(content)
        
        return metadata

    def _build_enriched_text(self, content: str, metadata: Dict) -> str:
        """Build text that prominently features all identifiers."""
        # Add a summary header with all identifiers
        identifier_summary = []
        
        if metadata.get('all_document_numbers'):
            identifier_summary.append(f"Document Numbers: {', '.join(metadata['all_document_numbers'])}")
        
        if metadata.get('all_agenda_items'):
            identifier_summary.append(f"Agenda Items: {', '.join(metadata['all_agenda_items'])}")
        
        if identifier_summary:
            enriched_header = "DOCUMENT IDENTIFIERS:\n" + '\n'.join(identifier_summary) + "\n\n"
            return enriched_header + content
        
        return content

    def _build_comprehensive_title(self, metadata: Dict) -> str:
        """Build a comprehensive title from metadata."""
        title_parts = []
        
        if metadata.get('all_agenda_items'):
            title_parts.append(f"Items: {', '.join(metadata['all_agenda_items'])}")
        
        if metadata.get('all_document_numbers'):
            title_parts.append(f"Docs: {', '.join(metadata['all_document_numbers'])}")
        
        if title_parts:
            return ' | '.join(title_parts)
        
        return "City Document"

    def _extract_relationships(self, content: str) -> List[Dict]:
        """Extract document relationships from content."""
        relationships = []
        
        # Look for patterns that indicate relationships
        relationship_patterns = [
            r'(?:amending|modifying|updating)\s+(?:Ordinance|Resolution)\s*(?:No\.\s*)?(\d+)',
            r'(?:relating to|concerning|regarding)\s+(?:agenda item|item)\s*([A-Z]-?\d+)',
            r'(?:pursuant to|under)\s+(?:agenda item|item)\s*([A-Z]-?\d+)'
        ]
        
        for pattern in relationship_patterns:
            matches = re.finditer(pattern, content, re.IGNORECASE)
            for match in matches:
                relationships.append({
                    'type': 'reference',
                    'target': match.group(1),
                    'context': match.group(0)
                })
        
        return relationships 