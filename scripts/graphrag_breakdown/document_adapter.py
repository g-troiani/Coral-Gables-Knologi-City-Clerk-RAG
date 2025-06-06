from pathlib import Path
import json
import pandas as pd
import csv
import re
from typing import List, Dict, Any
from datetime import datetime
import yaml

class CityClerkDocumentAdapter:
    """Adapt city clerk documents for GraphRAG processing."""
    
    def __init__(self, extracted_text_dir: Path):
        self.extracted_text_dir = Path(extracted_text_dir)
        
    def prepare_documents_for_graphrag(self, output_dir: Path) -> pd.DataFrame:
        """Convert ALL extracted documents to GraphRAG input format."""
        documents = []
        
        # Process all extracted JSON files (now includes all document types)
        for json_file in self.extracted_text_dir.glob("*_extracted.json"):
            with open(json_file, 'r') as f:
                data = json.load(f)
            
            doc_type = data.get('document_type', self._determine_document_type(json_file.name))
            meeting_date = data.get('meeting_date', self._extract_meeting_date(json_file.name))
            
            # Handle different document structures
            if doc_type == 'agenda' and 'agenda_items' in data:
                # Process agenda items individually (existing logic)
                for item in data['agenda_items']:
                    doc_record = {
                        'id': f"{json_file.stem}_{item['item_code']}",
                        'title': f"{item['item_code']}: {item.get('title', '')}",
                        'text': self._prepare_item_text(item),
                        'document_type': 'agenda_item',
                        'meeting_date': meeting_date,
                        'item_code': item['item_code'],
                        'source_file': json_file.name,
                        'urls': json.dumps(item.get('urls', []))
                    }
                    documents.append(doc_record)
            
            elif doc_type in ['ordinance', 'resolution']:
                # Process ordinances and resolutions
                doc_record = {
                    'id': f"{doc_type}_{data.get('document_number', json_file.stem)}",
                    'title': data.get('title', f"{doc_type.title()} {data.get('document_number', '')}"),
                    'text': data.get('full_text', ''),
                    'document_type': doc_type,
                    'meeting_date': meeting_date,
                    'item_code': data.get('item_code', ''),
                    'document_number': data.get('document_number', ''),
                    'source_file': json_file.name,
                    'urls': json.dumps([])  # No URLs in ordinances/resolutions
                }
                documents.append(doc_record)
            
            elif doc_type == 'verbatim_transcript':
                # Process verbatim transcripts
                item_codes_str = ', '.join(data.get('item_codes', []))
                doc_record = {
                    'id': f"transcript_{json_file.stem}",
                    'title': f"Transcript: {item_codes_str or data.get('transcript_type', 'Meeting')}",
                    'text': data.get('full_text', ''),
                    'document_type': 'verbatim_transcript',
                    'meeting_date': meeting_date,
                    'item_code': item_codes_str,
                    'transcript_type': data.get('transcript_type', ''),
                    'source_file': json_file.name,
                    'urls': json.dumps([])
                }
                documents.append(doc_record)
            
            # Always add full document as well
            full_doc_record = {
                'id': json_file.stem,
                'title': data.get('title', json_file.stem),
                'text': data.get('full_text', ''),
                'document_type': doc_type,
                'meeting_date': meeting_date,
                'source_file': json_file.name,
                'urls': json.dumps(data.get('hyperlinks', []))
            }
            documents.append(full_doc_record)
        
        # Convert to DataFrame
        df = pd.DataFrame(documents)
        
        # Log summary
        print(f"📊 Prepared {len(df)} documents for GraphRAG:")
        print(f"   - Agendas: {len(df[df['document_type'] == 'agenda'])}")
        print(f"   - Agenda Items: {len(df[df['document_type'] == 'agenda_item'])}")
        print(f"   - Ordinances: {len(df[df['document_type'] == 'ordinance'])}")
        print(f"   - Resolutions: {len(df[df['document_type'] == 'resolution'])}")
        print(f"   - Transcripts: {len(df[df['document_type'] == 'verbatim_transcript'])}")
        
        # Save as CSV for GraphRAG
        output_file = output_dir / "city_clerk_documents.csv"
        df.to_csv(output_file, index=False)
        
        return df
    
    def _prepare_item_text(self, item: Dict) -> str:
        """Prepare agenda item text with context."""
        parts = []
        
        # Add item header
        parts.append(f"Agenda Item {item['item_code']}")
        
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
            parts.append("Referenced Documents:")
            for url in item['urls']:
                parts.append(f"- {url.get('text', 'Link')}: {url.get('url', '')}")
        
        return "\n\n".join(parts)
    
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
        """Convert markdown files to GraphRAG CSV format."""
        
        markdown_dir = Path("city_clerk_documents/extracted_markdown")
        
        if not markdown_dir.exists():
            raise ValueError(f"Markdown directory not found: {markdown_dir}")
        
        documents = []
        
        # Process all markdown files
        for md_file in markdown_dir.glob("*.md"):
            try:
                with open(md_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Clean content for CSV
                content = self._clean_text_for_graphrag(content)
                
                # Ensure content is not empty
                if not content.strip():
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
                
                # Create simple title
                title = filename.replace('_', ' ').replace('-', ' ').title()
                
                doc_record = {
                    'id': filename,  # Simple ID without special chars
                    'title': title,
                    'text': content,
                    'document_type': doc_type,
                    'meeting_date': meeting_date,
                    'item_code': item_code,
                    'source_file': md_file.name
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