from pathlib import Path
import json
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime

class CityClerkDocumentAdapter:
    """Adapt city clerk documents for GraphRAG processing."""
    
    def __init__(self, extracted_text_dir: Path):
        self.extracted_text_dir = Path(extracted_text_dir)
        
    def prepare_documents_for_graphrag(self, output_dir: Path) -> pd.DataFrame:
        """Convert extracted documents to GraphRAG input format."""
        documents = []
        
        # Process each extracted JSON file
        for json_file in self.extracted_text_dir.glob("*_extracted.json"):
            with open(json_file, 'r') as f:
                data = json.load(f)
            
            # Extract metadata
            doc_type = self._determine_document_type(json_file.name)
            meeting_date = self._extract_meeting_date(json_file.name)
            
            # For agendas, process each agenda item separately
            if 'agenda_items' in data:
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
            
            # Also add full document
            doc_record = {
                'id': json_file.stem,
                'title': data.get('meeting_info', {}).get('title', json_file.stem),
                'text': data.get('full_text', ''),
                'document_type': doc_type,
                'meeting_date': meeting_date,
                'source_file': json_file.name,
                'urls': json.dumps(data.get('hyperlinks', []))
            }
            documents.append(doc_record)
        
        # Convert to DataFrame
        df = pd.DataFrame(documents)
        
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
        if 'agenda' in filename.lower():
            return 'agenda'
        elif 'minutes' in filename.lower():
            return 'minutes'
        elif 'ordinance' in filename.lower():
            return 'ordinance'
        elif 'resolution' in filename.lower():
            return 'resolution'
        else:
            return 'document'
    
    def _extract_meeting_date(self, filename: str) -> str:
        """Extract meeting date from filename."""
        # This would extract date patterns from the filename
        # For now, return empty string if not found
        import re
        date_pattern = r'(\d{4}-\d{2}-\d{2})'
        match = re.search(date_pattern, filename)
        return match.group(1) if match else "" 