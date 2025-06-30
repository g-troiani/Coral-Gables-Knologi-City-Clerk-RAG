#!/usr/bin/env python3
"""
Structural Query Enhancer
=========================
Enhances GraphRAG queries with structured data from extracted documents
to ensure complete and accurate responses for agenda/document structure queries.
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
from datetime import datetime

class StructuralQueryEnhancer:
    """Enhances queries with structural data from extracted documents."""
    
    def __init__(self, extracted_text_dir: Path):
        self.extracted_text_dir = Path(extracted_text_dir)
        self._agenda_cache = {}
        self._load_agenda_structures()
    
    def _load_agenda_structures(self):
        """Load all agenda structures from extracted JSON files."""
        print("🔍 Loading agenda structures from extracted documents...")
        
        # First pass: Find structured agenda files
        structured_agendas = {}
        individual_docs = []
        
        for json_file in self.extracted_text_dir.glob("*.json"):
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                
                # Extract meeting date from the document
                meeting_date_raw = data.get('meeting_date', '')
                meeting_date = self._parse_meeting_date(meeting_date_raw, json_file.name)
                
                if meeting_date:
                    # Check if this is a structured agenda file with sections
                    if 'sections' in data and data['sections']:
                        # This is a complete agenda JSON with sections - prioritize this
                        structured_agendas[meeting_date] = (json_file, data)
                    else:
                        # Individual document file
                        individual_docs.append((meeting_date, json_file, data))
                        
            except Exception as e:
                print(f"⚠️  Error loading {json_file}: {e}")
        
        # Process structured agendas first
        date_grouped_items = {}
        
        for meeting_date, (json_file, data) in structured_agendas.items():
            date_grouped_items[meeting_date] = {
                'source_files': [json_file.name],
                'agenda_items': [],
                'doc_ids': [data.get('doc_id', json_file.stem)]
            }
            
            # Process all sections and items from structured agenda
            for section in data['sections']:
                section_items = section.get('items', [])
                for item_data in section_items:
                    agenda_item = {
                        'item_code': item_data.get('item_code', ''),
                        'title': item_data.get('title', ''),
                        'document_type': 'agenda_item',
                        'document_number': item_data.get('document_reference', ''),
                        'section_name': section.get('section_name', ''),
                        'full_text': item_data.get('title', ''),
                        'source_file': json_file.name
                    }
                    date_grouped_items[meeting_date]['agenda_items'].append(agenda_item)
        
        # Only process individual docs for dates without structured agendas
        for meeting_date, json_file, data in individual_docs:
            if meeting_date not in date_grouped_items:
                # No structured agenda for this date, use individual docs
                date_grouped_items[meeting_date] = {
                    'source_files': [],
                    'agenda_items': [],
                    'doc_ids': []
                }
            
            # Handle individual document files (resolutions, ordinances, etc.)
            if data.get('document_type') == 'verbatim_transcript':
                # Verbatim transcripts have item_codes as an array
                item_codes = data.get('item_codes', [])
                item_code = item_codes[0] if item_codes else ''
                # Extract title from the full text or create a meaningful one
                title = self._extract_title_from_verbatim(data.get('full_text', ''), item_code)
            else:
                # Regular documents have single item_code and title
                item_code = data.get('item_code', '')
                title = data.get('title', '')
            
            agenda_item = {
                'item_code': item_code,
                'title': title,
                'document_type': data.get('document_type', ''),
                'document_number': data.get('document_number', ''),
                'full_text': data.get('full_text', ''),
                'source_file': json_file.name
            }
            
            date_grouped_items[meeting_date]['agenda_items'].append(agenda_item)
            date_grouped_items[meeting_date]['source_files'].append(json_file.name)
            date_grouped_items[meeting_date]['doc_ids'].append(data.get('doc_id', json_file.stem))
        
        # Convert to agenda cache format
        for meeting_date, items_data in date_grouped_items.items():
            self._agenda_cache[meeting_date] = {
                'source_files': items_data['source_files'],
                'doc_ids': items_data['doc_ids'],
                'meeting_info': {'date': meeting_date},
                'agenda_items': items_data['agenda_items'],
                'sections': [],
                'full_data': {}
            }
        
        print(f"✅ Loaded {len(self._agenda_cache)} agenda structures")
        for date, data in self._agenda_cache.items():
            structured_count = len([f for f in data['source_files'] if 'stage2_agenda' in f])
            total_items = len(data['agenda_items'])
            structure_type = "📋 Structured Agenda" if structured_count > 0 else "📄 Individual Documents"
            print(f"   📅 {date}: {total_items} items ({structure_type})")
    
    def _extract_title_from_verbatim(self, full_text: str, item_code: str) -> str:
        """Extract a meaningful title from verbatim transcript text."""
        if not full_text:
            return f"Verbatim Transcript - Agenda Item {item_code}"
        
        # Try to find title patterns in the text
        import re
        
        # Look for patterns like "RE: [title]" or "Subject: [title]"
        patterns = [
            r'RE:\s*([^\n]+)',
            r'Subject:\s*([^\n]+)',
            r'SUBJECT:\s*([^\n]+)',
            r'Matter:\s*([^\n]+)',
            r'Item\s+' + re.escape(item_code) + r'[:\-\s]*([^\n]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                title = match.group(1).strip()
                # Clean up the title
                title = re.sub(r'^\W+|\W+$', '', title)  # Remove leading/trailing punctuation
                if len(title) > 10:  # Only use if it's meaningful
                    return f"Verbatim: {title[:100]}"
        
        # Look for ordinance/resolution patterns in the text
        ordinance_match = re.search(r'(AN?\s+ORDINANCE[^\n]+)', full_text, re.IGNORECASE)
        if ordinance_match:
            return f"Verbatim: {ordinance_match.group(1)[:100]}"
        
        resolution_match = re.search(r'(AN?\s+RESOLUTION[^\n]+)', full_text, re.IGNORECASE)
        if resolution_match:
            return f"Verbatim: {resolution_match.group(1)[:100]}"
        
        # Look for the first meaningful line after the header
        lines = full_text.split('\n')
        for line in lines[5:15]:  # Skip header lines, check next 10 lines
            line = line.strip()
            if len(line) > 20 and not line.startswith('#') and not line.startswith('Mayor') and not line.startswith('Commissioner'):
                return f"Verbatim: {line[:100]}"
        
        # Fallback
        return f"Verbatim Transcript - Agenda Item {item_code}"
    
    def _parse_meeting_date(self, date_str: str, filename: str) -> Optional[str]:
        """Parse meeting date from various formats."""
        # Try to extract from filename first (more reliable)
        filename_match = re.search(r'(\d{2})_(\d{2})_(\d{4})', filename)
        if filename_match:
            month, day, year = filename_match.groups()
            return f"{year}-{month}-{day}"
        
        # Try to parse from date string
        date_patterns = [
            r'(\d{1,2})\.(\d{1,2})\.(\d{4})',  # MM.DD.YYYY (American format with dots)
            r'(\d{1,2})/(\d{1,2})/(\d{4})',   # MM/DD/YYYY
            r'(\d{4})-(\d{1,2})-(\d{1,2})',   # YYYY-MM-DD
            r'January (\d{1,2}), (\d{4})',    # January 9, 2024
            r'Jan (\d{1,2}), (\d{4})'         # Jan 9, 2024
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, date_str)
            if match:
                if 'January' in pattern or 'Jan' in pattern:
                    day, year = match.groups()
                    return f"{year}-01-{day.zfill(2)}"
                elif pattern.startswith(r'(\d{4})'):
                    year, month, day = match.groups()
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                elif pattern.startswith(r'(\d{1,2})\.') or pattern.startswith(r'(\d{1,2})/'):
                    # MM.DD.YYYY or MM/DD/YYYY format (American)
                    month, day, year = match.groups()
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        
        return None
    
    def is_agenda_completeness_query(self, query: str) -> bool:
        """Determine if query requires complete agenda listing."""
        completeness_patterns = [
            r'all.*items.*agenda',
            r'complete.*agenda',
            r'agenda.*items.*(?:presented|discussed|covered)',
            r'(?:what|which).*items.*(?:discussed|meeting|agenda)',
            r'(?:what|which).*agenda.*items',
            r'items.*from.*(?:agenda|meeting)',
            r'agenda.*items.*for',
            r'all.*resolutions?.*ordinances?',
            r'complete.*list.*items',
            r'(?:what|which).*(?:happened|occurred).*meeting',
            r'meeting.*agenda',
            r'agenda.*(?:august|september|october|november|december|january|february|march|april|may|june|july)',
            r'(?:august|september|october|november|december|january|february|march|april|may|june|july).*agenda'
        ]
        
        query_lower = query.lower()
        return any(re.search(pattern, query_lower) for pattern in completeness_patterns)
    
    def extract_date_from_query(self, query: str) -> Optional[str]:
        """Extract date from query."""
        date_patterns = [
            r'january?\s+(\d{1,2}),?\s+(\d{4})',
            r'february?\s+(\d{1,2}),?\s+(\d{4})',
            r'march?\s+(\d{1,2}),?\s+(\d{4})',
            r'april?\s+(\d{1,2}),?\s+(\d{4})',
            r'may\s+(\d{1,2}),?\s+(\d{4})',
            r'june?\s+(\d{1,2}),?\s+(\d{4})',
            r'july?\s+(\d{1,2}),?\s+(\d{4})',
            r'august?\s+(\d{1,2}),?\s+(\d{4})',
            r'september?\s+(\d{1,2}),?\s+(\d{4})',
            r'october?\s+(\d{1,2}),?\s+(\d{4})',
            r'november?\s+(\d{1,2}),?\s+(\d{4})',
            r'december?\s+(\d{1,2}),?\s+(\d{4})',
            r'jan\s+(\d{1,2}),?\s+(\d{4})',
            r'feb\s+(\d{1,2}),?\s+(\d{4})',
            r'mar\s+(\d{1,2}),?\s+(\d{4})',
            r'apr\s+(\d{1,2}),?\s+(\d{4})',
            r'jun\s+(\d{1,2}),?\s+(\d{4})',
            r'jul\s+(\d{1,2}),?\s+(\d{4})',
            r'aug\s+(\d{1,2}),?\s+(\d{4})',
            r'sep\s+(\d{1,2}),?\s+(\d{4})',
            r'oct\s+(\d{1,2}),?\s+(\d{4})',
            r'nov\s+(\d{1,2}),?\s+(\d{4})',
            r'dec\s+(\d{1,2}),?\s+(\d{4})',
            r'(\d{1,2})\.(\d{1,2})\.(\d{4})',  # DD.MM.YYYY
            r'(\d{1,2})/(\d{1,2})/(\d{4})',   # MM/DD/YYYY  
            r'(\d{4})-(\d{1,2})-(\d{1,2})'    # YYYY-MM-DD
        ]
        
        query_lower = query.lower()
        
        month_names = {
            'january': '01', 'jan': '01',
            'february': '02', 'feb': '02',
            'march': '03', 'mar': '03',
            'april': '04', 'apr': '04',
            'may': '05',
            'june': '06', 'jun': '06',
            'july': '07', 'jul': '07',
            'august': '08', 'aug': '08',
            'september': '09', 'sep': '09',
            'october': '10', 'oct': '10',
            'november': '11', 'nov': '11',
            'december': '12', 'dec': '12'
        }
        
        for pattern in date_patterns:
            match = re.search(pattern, query_lower)
            if match:
                # Check if it's a month name pattern
                is_month_pattern = any(month in pattern for month in month_names.keys())
                
                if is_month_pattern:
                    day, year = match.groups()
                    # Find which month is in the pattern
                    month_num = None
                    for month_name, month_num_str in month_names.items():
                        if month_name in pattern:
                            month_num = month_num_str
                            break
                    
                    if month_num:
                        return f"{year}-{month_num}-{day.zfill(2)}"
                elif pattern.startswith(r'(\d{4})'):
                    year, month, day = match.groups()
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                elif pattern.startswith(r'(\d{1,2})\.'):
                    # DD.MM.YYYY format (European)
                    day, month, year = match.groups()
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                else:
                    # MM/DD/YYYY format (American)
                    month, day, year = match.groups()
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        
        return None
    
    def get_complete_agenda_items(self, date: str) -> Dict[str, Any]:
        """Get complete agenda items for a specific date."""
        if date not in self._agenda_cache:
            return {"found": False, "message": f"No agenda found for {date}"}
        
        agenda_data = self._agenda_cache[date]
        items = agenda_data['agenda_items']
        
        # Extract all ordinances and resolutions
        ordinances = []
        resolutions = []
        other_items = []
        
        for item in items:
            item_code = item.get('item_code', '')
            title = item.get('title', '')
            
            # Categorize items
            if 'ordinance' in title.lower() or item_code.startswith('ORD'):
                ordinances.append(item)
            elif 'resolution' in title.lower() or item_code.startswith('RES'):
                resolutions.append(item)
            else:
                other_items.append(item)
        
        return {
            "found": True,
            "date": date,
            "source_files": agenda_data['source_files'],
            "doc_ids": agenda_data['doc_ids'],
            "meeting_info": agenda_data['meeting_info'],
            "total_items": len(items),
            "ordinances": ordinances,
            "resolutions": resolutions,
            "other_items": other_items,
            "all_items": items
        }
    
    def enhance_graphrag_response(self, query: str, graphrag_result: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance GraphRAG response with structural completeness data."""
        
        # Check if this is a completeness query
        if not self.is_agenda_completeness_query(query):
            return graphrag_result
        
        # Extract date from query
        query_date = self.extract_date_from_query(query)
        if not query_date:
            return graphrag_result
        
        # Get complete structural data
        structural_data = self.get_complete_agenda_items(query_date)
        if not structural_data["found"]:
            return graphrag_result
        
        # Create enhanced response
        enhanced_answer = self._create_enhanced_answer(
            graphrag_result.get('answer', ''),
            structural_data,
            query_date
        )
        
        # Update result
        enhanced_result = graphrag_result.copy()
        enhanced_result['answer'] = enhanced_answer
        enhanced_result['structural_enhancement'] = {
            'applied': True,
            'query_date': query_date,
            'total_items_found': structural_data['total_items'],
            'source_doc_ids': structural_data['doc_ids']
        }
        
        return enhanced_result
    
    def _create_enhanced_answer(self, original_answer: str, structural_data: Dict, query_date: str) -> str:
        """Create enhanced answer with complete structural information."""
        
        ordinances = structural_data['ordinances']
        resolutions = structural_data['resolutions']
        other_items = structural_data['other_items']
        
        enhancement = f"""

## 🔍 **STRUCTURAL COMPLETENESS ENHANCEMENT**
*Enhanced with graph_stages pipeline data to ensure ALL agenda items are included*

### 📅 **Complete Agenda Items for {query_date}**

**📊 SUMMARY**: {structural_data['total_items']} total agenda items identified from {len(structural_data['doc_ids'])} source documents

"""
        
        if ordinances:
            enhancement += "### 📜 **ORDINANCES**\n"
            for ord_item in ordinances:
                enhancement += f"- **{ord_item.get('item_code', 'Unknown')}**: {ord_item.get('title', 'No title')}\n"
            enhancement += "\n"
        
        if resolutions:
            enhancement += "### 📋 **RESOLUTIONS**\n"
            for res_item in resolutions:
                enhancement += f"- **{res_item.get('item_code', 'Unknown')}**: {res_item.get('title', 'No title')}\n"
            enhancement += "\n"
        
        if other_items:
            enhancement += "### 📌 **ALL AGENDA ITEMS**\n"
            
            # Group items by section
            section_groups = {}
            for item in other_items:
                section_name = item.get('section_name', 'Other Items')
                if section_name not in section_groups:
                    section_groups[section_name] = []
                section_groups[section_name].append(item)
            
            for section_name, section_items in section_groups.items():
                enhancement += f"\n**{section_name}**\n"
                for item in section_items:
                    item_code = item.get('item_code', 'Unknown')
                    title = item.get('title', '').strip()
                    doc_ref = item.get('document_number', '')
                    
                    # Handle empty or placeholder titles
                    if not title or title in ['', '****', 'No title', 'Unknown']:
                        doc_type = item.get('document_type', '')
                        if doc_type == 'verbatim_transcript':
                            title = f"Verbatim Transcript Discussion"
                        else:
                            title = "Agenda Item Discussion"
                    
                    # Format with document reference if available
                    if doc_ref:
                        enhancement += f"- **{item_code}** ({doc_ref}): {title}\n"
                    else:
                        enhancement += f"- **{item_code}**: {title}\n"
            enhancement += "\n"
        
        enhancement += """
### 🔗 **Pipeline Integration Note**
This enhanced response combines:
- **GraphRAG Analysis**: Semantic understanding and context from community reports
- **graph_stages Structure**: Complete itemized agenda structure ensuring no items are missed

*This demonstrates the successful linking of both knowledge graph pipelines for comprehensive responses.*
"""
        
        return original_answer + enhancement 