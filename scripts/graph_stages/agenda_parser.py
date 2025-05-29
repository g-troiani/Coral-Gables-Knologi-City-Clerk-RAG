"""
Agenda Parser Module
===================
Extracts item codes and mappings from city commission agendas.
"""
import re
from typing import Dict, List, Optional, Tuple
import logging

log = logging.getLogger(__name__)

class AgendaItemParser:
    """Parse agenda items and extract relationships to other documents."""
    
    # Common agenda item patterns
    ITEM_PATTERNS = [
        # E-1, F-12, K-3, etc.
        re.compile(r'^([A-Z])-(\d+)\.?\s*(.+)', re.MULTILINE),
        # E1, F12 (without dash)
        re.compile(r'^([A-Z])(\d+)\.?\s*(.+)', re.MULTILINE),
        # Item E-1, Item F-12
        re.compile(r'^Item\s+([A-Z])-(\d+)\.?\s*(.+)', re.MULTILINE | re.IGNORECASE),
        # Agenda Item E-1
        re.compile(r'^Agenda\s+Item\s+([A-Z])-(\d+)\.?\s*(.+)', re.MULTILINE | re.IGNORECASE),
    ]
    
    # Document type indicators
    TYPE_INDICATORS = {
        'ordinance': ['ordinance', 'amending', 'zoning', 'code amendment'],
        'resolution': ['resolution', 'approving', 'authorizing', 'accepting'],
        'proclamation': ['proclamation', 'declaring', 'recognizing'],
        'contract': ['contract', 'agreement', 'bid', 'purchase'],
        'minutes': ['minutes', 'approval of minutes'],
    }
    
    # Document number patterns
    DOC_NUMBER_PATTERNS = [
        re.compile(r'(?:Ordinance|Ord\.?)\s+(?:No\.?\s*)?(\d{4}-\d+)', re.IGNORECASE),
        re.compile(r'(?:Resolution|Res\.?)\s+(?:No\.?\s*)?(\d{4}-\d+)', re.IGNORECASE),
        re.compile(r'(?:Contract|Agreement)\s+(?:No\.?\s*)?(\d{4}-\d+)', re.IGNORECASE),
    ]

def parse_agenda_items(agenda_data: Dict) -> Dict[str, Dict]:
    """
    Parse agenda document and extract item mappings.
    
    Returns:
        Dict mapping document numbers to their agenda items and metadata
        Example: {
            "2024-66": {
                "item_code": "E-1",
                "type": "Resolution",
                "description": "A Resolution approving...",
                "sponsor": "Commissioner Smith"
            }
        }
    """
    parser = AgendaItemParser()
    item_mappings = {}
    
    # Combine all text from sections
    full_text = ""
    for section in agenda_data.get("sections", []):
        full_text += section.get("text", "") + "\n\n"
    
    # Find all agenda items
    items = _extract_agenda_items(full_text)
    
    for item in items:
        # Extract document numbers from item description
        doc_numbers = _extract_document_numbers(item['description'])
        
        # Determine document type
        doc_type = _determine_document_type(item['description'])
        
        # Extract sponsor if present
        sponsor = _extract_sponsor(item['description'])
        
        # Map each document number to this item
        for doc_num in doc_numbers:
            item_mappings[doc_num] = {
                'item_code': item['code'],
                'type': doc_type,
                'description': item['description'][:500],  # Truncate long descriptions
                'sponsor': sponsor
            }
            
            log.info(f"Mapped {doc_num} -> {item['code']} ({doc_type})")
    
    return item_mappings

def _extract_agenda_items(text: str) -> List[Dict]:
    """Extract all agenda items from text."""
    items = []
    
    # Try each pattern
    for pattern in AgendaItemParser.ITEM_PATTERNS:
        matches = pattern.finditer(text)
        for match in matches:
            letter, number, description = match.groups()
            code = f"{letter}-{number}"
            
            # Extract full item text (until next item or section)
            start_pos = match.start()
            end_pos = _find_item_end(text, start_pos)
            full_description = text[match.start():end_pos].strip()
            
            items.append({
                'code': code,
                'letter': letter,
                'number': number,
                'description': full_description
            })
    
    # Remove duplicates and sort
    seen = set()
    unique_items = []
    for item in sorted(items, key=lambda x: (x['letter'], int(x['number']))):
        if item['code'] not in seen:
            seen.add(item['code'])
            unique_items.append(item)
    
    return unique_items

def _find_item_end(text: str, start_pos: int) -> int:
    """Find where an agenda item description ends."""
    # Look for next item pattern or section header
    next_item_pos = len(text)
    
    # Check for next item
    for pattern in AgendaItemParser.ITEM_PATTERNS:
        match = pattern.search(text, start_pos + 1)
        if match:
            next_item_pos = min(next_item_pos, match.start())
    
    # Check for section headers
    section_pattern = re.compile(r'^[A-Z][.\s]+[A-Z\s]+$', re.MULTILINE)
    section_match = section_pattern.search(text, start_pos + 1)
    if section_match:
        next_item_pos = min(next_item_pos, section_match.start())
    
    return next_item_pos

def _extract_document_numbers(text: str) -> List[str]:
    """Extract document numbers from item description."""
    numbers = []
    
    for pattern in AgendaItemParser.DOC_NUMBER_PATTERNS:
        matches = pattern.findall(text)
        numbers.extend(matches)
    
    # Also look for standalone year-number patterns
    standalone_pattern = re.compile(r'\b(\d{4}-\d+)\b')
    matches = standalone_pattern.findall(text)
    for match in matches:
        if match not in numbers:  # Avoid duplicates
            numbers.append(match)
    
    return numbers

def _determine_document_type(text: str) -> str:
    """Determine document type from description text."""
    text_lower = text.lower()
    
    # Check each type's indicators
    for doc_type, indicators in AgendaItemParser.TYPE_INDICATORS.items():
        for indicator in indicators:
            if indicator in text_lower:
                return doc_type.title()
    
    # Check explicit type mentions
    if 'ordinance' in text_lower:
        return 'Ordinance'
    elif 'resolution' in text_lower:
        return 'Resolution'
    
    return 'Document'  # Default

def _extract_sponsor(text: str) -> Optional[str]:
    """Extract sponsor name from item description."""
    # Common sponsor patterns
    patterns = [
        re.compile(r'(?:Sponsored by|Sponsor:)\s*([^,\n]+)', re.IGNORECASE),
        re.compile(r'\(([^)]+)\)$'),  # Name in parentheses at end
        re.compile(r'(?:Commissioner|Mayor|Vice Mayor)\s+([A-Za-z\s]+?)(?:\n|$)', re.IGNORECASE)
    ]
    
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            sponsor = match.group(1).strip()
            # Clean up common suffixes
            sponsor = re.sub(r'\s*\)$', '', sponsor)
            return sponsor
    
    return None 