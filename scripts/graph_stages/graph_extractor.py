"""
Graph Data Extractor
===================
Specialized extraction logic for graph entities and relationships.
"""
import re
from typing import Dict, List, Optional, Tuple
import logging

log = logging.getLogger(__name__)

def extract_voting_records(verbatim_data: Dict) -> List[Dict]:
    """Extract voting records from verbatim transcripts."""
    voting_records = []
    
    # TODO: Implement voting extraction from verbatim transcripts
    # This would parse the transcript to find voting patterns like:
    # "Motion passes 5-0" or "Yeas: Lago, Anderson, Castro..."
    
    return voting_records

def extract_document_references(doc_data: Dict) -> List[Dict]:
    """Extract references to other documents."""
    references = []
    text = " ".join(s.get("text", "") for s in doc_data.get("sections", []))
    
    # Find references to other ordinances/resolutions
    patterns = [
        re.compile(r'(?:amends?|amending)\s+(?:Ordinance|Resolution)\s+(?:No\.?\s*)?(\d{4}-\d+)', re.IGNORECASE),
        re.compile(r'(?:repeals?|repealing)\s+(?:Ordinance|Resolution)\s+(?:No\.?\s*)?(\d{4}-\d+)', re.IGNORECASE),
        re.compile(r'(?:pursuant to|per|under)\s+(?:Ordinance|Resolution)\s+(?:No\.?\s*)?(\d{4}-\d+)', re.IGNORECASE),
    ]
    
    for pattern in patterns:
        matches = pattern.finditer(text)
        for match in matches:
            ref_type = 'amends' if 'amend' in match.group(0).lower() else \
                      'repeals' if 'repeal' in match.group(0).lower() else \
                      'references'
            
            references.append({
                'document_number': match.group(1),
                'reference_type': ref_type,
                'context': match.group(0)
            })
    
    return references

def extract_financial_data(doc_data: Dict) -> Optional[Dict]:
    """Extract financial information from documents."""
    text = " ".join(s.get("text", "") for s in doc_data.get("sections", []))
    
    # Look for dollar amounts
    amount_pattern = re.compile(r'\$[\d,]+(?:\.\d{2})?')
    amounts = amount_pattern.findall(text)
    
    if amounts:
        # Convert to numeric values
        numeric_amounts = []
        for amount in amounts:
            clean_amount = amount.replace('$', '').replace(',', '')
            try:
                numeric_amounts.append(float(clean_amount))
            except ValueError:
                continue
        
        if numeric_amounts:
            return {
                'amounts': numeric_amounts,
                'total': sum(numeric_amounts),
                'max_amount': max(numeric_amounts)
            }
    
    return None

def extract_locations(doc_data: Dict) -> List[str]:
    """Extract location/address mentions from documents."""
    locations = []
    text = " ".join(s.get("text", "") for s in doc_data.get("sections", []))
    
    # Common Coral Gables street patterns
    street_pattern = re.compile(
        r'\b\d+\s+(?:North|South|East|West|N|S|E|W|NW|NE|SW|SE)?\s*'
        r'(?:Street|Avenue|Road|Boulevard|Way|Place|Court|Drive|Lane|Terrace|Circle)'
        r'(?:\s+(?:North|South|East|West|N|S|E|W|NW|NE|SW|SE))?\b',
        re.IGNORECASE
    )
    
    matches = street_pattern.findall(text)
    locations.extend(matches)
    
    # Also look for specific landmarks
    landmarks = [
        'City Hall', 'Biltmore Hotel', 'Miracle Mile', 
        'Venetian Pool', 'Coral Gables Museum'
    ]
    
    for landmark in landmarks:
        if landmark.lower() in text.lower():
            locations.append(landmark)
    
    return list(set(locations))  # Remove duplicates 