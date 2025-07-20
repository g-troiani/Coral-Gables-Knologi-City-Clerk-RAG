"""
Use regex patterns to find entities before LLM extraction
"""

import re
from typing import Dict, List, Set

class PatternBasedPreExtractor:
    """Extract obvious entities using patterns before LLM."""
    
    # Patterns for common entities
    PATTERNS = {
        'Person': [
            r'(?:Mayor|Commissioner|Manager|Attorney|Clerk)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
            r'(?:Mr\.|Ms\.|Mrs\.|Dr\.)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:moved|seconded|presented)',
        ],
        'Asset': [
            r'\$[\d,]+(?:\.\d+)?(?:\s*(?:million|billion))?',
            r'[\d,]+(?:\.\d+)?\s+dollars',
        ],
        'Location': [
            r'\d+\s+[A-Z][a-z]+\s+(?:Street|Avenue|Boulevard|Way|Drive|Road|Lane)',
            r'(?:City\s+Hall|Commission\s+Chambers)',
        ],
        'Organization': [
            r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Department|Division|Commission|Committee)',
            r'(?:City\s+of\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
        ],
        'Policy': [
            r'(?:Ordinance|Resolution)\s+(?:No\.\s+)?(\d{4}-\d+)',
            r'(?:Section|Article)\s+(\d+(?:\.\d+)*)',
        ]
    }
    
    def pre_extract(self, text: str) -> Dict[str, Set[str]]:
        """Extract entities using patterns."""
        found_entities = {}
        
        for entity_type, patterns in self.PATTERNS.items():
            found_entities[entity_type] = set()
            
            for pattern in patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    # Get the captured group
                    entity_text = match.group(1) if match.lastindex else match.group(0)
                    found_entities[entity_type].add(entity_text.strip())
        
        return found_entities
    
    def create_enhanced_prompt(self, text: str, pre_extracted: Dict[str, Set[str]]) -> str:
        """Create prompt with pre-extracted entities for validation."""
        
        prompt = "I found these potential entities using patterns:\n\n"
        
        for entity_type, entities in pre_extracted.items():
            if entities:
                prompt += f"{entity_type}:\n"
                for entity in list(entities)[:10]:  # First 10
                    prompt += f"  - {entity}\n"
                prompt += "\n"
        
        prompt += """
Please:
1. Validate these entities (are they correct?)
2. Add any missing attributes
3. Find additional entities I missed
4. Extract relationships between all entities

Text:
"""
        
        return prompt + text 