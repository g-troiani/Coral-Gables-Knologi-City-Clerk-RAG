"""
Entity Deduplication Module
==========================
Handles deduplication of persons and other entities across documents.
"""
import difflib
from typing import Dict, List, Optional, Set, Tuple
import logging

log = logging.getLogger(__name__)

class EntityDeduplicator:
    """Deduplicate entities across documents."""
    
    def __init__(self, similarity_threshold: float = 0.85):
        self.similarity_threshold = similarity_threshold
        self.person_aliases: Dict[str, str] = {}  # alias -> canonical name
        self._load_known_aliases()
    
    def _load_known_aliases(self):
        """Load known name variations for city officials."""
        # Common variations
        self.person_aliases.update({
            # Mayors
            "Vince Lago": "Vince Lago",
            "Vincent Lago": "Vince Lago",
            "Mayor Lago": "Vince Lago",
            
            # Commissioners
            "Rhonda Anderson": "Rhonda Anderson",
            "Vice Mayor Anderson": "Rhonda Anderson",
            
            # Add more as discovered...
        })
    
    def deduplicate_person_name(self, name: str, existing_names: List[str]) -> str:
        """
        Find canonical version of a person name.
        
        Args:
            name: The name to check
            existing_names: List of known canonical names
            
        Returns:
            Canonical name (either existing match or the input name)
        """
        # First check known aliases
        if name in self.person_aliases:
            return self.person_aliases[name]
        
        # Clean the name
        clean_name = self._clean_person_name(name)
        
        # Find best match among existing names
        best_match = self._find_best_match(clean_name, existing_names)
        
        if best_match:
            # Store alias for future use
            self.person_aliases[name] = best_match
            return best_match
        
        # No match found, this is a new canonical name
        return clean_name
    
    def _clean_person_name(self, name: str) -> str:
        """Clean and normalize a person name."""
        # Remove titles
        titles = [
            'Mayor', 'Vice Mayor', 'Commissioner', 'Dr.', 'Mr.', 'Mrs.', 'Ms.',
            'City Attorney', 'City Manager', 'City Clerk'
        ]
        
        clean = name
        for title in titles:
            clean = clean.replace(title, '').strip()
        
        # Remove extra whitespace
        clean = ' '.join(clean.split())
        
        return clean
    
    def _find_best_match(self, name: str, candidates: List[str]) -> Optional[str]:
        """Find best matching name from candidates."""
        if not candidates:
            return None
        
        # Try exact match first
        if name in candidates:
            return name
        
        # Try fuzzy matching
        matches = difflib.get_close_matches(
            name, 
            candidates, 
            n=1, 
            cutoff=self.similarity_threshold
        )
        
        if matches:
            return matches[0]
        
        # Try last name matching for "FirstName LastName" patterns
        name_parts = name.split()
        if len(name_parts) >= 2:
            last_name = name_parts[-1]
            for candidate in candidates:
                if candidate.endswith(last_name):
                    # Additional check: first name initial match
                    if name[0] == candidate[0]:
                        return candidate
        
        return None
    
    def merge_person_roles(self, existing_roles: List[str], new_roles: List[str]) -> List[str]:
        """Merge role lists, maintaining uniqueness and hierarchy."""
        # Role hierarchy (higher number = higher priority)
        role_priority = {
            'Mayor': 10,
            'Vice Mayor': 9,
            'Commissioner': 8,
            'City Attorney': 7,
            'City Manager': 7,
            'City Clerk': 6,
            'Public Works Director': 6,
            'Sponsor': 5,
            'Public Speaker': 4,
        }
        
        all_roles = set(existing_roles + new_roles)
        
        # Sort by priority
        sorted_roles = sorted(
            all_roles,
            key=lambda r: role_priority.get(r, 0),
            reverse=True
        )
        
        return sorted_roles

class MeetingDeduplicator:
    """Deduplicate meeting entities."""
    
    @staticmethod
    def normalize_date(date_str: str) -> str:
        """Normalize date string to consistent format."""
        # Handle various formats:
        # 6.11.2024 -> 06.11.2024
        # 06_11_2024 -> 06.11.2024
        # June 11, 2024 -> 06.11.2024
        
        import re
        
        # Replace underscores with dots
        normalized = date_str.replace('_', '.')
        
        # Handle M.DD.YYYY -> MM.DD.YYYY
        match = re.match(r'^(\d)\.(\d{2})\.(\d{4})$', normalized)
        if match:
            month, day, year = match.groups()
            normalized = f"{int(month):02d}.{day}.{year}"
        
        return normalized 