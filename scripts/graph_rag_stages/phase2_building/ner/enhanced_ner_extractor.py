"""
Enhanced NER extractor - DEPRECATED
This file is maintained for backward compatibility only.
Use phase2_new_extractor.py instead.
"""

import logging
import warnings
from pathlib import Path
from typing import Dict, List, Any, Optional

from .phase2_new_extractor import Phase2NEWExtractor

log = logging.getLogger(__name__)

class EnhancedNERExtractor:
    """
    Compatibility wrapper for EnhancedNERExtractor.
    Redirects to Phase2NEWExtractor which is the new standard implementation.
    """
    
    def __init__(self, output_dir, seed_entities=None):
        warnings.warn(
            "EnhancedNERExtractor is deprecated. Use Phase2NEWExtractor directly.",
            DeprecationWarning,
            stacklevel=2
        )
        self.output_dir = Path(output_dir)
        self.extractor = Phase2NEWExtractor(output_dir)
        log.warning("EnhancedNERExtractor initialized in compatibility mode - redirecting to Phase2NEWExtractor")
    
    async def extract_entities_from_chunks(self, chunks_dir: Path, phase1_entities: Optional[List[Dict]] = None) -> Dict[str, int]:
        """Redirect to new extractor."""
        return await self.extractor.extract_entities_from_chunks(chunks_dir, phase1_entities)
    
    async def process_chunk(self, chunk_data: Dict, chunk_metadata: Dict) -> Dict:
        """Redirect to new extractor."""
        # Phase2NEWExtractor expects chunk files, not data dicts
        # This is a compatibility issue that callers need to handle
        raise NotImplementedError(
            "Direct chunk processing not supported in compatibility mode. "
            "Use extract_entities_from_chunks with chunk files instead."
        )
    
    def __getattr__(self, name):
        """Forward any other attribute access to the new extractor."""
        return getattr(self.extractor, name)
