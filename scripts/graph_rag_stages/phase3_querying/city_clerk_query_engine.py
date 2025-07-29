import sys
import os
import json
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Set
from enum import Enum
import logging
from .smart_query_router import SmartQueryRouter, QueryIntent
from .source_tracker import SourceTracker
from .structural_query_enhancer import StructuralQueryEnhancer
from .ner.simple_query_engine import SimpleNERQueryEngine

logger = logging.getLogger(__name__)

class CityClerkQueryEngine:
    """Enhanced query engine with inline source citations."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.source_tracker = SourceTracker()  # New component
        
        # Initialize structural query enhancer for completeness queries
        extracted_text_dir = self.output_dir.parent / "city_clerk_documents" / "extracted_json"
        self.structural_enhancer = StructuralQueryEnhancer(extracted_text_dir)
        
        # Initialize NER query engine
        self.ner_engine = SimpleNERQueryEngine(self.output_dir)
        
    async def query(self, query: str, **kwargs) -> Dict[str, Any]:
        """Execute query with source tracking and inline citations."""
        
        # Enable source tracking
        kwargs['track_sources'] = True
        
        # Determine which entities to search
        phase1_context = self._get_phase1_context(query)
        
        # If query mentions sections/agenda items, enrich with Phase 1
        if phase1_context:
            kwargs['seed_entities'] = phase1_context
            kwargs['include_structural_context'] = True
        
        # Route query
        router = SmartQueryRouter()
        route_info = router.determine_query_method(query)
        kwargs.update(route_info.get('params', {}))
        
        # Execute query with source tracking
        result = await self.ner_engine.query(query, **kwargs)
        
        # Clean up any JSON artifacts from the answer
        result['answer'] = self._clean_json_artifacts(result['answer'])
        
        # Process answer to add inline citations
        result['answer'] = self._add_inline_citations(result['answer'], result.get('sources_used', {}))
        
        # Apply structural enhancement for completeness queries
        result = self.structural_enhancer.enhance_response(query, result)
        
        return result
    
    def _get_phase1_context(self, query: str) -> List[Dict]:
        """Get Phase 1 entities relevant to the query."""
        phase1_entities = []
        
        query_lower = query.lower()
        
        # Check for section references
        if any(word in query_lower for word in ['section', 'agenda', 'ordinance', 'resolution']):
            # Extract phase1 entities from structural data
            try:
                extracted_text_dir = self.output_dir.parent / "city_clerk_documents" / "extracted_json"
                if extracted_text_dir.exists():
                    # Look for stage2 and stage3 files that contain section entities
                    for stage_dir in ['stage2', 'stage3']:
                        stage_path = extracted_text_dir / stage_dir
                        if stage_path.exists():
                            for json_file in stage_path.glob("*.json"):
                                try:
                                    import json
                                    with open(json_file, 'r', encoding='utf-8') as f:
                                        data = json.load(f)
                                    
                                    # Extract section entities if present
                                    section_entities = data.get('section_entities', [])
                                    for entity in section_entities:
                                        # Check if relevant to query
                                        if any(term in entity.get('name', '').lower() for term in query_lower.split()):
                                            phase1_entities.append(entity)
                                    
                                except Exception as e:
                                    continue
            except Exception as e:
                log.debug(f"Could not extract Phase 1 context: {e}")
        
        return phase1_entities
    
    def _clean_json_artifacts(self, answer: str) -> str:
        """Clean JSON artifacts and metadata from response."""
        if not answer:
            return answer
            
        import re
        
        # Remove metadata headers that sometimes appear
        metadata_patterns = [
            r'SUCCESS:\s*.*?\n',
            r'INFO:\s*.*?\n',
            r'DEBUG:\s*.*?\n',
            r'WARNING:\s*.*?\n',
            r'ERROR:\s*.*?\n',
            r'METADATA:\s*.*?\n',
            r'RESPONSE:\s*',
            r'QUERY:\s*.*?\n',
        ]
        
        for pattern in metadata_patterns:
            answer = re.sub(pattern, '', answer, flags=re.IGNORECASE)
        
        # Remove multiple consecutive newlines
        answer = re.sub(r'\n\s*\n\s*\n', '\n\n', answer)
        
        # Remove leading/trailing whitespace
        answer = answer.strip()
        
        return answer
    
    def _add_inline_citations(self, answer: str, sources_used: Dict[str, Any]) -> str:
        """Add inline citations to answer text."""
        
        # Extract entity and relationship IDs for citation
        entity_ids = list(sources_used.get('entities', {}).keys())
        rel_ids = list(sources_used.get('relationships', {}).keys())
        source_ids = list(sources_used.get('sources', {}).keys())
        
        # Split answer into paragraphs
        paragraphs = answer.split('\n\n')
        cited_paragraphs = []
        
        for para in paragraphs:
            if not para.strip():
                cited_paragraphs.append(para)
                continue
            
            # Determine which sources are relevant to this paragraph
            relevant_entities = []
            relevant_rels = []
            relevant_sources = []
            
            # Simple relevance check based on entity mentions
            para_lower = para.lower()
            
            for eid, entity in sources_used.get('entities', {}).items():
                if entity['title'].lower() in para_lower or \
                   any(word in para_lower for word in entity.get('description', '').lower().split()[:10]):
                    relevant_entities.append(str(eid))
            
            for rid, rel in sources_used.get('relationships', {}).items():
                if any(word in para_lower for word in rel.get('description', '').lower().split()[:10]):
                    relevant_rels.append(str(rid))
            
            # Add generic source references
            if relevant_entities or relevant_rels:
                relevant_sources = source_ids[:3]  # Use first few sources
            
            # Build citation
            if relevant_entities or relevant_rels or relevant_sources:
                citation_parts = []
                
                if relevant_sources:
                    citation_parts.append(f"Sources ({', '.join(map(str, relevant_sources[:5]))})")
                
                if relevant_entities:
                    citation_parts.append(f"Entities ({', '.join(relevant_entities[:7])})")
                
                if relevant_rels:
                    citation_parts.append(f"Relationships ({', '.join(relevant_rels[:5])})")
                
                citation = f" Data: {'; '.join(citation_parts)}."
                cited_paragraphs.append(para + citation)
            else:
                cited_paragraphs.append(para)
        
        return '\n\n'.join(cited_paragraphs) 