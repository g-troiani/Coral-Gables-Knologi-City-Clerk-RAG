"""
Integrated Entity Pipeline - Coordinates all enhanced NER components
"""

from pathlib import Path
from typing import Dict, List, Any
import logging
import asyncio

from .ner.pattern_extractor import PatternBasedPreExtractor
from .ner.enhanced_ner_extractor import EnhancedNERExtractor
from scripts.graph_rag_stages.common.entity_bridge import EntityBridge

log = logging.getLogger(__name__)

class IntegratedEntityPipeline:
    """Coordinates enhanced NER extraction with Phase 1 context awareness."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.pattern_extractor = PatternBasedPreExtractor()
        self.enhanced_extractor = EnhancedNERExtractor(output_dir)
        
    async def process_with_phase1_context(self, phase1_entities: List[Dict]) -> None:
        """Process chunks with awareness of Phase 1 entities"""
        
        # 1. Convert Phase 1 entities to Phase 2 format
        phase2_seed_entities = []
        for entity in phase1_entities:
            phase2_type, phase2_entity = EntityBridge.convert_phase1_to_phase2(entity)
            phase2_seed_entities.append(phase2_entity)
        
        log.info(f"Converted {len(phase1_entities)} Phase 1 entities to Phase 2 format")
        
        # 2. Use seed entities to enhance extraction
        # Pass them to enhanced extractor as context
        self.enhanced_extractor.seed_entities = phase2_seed_entities
        
        # 3. Process all chunks with enhanced context
        await self.enhanced_extractor.process_all_chunks()
        
        log.info("✅ Integrated pipeline processing completed")
    
    async def process_chunks_standard(self) -> int:
        """Standard processing without Phase 1 context"""
        
        # Process all chunks with enhanced extraction
        entity_count = await self.enhanced_extractor.process_all_chunks()
        
        log.info(f"✅ Enhanced extraction completed with {entity_count} entities")
        return entity_count
    
    def get_extraction_stats(self) -> Dict[str, Any]:
        """Get statistics from the integrated pipeline"""
        
        stats = {
            'pipeline_type': 'integrated_enhanced',
            'components': {
                'pattern_extractor': True,
                'enhanced_ner_extractor': True,
                'entity_bridge': True
            },
            'output_directory': str(self.output_dir)
        }
        
        return stats 