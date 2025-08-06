#!/usr/bin/env python3
"""
Debug Main Pipeline Path - Stage 2B Testing
Replicates the EXACT main pipeline entity extraction path to find the failure point.
"""

import asyncio
import logging
import sys
import json
from pathlib import Path
from datetime import datetime

# Add project paths
project_root = Path(__file__).parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "scripts"))

from scripts.graph_rag_stages.phase1_preprocessing.ner.markdown_chunker import MarkdownChunker
from scripts.graph_rag_stages.phase2_building.integrated_pipeline import IntegratedEntityPipeline
from scripts.graph_rag_stages.phase3_querying.ner import UnifiedQueryEngine

# Setup detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"debug_main_path_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)

log = logging.getLogger(__name__)


class MainPipelinePathDebugger:
    """Debug the exact main pipeline path to find where entity extraction fails"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
        
        # Create debug subdirectory
        self.debug_dir = self.output_dir / "debug_main_path"
        self.debug_dir.mkdir(exist_ok=True)
        
        log.info(f"🐛 Main Pipeline Path Debugger initialized")
        log.info(f"   Output directory: {self.output_dir}")
        log.info(f"   Debug directory: {self.debug_dir}")

    async def debug_main_pipeline_exact_path(self, markdown_source_dir: Path):
        """Debug the EXACT main pipeline path: UnifiedQueryEngine → IntegratedEntityPipeline"""
        
        log.info("=" * 80)
        log.info("🔍 DEBUGGING MAIN PIPELINE EXACT PATH")
        log.info("=" * 80)
        
        try:
            # EXACT REPLICATION OF MAIN PIPELINE STAGE 2B
            log.info("📋 Extracting Phase 1 entities for context (simulated)")
            phase1_entities = []  # Empty for now, same as main pipeline often has
            log.info(f"📋 Extracted {len(phase1_entities)} Phase 1 entities for context")
            
            # EXACT: Initialize UnifiedQueryEngine like main pipeline
            log.info("🔧 Initializing UnifiedQueryEngine (EXACT main pipeline path)")
            query_engine = UnifiedQueryEngine(self.output_dir)
            
            # EXACT: Call initialize_pipeline with same parameters as main pipeline
            log.info("🚀 Calling query_engine.initialize_pipeline() with main pipeline parameters")
            log.info(f"   markdown_source_dir: {markdown_source_dir}")
            log.info(f"   chunk_size: 2000")  # Same as main pipeline
            log.info(f"   chunk_overlap: 200")  # Same as main pipeline
            log.info(f"   use_integrated_pipeline: True")  # Same as main pipeline
            log.info(f"   phase1_entities: {len(phase1_entities)} entities")
            
            await query_engine.initialize_pipeline(
                markdown_source_dir=markdown_source_dir,
                chunk_size=2000,  # EXACT: Same as main pipeline
                chunk_overlap=200,  # EXACT: Same as main pipeline
                use_integrated_pipeline=True,  # EXACT: Same as main pipeline
                phase1_entities=phase1_entities  # EXACT: Same as main pipeline
            )
            
            log.info("✅ Main pipeline path completed - checking results...")
            
            # Check results
            await self._analyze_results()
            
        except Exception as e:
            log.error(f"❌ Main pipeline path failed: {e}", exc_info=True)

    async def debug_integrated_pipeline_directly(self, markdown_source_dir: Path):
        """Debug IntegratedEntityPipeline directly to isolate the issue"""
        
        log.info("=" * 80)
        log.info("🔍 DEBUGGING INTEGRATED PIPELINE DIRECTLY")
        log.info("=" * 80)
        
        try:
            # Step 1: Manual chunking (same as UnifiedQueryEngine does)
            log.info("📄 STEP 1: Manual chunking")
            chunker = MarkdownChunker(self.output_dir, chunk_size=2000, chunk_overlap=200)
            chunk_count = await chunker.process_directory(markdown_source_dir)
            log.info(f"✅ Created {chunk_count} chunks")
            
            if chunk_count == 0:
                log.error("❌ No chunks created - stopping")
                return
            
            # Step 2: Direct IntegratedEntityPipeline testing
            log.info("🔍 STEP 2: Testing IntegratedEntityPipeline directly")
            
            # Test both paths of IntegratedEntityPipeline
            integrated = IntegratedEntityPipeline(self.output_dir)
            
            # Test Path 1: process_chunks_standard (no phase1 entities)
            log.info("🧪 Testing integrated.process_chunks_standard() - no phase1 entities")
            try:
                entity_count = await integrated.process_chunks_standard()
                log.info(f"✅ process_chunks_standard() returned: {entity_count} entities")
            except Exception as e:
                log.error(f"❌ process_chunks_standard() failed: {e}", exc_info=True)
            
            # Check what was actually created
            log.info("📊 Checking what entities were actually created...")
            await self._analyze_results()
            
        except Exception as e:
            log.error(f"❌ Integrated pipeline direct test failed: {e}", exc_info=True)

    async def debug_enhanced_extractor_directly(self, markdown_source_dir: Path):
        """Debug EnhancedNERExtractor directly (bypass IntegratedEntityPipeline)"""
        
        log.info("=" * 80)
        log.info("🔍 DEBUGGING ENHANCED EXTRACTOR DIRECTLY (bypass wrapper)")
        log.info("=" * 80)
        
        try:
            # Step 1: Manual chunking
            log.info("📄 STEP 1: Manual chunking")
            chunker = MarkdownChunker(self.output_dir, chunk_size=2000, chunk_overlap=200)
            chunk_count = await chunker.process_directory(markdown_source_dir)
            log.info(f"✅ Created {chunk_count} chunks")
            
            if chunk_count == 0:
                log.error("❌ No chunks created - stopping")
                return
            
            # Step 2: Direct EnhancedNERExtractor testing (same as debug pipeline)
            log.info("🔍 STEP 2: Testing EnhancedNERExtractor directly (bypass IntegratedEntityPipeline)")
            
            from scripts.graph_rag_stages.phase2_building.ner.enhanced_ner_extractor import EnhancedNERExtractor
            extractor = EnhancedNERExtractor(self.output_dir)
            
            log.info("🧪 Calling extractor.process_all_chunks() directly")
            try:
                entity_count = await extractor.process_all_chunks()
                log.info(f"✅ Direct extractor returned: {entity_count} entities")
            except Exception as e:
                log.error(f"❌ Direct extractor failed: {e}", exc_info=True)
            
            # Check what was actually created
            log.info("📊 Checking what entities were actually created...")
            await self._analyze_results()
            
        except Exception as e:
            log.error(f"❌ Enhanced extractor direct test failed: {e}", exc_info=True)

    async def _analyze_results(self):
        """Analyze what entities were actually created"""
        
        # Check each entity type directory
        entity_types = ['Person', 'Organization', 'Document', 'Policy', 'Event', 
                       'Action', 'Asset', 'Project', 'Location', 'Role', 'Topic',
                       'AgendaItem', 'Section', 'Contract', 'Technology', 'VoteOutcome']
        
        total_entities = 0
        found_2024_01 = False
        
        for entity_type in entity_types:
            entity_dir = self.output_dir / entity_type
            if entity_dir.exists():
                entity_files = list(entity_dir.glob("*.json"))
                if entity_files:
                    type_total = 0
                    for entity_file in entity_files:
                        try:
                            with open(entity_file, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            entities = data.get('entities', [])
                            type_total += len(entities)
                            
                            # Check for our test chunk
                            if "fd96be7ea706" in entity_file.name and "2024-01" in entity_file.name:
                                found_2024_01 = True
                                log.info(f"🎯 FOUND 2024-01 ordinance entities in {entity_type}: {len(entities)} entities")
                                for i, entity in enumerate(entities[:2]):
                                    log.info(f"     [{i+1}] {json.dumps(entity, indent=8)}")
                        except Exception as e:
                            log.warning(f"Error reading {entity_file}: {e}")
                    
                    if type_total > 0:
                        log.info(f"📋 {entity_type}: {type_total} total entities")
                        total_entities += type_total
        
        # Check relationships
        rel_dir = self.output_dir / "relationships"
        if rel_dir.exists():
            rel_files = list(rel_dir.glob("*.json"))
            total_relationships = 0
            for rel_file in rel_files:
                try:
                    with open(rel_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    relationships = data.get('relationships', [])
                    total_relationships += len(relationships)
                    
                    # Check for our test chunk
                    if "fd96be7ea706" in rel_file.name and "2024-01" in rel_file.name:
                        log.info(f"🎯 FOUND 2024-01 ordinance relationships: {len(relationships)}")
                except Exception as e:
                    log.warning(f"Error reading relationships from {rel_file}: {e}")
            
            if total_relationships > 0:
                log.info(f"🔗 Total relationships: {total_relationships}")
        
        log.info(f"🎯 FINAL RESULT: {total_entities} entities extracted")
        if found_2024_01:
            log.info("✅ SUCCESS: 2024-01 ordinance entities FOUND")
        else:
            log.error("❌ FAILURE: 2024-01 ordinance entities NOT FOUND")
        
        return found_2024_01, total_entities

    async def run_comprehensive_debug(self):
        """Run all debug tests to isolate the exact failure point"""
        
        log.info("🚀 STARTING COMPREHENSIVE MAIN PIPELINE DEBUG")
        log.info("=" * 80)
        
        markdown_dir = project_root / "city_clerk_documents" / "extracted_markdown"
        
        # Clean output directory for fresh test
        import shutil
        if self.output_dir.exists():
            shutil.rmtree(self.output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        log.info("🧪 TEST 1: Main Pipeline Exact Path (UnifiedQueryEngine)")
        await self.debug_main_pipeline_exact_path(markdown_dir)
        
        log.info("\n" + "="*80 + "\n")
        
        # Clean for next test
        if self.output_dir.exists():
            shutil.rmtree(self.output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        log.info("🧪 TEST 2: IntegratedEntityPipeline Direct (bypass UnifiedQueryEngine)")
        await self.debug_integrated_pipeline_directly(markdown_dir)
        
        log.info("\n" + "="*80 + "\n")
        
        # Clean for next test
        if self.output_dir.exists():
            shutil.rmtree(self.output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        log.info("🧪 TEST 3: EnhancedNERExtractor Direct (bypass IntegratedEntityPipeline)")
        await self.debug_enhanced_extractor_directly(markdown_dir)
        
        log.info("=" * 80)
        log.info("🎉 COMPREHENSIVE DEBUG COMPLETED")
        log.info("=" * 80)


async def main():
    """Main debug function"""
    
    # Use different output directory to avoid conflicts
    debug_output_dir = project_root / "debug_main_path"
    
    log.info("🐛 DEBUG MAIN PIPELINE PATH - FIND THE FAILURE")
    log.info(f"📁 Working directory: {project_root}")
    log.info(f"📂 Output directory: {debug_output_dir}")
    
    # Initialize debugger
    debugger = MainPipelinePathDebugger(debug_output_dir)
    
    # Run comprehensive debug
    await debugger.run_comprehensive_debug()


if __name__ == "__main__":
    asyncio.run(main()) 