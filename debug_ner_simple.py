#!/usr/bin/env python3
"""
Simple Debug NER Extraction Script - Stage 2B Testing
Shows the actual prompts and results without interfering with the LLM calls.
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
from scripts.graph_rag_stages.phase2_building.ner.enhanced_ner_extractor import EnhancedNERExtractor

# Setup detailed logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"debug_ner_simple_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)

log = logging.getLogger(__name__)


class SimpleDebugExtractor:
    """Simple debug wrapper that shows what the NER extractor is doing"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
        
        # Create debug subdirectory
        self.debug_dir = self.output_dir / "debug_simple"
        self.debug_dir.mkdir(exist_ok=True)
        
        log.info(f"🐛 Simple Debug NER Extractor initialized")
        log.info(f"   Output directory: {self.output_dir}")
        log.info(f"   Debug directory: {self.debug_dir}")

    async def debug_document_simple(self, markdown_file: Path) -> None:
        """Debug NER extraction for a single document with basic logging"""
        
        log.info("=" * 80)
        log.info(f"🔍 DEBUGGING DOCUMENT: {markdown_file.name}")
        log.info("=" * 80)
        
        # Read the markdown content to show what we're processing
        with open(markdown_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        log.info(f"📄 Document size: {len(content)} characters")
        log.info(f"📝 Content preview (first 300 chars):")
        log.info(f"   {content[:300]}...")
        log.info("")
        
        # Step 1: Chunk the document
        log.info("📄 STEP 1: CHUNKING DOCUMENT")
        chunker = MarkdownChunker(self.output_dir, chunk_size=2000, chunk_overlap=200)
        
        # Create a temporary markdown directory for this single file
        temp_markdown_dir = self.debug_dir / "temp_markdown"
        temp_markdown_dir.mkdir(exist_ok=True)
        
        # Copy the file to temp directory
        import shutil
        temp_file = temp_markdown_dir / markdown_file.name
        shutil.copy2(markdown_file, temp_file)
        
        try:
            chunk_count = await chunker.process_directory(temp_markdown_dir)
            log.info(f"✅ Created {chunk_count} chunks from {markdown_file.name}")
            
            # Show the chunks that were created
            chunks_dir = self.output_dir / "document_chunks"
            if chunks_dir.exists():
                chunk_files = list(chunks_dir.glob("*.txt"))
                log.info(f"📊 Chunk files created: {len(chunk_files)}")
                
                for i, chunk_file in enumerate(chunk_files[-chunk_count:]):  # Show recent chunks
                    with open(chunk_file, 'r', encoding='utf-8') as f:
                        chunk_content = f.read()
                    log.info(f"   Chunk {i+1}: {chunk_file.name} ({len(chunk_content)} chars)")
                    log.info(f"      Preview: {chunk_content[:150]}...")
                    log.info("")
            
            # Step 2: Extract entities with basic logging
            log.info("🔍 STEP 2: ENTITY EXTRACTION")
            
            # Use the enhanced extractor
            extractor = EnhancedNERExtractor(self.output_dir)
            
            # Enable more detailed logging for the extractor
            extractor_logger = logging.getLogger('scripts.graph_rag_stages.phase2_building.ner.enhanced_ner_extractor')
            extractor_logger.setLevel(logging.INFO)
            
            # Process all chunks for this document
            entity_count = await extractor.process_all_chunks()
            
            log.info(f"✅ COMPLETED: Extracted {entity_count} total entities from {markdown_file.name}")
            
            # Show the results
            log.info("📊 RESULTS SUMMARY:")
            await self._show_extraction_results()
            
        except Exception as e:
            log.error(f"❌ Error processing {markdown_file.name}: {e}", exc_info=True)
        
        finally:
            # Clean up temp directory
            shutil.rmtree(temp_markdown_dir, ignore_errors=True)

    async def _show_extraction_results(self):
        """Show a summary of the extraction results"""
        
        # Check each entity type directory
        entity_types = ['Person', 'Organization', 'Document', 'Policy', 'Event', 
                       'Action', 'Asset', 'Project', 'Location', 'Role', 'Topic',
                       'AgendaItem', 'Section', 'Contract', 'Technology', 'VoteOutcome']
        
        total_entities = 0
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
                            
                            # Show sample entities
                            if entities:
                                log.info(f"   {entity_type}: {len(entities)} entities in {entity_file.name}")
                                for i, entity in enumerate(entities[:2]):  # Show first 2
                                    log.info(f"      [{i+1}] {json.dumps(entity, indent=8)}")
                                if len(entities) > 2:
                                    log.info(f"      ... and {len(entities) - 2} more")
                                log.info("")
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
                    
                    if relationships:
                        log.info(f"🔗 Relationships in {rel_file.name}: {len(relationships)}")
                        for i, rel in enumerate(relationships[:3]):  # Show first 3
                            log.info(f"   [{i+1}] {json.dumps(rel, indent=6)}")
                        if len(relationships) > 3:
                            log.info(f"   ... and {len(relationships) - 3} more")
                        log.info("")
                except Exception as e:
                    log.warning(f"Error reading relationships from {rel_file}: {e}")
            
            if total_relationships > 0:
                log.info(f"🔗 Total relationships: {total_relationships}")
        
        log.info(f"🎯 OVERALL TOTAL: {total_entities} entities extracted")

    async def run_simple_test(self):
        """Run simple debug test on sample documents"""
        
        log.info("🚀 STARTING SIMPLE DEBUG NER EXTRACTION TEST")
        log.info("=" * 80)
        
        markdown_dir = project_root / "city_clerk_documents" / "extracted_markdown"
        
        # Test with just one file first to see the process clearly
        test_files = [
            "2024-01 - 01_09_2024_enhanced_ordinance.md",      # Start with ordinance
        ]
        
        for filename in test_files:
            file_path = markdown_dir / filename
            if file_path.exists():
                await self.debug_document_simple(file_path)
                log.info("\n" + "="*80 + "\n")
            else:
                log.warning(f"⚠️ Test file not found: {filename}")
        
        log.info("🎉 SIMPLE DEBUG NER EXTRACTION TEST COMPLETED")
        log.info(f"📁 Results saved to: {self.output_dir}")


async def main():
    """Main debug function"""
    
    # Use same output directory as main pipeline
    debug_output_dir = project_root / "debug_ner_simple"
    
    log.info("🐛 SIMPLE DEBUG NER EXTRACTION - STAGE 2B TESTING")
    log.info(f"📁 Working directory: {project_root}")
    log.info(f"📂 Output directory: {debug_output_dir}")
    
    # Initialize debug extractor
    debug_extractor = SimpleDebugExtractor(debug_output_dir)
    
    # Run the debug test
    await debug_extractor.run_simple_test()


if __name__ == "__main__":
    asyncio.run(main()) 