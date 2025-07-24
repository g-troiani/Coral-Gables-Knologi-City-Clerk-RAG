#!/usr/bin/env python3
"""
Debug NER Extraction Script - Stage 2B Testing
Replicates the exact NER extraction logic from main_pipeline.py with detailed debugging.
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
from scripts.graph_rag_stages.phase2_building.ner.enhanced_ner_extractor import EnhancedNERExtractor

# Setup detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"debug_ner_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)

log = logging.getLogger(__name__)


class DebugNERExtractor:
    """Debug wrapper around the NER extraction process"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
        
        # Create debug subdirectory
        self.debug_dir = self.output_dir / "debug_output"
        self.debug_dir.mkdir(exist_ok=True)
        
        log.info(f"🐛 Debug NER Extractor initialized")
        log.info(f"   Output directory: {self.output_dir}")
        log.info(f"   Debug directory: {self.debug_dir}")

    async def debug_single_document(self, markdown_file: Path) -> None:
        """Debug NER extraction for a single document with detailed logging"""
        
        log.info("=" * 80)
        log.info(f"🔍 DEBUGGING DOCUMENT: {markdown_file.name}")
        log.info("=" * 80)
        
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
            
            # Step 2: Extract entities with debugging
            log.info("🔍 STEP 2: ENTITY EXTRACTION WITH DEBUG LOGGING")
            
            # Use the enhanced extractor directly for more control
            extractor = EnhancedNERExtractor(self.output_dir)
            
            # Monkey patch both the extractor methods and the LLM client to capture prompts
            original_extract = extractor._extract_entities_llm
            original_client_create = extractor.client.chat.completions.create
            
            # Store prompts for this chunk
            chunk_prompts = []
            
            def debug_client_create(*args, **kwargs):
                """Capture the actual prompts sent to LLM"""
                messages = kwargs.get('messages', [])
                if messages:
                    prompt_content = messages[-1].get('content', '') if len(messages) > 0 else ''
                    chunk_prompts.append({
                        'prompt': prompt_content,
                        'model': kwargs.get('model', 'unknown'),
                        'temperature': kwargs.get('temperature', 0),
                        'max_tokens': kwargs.get('max_tokens', 0)
                    })
                
                # Call original method - this returns a coroutine, not awaited yet
                return original_client_create(*args, **kwargs)
            
            async def debug_extract_entities_llm(chunk_text: str, chunk_metadata: dict):
                chunk_id = chunk_metadata.get('chunk_id', 'unknown')
                
                log.info("-" * 60)
                log.info(f"🧩 PROCESSING CHUNK: {chunk_id}")
                log.info(f"📊 Chunk metadata: {json.dumps(chunk_metadata, indent=2)}")
                log.info(f"📝 Chunk text preview (first 200 chars):")
                log.info(f"   {chunk_text[:200]}...")
                log.info("-" * 60)
                
                # Clear prompts for this chunk
                chunk_prompts.clear()
                
                # Monkey patch the client temporarily
                extractor.client.chat.completions.create = debug_client_create
                
                try:
                    # Call original method
                    result = await original_extract(chunk_text, chunk_metadata)
                    
                    # Log the prompts that were used
                    log.info("🤖 LLM PROMPTS USED:")
                    for i, prompt_data in enumerate(chunk_prompts):
                        log.info(f"   PROMPT {i+1}:")
                        log.info(f"     Model: {prompt_data['model']}")
                        log.info(f"     Temperature: {prompt_data['temperature']}")
                        log.info(f"     Max tokens: {prompt_data['max_tokens']}")
                        log.info(f"     Content (first 500 chars):")
                        log.info(f"       {prompt_data['prompt'][:500]}...")
                        if len(prompt_data['prompt']) > 500:
                            log.info(f"       ... (total length: {len(prompt_data['prompt'])} chars)")
                        log.info("")
                    
                    # Log the results in detail
                    log.info("🎯 EXTRACTION RESULTS:")
                    log.info(f"   Extraction method: {result.get('extraction_method', 'unknown')}")
                    
                    entities = result.get('entities', {})
                    relationships = result.get('relationships', [])
                    
                    log.info(f"📋 EXTRACTED ENTITIES ({sum(len(v) for v in entities.values())} total):")
                    for entity_type, entity_list in entities.items():
                        if entity_list:
                            log.info(f"   {entity_type}: {len(entity_list)} entities")
                            for i, entity in enumerate(entity_list[:3]):  # Show first 3
                                log.info(f"     [{i+1}] {json.dumps(entity, indent=6)}")
                            if len(entity_list) > 3:
                                log.info(f"     ... and {len(entity_list) - 3} more")
                    
                    log.info(f"🔗 EXTRACTED RELATIONSHIPS ({len(relationships)} total):")
                    for i, rel in enumerate(relationships[:5]):  # Show first 5
                        log.info(f"   [{i+1}] {json.dumps(rel, indent=6)}")
                    if len(relationships) > 5:
                        log.info(f"   ... and {len(relationships) - 5} more")
                    
                    # Save detailed debug info to file
                    debug_file = self.debug_dir / f"chunk_{chunk_id}_debug.json"
                    debug_data = {
                        "chunk_id": chunk_id,
                        "chunk_metadata": chunk_metadata,
                        "chunk_text": chunk_text,
                        "llm_prompts": chunk_prompts,
                        "extraction_result": result,
                        "timestamp": datetime.now().isoformat()
                    }
                    
                    with open(debug_file, 'w', encoding='utf-8') as f:
                        json.dump(debug_data, f, indent=2, ensure_ascii=False)
                    
                    log.info(f"💾 Detailed debug info saved to: {debug_file}")
                    log.info("-" * 60)
                    
                    return result
                    
                finally:
                    # Restore original client method
                    extractor.client.chat.completions.create = original_client_create
            
            # Monkey patch the method
            extractor._extract_entities_llm = debug_extract_entities_llm
            
            # Process all chunks for this document
            entity_count = await extractor.process_all_chunks()
            
            log.info(f"✅ COMPLETED: Extracted {entity_count} total entities from {markdown_file.name}")
            
        except Exception as e:
            log.error(f"❌ Error processing {markdown_file.name}: {e}", exc_info=True)
        
        finally:
            # Clean up temp directory
            shutil.rmtree(temp_markdown_dir, ignore_errors=True)

    async def run_debug_test(self):
        """Run debug test on sample documents"""
        
        log.info("🚀 STARTING DEBUG NER EXTRACTION TEST")
        log.info("=" * 80)
        
        markdown_dir = project_root / "city_clerk_documents" / "extracted_markdown"
        
        # Test files: ordinance, resolution, verbatim (using smaller files for clearer debugging)
        test_files = [
            "2024-01 - 01_09_2024_enhanced_ordinance.md",      # Ordinance (4.5KB)
            "2024-01 - 01_09_2024_enhanced_resolution.md",     # Resolution (4.5KB)
            "01_09_2024 - Verbatim Transcripts - E-5.md"       # Verbatim (24KB - smaller than E-4)
        ]
        
        for filename in test_files:
            file_path = markdown_dir / filename
            if file_path.exists():
                await self.debug_single_document(file_path)
            else:
                log.warning(f"⚠️ Test file not found: {filename}")
        
        log.info("=" * 80)
        log.info("🎉 DEBUG NER EXTRACTION TEST COMPLETED")
        log.info(f"📁 Debug files saved to: {self.debug_dir}")
        log.info("=" * 80)


async def main():
    """Main debug function"""
    
    # Use same output directory as main pipeline
    debug_output_dir = project_root / "debug_ner_test"
    
    log.info("🐛 DEBUG NER EXTRACTION - STAGE 2B TESTING")
    log.info(f"📁 Working directory: {project_root}")
    log.info(f"📂 Output directory: {debug_output_dir}")
    
    # Initialize debug extractor
    debug_extractor = DebugNERExtractor(debug_output_dir)
    
    # Run the debug test
    await debug_extractor.run_debug_test()


if __name__ == "__main__":
    asyncio.run(main()) 