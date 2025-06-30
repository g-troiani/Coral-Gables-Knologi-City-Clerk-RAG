#!/usr/bin/env python3
"""
Standalone Simple NER Pipeline Runner

This script runs only the Simple NER pipeline, avoiding the import issues
in the main pipeline.
"""

import asyncio
import sys
from pathlib import Path
import logging

# Add the Simple NER module to path
sys.path.append('scripts/graph_rag_stages')

# Import Simple NER directly
import simple_ner

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

async def run_simple_ner_only():
    """Run only the Simple NER pipeline."""
    log.info("🚀 Starting Simple NER Pipeline Only")
    
    # Set up directories
    project_root = Path(__file__).resolve().parent
    markdown_source_dir = project_root / "city_clerk_documents/extracted_markdown"
    simple_ner_output_dir = project_root / "simple_ner_graph"
    
    # Check if markdown directory exists
    if not markdown_source_dir.exists():
        log.warning("⚠️ Markdown directory not found, using city_clerk_documents directory")
        markdown_source_dir = project_root / "city_clerk_documents"
    
    if not markdown_source_dir.exists():
        log.error("❌ No markdown source directory found!")
        return
    
    log.info(f"📁 Source: {markdown_source_dir}")
    log.info(f"📁 Output: {simple_ner_output_dir}")
    
    # Run Simple NER Pipeline
    log.info("▶️ Running Simple NER Pipeline (Entity-based GraphRAG)")
    
    await simple_ner.run_simple_ner_pipeline(
        markdown_source_dir=markdown_source_dir,
        output_dir=simple_ner_output_dir,
        chunk_size=1000,
        chunk_overlap=100
    )
    
    log.info("✅ Simple NER pipeline completed")
    log.info("🎉 Done!")
    log.info(f"📊 Results saved to: {simple_ner_output_dir}")
    log.info("💡 To query: use UI with 'Simple NER (Entity-based)' option")

if __name__ == "__main__":
    asyncio.run(run_simple_ner_only()) 