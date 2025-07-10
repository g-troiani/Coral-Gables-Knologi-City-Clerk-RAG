"""
Simple NER Pipeline

A simple, low-latency approach that combines document chunking with 
Named Entity Recognition for fast entity-based queries.

Components:
- NER extractor: Extracts entities using LLM
- Query engine: Fast entity-based retrieval
"""

from .ner_extractor import NERExtractor
from .simple_query_engine import SimpleNERQueryEngine
from .markdown_chunker import MarkdownChunker
from .simple_graph_builder import SimpleGraphBuilder

import logging
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

async def run_simple_ner_pipeline(
    markdown_source_dir: Path,
    output_dir: Optional[Path] = None,
    chunk_size: int = 1000,
    chunk_overlap: int = 100
) -> None:
    """
    Run the complete Simple NER pipeline.
    
    Args:
        markdown_source_dir: Directory containing markdown files
        output_dir: Output directory for simple_ner_graph
        chunk_size: Target size for chunks in tokens
        chunk_overlap: Overlap between chunks in tokens
    """
    log.info("🚀 Starting Simple NER Pipeline")
    
    if output_dir is None:
        output_dir = Path("simple_ner_graph")
    
    try:
        # Step 1: Chunk documents
        log.info("📄 Step 1: Chunking documents...")
        chunker = MarkdownChunker(output_dir, chunk_size, chunk_overlap)
        chunk_count = await chunker.process_directory(markdown_source_dir)
        log.info(f"✅ Created {chunk_count} chunks")
        
        # Step 2: Extract entities
        log.info("🔍 Step 2: Extracting entities...")
        extractor = NERExtractor(output_dir)
        entity_count = await extractor.process_all_chunks()
        log.info(f"✅ Extracted {entity_count} total entities")
        
        # Step 3: Build indices
        log.info("📊 Step 3: Building indices...")
        builder = SimpleGraphBuilder(output_dir)
        await builder.build_indices()
        log.info("✅ Built entity and chunk indices")
        
        log.info("🎉 Simple NER Pipeline completed successfully")
        
    except Exception as e:
        log.error(f"❌ Simple NER Pipeline failed: {e}")
        raise

__all__ = [
    'NERExtractor',
    'SimpleNERQueryEngine',
    'MarkdownChunker',
    'SimpleGraphBuilder',
    'run_simple_ner_pipeline'
] 