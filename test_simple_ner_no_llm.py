#!/usr/bin/env python3
"""
Test SimpleNERQueryEngine without LLMs - just retrieval
"""

import asyncio
import sys
from pathlib import Path
import logging

# Add the correct path for the simple_ner module
sys.path.append('scripts/graph_rag_stages')

# Import from the simple_ner module
from simple_ner.simple_query_engine import SimpleNERQueryEngine

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

async def test_retrieval_only():
    """Test just the retrieval components without LLM calls."""
    log.info("Initializing SimpleNERQueryEngine...")
    
    # Initialize with the correct graph directory
    engine = SimpleNERQueryEngine("simple_ner_graph")
    
    query_text = "What is agenda item E-1?"
    log.info(f"Testing retrieval for: '{query_text}'")
    
    print("\n" + "="*60)
    print("SIMPLE NER RETRIEVAL TEST (NO LLM)")
    print("="*60)
    
    # Step 1: Use simple query analysis (no LLM)
    print("\n1. QUERY ANALYSIS (Pattern-based, no LLM):")
    query_analysis = engine._simple_query_analysis(query_text)
    print(f"   Entities found: {dict(query_analysis['entities'])}")
    print(f"   Intent: {query_analysis['intent']}")
    
    # Step 2: NER-based retrieval
    print("\n2. NER RETRIEVAL:")
    ner_chunks = await engine._ner_retrieval(query_analysis['entities'])
    print(f"   Found {len(ner_chunks)} chunks via entity matching")
    
    if ner_chunks:
        print("   Top NER matches:")
        for i, (chunk_id, score) in enumerate(ner_chunks[:5]):
            print(f"     {i+1}. {chunk_id}: {score:.3f}")
    
    # Step 3: Structural retrieval
    print("\n3. STRUCTURAL RETRIEVAL:")
    structural_chunks = await engine._structural_retrieval(query_analysis)
    print(f"   Found {len(structural_chunks)} chunks via structural filtering")
    
    if structural_chunks:
        print("   Top structural matches:")
        for i, (chunk_id, score) in enumerate(structural_chunks[:5]):
            print(f"     {i+1}. {chunk_id}: {score:.3f}")
    
    # Step 4: Fusion and ranking
    print("\n4. FUSED RESULTS:")
    ranked_chunks = engine._fuse_and_rank(
        ner_chunks, 
        structural_chunks, 
        query_analysis
    )[:10]
    
    print(f"   Final ranked results ({len(ranked_chunks)} chunks):")
    for i, chunk in enumerate(ranked_chunks):
        chunk_id = chunk['chunk_id']
        score = chunk['relevance_score']
        doc_name = chunk.get('document', 'Unknown')
        print(f"     {i+1}. {chunk_id} | {doc_name} | Score: {score:.3f}")
    
    # Step 5: Show sample chunk content
    if ranked_chunks:
        print(f"\n5. SAMPLE CONTENT FROM TOP RESULT:")
        top_chunk = ranked_chunks[0]
        chunk_text = top_chunk.get('text', 'No text available')[:300]
        print(f"   Document: {top_chunk.get('document', 'Unknown')}")
        print(f"   Chunk ID: {top_chunk['chunk_id']}")
        print(f"   Preview: {chunk_text}...")
        
        if 'entities' in top_chunk:
            print(f"   Entities in chunk: {top_chunk['entities']}")
    
    print("\n" + "="*60)
    print("✅ RETRIEVAL TEST COMPLETE - NO LLM REQUIRED!")
    print("="*60)
    
    return ranked_chunks

if __name__ == "__main__":
    asyncio.run(test_retrieval_only()) 