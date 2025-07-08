#!/usr/bin/env python3
"""
Debug script focused on Graph Query Agent and knowledge graph retrieval.
Traces where the 8598 nodes and 80 documents are getting lost.
"""

import asyncio
import json
import logging
from pathlib import Path
from scripts.graph_rag_stages.simple_ner.simple_query_engine import SimpleNERQueryEngine
from scripts.graph_rag_stages.common.temporal_utils import TemporalParser

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
log = logging.getLogger(__name__)

async def debug_graph_query():
    """Debug the graph query path step by step."""
    
    query = "What ordinances have been submitted by Commissioners since 2010?"
    
    print("=" * 80)
    print("🔍 DEBUGGING GRAPH QUERY AGENT & KNOWLEDGE GRAPH RETRIEVAL")
    print("=" * 80)
    print(f"Query: {query}")
    print()
    
    # Initialize query engine
    print("📊 STEP 1: Loading Simple NER Query Engine...")
    engine = SimpleNERQueryEngine(Path("simple_ner_graph"))
    print(f"✅ Knowledge graph: {len(engine.graph.nodes())} nodes, {len(engine.graph.edges())} edges")
    print()
    
    # Test query analysis
    print("🧠 STEP 2: Query analysis...")
    analysis = await engine._analyze_query(query)
    print(f"📋 Intent: {analysis.get('intent')}")
    print(f"📋 Entities: {analysis.get('entities')}")
    print(f"📋 Structural hints: {analysis.get('structural_hints')}")
    print()
    
    # Test graph temporal query
    print("🕰️ STEP 3: Testing graph temporal query...")
    structural_hints = analysis.get('structural_hints', {})
    date_range = structural_hints.get('date_range')
    
    if date_range:
        print(f"📅 Date range: {date_range}")
        graph_context = engine._query_knowledge_graph_temporal(date_range)
        
        print(f"📊 Graph query results:")
        print(f"   🔗 Temporal nodes: {len(graph_context.temporal_nodes)}")
        print(f"   📄 Document IDs: {len(graph_context.document_ids)}")
        print(f"   📅 Date range: {graph_context.date_range}")
        print(f"   🎯 Query type: {graph_context.query_type}")
        
        # Show sample document IDs
        if graph_context.document_ids:
            sample_docs = list(graph_context.document_ids)[:10]
            print(f"   📝 Sample document IDs: {sample_docs}")
        print()
        
        # Test chunk filtering with graph context
        print("🔍 STEP 4: Testing chunk filtering with graph context...")
        filtered_chunks = await engine._get_chunks_with_graph_filter(graph_context)
        print(f"📊 Filtered chunks: {len(filtered_chunks)}")
        
        if len(filtered_chunks) > 0:
            print(f"   First 5 chunk IDs: {[chunk_id for chunk_id, _ in filtered_chunks[:5]]}")
            
            # Check document types in filtered chunks
            doc_types = {}
            for chunk_id, chunk_data in filtered_chunks:
                doc_type = chunk_data.get('document_type', 'unknown')
                doc_types[doc_type] = doc_types.get(doc_type, 0) + 1
            print(f"   📈 Document types: {doc_types}")
        print()
        
        # Test ranking of filtered chunks
        print("📊 STEP 5: Testing chunk ranking...")
        ranked_chunks = engine._rank_chunks_by_relevance(filtered_chunks, analysis)
        print(f"📊 Ranked chunks: {len(ranked_chunks)}")
        
        if len(ranked_chunks) > 0:
            print(f"   Top 5 scores: {[(chunk['chunk_id'], chunk.get('relevance_score', 0)) for chunk in ranked_chunks[:5]]}")
            
            # Check ordinance chunks specifically
            ordinance_chunks = [c for c in ranked_chunks if 'ordinance' in c.get('document_type', '').lower()]
            print(f"   🏛️ Ordinance chunks: {len(ordinance_chunks)}")
        print()
        
        # Test temporal flow
        print("⏰ STEP 6: Testing temporal query flow...")
        try:
            temporal_result = await engine._temporal_query_flow(query, analysis, top_k=50)
            print(f"📊 Temporal flow result:")
            print(f"   Answer length: {len(temporal_result.get('answer', ''))}")
            print(f"   Sources: {len(temporal_result.get('sources', []))}")
            print(f"   Chunks retrieved: {temporal_result.get('chunks_retrieved', 0)}")
            print(f"   Retrieval method: {temporal_result.get('retrieval_method')}")
            
            if temporal_result.get('sources'):
                ordinance_sources = [s for s in temporal_result['sources'] if 'ordinance' in s.get('document_type', '').lower()]
                print(f"   🏛️ Ordinance sources: {len(ordinance_sources)}")
        except Exception as e:
            print(f"❌ Temporal flow failed: {e}")
        print()
        
        # Test Graph Query Agent directly
        print("🤖 STEP 7: Testing Graph Query Agent directly...")
        if engine.graph_query_agent:
            try:
                agent_result = await engine.graph_query_agent.generate_and_run(analysis)
                print(f"📊 Agent result:")
                print(f"   Query: {agent_result.get('query', 'N/A')}")
                print(f"   Results count: {len(agent_result.get('results', []))}")
                
                if 'error' in agent_result:
                    print(f"   ❌ Error: {agent_result['error']}")
                else:
                    results = agent_result.get('results', [])
                    if results:
                        print(f"   ✅ Sample result keys: {list(results[0].keys()) if results else 'None'}")
                        
                        # Check for ordinances in results
                        ordinance_results = []
                        for result in results:
                            if isinstance(result, dict):
                                # Check label or any field that might indicate ordinance
                                result_str = str(result).lower()
                                if 'ordinance' in result_str:
                                    ordinance_results.append(result)
                        print(f"   🏛️ Ordinance-related results: {len(ordinance_results)}")
            except Exception as e:
                print(f"❌ Graph Query Agent failed: {e}")
        else:
            print("❌ Graph Query Agent not available")
        print()
    
    else:
        print("❌ No date range found in analysis")
    
    # Summary
    print("=" * 80)
    print("📋 GRAPH QUERY DEBUGGING SUMMARY")
    print("=" * 80)
    
    if 'graph_context' in locals():
        print(f"🔍 Graph found {len(graph_context.temporal_nodes)} nodes, {len(graph_context.document_ids)} documents")
        
        if 'filtered_chunks' in locals():
            print(f"🔍 Graph context filtered to {len(filtered_chunks)} chunks")
            
            if 'ranked_chunks' in locals():
                print(f"🔍 Ranking produced {len(ranked_chunks)} chunks")
                
                if 'temporal_result' in locals():
                    final_sources = len(temporal_result.get('sources', []))
                    print(f"🔍 Final temporal flow: {final_sources} sources")
                    
                    if final_sources == 0 and len(ranked_chunks) > 0:
                        print("⚠️  ISSUE: Chunks lost between ranking and final temporal flow!")
                    elif final_sources > 0:
                        print("✅ Success: Temporal flow working correctly")

if __name__ == "__main__":
    asyncio.run(debug_graph_query()) 