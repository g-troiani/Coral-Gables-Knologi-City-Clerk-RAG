#!/usr/bin/env python3
"""
Debug script to trace the entire query workflow step by step.
This will help us understand why we're not getting exhaustive ordinance results.
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

async def debug_query_workflow():
    """Debug the entire query workflow step by step."""
    
    query = "What ordinances have been submitted by Commissioners since 2010?"
    
    print("=" * 80)
    print("🐛 DEBUGGING QUERY WORKFLOW")
    print("=" * 80)
    print(f"Query: {query}")
    print()
    
    # Initialize query engine
    print("📊 STEP 1: Loading Simple NER Query Engine...")
    engine = SimpleNERQueryEngine(Path("simple_ner_graph"))
    print(f"✅ Entity index loaded: {len(engine.entity_index)} categories")
    print(f"✅ Chunk index loaded: {len(engine.chunk_index)} chunks")
    print()
    
    # Check ordinance entities in index
    print("🔍 STEP 2: Checking ordinance entities in index...")
    if 'official_records' in engine.entity_index:
        ordinances = [k for k in engine.entity_index['official_records'].keys() if 'ord' in k.lower() or 'ordinance' in k.lower()]
        print(f"📋 Found {len(ordinances)} ordinance-related entries in official_records")
        if len(ordinances) > 0:
            print(f"   First 10: {ordinances[:10]}")
    else:
        print("❌ No 'official_records' found in entity index")
    
    if 'document_types' in engine.entity_index:
        doc_types = list(engine.entity_index['document_types'].keys())
        print(f"📄 Document types in index: {doc_types}")
    else:
        print("❌ No 'document_types' found in entity index")
    print()
    
    # Check chunk index
    print("🔍 STEP 3: Checking chunk index for ordinances...")
    ordinance_chunks = [chunk_id for chunk_id, chunk_data in engine.chunk_index.items() 
                       if chunk_data.get('document_type', '').lower() == 'ordinance']
    resolution_chunks = [chunk_id for chunk_id, chunk_data in engine.chunk_index.items() 
                        if chunk_data.get('document_type', '').lower() == 'resolution']
    agenda_chunks = [chunk_id for chunk_id, chunk_data in engine.chunk_index.items() 
                    if chunk_data.get('document_type', '').lower() == 'agenda']
    
    print(f"📊 Chunk breakdown:")
    print(f"   🏛️ Ordinance chunks: {len(ordinance_chunks)}")
    print(f"   📋 Resolution chunks: {len(resolution_chunks)}")
    print(f"   📅 Agenda chunks: {len(agenda_chunks)}")
    print(f"   📄 Total chunks: {len(engine.chunk_index)}")
    print()
    
    # Check meeting dates in ordinance chunks
    print("📅 STEP 4: Checking meeting dates in ordinance chunks...")
    ordinances_with_dates = []
    ordinances_without_dates = []
    year_counts = {}
    
    for chunk_id in ordinance_chunks:
        chunk_data = engine.chunk_index[chunk_id]
        meeting_date = chunk_data.get('meeting_date', '')
        if meeting_date:
            ordinances_with_dates.append(chunk_id)
            # Extract year from meeting date for counting
            normalized = TemporalParser.normalize_date(meeting_date)
            if normalized:
                year = normalized[:4]
                year_counts[year] = year_counts.get(year, 0) + 1
        else:
            ordinances_without_dates.append(chunk_id)
    
    print(f"📊 Meeting date analysis:")
    print(f"   ✅ Ordinances WITH meeting dates: {len(ordinances_with_dates)}")
    print(f"   ❌ Ordinances WITHOUT meeting dates: {len(ordinances_without_dates)}")
    print(f"   📈 Ordinances by year: {dict(sorted(year_counts.items())[:20])}")  # Show first 20 years
    print()
    
    # Test query analysis
    print("🧠 STEP 5: Testing query analysis...")
    try:
        query_analysis = await engine._analyze_query(query)
        print(f"📋 Query analysis result:")
        print(f"   Intent: {query_analysis.get('intent')}")
        print(f"   Entities: {query_analysis.get('entities')}")
        print(f"   Structural hints: {query_analysis.get('structural_hints')}")
        print()
    except Exception as e:
        print(f"❌ Query analysis failed: {e}")
        return
    
    # Test temporal parsing
    print("⏰ STEP 6: Testing temporal parsing...")
    date_range = TemporalParser.extract_date_range(query)
    if date_range:
        print(f"📅 Parsed date range: {date_range}")
        print(f"   Start: {date_range[0]}")
        print(f"   End: {date_range[1]}")
        
        # Count ordinances in date range
        start_date, end_date = date_range
        matching_ordinances = []
        for chunk_id in ordinances_with_dates:
            chunk_data = engine.chunk_index[chunk_id]
            meeting_date = chunk_data.get('meeting_date', '')
            if meeting_date:
                normalized = TemporalParser.normalize_date(meeting_date)
                if normalized and start_date <= normalized <= end_date:
                    matching_ordinances.append(chunk_id)
        
        print(f"   🎯 Ordinances matching date range: {len(matching_ordinances)}")
        if matching_ordinances:
            # Show date range of matching ordinances
            dates = []
            for chunk_id in matching_ordinances:
                chunk_data = engine.chunk_index[chunk_id]
                meeting_date = chunk_data.get('meeting_date', '')
                if meeting_date:
                    normalized = TemporalParser.normalize_date(meeting_date)
                    if normalized:
                        dates.append(normalized)
            if dates:
                dates.sort()
                print(f"   📅 Date range: {dates[0]} to {dates[-1]}")
        print()
    else:
        print("❌ No date range found")
        print()
    
    # **NEW: Test graph temporal query in detail**
    print("🧠 STEP 6.5: Testing graph temporal query in detail...")
    if date_range:
        graph_context = engine._query_knowledge_graph_temporal(list(date_range))
        print(f"📊 Graph context results:")
        print(f"   Query type: {graph_context.query_type}")
        print(f"   Date range: {graph_context.date_range}")
        print(f"   Temporal nodes: {len(graph_context.temporal_nodes)}")
        print(f"   Document IDs: {len(graph_context.document_ids)}")
        
        # Show first 20 document IDs
        doc_id_list = list(graph_context.document_ids)[:20]
        print(f"   First 20 doc IDs: {doc_id_list}")
        
        # Count ordinance-related document IDs
        ordinance_doc_ids = [doc_id for doc_id in graph_context.document_ids if 'ordinance' in doc_id.lower()]
        print(f"   🏛️ Ordinance-related doc IDs: {len(ordinance_doc_ids)}")
        if ordinance_doc_ids:
            print(f"   First 10 ordinance doc IDs: {ordinance_doc_ids[:10]}")
        print()
    
    # **NEW: Test chunk filtering with graph context**
    print("🔍 STEP 6.6: Testing chunk filtering with graph context...")
    if date_range:
        filtered_chunks = await engine._get_chunks_with_graph_filter(graph_context)
        print(f"📊 Filtered chunks:")
        print(f"   Total filtered chunks: {len(filtered_chunks)}")
        
        # Count by document type
        chunk_types = {}
        for chunk_id, chunk_data in filtered_chunks:
            doc_type = chunk_data.get('document_type', 'unknown')
            chunk_types[doc_type] = chunk_types.get(doc_type, 0) + 1
        
        print(f"   Chunks by type: {chunk_types}")
        
        # Show sample ordinance chunks
        ordinance_filtered = [(chunk_id, chunk_data) for chunk_id, chunk_data in filtered_chunks 
                             if chunk_data.get('document_type', '').lower() == 'ordinance']
        print(f"   🏛️ Ordinance chunks after filtering: {len(ordinance_filtered)}")
        
        if ordinance_filtered:
            print(f"   Sample ordinance chunk IDs: {[chunk_id for chunk_id, _ in ordinance_filtered[:10]]}")
        print()
    
    # Test NER retrieval
    print("🔍 STEP 7: Testing NER retrieval...")
    try:
        entities = query_analysis.get('entities', {})
        ner_results = await engine._ner_retrieval(entities)
        print(f"📊 NER retrieval found {len(ner_results)} chunks")
        if len(ner_results) > 0:
            print(f"   Top 5 scores: {[(chunk[0], chunk[1]) for chunk in ner_results[:5]]}")
            
            # Check ordinance chunks specifically
            ordinance_ner = []
            for chunk_id, score in ner_results:
                chunk_data = engine.chunk_index.get(chunk_id, {})
                if chunk_data.get('document_type', '').lower() == 'ordinance':
                    ordinance_ner.append((chunk_id, score))
            print(f"   🏛️ Ordinance chunks in NER: {len(ordinance_ner)}")
        print()
    except Exception as e:
        print(f"❌ NER retrieval failed: {e}")
        print()
    
    # Test structural retrieval
    print("🏗️ STEP 8: Testing structural retrieval...")
    try:
        structural_results = await engine._structural_retrieval(query_analysis)
        print(f"📊 Structural retrieval found {len(structural_results)} chunks")
        if len(structural_results) > 0:
            print(f"   Top 5 scores: {[(chunk[0], chunk[1]) for chunk in structural_results[:5]]}")
            
            # Check ordinance chunks specifically
            ordinance_structural = []
            for chunk_id, score in structural_results:
                chunk_data = engine.chunk_index.get(chunk_id, {})
                if chunk_data.get('document_type', '').lower() == 'ordinance':
                    ordinance_structural.append((chunk_id, score))
            print(f"   🏛️ Ordinance chunks in structural: {len(ordinance_structural)}")
        print()
    except Exception as e:
        print(f"❌ Structural retrieval failed: {e}")
        print()
    
    # **NEW: Test fusion and ranking**
    print("🔄 STEP 8.5: Testing fusion and ranking...")
    try:
        if 'ner_results' in locals() and 'structural_results' in locals():
            fused_results = engine._fuse_and_rank(ner_results, structural_results, query_analysis)
            print(f"📊 Fused results: {len(fused_results)} chunks")
            
            # Count by document type
            fused_types = {}
            for chunk in fused_results:
                doc_type = chunk.get('document_type', 'unknown')
                fused_types[doc_type] = fused_types.get(doc_type, 0) + 1
            
            print(f"   Fused chunks by type: {fused_types}")
            
            # Show top 10 with scores
            print(f"   Top 10 fused chunks:")
            for i, chunk in enumerate(fused_results[:10]):
                chunk_id = chunk.get('chunk_id', 'unknown')
                doc_type = chunk.get('document_type', 'unknown')
                score = chunk.get('relevance_score', 0)
                print(f"      {i+1}. {chunk_id} ({doc_type}) - score: {score:.3f}")
        print()
    except Exception as e:
        print(f"❌ Fusion and ranking failed: {e}")
        print()
    
    # Test full query execution
    print("🚀 STEP 9: Testing full query execution...")
    try:
        result = await engine.query(query, top_k=50)
        print(f"📊 Query returned:")
        print(f"   Answer length: {len(result.get('answer', ''))}")
        print(f"   Sources: {len(result.get('sources', []))}")
        print(f"   Chunks retrieved: {result.get('chunks_retrieved', 0)}")
        print(f"   Retrieval method: {result.get('retrieval_method')}")
        
        if result.get('sources'):
            ordinance_sources = [s for s in result['sources'] if 'ordinance' in s.get('document_type', '').lower()]
            print(f"   🏛️ Ordinance sources: {len(ordinance_sources)}")
            
            # Show ordinance sources
            if ordinance_sources:
                print(f"   Ordinance source details:")
                for i, source in enumerate(ordinance_sources[:5]):
                    print(f"      {i+1}. {source.get('document', 'Unknown')} ({source.get('meeting_date', 'No date')})")
    
    except Exception as e:
        print(f"❌ Query execution failed: {e}")
    print()
    
    # Summary
    print("=" * 80)
    print("📋 DEBUGGING SUMMARY")
    print("=" * 80)
    if 'ordinance_chunks' in locals():
        print(f"🔍 Found {len(ordinance_chunks)} ordinance chunks in system")
    if 'ordinances_with_dates' in locals():
        print(f"📅 {len(ordinances_with_dates)} have meeting dates, {len(ordinances_without_dates)} don't")
    if 'matching_ordinances' in locals():
        print(f"⏰ {len(matching_ordinances)} ordinances match temporal filter 'since 2010'")
    if 'ordinance_filtered' in locals():
        print(f"🔧 {len(ordinance_filtered)} ordinances remain after graph filtering")
    
    print()
    print("🔧 RECOMMENDED FIXES:")
    if 'ordinances_without_dates' in locals() and len(ordinances_without_dates) > 0:
        print(f"1. Fix missing meeting dates for {len(ordinances_without_dates)} ordinance chunks")
    if 'ordinance_filtered' in locals() and len(ordinance_filtered) < len(matching_ordinances):
        print(f"2. Fix graph filtering that reduced ordinances from {len(matching_ordinances)} to {len(ordinance_filtered)}")
    if 'ordinance_sources' in locals() and len(ordinance_sources) < 10:
        print(f"3. Fix final ranking/selection that reduced ordinances to only {len(ordinance_sources)} in answer")

if __name__ == "__main__":
    asyncio.run(debug_query_workflow()) 