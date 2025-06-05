#!/usr/bin/env python3
"""
Runner script for the City Clerk GraphRAG Pipeline.

This script demonstrates how to run the complete GraphRAG pipeline and view results.
"""

import asyncio
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from scripts.graphrag_breakdown import (
    CityClerkGraphRAGPipeline,
    CityClerkQueryEngine,
    SmartQueryRouter,
    GraphRAGCosmosSync,
    handle_user_query
)

async def main():
    """Main pipeline execution."""
    
    # Check environment variables
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ Error: OPENAI_API_KEY environment variable not set")
        print("Please set it with: export OPENAI_API_KEY='your-api-key'")
        return
    
    print("🚀 Starting City Clerk GraphRAG Pipeline")
    print("=" * 50)
    
    try:
        # Step 1: Initialize and run the pipeline
        print("\n📋 Step 1: Running GraphRAG Pipeline")
        pipeline = CityClerkGraphRAGPipeline(project_root)
        
        # Check if documents exist
        docs_dir = project_root / "city_clerk_documents/extracted_text"
        if not docs_dir.exists() or not list(docs_dir.glob("*_extracted.json")):
            print(f"❌ No extracted documents found in {docs_dir}")
            print("Please run the document extraction pipeline first.")
            return
        
        print(f"📄 Found documents in {docs_dir}")
        
        # Run the full pipeline
        print("🏗️ Running GraphRAG indexing (this may take several minutes)...")
        graph_data = await pipeline.run_full_pipeline()
        
        print("✅ Pipeline completed successfully!")
        
        # Step 2: Display results summary
        await display_results_summary(project_root)
        
        # Step 3: Test queries
        await test_queries(project_root)
        
        # Step 4: Sync to Cosmos DB (optional)
        await sync_to_cosmos(project_root)
        
    except Exception as e:
        print(f"❌ Error running pipeline: {e}")
        import traceback
        traceback.print_exc()

async def display_results_summary(project_root: Path):
    """Display summary of GraphRAG results."""
    print("\n📊 Step 2: Results Summary")
    print("-" * 30)
    
    from scripts.graphrag_breakdown import GraphRAGOutputProcessor
    
    output_dir = project_root / "graphrag_data/output"
    processor = GraphRAGOutputProcessor(output_dir)
    
    # Get summaries
    entity_summary = processor.get_entity_summary()
    relationship_summary = processor.get_relationship_summary()
    
    if entity_summary:
        print(f"🏷️ Entities extracted: {entity_summary.get('total_entities', 0)}")
        print("📋 Entity types:")
        for entity_type, count in entity_summary.get('entity_types', {}).items():
            print(f"   - {entity_type}: {count}")
    
    if relationship_summary:
        print(f"\n🔗 Relationships extracted: {relationship_summary.get('total_relationships', 0)}")
        print("📋 Relationship types:")
        for rel_type, count in relationship_summary.get('relationship_types', {}).items():
            print(f"   - {rel_type}: {count}")
    
    # Show file locations
    print(f"\n📁 Output files location: {output_dir}")
    output_files = [
        "entities.parquet",
        "relationships.parquet", 
        "communities.parquet",
        "community_reports.parquet"
    ]
    
    for filename in output_files:
        file_path = output_dir / filename
        if file_path.exists():
            size = file_path.stat().st_size / 1024  # KB
            print(f"   ✅ {filename} ({size:.1f} KB)")
        else:
            print(f"   ❌ {filename} (not found)")

async def test_queries(project_root: Path):
    """Test the query system with example queries."""
    print("\n🔍 Step 3: Testing Query System")
    print("-" * 30)
    
    # Example queries for city clerk documents
    test_queries = [
        "Who is Commissioner Smith?",  # Should use Local search
        "What are the main themes in city development?",  # Should use Global search
        "How has the waterfront project evolved?",  # Should use DRIFT search
        "Tell me about ordinance 2024-01",  # Should use Local search
        "What are the overall budget trends?",  # Should use Global search
    ]
    
    query_engine = CityClerkQueryEngine(project_root / "graphrag_data")
    router = SmartQueryRouter()
    
    for query in test_queries:
        print(f"\n❓ Query: '{query}'")
        
        # Show routing decision
        route_info = router.determine_query_method(query)
        print(f"🎯 Router selected: {route_info['method']} ({route_info['intent'].value})")
        
        try:
            # Execute query
            result = await query_engine.query(query)
            print(f"📝 Answer preview: {result['answer'][:200]}...")
            print(f"🔧 Parameters used: {result['parameters']}")
        except Exception as e:
            print(f"❌ Query failed: {e}")

async def sync_to_cosmos(project_root: Path):
    """Optionally sync results to Cosmos DB."""
    print("\n🌐 Step 4: Cosmos DB Sync (Optional)")
    print("-" * 30)
    
    user_input = input("Do you want to sync GraphRAG results to Cosmos DB? (y/N): ")
    
    if user_input.lower() in ['y', 'yes']:
        try:
            output_dir = project_root / "graphrag_data/output"
            sync = GraphRAGCosmosSync(output_dir)
            await sync.sync_to_cosmos()
            print("✅ Successfully synced to Cosmos DB")
        except Exception as e:
            print(f"❌ Cosmos DB sync failed: {e}")
    else:
        print("⏭️ Skipping Cosmos DB sync")

def show_usage():
    """Show usage instructions."""
    print("""
🚀 City Clerk GraphRAG Pipeline Runner

PREREQUISITES:
1. Set environment variables:
   export OPENAI_API_KEY='your-api-key-here'
   
2. Install dependencies:
   pip install -r requirements.txt
   
3. Ensure you have extracted documents:
   The pipeline expects JSON files in city_clerk_documents/extracted_text/
   
USAGE:
   python3 scripts/graphrag_breakdown/run_graphrag_pipeline.py

WHAT THIS SCRIPT DOES:
1. ✅ Initialize GraphRAG environment
2. 📄 Convert city clerk documents to GraphRAG format  
3. 🎯 Auto-tune prompts for city government domain
4. 🏗️ Run GraphRAG indexing (entity/relationship extraction)
5. 📊 Display results summary
6. 🔍 Test query system with example queries
7. 🌐 Optionally sync to Cosmos DB

OUTPUT LOCATIONS:
- GraphRAG data: ./graphrag_data/
- Entities: ./graphrag_data/output/entities.parquet
- Relationships: ./graphrag_data/output/relationships.parquet
- Communities: ./graphrag_data/output/communities.parquet
    """)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help', 'help']:
        show_usage()
    else:
        asyncio.run(main()) 