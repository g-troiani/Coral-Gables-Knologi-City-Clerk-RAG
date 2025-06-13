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

# Detect virtual environment
def get_venv_python():
    """Get the correct Python executable from virtual environment."""
    # Check if we're in a venv
    if sys.prefix != sys.base_prefix:
        return sys.executable
    
    # Try common venv locations
    venv_paths = [
        'venv/bin/python3',
        'venv/bin/python',
        '.venv/bin/python3',
        '.venv/bin/python',
        'city_clerk_rag/bin/python3',
        'city_clerk_rag/bin/python'
    ]
    
    for venv_path in venv_paths:
        full_path = os.path.join(os.getcwd(), venv_path)
        if os.path.exists(full_path):
            return full_path
    
    # Fallback
    return sys.executable

# Use this in all subprocess calls
PYTHON_EXE = get_venv_python()
print(f"🐍 Using Python: {PYTHON_EXE}")

from scripts.microsoft_framework import (
    CityClerkGraphRAGPipeline,
    CityClerkQueryEngine,
    SmartQueryRouter,
    GraphRAGCosmosSync,
    handle_user_query,
    GraphRAGInitializer,
    CityClerkDocumentAdapter,
    CityClerkPromptTuner,
    GraphRAGOutputProcessor
)
from scripts.microsoft_framework.enhanced_entity_deduplicator import EnhancedEntityDeduplicator, DEDUP_CONFIGS

# ============================================================================
# PIPELINE CONTROL FLAGS - Set these to control which modules run
# ============================================================================

# Core Pipeline Steps
RUN_INITIALIZATION = True      # Initialize GraphRAG environment and settings
RUN_DOCUMENT_PREP = True       # Convert extracted JSONs to GraphRAG CSV format
RUN_PROMPT_TUNING = True       # Auto-tune prompts for city clerk domain
RUN_GRAPHRAG_INDEX = True      # Run the actual GraphRAG indexing process

# Post-Processing Steps  
DISPLAY_RESULTS = True         # Show summary of extracted entities/relationships
TEST_QUERIES = True            # Run example queries to test the system
SYNC_TO_COSMOS = False         # Sync GraphRAG results to Cosmos DB

# Advanced Options
FORCE_REINDEX = False          # Force re-indexing even if output exists
VERBOSE_MODE = True            # Show detailed progress information
SKIP_CONFIRMATION = False      # Skip confirmation prompts

# Enhanced Deduplication Control
RUN_DEDUPLICATION = True       
DEDUP_CONFIG = 'conservative'   # CHANGED from 'name_focused' to 'conservative'
DEDUP_CUSTOM_CONFIG = {
    'min_combined_score': 0.8,  # INCREASED from 0.7
    'enable_partial_name_matching': True,
    'enable_abbreviation_matching': True,
    'weights': {
        'string_similarity': 0.3,
        'token_overlap': 0.2,
        'graph_structure': 0.4,  # INCREASED weight on graph structure
        'semantic_similarity': 0.1
    }
}

# ============================================================================

async def main():
    """Main pipeline execution with modular control."""
    
    # Check environment variables
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ Error: OPENAI_API_KEY environment variable not set")
        print("Please set it with: export OPENAI_API_KEY='your-api-key'")
        return
    
    print("🚀 City Clerk GraphRAG Pipeline")
    print("=" * 50)
    print("📋 Module Configuration:")
    print(f"   Initialize Environment: {'✅' if RUN_INITIALIZATION else '⏭️'}")
    print(f"   Prepare Documents:      {'✅' if RUN_DOCUMENT_PREP else '⏭️'}")
    print(f"   Tune Prompts:          {'✅' if RUN_PROMPT_TUNING else '⏭️'}")
    print(f"   Run GraphRAG Index:    {'✅' if RUN_GRAPHRAG_INDEX else '⏭️'}")
    print(f"   Display Results:       {'✅' if DISPLAY_RESULTS else '⏭️'}")
    print(f"   Test Queries:          {'✅' if TEST_QUERIES else '⏭️'}")
    print(f"   Sync to Cosmos:        {'✅' if SYNC_TO_COSMOS else '⏭️'}")
    print("=" * 50)
    
    if not SKIP_CONFIRMATION:
        confirm = input("\nProceed with this configuration? (y/N): ")
        if confirm.lower() not in ['y', 'yes']:
            print("❌ Pipeline cancelled")
            return
    
    try:
        graphrag_root = project_root / "graphrag_data"
        
        # Step 1: Initialize GraphRAG Environment
        if RUN_INITIALIZATION:
            print("\n📋 Step 1: Initializing GraphRAG Environment")
            print("-" * 30)
            
            initializer = GraphRAGInitializer(project_root)
            initializer.setup_environment()
            print("✅ GraphRAG environment initialized")
        else:
            print("\n⏭️  Skipping GraphRAG initialization")
            if not graphrag_root.exists():
                print("❌ GraphRAG root doesn't exist! Enable RUN_INITIALIZATION")
                return
        
        # Step 2: Prepare Documents
        if RUN_DOCUMENT_PREP:
            print("\n📋 Step 2: Preparing Documents for GraphRAG")
            print("-" * 30)
            
            adapter = CityClerkDocumentAdapter(
                project_root / "city_clerk_documents/extracted_text"
            )
            
            # Use JSON files directly for better structure preservation
            df = adapter.prepare_documents_for_graphrag(graphrag_root)
            print(f"✅ Prepared {len(df)} isolated documents for GraphRAG")
            print("   Each agenda item is now a completely separate entity")
        else:
            print("\n⏭️  Skipping document preparation")
            csv_path = graphrag_root / "city_clerk_documents.csv"
            if not csv_path.exists():
                print("❌ No prepared documents found! Enable RUN_DOCUMENT_PREP")
                return
        
        # Step 3: Prompt Tuning
        if RUN_PROMPT_TUNING:
            print("\n📋 Step 3: Tuning Prompts for City Clerk Domain")
            print("-" * 30)
            
            tuner = CityClerkPromptTuner(graphrag_root)
            prompts_dir = graphrag_root / "prompts"

            # If we are forcing a re-index or skipping confirmation, we should always regenerate prompts
            # to ensure the latest versions from the scripts are used.
            if FORCE_REINDEX or SKIP_CONFIRMATION:
                print("📝 Forcing prompt regeneration to apply new rules...")
                if prompts_dir.exists():
                    import shutil
                    shutil.rmtree(prompts_dir)
                tuner.create_manual_prompts()
                print("✅ Prompts regenerated successfully.")
            
            # Original interactive logic for manual runs
            else:
                if prompts_dir.exists() and list(prompts_dir.glob("*.txt")):
                    print("📁 Existing prompts found")
                    reuse = input("Use existing prompts? (Y/n): ")
                    if reuse.lower() != 'n':
                        print("🔄 Re-creating manual prompts...")
                        tuner.create_manual_prompts()
                        print("✅ Prompts created manually")
                    else:
                        print("✅ Using existing prompts")
                else:
                    print("📝 Creating prompts manually...")
                    tuner.create_manual_prompts()
                    print("✅ Prompts created")
        else:
            print("\n⏭️  Skipping prompt tuning")
        
        # Step 4: Run GraphRAG Indexing
        if RUN_GRAPHRAG_INDEX:
            print("\n📋 Step 4: Running GraphRAG Indexing")
            print("-" * 30)
            
            # Check if output already exists
            output_dir = graphrag_root / "output"
            if output_dir.exists() and list(output_dir.glob("*.parquet")):
                print("📁 Existing GraphRAG output found")
                if not FORCE_REINDEX:
                    reindex = input("Re-run indexing? This may take time (y/N): ")
                    if reindex.lower() != 'y':
                        print("✅ Using existing index")
                    else:
                        print("🏗️ Re-indexing documents...")
                        await run_graphrag_indexing(graphrag_root, VERBOSE_MODE)
                else:
                    print("🏗️ Force re-indexing documents...")
                    await run_graphrag_indexing(graphrag_root, VERBOSE_MODE)
            else:
                print("🏗️ Running GraphRAG indexing (this may take several minutes)...")
                await run_graphrag_indexing(graphrag_root, VERBOSE_MODE)
        else:
            print("\n⏭️  Skipping GraphRAG indexing")
        
        # Step 4.5: Enhanced Entity Deduplication
        if RUN_DEDUPLICATION and RUN_GRAPHRAG_INDEX:
            print("\n📋 Step 4.5: Enhanced Entity Deduplication")
            print("-" * 30)
            
            output_dir = graphrag_root / "output"
            if output_dir.exists() and list(output_dir.glob("*.parquet")):
                print(f"🔍 Running enhanced deduplication (config: {DEDUP_CONFIG})")
                
                # Get configuration
                config = DEDUP_CONFIGS.get(DEDUP_CONFIG, {})
                if DEDUP_CUSTOM_CONFIG:
                    config.update(DEDUP_CUSTOM_CONFIG)
                
                print("📊 Deduplication configuration:")
                print(f"   - Partial name matching: {config.get('enable_partial_name_matching', True)}")
                print(f"   - Token matching: {config.get('enable_token_matching', True)}")
                print(f"   - Semantic matching: {config.get('enable_semantic_matching', True)}")
                print(f"   - Min combined score: {config.get('min_combined_score', 0.7)}")
                
                deduplicator = EnhancedEntityDeduplicator(output_dir, config)
                
                try:
                    stats = deduplicator.deduplicate_entities()
                    
                    print(f"\n✅ Enhanced deduplication complete:")
                    print(f"   Original entities: {stats['original_entities']}")
                    print(f"   After deduplication: {stats['merged_entities']}")
                    print(f"   Entities merged: {stats['merged_count']}")
                    
                    if stats['merged_count'] > 0:
                        print(f"\n📁 Deduplicated data saved to: {output_dir}/deduplicated/")
                        print(f"📝 Detailed report: {output_dir}/enhanced_deduplication_report.txt")
                        
                        # Show some examples
                        report_path = output_dir / "enhanced_deduplication_report.txt"
                        if report_path.exists():
                            with open(report_path, 'r') as f:
                                lines = f.readlines()
                                # Find and show first few merges
                                for i, line in enumerate(lines):
                                    if "←" in line and i < len(lines) - 1:
                                        print(f"\n   Example: {line.strip()}")
                                        break
                        
                        # Ask user if they want to use deduplicated data
                        if not SKIP_CONFIRMATION:
                            use_dedup = input("\nUse deduplicated data for queries? (Y/n): ")
                            if use_dedup.lower() != 'n':
                                # Update the output directory for subsequent steps
                                output_dir = output_dir / "deduplicated"
                except Exception as e:
                    print(f"❌ Enhanced deduplication failed: {e}")
                    if VERBOSE_MODE:
                        import traceback
                        traceback.print_exc()
            else:
                print("⏭️  No GraphRAG output to deduplicate")
        else:
            print("\n⏭️  Skipping entity deduplication")
        
        # Step 5: Display Results Summary
        if DISPLAY_RESULTS:
            print("\n📊 Step 5: Results Summary")
            await display_results_summary(project_root)
        else:
            print("\n⏭️  Skipping results display")
        
        # Step 6: Test Queries
        if TEST_QUERIES:
            print("\n🔍 Step 6: Testing Query System")
            await test_queries(project_root)
        else:
            print("\n⏭️  Skipping query testing")
        
        # Step 7: Sync to Cosmos DB
        if SYNC_TO_COSMOS:
            print("\n🌐 Step 7: Syncing to Cosmos DB")
            await sync_to_cosmos(project_root, skip_prompt=SKIP_CONFIRMATION)
        else:
            print("\n⏭️  Skipping Cosmos DB sync")
        
        print("\n✅ Pipeline completed successfully!")
        print("\n📚 Next Steps:")
        print("   - Run queries: python scripts/microsoft_framework/test_queries.py")
        print("   - View results: Check graphrag_data/output/")
        print("   - Sync to Cosmos: Set SYNC_TO_COSMOS = True and re-run")
        
    except Exception as e:
        print(f"\n❌ Error running pipeline: {e}")
        if VERBOSE_MODE:
            import traceback
            traceback.print_exc()

async def run_graphrag_indexing(graphrag_root: Path, verbose: bool = True):
    """Run GraphRAG indexing with optimized concurrency settings."""
    import subprocess
    
    # Set environment variables for better performance
    env = os.environ.copy()
    
    # Increase concurrency based on system capabilities
    cpu_count = os.cpu_count() or 4
    optimal_concurrency = min(cpu_count * 2, 20)  # Increased from 5
    env['GRAPHRAG_CONCURRENCY'] = str(optimal_concurrency)
    
    # Add additional performance settings
    env['GRAPHRAG_CHUNK_PARALLELISM'] = str(optimal_concurrency)
    env['GRAPHRAG_ENTITY_EXTRACTION_PARALLELISM'] = str(optimal_concurrency)
    
    print(f"🚀 Running GraphRAG with concurrency level: {optimal_concurrency}")
    
    cmd = [
        PYTHON_EXE,  # Use the detected venv Python instead of sys.executable
        "-m", "graphrag", "index",
        "--root", str(graphrag_root),
    ]
    
    if verbose:
        cmd.append("--verbose")
    
    # Run with optimized environment
    process = subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT, 
        text=True,
        env=env
    )
    
    # Stream output
    for line in iter(process.stdout.readline, ''):
        if line:
            print(f"   {line.strip()}")
    
    process.wait()
    
    if process.returncode == 0:
        print("✅ GraphRAG indexing completed successfully")
    else:
        raise Exception(f"GraphRAG indexing failed with code {process.returncode}")

async def display_results_summary(project_root: Path):
    """Display summary of GraphRAG results."""
    print("-" * 30)
    
    from scripts.microsoft_framework import GraphRAGOutputProcessor
    
    output_dir = project_root / "graphrag_data/output"
    
    # Check if deduplicated data exists
    dedup_dir = output_dir / "deduplicated"
    if dedup_dir.exists() and list(dedup_dir.glob("*.parquet")):
        print("📊 Using deduplicated data")
        output_dir = dedup_dir
    
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

async def sync_to_cosmos(project_root: Path, skip_prompt: bool = False):
    """Optionally sync results to Cosmos DB."""
    print("-" * 30)
    
    if not skip_prompt:
        user_input = input("Do you want to sync GraphRAG results to Cosmos DB? (y/N): ")
        if user_input.lower() not in ['y', 'yes']:
            print("⏭️ Skipping Cosmos DB sync")
            return
    
    try:
        output_dir = project_root / "graphrag_data/output"
        sync = GraphRAGCosmosSync(output_dir)
        await sync.sync_to_cosmos()
        print("✅ Successfully synced to Cosmos DB")
    except Exception as e:
        print(f"❌ Cosmos DB sync failed: {e}")

def show_usage():
    """Show usage instructions."""
    print("""
🚀 City Clerk GraphRAG Pipeline Runner

CONTROL FLAGS:
   Edit the boolean flags at the top of this file to control which modules run:
   
   RUN_INITIALIZATION - Initialize GraphRAG environment
   RUN_DOCUMENT_PREP - Convert documents to CSV format
   RUN_PROMPT_TUNING - Auto-tune prompts
   RUN_GRAPHRAG_INDEX - Run indexing process
   RUN_DEDUPLICATION - Apply enhanced entity deduplication
   DEDUP_CONFIG - Deduplication preset: 'aggressive', 'conservative', 'name_focused'
   DISPLAY_RESULTS - Show summary statistics
   TEST_QUERIES - Test example queries
   SYNC_TO_COSMOS - Sync to Cosmos DB
   
USAGE:
   python3 scripts/microsoft_framework/run_graphrag_pipeline.py [options]
   
OPTIONS:
   -h, --help     Show this help message
   --force        Force re-indexing (sets FORCE_REINDEX=True)
   --quiet        Minimal output (sets VERBOSE_MODE=False)
   --yes          Skip confirmations (sets SKIP_CONFIRMATION=True)
   --cosmos       Enable Cosmos sync (sets SYNC_TO_COSMOS=True)
   --dedup-config TYPE  Set deduplication config (aggressive/conservative/name_focused)
   --no-dedup     Disable entity deduplication

EXAMPLES:
   # Run with default settings
   python3 scripts/microsoft_framework/run_graphrag_pipeline.py
   
   # Force complete re-index
   python3 scripts/microsoft_framework/run_graphrag_pipeline.py --force --yes
   
   # Just test queries (edit flags to disable other steps)
   python3 scripts/microsoft_framework/run_graphrag_pipeline.py
    """)

if __name__ == "__main__":
    # Parse command line arguments
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            if arg in ['-h', '--help', 'help']:
                show_usage()
                sys.exit(0)
            elif arg == '--force':
                FORCE_REINDEX = True
            elif arg == '--quiet':
                VERBOSE_MODE = False
            elif arg == '--yes':
                SKIP_CONFIRMATION = True
            elif arg == '--cosmos':
                SYNC_TO_COSMOS = True
            elif arg.startswith('--dedup-config='):
                DEDUP_CONFIG = arg.split('=')[1]
            elif arg == '--no-dedup':
                RUN_DEDUPLICATION = False
    
    # Run the pipeline
    asyncio.run(main()) 