#!/usr/bin/env python3
"""
Test script to verify the integrated extraction pipeline and graph building.

This script demonstrates the new unified flow:
PDFs → 3-Stage Extraction → JSON → NetworkX Graph
"""

import logging
import asyncio
from pathlib import Path
import sys

# Add scripts directory to path
sys.path.append(str(Path(__file__).parent / "scripts"))

try:
    from graph_rag_stages.phase1_preprocessing.extraction_integration import ExtractionPipelineIntegration
    from graph_rag_stages.phase2_building.local_graph_builder import LocalGraphBuilder
except ImportError as e:
    print(f"❌ Failed to import modules: {e}")
    print("Make sure you're running from the project root directory")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

async def test_integration():
    """Test the complete integration pipeline."""
    
    log.info("🧪 Testing Integrated Extraction Pipeline")
    
    # Set up test directories
    project_root = Path(__file__).parent
    test_pdf_dir = project_root / "city_clerk_documents" / "global" / "City Comissions 2024"
    json_output_dir = project_root / "city_clerk_documents/extracted_json"
    graph_output_dir = project_root / "test_local_graph_data"
    
    # Clean up previous test runs
    if graph_output_dir.exists():
        import shutil
        shutil.rmtree(graph_output_dir)
    
    try:
        # Step 1: Test extraction pipeline
        log.info("🔬 Step 1: Testing 3-stage extraction pipeline")
        integration = ExtractionPipelineIntegration(json_output_dir)
        
        if not test_pdf_dir.exists():
            log.warning(f"⚠️  Test PDF directory not found: {test_pdf_dir}")
            log.info("Creating mock test directory structure...")
            test_pdf_dir.mkdir(parents=True, exist_ok=True)
            (test_pdf_dir / "Agendas").mkdir(exist_ok=True)
            log.info("ℹ️  Please add PDF files to test with the extraction pipeline")
            return
        
        extracted_documents = await integration.run_extraction_pipeline(test_pdf_dir)
        log.info(f"✅ Extraction completed: {len(extracted_documents)} documents processed")
        
        # Step 2: Test graph building
        log.info("🔬 Step 2: Testing JSON-based graph building")
        builder = LocalGraphBuilder(graph_output_dir)
        await builder.build_graph_from_json(json_output_dir)
        
        # Step 3: Verify results
        log.info("🔬 Step 3: Verifying results")
        stats = builder.get_graph_stats()
        
        log.info("✅ Integration test completed successfully!")
        log.info("📊 Results:")
        log.info(f"  - Extracted JSON files: {json_output_dir}")
        log.info(f"  - Graph files: {graph_output_dir}")
        log.info(f"  - Graph stats: {stats}")
        
        # Check if graph files exist
        graphml_file = graph_output_dir / "city_clerk_graph.graphml"
        json_file = graph_output_dir / "city_clerk_graph.json"
        
        if graphml_file.exists():
            log.info(f"  ✅ GraphML file created: {graphml_file}")
        if json_file.exists():
            log.info(f"  ✅ JSON graph file created: {json_file}")
        
        return True
        
    except Exception as e:
        log.error(f"❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_dependencies():
    """Check if required dependencies are available."""
    log.info("🔍 Checking dependencies...")
    
    dependencies = {
        'groq': 'Groq API client (for LLM processing)',
        'networkx': 'NetworkX (for graph building)',
        'pathlib': 'Path handling (built-in)',
    }
    
    missing = []
    for dep, desc in dependencies.items():
        try:
            __import__(dep)
            log.info(f"  ✅ {dep}: {desc}")
        except ImportError:
            log.warning(f"  ⚠️  {dep}: {desc} - Not available")
            missing.append(dep)
    
    # Check for optional dependencies
    optional_deps = {
        'docling': 'Docling (for advanced PDF OCR)',
        'fitz': 'PyMuPDF (for PDF processing)',
    }
    
    for dep, desc in optional_deps.items():
        try:
            __import__(dep)
            log.info(f"  ✅ {dep}: {desc}")
        except ImportError:
            log.info(f"  ℹ️  {dep}: {desc} - Optional, will use fallbacks")
    
    if missing:
        log.error(f"❌ Missing required dependencies: {missing}")
        return False
    
    log.info("✅ All required dependencies available")
    return True

async def main():
    """Main test function."""
    log.info("🚀 Starting Integration Test")
    
    # Check dependencies first
    if not check_dependencies():
        log.error("❌ Dependency check failed")
        return
    
    # Run integration test
    success = await test_integration()
    
    if success:
        log.info("🎉 All tests passed!")
        log.info("💡 You can now run the full pipeline:")
        log.info("   python -m scripts.graph_rag_stages.main_pipeline")
    else:
        log.error("❌ Integration test failed")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 