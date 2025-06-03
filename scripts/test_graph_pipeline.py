"""
Test script for City Clerk Graph Pipeline
"""
import asyncio
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

async def test_pipeline():
    """Test basic pipeline functionality."""
    # Check environment variables
    required_vars = ['COSMOS_ENDPOINT', 'COSMOS_KEY', 'GROQ_API_KEY']
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        print(f"❌ Missing environment variables: {missing}")
        print("Please set these in your .env file")
        return
    
    # Test imports
    try:
        from graph_stages.cosmos_db_client import CosmosGraphClient
        from graph_stages.agenda_pdf_extractor import AgendaPDFExtractor
        from graph_stages.agenda_ontology_extractor import CityClerkOntologyExtractor
        from graph_stages.document_linker import DocumentLinker
        from graph_stages.agenda_graph_builder import AgendaGraphBuilder
        print("✅ All imports successful")
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return
    
    # Test Cosmos connection
    try:
        cosmos = CosmosGraphClient()
        await cosmos.connect()
        print("✅ Cosmos DB connection successful")
        await cosmos.close()
    except Exception as e:
        print(f"❌ Cosmos DB connection failed: {e}")
        return
    
    # Test PDF extraction
    test_dir = Path("city_clerk_documents/global")
    if test_dir.exists():
        pdfs = list(test_dir.glob("Agenda*.pdf"))
        if pdfs:
            print(f"✅ Found {len(pdfs)} agenda PDFs")
            
            # Test extraction on first PDF
            extractor = AgendaPDFExtractor()
            result = extractor.extract(pdfs[0])
            print(f"✅ Extracted {len(result.get('sections', []))} sections from {pdfs[0].name}")
        else:
            print("⚠️  No agenda PDFs found in test directory")
    else:
        print(f"⚠️  Test directory not found: {test_dir}")
    
    print("\n✅ Pipeline components are functional!")

if __name__ == "__main__":
    asyncio.run(test_pipeline()) 