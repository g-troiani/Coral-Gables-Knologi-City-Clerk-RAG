#!/usr/bin/env python3
"""
Test script to demonstrate enhanced LLM logging with chunk metadata
"""

import asyncio
import json
import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Set up environment variables (minimal setup)
os.environ.setdefault('MAX_TOKENS', '16384')

from scripts.graph_rag_stages.phase2_building.ner.enhanced_ner_extractor import EnhancedNERExtractor

class TestMetadataLoggingExtractor(EnhancedNERExtractor):
    """Test version that captures prompts and demonstrates metadata logging."""
    
    def __init__(self):
        # Mock initialization
        self.output_dir = "test_output"
        self.seed_entities = []
        
        # Mock LLM client and other required attributes
        self.client = None
        self.model = "gpt-4"
        
        # Initialize with REAL entity definitions from parent class
        super().__init__(self.output_dir)
    
    async def _call_llm(self, prompt: str, task_name: str, chunk_metadata: dict = None) -> str:
        """Mock LLM call that demonstrates the enhanced logging with metadata."""
        import logging
        log = logging.getLogger(__name__)
        
        # Demonstrate the chunk metadata logging
        if chunk_metadata:
            log.info("\n" + "🏷️  CHUNK METADATA:")
            
            # Extract chunk file name
            chunk_id = chunk_metadata.get('chunk_id', 'unknown')
            document = chunk_metadata.get('document', chunk_metadata.get('Source_File_Name', 'unknown'))
            chunk_file = chunk_metadata.get('chunk_file', f"{chunk_id}_{document}.txt")
            
            log.info(f"📄 Chunk File: {chunk_file}")
            log.info(f"🆔 Chunk ID: {chunk_id}")
            log.info(f"📋 Document: {document}")
            log.info(f"📝 Document Type: {chunk_metadata.get('document_type', 'unknown')}")
            log.info(f"📅 Meeting Date: {chunk_metadata.get('meeting_date', chunk_metadata.get('Meeting_Date', 'unknown'))}")
            log.info(f"📂 Source File: {chunk_metadata.get('Source_File_Name', 'unknown')}")
            if 'Index' in chunk_metadata or 'chunk_index' in chunk_metadata:
                index_info = chunk_metadata.get('Index', f"{chunk_metadata.get('chunk_index', 0) + 1}/{chunk_metadata.get('total_chunks', '?')}")
                log.info(f"🔢 Chunk Index: {index_info}")
        
        # Log the LLM call details for debugging
        log.info("\n" + "="*100)
        log.info(f"🤖 LLM CALL: {task_name}")
        log.info("="*100)
        
        log.info(f"📤 PROMPT SENT TO LLM:")
        log.info("-" * 80)
        log.info(prompt[:500] + "..." if len(prompt) > 500 else prompt)
        log.info("-" * 80)
        
        # Mock response
        mock_response = """{
  "Person": [
    {"personID": "person_lago_abc123", "name": "Vince Lago"},
    {"personID": "person_castro_def456", "name": "Castro"}
  ],
  "Organization": [
    {"orgID": "org_commission_ghi789", "name": "City Commission"}
  ],
  "Document": [
    {"documentID": "document_transcript_jkl012", "name": "Verbatim Transcript E-5"}
  ]
}"""
        
        log.info(f"📥 RESPONSE RECEIVED FROM LLM:")
        log.info("-" * 80)
        log.info(mock_response)
        log.info("-" * 80)
        
        log.info(f"📊 TOKEN USAGE:")
        log.info(f"  - Prompt tokens: 1456")
        log.info(f"  - Completion tokens: 234")
        log.info(f"  - Total tokens: 1690")
        
        log.info("="*100 + "\n")
        
        return mock_response

async def test_metadata_logging():
    """Test the enhanced metadata logging."""
    
    # Setup logging to show INFO level messages
    import logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
    
    print("🧪 Testing Enhanced LLM Logging with Chunk Metadata")
    print("=" * 80)
    
    # Create realistic chunk metadata (based on the attached file format)
    chunk_metadata = {
        'chunk_id': '1a62d7366c87',
        'chunk_file': '1a62d7366c87_01_09_2024 - Verbatim Transcripts - E-5.txt',
        'document': '01_09_2024 - Verbatim Transcripts - E-5',
        'document_type': 'VERBATIM_TRANSCRIPT',
        'meeting_date': '01.09.2024',
        'Meeting_Date': '01.09.2024',  # Alternative format
        'Source_File_Name': '01_09_2024 - Verbatim Transcripts - E-5.pdf',
        'Source_File_Path': '/Users/gianmariatroiani/Documents/knologi/graph_database/city_clerk_documents/global/City Comissions 2024/Verbatim Items/2024/01_09_2024 - Verbatim Transcripts - E-5.pdf',
        'Index': '4/4'
    }
    
    # Sample chunk text (verbatim transcript style)
    chunk_text = """[Start: 6:45 p.m.]

City Attorney Suárez: Thank you, Mr. Mayor. This is a verbatim transcript for agenda item E-5. 

Mayor Lago: Commissioner Anderson, you have the floor.

Commissioner Anderson: Thank you, Mr. Mayor. I'd like to discuss the proposed amendments to the zoning ordinance. The Planning and Zoning Board has recommended approval of the text amendments to Article 3, Section 3-104 of the Zoning Code.

City Clerk Urquia: For the record, this pertains to Ordinance 2024-05.

Commissioner Fernandez: I'd like to ask the City Attorney about the legal implications of these changes.

City Attorney Suárez: Commissioner, the proposed amendments are consistent with state law and our Comprehensive Plan..."""
    
    # Initialize the test extractor
    extractor = TestMetadataLoggingExtractor()
    
    print(f"📄 SAMPLE CHUNK METADATA:")
    print(json.dumps(chunk_metadata, indent=2))
    print()
    
    print(f"📝 SAMPLE CHUNK TEXT:")
    print("-" * 60)
    print(chunk_text)
    print("-" * 60)
    print()
    
    print("🚀 Testing enhanced logging output:")
    print("=" * 80)
    
    # Test entity extraction with metadata logging
    try:
        entities = await extractor._extract_entities_only(chunk_text, chunk_metadata)
        print("\n✅ Entity extraction test completed with enhanced metadata logging!")
        
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🚀 Starting Enhanced Metadata Logging Test")
    print(f"📁 Working Directory: {os.getcwd()}")
    print()
    
    asyncio.run(test_metadata_logging())
    
    print("\n✨ Test completed!")
    print("\n📋 **Enhanced Features:**")
    print("  🏷️  Chunk metadata displayed before each LLM call")
    print("  📄 Chunk file name for exact file identification")
    print("  🆔 Chunk ID for precise identification")
    print("  📋 Document name for context")
    print("  📝 Document type for processing clarity")
    print("  📅 Meeting date for temporal context")
    print("  📂 Source file for traceability")
    print("  🔢 Chunk index (X/Y) for progress tracking")
    print("  🤖 LLM call details follow metadata")
    print("\n🎯 **Perfect for debugging:**")
    print("  - Instantly identify which chunk file is being processed")
    print("  - Track progress through multi-chunk documents")
    print("  - Debug document type detection issues")
    print("  - Correlate extraction results with source documents")
    print("  - Locate exact chunk text files for manual inspection") 