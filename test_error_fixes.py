#!/usr/bin/env python3
"""
Test script to verify fixes for "'str' object has no attribute 'get'" error
"""

import asyncio
import json
import os
import sys
from pathlib import Path
import logging

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Set up environment variables
os.environ.setdefault('MAX_TOKENS', '16384')

from scripts.graph_rag_stages.phase2_building.ner.enhanced_ner_extractor import EnhancedNERExtractor

class TestErrorFixesExtractor(EnhancedNERExtractor):
    """Test version that simulates problematic LLM responses."""
    
    def __init__(self):
        # Mock initialization
        self.output_dir = "test_output"
        self.seed_entities = []
        
        # Mock LLM client and other required attributes
        self.client = None
        self.model = "gpt-4"
        
        # Initialize with REAL entity definitions from parent class
        super().__init__(self.output_dir)
        
        # Track test scenarios
        self.test_scenario = 0
    
    async def _call_llm(self, prompt: str, task_name: str, chunk_metadata: dict = None) -> str:
        """Mock LLM call that returns problematic responses for testing."""
        import logging
        log = logging.getLogger(__name__)
        
        # Use parent's metadata logging
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
        
        log.info(f"\n🧪 TEST SCENARIO {self.test_scenario + 1}: {task_name}")
        log.info("=" * 80)
        
        # Return different problematic responses to test fixes
        test_responses = [
            # Scenario 1: LLM returns a JSON string literal (causes original error)
            '"Error: Unable to extract entities from this document"',
            
            # Scenario 2: LLM returns valid JSON object
            '{"Person": [{"personID": "person_test", "name": "Test Person"}], "Organization": []}',
            
            # Scenario 3: LLM returns malformed JSON
            '{"Person": [{"personID": "person_test", "name": "Test Person"}, "Organization": []}',
            
            # Scenario 4: LLM returns JSON with unexpected structure
            '{"entities": {"Person": [], "Organization": []}}',
            
            # Scenario 5: LLM returns empty string
            '',
            
            # Scenario 6: LLM returns JSON array instead of object
            '[{"type": "Person", "name": "Test Person"}]'
        ]
        
        response = test_responses[self.test_scenario % len(test_responses)]
        self.test_scenario += 1
        
        log.info(f"📤 Simulated LLM Response:")
        log.info(f"'{response}'")
        log.info("=" * 80)
        
        return response

async def test_error_fixes():
    """Test all the error fixes."""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
    log = logging.getLogger(__name__)
    
    print("🧪 Testing Error Fixes for 'str' object has no attribute 'get'")
    print("=" * 80)
    
    # Test scenarios that previously caused errors
    test_cases = [
        {
            'name': 'String literal response',
            'description': 'LLM returns a JSON string literal instead of object'
        },
        {
            'name': 'Valid JSON object',
            'description': 'LLM returns proper JSON object (should work normally)'
        },
        {
            'name': 'Malformed JSON',
            'description': 'LLM returns invalid JSON syntax'
        },
        {
            'name': 'Unexpected JSON structure',
            'description': 'LLM returns JSON with nested entities structure'
        },
        {
            'name': 'Empty response',
            'description': 'LLM returns empty string'
        },
        {
            'name': 'JSON array response',
            'description': 'LLM returns array instead of object'
        }
    ]
    
    # Sample chunk metadata
    chunk_metadata = {
        'chunk_id': 'test123abc',
        'chunk_file': 'test123abc_sample_document.txt',
        'document': 'sample_document',
        'document_type': 'VERBATIM_TRANSCRIPT',
        'meeting_date': '01.09.2024',
        'Source_File_Name': 'sample_document.pdf'
    }
    
    # Sample chunk text
    chunk_text = "Mayor Lago: Thank you. Commissioner Anderson, you have the floor for agenda item E-5."
    
    # Initialize test extractor
    extractor = TestErrorFixesExtractor()
    
    print(f"Running {len(test_cases)} test scenarios...")
    print()
    
    for i, test_case in enumerate(test_cases):
        print(f"🔬 Test {i+1}: {test_case['name']}")
        print(f"   Description: {test_case['description']}")
        
        try:
            # Test entity extraction with problematic response
            entities = await extractor._extract_entities_only(chunk_text, chunk_metadata)
            
            # Verify result is a dict
            if isinstance(entities, dict):
                entity_count = sum(len(v) if isinstance(v, list) else 0 for v in entities.values())
                print(f"   ✅ Result: Valid dict with {entity_count} total entities")
                
                # Verify all entity types are present
                expected_types = set(extractor.ENTITY_TYPES.keys())
                actual_types = set(entities.keys())
                if expected_types == actual_types:
                    print(f"   ✅ All {len(expected_types)} entity types present")
                else:
                    missing = expected_types - actual_types
                    print(f"   ⚠️  Missing entity types: {missing}")
                
                # Verify all values are lists
                non_list_types = [k for k, v in entities.items() if not isinstance(v, list)]
                if not non_list_types:
                    print(f"   ✅ All entity values are lists")
                else:
                    print(f"   ⚠️  Non-list entity values: {non_list_types}")
            else:
                print(f"   ❌ Result is not a dict: {type(entities)}")
            
        except Exception as e:
            print(f"   ❌ Exception raised: {e}")
            import traceback
            traceback.print_exc()
        
        print()
    
    print("🎯 Testing document type detection with invalid inputs...")
    
    # Test document type detection with invalid inputs
    invalid_inputs = [
        "string instead of dict",
        123,
        [],
        None,
        {"invalid": "structure"}
    ]
    
    for i, invalid_input in enumerate(invalid_inputs):
        try:
            doc_type = extractor._detect_document_type(invalid_input)
            print(f"   Test {i+1}: Input {type(invalid_input).__name__} -> '{doc_type}' ✅")
        except Exception as e:
            print(f"   Test {i+1}: Input {type(invalid_input).__name__} -> Exception: {e} ❌")
    
    print()
    print("🧹 Testing JSON response parsing edge cases...")
    
    # Test _parse_json_response with various problematic inputs
    test_responses = [
        '"Just a string"',  # JSON string literal
        '{"malformed": json}',  # Invalid JSON
        '["array", "instead", "of", "object"]',  # Array instead of object
        '',  # Empty string
        'Not JSON at all',  # Plain text
        '```json\n{"wrapped": "in", "code": "blocks"}\n```',  # Code blocks
        '{"nested": {"entities": {"Person": []}}}',  # Nested structure
    ]
    
    for i, response in enumerate(test_responses):
        try:
            result = extractor._parse_json_response(response)
            print(f"   Test {i+1}: {type(result).__name__} result for: '{response[:30]}...' ✅")
        except Exception as e:
            print(f"   Test {i+1}: Exception for: '{response[:30]}...' -> {e} ❌")
    
    print()
    print("✅ All error fix tests completed!")
    print()
    print("📋 **Summary of Fixes Applied:**")
    print("  🔧 Enhanced _parse_json_response to handle string literals")
    print("  🔧 Added input validation to _detect_document_type")
    print("  🔧 Added validation to _extract_entities_only")
    print("  🔧 Improved error handling in _enhance_attributes_only")
    print("  🔧 Fixed filename sanitization in markdown_chunker")
    print()
    print("🎯 **These fixes prevent:**")
    print("  ❌ 'str' object has no attribute 'get' errors")
    print("  ❌ Type errors from unexpected LLM responses")
    print("  ❌ File parsing issues from spaces in filenames")
    print("  ❌ Crashes when LLM returns malformed JSON")

if __name__ == "__main__":
    print("🚀 Starting Error Fixes Test")
    print(f"📁 Working Directory: {os.getcwd()}")
    print()
    
    asyncio.run(test_error_fixes())
    
    print("\n✨ Test completed!") 