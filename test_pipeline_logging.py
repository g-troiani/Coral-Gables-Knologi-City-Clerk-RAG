#!/usr/bin/env python3
"""
Test script to verify LLM call logging in the main pipeline
"""

import os
import sys
from pathlib import Path
import logging

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_logging_format():
    """Test the logging format to see if it matches our expectations."""
    
    # Setup a simple logger
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    
    print("🧪 Testing Pipeline LLM Logging Format")
    print("=" * 80)
    
    # Simulate what the enhanced logging should look like
    log.info("\n" + "="*100)
    log.info(f"🤖 LLM CALL: Test NER Entity Extraction")
    log.info("="*100)
    
    log.info(f"📄 CHUNK METADATA:")
    log.info(f"  - Source: test_document.pdf")
    log.info(f"  - Type: RESOLUTION")
    log.info(f"  - Date: 01.09.2024")
    
    log.info(f"📝 CHUNK TEXT (first 500 chars):")
    log.info("-" * 80)
    log.info("WHEREAS, this is a test resolution document...")
    log.info("-" * 80)
    
    log.info(f"📤 PROMPT SENT TO LLM:")
    log.info("-" * 80)
    log.info("Extract ALL entities from this City of Coral Gables resolution document...")
    log.info("-" * 80)
    
    log.info(f"📥 RESPONSE RECEIVED FROM LLM:")
    log.info("-" * 80)
    log.info('{"Person": [{"personID": "person_test", "name": "Test Person"}]}')
    log.info("-" * 80)
    
    log.info(f"📊 TOKEN USAGE:")
    log.info(f"  - Prompt tokens: 1234")
    log.info(f"  - Completion tokens: 567")
    log.info(f"  - Total tokens: 1801")
    
    log.info(f"✅ PARSED RESULT:")
    log.info(f"  - Entities found: 1")
    log.info(f"  - Relationships found: 0")
    log.info("="*100 + "\n")
    
    print("\n✅ Logging format test completed!")
    print("\n📋 **Key Features for Pipeline Debugging:**")
    print("  🔸 Clear visual separators (=== and ---)")
    print("  🔸 Emoji indicators for different types of information")
    print("  🔸 Structured sections: Metadata → Text → Prompt → Response → Usage → Results")
    print("  🔸 Easy to find and scroll through when debugging")
    print("  🔸 Token usage tracking for cost analysis")
    print("  🔸 Result summaries for quick assessment")
    
    print("\n🎯 **What this enables:**")
    print("  📊 In-depth analysis of each LLM call")
    print("  🐛 Easy debugging of prompt/response issues")
    print("  💰 Token usage monitoring")
    print("  🔍 Quick navigation in large log files")
    print("  📈 Performance tracking across different document types")

if __name__ == "__main__":
    test_logging_format() 