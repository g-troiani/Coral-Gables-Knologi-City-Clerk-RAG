#!/usr/bin/env python3
"""
Test script to extract entities from chunk 4a92b97b0f53 using resolution prompt
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

class TestNERExtractor(EnhancedNERExtractor):
    """Test version that captures prompts and responses."""
    
    def __init__(self):
        # Mock initialization - we don't need real output dir or LLM client for this test
        self.output_dir = "test_output"
        self.seed_entities = []
        
        # Mock LLM client and other required attributes
        self.client = None
        self.model = "gpt-4"
        
        # Initialize with REAL entity definitions from parent class
        super().__init__(self.output_dir)
        
        # Store captured data
        self.captured_prompt = None
        self.captured_chunk = None
        self.captured_response = None
    
    async def _call_llm(self, prompt: str, task_name: str) -> str:
        """Mock LLM call that captures the prompt and returns a mock response."""
        # Capture the prompt for inspection
        self.captured_prompt = prompt
        
        # Mock response using ALL entity types from the comprehensive ontology
        mock_response = """{
  "Person": [
    {
      "personID": "person_lago_v1a2b3",
      "name": "Vince Lago"
    },
    {
      "personID": "person_castro_a1b2c3", 
      "name": "Castro"
    },
    {
      "personID": "person_fernandez_b2c3d4",
      "name": "Fernandez"
    },
    {
      "personID": "person_menendez_c3d4e5",
      "name": "Menendez"
    },
    {
      "personID": "person_anderson_d4e5f6",
      "name": "Anderson"
    },
    {
      "personID": "person_urquia_e5f6g7",
      "name": "Billy Y. Urquia"
    },
    {
      "personID": "person_suarez_f6g7h8",
      "name": "Cristina M. Suárez"
    }
  ],
  "Organization": [
    {
      "orgID": "org_commission_g7h8i9",
      "name": "City Commission"
    },
    {
      "orgID": "org_legislature_h8i9j0",
      "name": "Florida Legislature"
    },
    {
      "orgID": "org_coralgables_i9j0k1",
      "name": "City of Coral Gables"
    }
  ],
  "Document": [
    {
      "documentID": "document_resolution2024_j0k1l2",
      "name": "Resolution No. 2024-04"
    }
  ],
  "Policy": [
    {
      "policyID": "policy_statute166_k1l2m3",
      "name": "Florida Statute 166.0451"
    },
    {
      "policyID": "policy_livelocal_l2m3n4",
      "name": "Live Local Act"
    },
    {
      "policyID": "policy_chapter2023_m3n4o5",
      "name": "Chapter Law 2023-017"
    }
  ],
  "Action": [
    {
      "actionID": "action_passed_n4o5p6",
      "name": "Passed"
    },
    {
      "actionID": "action_moved_o5p6q7",
      "name": "Moved"
    },
    {
      "actionID": "action_seconded_p6q7r8",
      "name": "Seconded"
    }
  ],
  "Location": [
    {
      "locationID": "location_coralgables_q7r8s9",
      "name": "Coral Gables"
    },
    {
      "locationID": "location_florida_r8s9t0",
      "name": "Florida"
    }
  ],
  "Event": [],
  "Asset": [
    {
      "assetID": "asset_property_s9t0u1",
      "name": "city-owned real property"
    }
  ],
  "Project": [],
  "Role": [
    {
      "roleID": "role_mayor_t0u1v2",
      "name": "Mayor"
    },
    {
      "roleID": "role_clerk_u1v2w3",
      "name": "City Clerk"
    },
    {
      "roleID": "role_attorney_v2w3x4",
      "name": "City Attorney"
    }
  ],
  "Topic": [
    {
      "topicID": "topic_housing_w3x4y5",
      "name": "affordable housing"
    }
  ],
  "AgendaItem": [
    {
      "agendaItemID": "agendaitem_e9_x4y5z6",
      "name": "E-9"
    }
  ],
  "Contract": [],
  "Technology": [],
  "VoteOutcome": [
    {
      "voteOutcomeID": "vote_unanimous_y5z6a7",
      "name": "Unanimous: 5-0 Vote"
    }
  ]
}"""
        
        # Capture the response
        self.captured_response = mock_response
        
        print(f"\n{'='*80}")
        print(f"TASK: {task_name}")
        print(f"{'='*80}")
        
        return mock_response

async def test_resolution_extraction():
    """Test entity extraction from resolution chunk 4a92b97b0f53."""
    
    print("🧪 Testing Resolution Entity Extraction")
    print("📄 Chunk ID: 4a92b97b0f53")
    print("📋 Document Type: Resolution")
    print("-" * 80)
    
    # Read the chunk content
    chunk_file = "simple_ner_graph/document_chunks/4a92b97b0f53_2024-04 - 01_09_2024_enhanced_resolution.txt"
    
    try:
        with open(chunk_file, 'r', encoding='utf-8') as f:
            chunk_content = f.read()
    except FileNotFoundError:
        print(f"❌ Could not find chunk file: {chunk_file}")
        return
    
    # Extract just the text content (skip metadata lines)
    lines = chunk_content.split('\n')
    content_start = None
    for i, line in enumerate(lines):
        if line.strip() == '---' and i > 0:  # Second --- marks start of content
            content_start = i + 1
            break
    
    if content_start:
        chunk_text = '\n'.join(lines[content_start:]).strip()
    else:
        chunk_text = chunk_content
    
    # Create metadata to trigger resolution detection
    chunk_metadata = {
        'document_type': 'RESOLUTION',
        'Source_File_Name': '2024-04 - 01_09_2024.pdf',
        'meeting_date': '01.09.2024',
        'document_number': '2024-04',
        'agenda_item': 'E-9'
    }
    
    # Initialize the test extractor
    extractor = TestNERExtractor()
    
    # Capture the chunk for display
    extractor.captured_chunk = chunk_text[:3000]  # Match what gets sent to LLM
    
    print(f"📊 CHUNK METADATA:")
    print(json.dumps(chunk_metadata, indent=2))
    print()
    
    print(f"📝 CHUNK TEXT (first 3000 chars sent to LLM):")
    print("-" * 80)
    print(extractor.captured_chunk)
    print("-" * 80)
    print()
    
    # Test entity extraction
    try:
        entities = await extractor._extract_entities_only(chunk_text, chunk_metadata)
        
        print(f"🤖 LLM PROMPT SENT:")
        print("-" * 80)
        print(extractor.captured_prompt)
        print("-" * 80)
        print()
        
        print(f"📤 LLM RESPONSE RECEIVED:")
        print("-" * 80)
        print(extractor.captured_response)
        print("-" * 80)
        print()
        
        print(f"✅ PARSED ENTITIES:")
        print("-" * 80)
        for entity_type, entity_list in entities.items():
            print(f"{entity_type}: {len(entity_list)} entities")
            for entity in entity_list[:3]:  # Show first 3 entities of each type
                print(f"  - {entity}")
        print("-" * 80)
        
    except Exception as e:
        print(f"❌ Error during extraction: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🚀 Starting Resolution Entity Extraction Test")
    print(f"📁 Working Directory: {os.getcwd()}")
    print()
    
    asyncio.run(test_resolution_extraction())
    
    print("\n✨ Test completed! File not deleted as requested.") 