#!/usr/bin/env python3
"""
Test script to validate the split entity extraction implementation.
"""

import json
import logging
from pathlib import Path
import sys

# Ensure project root is on sys.path for package imports
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.graph_rag_stages.phase2_NEW.simple_ner_split import (
    extract_entities_split,
    ENTITY_TYPE_GROUP_1,
    ENTITY_TYPE_GROUP_2,
    parse_chunk_file
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


def test_split_extraction():
    """Test the split entity extraction with a sample chunk."""
    
    # Find a sample chunk file
    chunks_dir = _PROJECT_ROOT / "simple_ner_graph/document_chunks"
    
    if not chunks_dir.exists():
        log.error(f"Chunks directory not found: {chunks_dir}")
        return
    
    chunk_files = list(chunks_dir.glob("*.txt"))
    if not chunk_files:
        log.error(f"No chunk files found in {chunks_dir}")
        return
    
    # Use the first chunk as a test
    test_chunk = chunk_files[0]
    log.info(f"Testing with chunk: {test_chunk.name}")
    
    # Parse the chunk
    meta, text = parse_chunk_file(str(test_chunk))
    log.info(f"Parsed metadata: {meta}")
    log.info(f"Text length: {len(text)} characters")
    
    # Run the split extraction
    log.info("\n" + "="*60)
    log.info("Running split entity extraction...")
    log.info("="*60)
    
    log.info(f"\nGroup 1 entity types ({len(ENTITY_TYPE_GROUP_1)}): {ENTITY_TYPE_GROUP_1}")
    log.info(f"Group 2 entity types ({len(ENTITY_TYPE_GROUP_2)}): {ENTITY_TYPE_GROUP_2}")
    
    try:
        result, raw_text, rel_template, attr_template, sys_prompt = extract_entities_split(
            text,
            document_type=meta.get('documentType', 'unknown'),
            meeting_date=meta.get('meetingDate', 'unknown'),
            source_file=meta.get('sourceFileName', 'unknown'),
        )
        
        log.info("\n" + "="*60)
        log.info("Extraction Results:")
        log.info("="*60)
        
        if isinstance(result, dict) and 'entities' in result:
            entities_dict = result['entities']
            
            # Count entities by group
            group1_count = 0
            group2_count = 0
            
            log.info("\nGroup 1 Results:")
            for entity_type in ENTITY_TYPE_GROUP_1:
                count = len(entities_dict.get(entity_type, []))
                group1_count += count
                log.info(f"  {entity_type}: {count} entities")
            
            log.info(f"\nGroup 1 Total: {group1_count} entities")
            
            log.info("\nGroup 2 Results:")
            for entity_type in ENTITY_TYPE_GROUP_2:
                count = len(entities_dict.get(entity_type, []))
                group2_count += count
                log.info(f"  {entity_type}: {count} entities")
            
            log.info(f"\nGroup 2 Total: {group2_count} entities")
            
            total_count = group1_count + group2_count
            log.info(f"\nTotal Entities Extracted: {total_count}")
            
            # Save results for inspection
            output_file = Path(__file__).parent / "test_split_extraction_output.json"
            output_data = {
                "chunk_file": test_chunk.name,
                "metadata": meta,
                "entity_counts": {
                    "group1": {et: len(entities_dict.get(et, [])) for et in ENTITY_TYPE_GROUP_1},
                    "group2": {et: len(entities_dict.get(et, [])) for et in ENTITY_TYPE_GROUP_2},
                    "total": total_count
                },
                "entities": entities_dict
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            log.info(f"\n✅ Test completed successfully!")
            log.info(f"Results saved to: {output_file}")
            
        else:
            log.error(f"Unexpected result format: {type(result)}")
            
    except Exception as e:
        log.error(f"Error during extraction: {e}")
        log.exception("Full traceback:")


if __name__ == "__main__":
    test_split_extraction()
