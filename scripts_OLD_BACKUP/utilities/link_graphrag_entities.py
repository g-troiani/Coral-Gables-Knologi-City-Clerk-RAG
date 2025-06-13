#!/usr/bin/env python3
"""
Link GraphRAG Entities to Origin Data
=====================================

This script creates a linkage mapping between GraphRAG entities and their origin 
chunk/section IDs, implementing WP-6 from the integration strategy.

Usage:
    python3 scripts/utilities/link_graphrag_entities.py --graphrag-output ./graphrag_data/output
"""

import pandas as pd
import pyarrow.parquet as pq
import json
import argparse
from pathlib import Path
import logging
import re

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def extract_origin_ids_from_description(description: str) -> dict:
    """Extract origin IDs from entity description text."""
    origin_data = {}
    
    # Look for origin_chunk_id pattern
    chunk_match = re.search(r'"origin_chunk_id":\s*"([^"]+)"', description)
    if chunk_match:
        origin_data['origin_chunk_id'] = chunk_match.group(1)
    
    # Look for other origin patterns that might be in the description
    doc_match = re.search(r'"origin_doc_id":\s*"([^"]+)"', description)
    if doc_match:
        origin_data['origin_doc_id'] = doc_match.group(1)
        
    section_match = re.search(r'"origin_section_id":\s*"([^"]+)"', description)
    if section_match:
        origin_data['origin_section_id'] = section_match.group(1)
    
    return origin_data


def create_linkage_mapping(graphrag_output_dir: Path, output_path: Path = None) -> pd.DataFrame:
    """Create linkage mapping between GraphRAG entities and origin data."""
    
    graphrag_output_dir = Path(graphrag_output_dir)
    
    # Find the entities parquet file
    entities_file = graphrag_output_dir / "artifacts" / "create_final_entities.parquet"
    if not entities_file.exists():
        # Try alternative location
        entities_file = graphrag_output_dir / "create_final_entities.parquet"
        if not entities_file.exists():
            raise FileNotFoundError(f"Could not find entities parquet file in {graphrag_output_dir}")
    
    log.info(f"📊 Loading GraphRAG entities from {entities_file}")
    
    # Load entities
    entities_df = pd.read_parquet(entities_file)
    log.info(f"Found {len(entities_df)} entities")
    
    # Extract origin information from entities
    linkage_records = []
    
    for _, entity in entities_df.iterrows():
        entity_id = entity.get('id', '')
        entity_name = entity.get('name', '')
        entity_type = entity.get('type', '')
        entity_description = entity.get('description', '')
        
        # Extract origin IDs from description
        origin_data = extract_origin_ids_from_description(entity_description)
        
        # Also check if origin data is directly in the entity attributes
        for col in ['origin_chunk_id', 'origin_section_id', 'origin_doc_id']:
            if col in entity and pd.notna(entity[col]):
                origin_data[col] = entity[col]
        
        # Create linkage record if we have origin data
        if origin_data:
            record = {
                'graphrag_entity_id': entity_id,
                'entity_name': entity_name,
                'entity_type': entity_type,
                **origin_data
            }
            linkage_records.append(record)
    
    if not linkage_records:
        log.warning("⚠️  No origin data found in entities. Make sure the extraction pipeline is adding origin_* fields.")
        return pd.DataFrame()
    
    # Create linkage DataFrame
    linkage_df = pd.DataFrame(linkage_records)
    
    # Fill missing columns
    for col in ['origin_chunk_id', 'origin_section_id', 'origin_doc_id']:
        if col not in linkage_df.columns:
            linkage_df[col] = None
    
    log.info(f"📊 Created linkage mapping with {len(linkage_df)} entries")
    log.info(f"   - Entities with chunk IDs: {linkage_df['origin_chunk_id'].notna().sum()}")
    log.info(f"   - Entities with section IDs: {linkage_df['origin_section_id'].notna().sum()}")
    log.info(f"   - Entities with doc IDs: {linkage_df['origin_doc_id'].notna().sum()}")
    
    # Save linkage mapping
    if output_path is None:
        output_path = graphrag_output_dir / "linkage.parquet"
    
    linkage_df.to_parquet(output_path, index=False)
    log.info(f"💾 Saved linkage mapping to {output_path}")
    
    # Also save as JSON for easier inspection
    json_path = output_path.with_suffix('.json')
    linkage_df.to_json(json_path, orient='records', indent=2)
    log.info(f"💾 Saved linkage mapping to {json_path}")
    
    return linkage_df


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create linkage mapping between GraphRAG entities and origin data"
    )
    parser.add_argument(
        "--graphrag-output",
        type=str,
        default="./graphrag_data/output",
        help="Path to GraphRAG output directory"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output path for linkage file (default: <graphrag-output>/linkage.parquet)"
    )
    
    args = parser.parse_args()
    
    graphrag_output_dir = Path(args.graphrag_output)
    output_path = Path(args.output) if args.output else None
    
    try:
        linkage_df = create_linkage_mapping(graphrag_output_dir, output_path)
        
        if len(linkage_df) > 0:
            log.info("✅ Linkage mapping created successfully")
            
            # Show sample linkages
            log.info("\n📋 Sample linkages:")
            for _, row in linkage_df.head(3).iterrows():
                log.info(f"   Entity: {row['entity_name']} -> {row.get('origin_chunk_id', 'N/A')}")
        else:
            log.warning("⚠️  No linkages created. Check that entities have origin data.")
            
    except Exception as e:
        log.error(f"❌ Failed to create linkage mapping: {e}")
        raise


if __name__ == "__main__":
    main() 