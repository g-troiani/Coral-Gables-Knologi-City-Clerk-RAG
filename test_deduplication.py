#!/usr/bin/env python3
"""
Test script for entity deduplication functionality.

This script tests the AdvancedEntityDeduplicator with real or synthetic data
to validate the deduplication algorithms and configurations.
"""

import sys
import os
from pathlib import Path
import pandas as pd
import tempfile
import shutil

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from scripts.microsoft_framework.entity_deduplicator import AdvancedEntityDeduplicator


def test_deduplication():
    """
    Test the entity deduplication functionality.
    
    This function creates a temporary test environment with sample data
    that mimics GraphRAG output structure and tests the deduplication process.
    """
    print("🧪 Testing Entity Deduplication Functionality")
    print("=" * 50)
    
    # Create temporary directory for test data
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        print(f"📁 Using temporary directory: {temp_path}")
        
        # Create sample entities data that would contain duplicates
        entities_data = {
            'title': [
                'John Smith',
                'john smith',  # Similar to John Smith
                'JOHN SMITH',  # Similar to John Smith
                'Jane Doe', 
                'City Council',
                'City council',  # Similar to City Council
                'Mike Johnson',
                'Michael Johnson',  # Similar to Mike Johnson
                'Budget Committee',
                'Water Project',
                'Waterfront Project'  # Similar to Water Project
            ],
            'description': [
                'Council member since 2020',
                'City council member',
                'Long-serving council member',
                'Mayor of the city',
                'Legislative body of the city',
                'City legislative body',
                'Public works director',
                'Director of public works department',
                'Handles city budget matters',
                'Infrastructure development project',
                'Major waterfront development initiative'
            ],
            'type': [
                'PERSON', 'PERSON', 'PERSON', 'PERSON',
                'ORGANIZATION', 'ORGANIZATION',
                'PERSON', 'PERSON',
                'ORGANIZATION',
                'PROJECT', 'PROJECT'
            ]
        }
        
        # Create sample relationships data
        relationships_data = {
            'source': [
                'John Smith', 'Jane Doe', 'Mike Johnson',
                'City Council', 'Budget Committee'
            ],
            'target': [
                'City Council', 'Budget Committee', 'Water Project',
                'Budget Committee', 'Water Project'
            ],
            'description': [
                'is member of', 'chairs', 'oversees',
                'oversees', 'manages'
            ]
        }
        
        # Create DataFrames
        entities_df = pd.DataFrame(entities_data)
        relationships_df = pd.DataFrame(relationships_data)
        
        # Save to parquet files
        entities_df.to_parquet(temp_path / "entities.parquet", index=False)
        relationships_df.to_parquet(temp_path / "relationships.parquet", index=False)
        
        print(f"📊 Created test data:")
        print(f"   Entities: {len(entities_df)}")
        print(f"   Relationships: {len(relationships_df)}")
        print(f"   Expected duplicates: 'John Smith' variants, 'City Council' variants, etc.")
        
        # Test different threshold levels
        test_configs = [
            {"threshold": 0.9, "tolerance": 0.1, "strategy": "keep_most_connected"},
            {"threshold": 0.95, "tolerance": 0.2, "strategy": "keep_first"},
            {"threshold": 0.8, "tolerance": 0.1, "strategy": "keep_most_connected"}
        ]
        
        for i, config in enumerate(test_configs, 1):
            print(f"\n🔬 Test {i}: Threshold={config['threshold']}, Tolerance={config['tolerance']}")
            print("-" * 30)
            
            try:
                # Create deduplicator instance
                deduplicator = AdvancedEntityDeduplicator(
                    temp_path,
                    similarity_threshold=config['threshold'],
                    clustering_tolerance=config['tolerance'],
                    merge_strategy=config['strategy']
                )
                
                # Run deduplication
                stats = deduplicator.deduplicate_entities()
                
                # Display results
                print(f"✅ Test {i} Results:")
                print(f"   Original entities: {stats['original_entities']}")
                print(f"   After deduplication: {stats['merged_entities']}")
                print(f"   Entities merged: {stats['merged_count']}")
                print(f"   Merge groups: {stats['merge_groups']}")
                
                # Validate results
                if stats['merged_count'] > 0:
                    print(f"   ✅ Successfully found and merged duplicates")
                else:
                    print(f"   ⚠️  No duplicates found (may be too strict)")
                
                # Clean up for next test
                dedup_dir = temp_path / "deduplicated"
                if dedup_dir.exists():
                    shutil.rmtree(dedup_dir)
                    
            except Exception as e:
                print(f"   ❌ Test {i} failed: {e}")
                import traceback
                traceback.print_exc()
        
        print(f"\n🏁 Deduplication testing completed!")
        
        # Test with real data if available
        real_output_dir = project_root / "graphrag_data" / "output"
        if real_output_dir.exists() and (real_output_dir / "entities.parquet").exists():
            print(f"\n📊 Testing with real GraphRAG data...")
            print("-" * 30)
            
            try:
                # Load real data to show statistics
                real_entities = pd.read_parquet(real_output_dir / "entities.parquet")
                print(f"   Real data entities: {len(real_entities)}")
                
                # Run deduplication on real data
                deduplicator = AdvancedEntityDeduplicator(
                    real_output_dir,
                    similarity_threshold=0.95,
                    clustering_tolerance=0.1
                )
                
                stats = deduplicator.deduplicate_entities()
                
                print(f"   Real data results:")
                print(f"     Original entities: {stats['original_entities']}")
                print(f"     After deduplication: {stats['merged_entities']}")
                print(f"     Entities merged: {stats['merged_count']}")
                print(f"     Savings: {stats['merged_count'] / stats['original_entities'] * 100:.1f}% reduction")
                
            except Exception as e:
                print(f"   ❌ Real data test failed: {e}")
        else:
            print(f"\n📝 No real GraphRAG data found at {real_output_dir}")
            print("   Run GraphRAG indexing first to test with real data")


if __name__ == "__main__":
    """Main execution block for standalone testing."""
    
    print("🚀 Entity Deduplication Test Suite")
    print("=" * 50)
    
    # Check dependencies
    try:
        import pandas as pd
        import networkx as nx
        print("✅ Dependencies check passed")
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Please install required packages: pip install pandas networkx")
        sys.exit(1)
    
    # Run tests
    try:
        test_deduplication()
        print("\n✅ All tests completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1) 