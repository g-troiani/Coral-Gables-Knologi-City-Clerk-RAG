#!/usr/bin/env python3
"""Test enhanced deduplication with different configurations."""

import sys
sys.path.append('.')

from pathlib import Path
from scripts.microsoft_framework.enhanced_entity_deduplicator import EnhancedEntityDeduplicator, DEDUP_CONFIGS

def test_configurations():
    """Test different deduplication configurations."""
    
    output_dir = Path("graphrag_data/output")
    if not output_dir.exists():
        print("❌ No GraphRAG output found!")
        return
    
    print("🔍 Testing Enhanced Deduplication Configurations")
    print("=" * 60)
    
    for config_name, config in DEDUP_CONFIGS.items():
        print(f"\n📊 Testing '{config_name}' configuration:")
        print(f"   Min score: {config.get('min_combined_score', 0.7)}")
        
        deduplicator = EnhancedEntityDeduplicator(output_dir, config)
        
        # Just analyze without merging
        import pandas as pd
        entities_df = pd.read_parquet(output_dir / "entities.parquet")
        relationships_df = pd.read_parquet(output_dir / "relationships.parquet")
        
        # Build graph
        deduplicator.graph = deduplicator._build_graph(entities_df, relationships_df)
        entities_df = deduplicator._calculate_graph_features(entities_df)
        
        # Find candidates
        candidates = deduplicator._find_merge_candidates(entities_df, relationships_df)
        
        print(f"   Found {len(candidates)} merge candidates")
        
        if candidates:
            print("   Top 3 candidates:")
            for candidate in candidates[:3]:
                print(f"     - '{candidate['entity1_title']}' ← '{candidate['entity2_title']}'")
                print(f"       Score: {candidate['combined_score']:.3f}, Reason: {candidate['merge_reason']}")
    
    # Ask to run with selected config
    print("\n" + "="*60)
    config_choice = input("Run deduplication with which config? (aggressive/conservative/name_focused/none): ")
    
    if config_choice in DEDUP_CONFIGS:
        print(f"\n🚀 Running with '{config_choice}' configuration...")
        config = DEDUP_CONFIGS[config_choice]
        deduplicator = EnhancedEntityDeduplicator(output_dir, config)
        stats = deduplicator.deduplicate_entities()
        print(f"✅ Merged {stats['merged_count']} entities!")

if __name__ == "__main__":
    test_configurations() 