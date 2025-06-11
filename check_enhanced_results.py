#!/usr/bin/env python3
import pandas as pd
import sys
from pathlib import Path

# Add project root to path
sys.path.append('.')

def check_results():
    print('📊 Enhanced Deduplication Results:')
    
    # Load original and deduplicated data
    orig_entities = pd.read_parquet('graphrag_data/output/entities.parquet')
    dedup_entities = pd.read_parquet('graphrag_data/output/deduplicated/entities.parquet')
    
    print(f'   Original entities: {len(orig_entities)}')
    print(f'   Deduplicated entities: {len(dedup_entities)}')
    print(f'   Entities merged: {len(orig_entities) - len(dedup_entities)}')
    print(f'   Reduction: {((len(orig_entities) - len(dedup_entities))/len(orig_entities)*100):.1f}%')
    
    # Check for aliases
    if 'aliases' in dedup_entities.columns:
        entities_with_aliases = dedup_entities[dedup_entities['aliases'].notna() & (dedup_entities['aliases'] != '')]
        print(f'   Entities with aliases: {len(entities_with_aliases)}')
        
        if len(entities_with_aliases) > 0:
            print('\n📝 Example merged entities:')
            for idx, entity in entities_with_aliases.head(5).iterrows():
                print(f"   - '{entity['title']}'")
                print(f"     Aliases: {entity['aliases']}")
    else:
        print('   No aliases column found')
    
    # Check for enhanced reports
    output_dir = Path('graphrag_data/output')
    json_report = output_dir / 'enhanced_deduplication_report.json'
    text_report = output_dir / 'enhanced_deduplication_report.txt'
    
    if json_report.exists():
        print(f'\n📋 Enhanced report available: {json_report}')
    if text_report.exists():
        print(f'📋 Text report available: {text_report}')
    
    if not json_report.exists() and not text_report.exists():
        print('\n⚠️  Enhanced deduplication reports not found')

if __name__ == '__main__':
    check_results() 