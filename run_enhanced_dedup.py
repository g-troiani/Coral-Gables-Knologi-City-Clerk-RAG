#!/usr/bin/env python3
import sys
sys.path.append('scripts/microsoft_framework')
from enhanced_entity_deduplicator import EnhancedEntityDeduplicator
from pathlib import Path

if __name__ == '__main__':
    # Run with enhanced progress indicators - correct path
    output_dir = Path('graphrag_data/output')
    deduplicator = EnhancedEntityDeduplicator(output_dir)
    results = deduplicator.deduplicate_entities() 