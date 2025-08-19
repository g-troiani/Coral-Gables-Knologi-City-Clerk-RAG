# Phase2 Building Consolidation Proposal

## Current State Analysis

### Active Components
1. **Taxonomy Pipeline**: `taxonomy_synthesizer.py` - Processes JSON extraction output
2. **NER Pipeline**: `phase2_new_adapter.py` + `phase2_new_extractor.py` - Processes document chunks
3. **Deduplication**: `entity_deduplicator_extended.py` - Merges entities from both pipelines
4. **Graph Building**: `custom_graph_builder.py` - Pushes to Cosmos DB
5. **Vector DB**: `vector_db_pusher.py` - Handles embeddings

### Dead/Redundant Code Identified
- `three_pass_extractor.py` - Replaced by phase2_new_extractor
- `ner_extractor.py` - Replaced by phase2_new_adapter
- `extraction_config.py` - Replaced by phase2_NEW configs
- `extractor_util.py` - Integrated into phase2_NEW
- `entity_deduplicator.py` - Replaced by extended version
- `enhanced_ner_extractor.py` & `simple_graph_builder.py` - Only used when skip_internal_graph_build=False

## Key Integration Challenges

1. **Duplicate Entity Creation**: Both pipelines create entities independently
2. **ID Standardization**: Need consistent ID generation across pipelines
3. **Attribute Merging**: Complex logic for merging attributes from different sources
4. **Relationship Handling**: Both pipelines create relationships that need deduplication

## Proposed Consolidation Strategy

### 1. Unified Entity Factory Pattern
Create a single entity creation interface used by both pipelines:

```python
class UnifiedEntityBuilder:
    """Single point of entity creation for all pipelines."""
    
    def create_entity(self, entity_type: str, attributes: dict, source: str) -> dict:
        # Standardized entity creation with:
        # - Consistent ID generation
        # - Required attribute validation
        # - Source tracking
        # - Ontology compliance
```

### 2. Streamlined Pipeline Architecture

```
Input Documents
    ├─> JSON Extraction ─> Taxonomy Pipeline ─┐
    │                                          ├─> Unified Entity Builder ─> Deduplication ─> Graph/Vector DB
    └─> Chunk Processing ─> NER Pipeline ──────┘
```

### 3. Consolidation Steps

#### Phase 1: Clean Dead Code
1. Run `cleanup_phase2_building.py` to remove confirmed dead files
2. Update imports in query engines to handle missing files gracefully

#### Phase 2: Standardize Entity Creation
1. Extract common entity creation logic from both pipelines
2. Create `unified_entity_builder.py` with standardized methods
3. Update both pipelines to use the unified builder

#### Phase 3: Simplify Deduplication
1. Move deduplication logic earlier in the pipeline
2. Create entity registry that prevents duplicate creation
3. Simplify `entity_deduplicator_extended.py` 

#### Phase 4: Merge Pipeline Outputs
1. Create `pipeline_merger.py` to handle taxonomy + NER integration
2. Standardize relationship creation across pipelines
3. Implement conflict resolution strategies

### 4. Code Organization

```
phase2_building/
├── __init__.py
├── builders/
│   ├── unified_entity_builder.py      # NEW: Centralized entity creation
│   ├── custom_graph_builder.py        # Keep: Cosmos DB integration
│   └── vector_db_pusher.py           # Keep: Vector embeddings
├── extractors/
│   ├── taxonomy_synthesizer.py        # Keep: JSON processing
│   └── phase2_new_adapter.py         # Keep: NER processing
├── integration/
│   ├── pipeline_merger.py            # NEW: Merge taxonomy + NER
│   ├── entity_deduplicator_extended.py # Simplify: Earlier dedup
│   └── graph_sanity.py               # Keep: Validation
└── ner/
    ├── phase2_new_extractor.py       # Keep: Main NER logic
    └── file_index_builder.py         # Keep: Index creation
```

### 5. Benefits

1. **Reduced Complexity**: Single entity creation path
2. **Better Deduplication**: Entities deduplicated at creation time
3. **Cleaner Integration**: Clear separation of concerns
4. **Easier Testing**: Modular components
5. **Performance**: Less redundant processing

### 6. Implementation Priority

1. **Immediate**: Remove dead code (low risk)
2. **Next Sprint**: Create unified entity builder
3. **Following Sprint**: Refactor pipelines to use builder
4. **Future**: Full pipeline merger optimization

## Next Steps

1. Review and approve this proposal
2. Run cleanup script to remove dead code
3. Create unified entity builder module
4. Gradually migrate pipelines to new architecture
5. Test thoroughly at each step

## Notes

- Taxonomy pipeline functionality must be preserved (it works well)
- Changes should be incremental to avoid breaking production
- Each step should be independently testable
- Maintain backward compatibility where possible
