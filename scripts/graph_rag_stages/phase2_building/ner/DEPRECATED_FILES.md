# Deprecated NER Files

The following files have been replaced by the phase2_NEW integration:

## Files Already Removed (2025-01-19)

### 1. **three_pass_extractor.py** ✓ REMOVED
- **Replaced by**: phase2_new_extractor.py
- **Reason**: The three-pass extraction logic is replaced by the simpler phase2_NEW approach

### 2. **ner_extractor.py** ✓ REMOVED
- **Replaced by**: phase2_new_extractor.py (with direct core calls)
- **Reason**: Base extraction logic is replaced by consolidated core functions

### 3. **extraction_config.py** ✓ REMOVED
- **Replaced by**: phase2_NEW/ontology_context_camelCase.txt
- **Reason**: Configuration is now handled by phase2_NEW's prompt files

### 4. **extractor_util.py** ✓ REMOVED
- **Replaced by**: phase2_NEW logic
- **Reason**: Utility functions are integrated into phase2_NEW

### 5. **entity_deduplicator.py** ✓ REMOVED
- **Replaced by**: entity_deduplicator_extended.py
- **Reason**: Basic deduplicator replaced by extended version with multi-source support

## Files Modified for Compatibility

### 1. **enhanced_ner_extractor.py** ⚠️ CONVERTED TO STUB
- **Status**: Converted to compatibility wrapper
- **Reason**: Still referenced by query engines, now redirects to phase2_new_extractor.py
- **Note**: Shows deprecation warning when used

## Files to Keep

### 1. **phase2_new_extractor.py** ✅ UPDATED
- Drop-in replacement for ThreePassExtractor
- **Status**: Now uses direct core function calls (adapter layer removed)

### 2. **file_index_builder.py**
- Still needed for building indices after extraction

### 3. **simple_graph_builder.py**
- May still be used by query engines (needs verification)

## Files Recently Consolidated (2025-01-20)

### 1. **phase2_new_adapter.py** ✓ REMOVED
- **Status**: Logic moved directly into phase2_new_extractor.py
- **Reason**: Adapter layer eliminated for cleaner architecture

### 2. **phase2_new_adapter_triples.py** ✓ REMOVED
- **Status**: Logic moved directly into phase2_new_extractor.py  
- **Reason**: Adapter layer eliminated for cleaner architecture

## Migration Notes

The main pipeline now uses:
1. UnifiedQueryEngine for chunking (with skip_internal_graph_build=True)
2. Phase2NEWExtractor for entity extraction
3. EntityDeduplicatorExtended for deduplication
4. NERFileIndexBuilder for index building

The phase2_NEW approach simplifies the extraction process while maintaining compatibility with the rest of the pipeline.
