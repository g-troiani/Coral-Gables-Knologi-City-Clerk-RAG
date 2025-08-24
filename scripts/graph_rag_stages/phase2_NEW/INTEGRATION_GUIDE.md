# Integration Guide: Triple Extraction in Main Pipeline

## Current State

The main pipeline (`main_pipeline.py`) currently uses:
- `Phase2NEWExtractor` → `Phase2NEWAdapter` → `simple_ner_split.py` (three-phase extraction)

The triple extraction is implemented but **not yet integrated** into the main pipeline.

## How to Integrate Triple Extraction

### Option 1: Environment Variable (Minimal Changes)

1. Set environment variable before running the pipeline:
```bash
export USE_TRIPLE_EXTRACTION=true
```

2. Modify `phase2_new_adapter.py` to check this variable and conditionally import:
```python
if os.getenv("USE_TRIPLE_EXTRACTION", "false").lower() == "true":
    from scripts.graph_rag_stages.phase2_NEW.simple_ner_triples import (
        extract_triples, convert_triples_to_entities_relationships, ...
    )
else:
    from scripts.graph_rag_stages.phase2_NEW.simple_ner_split import (
        extract_entities_split as extract_entities, ...
    )
```

### Option 2: Use the New Adapter (Recommended)

1. Replace the import in `phase2_new_extractor.py`:
```python
# Change this:
from scripts.graph_rag_stages.phase2_building.ner.phase2_new_adapter import Phase2NEWAdapter

# To this:
from scripts.graph_rag_stages.phase2_building.ner.phase2_new_adapter_triples import Phase2NEWAdapterTriples as Phase2NEWAdapter
```

### Option 3: Configuration Flag

Add a configuration flag to `main_pipeline.py`:
```python
# Add to the top with other flags
USE_TRIPLE_EXTRACTION = True  # New flag for triple extraction

# Then in run_ner_stage():
if USE_TRIPLE_EXTRACTION:
    from scripts.graph_rag_stages.phase2_building.ner.phase2_new_adapter_triples import Phase2NEWAdapterTriples
    extractor = Phase2NEWExtractor(ner_output_dir, adapter_class=Phase2NEWAdapterTriples)
else:
    extractor = Phase2NEWExtractor(ner_output_dir)  # Uses default adapter
```

## Benefits of Integration

1. **Performance**: ~3x faster (single LLM call vs three)
2. **Accuracy**: Better relationship extraction with full context
3. **Cost**: Lower API costs (fewer LLM calls)
4. **Maintainability**: Simpler code, fewer failure points

## Testing Before Full Integration

```bash
# Test triple extraction standalone
cd scripts/graph_rag_stages/phase2_NEW/
python test_triple_extraction.py

# Run on sample chunks
python simple_ner_triples.py --use-triples --chunk-dir simple_ner_graph/document_chunks
```

## Rollback Plan

If issues arise, simply:
1. Set `USE_TRIPLE_EXTRACTION=false` 
2. Or revert the import change
3. The output format remains identical, so no downstream changes needed
