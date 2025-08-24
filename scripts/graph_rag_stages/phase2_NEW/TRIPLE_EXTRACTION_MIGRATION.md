# Triple Extraction Migration Guide

## Overview

This document describes the migration from the three-phase extraction approach to a single-phase triple extraction approach for NER (Named Entity Recognition) in city government documents.

## Key Changes

### 1. **Extraction Approach**

**Before (Three-Phase):**
- Phase 1: Extract entities with basic attributes
- Phase 2: Extract relationships between found entities  
- Phase 3: Enhance entity attributes
- Required 3 separate LLM calls per chunk

**After (Triple-Based):**
- Single LLM call extracts entities AND relationships as triples
- Format: `(subject, predicate, object)` where subject/object are entities
- More efficient and captures relationships in context

### 2. **New Files Created**

- `ner_prompt_triples.txt` - New prompt template for triple extraction
- `simple_ner_triples.py` - Updated extraction logic supporting both modes
- `test_triple_extraction.py` - Test script to validate the new approach

### 3. **Backward Compatibility**

The new implementation maintains 100% backward compatibility:
- Same output format (entities by type + relationships)
- Same ID generation logic
- Same ontology validation
- Same persistence structure

## Usage

### Option 1: Environment Variable (Recommended)

Set the extraction mode via environment variable:

```bash
# Use triple extraction (default)
export USE_TRIPLE_EXTRACTION=true
python simple_ner_triples.py --chunk-dir simple_ner_graph/document_chunks

# Use legacy three-phase extraction
export USE_TRIPLE_EXTRACTION=false
python simple_ner_triples.py --chunk-dir simple_ner_graph/document_chunks
```

### Option 2: Command Line Arguments

Override the extraction mode:

```bash
# Force triple extraction
python simple_ner_triples.py --use-triples --chunk-dir simple_ner_graph/document_chunks

# Force legacy extraction  
python simple_ner_triples.py --use-legacy --chunk-dir simple_ner_graph/document_chunks
```

### Option 3: Process Single Chunk

```bash
# Process a specific chunk file
python simple_ner_triples.py --chunk-file path/to/chunk.txt
```

## Triple Format

The new extraction produces triples in this format:

```json
{
  "triples": [
    {
      "subject": {
        "type": "Person",
        "id": "person_commissioner_smith",
        "attributes": {
          "personID": "person_commissioner_smith",
          "name": "Commissioner Smith",
          "title": "Commissioner",
          "affiliation": "City Council",
          "contactInfo": null
        }
      },
      "predicate": "performsAction",
      "object": {
        "type": "Action",
        "id": "action_approve",
        "attributes": {
          "actionID": "action_approve",
          "type": "approve",
          "dateTime": "2024-01-09",
          "outcome": "passed",
          "details": "Motion to approve ordinance"
        }
      },
      "provenance": {
        "chunkId": "chunk_123",
        "textEvidence": "Commissioner Smith moved to approve..."
      }
    }
  ]
}
```

## Benefits of Triple Extraction

1. **Efficiency**: Single LLM call instead of three
2. **Context Preservation**: Relationships extracted with full entity context
3. **Reduced Errors**: No need to match entities across phases
4. **Better Provenance**: Direct text evidence for each triple
5. **Simpler Pipeline**: Less orchestration code needed

## Testing

Run the test script to compare both approaches:

```bash
python test_triple_extraction.py
```

This will:
1. Test the triple format directly
2. Compare results between triple and legacy extraction
3. Validate output compatibility

## Migration Checklist

- [x] Create new triple-based prompt template
- [x] Implement triple extraction function
- [x] Add triple-to-entity/relationship conversion
- [x] Maintain backward compatibility
- [x] Create test scripts
- [x] Document the migration

## Notes

- The triple extraction maintains all existing standardization and normalization
- ID generation rules remain exactly the same
- All ontology validation is preserved
- Output format is identical for downstream compatibility
- Can switch between modes without code changes
