# Incremental Processing System

## Overview

The graph database system now supports incremental processing, allowing you to add new documents without reprocessing the entire dataset. This significantly reduces processing time and preserves existing data integrity.

## Features

### 1. Document State Tracking
- **Processing Registry** (`scripts/graph_rag_stages/common/processing_registry.py`)
  - Tracks processed documents using SHA-256 hashes
  - Detects new or modified files automatically
  - Maintains processing history and statistics

### 2. Incremental Extraction
- **Incremental Pipeline** (`scripts/graph_rag_stages/phase1_preprocessing/incremental_extraction.py`)
  - Processes only new/modified PDFs
  - Skips already processed documents
  - Falls back to full processing if issues detected

### 3. Entity Versioning
- **Version Tracking** (`scripts/graph_rag_stages/common/entity_versioning.py`)
  - Tracks entity versions and change history
  - Adds metadata: `_version`, `_created_at`, `_updated_at`
  - Supports rollback capability

### 4. Smart Entity Merging
- **Incremental Merger** (`scripts/graph_rag_stages/phase2_building/incremental_entity_merger.py`)
  - Merges new entities with existing ones
  - Preserves existing relationships
  - Handles property conflicts intelligently
  - Union strategy for list properties (roles, affiliations)

### 5. Incremental Cosmos Updates
- **Enhanced Graph Builder**
  - Updates existing vertices instead of overwriting
  - Preserves entity history
  - Tracks update statistics

## Usage

### Full Processing (Default)
```bash
# Process all documents from scratch
python -m scripts.graph_rag_stages.main_pipeline
```

### Incremental Processing
```bash
# Process only new/modified documents
python -m scripts.graph_rag_stages.main_pipeline --incremental

# Process specific folder with new meeting documents
python -m scripts.graph_rag_stages.main_pipeline --incremental --source-dir "city_clerk_documents/global/City Commissions 2024/2024-11-Meeting/"
```

### Debug Mode
```bash
# Run incremental with comprehensive debugging
python -m scripts.graph_rag_stages.main_pipeline --incremental --debug
```

## How It Works

### Stage 1: Document Detection
1. Calculates SHA-256 hash for each PDF
2. Compares with processing registry
3. Identifies new/modified files
4. Skips unchanged documents

### Stage 2: Entity Extraction
1. Processes only new documents
2. Extracts entities with version metadata
3. Maintains source document references

### Stage 3: Deduplication & Merging
1. Loads existing entities from Cosmos
2. Merges with new entities using smart resolution
3. Preserves all relationships
4. Tracks conflicts for review

### Stage 4: Cosmos Push
1. Updates existing vertices with new properties
2. Creates new vertices for new entities
3. Maintains relationship integrity
4. Reports update statistics

## Registry Management

### View Processing Status
```python
from scripts.graph_rag_stages.common.processing_registry import ProcessingRegistry
from pathlib import Path

registry = ProcessingRegistry(Path("simple_ner_graph/registry"))
stats = registry.get_processing_stats()
print(f"Total documents: {stats['total_documents']}")
print(f"Completed: {stats['completed_documents']}")
```

### Rebuild Registry
```python
# Rebuild registry from existing documents
registry.rebuild_from_directory(Path("city_clerk_documents/global"))
```

### Check Registry Integrity
```python
is_valid, issues = registry.validate_registry()
if not is_valid:
    print(f"Registry issues: {issues}")
```

## Best Practices

1. **Regular Incremental Updates**
   - Run incremental updates after each new meeting
   - Place new documents in dated folders

2. **Periodic Full Runs**
   - Run full processing monthly to ensure consistency
   - Helps catch any missed relationships

3. **Backup Before Updates**
   - Registry and entity versions provide recovery
   - Consider Cosmos DB backups for production

4. **Monitor Conflicts**
   - Review merge conflicts in logs
   - Adjust merge strategy if needed

## Configuration

### Environment Variables
```bash
# Entity similarity threshold for merging (default: 0.95)
ENTITY_SIMILARITY_THRESHOLD=0.95

# Cosmos partition configuration
COSMOS_PARTITION_VALUE=your_partition
```

### Merge Strategies
- **Conservative** (default): Only update empty/null properties
- **Aggressive**: Always take new values
- **Custom**: Implement specific rules per entity type

## Troubleshooting

### Registry Issues
```bash
# If registry is corrupted, rebuild it
python -c "from scripts.graph_rag_stages.common.processing_registry import ProcessingRegistry; from pathlib import Path; reg = ProcessingRegistry(Path('simple_ner_graph/registry')); reg.rebuild_from_directory(Path('city_clerk_documents/global'))"
```

### Force Full Processing
```bash
# Ignore incremental and process everything
python -m scripts.graph_rag_stages.main_pipeline
```

### Check What Would Be Processed
```python
from scripts.graph_rag_stages.common.processing_registry import ProcessingRegistry
from pathlib import Path

registry = ProcessingRegistry(Path("simple_ner_graph/registry"))
new_docs = registry.get_new_documents(Path("city_clerk_documents/global"))
print(f"Would process {len(new_docs)} documents")
for doc in new_docs:
    print(f"  - {doc.name}")
```

## Architecture Notes

- **Non-Breaking**: All changes are backward compatible
- **Fail-Safe**: Falls back to full processing on errors
- **Modular**: Each component can be used independently
- **Extensible**: Easy to add new merge strategies or tracking

## Future Enhancements

1. **Differential Sync**: Track exactly what changed in entities
2. **Parallel Processing**: Process multiple new documents concurrently
3. **Web UI**: Visual interface for managing incremental updates
4. **Change Notifications**: Alert when conflicts need review
