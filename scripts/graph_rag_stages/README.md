# Unified GraphRAG Pipeline

This is the unified, modular GraphRAG pipeline that combines and replaces the previous `graph_stages` and `microsoft_framework` directories. The pipeline is designed to be testable, maintainable, and easily configurable.

## Structure

```
graph_rag_stages/
├── main_pipeline.py              # Main orchestrator with boolean flags
├── phase1_preprocessing/          # Data extraction and processing
│   ├── pdf_extractor.py          # PDF text extraction with Docling
│   ├── agenda_extractor.py       # Agenda-specific extraction with LLM
│   ├── document_linker.py        # Generic document processing
│   └── transcript_linker.py      # Verbatim transcript processing
├── phase2_building/               # Graph construction (dual approach)
│   ├── custom_graph_builder.py   # Custom graph for Cosmos DB
│   ├── graphrag_adapter.py       # Data preparation for GraphRAG
│   ├── graphrag_indexer.py       # Microsoft GraphRAG indexing
│   └── entity_deduplicator.py    # Post-processing optimization
├── phase3_querying/               # Query processing and response
│   ├── query_engine.py           # Main query interface
│   ├── query_router.py           # Intelligent query routing
│   ├── response_enhancer.py      # Response post-processing
│   └── source_tracker.py         # Provenance tracking
├── common/                        # Shared utilities
│   ├── config.py                 # Configuration management
│   ├── cosmos_client.py          # Cosmos DB client
│   └── utils.py                  # Common utility functions
└── ui/                           # User interfaces
    └── query_app.py              # Query application UI
```

## Usage

### Main Pipeline Execution

The pipeline is controlled via boolean flags in `main_pipeline.py`:

```python
# Configure what runs
RUN_DATA_PREPROCESSING = True        # Extract and process PDFs
RUN_CUSTOM_GRAPH_PIPELINE = False   # Build custom graph (Cosmos DB)
RUN_GRAPHRAG_INDEXING_PIPELINE = True # Build GraphRAG index
RUN_QUERY_ENGINE = True             # Setup query capabilities

# Run the pipeline with arguments
python -m graph_rag_stages.main_pipeline --source-dir "path/to/pdfs"
```

### Individual Components

You can also use individual components:

```python
# Data preprocessing only
from graph_rag_stages.phase1_preprocessing import run_extraction_pipeline
await run_extraction_pipeline(source_dir, output_dir)

# GraphRAG indexing only
from graph_rag_stages.phase2_building import run_graphrag_indexing_pipeline
await run_graphrag_indexing_pipeline(markdown_dir, graphrag_dir)

# Query engine
from graph_rag_stages.phase3_querying import QueryEngine
engine = QueryEngine(graphrag_root)
response = await engine.answer_query("What agenda items were discussed?")
```

### Configuration

Configure the pipeline via:
1. Environment variables (`.env` file)
2. `settings.yaml` file in project root  
3. Direct configuration in code
4. Command line arguments

Required environment variables:
```bash
OPENAI_API_KEY=your_openai_key
COSMOS_ENDPOINT=your_cosmos_endpoint    # Optional, for custom graph
COSMOS_KEY=your_cosmos_key              # Optional, for custom graph
```

Command line usage:
```bash
# Use default source directory
python -m graph_rag_stages.main_pipeline

# Specify custom source directory
python -m graph_rag_stages.main_pipeline --source-dir "path/to/your/pdfs"
```

## Pipeline Stages

### 1. Data Preprocessing (`phase1_preprocessing/`)

Converts source PDFs into enriched markdown files:
- **PDF Extraction**: Uses Docling for OCR and structure preservation
- **Agenda Processing**: LLM-enhanced extraction of agenda items and metadata  
- **Document Linking**: Connects ordinances/resolutions to agenda items
- **Transcript Processing**: Handles verbatim transcripts with item linking

**Output**: Enriched markdown files with comprehensive metadata headers

### 2. Graph Building (`phase2_building/`)

Two parallel approaches for graph construction:

#### Custom Graph Pipeline (Optional)
- Builds knowledge graph in Azure Cosmos DB
- Creates entities for documents, meetings, agenda items
- Establishes relationships between entities
- Enables graph-based queries via Gremlin

#### GraphRAG Pipeline  
- Adapts markdown to GraphRAG CSV format
- Runs Microsoft GraphRAG indexing
- Optional entity deduplication for quality improvement
- Creates entity/relationship extraction and community detection

**Output**: Either Cosmos DB graph or GraphRAG parquet files

### 3. Query and Response (`phase3_querying/`)

Intelligent query processing system:
- **Query Routing**: Determines optimal query method (global vs local)
- **Query Execution**: Interfaces with GraphRAG via subprocess
- **Response Enhancement**: Cleans and enriches responses
- **Source Tracking**: Provides provenance and citations

**Output**: Enhanced responses with source attribution

## Key Features

### Boolean Control System
Easy on/off switching for pipeline components:
```python
RUN_DATA_PREPROCESSING = True      # Process source documents
RUN_CUSTOM_GRAPH_PIPELINE = False # Skip Cosmos DB graph  
RUN_GRAPHRAG_INDEXING_PIPELINE = True # Build GraphRAG index
```

### Modular Architecture
Each stage is independently testable and can be run separately.

### Dual Graph Approach
- **Custom Graph**: Traditional knowledge graph in Cosmos DB
- **GraphRAG**: Microsoft's approach with LLM-enhanced indexing

### Rich Metadata Headers
Generated markdown includes comprehensive headers for improved GraphRAG performance:
```markdown
---
DOCUMENT METADATA AND CONTEXT
=============================

**DOCUMENT IDENTIFICATION:**
- Document Type: AGENDA
- Meeting Date: 01.09.2024

**SEARCHABLE IDENTIFIERS:**
- AGENDA_ITEM: E-1
- AGENDA_ITEM: E-2
---
```

### Intelligent Query Routing
Automatically determines the best query method based on query characteristics.

### Configurable Source Directory
The pipeline now accepts command-line arguments for flexible source directory specification.

## Migration from Old Structure

The unified pipeline replaces:
- `scripts/graph_stages/` → `graph_rag_stages/phase1_preprocessing/` + `graph_rag_stages/phase2_building/custom_graph_builder.py`
- `scripts/microsoft_framework/` → `graph_rag_stages/phase2_building/` (GraphRAG components) + `graph_rag_stages/phase3_querying/`

All existing functionality has been preserved and enhanced in the new structure.

## Dependencies

Core dependencies:
- `docling` - PDF processing
- `openai` - LLM operations  
- `graphrag` - Microsoft GraphRAG
- `pandas` - Data manipulation
- `azure-cosmos` - Cosmos DB (if using custom graph)
- `fitz` (PyMuPDF) - PDF hyperlink extraction

Install with:
```bash
pip install -r requirements.txt
```

## Corrections Applied

This version addresses the following corrections:
1. **Valid Python Identifiers**: Renamed directories from `1_data_preprocessing` to `phase1_preprocessing`, etc.
2. **Proper Import Structure**: Updated all imports to use valid package names
3. **Command Line Arguments**: Added argument parsing for configurable source directory
4. **Complete Implementation**: Filled in placeholder logic throughout the pipeline
5. **Robust Error Handling**: Enhanced error checking and validation
6. **Comprehensive Documentation**: Updated documentation to reflect all changes 