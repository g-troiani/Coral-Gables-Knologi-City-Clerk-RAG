# Unified City Clerk Knowledge Graph Pipeline

This is the unified, modular pipeline that combines NER-based entity extraction with a custom knowledge graph. The pipeline is designed to be testable, maintainable, and easily configurable.

## Structure

```
graph_rag_stages/
├── main_pipeline.py              # Main orchestrator with boolean flags
├── phase1_preprocessing/          # Data extraction and processing
│   ├── pdf_extractor.py          # PDF text extraction with Docling
│   ├── agenda_extractor.py       # Agenda-specific extraction with LLM
│   ├── document_linker.py        # Generic document processing
│   └── transcript_linker.py      # Verbatim transcript processing
├── phase2_building/               # Graph construction
│   ├── custom_graph_builder.py   # Custom graph for Cosmos DB
│   ├── local_graph_builder.py    # Local graph with NetworkX
│   └── entity_deduplicator.py    # Post-processing optimization
├── phase3_querying/               # Query processing and response
│   ├── ner/                      # NER-based querying
│   │   ├── graph_query_agent.py  # Gremlin query generation
│   │   └── simple_query_engine.py # Entity-based retrieval
│   ├── query_router.py           # Intelligent query routing
│   ├── response_enhancer.py      # Response post-processing
│   └── source_tracker.py         # Provenance tracking
└── common/                        # Shared utilities
    ├── config.py                 # Configuration management
    ├── cosmos_client.py          # Cosmos DB client
    └── utils.py                  # Common utility functions
```

## Usage

### Main Pipeline Execution

The pipeline is controlled via boolean flags in `main_pipeline.py`:

```python
# Configure what runs
RUN_DATA_PREPROCESSING = True      # Extract and process PDFs
RUN_CUSTOM_GRAPH_PIPELINE = True   # Build custom graph (Cosmos DB)
RUN_NER_PIPELINE = True           # Run NER-based pipeline
```

### Individual Components

You can also use individual components:

```python
# Data preprocessing only
from graph_rag_stages.phase1_preprocessing import run_extraction_pipeline
await run_extraction_pipeline(source_dir, output_dir)

# Custom graph building
from graph_rag_stages.phase2_building import run_cosmos_graph_pipeline
await run_cosmos_graph_pipeline(json_dir)

# NER query engine
from graph_rag_stages.phase3_querying.ner import SimpleNERQueryEngine
engine = SimpleNERQueryEngine(output_dir)
response = await engine.query("What agenda items were discussed?")
```

### Configuration

Configure the pipeline via:
1. Environment variables (`.env` file)
2. `settings.yaml` file in project root  
3. Direct configuration in code
4. Command line arguments

Required environment variables:
```bash
AZURE_OPENAI_API_KEY=your_azure_openai_key
AZURE_OPENAI_ENDPOINT=your_azure_endpoint
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

Converts source PDFs into enriched JSON and markdown files:
- **PDF Extraction**: Uses Docling for OCR and structure preservation
- **Agenda Processing**: LLM-enhanced extraction of agenda items and metadata  
- **Document Linking**: Connects ordinances/resolutions to agenda items
- **Transcript Processing**: Handles verbatim transcripts with item linking

**Output**: Enriched JSON files with comprehensive metadata

### 2. Graph Building (`phase2_building/`)

Two approaches for graph construction:

#### Custom Graph Pipeline
- Builds knowledge graph in Azure Cosmos DB
- Creates entities for documents, meetings, agenda items
- Establishes relationships between entities
- Enables graph-based queries via Gremlin

#### Local Graph Pipeline
- Uses NetworkX for local graph building
- No cloud dependencies required
- Useful for development and testing
- Supports GraphML export

**Output**: Either Cosmos DB graph or local NetworkX graph

### 3. Query and Response (`phase3_querying/`)

Intelligent query processing system:
- **NER-based Retrieval**: Entity-focused document retrieval
- **Graph Query Agent**: Translates queries to Gremlin
- **Query Routing**: Determines optimal query method
- **Response Enhancement**: Cleans and enriches responses
- **Source Tracking**: Provides provenance and citations

**Output**: Enhanced responses with source attribution

## Key Features

### Boolean Control System
Easy on/off switching for pipeline components:
```python
RUN_DATA_PREPROCESSING = True      # Process source documents
RUN_CUSTOM_GRAPH_PIPELINE = True   # Build Cosmos DB graph  
RUN_NER_PIPELINE = True           # Run NER pipeline
```

### Modular Architecture
Each stage is independently testable and can be run separately.

### Dual Graph Approach
- **Custom Graph**: Traditional knowledge graph in Cosmos DB
- **Local Graph**: NetworkX-based graph for development

### Rich Metadata Headers
Generated markdown includes comprehensive headers:
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
The pipeline accepts command-line arguments for flexible source directory specification.

## Dependencies

Core dependencies:
- `docling` - PDF processing
- `openai` - Azure OpenAI operations
- `azure-cosmos` - Cosmos DB (if using custom graph)
- `networkx` - Local graph operations
- `pandas` - Data manipulation
- `fitz` (PyMuPDF) - PDF hyperlink extraction

Install with:
```bash
pip install -r requirements.txt
``` 