# Dual-Graph Municipal Knowledge System

A sophisticated data processing pipeline that transforms unstructured city government documents (PDFs) into a structured and queryable knowledge graph in Cosmos DB Gremlin and a searchable vector index in Azure Cognitive Search.

## 🚀 Executive Summary

This system processes municipal documents such as meeting agendas, verbatim transcripts, and legal documents through a **sequential refinement philosophy** that progressively builds precision:

- **Taxonomy Graph (The Skeleton)**: Extracts high precision from structured elements like agenda titles and item codes
- **NER Graph (The Flesh)**: Adds broad coverage from Named Entity Recognition over full text
- **Unified through deduplication and merge** for comprehensive knowledge representation

### What You Get

- **Cosmos DB Gremlin graph** for complex relationship queries
- **Azure Cognitive Search vector index** for semantic search
- **JSON manifests** for entities, relationships, and merge mappings
- **Query interface** supporting 4 distinct query types with intelligent routing

## 🏗️ System Architecture

### 7-Stage Processing Pipeline

```
PDF → Knowledge Graph + Vector Index
```

| Stage | Name | Purpose |
|-------|------|---------|
| 1 | **Data Preprocessing** | PDFs → structured JSON with 3-stage extraction |
| 2 | **JSON→Markdown Conversion** | Format bridge for NER processing |
| 3 | **Taxonomy Synthesis** | Deterministic parsing for high precision entities |
| 4 | **NER Extraction** | Azure OpenAI GPT-4.1 nano for broad entity coverage |
| 5 | **Deduplication & Merge** | Unify taxonomy and NER entities |
| 6 | **Graph Construction** | Push validated data to Cosmos DB |
| 7 | **Vector DB Push** | Index chunks in Azure Cognitive Search |

### Key Technologies

- **Document Processing**: Docling, PyMuPDF for OCR and structure extraction
- **AI/ML**: Azure OpenAI GPT-4.1 nano for entity extraction and classification
- **Graph Database**: Cosmos DB with Gremlin API
- **Vector Search**: Azure Cognitive Search
- **Language**: Python with asyncio for concurrent processing

## 📋 Prerequisites

- Python 3.9+
- Azure OpenAI account with GPT-4.1 nano access
- Cosmos DB account with Gremlin API
- Azure Cognitive Search service
- Virtual environment (recommended)

## 🔧 Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/g-troiani/Coral-Gables-Knologi-City-Clerk-RAG.git
   cd graph_database
   ```

2. **Create and activate virtual environment**
   ```bash
   python3 -m venv graphrag_env
   source graphrag_env/bin/activate  # On macOS/Linux
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   Create a `.env` file with your Azure credentials:
   ```env
   # Azure OpenAI Configuration
   AZURE_OPENAI_ENDPOINT=your_endpoint
   AZURE_OPENAI_API_KEY=your_api_key
   AZURE_OPENAI_API_VERSION=your_api_version
   AZURE_OPENAI_DEPLOYMENT_NAME=your_deployment_name
   AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT=text-embedding-ada-002
   
   # Cosmos DB Configuration
   COSMOS_ENDPOINT=your_cosmos_endpoint
   COSMOS_KEY=your_cosmos_key
   COSMOS_DATABASE=cgGraph
   COSMOS_CONTAINER=cityClerk
   
   # Azure Cognitive Search Configuration
   AZURE_SEARCH_ENDPOINT=your_search_endpoint
   VECTOR_DATABASE_KEY=your_search_key
   VECTOR_DATABASE_NAME=city-clerk-rag
   
   # Processing Configuration (optional - has defaults)
   ENTITY_CANDIDATE_THRESHOLD=0.85
   ENTITY_FINAL_THRESHOLD=0.95
   ```

## 🚀 Usage

### Processing Documents

#### 📁 Input File Organization

The system automatically scans for PDF documents in the `city_clerk_documents/` directory. **Place your PDF files here** organized by document type:

```
city_clerk_documents/
├── agenda/                    # Meeting agendas (any PDF format)
├── legal/                     # Ordinances, resolutions, policies  
├── verbatim/                  # Meeting transcripts
└── global/                    # General municipal documents
    └── City Comissions 2024/  # Example: organized by year/type
```

#### 📋 File Requirements & Naming

- **Supported formats**: PDF files only
- **Naming convention**: The system preserves original filenames and uses them for deterministic linking
- **Special linking**: Files like `MM_DD_YYYY - Verbatim Transcripts - <agenda-codes>.pdf` automatically link transcripts to agenda items
- **Change detection**: Uses SHA-256 checksums to detect new or modified files
- **No preprocessing needed**: System handles OCR and text extraction automatically

#### 🔄 Processing Workflow

1. **Place your PDF documents** in the appropriate subdirectory above
2. **Run the complete pipeline** (processes all documents):
   ```bash
   ./run_pipeline.sh
   ```
3. **For incremental processing** (only new/modified documents):
   ```bash
   ./run_pipeline.sh --incremental
   ```

#### 📤 Output Organization

The system processes your PDFs and creates structured outputs:

```
city_clerk_documents/
├── extracted_json/           # Stage 1: Structured JSON output
│   ├── agenda/              # Processed agenda files  
│   ├── legal/               # Processed legal documents
│   └── verbatim/            # Processed transcripts
└── extracted_markdown/      # Stage 2: Markdown for NER processing
```

#### 🔍 Document Discovery

- **Automatic scanning**: System recursively scans all subdirectories under `city_clerk_documents/global/`
- **Skip logic**: Already processed files are automatically skipped unless modified
- **Prioritization**: Partially processed files get priority in processing queue
- **Progress tracking**: Processing status maintained in internal registry

### Querying the System

Launch the query interface:
```bash
python3 ui/query_app.py
```

The system supports 4 query types:
- **SPECIFIC_FACT**: Direct lookups with clear identifiers
- **GENERAL_INFO**: Broad thematic requests
- **COMPLEX_HYBRID**: Multi-hop queries requiring both graph and vector search
- **UNCLEAR**: Ambiguous queries with disambiguation support

## 🎯 Key Features

### Sequential Refinement Approach
- **Phase 1**: Taxonomy extraction from structured JSON (high precision)
- **Phase 2**: NER extraction from markdown (broad coverage) 
- **Phase 3**: Sequential processing with Phase 1 entities feeding into NER for context
- **Unification**: Through sophisticated deduplication and merge

### Intelligent Entity Deduplication
- **Multi-source similarity scoring**: 60% name similarity + 30% attribute overlap + 10% contextual similarity
- **Two-stage thresholds**: Candidate generation (~0.85) + final merge (~0.95)
- **Taxonomy priority**: Authoritative taxonomy data preserved over NER duplicates
- **Relationship rewiring**: Automatic endpoint updates to canonical entity IDs

### Robust Processing
- **Incremental processing**: Skip already-processed documents
- **Fallback mechanisms**: Regex fallbacks for LLM failures
- **Error resilience**: Continue processing despite individual failures
- **Comprehensive provenance**: Full traceability from source to output

### Advanced Querying
- **Intelligent routing**: Automatic query type classification
- **Multi-modal fusion**: Combine graph precision with vector semantic search
- **Confidence scoring**: Calibrated confidence levels with source attribution
- **Rich citations**: Inline references with document, page, and chunk provenance

## 📁 Project Structure

```
graph_database/
├── scripts/                    # Core processing pipeline
│   └── graph_rag_stages/      # 7-stage pipeline implementation
│
├── city_clerk_documents/      # INPUT: Place your PDF files here
│   ├── agenda/                # → Meeting agenda PDFs
│   ├── legal/                 # → Ordinance, resolution PDFs  
│   ├── verbatim/             # → Meeting transcript PDFs
│   ├── global/               # → General municipal PDFs
│   ├── extracted_json/       # OUTPUT: Stage 1 structured JSON
│   └── extracted_markdown/   # OUTPUT: Stage 2 markdown files
│
├── simple_ner_graph/         # Processing workspace & outputs
│   ├── registry/             # Stage 3: Taxonomy entities
│   ├── entities/             # Stage 4: NER entities 
│   ├── merged/               # Stage 5: Final deduplicated entities
│   ├── relationships/        # Extracted relationships
│   └── document_chunks/      # Text chunks for vector indexing
│
├── ui/                       # Query interface
│   └── query_app.py         # Launch query system
├── tools/                    # Utility scripts
├── logs/                     # Processing logs and history
└── requirements.txt          # Python dependencies
```

## 🔍 Entity Types & Ontology

### Core Entity Classes
- **People & Organizations**: Person, Organization, Role
- **Documents & Legal**: Document, AgendaDocument, Policy, Contract, LegalReference
- **Meetings & Structure**: Meeting (Event), Section, AgendaItem, Board
- **Actions & Outcomes**: Event, Action, VoteOutcome
- **Resources & Context**: Asset, Project, Location, Technology, Topic
- **Participation**: Presentation, PublicComment, Appointment

### Relationship Examples
- `Section hasAgendaItem → AgendaItem`
- `Person votesOn → VoteOutcome`
- `AgendaItem implements → Policy`
- `Meeting hasAgenda → Document`

## 📊 Performance & Monitoring

### Processing Metrics
- **Document state tracking**: SHA-256 hashes for change detection
- **Batch optimization**: 500-item batches for database operations
- **Parallel processing**: Concurrent document processing with rate limiting
- **Comprehensive logging**: Validation, merge, and conflict logs

### Quality Gates
- **Stage 1**: Structural fidelity, metadata completeness, JSON integrity
- **Stage 3**: Deterministic ID generation, taxonomy relationships
- **Stage 5**: Entity deduplication accuracy, relationship integrity
- **Stage 6**: Schema validation, referential integrity

## 🛠️ Development

### Running Tests
```bash
python3 -m pytest test/
```

### Debug Mode
```bash
python3 enable_debug.py
```

### Viewing Processing Logs
```bash
./manage_logs.sh
```

### Clean Slate Processing
```bash
python3 delete_all_data.py  # Warning: Deletes all processed data
```

## 📈 Scaling Considerations

- **Incremental processing**: Designed for ongoing document ingestion
- **Azure scaling**: Leverages Azure services' built-in scalability
- **Batch processing**: Optimized batch sizes for performance
- **Connection pooling**: Efficient database connection management
- **Rate limiting**: Respects API quotas and limits

## 🤝 Contributing

1. Follow the coding protocol: minimal, clean, focused code
2. Include tests for new functionality
3. Update documentation for significant changes
4. Use the project's naming conventions (preserve digits in filenames)
5. Test with both incremental and full processing modes

## 📄 License

[Add license information]

## 📞 Support

For technical issues or questions:
1. Check the logs in the `logs/` directory
2. Review processing status and troubleshoot with debug utilities
3. Consult the codebase documentation and inline comments for implementation details
