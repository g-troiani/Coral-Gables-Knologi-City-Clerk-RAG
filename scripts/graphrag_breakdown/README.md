# City Clerk GraphRAG System

Microsoft GraphRAG integration for city clerk document processing with advanced entity extraction, community detection, and intelligent query routing.

## 🚀 Quick Start

### 1. Set up your environment:
```bash
export OPENAI_API_KEY='your-api-key-here'
```

### 2. Run the complete pipeline:
```bash
./run_graphrag.sh run
```

### 3. Test queries interactively:
```bash
./run_graphrag.sh query
```

## 📋 Prerequisites

1. **Environment Variables**:
   ```bash
   export OPENAI_API_KEY='your-openai-api-key'
   ```

2. **Extracted Documents**: 
   - City clerk documents should be extracted as JSON files in `city_clerk_documents/extracted_text/`
   - Run your existing document extraction pipeline first

3. **Dependencies**:
   - Python 3.8+
   - GraphRAG library
   - All dependencies in `requirements.txt`

## 🛠️ Installation & Setup

### Option 1: Using the Shell Script (Recommended)
```bash
# Setup environment and dependencies
./run_graphrag.sh setup

# Run the complete pipeline
./run_graphrag.sh run
```

### Option 2: Manual Setup
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run pipeline
python3 scripts/graphrag_breakdown/run_graphrag_pipeline.py
```

## 🔍 Query System

The system supports three types of queries with automatic routing:

### 1. 🎯 Local Search (Entity-Specific)
Best for specific entities and their immediate relationships:
```
"Who is Commissioner Smith?"
"What is ordinance 2024-01?"
"Tell me about agenda item E-1"
```

### 2. 🌐 Global Search (Holistic)
Best for broad themes and dataset-wide analysis:
```
"What are the main themes in city development?"
"Summarize all budget discussions"
"Overall trends in housing policy"
```

### 3. 🔄 DRIFT Search (Temporal/Complex)
Best for temporal changes and complex exploratory queries:
```
"How has the waterfront project evolved?"
"Timeline of budget decisions"
"Development of housing policy over time"
```

## 📊 Pipeline Steps

The GraphRAG pipeline includes:

1. **🔧 Environment Setup** - Initialize GraphRAG with city clerk configuration
2. **📄 Document Adaptation** - Convert extracted JSON documents to GraphRAG format
3. **🎯 Prompt Tuning** - Auto-tune prompts for city government domain
4. **🏗️ GraphRAG Indexing** - Extract entities, relationships, and communities
5. **📊 Results Processing** - Load and summarize GraphRAG outputs
6. **🔍 Query Testing** - Test with example queries
7. **🌐 Cosmos DB Sync** - Optionally sync to existing Cosmos DB

## 📁 Output Structure

After running the pipeline, you'll find:

```
graphrag_data/
├── settings.yaml           # GraphRAG configuration
├── city_clerk_documents.csv # Input documents in GraphRAG format
├── prompts/                # Auto-tuned prompts
│   ├── entity_extraction.txt
│   └── community_report.txt
└── output/                 # GraphRAG results
    ├── entities.parquet    # Extracted entities
    ├── relationships.parquet # Entity relationships
    ├── communities.parquet # Community clusters
    └── community_reports.parquet # Community summaries
```

## 🎮 Usage Examples

### Run Complete Pipeline
```bash
./run_graphrag.sh run
```

### Interactive Query Session
```bash
./run_graphrag.sh query
```

### View Results Summary
```bash
./run_graphrag.sh results
```

### Clean Up Data
```bash
./run_graphrag.sh clean
```

### Example Queries to Try

**Entity-specific (Local Search):**
- "Who is Mayor Johnson?"
- "What is resolution 2024-15?"
- "Tell me about the parks department"

**Holistic (Global Search):**
- "What are the main development themes?"
- "Summarize all transportation discussions"
- "Overall budget allocation patterns"

**Temporal (DRIFT Search):**
- "How has zoning policy evolved?"
- "Timeline of infrastructure projects"
- "Development of affordable housing initiatives"

## ⚙️ Configuration

### Model Settings
The system is configured to use:
- **Model**: `gpt-4.1-mini-2025-04-14`
- **Max Tokens**: `32768`
- **Temperature**: `0` (deterministic)

### Entity Types
Configured for city government entities:
- `person`, `organization`, `location`
- `document`, `meeting`, `agenda_item`
- `money`, `project`, `ordinance`, `resolution`, `contract`

### Query Configuration
- **Global Search**: Community-level analysis with dynamic selection
- **Local Search**: Top-K entity retrieval with community context
- **DRIFT Search**: Iterative exploration with follow-up expansion

## 🔧 Advanced Usage

### Python API
```python
from scripts.graphrag_breakdown import CityClerkQueryEngine

# Initialize query engine
engine = CityClerkQueryEngine("./graphrag_data")

# Auto-routed query
result = await engine.query("What are the main budget themes?")

# Specific method
result = await engine.query(
    "Who is Commissioner Smith?", 
    method="local",
    top_k_entities=15
)
```

### Custom Query Routing
```python
from scripts.graphrag_breakdown import SmartQueryRouter

router = SmartQueryRouter()
route_info = router.determine_query_method("Your query here")
print(f"Recommended method: {route_info['method']}")
```

### Cosmos DB Integration
```python
from scripts.graphrag_breakdown import GraphRAGCosmosSync

sync = GraphRAGCosmosSync("./graphrag_data/output")
await sync.sync_to_cosmos()
```

## 📈 Performance Tips

1. **Incremental Processing**: Use `IncrementalGraphRAGProcessor` for new documents
2. **Community Levels**: Adjust community levels for different query scopes
3. **Query Optimization**: Use specific entity names and agenda codes when known
4. **Batch Processing**: Process documents in batches for large datasets

## 🐛 Troubleshooting

### Common Issues

**GraphRAG not found:**
```bash
pip install graphrag
```

**No documents found:**
- Ensure documents are in `city_clerk_documents/extracted_text/`
- Run document extraction pipeline first

**API Key issues:**
```bash
export OPENAI_API_KEY='your-key-here'
```

**Memory issues:**
- Reduce `max_tokens` in settings
- Process documents in smaller batches

### Debug Mode
```bash
# Run with verbose output
python3 scripts/graphrag_breakdown/run_graphrag_pipeline.py --verbose

# Check GraphRAG logs
tail -f graphrag_data/logs/*.log
```

## 🤝 Integration with Existing System

This GraphRAG system integrates seamlessly with your existing infrastructure:

- **Reuses**: Docling PDF extraction, URL preservation, Cosmos DB client
- **Extends**: Adds advanced entity extraction and community detection
- **Maintains**: Existing graph schema and document processing pipeline
- **Enhances**: Query capabilities with intelligent routing

## 📚 More Information

- [Microsoft GraphRAG Documentation](https://microsoft.github.io/graphrag/)
- [Query Configuration Guide](./city_clerk_settings_template.yaml)
- [Entity Types and Prompts](./prompt_tuner.py)

---

For issues or questions, check the troubleshooting section or review the pipeline logs in `graphrag_data/logs/`. 