# Vector Database Setup (REQUIRED)

The vector database is an integral part of this application. You MUST configure Azure Cognitive Search before running the pipeline.

## Required Environment Variables

Add these to your `.env` file:

```bash
# Azure Cognitive Search (REQUIRED)
AZURE_SEARCH_ENDPOINT="https://your-search-service.search.windows.net"
VECTOR_DATABASE_KEY="your-search-api-key"
VECTOR_DATABASE_NAME="city-clerk-rag"  # or use default

# Azure OpenAI Embeddings (REQUIRED for vector DB)
AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT="text-embedding-ada-002"  # Your deployment name
```

## How to Get These Values

1. **AZURE_SEARCH_ENDPOINT**: 
   - Go to Azure Portal > Your Cognitive Search Service > Overview
   - Copy the URL (e.g., https://mysearch.search.windows.net)

2. **VECTOR_DATABASE_KEY**:
   - Go to Azure Portal > Your Cognitive Search Service > Keys
   - Copy either the primary or secondary admin key

3. **AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT**:
   - Go to Azure Portal > Your Azure OpenAI Service > Deployments
   - Find your text-embedding-ada-002 deployment name

## Verify Setup

Run this command to verify your setup:
```bash
python -m scripts.graph_rag_stages.test_vector_db_connection
```